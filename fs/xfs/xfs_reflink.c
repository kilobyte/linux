/*
 * Copyright (C) 2016 Oracle.  All Rights Reserved.
 *
 * Author: Darrick J. Wong <darrick.wong@oracle.com>
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it would be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write the Free Software Foundation,
 * Inc.,  51 Franklin St, Fifth Floor, Boston, MA  02110-1301, USA.
 */
#include "xfs.h"
#include "xfs_fs.h"
#include "xfs_shared.h"
#include "xfs_format.h"
#include "xfs_log_format.h"
#include "xfs_trans_resv.h"
#include "xfs_mount.h"
#include "xfs_defer.h"
#include "xfs_da_format.h"
#include "xfs_da_btree.h"
#include "xfs_inode.h"
#include "xfs_trans.h"
#include "xfs_inode_item.h"
#include "xfs_bmap.h"
#include "xfs_bmap_util.h"
#include "xfs_error.h"
#include "xfs_dir2.h"
#include "xfs_dir2_priv.h"
#include "xfs_ioctl.h"
#include "xfs_trace.h"
#include "xfs_log.h"
#include "xfs_icache.h"
#include "xfs_pnfs.h"
#include "xfs_refcount_btree.h"
#include "xfs_refcount.h"
#include "xfs_bmap_btree.h"
#include "xfs_trans_space.h"
#include "xfs_bit.h"
#include "xfs_alloc.h"
#include "xfs_quota_defs.h"
#include "xfs_quota.h"
#include "xfs_btree.h"
#include "xfs_bmap_btree.h"
#include "xfs_reflink.h"
#include "xfs_iomap.h"
#include "xfs_rmap_btree.h"

/*
 * Copy on Write of Shared Blocks
 *
 * XFS must preserve "the usual" file semantics even when two files share
 * the same physical blocks.  This means that a write to one file must not
 * alter the blocks in a different file; the way that we'll do that is
 * through the use of a copy-on-write mechanism.  At a high level, that
 * means that when we want to write to a shared block, we allocate a new
 * block, write the data to the new block, and if that succeeds we map the
 * new block into the file.
 *
 * XFS provides a "delayed allocation" mechanism that defers the allocation
 * of disk blocks to dirty-but-not-yet-mapped file blocks as long as
 * possible.  This reduces fragmentation by enabling the filesystem to ask
 * for bigger chunks less often, which is exactly what we want for CoW.
 *
 * The delalloc mechanism begins when the kernel wants to make a block
 * writable (write_begin or page_mkwrite).  If the offset is not mapped, we
 * create a delalloc mapping, which is a regular in-core extent, but without
 * a real startblock.  (For delalloc mappings, the startblock encodes both
 * a flag that this is a delalloc mapping, and a worst-case estimate of how
 * many blocks might be required to put the mapping into the BMBT.)  delalloc
 * mappings are a reservation against the free space in the filesystem;
 * adjacent mappings can also be combined into fewer larger mappings.
 *
 * When dirty pages are being written out (typically in writepage), the
 * delalloc reservations are converted into real mappings by allocating
 * blocks and replacing the delalloc mapping with real ones.  A delalloc
 * mapping can be replaced by several real ones if the free space is
 * fragmented.
 *
 * We want to adapt the delalloc mechanism for copy-on-write, since the
 * write paths are similar.  The first two steps (creating the reservation
 * and allocating the blocks) are exactly the same as delalloc except that
 * the mappings must be stored in a separate CoW fork because we do not want
 * to disturb the mapping in the data fork until we're sure that the write
 * succeeded.  IO completion in this case is the process of removing the old
 * mapping from the data fork and moving the new mapping from the CoW fork to
 * the data fork.  This will be discussed shortly.
 *
 * For now, unaligned directio writes will be bounced back to the page cache.
 * Block-aligned directio writes will use the same mechanism as buffered
 * writes.
 *
 * CoW remapping must be done after the data block write completes,
 * because we don't want to destroy the old data fork map until we're sure
 * the new block has been written.  Since the new mappings are kept in a
 * separate fork, we can simply iterate these mappings to find the ones
 * that cover the file blocks that we just CoW'd.  For each extent, simply
 * unmap the corresponding range in the data fork, map the new range into
 * the data fork, and remove the extent from the CoW fork.
 *
 * Since the remapping operation can be applied to an arbitrary file
 * range, we record the need for the remap step as a flag in the ioend
 * instead of declaring a new IO type.  This is required for direct io
 * because we only have ioend for the whole dio, and we have to be able to
 * remember the presence of unwritten blocks and CoW blocks with a single
 * ioend structure.  Better yet, the more ground we can cover with one
 * ioend, the better.
 */

/* Trim extent to fit a logical block range. */
static void
xfs_trim_extent(
	struct xfs_bmbt_irec	*irec,
	xfs_fileoff_t		bno,
	xfs_filblks_t		len)
{
	xfs_fileoff_t		distance;
	xfs_fileoff_t		end = bno + len;

	if (irec->br_startoff + irec->br_blockcount <= bno ||
	    irec->br_startoff >= end) {
		irec->br_blockcount = 0;
		return;
	}

	if (irec->br_startoff < bno) {
		distance = bno - irec->br_startoff;
		if (irec->br_startblock != DELAYSTARTBLOCK &&
		    irec->br_startblock != HOLESTARTBLOCK)
			irec->br_startblock += distance;
		irec->br_startoff += distance;
		irec->br_blockcount -= distance;
	}

	if (end < irec->br_startoff + irec->br_blockcount) {
		distance = irec->br_startoff + irec->br_blockcount - end;
		irec->br_blockcount -= distance;
	}
}

/*
 * Determine if any of the blocks in this mapping are shared.
 */
int
xfs_reflink_irec_is_shared(
	struct xfs_inode	*ip,
	struct xfs_bmbt_irec	*irec,
	bool			*shared)
{
	xfs_agnumber_t		agno;
	xfs_agblock_t		agbno;
	xfs_extlen_t		aglen;
	xfs_agblock_t		fbno;
	xfs_extlen_t		flen;
	int			error = 0;

	/* Holes, unwritten, and delalloc extents cannot be shared */
	if (!xfs_is_reflink_inode(ip) ||
	    ISUNWRITTEN(irec) ||
	    irec->br_startblock == HOLESTARTBLOCK ||
	    irec->br_startblock == DELAYSTARTBLOCK) {
		*shared = false;
		return 0;
	}

	trace_xfs_reflink_irec_is_shared(ip, irec);

	agno = XFS_FSB_TO_AGNO(ip->i_mount, irec->br_startblock);
	agbno = XFS_FSB_TO_AGBNO(ip->i_mount, irec->br_startblock);
	aglen = irec->br_blockcount;

	/* Are there any shared blocks here? */
	error = xfs_refcount_find_shared(ip->i_mount, agno, agbno,
			aglen, &fbno, &flen, false);
	if (error)
		return error;
	if (flen == 0) {
		*shared = false;
		return 0;
	}

	*shared = true;
	return 0;
}

/* Find the shared ranges under an irec, and set up delalloc extents. */
static int
xfs_reflink_reserve_cow_extent(
	struct xfs_inode	*ip,
	struct xfs_bmbt_irec	*irec)
{
	struct xfs_bmbt_irec	rec;
	xfs_agnumber_t		agno;
	xfs_agblock_t		agbno;
	xfs_extlen_t		aglen;
	xfs_agblock_t		fbno;
	xfs_extlen_t		flen;
	xfs_fileoff_t		lblk;
	xfs_off_t		foffset;
	xfs_extlen_t		distance;
	size_t			fsize;
	int			error = 0;

	/* Holes, unwritten, and delalloc extents cannot be shared */
	if (ISUNWRITTEN(irec) ||
	    irec->br_startblock == HOLESTARTBLOCK ||
	    irec->br_startblock == DELAYSTARTBLOCK)
		return 0;

	trace_xfs_reflink_reserve_cow_extent(ip, irec);
	agno = XFS_FSB_TO_AGNO(ip->i_mount, irec->br_startblock);
	agbno = XFS_FSB_TO_AGBNO(ip->i_mount, irec->br_startblock);
	lblk = irec->br_startoff;
	aglen = irec->br_blockcount;

	while (aglen > 0) {
		/* Find maximal fork range within this extent */
		error = xfs_refcount_find_shared(ip->i_mount, agno, agbno,
				aglen, &fbno, &flen, true);
		if (error)
			break;
		if (flen == 0) {
			distance = fbno - agbno;
			goto advloop;
		}

		/* Add as much as we can to the cow fork */
		foffset = XFS_FSB_TO_B(ip->i_mount, lblk + fbno - agbno);
		fsize = XFS_FSB_TO_B(ip->i_mount, flen);
		error = xfs_iomap_cow_delay(ip, foffset, fsize, &rec);
		if (error)
			break;

		distance = (rec.br_startoff - lblk) + rec.br_blockcount;
advloop:
		if (aglen < distance)
			break;
		aglen -= distance;
		agbno += distance;
		lblk += distance;
	}

	if (error)
		trace_xfs_reflink_reserve_cow_extent_error(ip, error, _RET_IP_);
	return error;
}

/*
 * Create CoW reservations for all shared blocks within a byte range of
 * a file.
 */
int
xfs_reflink_reserve_cow_range(
	struct xfs_inode	*ip,
	xfs_off_t		pos,
	xfs_off_t		len)
{
	struct xfs_bmbt_irec	imap;
	int			nimaps;
	int			error = 0;
	xfs_fileoff_t		lblk;
	xfs_fileoff_t		next_lblk;
	struct xfs_ifork	*ifp;
	struct xfs_bmbt_rec_host	*gotp;
	xfs_extnum_t		idx;

	if (!xfs_is_reflink_inode(ip))
		return 0;

	trace_xfs_reflink_reserve_cow_range(ip, len, pos, 0);

	lblk = XFS_B_TO_FSBT(ip->i_mount, pos);
	next_lblk = XFS_B_TO_FSB(ip->i_mount, pos + len);
	ifp = XFS_IFORK_PTR(ip, XFS_COW_FORK);
	xfs_ilock(ip, XFS_ILOCK_EXCL);
	while (lblk < next_lblk) {
		/* Already reserved?  Skip the refcount btree access. */
		gotp = xfs_iext_bno_to_ext(ifp, lblk, &idx);
		if (gotp) {
			xfs_bmbt_get_all(gotp, &imap);
			if (imap.br_startoff <= lblk &&
			    imap.br_startoff + imap.br_blockcount > lblk) {
				lblk = imap.br_startoff + imap.br_blockcount;
				continue;
			}
		}

		/* Read extent from the source file. */
		nimaps = 1;
		error = xfs_bmapi_read(ip, lblk, next_lblk - lblk, &imap,
				&nimaps, 0);
		if (error)
			break;

		if (nimaps == 0)
			break;

		/* Fork all the shared blocks in this extent. */
		error = xfs_reflink_reserve_cow_extent(ip, &imap);
		if (error)
			break;

		lblk += imap.br_blockcount;
	}
	xfs_iunlock(ip, XFS_ILOCK_EXCL);

	if (error)
		trace_xfs_reflink_reserve_cow_range_error(ip, error, _RET_IP_);
	return error;
}

/*
 * Allocate blocks to all CoW reservations within a byte range of a file.
 */
int
xfs_reflink_allocate_cow_range(
	struct xfs_inode	*ip,
	xfs_off_t		pos,
	xfs_off_t		len)
{
	struct xfs_ifork	*ifp;
	struct xfs_bmbt_rec_host	*gotp;
	struct xfs_bmbt_irec	imap;
	int			error = 0;
	xfs_fileoff_t		start_lblk;
	xfs_fileoff_t		end_lblk;
	xfs_extnum_t		idx;

	if (!xfs_is_reflink_inode(ip))
		return 0;

	trace_xfs_reflink_allocate_cow_range(ip, len, pos, 0);

	start_lblk = XFS_B_TO_FSBT(ip->i_mount, pos);
	end_lblk = XFS_B_TO_FSB(ip->i_mount, pos + len);
	ifp = XFS_IFORK_PTR(ip, XFS_COW_FORK);
	xfs_ilock(ip, XFS_ILOCK_EXCL);

	gotp = xfs_iext_bno_to_ext(ifp, start_lblk, &idx);
	while (gotp) {
		xfs_bmbt_get_all(gotp, &imap);

		if (imap.br_startoff >= end_lblk)
			break;
		if (!isnullstartblock(imap.br_startblock))
			goto advloop;
		xfs_trim_extent(&imap, start_lblk, end_lblk - start_lblk);
		trace_xfs_reflink_allocate_cow_extent(ip, &imap);

		xfs_iunlock(ip, XFS_ILOCK_EXCL);
		error = xfs_iomap_write_allocate(ip, XFS_COW_FORK,
				XFS_FSB_TO_B(ip->i_mount, imap.br_startoff +
						imap.br_blockcount - 1), &imap);
		xfs_ilock(ip, XFS_ILOCK_EXCL);
		if (error)
			break;
advloop:
		/* Roll on... */
		idx++;
		if (idx >= ifp->if_bytes / sizeof(xfs_bmbt_rec_t))
			break;
		gotp = xfs_iext_get_ext(ifp, idx);
	}

	xfs_iunlock(ip, XFS_ILOCK_EXCL);

	if (error)
		trace_xfs_reflink_allocate_cow_range_error(ip, error, _RET_IP_);
	return error;
}

/*
 * Determine if there's a CoW reservation at a byte offset of an inode.
 */
bool
xfs_reflink_is_cow_pending(
	struct xfs_inode		*ip,
	xfs_off_t			offset)
{
	struct xfs_ifork		*ifp;
	struct xfs_bmbt_rec_host	*gotp;
	struct xfs_bmbt_irec		irec;
	xfs_fileoff_t			bno;
	xfs_extnum_t			idx;

	if (!xfs_is_reflink_inode(ip))
		return false;

	ifp = XFS_IFORK_PTR(ip, XFS_COW_FORK);
	bno = XFS_B_TO_FSBT(ip->i_mount, offset);
	gotp = xfs_iext_bno_to_ext(ifp, bno, &idx);

	if (!gotp)
		return false;

	xfs_bmbt_get_all(gotp, &irec);
	if (bno >= irec.br_startoff + irec.br_blockcount ||
	    bno < irec.br_startoff)
		return false;
	return true;
}

/*
 * Find the CoW reservation (and whether or not it needs block allocation)
 * for a given byte offset of a file.
 */
int
xfs_reflink_find_cow_mapping(
	struct xfs_inode		*ip,
	xfs_off_t			offset,
	struct xfs_bmbt_irec		*imap,
	bool				*need_alloc)
{
	struct xfs_bmbt_irec		irec;
	struct xfs_ifork		*ifp;
	struct xfs_bmbt_rec_host	*gotp;
	xfs_fileoff_t			bno;
	xfs_extnum_t			idx;

	/* Find the extent in the CoW fork. */
	ifp = XFS_IFORK_PTR(ip, XFS_COW_FORK);
	bno = XFS_B_TO_FSBT(ip->i_mount, offset);
	gotp = xfs_iext_bno_to_ext(ifp, bno, &idx);
	xfs_bmbt_get_all(gotp, &irec);

	trace_xfs_reflink_find_cow_mapping(ip, offset, 1, XFS_IO_OVERWRITE,
			&irec);

	/* If it's still delalloc, we must allocate later. */
	*imap = irec;
	*need_alloc = !!(isnullstartblock(irec.br_startblock));

	return 0;
}

/*
 * Trim an extent to end at the next CoW reservation past offset_fsb.
 */
int
xfs_reflink_trim_irec_to_next_cow(
	struct xfs_inode		*ip,
	xfs_fileoff_t			offset_fsb,
	struct xfs_bmbt_irec		*imap)
{
	struct xfs_bmbt_irec		irec;
	struct xfs_ifork		*ifp;
	struct xfs_bmbt_rec_host	*gotp;
	xfs_extnum_t			idx;

	if (!xfs_is_reflink_inode(ip))
		return 0;

	/* Find the extent in the CoW fork. */
	ifp = XFS_IFORK_PTR(ip, XFS_COW_FORK);
	gotp = xfs_iext_bno_to_ext(ifp, offset_fsb, &idx);
	if (!gotp)
		return 0;
	xfs_bmbt_get_all(gotp, &irec);

	/* This is the extent before; try sliding up one. */
	if (irec.br_startoff < offset_fsb) {
		idx++;
		if (idx >= ifp->if_bytes / sizeof(xfs_bmbt_rec_t))
			return 0;
		gotp = xfs_iext_get_ext(ifp, idx);
		xfs_bmbt_get_all(gotp, &irec);
	}

	if (irec.br_startoff >= imap->br_startoff + imap->br_blockcount)
		return 0;

	imap->br_blockcount = irec.br_startoff - imap->br_startoff;
	trace_xfs_reflink_trim_irec(ip, imap);

	return 0;
}

/*
 * Cancel all pending CoW reservations for some block range of an inode.
 */
int
xfs_reflink_cancel_cow_blocks(
	struct xfs_inode		*ip,
	struct xfs_trans		**tpp,
	xfs_fileoff_t			offset_fsb,
	xfs_fileoff_t			end_fsb)
{
	struct xfs_bmbt_irec		irec;
	struct xfs_ifork		*ifp;
	xfs_filblks_t			count_fsb;
	xfs_fsblock_t			firstfsb;
	struct xfs_defer_ops		dfops;
	int				error = 0;
	int				nimaps;

	if (!xfs_is_reflink_inode(ip))
		return 0;

	/* Go find the old extent in the CoW fork. */
	count_fsb = (xfs_filblks_t)(end_fsb - offset_fsb);
	ifp = XFS_IFORK_PTR(ip, XFS_COW_FORK);
	while (count_fsb) {
		nimaps = 1;
		error = xfs_bmapi_read(ip, offset_fsb, count_fsb, &irec,
				&nimaps, XFS_BMAPI_COWFORK);
		if (error)
			break;
		ASSERT(nimaps == 1);

		xfs_trim_extent(&irec, offset_fsb, count_fsb);
		trace_xfs_reflink_cancel_cow(ip, &irec);

		if (irec.br_startblock == DELAYSTARTBLOCK) {
			/* Free a delayed allocation. */
			xfs_mod_fdblocks(ip->i_mount, irec.br_blockcount,
					false);
			ip->i_delayed_blks -= irec.br_blockcount;

			/* Remove the mapping from the CoW fork. */
			error = xfs_bunmapi_cow(ip, &irec);
			if (error)
				break;
		} else if (irec.br_startblock == HOLESTARTBLOCK) {
			/* empty */
		} else {
			xfs_trans_ijoin(*tpp, ip, 0);
			xfs_defer_init(&dfops, &firstfsb);

			xfs_bmap_add_free(ip->i_mount, &dfops,
					irec.br_startblock, irec.br_blockcount,
					NULL);

			/* Update quota accounting */
			xfs_trans_mod_dquot_byino(*tpp, ip, XFS_TRANS_DQ_BCOUNT,
					-(long)irec.br_blockcount);

			/* Roll the transaction */
			error = xfs_defer_finish(tpp, &dfops, ip);
			if (error) {
				xfs_defer_cancel(&dfops);
				break;
			}

			/* Remove the mapping from the CoW fork. */
			error = xfs_bunmapi_cow(ip, &irec);
			if (error)
				break;
		}

		/* Roll on... */
		count_fsb -= irec.br_startoff + irec.br_blockcount - offset_fsb;
		offset_fsb = irec.br_startoff + irec.br_blockcount;
	}

	return error;
}

/*
 * Cancel all pending CoW reservations for some byte range of an inode.
 */
int
xfs_reflink_cancel_cow_range(
	struct xfs_inode	*ip,
	xfs_off_t		offset,
	xfs_off_t		count)
{
	struct xfs_trans	*tp;
	xfs_fileoff_t		offset_fsb;
	xfs_fileoff_t		end_fsb;
	int			error;

	trace_xfs_reflink_cancel_cow_range(ip, offset, count);

	offset_fsb = XFS_B_TO_FSBT(ip->i_mount, offset);
	if (count == NULLFILEOFF)
		end_fsb = NULLFILEOFF;
	else
		end_fsb = XFS_B_TO_FSB(ip->i_mount, offset + count);

	/* Start a rolling transaction to remove the mappings */
	error = xfs_trans_alloc(ip->i_mount, &M_RES(ip->i_mount)->tr_write,
			0, 0, 0, &tp);
	if (error)
		goto out;

	xfs_ilock(ip, XFS_ILOCK_EXCL);
	xfs_trans_ijoin(tp, ip, 0);

	/* Scrape out the old CoW reservations */
	error = xfs_reflink_cancel_cow_blocks(ip, &tp, offset_fsb, end_fsb);
	if (error)
		goto out_defer;

	error = xfs_trans_commit(tp);
	if (error)
		goto out;

	xfs_iunlock(ip, XFS_ILOCK_EXCL);
	return 0;

out_defer:
	xfs_trans_cancel(tp);
	xfs_iunlock(ip, XFS_ILOCK_EXCL);
out:
	trace_xfs_reflink_cancel_cow_range_error(ip, error, _RET_IP_);
	return error;
}

/*
 * Remap parts of a file's data fork after a successful CoW.
 */
int
xfs_reflink_end_cow(
	struct xfs_inode		*ip,
	xfs_off_t			offset,
	xfs_off_t			count)
{
	struct xfs_bmbt_irec		irec;
	struct xfs_bmbt_irec		uirec;
	struct xfs_trans		*tp;
	struct xfs_ifork		*ifp;
	xfs_fileoff_t			offset_fsb;
	xfs_fileoff_t			end_fsb;
	xfs_filblks_t			count_fsb;
	xfs_fsblock_t			firstfsb;
	struct xfs_defer_ops		dfops;
	int				done;
	int				error;
	unsigned int			resblks;
	xfs_filblks_t			ilen;
	xfs_filblks_t			rlen;
	int				nimaps;

	trace_xfs_reflink_end_cow(ip, offset, count);

	offset_fsb = XFS_B_TO_FSBT(ip->i_mount, offset);
	end_fsb = XFS_B_TO_FSB(ip->i_mount, offset + count);
	count_fsb = (xfs_filblks_t)(end_fsb - offset_fsb);

	/* Start a rolling transaction to switch the mappings */
	resblks = XFS_EXTENTADD_SPACE_RES(ip->i_mount, XFS_DATA_FORK);
	error = xfs_trans_alloc(ip->i_mount, &M_RES(ip->i_mount)->tr_write,
			resblks, 0, 0, &tp);
	if (error)
		goto out;

	xfs_ilock(ip, XFS_ILOCK_EXCL);
	xfs_trans_ijoin(tp, ip, 0);

	/* Go find the old extent in the CoW fork. */
	ifp = XFS_IFORK_PTR(ip, XFS_COW_FORK);
	while (count_fsb) {
		/* Read extent from the source file */
		nimaps = 1;
		error = xfs_bmapi_read(ip, offset_fsb, count_fsb, &irec,
				&nimaps, XFS_BMAPI_COWFORK);
		if (error)
			goto out_cancel;
		ASSERT(nimaps == 1);

		ASSERT(irec.br_startblock != DELAYSTARTBLOCK);
		xfs_trim_extent(&irec, offset_fsb, count_fsb);
		trace_xfs_reflink_cow_remap(ip, &irec);

		/*
		 * We can have a hole in the CoW fork if part of a directio
		 * write is CoW but part of it isn't.
		 */
		rlen = ilen = irec.br_blockcount;
		if (irec.br_startblock == HOLESTARTBLOCK)
			goto next_extent;

		/* Unmap the old blocks in the data fork. */
		done = false;
		while (rlen) {
			xfs_defer_init(&dfops, &firstfsb);
			error = __xfs_bunmapi(tp, ip, irec.br_startoff,
					&rlen, 0, 1, &firstfsb, &dfops);
			if (error)
				goto out_defer;

			/* Trim the extent to whatever got unmapped. */
			uirec = irec;
			xfs_trim_extent(&uirec, irec.br_startoff + rlen,
					irec.br_blockcount - rlen);
			irec.br_blockcount = rlen;
			trace_xfs_reflink_cow_remap_piece(ip, &uirec);

			/* Map the new blocks into the data fork. */
			error = xfs_bmap_map_extent(tp->t_mountp, &dfops,
					ip, XFS_DATA_FORK, &uirec);
			if (error)
				goto out_defer;

			/* Remove the mapping from the CoW fork. */
			error = xfs_bunmapi_cow(ip, &uirec);
			if (error)
				goto out_defer;

			error = xfs_defer_finish(&tp, &dfops, ip);
			if (error)
				goto out_defer;
		}

next_extent:
		/* Roll on... */
		count_fsb -= irec.br_startoff + ilen - offset_fsb;
		offset_fsb = irec.br_startoff + ilen;
	}

	error = xfs_trans_commit(tp);
	xfs_iunlock(ip, XFS_ILOCK_EXCL);
	if (error)
		goto out;
	return 0;

out_defer:
	xfs_defer_cancel(&dfops);
out_cancel:
	xfs_trans_cancel(tp);
	xfs_iunlock(ip, XFS_ILOCK_EXCL);
out:
	trace_xfs_reflink_end_cow_error(ip, error, _RET_IP_);
	return error;
}
