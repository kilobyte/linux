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
#include "xfs_sb.h"
#include "xfs_ag_resv.h"

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

			/* Free the CoW orphan record. */
			error = xfs_refcount_free_cow_extent(ip->i_mount,
					&dfops, irec.br_startblock,
					irec.br_blockcount);
			if (error)
				break;

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

			/* Free the CoW orphan record. */
			error = xfs_refcount_free_cow_extent(tp->t_mountp,
					&dfops, uirec.br_startblock,
					uirec.br_blockcount);
			if (error)
				goto out_defer;

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

struct xfs_reflink_recovery {
	struct list_head		rr_list;
	struct xfs_refcount_irec	rr_rrec;
};

/*
 * Find and remove leftover CoW reservations.
 */
STATIC int
xfs_reflink_recover_cow_ag(
	struct xfs_mount		*mp,
	xfs_agnumber_t			agno)
{
	struct list_head		debris;
	struct xfs_trans		*tp;
	struct xfs_btree_cur		*cur;
	struct xfs_buf			*agbp;
	struct xfs_refcount_irec	tmp;
	struct xfs_reflink_recovery	*rr, *n;
	struct xfs_defer_ops		dfops;
	xfs_fsblock_t			fsb;
	int				i, have;
	int				error;

	error = xfs_alloc_read_agf(mp, NULL, agno, 0, &agbp);
	if (error)
		return error;
	cur = xfs_refcountbt_init_cursor(mp, NULL, agbp, agno, NULL);

	/* Start iterating btree entries. */
	INIT_LIST_HEAD(&debris);
	error = xfs_refcountbt_lookup_ge(cur, 0, &have);
	if (error)
		goto out_error;
	while (have) {
		/* If refcount == 1, save the stashed entry for later. */
		error = xfs_refcountbt_get_rec(cur, &tmp, &i);
		if (error)
			goto out_error;
		XFS_WANT_CORRUPTED_GOTO(mp, i == 1, out_error);
		if (tmp.rc_refcount != 1)
			goto advloop;

		rr = kmem_alloc(sizeof(struct xfs_reflink_recovery), KM_SLEEP);
		rr->rr_rrec = tmp;
		list_add_tail(&rr->rr_list, &debris);

advloop:
		/* Look at the next one */
		error = xfs_btree_increment(cur, 0, &have);
		if (error)
			goto out_error;
	}

	xfs_btree_del_cursor(cur, XFS_BTREE_NOERROR);
	xfs_buf_relse(agbp);

	/* Now iterate the list to free the leftovers */
	list_for_each_entry(rr, &debris, rr_list) {
		/* Set up transaction. */
		error = xfs_trans_alloc(mp, &M_RES(mp)->tr_write, 0, 0, 0, &tp);
		if (error)
			goto out_free;

		trace_xfs_reflink_recover_extent(mp, agno, &rr->rr_rrec);

		/* Free the orphan record */
		xfs_defer_init(&dfops, &fsb);
		fsb = XFS_AGB_TO_FSB(mp, agno, rr->rr_rrec.rc_startblock);
		error = xfs_refcount_free_cow_extent(mp, &dfops, fsb,
				rr->rr_rrec.rc_blockcount);
		if (error)
			goto out_defer;

		/* Free the block. */
		xfs_bmap_add_free(mp, &dfops, fsb,
				rr->rr_rrec.rc_blockcount, NULL);

		error = xfs_defer_finish(&tp, &dfops, NULL);
		if (error)
			goto out_defer;

		error = xfs_trans_commit(tp);
		if (error)
			goto out_cancel;
	}
	goto out_free;

out_defer:
	xfs_defer_cancel(&dfops);
out_cancel:
	xfs_trans_cancel(tp);

out_free:
	/* Free the leftover list */
	list_for_each_entry_safe(rr, n, &debris, rr_list) {
		list_del(&rr->rr_list);
		kmem_free(rr);
	}

	return error;

out_error:
	xfs_btree_del_cursor(cur, XFS_BTREE_ERROR);
	xfs_buf_relse(agbp);
	return error;
}

/*
 * Free leftover CoW reservations that didn't get cleaned out.
 */
int
xfs_reflink_recover_cow(
	struct xfs_mount	*mp)
{
	xfs_agnumber_t		agno;
	int			error = 0;

	if (!xfs_sb_version_hasreflink(&mp->m_sb))
		return 0;

	for (agno = 0; agno < mp->m_sb.sb_agcount; agno++) {
		error = xfs_reflink_recover_cow_ag(mp, agno);
		if (error)
			break;
	}

	return error;
}

/*
 * Reflinking (Block) Ranges of Two Files Together
 *
 * First, ensure that the reflink flag is set on both inodes.  The flag is an
 * optimization to avoid unnecessary refcount btree lookups in the write path.
 *
 * Now we can iteratively remap the range of extents (and holes) in src to the
 * corresponding ranges in dest.  Let drange and srange denote the ranges of
 * logical blocks in dest and src touched by the reflink operation.
 *
 * While the length of drange is greater than zero,
 *    - Read src's bmbt at the start of srange ("imap")
 *    - If imap doesn't exist, make imap appear to start at the end of srange
 *      with zero length.
 *    - If imap starts before srange, advance imap to start at srange.
 *    - If imap goes beyond srange, truncate imap to end at the end of srange.
 *    - Punch (imap start - srange start + imap len) blocks from dest at
 *      offset (drange start).
 *    - If imap points to a real range of pblks,
 *         > Increase the refcount of the imap's pblks
 *         > Map imap's pblks into dest at the offset
 *           (drange start + imap start - srange start)
 *    - Advance drange and srange by (imap start - srange start + imap len)
 *
 * Finally, if the reflink made dest longer, update both the in-core and
 * on-disk file sizes.
 *
 * ASCII Art Demonstration:
 *
 * Let's say we want to reflink this source file:
 *
 * ----SSSSSSS-SSSSS----SSSSSS (src file)
 *   <-------------------->
 *
 * into this destination file:
 *
 * --DDDDDDDDDDDDDDDDDDD--DDD (dest file)
 *        <-------------------->
 * '-' means a hole, and 'S' and 'D' are written blocks in the src and dest.
 * Observe that the range has different logical offsets in either file.
 *
 * Consider that the first extent in the source file doesn't line up with our
 * reflink range.  Unmapping  and remapping are separate operations, so we can
 * unmap more blocks from the destination file than we remap.
 *
 * ----SSSSSSS-SSSSS----SSSSSS
 *   <------->
 * --DDDDD---------DDDDD--DDD
 *        <------->
 *
 * Now remap the source extent into the destination file:
 *
 * ----SSSSSSS-SSSSS----SSSSSS
 *   <------->
 * --DDDDD--SSSSSSSDDDDD--DDD
 *        <------->
 *
 * Do likewise with the second hole and extent in our range.  Holes in the
 * unmap range don't affect our operation.
 *
 * ----SSSSSSS-SSSSS----SSSSSS
 *            <---->
 * --DDDDD--SSSSSSS-SSSSS-DDD
 *                 <---->
 *
 * Finally, unmap and remap part of the third extent.  This will increase the
 * size of the destination file.
 *
 * ----SSSSSSS-SSSSS----SSSSSS
 *                  <----->
 * --DDDDD--SSSSSSS-SSSSS----SSS
 *                       <----->
 *
 * Once we update the destination file's i_size, we're done.
 */

/*
 * Ensure the reflink bit is set in both inodes.
 */
STATIC int
xfs_reflink_set_inode_flag(
	struct xfs_inode	*src,
	struct xfs_inode	*dest)
{
	struct xfs_mount	*mp = src->i_mount;
	int			error;
	struct xfs_trans	*tp;

	if (xfs_is_reflink_inode(src) && xfs_is_reflink_inode(dest))
		return 0;

	error = xfs_trans_alloc(mp, &M_RES(mp)->tr_ichange, 0, 0, 0, &tp);
	if (error)
		goto out_error;

	/* Lock both files against IO */
	if (src->i_ino == dest->i_ino)
		xfs_ilock(src, XFS_ILOCK_EXCL);
	else
		xfs_lock_two_inodes(src, dest, XFS_ILOCK_EXCL);

	if (!xfs_is_reflink_inode(src)) {
		trace_xfs_reflink_set_inode_flag(src);
		xfs_trans_ijoin(tp, src, XFS_ILOCK_EXCL);
		src->i_d.di_flags2 |= XFS_DIFLAG2_REFLINK;
		xfs_trans_log_inode(tp, src, XFS_ILOG_CORE);
		xfs_ifork_init_cow(src);
	} else
		xfs_iunlock(src, XFS_ILOCK_EXCL);

	if (src->i_ino == dest->i_ino)
		goto commit_flags;

	if (!xfs_is_reflink_inode(dest)) {
		trace_xfs_reflink_set_inode_flag(dest);
		xfs_trans_ijoin(tp, dest, XFS_ILOCK_EXCL);
		dest->i_d.di_flags2 |= XFS_DIFLAG2_REFLINK;
		xfs_trans_log_inode(tp, dest, XFS_ILOG_CORE);
		xfs_ifork_init_cow(dest);
	} else
		xfs_iunlock(dest, XFS_ILOCK_EXCL);

commit_flags:
	error = xfs_trans_commit(tp);
	if (error)
		goto out_error;
	return error;

out_error:
	trace_xfs_reflink_set_inode_flag_error(dest, error, _RET_IP_);
	return error;
}

/*
 * Update destination inode size & cowextsize hint, if necessary.
 */
STATIC int
xfs_reflink_update_dest(
	struct xfs_inode	*dest,
	xfs_off_t		newlen,
	xfs_extlen_t		cowextsize)
{
	struct xfs_mount	*mp = dest->i_mount;
	struct xfs_trans	*tp;
	int			error;

	if (newlen <= i_size_read(VFS_I(dest)) && cowextsize == 0)
		return 0;

	error = xfs_trans_alloc(mp, &M_RES(mp)->tr_ichange, 0, 0, 0, &tp);
	if (error)
		goto out_error;

	xfs_ilock(dest, XFS_ILOCK_EXCL);
	xfs_trans_ijoin(tp, dest, XFS_ILOCK_EXCL);

	if (newlen > i_size_read(VFS_I(dest))) {
		trace_xfs_reflink_update_inode_size(dest, newlen);
		i_size_write(VFS_I(dest), newlen);
		dest->i_d.di_size = newlen;
	}

	if (cowextsize) {
		dest->i_d.di_cowextsize = cowextsize;
		dest->i_d.di_flags2 |= XFS_DIFLAG2_COWEXTSIZE;
	}

	xfs_trans_log_inode(tp, dest, XFS_ILOG_CORE);

	error = xfs_trans_commit(tp);
	if (error)
		goto out_error;
	return error;

out_error:
	trace_xfs_reflink_update_inode_size_error(dest, error, _RET_IP_);
	return error;
}

/*
 * Do we have enough reserve in this AG to handle a reflink?  The refcount
 * btree already reserved all the space it needs, but the rmap btree can grow
 * infinitely, so we won't allow more reflinks when the AG is down to the
 * btree reserves.
 */
static int
xfs_reflink_ag_has_free_space(
	struct xfs_mount	*mp,
	xfs_agnumber_t		agno)
{
	struct xfs_perag	*pag;
	int			error = 0;

	if (!xfs_sb_version_hasrmapbt(&mp->m_sb))
		return 0;

	pag = xfs_perag_get(mp, agno);
	if (xfs_ag_resv_critical(pag, XFS_AG_RESV_AGFL))
		error = -ENOSPC;
	xfs_perag_put(pag);
	return error;
}

/*
 * Unmap a range of blocks from a file, then map other blocks into the hole.
 * The range to unmap is (destoff : destoff + srcioff + irec->br_blockcount).
 * The extent irec is mapped into dest at irec->br_startoff.
 */
STATIC int
xfs_reflink_remap_extent(
	struct xfs_inode	*ip,
	struct xfs_bmbt_irec	*irec,
	xfs_fileoff_t		destoff,
	xfs_off_t		new_isize)
{
	struct xfs_mount	*mp = ip->i_mount;
	struct xfs_trans	*tp;
	xfs_fsblock_t		firstfsb;
	unsigned int		resblks;
	struct xfs_defer_ops	dfops;
	struct xfs_bmbt_irec	uirec;
	bool			real_extent;
	xfs_filblks_t		rlen;
	xfs_filblks_t		unmap_len;
	xfs_off_t		newlen;
	int			error;

	unmap_len = irec->br_startoff + irec->br_blockcount - destoff;
	trace_xfs_reflink_punch_range(ip, destoff, unmap_len);

	/* Only remap normal extents. */
	real_extent =  (irec->br_startblock != HOLESTARTBLOCK &&
			irec->br_startblock != DELAYSTARTBLOCK &&
			!ISUNWRITTEN(irec));

	/* No reflinking if we're low on space */
	if (real_extent) {
		error = xfs_reflink_ag_has_free_space(mp,
				XFS_FSB_TO_AGNO(mp, irec->br_startblock));
		if (error)
			goto out;
	}

	/* Start a rolling transaction to switch the mappings */
	resblks = XFS_EXTENTADD_SPACE_RES(ip->i_mount, XFS_DATA_FORK);
	error = xfs_trans_alloc(mp, &M_RES(mp)->tr_write, resblks, 0, 0, &tp);
	if (error)
		goto out;

	xfs_ilock(ip, XFS_ILOCK_EXCL);
	xfs_trans_ijoin(tp, ip, 0);

	/* If we're not just clearing space, then do we have enough quota? */
	if (real_extent) {
		error = xfs_trans_reserve_quota_nblks(tp, ip,
				irec->br_blockcount, 0, XFS_QMOPT_RES_REGBLKS);
		if (error)
			goto out_cancel;
	}

	trace_xfs_reflink_remap(ip, irec->br_startoff,
				irec->br_blockcount, irec->br_startblock);

	/* Unmap the old blocks in the data fork. */
	rlen = unmap_len;
	while (rlen) {
		xfs_defer_init(&dfops, &firstfsb);
		error = __xfs_bunmapi(tp, ip, destoff, &rlen, 0, 1,
				&firstfsb, &dfops);
		if (error)
			goto out_defer;

		/* Trim the extent to whatever got unmapped. */
		uirec = *irec;
		xfs_trim_extent(&uirec, destoff + rlen, unmap_len - rlen);
		unmap_len = rlen;

		/* If this isn't a real mapping, we're done. */
		if (!real_extent || uirec.br_blockcount == 0)
			goto next_extent;

		trace_xfs_reflink_remap(ip, uirec.br_startoff,
				uirec.br_blockcount, uirec.br_startblock);

		/* Update the refcount tree */
		error = xfs_refcount_increase_extent(mp, &dfops, &uirec);
		if (error)
			goto out_defer;

		/* Map the new blocks into the data fork. */
		error = xfs_bmap_map_extent(mp, &dfops, ip, XFS_DATA_FORK,
				&uirec);
		if (error)
			goto out_defer;

		/* Update quota accounting. */
		xfs_trans_mod_dquot_byino(tp, ip, XFS_TRANS_DQ_BCOUNT,
				uirec.br_blockcount);

		/* Update dest isize if needed. */
		newlen = XFS_FSB_TO_B(mp,
				uirec.br_startoff + uirec.br_blockcount);
		newlen = min_t(xfs_off_t, newlen, new_isize);
		if (newlen > i_size_read(VFS_I(ip))) {
			trace_xfs_reflink_update_inode_size(ip, newlen);
			i_size_write(VFS_I(ip), newlen);
			ip->i_d.di_size = newlen;
			xfs_trans_log_inode(tp, ip, XFS_ILOG_CORE);
		}

next_extent:
		/* Process all the deferred stuff. */
		error = xfs_defer_finish(&tp, &dfops, ip);
		if (error)
			goto out_defer;
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
	trace_xfs_reflink_remap_extent_error(ip, error, _RET_IP_);
	return error;
}

/*
 * Iteratively remap one file's extents (and holes) to another's.
 */
STATIC int
xfs_reflink_remap_blocks(
	struct xfs_inode	*src,
	xfs_fileoff_t		srcoff,
	struct xfs_inode	*dest,
	xfs_fileoff_t		destoff,
	xfs_filblks_t		len,
	xfs_off_t		new_isize)
{
	struct xfs_bmbt_irec	imap;
	int			nimaps;
	int			error = 0;
	xfs_filblks_t		range_len;

	/* drange = (destoff, destoff + len); srange = (srcoff, srcoff + len) */
	while (len) {
		trace_xfs_reflink_remap_blocks_loop(src, srcoff, len,
				dest, destoff);
		/* Read extent from the source file */
		nimaps = 1;
		xfs_ilock(src, XFS_ILOCK_EXCL);
		error = xfs_bmapi_read(src, srcoff, len, &imap, &nimaps, 0);
		xfs_iunlock(src, XFS_ILOCK_EXCL);
		if (error)
			goto err;
		ASSERT(nimaps == 1);
		xfs_trim_extent(&imap, srcoff, len);

		trace_xfs_reflink_remap_imap(src, srcoff, len, XFS_IO_OVERWRITE,
				&imap);

		/* Translate imap into the destination file. */
		range_len = imap.br_startoff + imap.br_blockcount - srcoff;
		imap.br_startoff += destoff - srcoff;

		/* Clear dest from destoff to the end of imap and map it in. */
		error = xfs_reflink_remap_extent(dest, &imap, destoff,
				new_isize);
		if (error)
			goto err;

		if (fatal_signal_pending(current)) {
			error = -EINTR;
			goto err;
		}

		/* Advance drange/srange */
		srcoff += range_len;
		destoff += range_len;
		len -= range_len;
	}

	return 0;

err:
	trace_xfs_reflink_remap_blocks_error(dest, error, _RET_IP_);
	return error;
}

/*
 * Read a page's worth of file data into the page cache.  Return the page
 * locked.
 */
static struct page *
xfs_get_page(
	struct inode	*inode,
	xfs_off_t	offset)
{
	struct address_space	*mapping;
	struct page		*page;
	pgoff_t			n;

	n = offset >> PAGE_SHIFT;
	mapping = inode->i_mapping;
	page = read_mapping_page(mapping, n, NULL);
	if (IS_ERR(page))
		return page;
	if (!PageUptodate(page)) {
		put_page(page);
		return ERR_PTR(-EIO);
	}
	lock_page(page);
	return page;
}

/*
 * Compare extents of two files to see if they are the same.
 */
static int
xfs_compare_extents(
	struct inode	*src,
	xfs_off_t	srcoff,
	struct inode	*dest,
	xfs_off_t	destoff,
	xfs_off_t	len,
	bool		*is_same)
{
	xfs_off_t	src_poff;
	xfs_off_t	dest_poff;
	void		*src_addr;
	void		*dest_addr;
	struct page	*src_page;
	struct page	*dest_page;
	xfs_off_t	cmp_len;
	bool		same;
	int		error;

	error = -EINVAL;
	same = true;
	while (len) {
		src_poff = srcoff & (PAGE_SIZE - 1);
		dest_poff = destoff & (PAGE_SIZE - 1);
		cmp_len = min(PAGE_SIZE - src_poff,
			      PAGE_SIZE - dest_poff);
		cmp_len = min(cmp_len, len);
		ASSERT(cmp_len > 0);

		trace_xfs_reflink_compare_extents(XFS_I(src), srcoff, cmp_len,
				XFS_I(dest), destoff);

		src_page = xfs_get_page(src, srcoff);
		if (IS_ERR(src_page)) {
			error = PTR_ERR(src_page);
			goto out_error;
		}
		dest_page = xfs_get_page(dest, destoff);
		if (IS_ERR(dest_page)) {
			error = PTR_ERR(dest_page);
			unlock_page(src_page);
			put_page(src_page);
			goto out_error;
		}
		src_addr = kmap_atomic(src_page);
		dest_addr = kmap_atomic(dest_page);

		flush_dcache_page(src_page);
		flush_dcache_page(dest_page);

		if (memcmp(src_addr + src_poff, dest_addr + dest_poff, cmp_len))
			same = false;

		kunmap_atomic(dest_addr);
		kunmap_atomic(src_addr);
		unlock_page(dest_page);
		unlock_page(src_page);
		put_page(dest_page);
		put_page(src_page);

		if (!same)
			break;

		srcoff += cmp_len;
		destoff += cmp_len;
		len -= cmp_len;
	}

	*is_same = same;
	return 0;

out_error:
	trace_xfs_reflink_compare_extents_error(XFS_I(dest), error, _RET_IP_);
	return error;
}

/*
 * Link a range of blocks from one file to another.
 */
int
xfs_reflink_remap_range(
	struct xfs_inode	*src,
	xfs_off_t		srcoff,
	struct xfs_inode	*dest,
	xfs_off_t		destoff,
	xfs_off_t		len,
	unsigned int		flags)
{
	struct xfs_mount	*mp = src->i_mount;
	xfs_fileoff_t		sfsbno, dfsbno;
	xfs_filblks_t		fsblen;
	int			error;
	xfs_extlen_t		cowextsize;
	bool			is_same;

	if (!xfs_sb_version_hasreflink(&mp->m_sb))
		return -EOPNOTSUPP;

	if (XFS_FORCED_SHUTDOWN(mp))
		return -EIO;

	/* Don't reflink realtime inodes */
	if (XFS_IS_REALTIME_INODE(src) || XFS_IS_REALTIME_INODE(dest))
		return -EINVAL;

	if (flags & ~XFS_REFLINK_ALL)
		return -EINVAL;

	trace_xfs_reflink_remap_range(src, srcoff, len, dest, destoff);

	/* Lock both files against IO */
	if (src->i_ino == dest->i_ino) {
		xfs_ilock(src, XFS_IOLOCK_EXCL);
		xfs_ilock(src, XFS_MMAPLOCK_EXCL);
	} else {
		xfs_lock_two_inodes(src, dest, XFS_IOLOCK_EXCL);
		xfs_lock_two_inodes(src, dest, XFS_MMAPLOCK_EXCL);
	}

	/*
	 * Check that the extents are the same.
	 */
	if (flags & XFS_REFLINK_DEDUPE) {
		is_same = false;
		error = xfs_compare_extents(VFS_I(src), srcoff, VFS_I(dest),
				destoff, len, &is_same);
		if (error)
			goto out_error;
		if (!is_same) {
			error = -EBADE;
			goto out_error;
		}
	}

	error = xfs_reflink_set_inode_flag(src, dest);
	if (error)
		goto out_error;

	/*
	 * Invalidate the page cache so that we can clear any CoW mappings
	 * in the destination file.
	 */
	truncate_inode_pages_range(&VFS_I(dest)->i_data, destoff,
				   PAGE_ALIGN(destoff + len) - 1);

	dfsbno = XFS_B_TO_FSBT(mp, destoff);
	sfsbno = XFS_B_TO_FSBT(mp, srcoff);
	fsblen = XFS_B_TO_FSB(mp, len);
	error = xfs_reflink_remap_blocks(src, sfsbno, dest, dfsbno, fsblen,
			destoff + len);
	if (error)
		goto out_error;

	/*
	 * Carry the cowextsize hint from src to dest if we're sharing the
	 * entire source file to the entire destination file, the source file
	 * has a cowextsize hint, and the destination file does not.
	 */
	cowextsize = 0;
	if (srcoff == 0 && len == i_size_read(VFS_I(src)) &&
	    (src->i_d.di_flags2 & XFS_DIFLAG2_COWEXTSIZE) &&
	    destoff == 0 && len >= i_size_read(VFS_I(dest)) &&
	    !(dest->i_d.di_flags2 & XFS_DIFLAG2_COWEXTSIZE))
		cowextsize = src->i_d.di_cowextsize;

	error = xfs_reflink_update_dest(dest, destoff + len, cowextsize);
	if (error)
		goto out_error;

out_error:
	xfs_iunlock(src, XFS_MMAPLOCK_EXCL);
	xfs_iunlock(src, XFS_IOLOCK_EXCL);
	if (src->i_ino != dest->i_ino) {
		xfs_iunlock(dest, XFS_MMAPLOCK_EXCL);
		xfs_iunlock(dest, XFS_IOLOCK_EXCL);
	}
	if (error)
		trace_xfs_reflink_remap_range_error(dest, error, _RET_IP_);
	return error;
}

/*
 * Dirty all the shared blocks within a byte range of a file so that they're
 * rewritten elsewhere.  Similar to generic_perform_write().
 */
static int
xfs_reflink_dirty_range(
	struct inode		*inode,
	xfs_off_t		pos,
	xfs_off_t		len)
{
	struct address_space	*mapping;
	const struct address_space_operations *a_ops;
	int			error;
	unsigned int		flags;
	struct page		*page;
	struct page		*rpage;
	unsigned long		offset;	/* Offset into pagecache page */
	unsigned long		bytes;	/* Bytes to write to page */
	void			*fsdata;

	mapping = inode->i_mapping;
	a_ops = mapping->a_ops;
	flags = AOP_FLAG_UNINTERRUPTIBLE;
	do {

		offset = (pos & (PAGE_SIZE - 1));
		bytes = min_t(unsigned long, PAGE_SIZE - offset, len);
		rpage = xfs_get_page(inode, pos);
		if (IS_ERR(rpage)) {
			error = PTR_ERR(rpage);
			break;
		}

		unlock_page(rpage);
		error = a_ops->write_begin(NULL, mapping, pos, bytes, flags,
					   &page, &fsdata);
		put_page(rpage);
		if (error < 0)
			break;

		trace_xfs_reflink_unshare_page(inode, page, pos, bytes);

		if (!PageUptodate(page)) {
			xfs_err(XFS_I(inode)->i_mount,
					"%s: STALE? ino=%llu pos=%llu\n",
					__func__, XFS_I(inode)->i_ino, pos);
			WARN_ON(1);
		}
		if (mapping_writably_mapped(mapping))
			flush_dcache_page(page);

		error = a_ops->write_end(NULL, mapping, pos, bytes, bytes,
					 page, fsdata);
		if (error < 0)
			break;
		else if (error == 0) {
			error = -EIO;
			break;
		} else {
			bytes = error;
			error = 0;
		}

		cond_resched();

		pos += bytes;
		len -= bytes;

		balance_dirty_pages_ratelimited(mapping);
		if (fatal_signal_pending(current)) {
			error = -EINTR;
			break;
		}
	} while (len > 0);

	return error;
}

/*
 * The user wants to preemptively CoW all shared blocks in this file,
 * which enables us to turn off the reflink flag.  Iterate all
 * extents which are not prealloc/delalloc to see which ranges are
 * mentioned in the refcount tree, then read those blocks into the
 * pagecache, dirty them, fsync them back out, and then we can update
 * the inode flag.  What happens if we run out of memory? :)
 */
STATIC int
xfs_reflink_dirty_extents(
	struct xfs_inode	*ip,
	xfs_fileoff_t		fbno,
	xfs_filblks_t		end,
	xfs_off_t		isize)
{
	struct xfs_mount	*mp = ip->i_mount;
	xfs_agnumber_t		agno;
	xfs_agblock_t		agbno;
	xfs_extlen_t		aglen;
	xfs_agblock_t		rbno;
	xfs_extlen_t		rlen;
	xfs_off_t		fpos;
	xfs_off_t		flen;
	struct xfs_bmbt_irec	map[2];
	int			nmaps;
	int			error;

	while (end - fbno > 0) {
		nmaps = 1;
		/*
		 * Look for extents in the file.  Skip holes, delalloc, or
		 * unwritten extents; they can't be reflinked.
		 */
		error = xfs_bmapi_read(ip, fbno, end - fbno, map, &nmaps, 0);
		if (error)
			goto out;
		if (nmaps == 0)
			break;
		if (map[0].br_startblock == HOLESTARTBLOCK ||
		    map[0].br_startblock == DELAYSTARTBLOCK ||
		    ISUNWRITTEN(&map[0]))
			goto next;

		map[1] = map[0];
		while (map[1].br_blockcount) {
			agno = XFS_FSB_TO_AGNO(mp, map[1].br_startblock);
			agbno = XFS_FSB_TO_AGBNO(mp, map[1].br_startblock);
			aglen = map[1].br_blockcount;

			error = xfs_refcount_find_shared(mp, agno, agbno, aglen,
							 &rbno, &rlen, true);
			if (error)
				goto out;
			if (rlen == 0)
				goto skip_copy;

			/* Dirty the pages */
			xfs_iunlock(ip, XFS_ILOCK_EXCL);
			fpos = XFS_FSB_TO_B(mp, map[1].br_startoff +
					(rbno - agbno));
			flen = XFS_FSB_TO_B(mp, rlen);
			if (fpos + flen > isize)
				flen = isize - fpos;
			error = xfs_reflink_dirty_range(VFS_I(ip), fpos, flen);
			xfs_ilock(ip, XFS_ILOCK_EXCL);
			if (error)
				goto out;
skip_copy:
			map[1].br_blockcount -= (rbno - agbno + rlen);
			map[1].br_startoff += (rbno - agbno + rlen);
			map[1].br_startblock += (rbno - agbno + rlen);
		}

next:
		fbno = map[0].br_startoff + map[0].br_blockcount;
	}
out:
	return error;
}

/* Iterate the extents; if there are no reflinked blocks, clear the flag. */
STATIC int
xfs_reflink_try_clear_inode_flag(
	struct xfs_inode	*ip,
	xfs_off_t		old_isize)
{
	struct xfs_mount	*mp = ip->i_mount;
	struct xfs_trans	*tp;
	xfs_fileoff_t		fbno;
	xfs_filblks_t		end;
	xfs_agnumber_t		agno;
	xfs_agblock_t		agbno;
	xfs_extlen_t		aglen;
	xfs_agblock_t		rbno;
	xfs_extlen_t		rlen;
	struct xfs_bmbt_irec	map[2];
	int			nmaps;
	int			error = 0;

	/* Start a rolling transaction to remove the mappings */
	error = xfs_trans_alloc(mp, &M_RES(mp)->tr_write, 0, 0, 0, &tp);
	if (error)
		return error;

	xfs_ilock(ip, XFS_ILOCK_EXCL);
	xfs_trans_ijoin(tp, ip, 0);

	if (old_isize != i_size_read(VFS_I(ip)))
		goto cancel;
	if (!(ip->i_d.di_flags2 & XFS_DIFLAG2_REFLINK))
		goto cancel;

	fbno = 0;
	end = XFS_B_TO_FSB(mp, old_isize);
	while (end - fbno > 0) {
		nmaps = 1;
		/*
		 * Look for extents in the file.  Skip holes, delalloc, or
		 * unwritten extents; they can't be reflinked.
		 */
		error = xfs_bmapi_read(ip, fbno, end - fbno, map, &nmaps, 0);
		if (error)
			goto cancel;
		if (nmaps == 0)
			break;
		if (map[0].br_startblock == HOLESTARTBLOCK ||
		    map[0].br_startblock == DELAYSTARTBLOCK ||
		    ISUNWRITTEN(&map[0]))
			goto next;

		map[1] = map[0];
		while (map[1].br_blockcount) {
			agno = XFS_FSB_TO_AGNO(mp, map[1].br_startblock);
			agbno = XFS_FSB_TO_AGBNO(mp, map[1].br_startblock);
			aglen = map[1].br_blockcount;

			error = xfs_refcount_find_shared(mp, agno, agbno, aglen,
							 &rbno, &rlen, false);
			if (error)
				goto cancel;
			/* Is there still a shared block here? */
			if (rlen > 0) {
				error = 0;
				goto cancel;
			}

			map[1].br_blockcount -= aglen;
			map[1].br_startoff += aglen;
			map[1].br_startblock += aglen;
		}

next:
		fbno = map[0].br_startoff + map[0].br_blockcount;
	}

	/*
	 * We didn't find any shared blocks so turn off the reflink flag.
	 * First, get rid of any leftover CoW mappings.
	 */
	error = xfs_reflink_cancel_cow_blocks(ip, &tp, 0, NULLFILEOFF);
	if (error)
		goto cancel;

	/* Clear the inode flag. */
	trace_xfs_reflink_unset_inode_flag(ip);
	ip->i_d.di_flags2 &= ~XFS_DIFLAG2_REFLINK;
	xfs_trans_ijoin(tp, ip, 0);
	xfs_trans_log_inode(tp, ip, XFS_ILOG_CORE);

	error = xfs_trans_commit(tp);
	if (error)
		goto out;

	xfs_iunlock(ip, XFS_ILOCK_EXCL);
	return 0;
cancel:
	xfs_trans_cancel(tp);
out:
	xfs_iunlock(ip, XFS_ILOCK_EXCL);
	return error;
}

/*
 * Pre-COW all shared blocks within a given byte range of a file and turn off
 * the reflink flag if we unshare all of the file's blocks.
 */
int
xfs_reflink_unshare(
	struct xfs_inode	*ip,
	xfs_off_t		offset,
	xfs_off_t		len)
{
	struct xfs_mount	*mp = ip->i_mount;
	xfs_fileoff_t		fbno;
	xfs_filblks_t		end;
	xfs_off_t		old_isize, isize;
	int			error;

	if (!xfs_is_reflink_inode(ip))
		return 0;

	trace_xfs_reflink_unshare(ip, offset, len);

	inode_dio_wait(VFS_I(ip));

	/* Try to CoW the selected ranges */
	xfs_ilock(ip, XFS_ILOCK_EXCL);
	fbno = XFS_B_TO_FSB(mp, offset);
	old_isize = isize = i_size_read(VFS_I(ip));
	end = XFS_B_TO_FSB(mp, offset + len);
	error = xfs_reflink_dirty_extents(ip, fbno, end, isize);
	if (error)
		goto out_unlock;
	xfs_iunlock(ip, XFS_ILOCK_EXCL);

	/* Wait for the IO to finish */
	error = filemap_write_and_wait(VFS_I(ip)->i_mapping);
	if (error)
		goto out;

	/* Turn off the reflink flag if we unshared the whole file */
	if (offset == 0 && len == isize) {
		error = xfs_reflink_try_clear_inode_flag(ip, old_isize);
		if (error)
			goto out;
	}

	return 0;

out_unlock:
	xfs_iunlock(ip, XFS_ILOCK_EXCL);
out:
	trace_xfs_reflink_unshare_error(ip, error, _RET_IP_);
	return error;
}

/*
 * If we're trying to truncate a file whose last block is shared and the new
 * size isn't aligned to a block boundary, we need to dirty that last block
 * ahead of the VFS zeroing the page.
 */
int
xfs_reflink_cow_eof_block(
	struct xfs_inode	*ip,
	xfs_off_t		newsize)
{
	struct xfs_mount	*mp = ip->i_mount;
	xfs_fileoff_t		fbno;
	xfs_off_t		isize;
	int			error;

	if (!xfs_is_reflink_inode(ip) ||
	    (newsize & ((1 << VFS_I(ip)->i_blkbits) - 1)) == 0)
		return 0;

	/* Try to CoW the shared last block */
	xfs_ilock(ip, XFS_ILOCK_EXCL);
	fbno = XFS_B_TO_FSBT(mp, newsize);
	isize = i_size_read(VFS_I(ip));

	if (newsize > isize)
		trace_xfs_reflink_cow_eof_block(ip, isize, newsize - isize);
	else
		trace_xfs_reflink_cow_eof_block(ip, newsize, isize - newsize);

	error = xfs_reflink_dirty_extents(ip, fbno, fbno + 1, isize);
	if (error)
		goto out_unlock;
	xfs_iunlock(ip, XFS_ILOCK_EXCL);

	return 0;

out_unlock:
	xfs_iunlock(ip, XFS_ILOCK_EXCL);
	trace_xfs_reflink_cow_eof_block_error(ip, error, _RET_IP_);
	return error;
}

/*
 * Ensure that the only change we allow to the inode reflink flag is to clear
 * it when the fs supports reflink and the size is zero.
 */
int
xfs_reflink_check_flag_adjust(
	struct xfs_inode	*ip,
	unsigned int		*xflags)
{
	unsigned int		chg;

	chg = !!(*xflags & FS_XFLAG_REFLINK) ^ !!xfs_is_reflink_inode(ip);

	if (!chg)
		return 0;
	if (!xfs_sb_version_hasreflink(&ip->i_mount->m_sb))
		return -EOPNOTSUPP;
	if (i_size_read(VFS_I(ip)) != 0)
		return -EINVAL;
	if (*xflags & FS_XFLAG_REFLINK) {
		*xflags &= ~FS_XFLAG_REFLINK;
		return 0;
	}
	return 0;
}
