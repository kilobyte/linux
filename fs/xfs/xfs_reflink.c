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
