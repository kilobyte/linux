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
#include "xfs_trans.h"
#include "xfs_trans_priv.h"
#include "xfs_bmap_item.h"
#include "xfs_alloc.h"
#include "xfs_bmap.h"
#include "xfs_inode.h"

/*
 * This routine is called to allocate an "bmap update intent"
 * log item that will hold nextents worth of extents.  The
 * caller must use all nextents extents, because we are not
 * flexible about this at all.
 */
struct xfs_bui_log_item *
xfs_trans_get_bui(
	struct xfs_trans		*tp,
	uint				nextents)
{
	struct xfs_bui_log_item		*buip;

	ASSERT(tp != NULL);
	ASSERT(nextents > 0);

	buip = xfs_bui_init(tp->t_mountp, nextents);
	ASSERT(buip != NULL);

	/*
	 * Get a log_item_desc to point at the new item.
	 */
	xfs_trans_add_item(tp, &buip->bui_item);
	return buip;
}

/*
 * This routine is called to indicate that the described
 * extent is to be logged as needing to be freed.  It should
 * be called once for each extent to be freed.
 */
void
xfs_trans_log_start_bmap_update(
	struct xfs_trans		*tp,
	struct xfs_bui_log_item		*buip,
	enum xfs_bmap_intent_type	type,
	__uint64_t			owner,
	int				whichfork,
	xfs_fileoff_t			startoff,
	xfs_fsblock_t			startblock,
	xfs_filblks_t			blockcount,
	xfs_exntst_t			state)
{
	uint				next_extent;
	struct xfs_map_extent		*bmap;

	tp->t_flags |= XFS_TRANS_DIRTY;
	buip->bui_item.li_desc->lid_flags |= XFS_LID_DIRTY;

	/*
	 * atomic_inc_return gives us the value after the increment;
	 * we want to use it as an array index so we need to subtract 1 from
	 * it.
	 */
	next_extent = atomic_inc_return(&buip->bui_next_extent) - 1;
	ASSERT(next_extent < buip->bui_format.bui_nextents);
	bmap = &(buip->bui_format.bui_extents[next_extent]);
	bmap->me_owner = owner;
	bmap->me_startblock = startblock;
	bmap->me_startoff = startoff;
	bmap->me_len = blockcount;
	bmap->me_flags = 0;
	if (state == XFS_EXT_UNWRITTEN)
		bmap->me_flags |= XFS_BMAP_EXTENT_UNWRITTEN;
	if (whichfork == XFS_ATTR_FORK)
		bmap->me_flags |= XFS_BMAP_EXTENT_ATTR_FORK;
	switch (type) {
	case XFS_BMAP_MAP:
		bmap->me_flags |= XFS_BMAP_EXTENT_MAP;
		break;
	case XFS_BMAP_UNMAP:
		bmap->me_flags |= XFS_BMAP_EXTENT_UNMAP;
		break;
	default:
		ASSERT(0);
	}
}


/*
 * This routine is called to allocate an "extent free done"
 * log item that will hold nextents worth of extents.  The
 * caller must use all nextents extents, because we are not
 * flexible about this at all.
 */
struct xfs_bud_log_item *
xfs_trans_get_bud(
	struct xfs_trans		*tp,
	struct xfs_bui_log_item		*buip,
	uint				nextents)
{
	struct xfs_bud_log_item		*budp;

	ASSERT(tp != NULL);
	ASSERT(nextents > 0);

	budp = xfs_bud_init(tp->t_mountp, buip, nextents);
	ASSERT(budp != NULL);

	/*
	 * Get a log_item_desc to point at the new item.
	 */
	xfs_trans_add_item(tp, &budp->bud_item);
	return budp;
}

/*
 * Finish an bmap update and log it to the BUD. Note that the transaction is
 * marked dirty regardless of whether the bmap update succeeds or fails to
 * support the BUI/BUD lifecycle rules.
 */
int
xfs_trans_log_finish_bmap_update(
	struct xfs_trans		*tp,
	struct xfs_bud_log_item		*budp,
	struct xfs_defer_ops		*dop,
	enum xfs_bmap_intent_type	type,
	struct xfs_inode		*ip,
	int				whichfork,
	xfs_fileoff_t			startoff,
	xfs_fsblock_t			startblock,
	xfs_filblks_t			blockcount,
	xfs_exntst_t			state,
	struct xfs_btree_cur		**pcur)
{
	uint				next_extent;
	struct xfs_map_extent		*bmap;
	int				error;

	error = -EFSCORRUPTED;

	/*
	 * Mark the transaction dirty, even on error. This ensures the
	 * transaction is aborted, which:
	 *
	 * 1.) releases the BUI and frees the BUD
	 * 2.) shuts down the filesystem
	 */
	tp->t_flags |= XFS_TRANS_DIRTY;
	budp->bud_item.li_desc->lid_flags |= XFS_LID_DIRTY;

	next_extent = budp->bud_next_extent;
	ASSERT(next_extent < budp->bud_format.bud_nextents);
	bmap = &(budp->bud_format.bud_extents[next_extent]);
	bmap->me_owner = ip->i_ino;
	bmap->me_startblock = startblock;
	bmap->me_startoff = startoff;
	bmap->me_len = blockcount;
	bmap->me_flags = 0;
	if (state == XFS_EXT_UNWRITTEN)
		bmap->me_flags |= XFS_BMAP_EXTENT_UNWRITTEN;
	if (whichfork == XFS_ATTR_FORK)
		bmap->me_flags |= XFS_BMAP_EXTENT_ATTR_FORK;
	switch (type) {
	case XFS_BMAP_MAP:
		bmap->me_flags |= XFS_BMAP_EXTENT_MAP;
		break;
	case XFS_BMAP_UNMAP:
		bmap->me_flags |= XFS_BMAP_EXTENT_UNMAP;
		break;
	default:
		ASSERT(0);
	}
	budp->bud_next_extent++;

	return error;
}
