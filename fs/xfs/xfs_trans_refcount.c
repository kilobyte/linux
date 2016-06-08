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
#include "xfs_refcount_item.h"
#include "xfs_alloc.h"
#include "xfs_refcount.h"

/*
 * This routine is called to allocate an "refcount update intent"
 * log item that will hold nextents worth of extents.  The
 * caller must use all nextents extents, because we are not
 * flexible about this at all.
 */
struct xfs_cui_log_item *
xfs_trans_get_cui(
	struct xfs_trans		*tp,
	uint				nextents)
{
	struct xfs_cui_log_item		*cuip;

	ASSERT(tp != NULL);
	ASSERT(nextents > 0);

	cuip = xfs_cui_init(tp->t_mountp, nextents);
	ASSERT(cuip != NULL);

	/*
	 * Get a log_item_desc to point at the new item.
	 */
	xfs_trans_add_item(tp, &cuip->cui_item);
	return cuip;
}

/*
 * This routine is called to indicate that the described
 * extent is to be logged as needing to be freed.  It should
 * be called once for each extent to be freed.
 */
void
xfs_trans_log_start_refcount_update(
	struct xfs_trans		*tp,
	struct xfs_cui_log_item		*cuip,
	enum xfs_refcount_intent_type	type,
	xfs_fsblock_t			startblock,
	xfs_filblks_t			blockcount)
{
	uint				next_extent;
	struct xfs_phys_extent		*refc;

	tp->t_flags |= XFS_TRANS_DIRTY;
	cuip->cui_item.li_desc->lid_flags |= XFS_LID_DIRTY;

	/*
	 * atomic_inc_return gives us the value after the increment;
	 * we want to use it as an array index so we need to subtract 1 from
	 * it.
	 */
	next_extent = atomic_inc_return(&cuip->cui_next_extent) - 1;
	ASSERT(next_extent < cuip->cui_format.cui_nextents);
	refc = &(cuip->cui_format.cui_extents[next_extent]);
	refc->pe_startblock = startblock;
	refc->pe_len = blockcount;
	refc->pe_flags = 0;
	switch (type) {
	case XFS_REFCOUNT_INCREASE:
		refc->pe_flags |= XFS_REFCOUNT_EXTENT_INCREASE;
		break;
	case XFS_REFCOUNT_DECREASE:
		refc->pe_flags |= XFS_REFCOUNT_EXTENT_DECREASE;
		break;
	case XFS_REFCOUNT_ALLOC_COW:
		refc->pe_flags |= XFS_REFCOUNT_EXTENT_ALLOC_COW;
		break;
	case XFS_REFCOUNT_FREE_COW:
		refc->pe_flags |= XFS_REFCOUNT_EXTENT_FREE_COW;
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
struct xfs_cud_log_item *
xfs_trans_get_cud(
	struct xfs_trans		*tp,
	struct xfs_cui_log_item		*cuip,
	uint				nextents)
{
	struct xfs_cud_log_item		*cudp;

	ASSERT(tp != NULL);
	ASSERT(nextents > 0);

	cudp = xfs_cud_init(tp->t_mountp, cuip, nextents);
	ASSERT(cudp != NULL);

	/*
	 * Get a log_item_desc to point at the new item.
	 */
	xfs_trans_add_item(tp, &cudp->cud_item);
	return cudp;
}

/*
 * Finish an refcount update and log it to the CUD. Note that the transaction is
 * marked dirty regardless of whether the refcount update succeeds or fails to
 * support the CUI/CUD lifecycle rules.
 */
int
xfs_trans_log_finish_refcount_update(
	struct xfs_trans		*tp,
	struct xfs_cud_log_item		*cudp,
	struct xfs_defer_ops		*dop,
	enum xfs_refcount_intent_type	type,
	xfs_fsblock_t			startblock,
	xfs_extlen_t			blockcount,
	xfs_extlen_t			*adjusted,
	struct xfs_btree_cur		**pcur)
{
	uint				next_extent;
	struct xfs_phys_extent		*refc;
	int				error;

	error = xfs_refcount_finish_one(tp, dop, type, startblock,
			blockcount, adjusted, pcur);

	/*
	 * Mark the transaction dirty, even on error. This ensures the
	 * transaction is aborted, which:
	 *
	 * 1.) releases the CUI and frees the CUD
	 * 2.) shuts down the filesystem
	 */
	tp->t_flags |= XFS_TRANS_DIRTY;
	cudp->cud_item.li_desc->lid_flags |= XFS_LID_DIRTY;

	next_extent = cudp->cud_next_extent;
	ASSERT(next_extent < cudp->cud_format.cud_nextents);
	refc = &(cudp->cud_format.cud_extents[next_extent]);
	refc->pe_startblock = startblock;
	refc->pe_len = blockcount;
	refc->pe_flags = 0;
	switch (type) {
	case XFS_REFCOUNT_INCREASE:
		refc->pe_flags |= XFS_REFCOUNT_EXTENT_INCREASE;
		break;
	case XFS_REFCOUNT_DECREASE:
		refc->pe_flags |= XFS_REFCOUNT_EXTENT_DECREASE;
		break;
	case XFS_REFCOUNT_ALLOC_COW:
		refc->pe_flags |= XFS_REFCOUNT_EXTENT_ALLOC_COW;
		break;
	case XFS_REFCOUNT_FREE_COW:
		refc->pe_flags |= XFS_REFCOUNT_EXTENT_FREE_COW;
		break;
	default:
		ASSERT(0);
	}
	cudp->cud_next_extent++;
	if (!error && *adjusted != blockcount) {
		refc->pe_len = *adjusted;
		cudp->cud_format.cud_nextents = cudp->cud_next_extent;
	}

	return error;
}
