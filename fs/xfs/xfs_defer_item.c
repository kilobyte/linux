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
#include "xfs_bit.h"
#include "xfs_sb.h"
#include "xfs_mount.h"
#include "xfs_defer.h"
#include "xfs_trans.h"
#include "xfs_trace.h"
#include "xfs_bmap.h"
#include "xfs_extfree_item.h"

/* Extent Freeing */

/* Sort bmap items by AG. */
static int
xfs_bmap_free_diff_items(
	void				*priv,
	struct list_head		*a,
	struct list_head		*b)
{
	struct xfs_mount		*mp = priv;
	struct xfs_bmap_free_item	*ra;
	struct xfs_bmap_free_item	*rb;

	ra = container_of(a, struct xfs_bmap_free_item, xbfi_list);
	rb = container_of(b, struct xfs_bmap_free_item, xbfi_list);
	return  XFS_FSB_TO_AGNO(mp, ra->xbfi_startblock) -
		XFS_FSB_TO_AGNO(mp, rb->xbfi_startblock);
}

/* Get an EFI. */
STATIC void *
xfs_bmap_free_create_intent(
	struct xfs_trans		*tp,
	unsigned int			count)
{
	return xfs_trans_get_efi(tp, count);
}

/* Log a free extent to the intent item. */
STATIC void
xfs_bmap_free_log_item(
	struct xfs_trans		*tp,
	void				*intent,
	struct list_head		*item)
{
	struct xfs_bmap_free_item	*free;

	free = container_of(item, struct xfs_bmap_free_item, xbfi_list);
	xfs_trans_log_efi_extent(tp, intent, free->xbfi_startblock,
			free->xbfi_blockcount);
}

/* Get an EFD so we can process all the free extents. */
STATIC void *
xfs_bmap_free_create_done(
	struct xfs_trans		*tp,
	void				*intent,
	unsigned int			count)
{
	return xfs_trans_get_efd(tp, intent, count);
}

/* Process a free extent. */
STATIC int
xfs_bmap_free_finish_item(
	struct xfs_trans		*tp,
	struct xfs_defer_ops		*dop,
	struct list_head		*item,
	void				*done_item,
	void				**state)
{
	struct xfs_bmap_free_item	*free;
	int				error;

	free = container_of(item, struct xfs_bmap_free_item, xbfi_list);
	error = xfs_trans_free_extent(tp, done_item,
			free->xbfi_startblock,
			free->xbfi_blockcount);
	kmem_free(free);
	return error;
}

/* Abort all pending EFIs. */
STATIC void
xfs_bmap_free_abort_intent(
	void				*intent)
{
	xfs_efi_release(intent);
}

/* Cancel a free extent. */
STATIC void
xfs_bmap_free_cancel_item(
	struct list_head		*item)
{
	struct xfs_bmap_free_item	*free;

	free = container_of(item, struct xfs_bmap_free_item, xbfi_list);
	kmem_free(free);
}

const struct xfs_defer_op_type xfs_extent_free_defer_type = {
	.type		= XFS_DEFER_OPS_TYPE_FREE,
	.max_items	= XFS_EFI_MAX_FAST_EXTENTS,
	.diff_items	= xfs_bmap_free_diff_items,
	.create_intent	= xfs_bmap_free_create_intent,
	.abort_intent	= xfs_bmap_free_abort_intent,
	.log_item	= xfs_bmap_free_log_item,
	.create_done	= xfs_bmap_free_create_done,
	.finish_item	= xfs_bmap_free_finish_item,
	.cancel_item	= xfs_bmap_free_cancel_item,
};

/* Deferred Item Initialization */

/* Initialize the deferred operation types. */
void
xfs_defer_init_types(void)
{
	xfs_defer_init_op_type(&xfs_extent_free_defer_type);
}
