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
#include "xfs_trans_resv.h"
#include "xfs_mount.h"
#include "xfs_defer.h"
#include "xfs_btree.h"
#include "xfs_bit.h"
#include "xfs_alloc.h"
#include "xfs_bmap.h"
#include "xfs_ialloc.h"
#include "xfs_refcount.h"
#include "xfs_alloc_btree.h"
#include "xfs_rmap_btree.h"
#include "xfs_log_format.h"
#include "xfs_trans.h"
#include "xfs_scrub.h"

static const char * const btree_types[] = {
	[XFS_BTNUM_BNO]		= "bnobt",
	[XFS_BTNUM_CNT]		= "cntbt",
	[XFS_BTNUM_RMAP]	= "rmapbt",
	[XFS_BTNUM_BMAP]	= "bmapbt",
	[XFS_BTNUM_INO]		= "inobt",
	[XFS_BTNUM_FINO]	= "finobt",
	[XFS_BTNUM_REFC]	= "refcountbt",
};

/* Report a scrub corruption in dmesg. */
void
xfs_btree_scrub_error(
	struct xfs_btree_cur		*cur,
	int				level,
	const char			*file,
	int				line,
	const char			*check)
{
	char				buf[16];
	xfs_fsblock_t			fsbno;

	if (cur->bc_ptrs[level] >= 1)
		snprintf(buf, 16, " ptr %d", cur->bc_ptrs[level]);
	else
		buf[0] = 0;

	fsbno = XFS_DADDR_TO_FSB(cur->bc_mp, cur->bc_bufs[level]->b_bn);
	xfs_alert(cur->bc_mp, "scrub: %s btree corruption in block %u/%u%s: %s, file: %s, line: %d",
			btree_types[cur->bc_btnum],
			XFS_FSB_TO_AGNO(cur->bc_mp, fsbno),
			XFS_FSB_TO_AGBNO(cur->bc_mp, fsbno),
			buf, check, file, line);
}

/* AG metadata scrubbing */

/*
 * Make sure this record is in order and doesn't stray outside of the parent
 * keys.
 */
static int
xfs_btree_scrub_rec(
	struct xfs_btree_scrub	*bs)
{
	struct xfs_btree_cur	*cur = bs->cur;
	union xfs_btree_rec	*rec;
	union xfs_btree_key	key;
	union xfs_btree_key	*keyp;
	struct xfs_btree_block	*block;
	struct xfs_btree_block	*keyblock;

	block = XFS_BUF_TO_BLOCK(cur->bc_bufs[0]);
	rec = xfs_btree_rec_addr(cur, cur->bc_ptrs[0], block);

	/* If this isn't the first record, are they in order? */
	XFS_BTREC_SCRUB_CHECK(bs, bs->firstrec ||
			cur->bc_ops->recs_inorder(cur, &bs->lastrec, rec));
	bs->firstrec = false;
	bs->lastrec = *rec;

	if (cur->bc_nlevels == 1)
		return 0;

	/* Is this at least as large as the parent low key? */
	cur->bc_ops->init_key_from_rec(&key, rec);
	keyblock = XFS_BUF_TO_BLOCK(cur->bc_bufs[1]);
	keyp = xfs_btree_key_addr(cur, cur->bc_ptrs[1], keyblock);

	XFS_BTKEY_SCRUB_CHECK(bs, 0,
			cur->bc_ops->diff_two_keys(cur, keyp, &key) >= 0);

	if (!(cur->bc_ops->flags & XFS_BTREE_OPS_OVERLAPPING))
		return 0;

	/* Is this no larger than the parent high key? */
	keyp = xfs_btree_high_key_addr(cur, cur->bc_ptrs[1], keyblock);

	XFS_BTKEY_SCRUB_CHECK(bs, 0,
			cur->bc_ops->diff_two_keys(cur, &key, keyp) >= 0);

	return 0;
}

/*
 * Make sure this key is in order and doesn't stray outside of the parent
 * keys.
 */
static int
xfs_btree_scrub_key(
	struct xfs_btree_scrub	*bs,
	int			level)
{
	struct xfs_btree_cur	*cur = bs->cur;
	union xfs_btree_key	*key;
	union xfs_btree_key	*keyp;
	struct xfs_btree_block	*block;
	struct xfs_btree_block	*keyblock;

	block = XFS_BUF_TO_BLOCK(cur->bc_bufs[level]);
	key = xfs_btree_key_addr(cur, cur->bc_ptrs[level], block);

	/* If this isn't the first key, are they in order? */
	XFS_BTKEY_SCRUB_CHECK(bs, level, bs->firstkey[level] ||
			cur->bc_ops->keys_inorder(cur, &bs->lastkey[level],
					key));
	bs->firstkey[level] = false;
	bs->lastkey[level] = *key;

	if (level + 1 >= cur->bc_nlevels)
		return 0;

	/* Is this at least as large as the parent low key? */
	keyblock = XFS_BUF_TO_BLOCK(cur->bc_bufs[level + 1]);
	keyp = xfs_btree_key_addr(cur, cur->bc_ptrs[level + 1], keyblock);

	XFS_BTKEY_SCRUB_CHECK(bs, level,
			cur->bc_ops->diff_two_keys(cur, keyp, key) >= 0);

	if (!(cur->bc_ops->flags & XFS_BTREE_OPS_OVERLAPPING))
		return 0;

	/* Is this no larger than the parent high key? */
	key = xfs_btree_high_key_addr(cur, cur->bc_ptrs[level], block);
	keyp = xfs_btree_high_key_addr(cur, cur->bc_ptrs[level + 1], keyblock);

	XFS_BTKEY_SCRUB_CHECK(bs, level,
			cur->bc_ops->diff_two_keys(cur, key, keyp) >= 0);

	return 0;
}

struct check_owner {
	struct list_head	list;
	xfs_agblock_t		bno;
};

/*
 * Make sure this btree block isn't in the free list and that there's
 * an rmap record for it.
 */
static int
xfs_btree_block_check_owner(
	struct xfs_btree_scrub		*bs,
	xfs_agblock_t			bno)
{
	bool				has_rmap;
	bool				is_freesp;
	int				error;

	/* Check that this block isn't free */
	error = xfs_alloc_record_exists(bs->bno_cur, bno, 1, &is_freesp);
	if (error)
		goto err;
	XFS_BTREC_SCRUB_CHECK(bs, !is_freesp);

	if (!bs->rmap_cur)
		return 0;

	/* Check that there's an rmap record for this */
	error = xfs_rmap_record_exists(bs->rmap_cur, bno, 1, &bs->oinfo,
			&has_rmap);
	if (error)
		goto err;
	XFS_BTREC_SCRUB_CHECK(bs, has_rmap);
err:
	return error;
}

/* Check the owner of a btree block. */
static int
xfs_btree_scrub_check_owner(
	struct xfs_btree_scrub		*bs,
	struct xfs_buf			*bp)
{
	struct xfs_btree_cur		*cur = bs->cur;
	xfs_agblock_t			bno;
	xfs_fsblock_t			fsbno;
	struct check_owner		*co;

	fsbno = XFS_DADDR_TO_FSB(cur->bc_mp, bp->b_bn);
	bno = XFS_FSB_TO_AGBNO(cur->bc_mp, fsbno);

	/* Do we need to defer this one? */
	if ((!bs->rmap_cur && xfs_sb_version_hasrmapbt(&cur->bc_mp->m_sb)) ||
	    !bs->bno_cur) {
		co = kmem_alloc(sizeof(struct check_owner), KM_SLEEP | KM_NOFS);
		co->bno = bno;
		list_add_tail(&co->list, &bs->to_check);
		return 0;
	}

	return xfs_btree_block_check_owner(bs, bno);
}

/*
 * Visit all nodes and leaves of a btree.  Check that all pointers and
 * records are in order, that the keys reflect the records, and use a callback
 * so that the caller can verify individual records.  The callback is the same
 * as the one for xfs_btree_query_range, so therefore this function also
 * returns XFS_BTREE_QUERY_RANGE_ABORT, zero, or a negative error code.
 */
int
xfs_btree_scrub(
	struct xfs_btree_scrub		*bs)
{
	struct xfs_btree_cur		*cur = bs->cur;
	union xfs_btree_ptr		ptr;
	union xfs_btree_ptr		*pp;
	union xfs_btree_rec		*recp;
	struct xfs_btree_block		*block;
	int				level;
	struct xfs_buf			*bp;
	int				i;
	struct check_owner		*co, *n;
	int				error;

	/* Finish filling out the scrub state */
	bs->error = 0;
	bs->firstrec = true;
	for (i = 0; i < XFS_BTREE_MAXLEVELS; i++)
		bs->firstkey[i] = true;
	bs->bno_cur = bs->rmap_cur = NULL;
	INIT_LIST_HEAD(&bs->to_check);
	if (bs->cur->bc_btnum != XFS_BTNUM_BNO)
		bs->bno_cur = xfs_allocbt_init_cursor(cur->bc_mp, NULL,
				bs->agf_bp, bs->cur->bc_private.a.agno,
				XFS_BTNUM_BNO);
	if (bs->cur->bc_btnum != XFS_BTNUM_RMAP &&
	    xfs_sb_version_hasrmapbt(&cur->bc_mp->m_sb))
		bs->rmap_cur = xfs_rmapbt_init_cursor(cur->bc_mp, NULL,
				bs->agf_bp, bs->cur->bc_private.a.agno);

	/* Load the root of the btree. */
	level = cur->bc_nlevels - 1;
	cur->bc_ops->init_ptr_from_cur(cur, &ptr);
	error = xfs_btree_lookup_get_block(cur, level, &ptr, &block);
	if (error)
		goto out;

	xfs_btree_get_block(cur, level, &bp);
	error = xfs_btree_check_block(cur, block, level, bp);
	if (error)
		goto out;
	error = xfs_btree_scrub_check_owner(bs, bp);
	if (error)
		goto out;

	cur->bc_ptrs[level] = 1;

	while (level < cur->bc_nlevels) {
		block = XFS_BUF_TO_BLOCK(cur->bc_bufs[level]);

		if (level == 0) {
			/* End of leaf, pop back towards the root. */
			if (cur->bc_ptrs[level] >
			    be16_to_cpu(block->bb_numrecs)) {
				if (level < cur->bc_nlevels - 1)
					cur->bc_ptrs[level + 1]++;
				level++;
				continue;
			}

			/* Records in order for scrub? */
			error = xfs_btree_scrub_rec(bs);
			if (error)
				goto out;

			recp = xfs_btree_rec_addr(cur, cur->bc_ptrs[0], block);
			error = bs->scrub_rec(bs, recp);
			if (error < 0 ||
			    error == XFS_BTREE_QUERY_RANGE_ABORT)
				break;

			cur->bc_ptrs[level]++;
			continue;
		}

		/* End of node, pop back towards the root. */
		if (cur->bc_ptrs[level] > be16_to_cpu(block->bb_numrecs)) {
			if (level < cur->bc_nlevels - 1)
				cur->bc_ptrs[level + 1]++;
			level++;
			continue;
		}

		/* Keys in order for scrub? */
		error = xfs_btree_scrub_key(bs, level);
		if (error)
			goto out;

		/* Drill another level deeper. */
		pp = xfs_btree_ptr_addr(cur, cur->bc_ptrs[level], block);
		level--;
		error = xfs_btree_lookup_get_block(cur, level, pp,
				&block);
		if (error)
			goto out;

		xfs_btree_get_block(cur, level, &bp);
		error = xfs_btree_check_block(cur, block, level, bp);
		if (error)
			goto out;

		error = xfs_btree_scrub_check_owner(bs, bp);
		if (error)
			goto out;

		cur->bc_ptrs[level] = 1;
	}

out:
	/*
	 * If we don't end this function with the cursor pointing at a record
	 * block, a subsequent non-error cursor deletion will not release
	 * node-level buffers, causing a buffer leak.  This is quite possible
	 * with a zero-results range query, so release the buffers if we
	 * failed to return any results.
	 */
	if (cur->bc_bufs[0] == NULL) {
		for (i = 0; i < cur->bc_nlevels; i++) {
			if (cur->bc_bufs[i]) {
				xfs_trans_brelse(cur->bc_tp, cur->bc_bufs[i]);
				cur->bc_bufs[i] = NULL;
				cur->bc_ptrs[i] = 0;
				cur->bc_ra[i] = 0;
			}
		}
	}

	/* Check the deferred stuff */
	if (!error) {
		if (bs->cur->bc_btnum == XFS_BTNUM_BNO)
			bs->bno_cur = bs->cur;
		else if (bs->cur->bc_btnum == XFS_BTNUM_RMAP)
			bs->rmap_cur = bs->cur;
		list_for_each_entry(co, &bs->to_check, list) {
			error = xfs_btree_block_check_owner(bs, co->bno);
			if (error)
				break;
		}
	}
	list_for_each_entry_safe(co, n, &bs->to_check, list) {
		list_del(&co->list);
		kmem_free(co);
	}

	if (bs->bno_cur && bs->bno_cur != bs->cur)
		xfs_btree_del_cursor(bs->bno_cur, XFS_BTREE_ERROR);
	if (bs->rmap_cur && bs->rmap_cur != bs->cur)
		xfs_btree_del_cursor(bs->rmap_cur, XFS_BTREE_ERROR);

	if (error || bs->error)
		xfs_alert(cur->bc_mp,
			"Corruption detected. Unmount and run xfs_repair.");

	return error;
}
