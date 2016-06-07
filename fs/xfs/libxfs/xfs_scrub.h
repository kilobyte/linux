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
#ifndef __XFS_SCRUB_H__
#define	__XFS_SCRUB_H__

/* btree scrub */
struct xfs_btree_scrub;

typedef int (*xfs_btree_scrub_rec_fn)(
	struct xfs_btree_scrub	*bs,
	union xfs_btree_rec	*rec);

struct xfs_btree_scrub {
	/* caller-provided scrub state */
	struct xfs_btree_cur		*cur;
	xfs_btree_scrub_rec_fn		scrub_rec;
	struct xfs_buf			*agi_bp;
	struct xfs_buf			*agf_bp;
	struct xfs_buf			*agfl_bp;
	struct xfs_owner_info		oinfo;

	/* internal scrub state */
	union xfs_btree_rec		lastrec;
	bool				firstrec;
	union xfs_btree_key		lastkey[XFS_BTREE_MAXLEVELS];
	bool				firstkey[XFS_BTREE_MAXLEVELS];
	struct xfs_btree_cur		*rmap_cur;
	struct xfs_btree_cur		*bno_cur;
	struct list_head		to_check;
	int				error;
};

int xfs_btree_scrub(struct xfs_btree_scrub *bs);
void xfs_btree_scrub_error(struct xfs_btree_cur *cur, int level,
		const char *file, int line, const char *check);
#define XFS_BTREC_SCRUB_CHECK(bs, fs_ok) \
	if (!(fs_ok)) { \
		xfs_btree_scrub_error((bs)->cur, 0, __FILE__, __LINE__, #fs_ok); \
		(bs)->error = -EFSCORRUPTED; \
	}
#define XFS_BTREC_SCRUB_GOTO(bs, fs_ok, label) \
	if (!(fs_ok)) { \
		xfs_btree_scrub_error((bs)->cur, 0, __FILE__, __LINE__, #fs_ok); \
		(bs)->error = -EFSCORRUPTED; \
		goto label; \
	}
#define XFS_BTKEY_SCRUB_CHECK(bs, level, fs_ok) \
	if (!(fs_ok)) { \
		xfs_btree_scrub_error((bs)->cur, (level), __FILE__, __LINE__, #fs_ok); \
		(bs)->error = -EFSCORRUPTED; \
	}
#define XFS_BTKEY_SCRUB_GOTO(bs, level, fs_ok, label) \
	if (!(fs_ok)) { \
		xfs_btree_scrub_error((bs)->cur, 0, __FILE__, __LINE__, #fs_ok); \
		(bs)->error = -EFSCORRUPTED; \
		goto label; \
	}

#endif	/* __XFS_SCRUB_H__ */
