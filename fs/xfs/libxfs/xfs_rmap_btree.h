/*
 * Copyright (c) 2014 Red Hat, Inc.
 * All Rights Reserved.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License as
 * published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it would be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write the Free Software Foundation,
 * Inc.,  51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 */
#ifndef __XFS_RMAP_BTREE_H__
#define	__XFS_RMAP_BTREE_H__

struct xfs_buf;
struct xfs_btree_cur;
struct xfs_mount;

/* rmaps only exist on crc enabled filesystems */
#define XFS_RMAP_BLOCK_LEN	XFS_BTREE_SBLOCK_CRC_LEN

/*
 * Record, key, and pointer address macros for btree blocks.
 *
 * (note that some of these may appear unused, but they are used in userspace)
 */
#define XFS_RMAP_REC_ADDR(block, index) \
	((struct xfs_rmap_rec *) \
		((char *)(block) + XFS_RMAP_BLOCK_LEN + \
		 (((index) - 1) * sizeof(struct xfs_rmap_rec))))

#define XFS_RMAP_KEY_ADDR(block, index) \
	((struct xfs_rmap_key *) \
		((char *)(block) + XFS_RMAP_BLOCK_LEN + \
		 ((index) - 1) * 2 * sizeof(struct xfs_rmap_key)))

#define XFS_RMAP_HIGH_KEY_ADDR(block, index) \
	((struct xfs_rmap_key *) \
		((char *)(block) + XFS_RMAP_BLOCK_LEN + \
		 sizeof(struct xfs_rmap_key) + \
		 ((index) - 1) * 2 * sizeof(struct xfs_rmap_key)))

#define XFS_RMAP_PTR_ADDR(block, index, maxrecs) \
	((xfs_rmap_ptr_t *) \
		((char *)(block) + XFS_RMAP_BLOCK_LEN + \
		 (maxrecs) * 2 * sizeof(struct xfs_rmap_key) + \
		 ((index) - 1) * sizeof(xfs_rmap_ptr_t)))

struct xfs_btree_cur *xfs_rmapbt_init_cursor(struct xfs_mount *mp,
				struct xfs_trans *tp, struct xfs_buf *bp,
				xfs_agnumber_t agno);
int xfs_rmapbt_maxrecs(struct xfs_mount *mp, int blocklen, int leaf);
extern void xfs_rmapbt_compute_maxlevels(struct xfs_mount *mp);

int xfs_rmap_lookup_le(struct xfs_btree_cur *cur, xfs_agblock_t bno,
		xfs_extlen_t len, uint64_t owner, uint64_t offset,
		unsigned int flags, int *stat);
int xfs_rmap_lookup_eq(struct xfs_btree_cur *cur, xfs_agblock_t bno,
		xfs_extlen_t len, uint64_t owner, uint64_t offset,
		unsigned int flags, int *stat);
int xfs_rmapbt_insert(struct xfs_btree_cur *rcur, xfs_agblock_t agbno,
		xfs_extlen_t len, uint64_t owner, uint64_t offset,
		unsigned int flags);
int xfs_rmap_get_rec(struct xfs_btree_cur *cur, struct xfs_rmap_irec *irec,
		int *stat);

int xfs_rmap_find_left_neighbor(struct xfs_btree_cur *cur, xfs_agblock_t bno,
		uint64_t owner, uint64_t offset, unsigned int flags,
		struct xfs_rmap_irec *irec, int	*stat);
int xfs_rmap_lookup_le_range(struct xfs_btree_cur *cur, xfs_agblock_t bno,
		uint64_t owner, uint64_t offset, unsigned int flags,
		struct xfs_rmap_irec *irec, int	*stat);

/* functions for updating the rmapbt for bmbt blocks and AG btree blocks */
int xfs_rmap_alloc(struct xfs_trans *tp, struct xfs_buf *agbp,
		   xfs_agnumber_t agno, xfs_agblock_t bno, xfs_extlen_t len,
		   struct xfs_owner_info *oinfo);
int xfs_rmap_free(struct xfs_trans *tp, struct xfs_buf *agbp,
		  xfs_agnumber_t agno, xfs_agblock_t bno, xfs_extlen_t len,
		  struct xfs_owner_info *oinfo);

typedef int (*xfs_rmapbt_query_range_fn)(
	struct xfs_btree_cur	*cur,
	struct xfs_rmap_irec	*rec,
	void			*priv);

int xfs_rmapbt_query_range(struct xfs_btree_cur *cur,
		struct xfs_rmap_irec *low_rec, struct xfs_rmap_irec *high_rec,
		xfs_rmapbt_query_range_fn fn, void *priv);

enum xfs_rmap_intent_type {
	XFS_RMAP_MAP,
	XFS_RMAP_MAP_SHARED,
	XFS_RMAP_UNMAP,
	XFS_RMAP_UNMAP_SHARED,
	XFS_RMAP_CONVERT,
	XFS_RMAP_CONVERT_SHARED,
	XFS_RMAP_ALLOC,
	XFS_RMAP_FREE,
};

struct xfs_rmap_intent {
	struct list_head			ri_list;
	enum xfs_rmap_intent_type		ri_type;
	__uint64_t				ri_owner;
	int					ri_whichfork;
	struct xfs_bmbt_irec			ri_bmap;
};

/* functions for updating the rmapbt based on bmbt map/unmap operations */
int xfs_rmap_map_extent(struct xfs_mount *mp, struct xfs_defer_ops *dfops,
		struct xfs_inode *ip, int whichfork,
		struct xfs_bmbt_irec *imap);
int xfs_rmap_unmap_extent(struct xfs_mount *mp, struct xfs_defer_ops *dfops,
		struct xfs_inode *ip, int whichfork,
		struct xfs_bmbt_irec *imap);
int xfs_rmap_convert_extent(struct xfs_mount *mp, struct xfs_defer_ops *dfops,
		struct xfs_inode *ip, int whichfork,
		struct xfs_bmbt_irec *imap);
int xfs_rmap_alloc_defer(struct xfs_mount *mp, struct xfs_defer_ops *dfops,
		xfs_agnumber_t agno, xfs_agblock_t bno, xfs_extlen_t len,
		__uint64_t owner);
int xfs_rmap_free_defer(struct xfs_mount *mp, struct xfs_defer_ops *dfops,
		xfs_agnumber_t agno, xfs_agblock_t bno, xfs_extlen_t len,
		__uint64_t owner);

void xfs_rmap_finish_one_cleanup(struct xfs_trans *tp,
		struct xfs_btree_cur *rcur, int error);
int xfs_rmap_finish_one(struct xfs_trans *tp, enum xfs_rmap_intent_type type,
		__uint64_t owner, int whichfork, xfs_fileoff_t startoff,
		xfs_fsblock_t startblock, xfs_filblks_t blockcount,
		xfs_exntst_t state, struct xfs_btree_cur **pcur);

extern xfs_extlen_t xfs_rmapbt_calc_size(struct xfs_mount *mp,
		unsigned long long len);
extern xfs_extlen_t xfs_rmapbt_max_size(struct xfs_mount *mp);

extern int xfs_rmapbt_calc_reserves(struct xfs_mount *mp,
		xfs_agnumber_t agno, xfs_extlen_t *ask, xfs_extlen_t *used);

#endif	/* __XFS_RMAP_BTREE_H__ */
