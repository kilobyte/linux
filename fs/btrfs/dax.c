// SPDX-License-Identifier: GPL-2.0
/*
 * DAX support for BTRFS
 *
 * Copyright (c) 2019  SUSE Linux
 * Author: Goldwyn Rodrigues <rgoldwyn@suse.com>
 */

#ifdef CONFIG_FS_DAX
#include <linux/dax.h>
#include <linux/iomap.h>
#include <linux/uio.h>
#include "ctree.h"
#include "btrfs_inode.h"

struct btrfs_iomap {
	u64 start;
	u64 end;
	bool nocow;
	struct extent_changeset *data_reserved;
	struct extent_state *cached_state;
};

static struct btrfs_iomap *btrfs_iomap_init(struct inode *inode,
				     struct extent_map **em,
				     loff_t pos, loff_t length)
{
	int ret = 0;
	struct extent_map *map = *em;
	struct btrfs_iomap *bi;

	bi = kzalloc(sizeof(struct btrfs_iomap), GFP_NOFS);
	if (!bi)
		return ERR_PTR(-ENOMEM);

	bi->start = round_down(pos, PAGE_SIZE);
	bi->end = PAGE_ALIGN(pos + length);

	/* Wait for existing ordered extents in range to finish */
	btrfs_wait_ordered_range(inode, bi->start, bi->end - bi->start);

	lock_extent_bits(&BTRFS_I(inode)->io_tree, bi->start, bi->end, &bi->cached_state);

	ret = btrfs_delalloc_reserve_space(inode, &bi->data_reserved,
			bi->start, bi->end - bi->start);
	if (ret) {
		unlock_extent_cached(&BTRFS_I(inode)->io_tree, bi->start, bi->end,
				&bi->cached_state);
		kfree(bi);
		return ERR_PTR(ret);
	}

	refcount_inc(&map->refs);
	ret = btrfs_get_extent_map_write(em, NULL,
			inode, bi->start, bi->end - bi->start, &bi->nocow);
	if (ret) {
		unlock_extent_cached(&BTRFS_I(inode)->io_tree, bi->start, bi->end,
				&bi->cached_state);
		btrfs_delalloc_release_space(inode,
				bi->data_reserved, bi->start,
				bi->end - bi->start, true);
		extent_changeset_free(bi->data_reserved);
		kfree(bi);
		return ERR_PTR(ret);
	}
	free_extent_map(map);
	return bi;
}

static void *dax_address(struct block_device *bdev, struct dax_device *dax_dev,
			 sector_t sector, loff_t pos, loff_t length)
{
	size_t size = ALIGN(pos + length, PAGE_SIZE);
	int id, ret = 0;
	void *kaddr = NULL;
	pgoff_t pgoff;
	long map_len;

	id = dax_read_lock();

	ret = bdev_dax_pgoff(bdev, sector, size, &pgoff);
	if (ret)
		goto out;

	map_len = dax_direct_access(dax_dev, pgoff, PHYS_PFN(size),
			&kaddr, NULL);
	if (map_len < 0)
		ret = map_len;

out:
	dax_read_unlock(id);
	if (ret)
		return ERR_PTR(ret);
	return kaddr;
}

static int btrfs_iomap_begin(struct inode *inode, loff_t pos,
		loff_t length, unsigned flags, struct iomap *iomap)
{
	struct extent_map *em;
	struct btrfs_fs_info *fs_info = btrfs_sb(inode->i_sb);
	struct btrfs_iomap *bi = NULL;
	unsigned offset = pos & (PAGE_SIZE - 1);
	u64 srcblk = 0;
	loff_t diff;

	em = btrfs_get_extent(BTRFS_I(inode), NULL, 0, pos, length, 0);

	iomap->type = IOMAP_MAPPED;

	if (flags & IOMAP_WRITE) {
		if (em->block_start != EXTENT_MAP_HOLE)
			srcblk = em->block_start + pos - em->start - offset;

		bi = btrfs_iomap_init(inode, &em, pos, length);
		if (IS_ERR(bi))
			return PTR_ERR(bi);

	}

	/*
	 * Advance the difference between pos and start, to align well with
	 * inline_data in case of writes
	 */
	diff = round_down(pos - em->start, PAGE_SIZE);
	iomap->offset = em->start + diff;
	iomap->length = em->len - diff;
	iomap->bdev = em->bdev;
	iomap->dax_dev = fs_info->dax_dev;

	/*
	 * This will be true for reads only since we have already
	 * allocated em
	 */
	if (em->block_start == EXTENT_MAP_HOLE ||
			em->flags == EXTENT_FLAG_FILLING) {
		iomap->type = IOMAP_HOLE;
		return 0;
	}

	iomap->addr = em->block_start + diff;
	/* Check if we really need to copy data from old extent */
	if (bi && !bi->nocow && (offset || pos + length < bi->end || flags & IOMAP_FAULT)) {
		iomap->type = IOMAP_DAX_COW;
		if (srcblk) {
			sector_t sector = (srcblk + (pos & PAGE_MASK) -
					  iomap->offset) >> 9;
			iomap->inline_data = dax_address(em->bdev,
					fs_info->dax_dev, sector, pos, length);
			if (IS_ERR(iomap->inline_data)) {
				kfree(bi);
				return PTR_ERR(iomap->inline_data);
			}
		}
	}

	iomap->private = bi;
	return 0;
}

static int btrfs_iomap_end(struct inode *inode, loff_t pos,
		loff_t length, ssize_t written, unsigned flags,
		struct iomap *iomap)
{
	struct btrfs_iomap *bi = iomap->private;
	u64 wend;

	if (!bi)
		return 0;

	unlock_extent_cached(&BTRFS_I(inode)->io_tree, bi->start, bi->end,
			&bi->cached_state);

	wend = PAGE_ALIGN(pos + written);
	if (wend < bi->end) {
		btrfs_delalloc_release_space(inode,
				bi->data_reserved, wend,
				bi->end - wend, true);
	}

	btrfs_update_ordered_extent(inode, bi->start, wend - bi->start, true);
	btrfs_delalloc_release_extents(BTRFS_I(inode), wend - bi->start, false);
	extent_changeset_free(bi->data_reserved);
	kfree(bi);
	return 0;
}

static const struct iomap_ops btrfs_iomap_ops = {
	.iomap_begin		= btrfs_iomap_begin,
	.iomap_end		= btrfs_iomap_end,
};

ssize_t btrfs_file_dax_read(struct kiocb *iocb, struct iov_iter *to)
{
	ssize_t ret;
	struct inode *inode = file_inode(iocb->ki_filp);

	inode_lock_shared(inode);
	ret = dax_iomap_rw(iocb, to, &btrfs_iomap_ops);
	inode_unlock_shared(inode);

	return ret;
}

ssize_t btrfs_file_dax_write(struct kiocb *iocb, struct iov_iter *iter)
{
	ssize_t ret = 0;
	u64 pos = iocb->ki_pos;
	struct inode *inode = file_inode(iocb->ki_filp);
	ret = dax_iomap_rw(iocb, iter, &btrfs_iomap_ops);

	if (ret > 0) {
		pos += ret;
		if (pos > i_size_read(inode))
			i_size_write(inode, pos);
		iocb->ki_pos = pos;
	}
	return ret;
}

vm_fault_t btrfs_dax_fault(struct vm_fault *vmf)
{
	vm_fault_t ret;
	pfn_t pfn;
	ret = dax_iomap_fault(vmf, PE_SIZE_PTE, &pfn, NULL, &btrfs_iomap_ops);
	if (ret & VM_FAULT_NEEDDSYNC)
		ret = dax_finish_sync_fault(vmf, PE_SIZE_PTE, pfn);

	return ret;
}

int btrfs_dax_file_range_compare(struct inode *src, loff_t srcoff,
		struct inode *dest, loff_t destoff, loff_t len,
		bool *is_same)
{
	return dax_file_range_compare(src, srcoff, dest, destoff, len,
				      is_same, &btrfs_iomap_ops);
}

/*
 * zero a part of the page only. This should CoW (via iomap_begin) if required
 */
int btrfs_dax_zero_block(struct inode *inode, loff_t from, loff_t len, bool front)
{
	loff_t start = round_down(from, PAGE_SIZE);
	loff_t end = round_up(from, PAGE_SIZE);
	loff_t offset = from;
	int ret = 0;

	if (front) {
		len = from - start;
		offset = start;
	} else	{
		if (!len)
			len = end - from;
	}

	if (len)
		ret = iomap_zero_range(inode, offset, len, NULL, &btrfs_iomap_ops);

	return (ret < 0) ? ret : 0;
}
#endif /* CONFIG_FS_DAX */
