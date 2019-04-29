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
#include "ctree.h"
#include "btrfs_inode.h"

static int btrfs_iomap_begin(struct inode *inode, loff_t pos,
		loff_t length, unsigned flags, struct iomap *iomap)
{
	struct extent_map *em;
	struct btrfs_fs_info *fs_info = btrfs_sb(inode->i_sb);
	em = btrfs_get_extent(BTRFS_I(inode), NULL, 0, pos, length, 0);
	if (em->block_start == EXTENT_MAP_HOLE) {
		iomap->type = IOMAP_HOLE;
		return 0;
	}
	iomap->type = IOMAP_MAPPED;
	iomap->bdev = em->bdev;
	iomap->dax_dev = fs_info->dax_dev;
	iomap->offset = em->start;
	iomap->length = em->len;
	iomap->addr = em->block_start;
	return 0;
}

static const struct iomap_ops btrfs_iomap_ops = {
	.iomap_begin		= btrfs_iomap_begin,
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
#endif /* CONFIG_FS_DAX */
