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
#include "xfs_bmap.h"
#include "xfs_refcount.h"
#include "xfs_rmap_btree.h"
#include "xfs_alloc.h"
#include "xfs_ialloc.h"
#include "xfs_sysfs.h"
#include <linux/kernel.h>

/* general scrub attributes */
struct xfs_scrub_attr {
	struct attribute attr;
	bool (*is_visible)(struct xfs_mount *mp, struct xfs_scrub_attr *attr);
	ssize_t (*show)(struct xfs_mount *mp, struct xfs_scrub_attr *attr,
			char *buf);
	ssize_t (*store)(struct xfs_mount *mp, struct xfs_scrub_attr *attr,
			const char *buf, size_t count);
};

static inline struct xfs_scrub_attr *
to_scrub_attr(struct attribute *attr)
{
	return container_of(attr, struct xfs_scrub_attr, attr);
}

static inline struct xfs_mount *to_mount(struct kobject	*kobj)
{
	struct xfs_kobj *k = container_of(kobj, struct xfs_kobj, kobject);

	return container_of(k, struct xfs_mount, m_scrub_kobj);
}

STATIC ssize_t
xfs_scrub_attr_show(
	struct kobject		*kobject,
	struct attribute	*attr,
	char			*buf)
{
	struct xfs_scrub_attr	*sa = to_scrub_attr(attr);
	struct xfs_mount	*mp = to_mount(kobject);

	return sa->show ? sa->show(mp, sa, buf) : 0;
}

STATIC ssize_t
xfs_scrub_attr_store(
	struct kobject		*kobject,
	struct attribute	*attr,
	const char		*buf,
	size_t			count)
{
	struct xfs_scrub_attr	*sa = to_scrub_attr(attr);
	struct xfs_mount	*mp = to_mount(kobject);

	return sa->store ? sa->store(mp, sa, buf, count) : 0;
}

STATIC umode_t
xfs_scrub_attr_visible(
	struct kobject		*kobject,
	struct attribute	*attr,
	int unused)
{
	struct xfs_scrub_attr	*sa = to_scrub_attr(attr);
	struct xfs_mount	*mp = to_mount(kobject);

	if (!sa->is_visible || sa->is_visible(mp, sa))
		return attr->mode;
	return 0;
}

static const struct sysfs_ops xfs_scrub_ops = {
	.show = xfs_scrub_attr_show,
	.store = xfs_scrub_attr_store,
};

static struct kobj_type xfs_scrub_ktype = {
	.release = xfs_sysfs_release,
	.sysfs_ops = &xfs_scrub_ops,
};

/* per-AG scrub attributes */
struct xfs_agdata_scrub_attr {
	struct xfs_scrub_attr sa;
	bool (*has_feature)(struct xfs_sb *);
	int (*scrub)(struct xfs_mount *mp, xfs_agnumber_t agno);
};

static inline struct xfs_agdata_scrub_attr *
to_agdata_scrub_attr(struct xfs_scrub_attr *sa)
{
	return container_of(sa, struct xfs_agdata_scrub_attr, sa);
}

STATIC bool
xfs_agdata_scrub_visible(
	struct xfs_mount		*mp,
	struct xfs_scrub_attr		*sa)
{
	struct xfs_agdata_scrub_attr	*asa = to_agdata_scrub_attr(sa);

	return (!asa->has_feature || asa->has_feature(&mp->m_sb));
}

STATIC ssize_t
xfs_agdata_scrub_show(
	struct xfs_mount		*mp,
	struct xfs_scrub_attr		*sa,
	char				*buf)
{
	return snprintf(buf, PAGE_SIZE, "0:%u\n", mp->m_sb.sb_agcount - 1);
}

STATIC ssize_t
xfs_agdata_scrub_store(
	struct xfs_mount		*mp,
	struct xfs_scrub_attr		*sa,
	const char			*buf,
	size_t				count)
{
	unsigned long			val;
	xfs_agnumber_t			agno;
	struct xfs_agdata_scrub_attr	*asa = to_agdata_scrub_attr(sa);
	int				error;

	error = kstrtoul(buf, 0, &val);
	if (error)
		return error;
	agno = val;
	if (agno >= mp->m_sb.sb_agcount)
		return -EINVAL;
	error = asa->scrub(mp, agno);
	if (error)
		return error;
	return count;
}

#define XFS_AGDATA_SCRUB_ATTR(_name, _fn)	     \
static struct xfs_agdata_scrub_attr xfs_agdata_scrub_attr_##_name = {	     \
	.sa = {								     \
		.attr = {.name = __stringify(_name), .mode = 0600 },	     \
		.is_visible = xfs_agdata_scrub_visible,			     \
		.show = xfs_agdata_scrub_show,				     \
		.store = xfs_agdata_scrub_store,			     \
	},								     \
	.has_feature = _fn,						     \
	.scrub = xfs_##_name##_scrub,				     \
}
#define XFS_AGDATA_SCRUB_LIST(name)	&xfs_agdata_scrub_attr_##name.sa.attr

XFS_AGDATA_SCRUB_ATTR(bnobt, NULL);
XFS_AGDATA_SCRUB_ATTR(cntbt, NULL);
XFS_AGDATA_SCRUB_ATTR(inobt, NULL);
XFS_AGDATA_SCRUB_ATTR(finobt, xfs_sb_version_hasfinobt);
XFS_AGDATA_SCRUB_ATTR(rmapbt, xfs_sb_version_hasrmapbt);

static struct attribute *xfs_agdata_scrub_attrs[] = {
	XFS_AGDATA_SCRUB_LIST(bnobt),
	XFS_AGDATA_SCRUB_LIST(cntbt),
	XFS_AGDATA_SCRUB_LIST(inobt),
	XFS_AGDATA_SCRUB_LIST(finobt),
	XFS_AGDATA_SCRUB_LIST(rmapbt),
	NULL,
};

static const struct attribute_group xfs_agdata_scrub_attr_group = {
	.is_visible = xfs_scrub_attr_visible,
	.attrs = xfs_agdata_scrub_attrs,
};

int
xfs_scrub_init(
	struct xfs_mount	*mp)
{
	int			error;

	error = xfs_sysfs_init(&mp->m_scrub_kobj, &xfs_scrub_ktype,
			&mp->m_kobj, "check");
	if (error)
		return error;

	error = sysfs_create_group(&mp->m_scrub_kobj.kobject,
			&xfs_agdata_scrub_attr_group);
	if (error)
		goto err;
	return error;
err:
	xfs_sysfs_del(&mp->m_scrub_kobj);
	return error;
}

void
xfs_scrub_free(
	struct xfs_mount	*mp)
{
	sysfs_remove_group(&mp->m_scrub_kobj.kobject,
			&xfs_agdata_scrub_attr_group);
	xfs_sysfs_del(&mp->m_scrub_kobj);
}
