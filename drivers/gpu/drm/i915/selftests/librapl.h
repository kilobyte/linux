/* SPDX-License-Identifier: MIT */
/*
 * Copyright © 2020 Intel Corporation
 */

#ifndef SELFTEST_LIBRAPL_H
#define SELFTEST_LIBRAPL_H

#include <linux/types.h>

struct drm_i915_private;

#ifdef CONFIG_X86
bool librapl_supported(const struct drm_i915_private *i915);

u64 librapl_energy_uJ(void);
#else
# define librapl_supported(x) 0
# define librapl_energy_uJ() 0
#endif

#endif /* SELFTEST_LIBRAPL_H */
