/* SPDX-License-Identifier: MIT */
/*
 * Copyright © 2019 Intel Corporation
 */

#ifndef INTEL_LLC_H
#define INTEL_LLC_H

struct intel_llc;

#ifdef CONFIG_X86
void intel_llc_enable(struct intel_llc *llc);
void intel_llc_disable(struct intel_llc *llc);
#else
# define intel_llc_enable(x)
# define intel_llc_disable(x)
#endif

#endif /* INTEL_LLC_H */
