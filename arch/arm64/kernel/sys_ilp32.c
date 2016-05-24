/*
 * AArch64- ILP32 specific system calls implementation
 *
 * Copyright (C) 2016 Cavium Inc.
 * Author: Andrew Pinski <apinski@cavium.com>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#define __SYSCALL_COMPAT

#include <linux/compiler.h>
#include <linux/errno.h>
#include <linux/fs.h>
#include <linux/mm.h>
#include <linux/msg.h>
#include <linux/export.h>
#include <linux/sched.h>
#include <linux/slab.h>
#include <linux/syscalls.h>
#include <linux/compat.h>
#include <asm-generic/syscalls.h>

/*
 * Using aarch32 syscall handlerss where off_t is passed.
 */
#define compat_sys_fadvise64_64		compat_sys_fadvise64_64_wrapper
#define compat_sys_fallocate		compat_sys_fallocate_wrapper
#define compat_sys_fcntl64		sys_fcntl
#define compat_sys_ftruncate64		compat_sys_ftruncate64_wrapper
#define compat_sys_pread64		compat_sys_pread64_wrapper
#define compat_sys_pwrite64		compat_sys_pwrite64_wrapper
#define compat_sys_readahead		compat_sys_readahead_wrapper
#define compat_sys_shmat		sys_shmat
#define compat_sys_sync_file_range	compat_sys_sync_file_range2_wrapper
#define compat_sys_truncate64		compat_sys_truncate64_wrapper
#define sys_mmap2			compat_sys_mmap2_wrapper
#define sys_ptrace			compat_sys_ptrace

/*
 * Use non-compat syscall handlerss where stat and statfs structure
 * pointers are passed, as their layout is identical to LP64.
 */
#define compat_sys_fstatfs64		sys_fstatfs
#define compat_sys_statfs64		sys_statfs
#define sys_fstat64			sys_newfstat
#define sys_fstatat64			sys_newfstatat

asmlinkage long compat_sys_fadvise64_64_wrapper(void);
asmlinkage long compat_sys_fallocate_wrapper(void);
asmlinkage long compat_sys_ftruncate64_wrapper(void);
asmlinkage long compat_sys_mmap2_wrapper(void);
asmlinkage long compat_sys_pread64_wrapper(void);
asmlinkage long compat_sys_pwrite64_wrapper(void);
asmlinkage long compat_sys_readahead_wrapper(void);
asmlinkage long compat_sys_sync_file_range2_wrapper(void);
asmlinkage long compat_sys_truncate64_wrapper(void);

asmlinkage long ilp32_sys_rt_sigreturn_wrapper(void);
#define compat_sys_rt_sigreturn        ilp32_sys_rt_sigreturn_wrapper

#include <asm/syscall.h>

#undef __SYSCALL
#define __SYSCALL(nr, sym)	[nr] = sym,

/*
 * The sys_call_ilp32_table array must be 4K aligned to be accessible from
 * kernel/entry.S.
 */
void *sys_call_ilp32_table[__NR_syscalls] __aligned(4096) = {
	[0 ... __NR_syscalls - 1] = sys_ni_syscall,
#include <asm/unistd.h>
};
