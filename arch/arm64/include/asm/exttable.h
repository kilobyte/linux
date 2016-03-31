#ifndef __ASM_EXTABLE_H
#define __ASM_EXTABLE_H

#include <asm/ptrace.h>

#define ARCH_HAS_RELATIVE_EXTABLE

#define _ASM_EXTABLE(from, to)						\
	"	.pushsection	__ex_table, \"a\"\n"			\
	"	.align		3\n"					\
	"	.long		(" #from " - .), (" #to " - .)\n"	\
	"	.popsection\n"

struct exception_table_entry
{
	int insn, fixup;
};

extern int fixup_exception(struct pt_regs *regs);

#endif /* __ASM_EXTABLE_H */
