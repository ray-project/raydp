# Plans

A plan captures **what we will build and in what order**, and why, agreed before the implementation PR.

Name plans `<slug>-plan.md`, and start from [`TEMPLATE.md`](TEMPLATE.md).

The template follows a principal-engineer review order — framing, requirements, design, risk and operations, delivery. It is deliberately longer than most changes need: skip a section that does not apply, but say so in a line rather than deleting the heading, so a reviewer can see what you chose not to write.

The full bar — what a plan must contain, and what each section is for — lives in the **Plans** section of [`AGENTS.md`](../../AGENTS.md). In short:

- **Executable by someone else.** Concrete file paths, signatures, message shapes, edge cases, and acceptance criteria. Load-bearing decisions are made here, not deferred to implementation.
- **Exhaustive about surfaces.** Every module, Spark shim, Maven wiring change, Python API, test, and doc page the change touches. A surprise file in the implementation PR means the plan was incomplete.
- **Immutable once merged.** Trivial edits only. A later design shift goes in a new plan that supersedes it.

_No plans recorded yet._
