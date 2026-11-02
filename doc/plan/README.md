# Plans

A plan captures **what we will build and in what order**, agreed before the implementation PR. An ADR in [`../adr/`](../adr) captures **why** a decision was made, and lands **first** — a plan schedules decisions that are already recorded, so it never depends on an unlanded ADR. If drafting a plan surfaces a load-bearing decision nobody has written down, land that ADR before the plan.

Name plans `<slug>-plan.md`, and start from [`TEMPLATE.md`](TEMPLATE.md).

The template follows a principal-engineer review order — framing, requirements, design, risk and operations, delivery. It is deliberately longer than most changes need: skip a section that does not apply, but say so in a line rather than deleting the heading, so a reviewer can see what you chose not to write.

The full bar — what a plan must contain, and what each section is for — lives in the **Plans** section of [`AGENTS.md`](../../AGENTS.md). In short:

- **Executable by someone else.** Concrete file paths, signatures, message shapes, edge cases, and acceptance criteria. Load-bearing decisions are made here, not deferred to implementation.
- **Exhaustive about surfaces.** Every module, Spark shim, Maven wiring change, Python API, test, and doc page the change touches. A surprise file in the implementation PR means the plan was incomplete.
- **Immutable once merged.** Trivial edits only. A later design shift goes in a new ADR, or a new plan when the scope needs its own phasing.

_No plans recorded yet._
