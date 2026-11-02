# Architecture Decision Records

This directory records decisions that constrain future work in RayDP — a Spark or Ray contract the project now depends on, an executor-lifecycle model, a compatibility boundary, or a deliberate divergence from upstream. Routine fixes that follow an existing decision do not need one.

An ADR answers **why**. A plan in [`../plan/`](../plan) answers **what we will build and in what order**. When a change needs both, **the ADR lands first** — a plan schedules decisions that are already made, so it should never depend on an unlanded ADR. If a plan under review turns out to rest on an unrecorded decision, split that decision out and land it here before approving the plan.

## Writing one

1. Copy [`0000-template.md`](0000-template.md) to `NNNN-kebab-title.md`, using the next free number.
2. Fill it in. Keep the diagram small and use real names from the code.
3. Open it as part of the PR that makes the decision real, not weeks later.

## Immutability

**Merged ADRs are historical records and are not rewritten.** Once an ADR merges, it is not edited to reverse, expand, or refine the decision it captured — even when the new thinking is correct.

- **Allowed:** typo fixes, broken-link repair, resolving a deliberate placeholder (for example, filling in a PR number in the same PR that lands it).
- **Not allowed:** rewriting Context, Decision, or Consequences after merge to match a later design.

To change a decision, write a **new** ADR that references the original and explains what it changes, then mark the original `Status: Superseded by ADR-NNNN`. If the scope is large enough to need its own phased approach, write a new plan instead. The point is an auditable trail: a reader should be able to see what was believed at each step, not just the current answer.

Because ADRs are immutable, **do not reference them from code comments** — a pointer to "ADR-0003 §Alternatives" rots silently when that ADR is superseded or its sections are renamed. State the contract in the comment itself.

## Index

| ADR | Title | Status |
| --- | ----- | ------ |
| — | _No ADRs recorded yet._ | — |
