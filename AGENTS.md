# Agent Instructions

This file is the primary instruction surface for agents contributing to RayDP. It is injected into your context on every interaction — keep that in mind when proposing changes to it.

See [README.md](README.md) for what RayDP is and its user-facing APIs, and [CONTRIBUTING.md](CONTRIBUTING.md) for environment prerequisites and the build.

## Project Identity

RayDP runs Apache Spark on [Ray](https://github.com/ray-project/ray) and lets a single Python program mix PySpark with Ray libraries. Ray is the Spark resource manager: **Spark executors run inside Ray actors**, while communication between executors still uses Spark's own internal protocol.

Two consequences shape almost every change here:

- **This is a mixed-language project.** Core logic is Scala/Java under `core/`, the user-facing API is Python under `python/`. A change to executor lifecycle, scheduling, or DataFrame conversion usually touches both sides, and the Python wheel bundles the built JAR.
- **RayDP straddles two projects it does not own.** It implements Spark's cluster-manager contracts and depends on Ray's actor semantics. When behavior is surprising, the answer is usually in Spark's `CoarseGrainedSchedulerBackend` or Ray's actor lifecycle, not in RayDP. Read the relevant upstream source before inventing a RayDP-side workaround.

## Repository Structure

| Path | Purpose |
|---|---|
| `core/` | Maven root — import `core/pom.xml`, not the repo root |
| `core/raydp-main/` | Main Scala/Java implementation (app master, executors, object store) |
| `core/shims/` | Per-Spark-version compatibility layer |
| `core/shims/common/` | Shim interfaces and `SparkShimLoader` |
| `core/shims/spark322/` … `spark410/` | One module per supported Spark line |
| `core/agent/` | Java agent |
| `python/raydp/` | Python package (`init_spark`, estimators, dataset conversion) |
| `python/raydp/spark/` | Spark-on-Ray cluster management and DataFrame/Dataset bridge |
| `python/raydp/tests/` | pytest suite — the end-to-end test lane |
| `core/raydp-main/src/test/scala/` | JVM unit tests, run by surefire during `mvn verify` |
| `doc/` | Guides — `spark_on_ray.md`, `mpi.md` |
| `doc/plan/` | Implementation plans (immutable once merged) |
| `doc/adr/` | Architecture decision records (immutable once merged) |
| `examples/`, `tutorials/` | Runnable examples and Colab notebooks |
| `build.sh` | Builds core + Python wheel into `dist/` |

### Spark shims

Supported Spark lines each get a module under `core/shims/`. Anything version-dependent goes behind a shim interface in `core/shims/common/` and is resolved at runtime through `SparkShimLoader` — do **not** add Spark-version `if` branches in `raydp-main`. Adding support for a new Spark line means a new shim module plus its `<module>` entry, not conditionals in shared code.

**Adding Spark version support.** A new **patch** of a line that already has a shim is usually just a wider patch range; a new **minor** needs the full list. Work through all of it — a partial job builds fine and fails at runtime or in packaging:

1. **Shim coverage.** For a new patch, widen `SUPPORTED_PATCHES` in that line's `SparkShimProvider.scala` (for example `core/shims/spark410/`); `matches()` compares against exact `major.minor.patch` strings, so an uncovered patch fails shim resolution at runtime. For a new minor, add a module under `core/shims/sparkNMM/` with `SparkShimProvider.scala`, `SparkShims.scala`, and the `META-INF/services/com.intel.raydp.shims.SparkShimProvider` file, plus the `<module>` entry in `core/shims/pom.xml`.
2. **Maven version property.** Add or update `<sparkNMM.version>` in `core/pom.xml` (alongside `spark400.version`, `spark410.version`).
3. **PySpark bound in `python/setup.py`.** The requirement is pinned as a range (`pyspark >= 4.0.0, <= 4.1.1`). Raise the upper bound to match the highest version the shims actually cover — and no further. The cap exists so pip cannot install a PySpark that has no shim.
4. **Scala version.** Check what Scala version the new Spark release is built against and reconcile it with `<scala.version>` in `core/pom.xml`. A mismatch shows up as a `requires scala version:` warning during compilation and can become a runtime failure, so do not dismiss it.
5. **CI matrix.** For a new **minor**, add a `spark-version` entry in `.github/workflows/raydp.yml` so the new line is tested against every `ray-version`. **Do not add patch versions to the matrix** — patch coverage is a shim concern, and matrix points multiply against Ray and Python versions.
6. **Docs.** Update any supported-version statements in `README.md` or `doc/`.

## Environment

Per [CONTRIBUTING.md](CONTRIBUTING.md):

- **Java** — JDK 8 or 17. The build targets Java 8 source/target, but CI runs **JDK 17**; use 17 locally to match.
- **Python** — 3.10+.
- **Maven** — 3.6+.
- **Scala** — 2.13.12 (`scala.binary.version` = 2.13).

IDE setup: import `core/pom.xml` as the Maven project, set the SDK to Java 8 or 17, and mark `python/` as a source root so the `raydp` package resolves.

## Build

```bash
./build.sh                    # core + Python wheel -> dist/
pip install dist/raydp*.whl
```

`build.sh` branches on `$GITHUB_CI`: locally it runs `mvn clean package -q -DskipTests` in `core/`, in CI it runs `mvn verify -q` (tests **not** skipped). Then it builds the wheel from `python/` and copies it to `dist/`.

Manual core build:

```bash
cd core
mvn clean package -DskipTests
```

Useful while iterating on one module:

```bash
cd core
mvn -pl raydp-main -am compile        # -am is required; raydp-main depends on the shims
```

**Prefer `clean` when results look impossible.** The Scala incremental compiler in this build can report "Nothing to compile - all classes are up to date" while `target/classes` holds only the Java classes, which surfaces later as bogus "not found: type" errors. A clean build resolves it.

## Testing

- **`pytest python/raydp/tests/`** — the end-to-end suite, run in CI after `./build.sh`. This is the primary gate.
- JVM unit tests run during `mvn verify` (see below), so `./build.sh` in CI covers both lanes.
- CI matrix: Python 3.10/3.11 × Spark 4.0.0/4.1.0 × Ray 2.37.0/2.40.0/2.50.0 on `ubuntu-latest`, JDK 17. A change that only works on one Spark line will fail.
- Many pytest cases spin up real Ray clusters and Spark sessions, so they are slow and order-sensitive. Run the single relevant file while iterating.

### JVM tests

JVM tests live in `core/raydp-main/src/test/scala` and run through surefire during `mvn verify`, so CI executes them via `./build.sh`. `core/raydp-main/src/test/scala/org/apache/spark/deploy/raydp/ApplicationInfoTest.scala` is the working example — it drives `ApplicationInfo` directly, with no Ray runtime.

Two traps to respect when adding one:

- **Class naming must match surefire's default includes** (`Test*`, `*Test`, `*Tests`, `*TestCase`). A `*Suite` class compiles and is then silently never run.
- **Do not lower the `maven-surefire-plugin` version** in `core/raydp-main/pom.xml`. Versions before 2.22 have no JUnit Platform provider; with TestNG on the classpath they auto-select the TestNG provider and discover **zero** JUnit 5 tests while leaving the build green. Whenever you add a JVM test, confirm the reported run count includes it — a passing build is not evidence.

Anything needing a live Ray cluster or Spark session belongs in the pytest suite instead.

## Style

Enforced by the build, so a violation fails CI:

- **Scala** — `core/scalastyle.xml`, applied to `src/main/scala`. Max line length **100**. The Apache license header check is **enabled**, so every new file needs it. Method-length and cyclomatic-complexity checks are disabled.
- **Java** — checkstyle via `core/javastyle.xml`, applied to `src/main/java`, failing on warnings.
- **Python** — `pylint --rcfile=python/pylintrc` over `python/raydp` and `examples/*.py` in CI.
- Import order in Scala follows Spark convention: `java`/`javax`, `scala`, third-party, then `org.apache.spark`, separated by blank lines.

## Design and Review Bar

These exist to stop the symptom-fix ratchet: a subsystem accretes one guard after another, each closing a single edge case while deferring the model fix.

- **Root cause over symptom for repeat bugs.** If a fix is the second or later attempt at the same mechanism, a point-fix is a **blocker**, not an acceptable patch. Either fix the underlying model or state explicitly why the model is already right. The recurrence is the signal.
- **A special case that absorbs a condition is a missing-abstraction smell.** When a fix reads "in situation X, skip / ignore / swallow Y," that usually means an unmodeled state. Prefer generalizing the mechanism over special-casing around the symptom. Executor-lifecycle bookkeeping is where this bites hardest in this repo.
- **You may defer an enhancement, never a known correctness gap.** A reachable wrong state is either fixed in the change or filed as a tracked issue with an owner and linked. Writing a paragraph about it does not resolve it.

## Testing Bar

Coverage claims are load-bearing — a reviewer relies on them to decide whether a change is safe. Meet this bar before saying something is tested.

- **A test that cannot fail is not a test.** Before claiming a test covers a fix, break the fix and watch the test fail. Reverting the change, or neutralizing just the new guard, and re-running is the cheapest way to prove the assertion has teeth. Report the failure you observed, not just the pass.
- **A green build is not evidence that tests ran.** This repo has already shipped configurations where JVM tests compile and are silently never executed (see the surefire and class-naming traps above). Check the reported run count — `Tests run: N` with the N you expect — and treat "no failures" with zero tests as a red flag, not a pass.
- **Verify at the real gate, not the nearest one.** A successful `mvn compile` says nothing about behavior. The gate is `./build.sh` — which runs the JVM tests via `mvn verify` — followed by the relevant `pytest python/raydp/tests/<file>`. Do not report a change as working on the strength of a compile, a partial build, or an offline run.
- **Test at the most deterministic level that reproduces the bug.** Ordering and lifecycle bugs — executor disconnect racing a kill, restart racing registration — are unreliable to trigger end to end. Drive the state machine directly instead, so the ordering is chosen rather than hoped for. Reach for an end-to-end test when the integration itself is what's under test.
- **A repeat bug needs a regression test, not just a fix.** This pairs with the root-cause rule above: if a change is the second or later fix in the same mechanism and it lands without a test that pins the corrected behavior, that is a **blocker**. The recurrence is the evidence that the invariant was never captured.
- **Never move the goalposts to get green.** Do not weaken an assertion, widen a timeout, loosen a policy, or mark a test skipped to make a run pass. If a test is genuinely flaky, the skip must carry a reason and a tracking issue — `@pytest.mark.skip("flaky")` on `test_custom_installed_spark` is an existing example of the pattern, not a licence to add more.
- **Say what you did not run.** The CI matrix has three dimensions; local runs almost never cover it. State which Spark, Ray, and Python versions you exercised, and name the checks you skipped — a style check that could not run offline is a gap the reviewer needs to know about.

## Plans

Plans capture **what we will build and in what order**, before the implementation PR. Store them in `doc/plan/` as `<slug>-plan.md`, starting from [`doc/plan/TEMPLATE.md`](doc/plan/TEMPLATE.md). When asked to write a plan, write it there without asking where it goes.

**The template carries the full section list and per-section guidance** — framing, requirements, design, risk and operations, delivery, in principal-engineer review order. Read it when writing or reviewing a plan; you do not need it for anything else.

Four rules hold whether or not you have the template open:

- **Executable by someone else.** Concrete file paths, class and method signatures, message shapes, config keys, edge cases, and acceptance criteria — enough that another contributor or agent can implement it without going back to the author. Do not approve a plan whose load-bearing decisions are deferred to "decided during implementation"; locking those decisions first is the entire point.
- **Exhaustive about surfaces.** Scala/Java modules under `core/`, **each affected Spark shim** and any new shim module, Maven wiring, the Python API, wheel packaging, the pytest suite, the CI matrix if supported versions change, and `doc/` pages. A surprise file or shim in the implementation PR means the plan was incomplete. If you are unsure whether something is in scope, list it under *Out of scope* with a reason — silence is not an answer.
- **Diagram when structural.** A mermaid `flowchart` for wiring, `sequenceDiagram` for ordered driver / app master / executor interactions, `stateDiagram-v2` for lifecycle phases. Readability beats type-by-shape orthodoxy.
- **Immutable once merged.** Trivial edits only — typos, broken links, resolving a deliberate placeholder. A later design shift goes in a new ADR, or a new plan when the scope needs its own phasing. The one exception: amending a plan in its own still-open PR.
- **No open decisions.** A plan schedules decisions that are already made, so it never depends on an unlanded ADR. If writing or reviewing one surfaces a load-bearing decision that is not recorded yet, land the ADR first rather than carrying the decision inside the plan. Sequencing questions may stay open; design questions may not.

## Decision Recording

ADRs record **why** a decision was made, and they come **before** the plan that schedules the work. Store them in `doc/adr/` as `NNNN-kebab-title.md`, starting from the next free number. Copy `doc/adr/0000-template.md`. When asked to write an ADR, write it there without asking where it goes.

- **ADRs are historical records.** Once merged, an ADR is not edited to reverse, expand, or refine the decision it captured. If a later change supersedes it, write a **new** ADR that references the old one and mark the original `Status: Superseded by ADR-NNNN`. The original stays as written so the decision trail stays auditable.
- Write an ADR when a decision constrains future work: a Spark or Ray contract RayDP now depends on, an executor-lifecycle model, a compatibility boundary, a deliberate divergence from upstream. Skip it for routine fixes that follow an existing decision.
- **Do not point code comments at ADRs.** ADRs are immutable records; a comment referencing "see ADR-0003 §Alternatives" rots silently when the decision is superseded. State the contract in the comment itself.

## Releases

Releases are cut by hand. Nothing below is automated beyond the two publish workflows, so the sequence matters.

### Branch layout

`master` is the development line. Each minor version gets a long-lived release branch, and **the release branch determines which Spark versions that release supports** — `master` tracks the newest Spark line while release branches stay on older ones.

| Branch | RayDP version | PySpark requirement | Shim modules |
| ------ | ------------- | ------------------- | ------------ |
| `master` | `1.7.0.dev0` | `>= 4.0.0, <= 4.1.1` | `common`, `spark322`, `spark330`, `spark340`, `spark350`, `spark400`, `spark410` |
| `branch-1.6` | `1.6.5` | `>= 3.1.1, <= 3.5.7` | `common`, `spark322`, `spark330`, `spark340`, `spark350` |

Older `branch-*` lines exist but are historical. Note that `master` still builds the 3.x shim modules; it is the `pyspark` requirement in `python/setup.py`, not the shim list, that decides what pip will install.

Version numbers live in two places with different conventions. `master` computes the version in `python/setup.py` from `BASE_VERSION` plus a `.dev0` (or nightly) suffix, driven by `RAYDP_BUILD_MODE` and `RAYDP_PACKAGE_NAME`. A release branch replaces that logic with a literal `VERSION = "x.y.z"`. Do not port the release branch's literal back to `master`.

### Cherry-picking to a release branch

Work lands on `master` first, as a squash-merged PR — which is why history reads `Title (#NNN)`. It is then cherry-picked onto the active release branch:

```bash
git checkout branch-1.6
git cherry-pick <master-commit>
```

- **Plain `git cherry-pick`, not `-x`.** Existing release-branch commits carry no `(cherry picked from commit ...)` trailer; provenance is matched by the identical `Title (#NNN)` subject. Follow the convention so history stays uniform, and expect to identify a backport by subject line rather than SHA.
- **Never commit a fix directly to a release branch.** The only commits unique to a release branch are `Prepare for X.Y.Z release`. Everything else must exist on `master` first, or the next release silently regresses it.
- Skip anything whose Spark support does not apply to that branch — a fix touching `spark400`/`spark410` has no meaning on `branch-1.6`.

### Cutting the release

1. Cherry-pick everything intended for the release, then verify the branch: `./build.sh` and `pytest python/raydp/tests/`.
2. Commit the version bump as a single `Prepare for X.Y.Z release` commit. It touches every Maven version string — `core/pom.xml`, `core/agent/pom.xml`, `core/raydp-main/pom.xml`, `core/shims/pom.xml`, `core/shims/common/pom.xml` and every `core/shims/spark*/pom.xml` — plus `VERSION` in `python/setup.py` and `__version__` in `python/raydp/__init__.py`. Missing one leaves a wheel whose jars disagree with its metadata. **Bump these deliberately rather than with a global search-replace of the old version string:** the 1.6.4 and 1.6.5 commits each rewrote an unrelated `astunparse 1.6.x` line inside stored notebook output in `tutorials/raytrain_example.ipynb`, which is the tell that the shortcut was used. Nothing in `tutorials/` needs a version bump.
3. Tag that exact commit `vX.Y.Z`. Tags are lightweight, not annotated, and every published tag points at its `Prepare for` commit.
4. Run the **RayDP PyPI Release** workflow (`.github/workflows/pypi_release.yml`) manually, passing the tag. It checks out the tag, builds with `GITHUB_CI=1 ./build.sh` so `mvn verify` runs the JVM tests, installs the wheel, lints, runs the pytest suite and the `examples/` scripts, then publishes with `secrets.PYPI_API_TOKEN`. It is guarded by `github.repository_owner == 'ray-project'`, so it cannot publish from a fork.
5. Publish a GitHub Release on the tag. Every existing tag has one, using GitHub's generated `## What's Changed` list plus a line thanking outside contributors.

To open a new minor line, branch from `master` at the release point and bump `BASE_VERSION` on `master` to the next minor — `branch-1.6` was cut this way, followed by `bump to 1.7.0 (#374)` on `master`.

### Nightlies

`.github/workflows/pypi.yml` runs on a daily cron and on demand. It builds with `RAYDP_BUILD_MODE=nightly`, producing `<BASE_VERSION>bYYYYMMDD.dev0`, and publishes to PyPI as a pre-release — that is what `pip install --pre raydp` resolves. It needs no tag and no release branch. The legacy `raydp-nightly` package name is no longer updated.

## Commits and PRs

- Report bugs and request features through GitHub issues.
- Keep changes focused: one concern per PR, and update the affected docs in the same change.
- Verify against the real gate before claiming success: `./build.sh` then the relevant `pytest` file. Do not report a change as working on the basis of a compile alone.
- Never commit secrets or credentials. Do not run destructive git operations (force push, hard reset) without explicit confirmation.
- Do not attribute commits to Claude or any AI agent — no `Co-Authored-By`, no references in messages.

## Working Agreements

- **Verify, don't infer.** Plugin versions, shim coverage, and CI matrices are all readable in the repo. Check the file rather than reasoning from a name or a memory.
- **State what you actually ran.** If a build was offline, a check was skipped, or only one Spark version was exercised, say so.
- **Respect the upstream boundary.** This repo is a fork; when a fix belongs in Spark or Ray, say that instead of layering a workaround here.
