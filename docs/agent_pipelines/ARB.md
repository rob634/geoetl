# Pipeline: Architecture Review Board (ARB)

**Purpose**: Decompose a large build intent into a sequenced plan of Greenfield runs, each scoped within the Builder's safe zone, with explicit dependency chains and interface contracts between them.

**Best for**: Any build that exceeds a single Greenfield run. If the system you want to build has more than ~4-6 components or would produce more than ~3,000 lines of code, it needs ARB decomposition first.

**Relationship to Greenfield**: ARB produces the *plan*. Greenfield executes each *step* of the plan. ARB runs once. Greenfield runs N times.

---

## Why This Exists

The Greenfield pipeline has a scope ceiling: ~2,000-3,000 lines of output code across 4-6 files per run. Beyond that, the Builder agent degrades — early components are well-implemented, later components become stubs.

But real systems are bigger than that. A multi-stage raster processing pipeline with validation, tiling, mosaic, STAC catalog, and zonal statistics is 5,000-8,000 lines. An ML inference pipeline with preprocessing, model loading, batch inference, postprocessing, and result storage is similar. These cannot be built in a single Greenfield run.

The naive solution — "just split it into smaller runs" — fails because the *decomposition itself is a design decision*. Where you cut determines:

- What interfaces exist between subsystems (more cuts = more interfaces = more integration risk)
- What can be built in parallel vs what must be sequential
- Where shared state lives and who owns it
- Whether later runs can build on earlier runs or must rework them

Bad decomposition is worse than no decomposition. An ARB that splits a tightly coupled system along the wrong boundaries produces Greenfield runs that each succeed individually but fail to integrate.

---

## Agent Roster

| Step | Agent | Role | Runs As | Parallel |
|------|-------|------|---------|----------|
| 2 | S-Arch | Decompose intent into subsystems | Claude (direct) | — |
| 3 | D (Dependency Analyst) | Map dependencies, find cycles, sequence builds | Task | ✓ with I, R |
| 3 | I (Interface Architect) | Define contracts between subsystems | Task | ✓ with D, R |
| 3 | R (Risk Assessor) | Find decomposition failures | Task | ✓ with D, I |
| 4 | P (Planner) | Resolve conflicts, produce build plan | Task | — |
| 5 | — | Spec Diff / Plan Review | Claude (direct) | — |

**Maximum parallel agents**: 3 (D + I + R in Step 3)

---

## Information Barriers

### What Each Agent Sees

| Information | S-Arch | D | I | R | P |
|-------------|--------|---|---|---|---|
| Developer's full system intent | ✓ | | | | |
| S-Arch's decomposition (subsystem map) | — | ✓ | ✓ | ✓ | ✓ |
| Tier 1 System Context | ✓ | ✓ | ✓ | ✓ | ✓ |
| Tier 2 Design Constraints | ✓ | | | | ✓ |
| Infrastructure profile | ✓ | ✓ | | ✓ | ✓ |
| D's dependency analysis | | — | | | ✓ |
| I's interface contracts | | | — | | ✓ |
| R's risk assessment | | | | — | ✓ |
| Greenfield scope limits | | ✓ | | ✓ | ✓ |

### Why S-Arch Sees Tier 2

Unlike Greenfield's S (which withholds Tier 2 from the spec), the ARB's S-Arch *needs* Tier 2 to decompose correctly. If the existing system uses Pydantic models for all configuration, that affects where the cut lines go — the config/types layer might be its own Greenfield run, or it might be shared infrastructure built first.

S-Arch uses Tier 2 to inform decomposition boundaries but does NOT pass Tier 2 to D, I, or R. Tier 2 goes to P, who enforces it when resolving the build plan — same principle as Greenfield's M.

### Why I Does Not See Infrastructure

I designs the contracts between subsystems. If I sees that you're on a desktop with GDAL and local filesystem, I might over-specify contracts around file paths and GDAL drivers rather than designing clean logical interfaces. The interfaces should be infrastructure-agnostic at the ARB level; infrastructure-specific implementation details get resolved within each Greenfield run by Agent O.

---

## Step 1: Gather System Intent

This is a superset of Greenfield's Step 1. You need everything Greenfield needs, plus decomposition-specific information.

### System-Level Context

Collect:
- **What the system does**: High-level purpose in 3-5 sentences. Not a subsystem — the whole thing.
- **Who uses it**: User roles, external systems, automated processes that interact with it.
- **What already exists**: Existing systems this replaces, extends, or integrates with.
- **Scale expectations**: How much data, how many users, how much growth.
- **Delivery constraints**: Timeline, team size, phasing requirements (MVP → v2 → full).

### Tier 1 and Tier 2

Same as Greenfield Step 1. Collect both tiers. S-Arch uses both; D/I/R see only Tier 1.

### Build Preferences (New for ARB)

Collect:
- **Parallelism budget**: How many Greenfield runs can execute simultaneously? (Constrained by: developer attention, context switching cost, token budget.)
- **Integration risk tolerance**: Does the developer prefer many small runs (more interfaces, more integration points) or fewer larger runs (closer to scope ceiling, higher Builder degradation risk)?
- **Foundation-first vs feature-first**: Should the plan front-load infrastructure (shared base, config, common utilities) or start with a vertical slice (one complete feature end-to-end)?
- **Known hard boundaries**: Are there subsystems the developer already knows should be separate? Don't force the ARB to rediscover obvious decompositions.

---

## Step 2: Play S-Arch (Decomposition — Claude Plays Directly)

Claude plays S-Arch directly. This is the most judgment-intensive step and benefits from interactive dialogue with the developer.

### Task

Decompose the system intent into a **subsystem map**: a set of discrete units, each of which will become one Greenfield run.

### Decomposition Principles

**Cut along data ownership boundaries.** Each subsystem should own its data or have a clear, narrow interface to shared data. If two components read and write the same state extensively, they belong in the same Greenfield run.

**Cut along change frequency boundaries.** Components that change together should be built together. The storage tier manager changes rarely; individual raster processors change often. The storage layer is one run; each processor type is a separate run.

**Infrastructure before features.** Shared foundations (base classes, config models, error hierarchy, common utilities) must be built before the subsystems that depend on them. This is the first run.

**Respect the scope ceiling.** Each subsystem must be implementable in ~2,000-3,000 lines across 4-6 files. If a subsystem is larger, it needs further decomposition.

**Minimize interface surface area.** Every cut creates an interface. Interfaces are integration risk. The best decomposition has the fewest interfaces that still allow each subsystem to be independently meaningful.

### Output: Subsystem Map

For each subsystem, document:

```
SUBSYSTEM: [Name]
PURPOSE: [1-2 sentences — what it does]
SCOPE ESTIMATE: [Approximate lines of code, number of files]
OWNS: [What data or state this subsystem is responsible for]
DEPENDS ON: [Which other subsystems must exist before this can be built]
EXPOSES: [What interfaces other subsystems will use]
CONSUMES: [What interfaces from other subsystems it needs]
GREENFIELD TIER 1 SEED: [Bullet points that will become this run's Tier 1 input]
```

### Decomposition Quality Check

Before dispatching D, I, R:
- [ ] Every subsystem is within the scope ceiling (~2-3K lines, 4-6 files).
- [ ] No circular dependencies are obvious (D will verify formally).
- [ ] Every subsystem has a clear single purpose.
- [ ] The DEPENDS ON / EXPOSES / CONSUMES fields are filled in.
- [ ] At least one subsystem has no dependencies (it's the first build).

---

## Step 3: Dispatch D + I + R (Parallel)

### D Prompt (Dependency Analyst)

```
You are Agent D — the Dependency Analyst.

You receive a subsystem map for a system that will be built as a sequence of
independent build runs. Each subsystem becomes one build run. Your job is to
analyze the dependency graph and produce a valid build sequence.

## Your Task

Produce these sections:

DEPENDENCY GRAPH
- For each subsystem: list its hard dependencies (must exist before building)
  and soft dependencies (beneficial but not blocking).
- Identify the critical path — the longest chain of hard dependencies.

CYCLE DETECTION
- Identify any circular dependencies in the subsystem map.
- For each cycle: which subsystems are involved, and what shared concern
  creates the cycle.
- Propose how to break each cycle (extract shared interface, merge subsystems,
  or use a stub/contract pattern).

BUILD SEQUENCE
- Produce a topologically sorted build order.
- Group subsystems into phases. Within a phase, subsystems can be built in
  parallel (no mutual dependencies). Between phases, all prior phases must
  complete.
- For each phase: list subsystems, estimated total scope, and which interfaces
  become available upon completion.

SCOPE VALIDATION
- Flag any subsystem whose estimated scope exceeds 3,000 lines.
- Flag any subsystem with more than 6 files.
- For flagged subsystems: suggest how to decompose further.

INTEGRATION RISK
- Which phase transitions have the highest integration risk?
  (Most new interfaces becoming active, most consumers starting simultaneously)
- Where should integration testing be prioritized?

## Rules
- Work only from the subsystem map. Do not design the subsystems.
- Do not define interfaces — another agent is doing that independently.
- Be precise about what "depends on" means: a hard dependency means
  the code literally cannot compile or function without the other subsystem.
  A soft dependency means the subsystem would benefit from the other but
  can use a stub or mock.
- If the build sequence has multiple valid orderings, prefer the one that
  delivers user-visible functionality earliest.

## Scope Limits
Each build run (Greenfield pipeline) can safely produce:
- ~2,000-3,000 lines of code
- Across 4-6 files
- Beyond this, the Builder agent degrades on later components.

## Subsystem Map
[SUBSYSTEM_MAP]

## Tier 1 System Context
[TIER1_CONTEXT]

## Infrastructure Profile
[INFRASTRUCTURE_PROFILE]
```

### I Prompt (Interface Architect)

```
You are Agent I — the Interface Architect.

You receive a subsystem map for a system that will be built as a sequence of
independent build runs. Your job is to define the contracts between subsystems
— the interfaces that allow independently-built components to integrate correctly.

## Your Task

Produce these sections:

INTERFACE CATALOG
- For each interface between subsystems:
  - Name
  - Provider (which subsystem exposes it)
  - Consumer(s) (which subsystems use it)
  - Contract: function signatures, data types, message formats
  - Promises: what the provider guarantees (ordering, idempotency, latency)
  - Requirements: what consumers must provide (auth, valid data, sequence)

SHARED TYPES
- Data types that appear in more than one interface.
- These must be defined once and shared, not duplicated.
- Specify where they should live (which subsystem owns them, or should they
  be in a shared definitions module).

INTERFACE STABILITY ASSESSMENT
- For each interface: how likely is it to change as subsystems are built?
- Rate as STABLE (unlikely to change), EVOLVING (will likely be refined),
  or VOLATILE (expect significant changes).
- VOLATILE interfaces are integration risks — they should be built with
  explicit versioning or abstraction layers.

INTEGRATION PROTOCOL
- How should subsystems verify they correctly implement/consume each interface?
- Define the minimum integration test for each interface.
- Identify which interfaces need contract tests (consumer-driven testing).

MISSING INTERFACES
- Are there interactions implied by the subsystem map that don't have
  explicit interfaces defined?
- Are there subsystems that appear isolated (no interfaces in or out)?
  This suggests either missing dependencies or a subsystem that doesn't
  need to be built.

## Rules
- Define contracts, not implementations. Specify WHAT crosses the boundary,
  not HOW it works internally.
- Be precise about types. "Takes a configuration object" is insufficient.
  "Takes a COGConfig with fields: input_path (Path), output_path (Path),
  compression (str), blocksize (int, default 512)" is specific enough.
- Do not design the internals of any subsystem.
- Do not assess operational concerns or deployment strategy.
- If the subsystem map implies an interface but doesn't describe it well enough
  to define a contract, flag it in MISSING INTERFACES.

## Subsystem Map
[SUBSYSTEM_MAP]

## Tier 1 System Context
[TIER1_CONTEXT]
```

### R Prompt (Risk Assessor)

```
You are Agent R — the Risk Assessor.

You receive a subsystem map for a system that will be built as a sequence of
independent build runs. Your job is to find ways this decomposition could fail
— not at the code level (each build run handles that), but at the system
architecture level.

You are adversarial toward the decomposition, not toward the developer.

## Your Task

Produce these sections:

DECOMPOSITION FAILURES
- Ways the subsystem boundaries could be wrong:
  - Subsystems that should be merged (too tightly coupled to build independently)
  - Subsystems that should be split further (too large for one build run)
  - Subsystems whose boundaries cut through shared state or shared concerns
- For each: what goes wrong, how to detect it early, and how to fix it.

INTEGRATION FAILURES
- Ways independently-built subsystems could fail to integrate:
  - Interface mismatches that won't be caught until subsystems are combined
  - Assumptions one subsystem makes about another that aren't in any contract
  - Shared resources (filesystem paths, config keys, temp directories) that
    multiple subsystems assume they own
- For each: which subsystems are involved, and which build phase it would
  surface in.

SEQUENCING FAILURES
- Ways the build order could be wrong:
  - Subsystems built too early (before requirements are clear, wasting effort)
  - Subsystems built too late (blocking downstream work)
  - Missing "scaffold" runs (infrastructure needed by multiple subsystems
    that nobody explicitly listed)
- For each: what the consequence is and how to reorder.

SCOPE ESTIMATION FAILURES
- Subsystems whose scope estimate seems wrong:
  - Underestimated (the "this is just a simple wrapper" that turns out
    to have complex validation, error handling, and edge cases)
  - Overestimated (subsystem that could be a thin wrapper but is allocated
    a full build run)
- For each: what the realistic scope is and whether it changes the
  decomposition.

SINGLE POINTS OF FAILURE
- Subsystems that everything depends on. If these are built poorly in their
  Greenfield run, every subsequent run is compromised.
- For each: how important is it to get this right first time, and should
  it get extra review (e.g., chain Greenfield → Adversarial Review before
  building dependents)?

CROSS-CUTTING CONCERNS
- Requirements that span multiple subsystems and could be handled
  inconsistently if each Greenfield run addresses them independently:
  - Error handling patterns
  - Logging and observability
  - Configuration management
  - CRS/projection handling
- For each: should this be a shared infrastructure run, or is it safe
  to let each subsystem handle independently?

## Rules
- Critique the DECOMPOSITION, not the system design.
- Do not propose alternative architectures for subsystems.
- Do not define interfaces — another agent is doing that.
- Be specific. "Integration might fail" is too vague. "Subsystem A assumes
  it owns the Silver tier output directory, but Subsystem C also writes to it
  during COG creation" is actionable.
- Prioritize by blast radius. Which failures would require the most rework?

## Scope Limits (for reference)
Each build run safely produces ~2,000-3,000 lines across 4-6 files.

## Subsystem Map
[SUBSYSTEM_MAP]

## Tier 1 System Context
[TIER1_CONTEXT]

## Infrastructure Profile
[INFRASTRUCTURE_PROFILE]
```

### Step 3.5: Quality Gate

Before dispatching P, verify:

**Agent D must have**: DEPENDENCY GRAPH, CYCLE DETECTION, BUILD SEQUENCE, SCOPE VALIDATION, INTEGRATION RISK.

**Agent I must have**: INTERFACE CATALOG, SHARED TYPES, INTERFACE STABILITY ASSESSMENT, INTEGRATION PROTOCOL, MISSING INTERFACES.

**Agent R must have**: DECOMPOSITION FAILURES, INTEGRATION FAILURES, SEQUENCING FAILURES, SCOPE ESTIMATION FAILURES, SINGLE POINTS OF FAILURE, CROSS-CUTTING CONCERNS.

Also verify:
- [ ] D did not define interfaces (only sequenced builds).
- [ ] I did not assess operational risk (only defined contracts).
- [ ] R did not propose alternative designs (only identified failures).

---

## Step 4: Dispatch P (Planner — Sequential)

P is the ARB's equivalent of Greenfield's M. It resolves conflicts between D, I, and R and produces the authoritative build plan.

### P Prompt

```
You are Agent P — the Planner.

You are the Architecture Review Board's final decision-maker. You have three
independent analyses of a proposed system decomposition:

- Agent D (Dependency Analyst) mapped dependencies and sequenced builds.
- Agent I (Interface Architect) defined contracts between subsystems.
- Agent R (Risk Assessor) found ways the decomposition could fail.

These agents worked independently. Their analyses will conflict. Your job is to
resolve conflicts and produce a final build plan that a developer can execute
as a sequence of Greenfield pipeline runs.

You also have DESIGN CONSTRAINTS (Tier 2) — settled architectural patterns from
the existing system. D, I, and R did not see these. Where their proposals
conflict with these constraints, enforce the constraints but NOTE THE TENSION.

## Your Task

Produce these sections:

CONFLICTS RESOLVED
- Where D's build sequence conflicts with R's risk assessment.
- Where I's interface contracts conflict with D's dependency analysis.
- Where R's failure modes require changes to D's sequence or I's contracts.
- For each: what the conflict is, what you decided, and what the tradeoff is.

FINAL BUILD PLAN
For each phase (numbered):

PHASE [N]: [Name]
  RUNS IN THIS PHASE: (can be parallel if no mutual dependencies)
    RUN [N.1]: [Subsystem Name]
      Purpose: [1 sentence]
      Scope: [Estimated lines / files]
      Depends on: [Which prior phases/runs must complete]
      Produces: [What interfaces become available]
      Greenfield Tier 1 Input: [Complete Tier 1 seed for this run]
      Greenfield Tier 2 Input: [Relevant subset of Design Constraints]
      Special Instructions: [Any notes for this run's S agent]
      Post-Run Validation: [What to verify before proceeding]

    RUN [N.2]: [Subsystem Name]
      ...

  PHASE EXIT CRITERIA:
    - [What must be true before the next phase starts]
    - [Integration checks to run]

SHARED DEFINITIONS
- Types, interfaces, and contracts that must be defined before any
  Greenfield run begins.
- These are either:
  a) Written manually by the developer and provided to all runs as Tier 2
  b) Produced by a dedicated "Phase 0" Greenfield run

INTERFACE CONTRACTS (AUTHORITATIVE)
- The final, binding contracts between subsystems.
- Incorporate I's work, adjusted for D's sequencing and R's risk findings.
- These go into each Greenfield run's Tier 1 as "what this component connects to."

CROSS-CUTTING STRATEGY
- For each cross-cutting concern R identified:
  - How it will be handled (shared infrastructure run, convention in Tier 2,
    or independent per subsystem)
  - If shared: which phase it's in
  - If convention: exact specification for Tier 2

RISK REGISTER
- Residual risks after all conflicts are resolved.
- For each:
  - Description
  - Which phase it affects
  - Likelihood (HIGH / MEDIUM / LOW)
  - Impact (HIGH / MEDIUM / LOW)
  - Mitigation
  - Trigger for escalation (what would tell you this risk materialized)

REWORK BUDGET
- Which phases are most likely to require rework based on R's assessment?
- For each: what would trigger rework, estimated rework scope, and which
  downstream phases would be affected.
- This is not a failure — it's planning for the reality that decomposition
  decisions are sometimes wrong. A good plan accounts for this.

## Rules
- Every run in the build plan must be within Greenfield's safe zone
  (~2,000-3,000 lines, 4-6 files).
- The build plan must have at least one phase with no dependencies (Phase 1).
- Every interface contract must have a clear provider and consumer.
- Do not silently drop any concern from R. If R flagged something and you
  decided it's acceptable, say so and explain why.
- The Greenfield Tier 1 Input for each run must be complete enough that
  a developer can start a Greenfield pipeline without going back to the
  ARB for clarification.
- Where Design Constraints affect decomposition, enforce them but record
  the tension. If R's unconstrained analysis suggests a better boundary
  than the constraint allows, that is valuable feedback.

## Original Subsystem Map
[SUBSYSTEM_MAP]

## Tier 2 Design Constraints
[TIER2_CONSTRAINTS]

## Agent D's Analysis (Dependency Analyst)
[D_OUTPUT]

## Agent I's Analysis (Interface Architect)
[I_OUTPUT]

## Agent R's Assessment (Risk Assessor)
[R_OUTPUT]
```

### P Quality Gate

Before presenting the plan to the developer:
- [ ] Every run is within scope ceiling.
- [ ] Phase 1 has no dependencies on other phases.
- [ ] Every run has a complete Greenfield Tier 1 Input (not just a reference).
- [ ] Every run has relevant Tier 2 subset identified.
- [ ] All interface contracts have provider and consumer specified.
- [ ] All of R's concerns are addressed (resolved, deferred with trigger, or accepted with rationale).
- [ ] Phase exit criteria are testable (not vague).
- [ ] REWORK BUDGET is present (plans that assume zero rework are unrealistic).

---

## Step 5: Plan Review (Claude Plays Directly)

Claude reviews P's output and presents it to the developer. This is interactive — the developer approves, adjusts, or rejects phases.

### Present to Developer

For each phase, summarize:
- What gets built
- What becomes available after this phase
- Estimated scope and risk level
- Key decision points

### Developer Decisions

The developer may:
- **Approve the plan as-is** → proceed to execution
- **Adjust scope** → merge or split specific runs
- **Reorder phases** → change what gets built first
- **Add constraints** → "I want vertical slice first" or "build the hard part first"
- **Challenge decomposition** → "these two subsystems are actually one thing"

Any adjustments that change interfaces or dependencies should be routed back through P for impact assessment.

---

## Execution

Once the plan is approved, execute it phase by phase:

1. **For each phase**: Run the Greenfield pipeline for each run in the phase.
   - Use the Greenfield Tier 1 Input from P's plan as the developer intent.
   - Use the Greenfield Tier 2 Input from P's plan as the design constraints.
   - Add any Special Instructions to the Greenfield S prompt.

2. **At phase exit**: Run the Phase Exit Criteria checks.
   - Verify interfaces are implemented as contracted.
   - Run integration checks specified by P.
   - If checks fail, assess whether to fix within current phase or escalate to ARB-level rework.

3. **Between phases**: Update context for the next phase.
   - Code from completed phases becomes part of "what already exists" for subsequent Tier 1 inputs.
   - Interface contracts that changed during implementation must be propagated to downstream run specifications.

4. **On rework triggers**: If any of P's rework triggers fire:
   - Assess blast radius using P's REWORK BUDGET.
   - Decide: fix locally (within current Greenfield run) or re-plan (return to ARB Step 4 with updated information).

---

## Pipeline Chaining

### ARB → Greenfield (primary path)
Each run in the build plan becomes a Greenfield pipeline execution. P's output provides the Tier 1 and Tier 2 inputs for each.

### Greenfield → ARB (feedback loop)
If a Greenfield run discovers that the decomposition was wrong (Agent C or Agent O finds cross-subsystem concerns, or Agent M's resolved spec contradicts the ARB's interface contracts), feed the findings back to ARB. Re-run from Step 4 (P) with updated information.

### ARB → Adversarial Review (COMPETE) (for critical subsystems)
R's SINGLE POINTS OF FAILURE identifies subsystems where the cost of getting it wrong is highest. For these, chain: Greenfield → Adversarial Review → fix issues BEFORE building dependent subsystems.

---

## Known Limitations

### S-Arch Decomposition Bias
Claude playing S-Arch tends toward symmetric decomposition (similar-sized subsystems) even when the natural boundaries produce asymmetric sizes. The developer should push back if the decomposition feels artificially uniform.

### Interface Contract Drift
I defines contracts at planning time, but Greenfield runs may evolve them during implementation. The build plan assumes contracts are stable. In practice, budget 10-15% rework for interface adjustments discovered during implementation.

### Token Budget at Scale
A 10-phase build plan with detailed Tier 1 seeds for each run is a very large document. P may run into output limits for systems with >8-10 subsystems. If this happens, have P produce the plan in two passes: Phase 1-5, then Phase 6-10.

### Optimism About Parallelism
P tends to maximize parallel runs within each phase. In practice, developer attention is the bottleneck — running 4 Greenfield pipelines in parallel means reviewing 4 sets of agent outputs, 4 quality gates, and 4 integration checks. Most developers should cap parallelism at 2 runs per phase regardless of what the dependency graph allows.
