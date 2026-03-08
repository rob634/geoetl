# Pipeline 3: Greenfield

**Purpose**: Design and build new code from intent, using adversarial agents to stress-test the design before any code is written. The pipeline ensures that architecture, edge cases, and operational reality are all resolved before the first line of code exists.

**Best for**: New subsystems, new features, new services. When you know *what* you want to build but want the design to survive contact with reality before you commit to code.

---

## Agent Roles

| Agent | Role | Runs As | Input |
|-------|------|---------|-------|
| S | Formalize intent into a spec | Claude (no subagent) | Developer's description of what they want to build |
| A | Design the system optimistically | Task (parallel with C and O) | Spec only |
| C | Find what the spec does not cover | Task (parallel with A and O) | Spec only |
| O | Assess operational and infrastructure reality | Task (parallel with A and C) | Spec + infrastructure profile |
| M | Resolve conflicts between A, C, and O | Task (sequential) | All three outputs + spec |
| B | Write the code | Task (sequential) | Resolved spec from M |
| V | Reverse-engineer the code and compare to spec | Task (sequential) | Code only (no spec) |

**Maximum parallel agents**: 3 (A + C + O)

---

## Flow

```
Developer intent
    |
    S (Claude plays this role)
    Formalize intent into contracts, boundaries, and requirements
    |
    +--- A (Task) ------+--- C (Task) ------+--- O (Task) ------+  [parallel]
    |    Advocate         |    Critic          |    Operator        |
    |    Design it        |    Break it        |    Run it          |
    +--------------------+--------------------+--------------------+
                         |
                  M (Task)                                          [sequential]
                  Resolve conflicts, produce final spec
                         |
                  B (Task)                                          [sequential]
                  Write code against resolved spec
                         |
                  V (Task)                                          [sequential]
                  Reverse-engineer code, diff against original spec
```

---

## Step 1: Gather Intent (Two-Tier Briefing)

Most greenfield work is building a new component inside an existing system. The briefing must separate two kinds of information, because they go to different agents at different stages.

### Tier 1: System Context

This describes **what exists around the thing we are building**. Facts about the environment. Not design decisions.

Collect:
- **What this component does**: One to three sentences describing the purpose.
- **What it connects to**: Upstream systems that send data or requests. Downstream systems that receive data or requests. Name the specific services.
- **What already exists**: Other components, shared libraries, repository layers, storage tiers that this component will interact with. Describe what they do, not how to use them.
- **What it must guarantee**: Hard requirements that cannot be compromised.
- **Infrastructure profile**: Runtime environment, resource limits, scaling model, known constraints.

Tier 1 goes into the spec that A, C, and O all see. It is like giving an architect the building code and the lot survey — it constrains the solution space without dictating the design.

**geoetl Tier 1 example sketch:**
```
What: Zonal statistics engine that computes per-polygon raster summaries
Connects to: raster/cog.py (COG creation), vector/ (polygon validation),
  batch/raster_ops.py (parallel execution), storage/manager.py (tier management)
Already exists: Bronze→Silver→Gold storage tiers, spawn-context multiprocessing
  pool, COG creation pipeline, vector validation (8-stage)
Must guarantee: No data loss during partial failures, correct CRS handling
Infrastructure: Ubuntu desktop, RTX 4080, Python 3.10+, GDAL/rasterio,
  spawn-context multiprocessing (GDAL not fork-safe), local filesystem
```

### Tier 2: Design Constraints

This describes **how we do things here**. Architectural decisions that are already settled. Patterns that the new component must follow to fit into the existing system.

Collect:
- **Settled architectural patterns**: How configuration works (Pydantic v2 models), how storage tiers work, how multiprocessing is done (spawn context).
- **Integration rules**: Which existing modules or layers must be used for specific operations (example: "all raster I/O goes through rasterio, vector I/O through geopandas/fiona").
- **Anti-patterns**: Things that have been tried and failed, or things that are explicitly forbidden in this codebase.
- **Conventions**: Data contract formats (Pydantic models), naming conventions, logging standards.

Tier 2 is held back from A, C, and O. It goes ONLY to M (Step 4) and B (Step 5).

**geoetl Tier 2 example sketch:**
```
Config: Pydantic v2 models in config.py (StorageTier, RasterType, COGConfig, etc.)
Raster I/O: Always through rasterio; COG creation via rio-cogeo
Vector I/O: Always through geopandas/fiona
Multiprocessing: Must use spawn context (not fork) for GDAL/rasterio safety
Storage: Bronze→Silver→Gold tier pattern via storage/manager.py
Errors: GeoETLError hierarchy in exceptions.py
Compression: Raster-type-specific (JPEG for RGB, DEFLATE for DEM, etc.)
Secrets: In .env via python-dotenv, never hardcoded
```

### Why the Tiers Are Separated

If A sees "raster I/O goes through rasterio," A will design around that constraint without questioning whether it is the right approach. If C sees it, C will not challenge it. The whole point of the parallel agents is that A designs freely and C critiques freely.

M then applies the Design Constraints during conflict resolution. If A's ideal design suggests a different pattern from your existing one, that is valuable signal — it tells you whether your existing pattern is serving you well or creating friction. M notes this tension even when enforcing the existing constraint.

### Completeness Check

If any of the following are missing, ask the developer before proceeding:
- Tier 1: What the component does, what it connects to, infrastructure profile.
- Tier 2: At least the settled config pattern, data I/O pattern, and multiprocessing pattern.

Incomplete intent leads to a spec full of assumptions, which leads to agents arguing about things that could have been settled upfront.

## Step 2: Play S (Spec Writer — No Subagent)

Claude plays S directly. Transform the developer's Tier 1 System Context into a formal spec.

**Important**: S uses ONLY Tier 1 information. Tier 2 Design Constraints are held back for M and B. Do not include settled architectural patterns, integration rules, or internal conventions in the spec. The spec describes *what* the component must do, not *how* the existing system expects it to be done.

The spec must include ALL of the following sections:

**PURPOSE**
- What this subsystem does, in two to four sentences.
- What problem it solves and for whom.

**BOUNDARIES**
- What is IN scope for this subsystem.
- What is explicitly OUT of scope (handled elsewhere or not handled at all).

**CONTRACTS**
- For each interface this subsystem exposes:
  - Input: What it accepts (types, formats, required vs optional).
  - Output: What it returns (types, formats, error shapes).
  - Promises: What the caller can rely on (ordering, idempotency, latency).
  - Requirements: What the caller must provide (authentication, valid data, sequence).

**INVARIANTS**
- Conditions that must always be true while this subsystem is running.
- Example: "Every raster that enters the pipeline must eventually reach a terminal state (completed, failed, or skipped) within the timeout window."

**NON-FUNCTIONAL REQUIREMENTS**
- Performance targets (latency, throughput, concurrency).
- Reliability targets (what happens during partial failure?).
- Security requirements (authentication, authorization, data sensitivity).
- Observability requirements (what must be logged, monitored, or alerted on?).

**INFRASTRUCTURE CONTEXT**
- Runtime environment and its constraints.
- Paste or summarize the infrastructure profile from Tier 1.

**EXISTING SYSTEM CONTEXT**
- What other components exist that this subsystem will interact with.
- Describe their interfaces at the level A, C, and O need to reason about them.
- Do NOT include Tier 2 Design Constraints (how to use those components).

**OPEN QUESTIONS**
- Anything S is unsure about. These become priority targets for Agent C.

### Spec Quality Check

Before dispatching A, C, and O, verify:
- Every section is filled in (not left as placeholder text).
- BOUNDARIES clearly separate what is in scope from what is out of scope.
- CONTRACTS specify types and shapes, not just descriptions.
- At least one INVARIANT is stated.

If the spec is thin in any section, strengthen it before proceeding. A weak spec wastes agent tokens on obvious questions.

## Step 3: Dispatch A + C + O (Parallel)

Send all three Task calls in a single message so they run in parallel.

### A Prompt (Advocate)

```
You are Agent A — the Advocate.

You receive a spec for a subsystem that does not exist yet. Your job is to design
the best possible architecture for this subsystem.

You are optimistic. Assume the spec is complete and correct. Design for the spec
as written.

## Your Task

Produce these sections:

COMPONENT DESIGN
- List every component or module this subsystem needs.
- For each: its single responsibility, what it depends on, what depends on it.

INTERFACE CONTRACTS
- For each component: exact function signatures, parameter types, return types.
- For boundaries between components: who calls whom, with what data.

DATA FLOW
- Trace the path of data from entry to exit.
- Identify every transformation, validation, and storage step.
- Note where data is copied vs referenced.

GOLDEN PATH
- Walk through the complete happy path from start to finish.
- Be specific: "User sends X, component A does Y, passes Z to component B..."

STATE MANAGEMENT
- All mutable state: where it lives, who writes it, who reads it.
- State transitions: what events cause state changes.

EXTENSION POINTS
- Where can new behavior be added without modifying existing code?
- What patterns enable this (registry, plugin, strategy, event)?

DESIGN RATIONALE
- For each major design decision: what you chose, what you rejected, and why.

## Rules
- Design for simplicity. Choose the simplest approach that satisfies the spec.
- Do not add features or capabilities beyond what the spec requires.
- Do not address failure modes, operational concerns, or edge cases.
  Other agents are handling those independently. You do not know what they
  are looking at.
- Be specific enough that a developer could start coding from your design.

## Spec
[Paste the spec from Step 2]
```

### C Prompt (Critic)

```
You are Agent C — the Critic.

You receive a spec for a subsystem that does not exist yet. Your job is to find
everything the spec does NOT address — ambiguities, missing edge cases, unstated
assumptions, and scenarios that will surprise the development team.

You are adversarial toward the spec, not toward the developer. Your goal is to
make the spec stronger by finding its weaknesses.

## Your Task

Produce these sections:

AMBIGUITIES
- Places where the spec could be interpreted in more than one way.
- For each: the two or more possible interpretations, and why it matters.

MISSING EDGE CASES
- Scenarios the spec does not address that could realistically occur.
- For each: what happens, how likely it is, and how severe the impact would be.

UNSTATED ASSUMPTIONS
- Things the spec assumes but does not say explicitly.
- For each: rate as SAFE (reasonable assumption) or RISKY (could be wrong).

SPEC GAPS
- Requirements that a production system would need but the spec does not mention.
- Examples: error handling strategy, retry policy, timeout values, logging
  requirements, graceful shutdown behavior, backwards compatibility.

CONTRADICTIONS
- Places where two parts of the spec disagree or are incompatible.

OPEN QUESTIONS
- Questions that the spec's own "Open Questions" section raised, plus any
  new questions you identified.
- Rank by impact: which questions, if answered wrong, would cause the most damage?

## Rules
- Critique the SPEC, not a design. You have not seen any design.
  Another agent is designing the system independently.
- Do not propose solutions. Only identify problems.
- Be specific. "The spec doesn't address error handling" is too vague.
  "The spec doesn't say what happens when the upstream service returns a 429
  during a batch operation" is specific enough to act on.
- Prioritize by impact. Which gaps would cause the most damage if left unresolved?

## Spec
[Paste the spec from Step 2]
```

### O Prompt (Operator)

```
You are Agent O — the Operator.

You receive a spec for a subsystem that does not exist yet, along with a description
of the infrastructure it will run on. Your job is to assess the operational reality:
what will this system need to be deployed, monitored, and kept running?

You think about what happens at 3 in the morning when nobody is awake. You think about
what happens six months from now when the original developer is on a different project.

## Your Task

Produce these sections:

INFRASTRUCTURE FIT
- How well does this spec fit the described infrastructure?
- Where does the infrastructure impose constraints that the spec does not acknowledge?
- Are there infrastructure capabilities the spec could use but does not mention?

DEPLOYMENT REQUIREMENTS
- What does deploying this subsystem require?
  (Configuration, secrets, conda dependencies, GDAL drivers, GPU setup)
- What is the rollback plan if deployment fails?
- Can this be deployed without disrupting existing processing?

FAILURE MODES
- How will this subsystem fail in practice? List realistic scenarios.
- For each: trigger, detection method, blast radius, recovery path.
- Focus on infrastructure-level failures, not application logic bugs.
  (GPU OOM, disk full, GDAL driver crashes, API rate limits, corrupt rasters,
  spawn-context worker failures, temp file accumulation)

OBSERVABILITY
- What must be logged for debugging?
- What metrics indicate health?
- What alerts should fire, and at what thresholds?
- Can an operator diagnose a problem from logs alone?

SCALING BEHAVIOR
- How does this subsystem behave under increasing load?
- Where is the first bottleneck?
- Is scaling automatic, manual, or not possible?

OPERATIONAL HANDOFF
- What does a new operator need to know to maintain this system?
- What documentation must exist before this ships?
- What runbooks are needed?

COST MODEL (if applicable)
- What are the primary cost drivers?
- How does cost scale with usage?
- Are there any cost traps (operations that are cheap at low volume but expensive
  at high volume)?

## Rules
- Do not design the system. Another agent is doing that independently.
- Do not evaluate application logic or business rules.
- Focus on what happens AFTER the code is written: deployment, monitoring,
  maintenance, failure, and recovery.
- Use the infrastructure profile to be specific. Do not give generic cloud advice.
  Reference actual hardware limits, library constraints, and environment details.

## Spec
[Paste the spec from Step 2]

## Infrastructure Profile
[Paste the infrastructure profile from Step 1]
```

## Step 3.5: Quality Gate

Before dispatching M, verify all three agents produced the requested sections:

- A must have: COMPONENT DESIGN, INTERFACE CONTRACTS, DATA FLOW, GOLDEN PATH, STATE MANAGEMENT, EXTENSION POINTS, DESIGN RATIONALE.
- C must have: AMBIGUITIES, MISSING EDGE CASES, UNSTATED ASSUMPTIONS, SPEC GAPS, CONTRADICTIONS, OPEN QUESTIONS.
- O must have: INFRASTRUCTURE FIT, DEPLOYMENT REQUIREMENTS, FAILURE MODES, OBSERVABILITY, SCALING BEHAVIOR, OPERATIONAL HANDOFF.

If any agent returned unstructured prose or skipped sections, re-dispatch with format instructions emphasized.

Also verify:
- C did NOT propose solutions (only identified problems).
- O did NOT design architecture (only assessed operational concerns).
- A did NOT address failure modes (only designed the happy path).

If any agent crossed into another's scope, note this for M — scope violations are themselves a signal that the spec boundary was unclear.

## Step 4: Dispatch M (Mediator)

M is the most important agent in this pipeline. M receives three perspectives that will inevitably conflict and must produce a single coherent blueprint.

### M Prompt

```
You are Agent M — the Mediator.

You have three independent analyses of a spec for a subsystem that does not exist yet:

- Agent A (Advocate) designed the system optimistically, assuming the spec is correct.
- Agent C (Critic) found everything the spec does not address.
- Agent O (Operator) assessed what it takes to deploy and run this in production.

These three agents worked independently. They did not see each other's output.
Their analyses will conflict. Your job is to resolve those conflicts and produce
a final spec that the Builder agent can code against.

You also have DESIGN CONSTRAINTS — settled architectural decisions from the existing
system that the new component must follow. A, C, and O did not see these constraints.
Where their proposals conflict with these constraints, enforce the constraints but
NOTE THE TENSION. If an agent's unconstrained analysis suggests a better pattern
than the existing constraint, record that observation in DESIGN TENSIONS — it is
valuable feedback even when the constraint is enforced.

## Your Task

Produce these sections:

CONFLICTS FOUND
- Where A's design violates O's infrastructure constraints.
  For each: what A proposed, what O's constraint is, and your resolution.
- Where C's edge cases require changes to A's design.
  For each: what C found, how it affects A's design, and your resolution.
- Where O's operational requirements add complexity that A did not anticipate.
  For each: what O requires, what it costs, and whether it is justified.
- Where C's concerns are already addressed by O's infrastructure, or made worse by it.

DESIGN TENSIONS
- Where an agent's unconstrained proposal conflicts with a Tier 2 Design Constraint.
  For each: what the agent proposed, what the existing constraint requires,
  which one you enforced, and whether the tension suggests the existing
  constraint should be revisited in the future.
- This section may be empty if no tensions exist. That is fine.

RESOLVED SPEC
- The final, unified spec that incorporates A's design, addresses C's concerns,
  satisfies O's operational requirements, AND conforms to the Design Constraints.
- Organize by component. For each component:
  - Responsibility (one sentence)
  - Interface (function signatures with types)
  - Error handling strategy (what exceptions, what recovery)
  - Operational requirements (logging, monitoring, configuration)
  - Integration notes (which existing modules or layers to use, from Design Constraints)
- This section must be detailed enough that a developer can code from it
  without asking follow-up questions.

DEFERRED DECISIONS
- Issues raised by C or O that are real but do not need to be solved in the
  first version. For each: what it is, why it can wait, and what would trigger
  revisiting it.

RISK REGISTER
- Residual risks after all conflicts are resolved. For each:
  - Description
  - Likelihood (HIGH / MEDIUM / LOW)
  - Impact (HIGH / MEDIUM / LOW)
  - Mitigation (what reduces the risk, even if it does not eliminate it)

## Rules
- Every resolution must explain the tradeoff. Do not silently drop a concern.
- If A's design and O's constraints are incompatible, prefer O's constraints.
  Infrastructure limits are not negotiable; designs can be changed.
- If A's design and a Tier 2 Design Constraint are incompatible, enforce the
  Design Constraint but record the tension in DESIGN TENSIONS.
- If C raised a concern that neither A nor O addressed, you must address it.
- The RESOLVED SPEC must be self-contained. The Builder should not need to read
  A, C, or O's outputs.

## Original Spec
[Paste the spec from Step 2]

## Tier 2 Design Constraints
[Paste the Design Constraints collected in Step 1]

## Agent A's Design (Advocate)
[Paste A output]

## Agent C's Analysis (Critic)
[Paste C output]

## Agent O's Assessment (Operator)
[Paste O output]
```

### M Quality Check

Before dispatching B, verify:
- CONFLICTS FOUND explicitly addresses disagreements between agents, not just summaries.
- DESIGN TENSIONS is present (even if empty — M must acknowledge the section).
- RESOLVED SPEC includes function signatures, error handling, and integration notes.
- DEFERRED DECISIONS has a "trigger for revisiting" for each item.
- No concern from C was silently dropped.
- All Tier 2 Design Constraints are reflected in the RESOLVED SPEC.

## Step 5: Dispatch B (Builder)

### B Prompt

```
You are Agent B — the Builder.

You receive a resolved spec for a subsystem that does not exist yet. The spec has
already been through adversarial review: an architect designed it, a critic stress-tested
it, an operator assessed it, and a mediator resolved all conflicts.

You also receive Design Constraints — settled patterns from the existing system.
The resolved spec already accounts for these, but you have them for reference.

Your job is to write the code.

## Your Task

Write production-quality code that implements the RESOLVED SPEC exactly.

## Requirements

TRACEABILITY
- Every function must have a docstring that states which spec requirement it implements.
- Every error handler must state which concern (from the Critic or Operator) it addresses.
- Use this format in docstrings:
  """
  [Brief description]

  Spec: [Which requirement or component from the resolved spec]
  Handles: [Which concern from C or O, if applicable]
  """

CODE ORGANIZATION
- One file per component unless the component is trivially small (under 50 lines).
- Clear module boundaries that match the component boundaries in the spec.
- Imports grouped: standard library, third-party, internal.

INTEGRATION
- Follow the integration notes in the resolved spec for each component.
- Use the existing modules and layers specified in the Design Constraints.
- Do not create new patterns for operations that have existing patterns
  (configuration, data access, error handling, etc.).

ERROR HANDLING
- Follow the error handling strategy specified in the resolved spec for each component.
- Never swallow exceptions silently.
- Every except block must either handle the error meaningfully or re-raise.

OBSERVABILITY
- Include the logging specified in the resolved spec.
- Log at appropriate levels: DEBUG for flow tracing, INFO for business events,
  WARNING for recoverable issues, ERROR for failures.

CONFIGURATION
- No hardcoded values for anything that could change between environments.
- Use environment variables or configuration objects as specified in the resolved spec.

## Rules
- Implement what the spec says. Do not add features, optimizations, or abstractions
  that the spec does not call for.
- If the spec is ambiguous on any point, choose the simpler interpretation
  and add a comment noting the ambiguity.
- Write code that a developer who has never seen the spec can understand.
  The code should be self-documenting through names, structure, and docstrings.

## Resolved Spec
[Paste M's RESOLVED SPEC section]

## Tier 2 Design Constraints
[Paste the Design Constraints collected in Step 1]

## Risk Register
[Paste M's RISK REGISTER section — B should be aware of known risks]

## Design Tensions (for awareness only)
[Paste M's DESIGN TENSIONS section — B should know where existing patterns
were enforced over potentially better alternatives, in case it affects
implementation choices]
```

## Step 6: Dispatch V (Validator)

V receives ONLY the code. V does NOT receive the spec. This is the same principle as Agent R in the Reflexion Agent pipeline: the gap between what V infers and what S intended reveals whether the code expresses its purpose clearly.

### V Prompt

```
You are Agent V — the Validator.

You receive source code with NO external context. You have not been told what this
code is for, what system it belongs to, or what its requirements are.

A spec exists for this code, but you have not seen it. Your analysis will be compared
against that spec to find gaps.

## Your Task

Produce these sections:

INFERRED PURPOSE
- What does this code do? Describe it as if explaining to a new team member.

INFERRED CONTRACTS
- For each interface: what it accepts, what it returns, what it promises.

INFERRED INVARIANTS
- What must be true for this code to work correctly?

INFERRED BOUNDARIES
- What is in scope for this code? What does it explicitly NOT do?

CONCERNS
- Anything in the code that seems incomplete, inconsistent, or unclear.
- Anything that looks like it was intended to handle a specific scenario
  but might not handle it correctly.

QUALITY ASSESSMENT
- Is the code self-documenting? Could a new developer understand it without the spec?
- Are the error handling patterns consistent across components?
- Is the logging sufficient for debugging production issues?
- Rate overall: PRODUCTION READY / NEEDS MINOR WORK / NEEDS SIGNIFICANT WORK

## Rules
- Work ONLY from the code. Do not guess about external context.
- Be specific. Cite file names, function names, and line numbers.

## Code
[Paste all code from Agent B — do NOT include any spec or context]
```

## Step 7: Spec Diff

After V returns, Claude compares V's output against S's original spec. This is not a subagent — Claude does this directly.

Produce:

**MATCHES**
- Where V's inferred purpose matches S's spec. These areas are well-implemented:
  the code says what it means.

**GAPS**
- Where S's spec includes something that V did not infer from the code.
  These are implementation gaps (code does not fully implement the spec)
  or documentation gaps (code implements it but does not express it clearly).

**EXTRAS**
- Where V inferred something that is NOT in S's spec. These are either:
  - Scope creep: B added something the spec did not call for.
  - Undocumented behavior: the code does something useful but not specified.
  - Emergent behavior: side effects of the implementation that were not designed.

**VERDICT**
- Is the code ready to use? If not, what must change?
- List specific files and functions that need revision.

## Step 8: Save Results

Write the full report to `docs/agent_reviews/GREENFIELD_[SUBSYSTEM].md`. Include:
- S's spec
- A, C, O outputs (for traceability)
- M's resolved spec
- B's code
- V's analysis
- The spec diff

Save the code files separately in the appropriate project directory.

---

## Chaining to Other Pipelines

### Greenfield → Adversarial Review (COMPETE)

After the code is written and passes the V-check, run an Adversarial Review on the new code. This catches implementation-level issues that the design-level agents did not anticipate. Use the resolved spec as developer context for Omega's scope selection.

### Greenfield → Reflexion

Run the Reflexion pipeline on the generated code to find fault-level vulnerabilities. Agent R's analysis should closely match Agent V's — if it does not, the code is harder to understand than expected. Use M's Risk Register as developer context for Agent F.

### Adversarial Review (COMPETE) → Greenfield

If the Adversarial Review finds that an existing subsystem needs to be replaced rather than patched, use Greenfield to design the replacement. Feed the Adversarial Review's findings into S as constraints and anti-patterns to avoid.

---

## Known Limitations & Lessons Learned

### 1. Builder Agent Output Budget Collapse

**Problem**: Agent B receives the entire resolved spec (~1100 lines from M) and must produce all code in a single session. For large subsystems, B runs out of output capacity before completing all components. The result is a **quality gradient** — early components are well-implemented while later components degrade to stubs.

**Guideline**: If M's resolved spec implies >3,000 lines of implementation code, the Builder will likely produce stubs for later components. The safe zone is ~2,000-3,000 lines across 4-6 files.

### 2. Validator Blind Spot on Feature Completeness

**Problem**: Agent V reverse-engineers from code only (by design — no spec). This means V treats whatever B built as the intended scope. If B produced a stub, V validates the stub as correct. V catches **bugs in what exists** but cannot catch **features that were never built**.

The Step 7 Spec Diff (Claude comparing V's output against S's spec) should catch this, but pay attention to feature-level completeness, not just architectural compliance.

### 3. Scope Ceiling

**Observed safe zone**: ~2,000-3,000 lines of output code across 4-6 files.

**Guideline**: If M's resolved spec implies >3,000 lines, consider using the ARB pipeline to decompose into multiple GREENFIELD runs.

### Recommended Mitigations

#### Option A: Narrower Scope Per Run

Split large subsystems into multiple GREENFIELD runs, each producing one module.

**Pro**: Each B agent has a focused, completable scope.
**Con**: More runs = more tokens total, and cross-module consistency must be enforced via shared Design Constraints.

#### Option B: Multi-Pass Builder

Replace single Agent B with a B₁ → B₂ → ... sequence:
- **B₁**: Write infrastructure (shared base, configuration, types)
- **B₂**: Write Module 1 (receives B₁'s code as context + relevant spec section)
- **B₃**: Write Module 2 (receives B₁ + B₂ code as context)

Each B agent receives only its section of the resolved spec plus the shared infrastructure code.

**Pro**: Natural dependency ordering, each agent sees prior code for consistency.
**Con**: Sequential execution (no parallelism), later agents have growing context.

#### Option C: Builder + Completeness Auditor

Keep single Agent B but add a **B-Audit** agent after B:
- B-Audit receives B's code AND M's resolved spec
- B-Audit produces a checklist: for each spec requirement, is it fully implemented, partially implemented, or missing?
- Any partially/missing items get dispatched to a B-Fix agent with targeted scope

**Pro**: Minimal pipeline change, catches exactly this class of problem.
**Con**: Adds 1-2 agents and their token cost.

#### Option D: Spec Decomposition in M

Have M produce not just a resolved spec but a **build plan** that explicitly sequences the work. B then follows the build plan, prioritizing complex components before simpler ones.

**Pro**: Addresses the root cause (complex components built last get short-changed).
**Con**: Requires M to estimate implementation complexity, which design agents aren't calibrated for.
