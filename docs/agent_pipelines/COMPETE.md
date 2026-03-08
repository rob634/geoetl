# Pipeline 1: Adversarial Review

**Purpose**: Review a code subsystem for architecture and correctness problems using agents with deliberate information asymmetry.

**Best for**: 5–20 key files. After completing a major feature, or during architecture review sprints.

---

## Agent Roles

| Agent | Role | Runs As | Input |
|-------|------|---------|-------|
| Omega | Divide the review into two scopes | Claude (no subagent) | Code files |
| Alpha | Review architecture and design only | Task (parallel with Beta) | Code + architecture scope |
| Beta | Review correctness and reliability only | Task (parallel with Alpha) | Code + correctness scope |
| Gamma | Find contradictions, blind spots, and missed issues | Task (sequential) | Alpha output + Beta output + priority files |
| Delta | Produce the final actionable report | Task (sequential) | All previous outputs |

**Maximum parallel agents**: 2 (Alpha + Beta only)

---

## Flow

```
Code files
    |
    Omega (Claude plays this role)
    Divide review into two asymmetric scopes
    |
    +--- Alpha (Task) ---+--- Beta (Task) ---+   [parallel]
    |    Architecture     |    Correctness    |
    +---------------------+-------------------+
                |
         Gamma (Task)                             [sequential]
         Find contradictions and blind spots
                |
         Delta (Task)                             [sequential]
         Final prioritized report
```

---

## Step 1: Read the Code

Read all files in the target subsystem. Use the Explore agent if needed to build the full file list.

## Step 2: Play Omega (No Subagent)

Claude plays Omega directly. Omega has two jobs:

1. **Choose the scope split** for this specific review.
2. **Prepare Gamma's priority files** for blind spot detection.

### Choosing the Scope Split

Do NOT always use the same split. Read the code first, understand what kind of subsystem it is, then choose the two-scope partition that will produce the most useful friction.

**Hard rules for any split:**
- The two scopes MUST be non-overlapping. No item appears in both.
- The two scopes MUST be roughly equal in depth (not 3 items vs 12 items).
- The split MUST create productive tension — the two scopes should naturally disagree about something.
- Omega MUST state why this split was chosen for this specific code.

**Why this matters**: Each agent has different blind spots. Where they disagree, the disagreement itself is a finding. Where they both miss something, Gamma catches it. If you always use the same split, the pipeline develops its own consistent blind spots. Varying the split based on the code gives you coverage diversity across reviews.

### Scope Split Options

Choose the split that best fits the subsystem being reviewed. These are starting points — Omega may adjust the item lists to fit the specific code.

**geoetl-specific guidance**: Split C (Data vs Control Flow) is the natural default for ETL and pipeline modules (`pipeline/`, `batch/raster_ops.py`). Split A (Design vs Runtime) works well for raster/vector processing modules (`raster/`, `vector/`). Split D (Security vs Functionality) is appropriate for `vantor/` since it handles API keys and external requests. Split B (Internal vs External) fits integration layers that bridge local processing with external APIs.

---

**Split A: Design vs Runtime** (default — good for most subsystems)

Use when: General-purpose review of a subsystem with both structural design and runtime behavior concerns.

Alpha — Architecture, Design, and Contracts:
- Composition vs inheritance, coupling, cohesion
- Separation of concerns, layering violations
- Contract design (what each component promises and requires)
- Extension points, plugin and registry patterns
- Dependency direction and inversion
- API surface area (too wide? too narrow? leaky abstractions?)
- Configuration management (hardcoded values, environment coupling, secret handling)
- Naming, discoverability, cognitive load for new developers
- Test architecture (are the boundaries testable? not whether tests pass)

Does NOT see: runtime behavior, failure modes, performance, concurrency

Beta — Correctness, Reliability, and Runtime Behavior:
- Race conditions, concurrency, shared mutable state
- State machine completeness (missing transitions, unreachable states)
- Error recovery and state consistency after failure
- Idempotency and atomicity of multi-step operations
- Exception swallowing, silent failures, error masking
- Resource lifecycle (connections, handles, pools — opened but never closed?)
- Timeout handling, operations that can hang indefinitely
- Performance traps (N+1 queries, unbounded loops, memory accumulation)
- Security at runtime (input validation, injection vectors, auth bypass paths)
- Logging quality (too little? too much? leaking sensitive data?)

Does NOT see: design intent, architectural rationale, code organization choices

---

**Split B: Internal vs External** (good for integration layers and APIs)

Use when: The subsystem sits between internal logic and external systems (APIs, message queues, databases, third-party services).

Alpha — Internal Logic and Invariants:
- Business rule correctness and completeness
- State management and transitions
- Data transformation accuracy
- Validation logic (is every input checked before use?)
- Internal consistency (do components agree on data shapes and meanings?)
- Algorithm correctness and edge case handling
- Error classification (which errors are retryable vs fatal?)

Does NOT see: how external systems behave, network conditions, deployment context

Beta — External Interfaces and Boundaries:
- API contract compliance (does the code honor what it promises to callers?)
- Dependency resilience (what happens when an external service is slow, down, or wrong?)
- Message format and serialization (can data be corrupted in transit?)
- Authentication and authorization at boundaries
- Rate limiting, backpressure, and timeout behavior
- Observability at boundaries (can you tell what happened from logs and metrics?)
- Configuration and secret management for external connections

Does NOT see: internal business logic, algorithm design, data model choices

---

**Split C: Data vs Control Flow** (good for ETL pipelines and data processing)

Use when: The subsystem primarily moves, transforms, or stores data. Pipelines, ETL jobs, batch processors, and data APIs.

Alpha — Data Integrity and Lifecycle:
- Data validation at entry points (schema, types, ranges, nulls)
- Transformation correctness (does the output match the documented intent?)
- Data consistency across stores (if written to two places, can they diverge?)
- Schema evolution and backward compatibility
- Data loss scenarios (where can records be silently dropped?)
- Encoding and format handling (character sets, coordinate systems, units)
- Partitioning and deduplication logic

Does NOT see: orchestration, flow control, infrastructure behavior

Beta — Orchestration, Flow Control, and Failure Handling:
- Job lifecycle (start, progress, completion, failure, retry, timeout)
- Ordering guarantees (does sequence matter? is it preserved?)
- Concurrency control (parallel workers, fan-out/fan-in, resource contention)
- Partial failure recovery (what if step 3 of 5 fails?)
- Backpressure and queue management
- Idempotency of operations (safe to re-run?)
- Monitoring and alerting (can you tell if the pipeline is stuck or failing silently?)

Does NOT see: data content, transformation logic, schema design

---

**Split D: Security vs Functionality** (use for security-sensitive subsystems)

Use when: The subsystem handles authentication, authorization, sensitive data, or operates at a trust boundary.

Alpha — Application Logic and Functionality:
- Feature completeness (does the code do what it is supposed to do?)
- Business rule correctness
- State management and transitions
- Error handling and user-facing behavior
- Performance and resource usage
- API design and usability
- Configuration and defaults

Does NOT see: security implications, trust boundaries, attack surfaces

Beta — Security and Trust Boundaries:
- Input validation and sanitization (injection, XSS, path traversal)
- Authentication and session management
- Authorization checks (are they applied consistently at every entry point?)
- Sensitive data handling (encryption at rest and in transit, logging exposure)
- Cryptographic correctness (key management, algorithm choices, randomness)
- Trust boundary enforcement (what is trusted vs untrusted input?)
- Audit trail completeness

Does NOT see: business logic, feature design, user experience

---

### Omega's Decision Process

1. Read the code and identify the subsystem type.
2. Choose the split that creates the most productive tension for this code.
3. Adjust the item lists if needed — add items that are specific to this codebase, remove items that clearly do not apply.
4. State which split you chose and why in one or two sentences.
5. Assign specific files to each agent. If a file is relevant to both scopes, assign it to both — the agents will examine it through different lenses.

### Preparing Gamma's Priority Files

After Alpha and Beta return, compare which files they cited against the full file list. Files that neither reviewer cited heavily are Gamma's priority targets — these are where blind spots hide.

### Optional Dry Run

Show the user the chosen split, the reasoning, and the file assignments before dispatching. Only proceed after confirmation.

## Step 3: Dispatch Alpha + Beta (Parallel)

Send both Task calls in a single message so they run in parallel.

**IMPORTANT**: If a project-specific rules file exists (e.g., CLAUDE.md coding standards), both agents receive relevant sections. Violations of project rules are findings — same severity scale as architectural or correctness issues.

- **Alpha** (architecture scope): Gets rules related to design patterns, configuration, dependencies
- **Beta** (correctness scope): Gets rules related to error handling, safety constraints, runtime behavior
- For non-default splits, Omega assigns rule sections based on relevance to each scope.

### Alpha Prompt

```
You are Alpha — Reviewer A in an adversarial code review.

You are reviewing: [SUBSYSTEM DESCRIPTION]

You have been assigned a specific review scope. Another reviewer is independently
handling a different, non-overlapping scope. You do not know what they are looking at.

## Your Scope
[Paste the Alpha scope items chosen by Omega]

## What Is NOT Your Scope
[Paste the "Does NOT see" list chosen by Omega]
If you notice something outside your scope, ignore it. The other reviewer will handle it.

## Project Rules (Your Sections)
[Paste relevant project rules (e.g., from CLAUDE.md coding standards).
Violations are findings at the severity appropriate to the rule.]

## Files to Read
[List exact file paths]

## Output Format (use these exact sections)

STRENGTHS
- What is well-done within your scope. Cite file and line numbers.

CONCERNS
- Issues ranked HIGH / MEDIUM / LOW. For each: file, line range, impact description.

ASSUMPTIONS
- Assumptions within your scope that may or may not hold true in production.

RECOMMENDATIONS
- Specific improvements. Each must include: what to change, where, and why.

## Rules
- Stay strictly within your assigned scope.
- Cite specific file paths and line numbers for every finding.
- If you are unsure whether something belongs to your scope, skip it.
```

### Beta Prompt

```
You are Beta — Reviewer B in an adversarial code review.

You are reviewing: [SUBSYSTEM DESCRIPTION]

You have been assigned a specific review scope. Another reviewer is independently
handling a different, non-overlapping scope. You do not know what they are looking at.

## Your Scope
[Paste the Beta scope items chosen by Omega]

## What Is NOT Your Scope
[Paste the "Does NOT see" list chosen by Omega]
If you notice something outside your scope, ignore it. The other reviewer will handle it.

## Project Rules (Your Sections)
[Paste relevant project rules (e.g., from CLAUDE.md coding standards).
Violations are findings at the severity appropriate to the rule.]

## Files to Read
[List exact file paths]

## Output Format (use these exact sections)

VERIFIED SAFE
- Patterns you confirmed are correct within your scope. Cite file and line numbers.

FINDINGS
- Confirmed or likely issues. Rank: CRITICAL / HIGH / MEDIUM.
  For each: the scenario that triggers it, and the impact.

RISKS
- Issues that depend on runtime conditions (load, timing, external services).

EDGE CASES
- Scenarios that may not be handled. For each: how likely, how severe.

## Rules
- Stay strictly within your assigned scope.
- Cite specific file paths and line numbers for every finding.
- If you are unsure whether something belongs to your scope, skip it.
```

## Step 3.5: Quality Gate

Before dispatching Gamma, verify:

1. Alpha used the correct output sections (STRENGTHS / CONCERNS / ASSUMPTIONS / RECOMMENDATIONS).
2. Beta used the correct output sections (VERIFIED SAFE / FINDINGS / RISKS / EDGE CASES).
3. Both cited specific file paths and line numbers.

If either agent returned unstructured text instead of the requested sections, re-dispatch that agent with the format instructions emphasized.

## Step 4: Dispatch Gamma

Gamma receives BOTH reviews and the priority file list.

### Gamma Prompt

```
You are Gamma — the Adversarial Contradiction Finder.

You have two independent reviews of the same codebase. The reviewers had information
asymmetry — neither saw the other's scope:
- Alpha reviewed ONLY architecture and design.
- Beta reviewed ONLY correctness and reliability.

## Alpha's Review
[Paste Alpha output]

## Beta's Review
[Paste Beta output]

## Gamma's Priority Files
[List files that neither Alpha nor Beta cited. These are where blind spots hide.]

## Your Task

Produce these sections:

CONTRADICTIONS
- Where Alpha and Beta disagree or reach incompatible conclusions.

AGREEMENT REINFORCEMENT
- Where both independently found the same issue. These are highest confidence findings.

BLIND SPOTS
- Read the Priority Files above first. Find issues that neither reviewer caught.
  Explain why both scopes missed it.

SEVERITY RECALIBRATION
- Re-rank ALL findings (from Alpha, Beta, and your own) on a unified scale.

## Confidence Tagging (required for every finding)

Tag each finding with one of:
- CONFIRMED: You read the code and traced the execution path. Cite the line numbers.
- PROBABLE: Strongly suggested by code patterns, but you did not fully trace it.
- SPECULATIVE: Plausible based on the reviews, but you did not verify against code.

A finding is only CONFIRMED if you cite the specific line numbers and walk through
the execution path. If you did not do that, use PROBABLE or SPECULATIVE.

## Project Rules (All Sections)
[Paste the FULL project rules — Gamma checks what Alpha and Beta missed,
including rule violations neither caught.]

## Additional Focus Areas
Look specifically for: security issues, concurrency gaps, silent data loss,
configuration-dependent failures, logging that exposes sensitive data, and
PROJECT RULE VIOLATIONS that neither Alpha nor Beta flagged.
```

### Gamma Quality Check

Before dispatching Delta, verify that Gamma:
- Actually cited files from the Priority Files list (not just Alpha/Beta's files).
- Used the confidence tags (CONFIRMED / PROBABLE / SPECULATIVE) on every finding.
- Produced all four sections.

If Gamma only paraphrased Alpha and Beta without reading the Priority Files, re-dispatch.

## Step 5: Dispatch Delta

Delta synthesizes the final report.

### Delta Prompt

```
You are Delta — the Lead Architect and Final Arbiter.

You have the complete picture: two independent reviews and Gamma's adversarial analysis.

Produce a final report with exactly these sections:

EXECUTIVE SUMMARY
- 3 to 5 sentences. What is the overall state of this subsystem?

TOP 5 FIXES
For each fix, include ALL of these fields:
- WHAT: One sentence describing the change.
- WHY: The risk or bug this addresses.
- WHERE: Exact file path, function name, and line range.
- HOW: Specific implementation guidance.
- EFFORT: Small (< 1 hour) / Medium (1–4 hours) / Large (> 4 hours).
- RISK OF FIX: Low / Medium / High (how likely is this fix to break something?).

ACCEPTED RISKS
- Real issues that are not worth fixing now. For each: what it is, why it is acceptable,
  and under what conditions you would revisit this decision.

ARCHITECTURE WINS
- What is genuinely well-done. Preserve these patterns. Be specific.

## Rules
- Prioritize CONFIRMED findings over PROBABLE. Prioritize PROBABLE over SPECULATIVE.
- Every fix must include the exact file path, function name, and line range.
  Do not give abstract recommendations.
- A developer should be able to read this report, open the file, and navigate
  directly to the issue.
- Keep it concise. Write for a senior developer who already knows the codebase.

## Gamma's Recalibrated Findings
[Paste Gamma output]

## Key Files to Confirm
[List the files referenced in the top findings — Delta should read these to verify.]
```

## Step 6: Save Results

Write the final report to `docs/agent_reviews/COMPETE_[SCOPE].md` in the project root.

---

## Chaining to Reflexion

After the Adversarial Review, use the Top 5 Fixes as input to the Reflexion pipeline:

1. Scope the Reflexion pipeline to the specific files from the Top 5 Fixes.
2. Use the fix descriptions as Developer Context for Agent F.
3. Focus Agent F on the fault categories that match the findings.

Example: If the review found "non-atomic batch processing in `batch/raster_ops.py`", run Reflexion on `batch/raster_ops.py` with F focused on atomicity and partial-failure scenarios.
