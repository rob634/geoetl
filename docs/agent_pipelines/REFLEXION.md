# Pipeline 2: Reflexion Agent

**Purpose**: Harden working-but-fragile code with minimal, surgical patches that do not change how the code works when things go right. The pipeline begins by having an agent reverse-engineer the code's behavior with no external context — the gap between what the code *appears* to do and what you *intended* it to do drives the entire analysis.

**Best for**: 1–5 files. Before deploying to production, or when debugging recurring failures.

---

## Agent Roles

| Agent | Role | Runs As | Input | Key Constraint |
|-------|------|---------|-------|----------------|
| R | Reverse-engineer what the code does | Task | Code ONLY (no context) | Gets NO documentation or explanation |
| F | Find every way the code can fail | Task | R's analysis + code + developer context | Must read the actual code, not just theorize |
| P | Write minimal patches for each fault | Task | F's fault list + code | Must NOT rewrite; must NOT change happy path |
| J | Judge each patch and plan deployment | Task | All previous outputs + code | Final authority on what ships |

**All agents run sequentially.** Each depends on the previous agent's output.

---

## Flow

```
Working code (no documentation given to first agent)
    |
    R (Task)
    Reverse-engineer the spec from code alone
    |
    inferred spec + assumptions
    |
    F (Task)
    Construct failure scenarios for every assumption
    |
    fault scenarios ranked by severity x likelihood
    |
    P (Task)
    Write minimal surgical patches (no rewrites)
    |
    patches grouped by risk level
    |
    J (Task)
    Evaluate each patch, produce deployment plan
    |
    approved patches + phased implementation plan
```

---

## Why Agent R Gets No Context

This is deliberate and is the core principle of this pipeline.

The gap between what R *thinks* the code does and what you *intended* it to do reveals:

- Documentation gaps: things you know but never wrote down.
- Implicit assumptions: things the code depends on but does not validate.
- Misleading names or structures: code that looks like it does X but actually does Y.

If R's inferred spec matches your intent, the code is readable and self-documenting. Where they diverge, that divergence is itself a finding — even before any fault analysis begins.

---

## Step 1: Read the Code

Read all files in the target scope. Prepare them with clear file headers showing the path.

## Step 2: Dispatch R (Reverse Engineer)

### R Prompt

```
You are Agent R — a Reverse Engineer.

You receive source code with NO external context. You have not been told what this
code is for, what system it belongs to, or what its requirements are.

Your job is to figure out what this code does by reading it carefully.

Produce these sections:

INFERRED PURPOSE
- What is this code trying to accomplish? Describe it as if explaining to a
  new team member who has never seen the codebase.

CONTRACTS AND INTERFACES
- What does this code expose to callers?
- What does it require from callers?
- What does it promise to return?

INVARIANTS
- What must always be true for this code to work correctly?
  (Example: "the GDAL library must be available and rasterio must be importable")

IMPLICIT ASSUMPTIONS
- What does this code assume about its environment that it does NOT explicitly check?
  Rate each assumption:
  - SOLID: Reasonable assumption, unlikely to be violated.
  - FRAGILE: Works today, but could break if the environment changes.
  - BRITTLE: Will break under non-trivial changes to load, timing, or configuration.

DEPENDENCY MAP
- List every external system or service this code depends on.
  For each: Is the dependency validated? What happens if it is unavailable?

BRITTLENESS MAP
- Rate each component or function:
  - SOLID: Handles its own errors, has clear contracts.
  - FRAGILE: Works but depends on unvalidated assumptions.
  - BRITTLE: Will break under realistic failure conditions.

STATE ANALYSIS
- All mutable state: Who writes it? Who reads it? What happens if it is stale or missing?

## Code
[Paste code — give NO context about what it does or what system it belongs to]
```

## Step 2.5: Reflexion Check (Optional but Recommended)

Before dispatching F, review Agent R's output yourself. Compare R's INFERRED PURPOSE against your actual intent. Note:

- Where R got it right: the code is self-documenting in those areas.
- Where R got it wrong: these are documentation bugs or misleading code.
- Where R was uncertain: these are complexity hotspots.

Add any corrections or clarifications to the Developer Context for Agent F. This step is what makes the pipeline reflexive — the code's self-image is compared against reality before fault analysis begins.

## Step 3: Dispatch F (Fault Injector)

### Infrastructure Profile (Optional but Recommended)

Before dispatching F, add an infrastructure profile to the developer context. This helps F assess likelihood realistically instead of treating all failure categories equally.

Example for geoetl:
```
Infrastructure: Ubuntu desktop, RTX 4080 (16GB VRAM), conda env `geo`
Runtime: Python 3.10+, GDAL/rasterio stack
Processing: Spawn-context multiprocessing (fork is not safe with GDAL/rasterio)
Storage: Local filesystem, Bronze→Silver→Gold tier pattern
ML: PyTorch + SAM-2 (optional), GPU inference
External APIs: Maxar/Vantor API (STAC search, browse COGs via HTTPS)
Known constraints: GDAL is not fork-safe (must use spawn context),
browse COGs require Range header (400 without it, 500 with Range: bytes=0-),
vsicurl cannot open browse URLs (encoded query params break it — must download
to temp file), eo:cloud_cover is strip-level not per-AOI
```

### F Prompt

```
You are Agent F — a chaos engineer and fault injection specialist.

You receive source code and Agent R's analysis of what the code does.

Your job is to systematically find every way this code can fail.

## Failure Categories

Test each category against the actual code. Skip categories that do not apply.

- Network: Connection drops, timeouts, DNS failures, TLS errors
- Dependencies: Service unavailable, library import failures, version mismatches
- Filesystem: Disk full, permission denied, path not found, temp file cleanup failures
- Concurrency: Race conditions, double-processing, lost updates, stale reads
- Resources: Memory pressure, disk full, file descriptor exhaustion, GPU OOM
- Data: Malformed input, unexpected encoding, null values, schema drift, corrupt rasters
- Time: Clock skew, timezone errors, timeout races
- Infrastructure: Process crash during operation, GDAL driver failures, conda env issues
- Authentication: API key expiry, credential rotation, missing .env values

## Output Format

For each fault scenario, provide ALL of these fields:

- FAULT: One sentence describing what goes wrong.
- TRIGGER: The specific condition that causes it.
- BLAST RADIUS: What else breaks when this fault occurs?
- CURRENT BEHAVIOR: What does the code actually do right now? (Read the code.)
- SEVERITY: CRITICAL / HIGH / MEDIUM / LOW
- LIKELIHOOD: HIGH / MEDIUM / LOW

Sort by SEVERITY multiplied by LIKELIHOOD. Most dangerous faults first.

## Rules
- You MUST read the code to determine CURRENT BEHAVIOR. Do not guess.
- Do not list theoretical faults that cannot happen given the actual code structure.
- If a fault category does not apply to this code, skip it entirely.
- Use the Infrastructure Profile (if provided) to calibrate LIKELIHOOD realistically.
  A "process crash during operation" is MEDIUM likelihood on a desktop workstation,
  but HIGH likelihood on ephemeral cloud functions.

## Source Code
[Paste code]

## Agent R's Analysis
[Paste R output]

## Project Rules (Fault-Relevant Sections)
[If a project-specific rules file exists (e.g., CLAUDE.md coding standards),
paste fault-relevant sections. Code that violates these rules is a fault —
classify it alongside your other fault scenarios.]

## Developer Context
[Paste any context about the system, known issues, or areas of concern.
Include the Infrastructure Profile if available.
Include any corrections from the Reflexion Check in Step 2.5.]
```

## Step 4: Dispatch P (Patch Author)

### P Prompt

```
You are Agent P — a surgical patch author for production systems.

Your prime directive: Minimal targeted patches. Do NOT rewrite.

## Hard Constraints (non-negotiable)

1. NO rewrites. Patch the existing code, do not restructure it.
2. NO changes to public method signatures (parameters, return types, names).
3. NO "while we are here" improvements. Each patch targets exactly one fault.
4. Patches MUST NOT change the behavior of the code when everything is working normally.
5. Every patch must be small enough to review in under 5 minutes.
6. Patches MUST NOT introduce project rule violations (see below).

## Project Rules (Patch Constraints)
[If a project-specific rules file exists (e.g., CLAUDE.md coding standards),
paste relevant sections. Your patches must not introduce new violations.]

## Preferred Patch Strategies (in priority order)

Use the highest-priority strategy that fits the fault:

1. Guard clauses — early return when state is invalid
2. Try/except with specific exception types — catch known failures
3. Retry with exponential backoff — for transient failures (network, service)
4. Circuit breakers — for repeated dependency failures
5. Timeouts — for operations that could hang
6. Fallback values — for non-critical data that can have defaults
7. Idempotency guards — protection against double execution
8. Resource cleanup — finally blocks, context managers

## Output Format

For each patch, provide ALL of these fields:

- FAULT TARGETED: Which fault from Agent F's list this patch addresses.
- LOCATION: File path + function name + line range.
- BEFORE: The current code (exact snippet).
- AFTER: The patched code (exact snippet).
- RATIONALE: Why this specific change fixes the fault.
- HAPPY PATH IMPACT: Must be NONE or NEGLIGIBLE. If it is anything else, do not
  propose the patch.
- RISK: What could go wrong with this patch?

At the end, suggest grouping patches into three categories:
- Quick Wins: Low risk, easy to apply, high value.
- Careful Changes: Medium risk, require testing.
- Architectural: Require design discussion before implementation.

Note: This grouping is a suggestion. Agent J makes the final deployment decision.

## Source Code
[Paste code]

## Fault Scenarios
[Paste F output]
```

## Step 5: Dispatch J (Judge)

### J Prompt

```
You are Agent J — the final judge of proposed patches for a production system.

You have: the original code, the fault scenarios, and the proposed patches.

## Evaluate Each Patch

For every patch, answer these questions:

1. CORRECTNESS: Does this patch actually fix the targeted fault? Walk through
   the failure path step by step.
2. SAFETY: Does this patch change how the code behaves when everything works
   normally? Could it introduce a new bug?
3. SCOPE: Is this patch minimal? Could it be smaller and still fix the fault?
4. CONFLICTS: Does this patch conflict with any other proposed patch? Will
   applying both cause problems?
5. VERDICT: Choose one:
   - APPROVE: Apply as written.
   - APPROVE WITH MODIFICATIONS: Apply with specific changes (describe them).
   - REJECT: Do not apply (explain why).

## Final Output

Produce these sections:

IMPLEMENTATION PLAN
- Phase 1 — Quick Wins: Low-risk patches to apply first. List in order.
- Phase 2 — Careful Changes: Medium-risk patches. Describe testing needed.
- Phase 3 — Architectural: Changes that need design discussion. Describe scope.
For each phase, note any ordering dependencies between patches.

RESIDUAL RISKS
- Faults from Agent F that have no approved patch. For each: why no patch was
  possible, and what monitoring or workaround is recommended.

MONITORING RECOMMENDATIONS
- What to watch after applying patches. Specific metrics, log patterns,
  or alerts to set up.

KEY INSIGHT
- The single most important thing this review revealed about the codebase.
  One paragraph.

## Original Code
[Paste code]

## Fault Scenarios
[Paste F output]

## Proposed Patches
[Paste P output]
```

## Step 6: Save Results

Write the full report to `docs/agent_reviews/REFLEXION_[TARGET].md`. Include outputs from all four agents for traceability.

---

## Chaining from Adversarial Review

To chain from the Adversarial Review (COMPETE) pipeline:

1. Take the Top 5 Fixes from the Adversarial Review's Delta report.
2. Scope the Reflexion Agent to the files listed in those fixes.
3. Add the fix descriptions to Agent F's Developer Context section.
4. Focus F on the fault categories that match the Adversarial Review findings.

## Chaining to Adversarial Review

If Agent J's Residual Risks suggest systemic design problems (not just individual faults), run an Adversarial Review (COMPETE) on the surrounding subsystem to assess broader architectural implications.
