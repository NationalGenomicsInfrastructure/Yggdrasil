# Test Realm Scenarios

This document describes test scenario documents that exercise different aspects of the plan approval and execution pipeline.

**_Disclaimer: Mainly used for internal dev/test purposes. Might contain mistakes or deprecated information_**

## Overview

Test scenarios are inserted into the yggdrasil database as documents with `type="ygg_test_scenario"`. The `ScenarioDocWatcher` detects them and generates plans via predefined recipes **or custom step definitions**.

**Two modes supported:**

1. **Recipe-based**: Use predefined recipes (easy, recommended for common patterns)
2. **Custom steps**: Define steps directly in the document (flexible, for advanced testing)

Each scenario below shows:
- **Recipe** (if using recipe mode): Which predefined recipe to use
- **Purpose**: What it tests
- **Auto-run**: Whether the plan auto-executes or waits for approval
- **Overrides**: Optional parameter customizations
- **Expected behavior**: What should happen

Available recipes:
- **happy_path**: All steps succeed (echo_start → brief_sleep(0.5s) → echo_end)
- **random_fail**: Probabilistic failure (50% chance by default) - tests retry logic
- **fail_fast**: First step always fails
- **fail_mid_plan**: Succeeds initially (echo → sleep), then fails mid-execution
- **long_running**: Extended sleep (30s default) for testing responsiveness
- **artifact_write**: Creates files and registers artifacts

Available steps (for custom mode):
- **step_echo**: Echo message (params: `message`)
- **step_sleep**: Configurable sleep with progress events (params: `duration_sec`)
- **step_fail**: Always fails (params: `error_message`)
- **step_random_fail**: Probabilistic failure (params: `failure_probability`, `success_message`, `failure_message`)
- **step_write_file**: Write file to workdir (params: `filename`, `content`)

---

## Scenario 1: Simple Success (Auto-run, Happy Path)

**Purpose**: Verify basic happy path with auto-execution.

**Insert as**:
```json
{
  "_id": "test_scenario:simple_success",
  "type": "ygg_test_scenario",
  "recipe": "happy_path",
  "name": "Simple Success",
  "description": "Basic happy path: echo → sleep → echo, auto-runs immediately",
  "auto_run": true
}
```

**Expected**:
- Plan created with `auto_run=true` → `status="approved"`
- PlanWatcher picks it up immediately
- All steps succeed (echo_start → brief_sleep(0.5s) → echo_end)
- Execution completes in ~0.5 seconds
- `executed_run_token` updated after completion

---

## Scenario 2: Pending Approval (Draft)

**Purpose**: Test plan approval workflow.

**Insert as**:
```json
{
  "_id": "test_scenario:pending_approval",
  "type": "ygg_test_scenario",
  "recipe": "happy_path",
  "name": "Pending Approval",
  "description": "Plan requires manual approval before execution",
  "auto_run": false
}
```

**Expected**:
- Plan created with `auto_run=false` → `status="draft"`
- PlanWatcher ignores it (not approved yet)
- Plan stays in DB indefinitely
- **Manual step**: Approve plan manually (set `status="approved"`, increment `run_token`)
- Then PlanWatcher detects change and executes
- Demonstrates approval workflow integration

---

## Scenario 3: Long-Running (Thread Pool Test)

**Purpose**: Verify that long steps don't block the event loop.

**Insert as**:
```json
{
  "_id": "test_scenario:long_running",
  "type": "ygg_test_scenario",
  "recipe": "long_running",
  "name": "Long Running (40s)",
  "description": "Tests that watchers remain responsive during 40-second execution",
  "auto_run": true,
  "overrides": {
    "long_sleep": {
      "duration_sec": 40.0
    }
  }
}
```

**Expected**:
- Plan executes in thread pool (non-blocking)
- During execution, other watchers remain responsive
- Progress events emitted at 25%, 50%, 75%, 100% by the sleep step
- Execution takes ~40 seconds (wall clock)
- Event loop not blocked; other plans can be queued
- Can monitor watchers in separate terminal to verify responsiveness

**Variant (shorter test, 5 seconds)**:
```json
{
  "_id": "test_scenario:long_running_5s",
  "type": "ygg_test_scenario",
  "recipe": "long_running",
  "name": "Long Running (5s)",
  "description": "5-second sleep for faster testing of thread pool",
  "auto_run": true,
  "overrides": {
    "long_sleep": {
      "duration_sec": 5.0
    }
  }
}
```

---

## Scenario 4: Fail Fast

**Purpose**: Test error handling and immediate failure.

**Insert as**:
```json
{
  "_id": "test_scenario:fail_fast",
  "type": "ygg_test_scenario",
  "recipe": "fail_fast",
  "name": "Fail Fast",
  "description": "First step fails immediately; subsequent steps never execute",
  "auto_run": true
}
```

**Expected**:
- Step 1 (fail_immediately) fails with RuntimeError (emits `step.failed`)
- Step 2 (never_reached) never executes
- Plan execution halts with error
- `executed_run_token` NOT updated (plan remains eligible for retry)

---

## Scenario 5: Fail Mid-Plan

**Purpose**: Test partial execution and failure recovery.

**Insert as**:
```json
{
  "_id": "test_scenario:fail_mid_plan",
  "type": "ygg_test_scenario",
  "recipe": "fail_mid_plan",
  "name": "Fail Mid-Plan",
  "description": "Starts successfully, fails in the middle, last steps never execute",
  "auto_run": true
}
```

**Expected**:
- Step 1 (echo_start) succeeds (emits `step.succeeded`)
- Step 2 (brief_sleep, 0.3s) succeeds (emits `step.succeeded`)
- Step 3 (mid_failure) fails with RuntimeError (emits `step.failed`)
- Step 4 (never_reached) never executes
- Plan marked as failed
- `executed_run_token` NOT updated (eligible for retry)

---

## Scenario 6: Artifact Write

**Purpose**: Test artifact registration and retrieval.

**Insert as**:
```json
{
  "_id": "test_scenario:artifact_write",
  "type": "ygg_test_scenario",
  "recipe": "artifact_write",
  "name": "Artifact Write",
  "description": "Creates output files and registers artifacts",
  "auto_run": true
}
```

**Expected**:
- Step 1 (echo_start) succeeds
- Step 2 (write_artifact) creates a file and registers it as artifact
- Step 3 (echo_end) succeeds
- All steps complete successfully
- Artifacts tracked in plan document
- Files retrievable from plan's scope directory

---

## Scenario 7: Happy Path with Custom Sleep Duration

**Purpose**: Test parameter override mechanism.

**Insert as**:
```json
{
  "_id": "test_scenario:custom_sleep",
  "type": "ygg_test_scenario",
  "recipe": "happy_path",
  "name": "Custom Sleep Duration",
  "description": "Happy path with longer sleep via overrides",
  "auto_run": true,
  "overrides": {
    "brief_sleep": {
      "duration_sec": 3.0
    }
  }
}
```

**Expected**:
- All steps succeed
- Sleep step takes 3.0 seconds (instead of default 0.5s)
- Progress events emitted at 25%, 50%, 75%, 100% (at ~0.75s, ~1.5s, ~2.25s, ~3.0s)
- Total execution time ~3.0 seconds
- Demonstrates parameter override system

---

## Scenario 8: Quick Echo (Baseline Performance)

**Purpose**: Baseline performance test; verify minimal overhead.

**Insert as**:
```json
{
  "_id": "test_scenario:quick_echo",
  "type": "ygg_test_scenario",
  "recipe": "happy_path",
  "name": "Quick Echo",
  "description": "Minimal execution; should complete in <100ms",
  "auto_run": true,
  "overrides": {
    "brief_sleep": {
      "duration_sec": 0.01
    }
  }
}
```

**Expected**:
- All steps execute quickly
- Execution latency <100ms (minimal sleep, mostly overhead)
- Events emitted: echo_start, sleep (brief), echo_end
- Plan marked complete almost immediately
- Useful for timing overhead

---

## Scenario 9: Random Failure (Retry Logic Test)

**Purpose**: Test probabilistic failure and retry logic.

**Insert as**:
```json
{
  "_id": "test_scenario:random_fail_50",
  "type": "ygg_test_scenario",
  "recipe": "random_fail",
  "name": "Random Failure (50%)",
  "description": "50% chance of failure; ideal for testing retry mechanisms",
  "auto_run": true
}
```

**Expected**:
- Step 1 (echo_start) always succeeds
- Step 2 (random_step) has 50% chance of failure
  - If succeeds: Step 3 (echo_end) executes; plan completes
  - If fails: Plan execution halts; `executed_run_token` NOT updated (eligible for retry)
- On retry (manual re-approval or automatic), rolls dice again

**Variant (90% failure for testing retry resilience)**:
```json
{
  "_id": "test_scenario:random_fail_90",
  "type": "ygg_test_scenario",
  "recipe": "random_fail",
  "name": "Random Failure (90%)",
  "description": "90% failure rate; tests retry exhaustion",
  "auto_run": true,
  "overrides": {
    "random_step": {
      "failure_probability": 0.9
    }
  }
}
```

**Variant (10% failure for testing eventual success)**:
```json
{
  "_id": "test_scenario:random_fail_10",
  "type": "ygg_test_scenario",
  "recipe": "random_fail",
  "name": "Random Failure (10%)",
  "description": "10% failure rate; usually succeeds",
  "auto_run": true,
  "overrides": {
    "random_step": {
      "failure_probability": 0.1
    }
  }
}
```

---

## Scenario 10: Custom Steps (Single Echo)

**Purpose**: Test custom step definition without recipe.

**Insert as**:
```json
{
  "_id": "test_scenario:custom_single_echo",
  "type": "ygg_test_scenario",
  "name": "Custom Single Echo",
  "description": "Single custom step: echo",
  "auto_run": true,
  "steps": [
    {
      "step_id": "my_echo",
      "name": "My Custom Echo",
      "fn_name": "step_echo",
      "params": {
        "message": "Hello from custom step definition!"
      }
    }
  ]
}
```

**Expected**:
- Plan created with 1 step (my_echo)
- Step executes successfully
- Message emitted: "Hello from custom step definition!"
- Demonstrates custom step syntax

---

## Scenario 11: Custom Steps (Multi-Step Pipeline)

**Purpose**: Test custom multi-step plan with dependencies.

**Insert as**:
```json
{
  "_id": "test_scenario:custom_pipeline",
  "type": "ygg_test_scenario",
  "name": "Custom Pipeline",
  "description": "Multi-step custom plan: echo → random → sleep → echo",
  "auto_run": true,
  "steps": [
    {
      "step_id": "start",
      "name": "Start Pipeline",
      "fn_name": "step_echo",
      "params": {
        "message": "Pipeline starting"
      }
    },
    {
      "step_id": "chaos",
      "name": "Chaos Step",
      "fn_name": "step_random_fail",
      "params": {
        "failure_probability": 0.3,
        "success_message": "Chaos survived",
        "failure_message": "Chaos triggered failure"
      },
      "deps": ["start"]
    },
    {
      "step_id": "wait",
      "name": "Wait Step",
      "fn_name": "step_sleep",
      "params": {
        "duration_sec": 2.0
      },
      "deps": ["chaos"]
    },
    {
      "step_id": "finish",
      "name": "Finish Pipeline",
      "fn_name": "step_echo",
      "params": {
        "message": "Pipeline complete!"
      },
      "deps": ["wait"]
    }
  ]
}
```

**Expected**:
- 4 steps execute in order (start → chaos → wait → finish)
- 30% chance of failure at chaos step
- If chaos succeeds, waits 2s then finishes
- If chaos fails, wait and finish never execute
- Demonstrates custom step dependencies

---

## Scenario 12: Custom Steps (Isolated Random Test)

**Purpose**: Test single random_fail step in isolation (for debugging).

**Insert as**:
```json
{
  "_id": "test_scenario:isolated_random",
  "type": "ygg_test_scenario",
  "name": "Isolated Random Step",
  "description": "Single random_fail step at 50% for quick retry testing",
  "auto_run": true,
  "steps": [
    {
      "step_id": "random_only",
      "name": "Random Failure",
      "fn_name": "step_random_fail",
      "params": {
        "failure_probability": 0.5
      }
    }
  ]
}
```

**Expected**:
- 50% chance of immediate success
- 50% chance of immediate failure (eligible for retry)
- Fastest scenario for testing retry logic
- No dependencies, just pure random outcome

---

## Scenario 13: Parallel Random Failures

**Purpose**: Test parallel step execution where all must succeed.

**Insert as**:
```json
{
  "_id": "test_scenario:parallel_chaos",
  "type": "ygg_test_scenario",
  "name": "Parallel Chaos",
  "description": "Three parallel random steps; all must succeed",
  "auto_run": true,
  "steps": [
    {"step_id": "init", "name": "Init", "fn_name": "step_echo", "params": {"message": "Starting parallel chaos"}},
    {"step_id": "chaos1", "name": "Chaos 1", "fn_name": "step_random_fail", "params": {"failure_probability": 0.3}, "deps": ["init"]},
    {"step_id": "chaos2", "name": "Chaos 2", "fn_name": "step_random_fail", "params": {"failure_probability": 0.3}, "deps": ["init"]},
    {"step_id": "chaos3", "name": "Chaos 3", "fn_name": "step_random_fail", "params": {"failure_probability": 0.3}, "deps": ["init"]},
    {"step_id": "finish", "name": "Finish", "fn_name": "step_echo", "params": {"message": "All survived!"}, "deps": ["chaos1", "chaos2", "chaos3"]}
  ]
}
```

**Expected**:
- init step runs first
- chaos1, chaos2, chaos3 run in parallel (all depend on init)
- Each has 30% failure probability
- **Probability all succeed:** 0.7³ = 34.3% (high chance of retry needed)
- finish only runs if all chaos steps succeed
- Tests dependency fan-out and fan-in patterns

---

## Scenario 14: Artifact Write with Random Failure

**Purpose**: Test artifact persistence when subsequent step fails.

**Insert as**:
```json
{
  "_id": "test_scenario:artifact_with_chaos",
  "type": "ygg_test_scenario",
  "name": "Artifact with Chaos",
  "description": "Write artifact, then 50% chance of failure",
  "auto_run": true,
  "steps": [
    {"step_id": "write", "name": "Write Artifact", "fn_name": "step_write_file", "params": {"filename": "output.txt", "content": "Test data created before chaos"}},
    {"step_id": "chaos", "name": "Chaos", "fn_name": "step_random_fail", "params": {"failure_probability": 0.5}, "deps": ["write"]},
    {"step_id": "verify", "name": "Verify", "fn_name": "step_echo", "params": {"message": "Artifact survived chaos"}, "deps": ["chaos"]}
  ]
}
```

**Expected**:
- Step 1 (write) creates file and registers artifact
- Step 2 (chaos) has 50% chance of failure
- If chaos fails: artifact was still created (partial execution)
- If chaos succeeds: verify step confirms completion
- Tests artifact persistence across retries

---

## How to Insert Scenarios

### Via `curl` (local CouchDB):

```bash
curl -X POST http://localhost:5984/yggdrasil \
  -H "Content-Type: application/json" \
  -d '{
    "_id": "test_scenario:simple_success",
    "type": "ygg_test_scenario",
    "recipe": "happy_path",
    "name": "Simple Success",
    "auto_run": true
  }'
```

### Via Python REPL:

```python
from lib.couchdb.yggdrasil_db_manager import YggdrasilDBManager

ydm = YggdrasilDBManager()

scenario = {
    "_id": "test_scenario:simple_success",
    "type": "ygg_test_scenario",
    "recipe": "happy_path",
    "name": "Simple Success",
    "auto_run": true
}

ydm.server.post_document(db="yggdrasil", document=scenario).get_result()
print("Scenario inserted successfully")
```

### Via Python script:

```python
#!/usr/bin/env python3
import json
from lib.couchdb.yggdrasil_db_manager import YggdrasilDBManager

scenarios = [
    {
        "_id": "test_scenario:simple_success",
        "type": "ygg_test_scenario",
        "recipe": "happy_path",
        "name": "Simple Success",
        "auto_run": true
    },
    {
        "_id": "test_scenario:fail_fast",
        "type": "ygg_test_scenario",
        "recipe": "fail_fast",
        "name": "Fail Fast",
        "auto_run": true
    },
]

ydm = YggdrasilDBManager()
for scenario in scenarios:
    ydm.server.post_document(db="yggdrasil", document=scenario).get_result()
    print(f"Inserted: {scenario['_id']}")
```

---

## Observing Execution

### 1. Watch logs in real-time:

```bash
tail -f yggdrasil.log | grep -E "(TEST_SCENARIO|test_realm|step\.)"
```

### 2. Check event spool (emitted events):

```bash
# List recent events
find $YGG_EVENT_SPOOL -name "*.json" -type f -mmin -5 | sort

# Pretty-print a specific event
cat $YGG_EVENT_SPOOL/.../step_succeeded.json | jq .
```

### 3. Query plan status:

```bash
# Get plan document
curl http://localhost:5984/yggdrasil_plans/test_scenario:simple_success | jq .

# Extract key fields
curl http://localhost:5984/yggdrasil_plans/test_scenario:simple_success | \
  jq '{status, run_token, executed_run_token, realm}'
```

### 4. Monitor watchers responsiveness (during long-running):

In one terminal, start daemon:
```bash
yggdrasil daemon --dev
```

In another terminal, insert long-running scenario while it's executing:
```bash
# Monitor how quickly a new scenario is detected
while true; do curl http://localhost:5984/yggdrasil | jq '.total_rows' 2>/dev/null; sleep 1; done
```

---

## Testing Approval Workflow

For **Scenario 2 (Pending Approval)**:

1. **Insert scenario**:
```bash
curl -X POST http://localhost:5984/yggdrasil \
  -H "Content-Type: application/json" \
  -d '{"_id":"test_scenario:pending_approval","type":"ygg_test_scenario","recipe":"happy_path","auto_run":false}'
```

2. **Verify plan is drafted**:
```bash
curl http://localhost:5984/yggdrasil_plans/test_scenario:pending_approval | \
  jq '{status, run_token, executed_run_token}'
# Should show: status="draft", run_token=0, executed_run_token=-1
```

3. **Simulate approval** (increment run_token):
```bash
# Fetch current doc
PLAN=$(curl http://localhost:5984/yggdrasil_plans/test_scenario:pending_approval)

# Update with status="approved" and run_token increment
curl -X PUT http://localhost:5984/yggdrasil_plans/test_scenario:pending_approval \
  -H "Content-Type: application/json" \
  -d "$(echo $PLAN | jq '.status="approved" | .run_token=1')"
```

4. **Watch PlanWatcher execute**:
```bash
tail -f yggdrasil.log | grep "Eligible plan detected"
```

---

## Testing Retry Logic (Future Implementation)

Once retry logic is implemented, use **fail_fast** or **fail_mid_plan** scenarios:

1. Insert a failing scenario
2. Observe plan fails (executed_run_token NOT updated)
3. Verify plan remains eligible: `is_plan_eligible(plan_doc) == True`
4. Trigger retry (manual re-approval or automatic)
5. Observe re-execution

---

## Summary: Quick Reference

| Scenario | Recipe/Custom | Auto-run | Duration | Expected Result |
|----------|----------------|----------|----------|-----------------|
| Simple Success | happy_path | ✓ | ~0.5s | All steps succeed |
| Pending Approval | happy_path | ✗ | manual | Draft, waits for approval |
| Long Running (30s) | long_running | ✓ | ~30s | Non-blocking, responsive watchers |
| Long Running (5s) | long_running | ✓ | ~5s | Faster variant |
| Fail Fast | fail_fast | ✓ | <1s | Step 1 fails immediately |
| Fail Mid-Plan | fail_mid_plan | ✓ | ~0.3s | Steps 1-2 succeed, 3 fails |
| Artifact Write | artifact_write | ✓ | <1s | Files created, artifacts tracked |
| Custom Sleep | happy_path | ✓ | ~3s | Tests parameter override |
| Quick Echo | happy_path | ✓ | <100ms | Baseline overhead |
| Random Fail (50%) | random_fail | ✓ | ~0.5s | 50% chance of failure |
| Random Fail (90%) | random_fail | ✓ | ~0.5s | 90% chance of failure |
| Random Fail (10%) | random_fail | ✓ | ~0.5s | 10% chance of failure |
| Custom Single Echo | Custom steps | ✓ | <50ms | Single echo step |
| Custom Pipeline | Custom steps | ✓ | ~2s | Multi-step with 30% fail chance |
| Isolated Random | Custom steps | ✓ | <50ms | Pure random 50/50 outcome |
| Parallel Chaos | Custom steps | ✓ | <1s | 3 parallel random (34% all succeed) |
| Artifact with Chaos | Custom steps | ✓ | <1s | Artifact + 50% failure |

---

## Troubleshooting

**Plan not created after inserting scenario:**
- Check ScenarioDocWatcher is running: `tail -f yggdrasil.log | grep ScenarioDocWatcher`
- Verify `_id` field is set (must be unique)
- Verify `type="ygg_test_scenario"` 
- Verify EITHER `recipe` field OR `steps` array exists
- Check if plan was created with different ID: `curl http://localhost:5984/yggdrasil_plans/_all_docs | jq '.rows[] | select(.id | contains("test_scenario"))'`

**Plan created but not executing:**
- Check plan status: `curl http://localhost:5984/yggdrasil_plans/<plan_id> | jq '.status'`
- If `status="draft"`, manually approve (increment `run_token`)
- Check PlanWatcher is running: `tail -f yggdrasil.log | grep PlanWatcher`

**Step failures with missing fn_ref:**
- Ensure recipe exists in `lib/realms/test_realm/recipes.py`
- Verify step function exists in `lib/realms/test_realm/steps.py`
- Check error message for typos in override field names or fn_name

**Custom steps not working:**
- Verify `steps` is an array of dicts
- Each step must have `step_id` and `fn_name`
- Valid `fn_name` values: `step_echo`, `step_sleep`, `step_fail`, `step_random_fail`, `step_write_file`
- Check deps refer to existing step_id values

For broader troubleshooting (CouchDB connectivity, config errors, realm discovery, DataAccess), see [troubleshooting.md](troubleshooting.md).
