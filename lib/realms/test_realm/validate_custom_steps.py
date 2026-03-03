#!/usr/bin/env python3
"""
Example: Validating custom step definitions in a realm handler.

This script demonstrates how realm developers can validate their handler's
ability to generate plans from custom step configurations (without recipes).

Usage:
    cd /path/to/Yggdrasil
    python lib/realms/test_realm/example_custom_steps.py

Expected output:
    - Plan generated with 3 steps
    - Each step shows fn_ref, deps, and params
    - No database interaction (just validates handler logic)
"""

import os
from pathlib import Path

from lib.realms.test_realm.handler import TestRealmHandler
from yggdrasil.flow.events.emitter import FileSpoolEmitter

# Create a simple test scenario with custom steps
scenario_doc = {
    "_id": "test_scenario:custom_test",
    "type": "ygg_test_scenario",
    "name": "Custom Steps Test",
    "steps": [
        {
            "step_id": "echo1",
            "name": "First Echo",
            "fn_name": "step_echo",
            "params": {"message": "Hello from custom step 1"},
        },
        {
            "step_id": "random1",
            "name": "Random Failure",
            "fn_name": "step_random_fail",
            "params": {"failure_probability": 0.3},
            "deps": ["echo1"],
        },
        {
            "step_id": "echo2",
            "name": "Second Echo",
            "fn_name": "step_echo",
            "params": {"message": "Survived! Custom step 2"},
            "deps": ["random1"],
        },
    ],
    "auto_run": True,
}

# Create handler and planning context
handler = TestRealmHandler()
handler.realm_id = "test_realm"  # Set manually since not registered via core
scope = handler.derive_scope(scenario_doc)

work_root = Path(os.getenv("YGG_WORK_ROOT", "/tmp/ygg_work"))
scope_dir = work_root / handler.realm_id / scope["id"]

emitter = FileSpoolEmitter(spool_dir=os.getenv("YGG_EVENT_SPOOL", "/tmp/ygg_events"))

ctx = handler.build_planning_context(
    scope=scope,
    scope_dir=scope_dir,
    emitter=emitter,
    source_doc=scenario_doc,
    reason="test custom steps",
)

payload = {"doc": scenario_doc, "planning_ctx": ctx}

# Generate plan draft
print("Testing custom steps support...")
try:
    draft = handler.run_now(payload)
    print("✓ Plan generated successfully!")
    print(f"  Plan ID: {draft.plan.plan_id}")
    print(f"  Steps: {len(draft.plan.steps)}")
    print(f"  Auto-run: {draft.auto_run}")

    for i, step in enumerate(draft.plan.steps, 1):
        print(f"  {i}. {step.step_id} ({step.name}) -> {step.fn_ref}")
        if step.deps:
            print(f"     deps: {step.deps}")
        if step.params:
            print(f"     params: {step.params}")

    print("\n✓ Custom steps feature working correctly!")

except Exception as e:
    print(f"✗ Error: {e}")
    import traceback

    traceback.print_exc()
