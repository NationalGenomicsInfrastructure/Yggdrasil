#!/usr/bin/env python3
"""
Example: Validating recipe-based plan generation in a realm handler.

This script demonstrates how realm developers can validate their handler's
ability to generate plans from recipe configurations.

Usage:
    cd /path/to/Yggdrasil
    python lib/realms/test_realm/example_random_recipe.py

Expected output:
    - Plan generated from 'random_fail' recipe
    - Shows steps: echo_start → random_step → echo_end
    - No database interaction (just validates handler logic)
"""

import os
from pathlib import Path

from lib.realms.test_realm.handler import TestRealmHandler
from yggdrasil.flow.events.emitter import FileSpoolEmitter

# Test scenario with random_fail recipe
scenario_doc = {
    "_id": "test_scenario:random_recipe_test",
    "type": "ygg_test_scenario",
    "recipe": "random_fail",
    "name": "Random Fail Recipe Test",
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
    reason="test random_fail recipe",
)

payload = {"doc": scenario_doc, "planning_ctx": ctx}

# Generate plan draft
print("Testing random_fail recipe...")
try:
    draft = handler.run_now(payload)
    print("✓ Plan generated successfully!")
    print(f"  Plan ID: {draft.plan.plan_id}")
    print(f"  Steps: {len(draft.plan.steps)}")
    print(f"  Auto-run: {draft.auto_run}")

    for i, step in enumerate(draft.plan.steps, 1):
        print(f"  {i}. {step.step_id} ({step.name})")
        if step.params:
            print(f"     params: {step.params}")

    print("✓ random_fail recipe working correctly!")

except Exception as e:
    print(f"✗ Error: {e}")
    import traceback

    traceback.print_exc()
