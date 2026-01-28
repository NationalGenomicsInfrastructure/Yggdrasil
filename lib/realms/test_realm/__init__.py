"""
Test Realm - Dev-only internal test infrastructure.

This realm provides controllable test scenarios for validating Yggdrasil's
execution pipeline without external dependencies.

IMPORTANT: This realm is only available when running in dev mode (--dev flag).
In production mode, the handler and watcher will not be registered.

Components:
    - TestRealmHandler: Processes TEST_SCENARIO_CHANGE events
    - ScenarioDocWatcher: Monitors yggdrasil DB for scenario documents
    - Templates: Pre-defined test plans (happy_path, fail_fast, etc.)
    - Steps: Controllable test steps (echo, sleep, fail, write_file, random_fail)

Usage:
    1. Start Yggdrasil in dev mode: `yggdrasil --dev daemon`
    2. Create a scenario document in yggdrasil DB:
        {
            "_id": "test_scenario:my_test",
            "type": "ygg_test_scenario",
            "template": "happy_path",
            "auto_run": true
        }
    3. The watcher detects the document and triggers the handler
    4. Handler generates a plan from the template
    5. Engine executes the plan

Templates:
    - happy_path: All steps succeed
    - fail_fast: First step fails
    - fail_mid_plan: Middle step fails
    - long_running: Extended sleep (30s default)
    - artifact_write: Writes files and registers artifacts

Scenario Document Schema:
    {
        "_id": "test_scenario:<unique_id>",  # Required
        "type": "ygg_test_scenario",          # Required (must be exact)
        "template": "<template_name>",        # Required
        "auto_run": true | false,             # Optional (default: true)
        "overrides": {                        # Optional
            "<step_id>": {"param": "value"}
        }
    }
"""

from lib.core_utils.ygg_session import YggSession


def is_test_realm_enabled() -> bool:
    """
    Check if test realm should be enabled.

    Returns True only when running in dev mode (--dev flag).
    """
    return YggSession.is_dev()


def get_handler():
    """
    Get TestRealmHandler if dev mode is enabled.

    Returns:
        TestRealmHandler instance or None if not in dev mode
    """
    if not is_test_realm_enabled():
        return None

    from lib.realms.test_realm.handler import TestRealmHandler

    return TestRealmHandler()


def get_watcher(on_event, logger=None):
    """
    Get ScenarioDocWatcher if dev mode is enabled.

    Args:
        on_event: Callback for YggdrasilEvent
        logger: Optional logger instance

    Returns:
        ScenarioDocWatcher instance or None if not in dev mode
    """
    if not is_test_realm_enabled():
        return None

    from lib.realms.test_realm.watcher import ScenarioDocWatcher

    return ScenarioDocWatcher(on_event=on_event, logger=logger)


# Expose key components for direct import (only if in dev mode)
__all__ = [
    "is_test_realm_enabled",
    "get_handler",
    "get_watcher",
]
