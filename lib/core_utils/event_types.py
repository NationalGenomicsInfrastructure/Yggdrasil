from enum import Enum


class EventType(str, Enum):
    PROJECT_CHANGE = "project_change"
    FLOWCELL_READY = "flowcell_ready"
    DELIVERY_READY = "delivery_ready"
    PLAN_EXECUTION = "plan_execution"
    TEST_SCENARIO_CHANGE = "test_scenario_change"  # Dev-only test realm
