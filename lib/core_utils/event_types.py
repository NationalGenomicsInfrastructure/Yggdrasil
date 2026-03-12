from enum import Enum


class EventType(str, Enum):
    """
    Core event types for Yggdrasil.

    Generic Ingress Types (from watcher backends):
        These are backend-sourced events. Realms use filter_expr and
        target_handlers in WatchSpec to route to specific handlers.

    Internal Types:
        Used by core infrastructure (e.g., PlanWatcher).

    Legacy Types (deprecated):
        Domain-specific types from pre-refactor. Will be removed
        after migration to generic ingress types.
    """

    # --- Generic Ingress Types (NEW) ---
    # CouchDB backend events
    COUCHDB_DOC_CHANGED = "couchdb_doc_changed"
    COUCHDB_DOC_DELETED = "couchdb_doc_deleted"

    # Filesystem backend events (future)
    # FS_FILE_CREATED = "fs_file_created"
    # FS_FILE_MODIFIED = "fs_file_modified"
    # FS_FILE_DELETED = "fs_file_deleted"

    # --- Internal Types ---
    PLAN_EXECUTION = "plan_execution"  # PlanWatcher → Engine

    # --- Legacy Types (deprecated, remove after migration) ---
    PROJECT_CHANGE = "project_change"
    FLOWCELL_READY = "flowcell_ready"
    DELIVERY_READY = "delivery_ready"
    TEST_SCENARIO_CHANGE = "test_scenario_change"
