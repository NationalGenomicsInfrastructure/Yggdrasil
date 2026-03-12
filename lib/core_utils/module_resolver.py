import logging

from .config_loader import ConfigLoader


def get_module_location(document: dict) -> str | None:
    try:
        registry = ConfigLoader().load_config("module_registry.json")
        method = document["details"]["library_construction_method"]
        if method in registry:
            return registry[method]["module"]
        for key, cfg in registry.items():
            if cfg.get("prefix") and method.startswith(key):
                return cfg["module"]
    except KeyError as e:
        logging.error("Missing key in document: %s", e)
    except Exception as e:
        logging.error("Error mapping method '%s': %s", method, e)
    return None
