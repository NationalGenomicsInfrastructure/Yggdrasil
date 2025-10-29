from typing import Any

from lib.core_utils.config_loader import ConfigLoader
from lib.realms.tenx_ext.project_model import TenxProjectModel
from yggdrasil.flow.planner.facts_base import DistilledFacts, FactDistiller


class TenxFactsProvider(FactDistiller):
    """
    Thin adapter: domain --> stable facts JSON.
    """

    def distil_facts(
        self, *, doc: dict[str, Any], realm: str, scope: dict[str, Any]
    ) -> DistilledFacts:
        cfg = ConfigLoader().load_config("10x_config.json")

        # Important: TenxProjectModel.from_doc(...) must be PURE (no DB/FS/Slurm)
        model = TenxProjectModel.from_doc(doc=doc, config=cfg)

        # Single source of truth—no re-implementation here
        data = model.to_facts()

        print("Distilled Tenx facts:", data)

        return DistilledFacts(
            realm=realm,
            scope=scope,
            version=2,  # bump when the shape changes
            data=data,
        )
