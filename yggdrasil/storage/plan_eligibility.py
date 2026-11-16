"""Former home of the plan eligibility decision.

Whether a stored plan document is eligible for execution is decided by
:func:`yggdrasil.contract.read.eligibility`, in one order and with closed
reason codes. The decision checks the document's family first: a document
whose ``schema_version`` is absent, invalid or names an unsupported family is
not eligible, whatever its status and tokens say. The token pair, the status
and whether the request was already executed are judged after it.

``is_plan_eligible``, ``get_eligibility_reason`` and ``APPROVED_STATUS`` are
kept for one release for code outside this repository; use
``yggdrasil.contract.read.is_eligible``, ``eligibility_reason`` and
``yggdrasil.contract.fields.APPROVED``.
"""

from yggdrasil.contract.fields import APPROVED
from yggdrasil.contract.read import eligibility_reason, is_eligible

__all__ = ["APPROVED_STATUS", "get_eligibility_reason", "is_plan_eligible"]

# The old names, for one release (see the module docstring): the same
# objects, so they answer exactly as the reader does.
is_plan_eligible = is_eligible
get_eligibility_reason = eligibility_reason
APPROVED_STATUS = APPROVED
