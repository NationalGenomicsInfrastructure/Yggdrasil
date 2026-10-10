def partition_key(scope: dict) -> str:
    """
    Partition key for a partitioned Couch DB, based on plan/step scope.
    Examples:
      {"kind":"project","id":"P12345"}   -> "proj-P12345"
      {"kind":"flowcell","id":"A22FN2"} -> "fc-A22FN2"
      {"kind":"bundle","id":"B12345"}   -> "bundle-B12345"
    """
    kind = scope.get("kind")
    sid = scope.get("id")
    if not kind or not sid:
        raise ValueError(f"Bad scope: {scope}")
    if kind == "project":
        return f"proj-{sid}"
    if kind == "flowcell":
        return f"fc-{sid}"
    # if kind == "bundle":   return f"bundle-{sid}"
    return f"{kind}-{sid}"
