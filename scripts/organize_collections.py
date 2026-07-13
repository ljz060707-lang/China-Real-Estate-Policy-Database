from __future__ import annotations

import json

from policydb.query.database import build_database
from policydb.transform.collections import build_collection_layer

if __name__ == "__main__":
    report = build_collection_layer()
    build_database()
    print(json.dumps(report, ensure_ascii=False, indent=2))
