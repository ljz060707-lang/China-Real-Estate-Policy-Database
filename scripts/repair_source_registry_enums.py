from __future__ import annotations

import shutil
import types
from pathlib import Path
from typing import Union, get_args, get_origin

import yaml

from policydb.crawl.models import RegisteredSource

ROOT = Path(__file__).resolve().parents[1]
REGISTRY = ROOT / "data" / "reference" / "source_registry.yaml"
BACKUP = ROOT / "data" / "reference" / "backups" / "source_registry_before_enum_repair.yaml"


def literal_values(annotation):
    """Recursively extract Literal values from Optional/Union annotations."""
    origin = get_origin(annotation)
    if origin is None:
        return set()
    if str(origin).endswith("Literal"):
        return set(get_args(annotation))
    if origin in (Union, types.UnionType):
        values = set()
        for arg in get_args(annotation):
            values |= literal_values(arg)
        return values
    return set()


scope_allowed = literal_values(RegisteredSource.model_fields["scope_type"].annotation)
agency_allowed = literal_values(RegisteredSource.model_fields["agency_type"].annotation)

print("scope_type allowed:", sorted(scope_allowed))
print("agency_type allowed:", sorted(agency_allowed))

scope_aliases = {
    "city": ["municipal"],
}

agency_aliases = {
    "government_portal": ["municipal_government"],
    "government_office": ["municipal_government"],
    "housing_bureau": ["housing_department"],
    "housing_fund": [
        "housing_fund_center",
        "housing_department",
        "municipal_government",
    ],
    "natural_resources": [
        "natural_resources_department",
        "municipal_government",
    ],
}


def choose(field_name, current, aliases, allowed):
    if current in allowed:
        return current
    candidates = aliases.get(current, [])
    for candidate in candidates:
        if candidate in allowed:
            return candidate
    raise ValueError(
        f"{field_name}={current!r} 无法映射。"
        f" 模型允许值：{sorted(allowed)}"
    )


payload = yaml.safe_load(REGISTRY.read_text(encoding="utf-8"))
changed = []

for source in payload.get("sources", []):
    before_scope = source.get("scope_type")
    before_agency = source.get("agency_type")

    source["scope_type"] = choose(
        "scope_type", before_scope, scope_aliases, scope_allowed
    )
    source["agency_type"] = choose(
        "agency_type", before_agency, agency_aliases, agency_allowed
    )

    if (
        source["scope_type"] != before_scope
        or source["agency_type"] != before_agency
    ):
        changed.append(source.get("source_id"))

# Validate every row before writing anything.
errors = []
for index, source in enumerate(payload.get("sources", [])):
    try:
        RegisteredSource.model_validate(source)
    except Exception as exc:
        errors.append(
            {
                "index": index,
                "source_id": source.get("source_id"),
                "error": str(exc),
            }
        )

if errors:
    print("\n仍有验证错误，原文件未被改写：")
    for error in errors[:50]:
        print(error)
    raise SystemExit(1)

BACKUP.parent.mkdir(parents=True, exist_ok=True)
if REGISTRY.exists() and not BACKUP.exists():
    shutil.copy2(REGISTRY, BACKUP)

temp = REGISTRY.with_suffix(".yaml.tmp")
temp.write_text(
    yaml.safe_dump(
        payload,
        allow_unicode=True,
        sort_keys=False,
        width=1000,
    ),
    encoding="utf-8",
)
temp.replace(REGISTRY)

print(f"验证通过：{len(payload.get('sources', []))} 条")
print(f"修复记录：{len(changed)} 条")
print(f"备份：{BACKUP}")
print(f"写入：{REGISTRY}")
