from __future__ import annotations

import sys
from pathlib import Path

from policydb.ingest.excel import inventory_excel

d = inventory_excel(Path(sys.argv[1]))
out = Path("docs/data_inventory.md")
targets = {
    "T1 房地产政策目录": "records/policies/documents/relations",
    "T2 城市房地产政策现状": "city_policy_rules + cell staging",
    "能级划分": "jurisdiction_attributes",
    "T4 2023年数量统计图表": "派生统计，仅 staging",
}
lines = [
    "# Excel 数据清单",
    "",
    f"源文件 SHA-256：`{d['sha256']}`。共 {d['sheet_count']} 个工作表（含隐藏表）。",
    "",
    "|序号|工作表|状态|非空行|非空列|公式|合并区域|性质/迁移目标|",
    "|---:|---|---|---:|---:|---:|---:|---|",
]
for s in d["sheets"]:
    default = "专题原始/人工编码 → records 或专题表；完整 cell staging"
    if any(x in s["sheet_name"] for x in ("简版", "横版", "视频", "汇总", "数量统计图表")):
        default = "派生展示 → 仅 staging/关联基础事件"
    lines.append(
        f"|{s['sheet_index']}|{s['sheet_name']}|{s['state']}|{s['nonempty_row_count']}|{s['nonempty_column_count']}|{s['formula_count']}|{s['merged_range_count']}|{targets.get(s['sheet_name'], default)}|"
    )
lines += [
    "",
    "## 无法可靠解释的问题",
    "",
    "- T2 的多层标题、跨列城市分块和 627 个合并区域无法全部确定性拆解；完整单元格与合并元数据已保留，未猜测。",
    "- T4 的公式、图表辅助列和能级虚拟变量属于派生内容，保留在 Staging，研究统计从 Curated 重算。",
    "- 旧表中未明确发布日期/生效日/失效日的值不从单一‘日期’列猜测。",
]
out.write_text("\n".join(lines), encoding="utf-8")
