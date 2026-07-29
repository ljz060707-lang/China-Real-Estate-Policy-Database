# CRPD Dashboard redesign fidelity review

Reference image: [`ui-reference/policy_center_qinghua_purple.png`](ui-reference/policy_center_qinghua_purple.png).

## Comparison record

| Reference requirement | Implementation evidence | Deviation | Resolution | Remaining adjustment |
| --- | --- | --- | --- | --- |
| White canvas, one Tsinghua-purple primary color, pale-purple selected states | `app/theme.py` defines `#4A148C`, `#5B1AA8`, `#6B21C8`, `#7C3AED`, `#F1E8FA`, and `#FAF7FD` | Streamlit's native control chrome remains visible | Theme rules constrain cards, tabs, tables, controls and Plotly to the same palette | Visual browser capture is blocked in this execution environment; capture again on a local desktop before a public design sign-off |
| Few primary navigation choices | `app/dashboard.py` exposes exactly six user-facing routes | Reference uses a horizontal header while the product requirement specifies a sidebar | Kept the requested sidebar and removed legacy pages from ordinary navigation | None for the information architecture |
| Left filters, central table/charts, right detail panel | `app/policy_center.py` uses a `1.2 : 4.2 : 1.7` three-column desktop layout | The reference uses icon-rich cards; the implementation deliberately avoids decorative icons | Four compact metrics, two default charts, paginated table and lazy detail view are present | Verify actual 1366 px wrapping in a local browser |
| Table as the central working surface | `policy_list()` is paginated in DuckDB and `policy_detail()` loads the body only after selection | Streamlit cannot render a per-row icon button inside its native dataframe | A dedicated `查看政策` selector updates the adjacent detail panel without navigation | Replace with a dataframe event/action column if Streamlit's stable row-selection API is adopted later |
| No rainbow charts or stacked card walls | `app/policy_center.py` uses five purple shades and no more than two default charts | Additional analytical charts remain behind tabs | Keeps first view focused while retaining analysis tools | None |

## Screenshot status

The expected local captures are:

- `outputs/ui/policy_center_desktop.png`
- `outputs/ui/update_completeness_desktop.png`

This repository's visual acceptance script should generate both from a local desktop browser at 1920 x 1080 after the dashboard is started. In this Codex runtime, the in-app browser is isolated from the host `127.0.0.1` listener and local Chrome headless terminates before GPU initialization, so no screenshot is treated as evidence of completion here. Functional Streamlit AppTests and the local health endpoint remain the verification evidence for this run.
