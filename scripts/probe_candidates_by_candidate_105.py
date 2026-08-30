from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

from policydb.scope import load_cities_105
from policydb.source_slots import list_candidates


def configure_console() -> None:
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except AttributeError:
        pass


def decode_output(data: bytes) -> str:
    for encoding in ("utf-8", "gb18030", "cp936"):
        try:
            return data.decode(encoding)
        except UnicodeDecodeError:
            continue
    return data.decode("utf-8", errors="replace")


def kill_process_tree(pid: int) -> None:
    subprocess.run(
        ["taskkill", "/PID", str(pid), "/T", "/F"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )


def progress_bar(value: int, total: int, width: int = 30) -> str:
    if total <= 0:
        return "[NO DATA]"

    ratio = max(0.0, min(1.0, value / total))
    filled = int(ratio * width)

    return (
        "["
        + "#" * filled
        + "-" * (width - filled)
        + f"] {value}/{total}  {ratio * 100:5.1f}%"
    )


def format_elapsed(seconds: float) -> str:
    seconds = int(seconds)
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)

    if hours:
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

    return f"{minutes:02d}:{seconds:02d}"


def load_state(path: Path) -> dict:
    if not path.exists():
        return {"completed": {}, "failed": {}}

    try:
        data = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, ValueError, TypeError):
        return {"completed": {}, "failed": {}}

    completed = data.get("completed", {})
    failed = data.get("failed", {})

    if isinstance(completed, list):
        completed = {
            str(item["candidate_id"]): item
            for item in completed
            if item.get("candidate_id")
        }

    if isinstance(failed, list):
        failed = {
            str(item["candidate_id"]): item
            for item in failed
            if item.get("candidate_id")
        }

    return {
        "completed": completed if isinstance(completed, dict) else {},
        "failed": failed if isinstance(failed, dict) else {},
    }


def save_state(
    path: Path,
    *,
    status: str,
    completed: dict,
    failed: dict,
    total: int,
    current: dict | None = None,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    payload = {
        "status": status,
        "updated_at": datetime.now().astimezone().isoformat(),
        "total_candidates": total,
        "completed_count": len(completed),
        "failed_count": len(failed),
        "current": current,
        "completed": completed,
        "failed": failed,
    }

    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    os.replace(temporary, path)


def already_probed(row: dict, rounds: int) -> bool:
    try:
        count = int(row.get("health_probe_count") or 0)
    except (TypeError, ValueError):
        count = 0

    return bool(row.get("last_checked_at")) and count >= rounds


def safe_filename(value: str) -> str:
    result = re.sub(r"[^A-Za-z0-9_-]+", "_", value)
    return result[:80] or "candidate"


def main() -> int:
    configure_console()

    parser = argparse.ArgumentParser()
    parser.add_argument("--rounds", type=int, default=2)
    parser.add_argument(
        "--candidate-timeout-seconds",
        type=int,
        default=180,
    )
    parser.add_argument(
        "--refresh-seconds",
        type=int,
        default=5,
    )
    parser.add_argument(
        "--data-root",
        default=r"E:\Data Set\CRPD",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-probe candidates already successfully checked.",
    )
    args = parser.parse_args()

    if args.rounds < 2:
        raise ValueError("--rounds must be at least 2")

    repo = Path(__file__).resolve().parents[1]
    data_root = Path(args.data_root)
    policydb = repo / ".venv" / "Scripts" / "policydb.exe"

    if not policydb.exists():
        raise FileNotFoundError(f"policydb.exe not found: {policydb}")

    state_path = (
        data_root
        / "control"
        / "source_completion_105"
        / "probe_by_candidate_state.json"
    )

    log_dir = (
        data_root
        / "logs"
        / "source_completion_105"
        / "probe_by_candidate"
    )
    log_dir.mkdir(parents=True, exist_ok=True)

    environment = os.environ.copy()
    environment["CRPD_DATA_ROOT"] = str(data_root)
    environment["PYTHONUTF8"] = "1"
    environment["PYTHONIOENCODING"] = "utf-8"

    city_frame = load_cities_105()
    city_lookup = {
        str(row["city_id"]): str(row["city_name"])
        for row in city_frame.to_dicts()
    }

    frame = list_candidates()

    if frame.is_empty():
        print("No source candidates found.")
        return 0

    sort_columns = [
        column
        for column in ("city_id", "source_role", "candidate_id")
        if column in frame.columns
    ]

    if sort_columns:
        frame = frame.sort(sort_columns)

    candidates = [
        row
        for row in frame.to_dicts()
        if bool(row.get("is_official"))
        and (
            bool(row.get("entry_eligible"))
            or str(row.get("candidate_kind") or "")
            == "site_or_column_entry"
        )
        and str(row.get("page_type") or "") not in {
            "policy_detail",
            "content_page",
            "policy_content_page",
            "pdf",
        }
    ]
    total = len(candidates)

    state = load_state(state_path)
    valid_candidate_ids = {
        str(row.get("candidate_id") or "")
        for row in candidates
        if row.get("candidate_id")
    }

    completed = {
        str(candidate_id): record
        for candidate_id, record in dict(state["completed"]).items()
        if str(candidate_id) in valid_candidate_ids
    }

    failed = {
        str(candidate_id): record
        for candidate_id, record in dict(state["failed"]).items()
        if str(candidate_id) in valid_candidate_ids
    }

    # Trust existing database probe evidence as completed work.
    if not args.force:
        for row in candidates:
            candidate_id = str(row.get("candidate_id") or "")
            if candidate_id and already_probed(row, args.rounds):
                completed.setdefault(
                    candidate_id,
                    {
                        "candidate_id": candidate_id,
                        "city_id": str(row.get("city_id") or ""),
                        "source_role": str(row.get("source_role") or ""),
                        "status": "EXISTING_DATABASE_EVIDENCE",
                    },
                )

    print("=" * 76)
    print(" CRPD SOURCE PROBE - CANDIDATE LEVEL")
    print("=" * 76)
    print(f"Candidates:        {total}")
    print(f"Already completed: {len(completed)}")
    print(f"Previous failures: {len(failed)}")
    print(f"Timeout/candidate: {args.candidate_timeout_seconds}s")
    print(f"State:             {state_path}")
    print(f"Logs:              {log_dir}")
    print()

    current_process: subprocess.Popen | None = None

    try:
        for index, row in enumerate(candidates, start=1):
            candidate_id = str(row.get("candidate_id") or "")
            city_id = str(row.get("city_id") or "")
            source_role = str(row.get("source_role") or "")
            city_name = city_lookup.get(city_id, city_id)

            if not candidate_id:
                continue

            if candidate_id in completed and not args.force:
                continue

            current = {
                "index": index,
                "candidate_id": candidate_id,
                "city_id": city_id,
                "city_name": city_name,
                "source_role": source_role,
            }

            save_state(
                state_path,
                status="RUNNING",
                completed=completed,
                failed=failed,
                total=total,
                current=current,
            )

            name = (
                f"{index:04d}_"
                f"{safe_filename(city_id)}_"
                f"{safe_filename(source_role)}_"
                f"{safe_filename(candidate_id)}"
            )

            raw_log = log_dir / f"{name}.raw"
            text_log = log_dir / f"{name}.log"

            command = [
                str(policydb),
                "sources",
                "probe-candidates",
                "--candidate-id",
                candidate_id,
                "--rounds",
                str(args.rounds),
            ]

            print()
            print(
                f"[{index:04d}/{total}] START "
                f"{city_name} | {source_role}"
            )
            print(f"Candidate: {candidate_id}")

            started = time.monotonic()
            timed_out = False

            with raw_log.open("wb") as raw_handle:
                current_process = subprocess.Popen(
                    command,
                    cwd=repo,
                    env=environment,
                    stdout=raw_handle,
                    stderr=subprocess.STDOUT,
                    creationflags=subprocess.CREATE_NEW_PROCESS_GROUP,
                )

                while current_process.poll() is None:
                    elapsed = time.monotonic() - started
                    processed = len(completed) + len(failed)

                    print(
                        (
                            "\r"
                            + progress_bar(processed, total)
                            + f"  Current: {city_name}/{source_role}"
                            + f"  Elapsed: {format_elapsed(elapsed)}"
                        ).ljust(170),
                        end="",
                        flush=True,
                    )

                    if elapsed >= args.candidate_timeout_seconds:
                        timed_out = True
                        kill_process_tree(current_process.pid)
                        break

                    time.sleep(max(1, args.refresh_seconds))

                exit_code = current_process.wait()
                current_process = None

            print()

            raw_data = raw_log.read_bytes() if raw_log.exists() else b""
            decoded = decode_output(raw_data)
            text_log.write_text(
                decoded,
                encoding="utf-8",
                errors="replace",
            )

            record = {
                **current,
                "finished_at": datetime.now().astimezone().isoformat(),
                "elapsed_seconds": round(time.monotonic() - started, 1),
                "exit_code": exit_code,
                "timed_out": timed_out,
                "log_path": str(text_log),
                "raw_log_path": str(raw_log),
            }

            if exit_code == 0 and not timed_out:
                record["status"] = "COMPLETED"
                completed[candidate_id] = record
                failed.pop(candidate_id, None)

                print(
                    f"[{index:04d}/{total}] OK   "
                    f"{city_name} | {source_role} | "
                    f"{format_elapsed(record['elapsed_seconds'])}"
                )
            else:
                record["status"] = (
                    "TIMEOUT" if timed_out else f"EXIT_{exit_code}"
                )
                failed[candidate_id] = record

                print(
                    f"[{index:04d}/{total}] FAIL "
                    f"{city_name} | {source_role} | "
                    f"{record['status']}"
                )

            save_state(
                state_path,
                status="RUNNING",
                completed=completed,
                failed=failed,
                total=total,
                current=current,
            )

    except KeyboardInterrupt:
        print("\nStopping current candidate...")

        if current_process is not None:
            kill_process_tree(current_process.pid)

        save_state(
            state_path,
            status="INTERRUPTED",
            completed=completed,
            failed=failed,
            total=total,
            current=None,
        )

        print("Progress saved. Run the same command to resume.")
        return 130

    final_status = "COMPLETED" if not failed else "PARTIAL"

    save_state(
        state_path,
        status=final_status,
        completed=completed,
        failed=failed,
        total=total,
        current=None,
    )

    print()
    print("=" * 76)
    print(f"Status:    {final_status}")
    print(f"Completed: {len(completed)}/{total}")
    print(f"Failed:    {len(failed)}")
    print(f"State:     {state_path}")
    print(f"Logs:      {log_dir}")
    print("=" * 76)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

