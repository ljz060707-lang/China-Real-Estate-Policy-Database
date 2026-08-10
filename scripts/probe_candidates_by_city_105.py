from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

from policydb.scope import load_cities_105


def configure_console() -> None:
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except AttributeError:
        pass


def decode_bytes(data: bytes) -> str:
    for encoding in ("utf-8", "gb18030", "cp936"):
        try:
            return data.decode(encoding)
        except UnicodeDecodeError:
            continue
    return data.decode("utf-8", errors="replace")


def progress_bar(value: int, total: int, width: int = 32) -> str:
    if total <= 0:
        return "[NO DATA]"

    ratio = max(0.0, min(1.0, value / total))
    filled = int(ratio * width)
    percent = ratio * 100

    return (
        "[" + "#" * filled + "-" * (width - filled) + "] "
        f"{value}/{total}  {percent:5.1f}%"
    )


def elapsed_text(seconds: float) -> str:
    seconds = int(seconds)
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    if hours:
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

    return f"{minutes:02d}:{seconds:02d}"


def load_state(path: Path) -> dict:
    if not path.exists():
        return {
            "status": "NEW",
            "completed": [],
            "failed": [],
        }

    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, ValueError, TypeError):
        return {
            "status": "RECOVERED_EMPTY",
            "completed": [],
            "failed": [],
        }


def save_state(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")

    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    os.replace(temporary, path)


def kill_process_tree(pid: int) -> None:
    subprocess.run(
        ["taskkill", "/PID", str(pid), "/T", "/F"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )


def main() -> int:
    configure_console()

    parser = argparse.ArgumentParser()
    parser.add_argument("--rounds", type=int, default=2)
    parser.add_argument("--timeout-minutes", type=int, default=15)
    parser.add_argument("--refresh-seconds", type=int, default=5)
    parser.add_argument(
        "--data-root",
        default=r"D:\Data Set\CRPD",
    )
    args = parser.parse_args()

    if not 2 <= args.rounds <= 10:
        raise ValueError("--rounds must be between 2 and 10")

    repo = Path(__file__).resolve().parents[1]
    data_root = Path(args.data_root)
    policydb = repo / ".venv" / "Scripts" / "policydb.exe"

    if not policydb.exists():
        raise FileNotFoundError(f"policydb.exe not found: {policydb}")

    state_path = (
        data_root
        / "control"
        / "source_completion_105"
        / "probe_by_city_state_v2.json"
    )

    log_dir = (
        data_root
        / "logs"
        / "source_completion_105"
        / "probe_by_city_v2"
    )
    log_dir.mkdir(parents=True, exist_ok=True)

    environment = os.environ.copy()
    environment["CRPD_DATA_ROOT"] = str(data_root)
    environment["PYTHONUTF8"] = "1"
    environment["PYTHONIOENCODING"] = "utf-8"

    cities_frame = load_cities_105().select(
        ["city_id", "city_name"]
    )
    cities = cities_frame.to_dicts()

    state = load_state(state_path)

    completed = {
        str(item["city_id"]): item
        for item in state.get("completed", [])
        if item.get("city_id")
    }

    failed = {
        str(item["city_id"]): item
        for item in state.get("failed", [])
        if item.get("city_id")
    }

    total = len(cities)

    print("=" * 68)
    print(" CRPD CANDIDATE NETWORK PROBE - 105 CITIES")
    print("=" * 68)
    print(f"Already completed: {len(completed)}/{total}")
    print(f"Previous failures: {len(failed)}")
    print(f"State: {state_path}")
    print(f"Logs:  {log_dir}")
    print()

    current_process: subprocess.Popen | None = None

    try:
        for index, city in enumerate(cities, start=1):
            city_id = str(city["city_id"])
            city_name = str(city["city_name"])

            if city_id in completed:
                print(
                    f"[{index:03d}/{total}] SKIP "
                    f"{city_name} ({city_id})"
                )
                continue

            safe_city_id = "".join(
                character
                if character.isalnum() or character in "_-"
                else "_"
                for character in city_id
            )

            raw_log = log_dir / f"{index:03d}_{safe_city_id}.raw"
            text_log = log_dir / f"{index:03d}_{safe_city_id}.log"

            command = [
                str(policydb),
                "sources",
                "probe-candidates",
                "--city",
                city_id,
                "--rounds",
                str(args.rounds),
            ]

            state_payload = {
                "status": "RUNNING",
                "updated_at": datetime.now().astimezone().isoformat(),
                "total_cities": total,
                "completed_count": len(completed),
                "failed_count": len(failed),
                "current_index": index,
                "current_city_id": city_id,
                "current_city_name": city_name,
                "rounds": args.rounds,
                "completed": list(completed.values()),
                "failed": list(failed.values()),
            }
            save_state(state_path, state_payload)

            print()
            print(
                f"[{index:03d}/{total}] START "
                f"{city_name} ({city_id})"
            )

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
                    raw_size = (
                        raw_log.stat().st_size
                        if raw_log.exists()
                        else 0
                    )

                    done = len(completed)

                    line = (
                        "\r"
                        + progress_bar(done, total)
                        + f"  Current: {city_name}"
                        + f"  Elapsed: {elapsed_text(elapsed)}"
                        + f"  Output: {raw_size / 1024:.1f} KB"
                    )

                    print(line.ljust(150), end="", flush=True)

                    if elapsed > args.timeout_minutes * 60:
                        timed_out = True
                        kill_process_tree(current_process.pid)
                        break

                    time.sleep(max(1, args.refresh_seconds))

                exit_code = current_process.wait()
                current_process = None

            print()

            raw_data = (
                raw_log.read_bytes()
                if raw_log.exists()
                else b""
            )
            decoded = decode_bytes(raw_data)
            text_log.write_text(
                decoded,
                encoding="utf-8",
                errors="replace",
            )

            record = {
                "city_id": city_id,
                "city_name": city_name,
                "finished_at": datetime.now()
                .astimezone()
                .isoformat(),
                "exit_code": exit_code,
                "timed_out": timed_out,
                "elapsed_seconds": round(
                    time.monotonic() - started,
                    1,
                ),
                "log_path": str(text_log),
                "raw_log_path": str(raw_log),
            }

            if exit_code == 0 and not timed_out:
                completed[city_id] = record
                failed.pop(city_id, None)

                print(
                    f"[{index:03d}/{total}] OK   "
                    f"{city_name}  "
                    f"elapsed={elapsed_text(record['elapsed_seconds'])}"
                )
            else:
                failed[city_id] = record

                reason = (
                    "TIMEOUT"
                    if timed_out
                    else f"EXIT={exit_code}"
                )

                print(
                    f"[{index:03d}/{total}] FAIL "
                    f"{city_name}  {reason}"
                )

            state_payload = {
                "status": "RUNNING",
                "updated_at": datetime.now().astimezone().isoformat(),
                "total_cities": total,
                "completed_count": len(completed),
                "failed_count": len(failed),
                "current_index": index,
                "current_city_id": city_id,
                "current_city_name": city_name,
                "rounds": args.rounds,
                "completed": list(completed.values()),
                "failed": list(failed.values()),
            }
            save_state(state_path, state_payload)

    except KeyboardInterrupt:
        print("\nStopping current city process...")

        if current_process is not None:
            kill_process_tree(current_process.pid)

        save_state(
            state_path,
            {
                "status": "INTERRUPTED",
                "updated_at": datetime.now()
                .astimezone()
                .isoformat(),
                "total_cities": total,
                "completed_count": len(completed),
                "failed_count": len(failed),
                "completed": list(completed.values()),
                "failed": list(failed.values()),
            },
        )

        print("Progress saved. Run the same command to resume.")
        return 130

    final_status = (
        "COMPLETED"
        if not failed
        else "PARTIAL"
    )

    save_state(
        state_path,
        {
            "status": final_status,
            "updated_at": datetime.now()
            .astimezone()
            .isoformat(),
            "total_cities": total,
            "completed_count": len(completed),
            "failed_count": len(failed),
            "completed": list(completed.values()),
            "failed": list(failed.values()),
        },
    )

    print()
    print("=" * 68)
    print(f"Status:    {final_status}")
    print(f"Completed: {len(completed)}/{total}")
    print(f"Failed:    {len(failed)}")
    print(f"State:     {state_path}")
    print(f"Logs:      {log_dir}")
    print("=" * 68)

    return 0 if not failed else 2


if __name__ == "__main__":
    raise SystemExit(main())
