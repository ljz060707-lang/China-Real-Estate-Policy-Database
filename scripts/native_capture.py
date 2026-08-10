from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


def decode_output(data: bytes) -> str:
    for encoding in ("utf-8", "gb18030", "cp936"):
        try:
            return data.decode(encoding)
        except UnicodeDecodeError:
            continue
    return data.decode("utf-8", errors="replace")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--log", required=True)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()

    command = list(args.command)
    if command and command[0] == "--":
        command = command[1:]

    if not command:
        print("No child command supplied.", file=sys.stderr)
        return 2

    log_path = Path(args.log)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    raw_path = log_path.with_suffix(log_path.suffix + ".raw")

    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except AttributeError:
        pass

    with (
        log_path.open("w", encoding="utf-8", errors="replace") as text_log,
        raw_path.open("wb") as raw_log,
    ):
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            bufsize=0,
        )

        assert process.stdout is not None

        while True:
            chunk = process.stdout.readline()
            if not chunk:
                break

            raw_log.write(chunk)
            raw_log.flush()

            text = decode_output(chunk)
            text_log.write(text)
            text_log.flush()

            print(text, end="", flush=True)

        return int(process.wait())


if __name__ == "__main__":
    raise SystemExit(main())
