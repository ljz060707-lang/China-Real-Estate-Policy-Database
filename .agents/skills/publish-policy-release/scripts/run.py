import subprocess
import sys

raise SystemExit(
    subprocess.call([sys.executable, "-m", "policydb.cli", "release", "--version", sys.argv[1]])
)
