"""
Copyright 2025 Adobe
All Rights Reserved.

NOTICE: Adobe permits you to use, modify, and distribute this file in accordance
with the terms of the Adobe license agreement accompanying it.
"""
import subprocess
from pathlib import Path


def main():
    base_dir = Path(__file__).parent.parent
    base_version_file = base_dir / "BASE_VERSION"

    base_version = base_version_file.read_text(encoding="utf8").strip()

    major, minor = map(int, base_version.split(".")[:2])

    try:
        commit_count = (
            subprocess.check_output(["git", "rev-list", "--count", "HEAD"])
            .strip()
            .decode("utf-8")
        )
    except Exception:
        commit_count = "0"

    version_file = base_dir / "dysql" / "version.py"

    version = f"{major}.{minor}.{commit_count}"
    version_file.write_text(f'__version__ = "{version}"\n', encoding="utf8")


if __name__ == "__main__":
    main()
