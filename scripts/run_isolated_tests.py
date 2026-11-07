#!/usr/bin/env python3
"""Run the retained backend regressions with synthetic fixtures only."""
import argparse
import os
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()
    os.chdir(ROOT)
    sys.path[:0] = [str(ROOT), str(ROOT / "tests")]
    suite = unittest.defaultTestLoader.discover(str(ROOT / "tests"), pattern="test_*.py")
    print(f"Synthetic backend tests: {suite.countTestCases()}", flush=True)
    result = unittest.TextTestRunner(verbosity=2 if args.verbose else 1).run(suite)
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())
