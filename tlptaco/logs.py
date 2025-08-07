"""tlptaco.logs – lightweight log-inspection CLI utility

This standalone helper **does not interfere** with the main tlptaco pipeline.
It simply parses a tlptaco log file and, restricted to the *most recent run*,
either prints a summary of log-level counts or echoes the selected lines.

Typical usage::

    # Show counts for the latest run (all levels)
    python -m tlptaco.logs -f logs/tlptaco_poc.log --summary

    # Print only ERROR and WARNING lines
    python -m tlptaco.logs -f logs/tlptaco_poc.log -l error warning --print

If neither ``--summary`` nor ``--print`` is supplied we default to
``--summary`` so the command always produces output.
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Iterable, Mapping

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")


def _extract_level(line: str) -> str | None:
    """Return the log level found in *line* or ``None``.

    We search for the *first* occurrence of one of the standard level names
    bounded by word boundaries.  This is robust against the emoji prefix and
    other decorations added by EmojiFormatter.
    """

    m = re.search(r"\b(DEBUG|INFO|WARNING|ERROR|CRITICAL)\b", line)
    if m:
        return m.group(1)
    return None


def _slice_to_last_run(lines: list[str]) -> list[str]:
    """Return only the lines *after* the last **TLPTACO RUN START** header.

    Each tlptaco run writes a distinctive multi-line header – the middle line
    contains the marker text.  We locate the *last* occurrence so that old
    runs earlier in the file do not contaminate the analysis.
    """

    for idx in range(len(lines) - 1, -1, -1):  # iterate backwards
        if "TLPTACO RUN START" in lines[idx]:
            return lines[idx + 1 :]
    return lines  # marker not found – analyse full file


def parse_log(
    file_path: str | Path,
    levels: Iterable[str] | None = None,
) -> tuple[Mapping[str, int], list[str]]:
    """Parse *file_path* and return (counts, matching_lines).

    Parameters
    ----------
    file_path : str or pathlib.Path
        Log file to read.
    levels : iterable[str] | None
        Which log levels to include (case-insensitive).  ``None`` ⇒ all.
    """

    path = Path(file_path)
    if not path.is_file():
        raise FileNotFoundError(path)

    wanted = {lvl.upper() for lvl in (levels or _LEVELS)}

    with path.open("r", encoding="utf-8", errors="replace") as f:
        lines = f.read().splitlines()

    lines = _slice_to_last_run(lines)

    counts = {lvl: 0 for lvl in wanted}
    matched_lines: list[str] = []

    for line in lines:
        lvl = _extract_level(line)
        if lvl is None:
            continue
        if lvl.upper() in wanted:
            counts[lvl.upper()] += 1
            matched_lines.append(line)

    return counts, matched_lines


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="tlptaco.logs",
        description="Inspect tlptaco log files (latest run only)",
    )
    p.add_argument(
        "-f",
        "--file",
        required=True,
        help="Path to the tlptaco *.log* file",
    )
    p.add_argument(
        "-l",
        "--levels",
        nargs="+",
        metavar="LEVEL",
        choices=_LEVELS,
        type=str.upper,
        help="One or more levels to consider (default: all)",
    )
    grp = p.add_mutually_exclusive_group()
    grp.add_argument(
        "--summary",
        action="store_true",
        help="Print a count of matching lines per level",
    )
    grp.add_argument(
        "--print",
        dest="print_lines",
        action="store_true",
        help="Print the matching log lines themselves",
    )
    # Note: We allow neither option which we will treat as --summary later.
    return p


def main(argv: list[str] | None = None):  # pragma: no cover – entry-point
    parser = _build_arg_parser()
    args = parser.parse_args(argv)

    # Default behaviour: summary if user didn’t pick anything
    if not args.summary and not args.print_lines:
        args.summary = True

    counts, matched = parse_log(args.file, levels=args.levels)

    if args.summary:
        total = sum(counts.values())
        if total == 0:
            print("No matching log lines found for the last run.")
        else:
            # Preserve display order using _LEVELS constant
            parts = [f"{lvl}: {counts.get(lvl, 0)}" for lvl in _LEVELS if lvl in counts]
            print(", ".join(parts))

    if args.print_lines:
        if not matched:
            print("No matching log lines found for the last run.")
        else:
            for line in matched:
                print(line)


if __name__ == "__main__":  # pragma: no cover – module executed directly
    main()
