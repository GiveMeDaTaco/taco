"""
loading_bar.py
--------------------------------

A Rich‑powered utility that mimics Docker‑style multi‑layer progress with a
single **aggregate** bar on top and any number of **layer** bars underneath.

New in this version
~~~~~~~~~~~~~~~~~~~
* **Unit agnostic** – track *bytes* **or** *arbitrary steps* (e.g. epochs,
  items processed, test cases run). Switch with the `units` arg.
* Clean, single‑Live implementation (avoids *rich.errors.LiveError*).
* Minimal public API: call `simulate()` or import the helpers into your own
  workflow.

Quick start
-----------
```
pip install rich
python docker_style_overall_progress.py            # bytes demo (default)
python docker_style_overall_progress.py steps      # steps demo
```

You can also `import simulate` and feed it your own task list.

API
---
```python
def simulate(layers: List[Tuple[str, int]], *, units: str = "bytes") -> None:
    '''Render an overall bar plus one bar per layer.

    Parameters
    ----------
    layers : list[tuple[str, int]]
        (label, total) pairs. *total* is bytes when units="bytes" or raw
        step counts when units="steps".
    units : {"bytes", "steps"}, default "bytes"
        Controls which columns are shown:
        * "bytes"  → size, speed, ETA columns (like Docker)
        * "steps"  → simple "completed/total steps" column instead.
    '''
```
"""

from __future__ import annotations

import random
import time
from typing import List, Tuple

from rich.console import Console, Group
from rich.live import Live
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)

# ────────────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────────────


def _build_columns(units: str, overall: bool = False):
    """Return a tuple of Rich column objects for *bytes* or *steps* mode."""

    if units == "bytes":
        if overall:
            return (
                TextColumn("[bold green][+] Running", justify="right"),
                BarColumn(bar_width=None, complete_style="cyan"),
                DownloadColumn(binary_units=True),
                TimeRemainingColumn(),
            )
        return (
            SpinnerColumn(style="bold magenta"),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(bar_width=None),
            DownloadColumn(binary_units=True),
            TransferSpeedColumn(),
            TimeRemainingColumn(),
        )

    # steps mode
    if overall:
        return (
            TextColumn("[bold green][+] Running", justify="right"),
            BarColumn(bar_width=None, complete_style="cyan"),
            TextColumn("[progress.percentage]{task.completed}/{task.total} steps"),
            TimeRemainingColumn(),
        )
    return (
        SpinnerColumn(style="bold magenta"),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(bar_width=None),
        TextColumn("{task.completed}/{task.total} steps"),
        TimeRemainingColumn(),
    )
   
# ────────────────────────────────────────────────────────────────────────────────
# ProgressManager: reusable class for multi-layer progress bars
# ────────────────────────────────────────────────────────────────────────────────
class ProgressManager:
    """Manage a multi-layer progress display with an overall bar and individual layer bars.

    Parameters
    ----------
    layers : List[Tuple[str, int]]
        List of (label, total) pairs for each layer.
    units : str
        "bytes" or "steps", controls display style.
    """

    def __init__(self, layers: List[Tuple[str, int]], *, units: str = "steps"):
        if units not in {"bytes", "steps"}:
            raise ValueError("units must be 'bytes' or 'steps'")
        self.console = Console()
        self.units = units
        # Sum totals for overall progress
        self.grand_total = sum(total for _, total in layers)
        # Create Progress instances
        self.overall = Progress(*_build_columns(units, overall=True))
        self.layers = Progress(*_build_columns(units))
        # Add overall and layer tasks
        self.total_task = self.overall.add_task("overall", total=self.grand_total)
        self.task_ids = { name: self.layers.add_task(name, total=total) for name, total in layers }
        # Group for live layout
        self.layout = Group(self.overall, self.layers)
        self.live = None

    def __enter__(self):
        self.live = Live(self.layout, console=self.console, refresh_per_second=10)
        self.live.__enter__()
        return self

    def update(self, layer_name: str, advance: int = 1):
        """Advance the given layer and the overall bar by the specified amount."""
        task_id = self.task_ids.get(layer_name)
        if task_id is None:
            raise KeyError(f"Unknown layer '{layer_name}'")
        self.layers.update(task_id, advance=advance)
        self.overall.update(self.total_task, advance=advance)

    def __exit__(self, exc_type, exc, tb):
        if self.live:
            self.live.__exit__(exc_type, exc, tb)


# ────────────────────────────────────────────────────────────────────────────────
# Main simulation logic
# ────────────────────────────────────────────────────────────────────────────────


def simulate(layers: List[Tuple[str, int]], *, units: str = "bytes") -> None:
    """Render an overall bar + individual layer bars until all layers finish."""

    if units not in {"bytes", "steps"}:
        raise ValueError("units must be 'bytes' or 'steps'")

    console = Console()
    grand_total = sum(size for _, size in layers)

    overall_progress = Progress(*_build_columns(units, overall=True))
    layers_progress = Progress(*_build_columns(units))

    total_task = overall_progress.add_task("overall", total=grand_total)
    per_layer_ids = {
        name: layers_progress.add_task(name, total=size) for name, size in layers
    }

    layout = Group(overall_progress, layers_progress)

    with Live(layout, console=console, refresh_per_second=10):
        while not layers_progress.finished:
            for name, size in layers:
                tid = per_layer_ids[name]
                if layers_progress.tasks[tid].finished:
                    continue

                # Determine synthetic 'work' chunk
                if units == "bytes":
                    chunk = random.randint(200_000, 2_000_000)  # 200 KB–2 MB
                else:
                    chunk = random.randint(1, max(1, size // 100))  # ~1% steps

                layers_progress.update(tid, advance=chunk)
                overall_progress.update(total_task, advance=chunk)

            time.sleep(0.05)


# ────────────────────────────────────────────────────────────────────────────────
# CLI demo
# ────────────────────────────────────────────────────────────────────────────────


if __name__ == "__main__":
    import sys

    mode = sys.argv[1].lower() if len(sys.argv) > 1 else "bytes"

    DEMO_LAYERS = [
        ("a9c6b9f9: Pulling fs layer", 120_000_000 if mode == "bytes" else 500),
        ("5b4d5e38: Pulling fs layer", 180_000_000 if mode == "bytes" else 800),
        ("d18f1171: Downloading", 100_000_000 if mode == "bytes" else 400),
        ("e2f3c1d2: Downloading", 160_000_000 if mode == "bytes" else 700),
    ]

    simulate(DEMO_LAYERS, units=mode)