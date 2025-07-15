"""
loading_bar.py
--------------------------------

A Rich-powered utility that mimics Docker-style multi-layer progress with a
single **aggregate** bar on top and any number of **layer** bars underneath.

New in this version
~~~~~~~~~~~~~~~~~~~
* **Unit agnostic** – track *bytes* **or** *arbitrary steps* (e.g. epochs,
  items processed, test cases run). Switch with the `units` arg.
* **IDE Compatibility** - Works correctly in IDE terminals (like PyCharm)
  by forcing a live display.
* **Graceful Exit** - Handles `Ctrl+C` (KeyboardInterrupt) cleanly, showing a
  custom spinner with shutdown messages before exiting.
* **Robust Threaded Design** - The main logic now runs in a worker thread,
  allowing the main thread to remain responsive and catch `Ctrl+C` instantly,
  even during blocking I/O operations (like database calls).
* Clean, single-Live implementation (avoids *rich.errors.LiveError*).
* Minimal public API: call `simulate()` or import the helpers into your own
  workflow.

Quick start
-----------
```
pip install rich
python loading_bar.py            # bytes demo (default)
python loading_bar.py steps      # steps demo
```

You can also `import simulate` and feed it your own task list.

"""

from __future__ import annotations

import random
import time
import threading
import sys
import os
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


def _build_columns(units: str, overall: bool = False, title: str = "Running"):
    """Return a tuple of Rich column objects for *bytes* or *steps* mode.
    When overall=True, the first column shows the given title."""

    if units == "bytes":
        if overall:
            return (
                TextColumn(f"[bold green][+] {title}", justify="right"),
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
            TextColumn(f"[bold green][+] {title}", justify="right"),
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

    def __init__(self,
                 layers: List[Tuple[str, int]],
                 *,
                 units: str = "steps",
                 title: str = "Running"):
        """Manage a multi-layer progress display with an overall bar and individual layer bars.

        Parameters
        ----------
        layers : list[tuple[str, int]]
            (label, total) pairs for each layer.
        units : {"bytes", "steps"}
            Controls display style.
        title : str
            Title text shown next to the overall progress bar.
        """
        if units not in {"bytes", "steps"}:
            raise ValueError("units must be 'bytes' or 'steps'")
        # Determine if terminal supports interactive progress
        self.console = Console()
        term_env = os.environ.get('TERM', '')
        self.enabled = self.console.is_terminal and term_env.lower() != 'dumb'
        self.units = units
        self.title = title
        # If not enabled, skip rich progress and avoid spamming output
        if not self.enabled:
            # Fallback: no progress display
            self.grand_total = sum(total for _, total in layers)
            self.overall = None
            self.layers = None
            self.total_task = None
            self.task_ids = {}
            self.finished = False
            self.layout = None
            self.live = None
            return
        # Interactive mode: build rich Progress bars
        self.grand_total = sum(total for _, total in layers)
        self.overall = Progress(*_build_columns(units, overall=True, title=title), console=self.console)
        self.layers = Progress(*_build_columns(units), console=self.console)
        self.total_task = self.overall.add_task("overall", total=self.grand_total)
        self.task_ids = {name: self.layers.add_task(name, total=total) for name, total in layers}
        # Track completion
        self.finished = False
        self.layout = Group(self.overall, self.layers)
        self.live = None

        self.units = units
        self.title = title
        self.grand_total = sum(total for _, total in layers)
        self.overall = Progress(*_build_columns(units, overall=True, title=title), console=self.console)
        self.layers = Progress(*_build_columns(units), console=self.console)
        self.total_task = self.overall.add_task("overall", total=self.grand_total)
        self.task_ids = {name: self.layers.add_task(name, total=total) for name, total in layers}
        # A flag to check if all tasks are done
        self.finished = False
        self.layout = Group(self.overall, self.layers)
        self.live = None

    def __enter__(self):
        if not self.enabled:
            return self
        self.live = Live(self.layout, console=self.console, refresh_per_second=10)
        self.live.__enter__()
        return self

    def update(self, layer_name: str, advance: int = 1):
        """Advance the given layer and the overall bar by the specified amount."""
        if not self.enabled:
            return
        task_id = self.task_ids.get(layer_name)
        if task_id is None:
            raise KeyError(f"Unknown layer '{layer_name}'")
        self.layers.update(task_id, advance=advance)
        self.overall.update(self.total_task, advance=advance)
        # Check if all layer tasks are finished
        self.finished = all(task.finished for task in self.layers.tasks)


    def __exit__(self, exc_type, exc, tb):
        if not self.enabled:
            return
        if self.live:
            self.live.__exit__(exc_type, exc, tb)

# ────────────────────────────────────────────────────────────────────────────────
# Worker Function
# ────────────────────────────────────────────────────────────────────────────────

def worker_function(
    progress_manager: ProgressManager,
    layers: List[Tuple[str, int]],
    units: str,
    stop_event: threading.Event
):
    """This function contains the actual work being done.

    In a real application, this is where you would put your long-running,
    blocking calls (e.g., database queries, file processing).
    """
    while not progress_manager.finished:
        if stop_event.is_set():
            # The main thread has told us to stop.
            return

        for name, size in layers:
            task_id = progress_manager.task_ids[name]
            if progress_manager.layers.tasks[task_id].finished:
                continue

            if units == "bytes":
                chunk = random.randint(200_000, 2_000_000)
            else:
                chunk = random.randint(1, max(1, size // 100))

            progress_manager.update(name, advance=chunk)

        # This sleep simulates I/O latency or other work
        time.sleep(0.05)


# ────────────────────────────────────────────────────────────────────────────────
# CLI initialization spinner with funny snippets
# ────────────────────────────────────────────────────────────────────────────────
DEFAULT_SNIPPETS = [
    "Reticulating splines...",
    "Polishing the flux capacitor...",
    "Negotiating with the server elves...",
    "Counting to infinity...",
    "Charging the warp drive...",
    "Tickling the hamsters...",
    "Aligning bits to bytes...",
    "Spinning up the fun...",
    "Herding cats...",
    "Reheating pizza..."
]

class LoadingSpinner:
    """Simple CLI spinner with rotating missing dot and funny snippets."""

    def __init__(self, interval: float = 0.2, snippet_interval: float = 2.0, snippets: list[str] | None = None):
        # Enable spinner only on interactive terminals
        self.enabled = sys.stdout.isatty() and os.environ.get('TERM', '').lower() != 'dumb'
        self.interval = interval
        self.snippet_interval = snippet_interval
        self.snippets = snippets or DEFAULT_SNIPPETS
        self._stop_event = threading.Event()
        self._thread = None

    def _generate_frames(self) -> list[str]:
        frames = []
        total = 6
        for miss in range(total):
            symbols = ["●" if i != miss else " " for i in range(total)]
            frames.append("[" + "".join(symbols) + "]")
        return frames

    def _spin(self):
        frames = self._generate_frames()
        frame_count = len(frames)
        snippet_count = len(self.snippets)
        idx_frame = idx_snip = 0
        last_snip_time = time.time()
        sys.stdout.write("\r" + " " * 80 + "\r")
        sys.stdout.flush()
        while not self._stop_event.is_set():
            now = time.time()
            if now - last_snip_time >= self.snippet_interval:
                idx_snip = random.randrange(snippet_count)
                last_snip_time = now
            frame = frames[idx_frame]
            snippet = self.snippets[idx_snip]
            text = f"{frame} {snippet}"
            sys.stdout.write("\r" + text.ljust(80))
            sys.stdout.flush()
            time.sleep(self.interval)
            idx_frame = (idx_frame + 1) % frame_count

    def start(self):
        """Start the spinner in a background thread."""
        # Only start spinner if terminal supports it
        if not getattr(self, 'enabled', False):
            return
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._spin, daemon=True)
        self._thread.start()

    def stop(self):
        """Stop spinner and move to next line."""
        # Only stop spinner if it was started
        if not getattr(self, 'enabled', False):
            return
        self._stop_event.set()
        if self._thread:
            self._thread.join()
        # Clear spinner line
        try:
            sys.stdout.write("\r" + " " * 80 + "\r")
            sys.stdout.flush()
        except Exception:
            pass

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()


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

    # Set up the progress manager and threading events
    pm = ProgressManager(DEMO_LAYERS, units=mode)
    stop_event = threading.Event()

    # Set up and start the worker thread. It's a daemon so it will exit
    # when the main thread exits.
    worker = threading.Thread(
        target=worker_function,
        args=(pm, DEMO_LAYERS, mode, stop_event),
        daemon=True
    )

    try:
        # Use the ProgressManager as a context manager to handle the Live display
        with pm:
            worker.start()
            # This loop keeps the main thread alive and responsive to Ctrl+C
            # while the worker thread does its job.
            while worker.is_alive():
                # We use a non-blocking join to prevent this loop from
                # locking up the main thread.
                worker.join(timeout=0.1)

    except KeyboardInterrupt:
        # This block now runs immediately when you press Ctrl+C.
        console = Console()
        console.print("\n[bold yellow]Interruption received. Telling worker to stop...[/bold yellow]")
        stop_event.set()
        # Give the worker a moment to shut down
        worker.join()

        # Now run the shutdown spinner for a nice exit effect
        shutdown_snippets = [
            "Cleaning up...",
            "Putting the tools away...",
            "Turning off the lights...",
            "One moment...",
            "Shutting down gracefully...",
        ]
        with LoadingSpinner(snippets=shutdown_snippets):
            time.sleep(2)
        print("Process interrupted. Exiting.")
        sys.exit(0)

    except Exception as e:
        console = Console()
        console.print_exception()
        sys.exit(1)

