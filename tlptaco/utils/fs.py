"""Filesystem permission helpers used across tlptaco.

The project needs every artefact it creates (logs, output files, Excel
reports, etc.) to be accessible by members of the group that owns the working
directory.  We therefore:

1. Detect that group (``gid`` via ``os.stat(os.getcwd())``).
2. Attempt ``os.chown(path, -1, gid)`` to change group ownership *only*.
3. Apply UNIX mode ``0o770`` (rwx for owner + group, none for others).

All operations ignore errors such as lack of permission or unavailability of
``os.chown`` on the platform.
"""

from __future__ import annotations

import os
import stat
import logging


def _get_workdir_gid() -> int | None:
    """Return the *group id* (gid) of the current working directory.

    Returns ``None`` when the information is not available (e.g. Windows).
    """
    try:
        return os.stat(os.getcwd()).st_gid
    except Exception:
        return None


def grant_group_rwx(path: str):
    """Grant *rwx* (770) to owner & workdir group on *path* (file or dir)."""
    gid = _get_workdir_gid()
    # ------------------------------------------------------------------
    # Attempt chown (group only)
    # ------------------------------------------------------------------
    if gid is not None and hasattr(os, "chown"):
        try:
            os.chown(path, -1, gid)
        except PermissionError:
            logging.getLogger("tlptaco.fs").debug(
                "No permission to chown %s", path, exc_info=False
            )
        except Exception:
            # ignore other issues (e.g. not supported FS)
            pass

    # ------------------------------------------------------------------
    # chmod 770 (owner+group full access)
    # ------------------------------------------------------------------
    mode = (
        stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR |  # owner
        stat.S_IRGRP | stat.S_IWGRP | stat.S_IXGRP    # group
    )
    try:
        os.chmod(path, mode)
    except PermissionError:
        logging.getLogger("tlptaco.fs").debug(
            "No permission to chmod %s", path, exc_info=False
        )
    except Exception:
        pass
