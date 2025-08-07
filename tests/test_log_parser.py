"""Unit tests for tlptaco.logs utility."""

import textwrap


import tlptaco.logs as log_util


SAMPLE_LOG = textwrap.dedent(
    """
    ================================
    TLPTACO RUN START 2025-08-06 10:00:00
    ================================
    ℹ️  10:00:00 root INFO: first run message
    ❌ 10:00:01 root ERROR: something bad
    🔥 10:00:02 root CRITICAL: very bad

    ================================
    TLPTACO RUN START 2025-08-07 12:00:00
    ================================
    ℹ️  12:00:00 root INFO: second run message
    ⚠️  12:00:01 root WARNING: beware
    ❌ 12:00:02 root ERROR: oops again
    ℹ️  12:00:03 root INFO: done
    """
).strip()


def test_parse_log_counts(tmp_path):
    """It should count levels for the *last* run only."""

    log_path = tmp_path / "tlptaco_test.log"
    log_path.write_text(SAMPLE_LOG, encoding="utf-8")

    counts, lines = log_util.parse_log(log_path)

    # Expect only messages from the 2nd run (INFO WARNING ERROR)
    assert counts["INFO"] == 2
    assert counts["WARNING"] == 1
    assert counts["ERROR"] == 1
    # No debug/critical after slicing
    assert counts.get("CRITICAL", 0) == 0

    # Lines list should have 4 entries (same as total above)
    assert len(lines) == 4


def test_level_filtering(tmp_path):
    log_path = tmp_path / "t.log"
    log_path.write_text(SAMPLE_LOG, encoding="utf-8")

    # Only ERROR lines requested
    counts, lines = log_util.parse_log(log_path, levels=["error"])

    assert counts["ERROR"] == 1
    # INFO default absent because not requested
    assert "INFO" not in counts
    assert len(lines) == 1
