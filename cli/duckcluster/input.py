"""Readline-backed input with persistent query history."""

from __future__ import annotations

import atexit
import os
from pathlib import Path

try:
    import readline
except ImportError:  # pragma: no cover - Windows fallback
    readline = None  # type: ignore[assignment]

_HISTORY_LENGTH = 200
_HISTORY_PATH = Path(
    os.environ.get("DUCKCLUSTER_HISTORY", Path.home() / ".duckcluster_history")
)


def setup_history() -> None:
    if readline is None:
        return
    try:
        if _HISTORY_PATH.exists():
            readline.read_history_file(_HISTORY_PATH)
            _dedupe_loaded_history()
    except OSError:
        pass
    readline.set_history_length(_HISTORY_LENGTH)
    atexit.register(_save_history)


def read_command(prompt: str) -> str:
    line = input(prompt)
    _undo_readline_auto_add(line)
    return line


def remember_command(entry: str) -> None:
    """Store exactly what the user typed (including trailing ';' when present)."""
    if readline is None:
        return
    text = entry.strip()
    if not text:
        return
    if _last_history_item() != text:
        readline.add_history(text)


def _last_history_item() -> str | None:
    length = readline.get_current_history_length()
    if length == 0:
        return None
    return readline.get_history_item(length)


def _undo_readline_auto_add(line: str) -> None:
    """Remove the line readline auto-added during input(); we store SQL ourselves."""
    if readline is None:
        return
    length = readline.get_current_history_length()
    if length == 0:
        return
    last = readline.get_history_item(length)
    if last is None:
        return
    submitted = line.rstrip("\n")
    if last != submitted and last.rstrip(";").strip() != submitted.rstrip(";").strip():
        return
    try:
        readline.remove_history_item(length)
    except ValueError:
        pass


def _dedupe_loaded_history() -> None:
    """Collapse semicolon duplicates; keep the ';'-terminated form when both exist."""
    if readline is None:
        return
    best: dict[str, str] = {}
    order: list[str] = []
    for i in range(1, readline.get_current_history_length() + 1):
        item = readline.get_history_item(i)
        if item is None:
            continue
        text = item.strip()
        if not text:
            continue
        norm = text.rstrip(";").strip()
        if norm not in best:
            order.append(norm)
            best[norm] = text
        elif text.endswith(";") and not best[norm].endswith(";"):
            best[norm] = text
    readline.clear_history()
    for norm in order:
        readline.add_history(best[norm])


def _save_history() -> None:
    if readline is None:
        return
    try:
        _HISTORY_PATH.parent.mkdir(parents=True, exist_ok=True)
        readline.write_history_file(_HISTORY_PATH)
    except OSError:
        pass
