"""Guards for the per-directory AGENTS.md guidance tree.

These files are instructions agents act on, so a wrong one costs more than a
wrong comment. They are also derived from code and therefore rot silently. The
repository already asserts on config files this way in tests/test_ci_workflows.py
and tests/test_local_dev_setup.py; these guards extend that to the guidance tree.

The tracked-file list comes from git, so it is read at collection time inside
pytest_generate_tests rather than at import time. Outside a git work tree (a
source export, an image without .git or without git) only this module skips.
"""
import functools
import re
import subprocess
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]

# Data-only directories that deliberately carry no AGENTS.md of their own. Each
# entry must still be described by a Subdirectories row in its parent's file.
DIRS_WITHOUT_GUIDANCE = {"tests/fixtures"}

CITATION = re.compile(r"`([A-Za-z0-9_./-]+\.(?:py|ya?ml|sql|json|toml|txt|md)):(\d+(?:[-,]\d+)*)`")
PARENT_TAG = re.compile(r"<!-- Parent: (\S+) -->")
MANUAL_MARKER = "<!-- MANUAL:"
BACKTICKED = re.compile(r"`([^`]+)`")


def _git(*args: str) -> list[str]:
    out = subprocess.run(
        ["git", "-C", str(ROOT), *args], capture_output=True, text=True, check=True
    ).stdout
    return [line for line in out.splitlines() if line]


@functools.cache
def _tracked_files() -> tuple[tuple[str, ...], str | None]:
    """Tracked paths, or the reason they cannot be listed. Never raises."""
    try:
        return tuple(_git("ls-files")), None
    except (OSError, subprocess.CalledProcessError) as exc:
        return (), f"the AGENTS.md guards need git and a git work tree: {exc}"


def _agents_files() -> list[Path]:
    tracked, _ = _tracked_files()
    return sorted(ROOT / p for p in tracked if Path(p).name == "AGENTS.md")


def _rel(path: Path) -> str:
    return path.relative_to(ROOT).as_posix()


def pytest_generate_tests(metafunc):
    if "path" not in metafunc.fixturenames:
        return
    _, reason = _tracked_files()
    if reason:
        metafunc.parametrize("path", [pytest.param(None, marks=pytest.mark.skip(reason=reason))])
        return
    metafunc.parametrize("path", _agents_files(), ids=_rel)


@pytest.fixture(scope="module")
def tracked_files() -> tuple[str, ...]:
    tracked, reason = _tracked_files()
    if reason:
        pytest.skip(reason)
    return tracked


def test_agents_files_are_tracked(tracked_files):
    """An empty file list would silently skip every per-file guard below."""
    assert any(Path(p).name == "AGENTS.md" for p in tracked_files), (
        "no tracked AGENTS.md found; check the *.md ignore rule in .gitignore"
    )


def test_agents_file_is_not_ignored(path: Path):
    """The *.md blanket rule must keep losing to the !AGENTS.md negation.

    Loosening that block would not break anything visibly — already-tracked files
    stay tracked — so the damage would only surface when someone re-adds one.
    """
    result = subprocess.run(
        ["git", "-C", str(ROOT), "check-ignore", "-q", "--no-index", _rel(path)],
        capture_output=True,
    )
    assert result.returncode == 1, f"{_rel(path)} is ignored by .gitignore"


def test_agents_file_carries_its_header_and_manual_block(path: Path):
    content = path.read_text()

    assert "<!-- Generated:" in content, "missing the Generated/Updated header"
    assert content.count(MANUAL_MARKER) == 1, "the MANUAL marker must appear exactly once"
    assert content.rstrip().splitlines()[-1].startswith(MANUAL_MARKER), (
        "the MANUAL marker must be the last non-empty line; put new facts in the section "
        "they belong to (see 'Keeping These Files True' in the root AGENTS.md)"
    )


def test_agents_parent_link_resolves(path: Path):
    """Every file except the root one points at the nearest AGENTS.md above it."""
    content = path.read_text()
    match = PARENT_TAG.search(content)

    if path == ROOT / "AGENTS.md":
        assert match is None, "the root file must not declare a parent"
        return

    assert match, "missing the <!-- Parent: ... --> tag"
    documented = set(_agents_files())
    expected = next(
        (d / "AGENTS.md" for d in path.parent.parents if d / "AGENTS.md" in documented), None
    )
    assert expected is not None, "no AGENTS.md above this file to be its parent"
    resolved = (path.parent / match.group(1)).resolve()
    assert resolved == expected, (
        f"parent {match.group(1)!r} resolves to {resolved}, expected {_rel(expected)}"
    )


def test_agents_citations_point_at_lines_that_exist(path: Path):
    """`file.py:123` citations rot every time the cited file changes.

    They are the most load-bearing part of this documentation and the part that
    goes stale first, so a citation fails here when its file does not resolve or
    when any line number in it (`:12`, `:12-20`, `:12,40`) is outside 1..EOF,
    rather than sending a reader to the wrong place.
    """
    stale = []
    for cited, numbers in CITATION.findall(path.read_text()):
        # Citations are written either from the repo root or relative to the
        # directory the file documents; both spellings appear in this tree.
        target = next(
            (c for c in (ROOT / cited, path.parent / cited) if c.is_file()), None
        )
        if target is None:
            stale.append(
                f"{cited}:{numbers} (no such file from the repo root or {_rel(path.parent)}/)"
            )
            continue
        line_count = len(target.read_text(errors="replace").splitlines())
        if any(not 1 <= int(n) <= line_count for n in re.findall(r"\d+", numbers)):
            stale.append(f"{cited}:{numbers} (file has {line_count} lines)")

    assert not stale, "citations that do not point at existing lines: " + ", ".join(stale)


def test_agents_key_files_exist(path: Path):
    """Every name in the first column of a table must exist in that directory.

    That covers each file of a multi-file row (`a.py` / `b.py`) and the `subdir/`
    rows of a Subdirectories table.
    """
    missing = []
    for line in path.read_text().splitlines():
        if not line.startswith("| `"):
            continue
        for name in BACKTICKED.findall(line.split("|")[1]):
            if not (path.parent / name).exists():
                missing.append(name)

    assert not missing, f"named in a table but absent from {_rel(path.parent)}: {missing}"


def test_every_directory_with_tracked_files_has_guidance(tracked_files):
    """A new directory must bring its own AGENTS.md, or be listed as an exception."""
    documented = {_rel(p.parent) for p in _agents_files()}
    with_code = {
        Path(f).parent.as_posix() for f in tracked_files if Path(f).parent != Path(".")
    }
    with_code.add(".")

    undocumented = sorted(with_code - documented - DIRS_WITHOUT_GUIDANCE)
    assert not undocumented, f"directories missing AGENTS.md: {undocumented}"


def test_guidance_exceptions_are_still_described_by_their_parent():
    """An exempt directory must at least appear in its parent's Subdirectories table."""
    for relative in sorted(DIRS_WITHOUT_GUIDANCE):
        directory = Path(relative)
        parent_doc = ROOT / directory.parent / "AGENTS.md"

        assert parent_doc.exists(), f"{relative} has no parent AGENTS.md to describe it"
        assert f"`{directory.name}/`" in parent_doc.read_text(), (
            f"{relative} is exempt from having its own AGENTS.md but "
            f"{_rel(parent_doc)} does not describe it"
        )
