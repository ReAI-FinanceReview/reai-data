"""Guards for the per-directory AGENTS.md guidance tree.

These files are instructions agents act on, so a wrong one costs more than a
wrong comment. They are also derived from code and therefore rot silently. The
repository already asserts on config files this way in tests/test_ci_workflows.py
and tests/test_local_dev_setup.py; these guards extend that to the guidance tree.
"""
import re
import subprocess
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]

# Data-only directories that deliberately carry no AGENTS.md of their own. Each
# entry must still be described by a Subdirectories row in its parent's file.
DIRS_WITHOUT_GUIDANCE = {"tests/fixtures"}

CITATION = re.compile(r"`([A-Za-z0-9_./-]+\.(?:py|ya?ml|sql|json|toml|txt|md)):(\d+)(?:[-,]\d+)*`")
PARENT_TAG = re.compile(r"<!-- Parent: (\S+) -->")


def _git(*args: str) -> list[str]:
    out = subprocess.run(
        ["git", "-C", str(ROOT), *args], capture_output=True, text=True, check=True
    ).stdout
    return [line for line in out.splitlines() if line]


def tracked_files() -> list[str]:
    return _git("ls-files")


def agents_files() -> list[Path]:
    paths = [ROOT / p for p in tracked_files() if Path(p).name == "AGENTS.md"]
    assert paths, "no tracked AGENTS.md found; check the *.md ignore rule in .gitignore"
    return sorted(paths)


AGENTS_FILES = agents_files()


def _rel(path: Path) -> str:
    return path.relative_to(ROOT).as_posix()


@pytest.mark.parametrize("path", AGENTS_FILES, ids=_rel)
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


@pytest.mark.parametrize("path", AGENTS_FILES, ids=_rel)
def test_agents_file_carries_its_header_and_manual_block(path: Path):
    content = path.read_text()

    assert "<!-- Generated:" in content, "missing the Generated/Updated header"
    assert "<!-- MANUAL:" in content, "missing the MANUAL block that survives regeneration"


@pytest.mark.parametrize("path", AGENTS_FILES, ids=_rel)
def test_agents_parent_link_resolves(path: Path):
    """Every file except the root one points at a parent that exists."""
    content = path.read_text()
    match = PARENT_TAG.search(content)

    if path == ROOT / "AGENTS.md":
        assert match is None, "the root file must not declare a parent"
        return

    assert match, "missing the <!-- Parent: ... --> tag"
    assert (path.parent / match.group(1)).resolve().exists(), (
        f"parent {match.group(1)!r} does not resolve to a file"
    )


@pytest.mark.parametrize("path", AGENTS_FILES, ids=_rel)
def test_agents_citations_point_at_lines_that_exist(path: Path):
    """`file.py:123` citations rot every time the cited file changes.

    They are the most load-bearing part of this documentation and the part that
    goes stale first, so an out-of-range line number fails here rather than
    sending a reader to the wrong place.
    """
    stale = []
    for cited, lineno in CITATION.findall(path.read_text()):
        # Citations are written either from the repo root or relative to the
        # directory the file documents; both spellings appear in this tree.
        target = next(
            (c for c in (ROOT / cited, path.parent / cited) if c.is_file()), None
        )
        if target is None:
            continue  # covered by test_agents_key_files_exist
        line_count = len(target.read_text(errors="replace").splitlines())
        if int(lineno) > line_count:
            stale.append(f"{cited}:{lineno} (file has {line_count} lines)")

    assert not stale, "citations point past the end of their file: " + ", ".join(stale)


@pytest.mark.parametrize("path", AGENTS_FILES, ids=_rel)
def test_agents_key_files_exist(path: Path):
    """Every file named in a Key Files table must exist in that directory."""
    missing = []
    for line in path.read_text().splitlines():
        if not line.startswith("| `"):
            continue
        name = line.split("`")[1]
        if "/" in name or not Path(name).suffix:
            continue  # a subdirectory row, or a symbol rather than a file
        if not (path.parent / name).exists():
            missing.append(name)

    assert not missing, f"named in a table but absent from {_rel(path.parent)}: {missing}"


def test_every_directory_with_tracked_files_has_guidance():
    """A new directory must bring its own AGENTS.md, or be listed as an exception."""
    documented = {_rel(p.parent) for p in AGENTS_FILES}
    with_code = {
        Path(f).parent.as_posix() for f in tracked_files() if Path(f).parent != Path(".")
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
