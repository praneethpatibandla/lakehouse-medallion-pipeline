"""Checks that keep the documentation in step with the code.

Docs go stale quietly: a file is renamed and a link breaks, or a table is
added and never documented. These tests are plain text checks, so they run
in milliseconds and need no Spark.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from lakehouse.maintenance import TABLES

REPO_ROOT = Path(__file__).resolve().parents[1]
DOC_FILES = [REPO_ROOT / "README.md", REPO_ROOT / "ROADMAP.md",
             *sorted((REPO_ROOT / "docs").glob("*.md"))]
ARCHITECTURE = REPO_ROOT / "docs" / "architecture.md"

# [text](target) in Markdown. Images use the same syntax after a "!".
_LINK = re.compile(r"\[[^\]]*\]\(([^)\s]+)\)")
# Audit tables that are written by the runner and the quality suites, but
# not part of the maintenance list.
EXTRA_TABLES = ("pipeline_runs", "dq_results")


def relative_links(markdown: str) -> list[str]:
    """Return link targets that point inside the repository.

    Web links, mail links and in-page anchors (#section) are skipped, and an
    anchor after a file name is dropped: `docs/build-log.md#22` -> the file.
    """
    targets = []
    for target in _LINK.findall(markdown):
        if re.match(r"^[a-z]+:", target) or target.startswith("#"):
            continue
        targets.append(target.split("#", 1)[0])
    return targets


def test_relative_links_skips_web_links_and_anchors():
    text = ("[a](docs/x.md) [b](https://example.org) [c](#top) "
            "[d](build-log.md#22-databricks) ![e](img/arch.png)")
    assert relative_links(text) == ["docs/x.md", "build-log.md", "img/arch.png"]


@pytest.mark.parametrize("doc", DOC_FILES, ids=lambda p: p.name)
def test_relative_links_point_at_existing_files(doc: Path):
    missing = [t for t in relative_links(doc.read_text(encoding="utf-8"))
               if not (doc.parent / t).exists()]
    assert not missing, f"{doc.name} links to missing files: {missing}"


def test_architecture_documents_every_table():
    text = ARCHITECTURE.read_text(encoding="utf-8")
    tables = {t.table for t in TABLES} | set(EXTRA_TABLES)
    undocumented = sorted(t for t in tables if t not in text)
    assert not undocumented, f"Add these tables to docs/architecture.md: {undocumented}"


def test_roadmap_items_have_build_log_entries():
    """Every ticked roadmap step has a build log heading with its number."""
    roadmap = (REPO_ROOT / "ROADMAP.md").read_text(encoding="utf-8")
    log = (REPO_ROOT / "docs" / "build-log.md").read_text(encoding="utf-8")
    done = re.findall(r"^- \[x\] (\d+)\.", roadmap, flags=re.MULTILINE)
    logged = set(re.findall(r"^## (\d+)\.", log, flags=re.MULTILINE))
    assert done, "no ticked items found; has the roadmap format changed?"
    assert sorted(set(done) - logged) == []
