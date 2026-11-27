"""Checks on the Databricks job definition in databricks.yml.

The job cannot be run in CI, but the mistakes that break it most often can be
caught by reading the file: a notebook path that does not exist, tasks in the
wrong order, or a job parameter whose name does not match any notebook widget
(the value would then be silently ignored).
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

yaml = pytest.importorskip("yaml")

REPO_ROOT = Path(__file__).resolve().parents[1]
BUNDLE_FILE = REPO_ROOT / "databricks.yml"
NOTEBOOK_ORDER = ("01_bronze", "02_silver", "03_gold")


@pytest.fixture(scope="module")
def job() -> dict:
    bundle = yaml.safe_load(BUNDLE_FILE.read_text())
    return bundle["resources"]["jobs"]["lakehouse_daily"]


def widget_names(notebook: Path) -> set[str]:
    """Names of the widgets a notebook defines, e.g. dbutils.widgets.text("catalog", ...)."""
    return set(re.findall(r'dbutils\.widgets\.\w+\(\s*"(\w+)"', notebook.read_text()))


def task_order(tasks: list[dict]) -> list[str]:
    """Return task keys in dependency order; fail on a cycle or unknown dependency."""
    deps = {t["task_key"]: {d["task_key"] for d in t.get("depends_on", [])} for t in tasks}
    order: list[str] = []
    while deps:
        ready = sorted(k for k, needs in deps.items() if needs <= set(order))
        if not ready:
            raise AssertionError(f"Cycle or unknown dependency among tasks: {sorted(deps)}")
        order.extend(ready)
        for key in ready:
            del deps[key]
    return order


def test_task_order_helper_detects_cycles():
    tasks = [{"task_key": "a", "depends_on": [{"task_key": "b"}]},
             {"task_key": "b", "depends_on": [{"task_key": "a"}]}]
    with pytest.raises(AssertionError):
        task_order(tasks)


def test_tasks_run_the_notebooks_in_medallion_order(job):
    by_key = {t["task_key"]: t for t in job["tasks"]}
    notebooks = [Path(by_key[key]["notebook_task"]["notebook_path"]).stem
                 for key in task_order(job["tasks"])]
    assert tuple(notebooks) == NOTEBOOK_ORDER


def test_every_notebook_path_exists(job):
    for task in job["tasks"]:
        path = REPO_ROOT / task["notebook_task"]["notebook_path"]
        assert path.is_file(), f"{task['task_key']}: {path} not found"
        assert path.read_text().startswith("# Databricks notebook source")


def test_tasks_use_serverless_compute(job):
    # Free Edition only has serverless; any cluster setting would fail to deploy.
    for task in job["tasks"]:
        assert not {"job_cluster_key", "new_cluster", "existing_cluster_id"} & task.keys()


def test_job_parameters_match_notebook_widgets(job):
    widgets = widget_names(REPO_ROOT / "databricks" / "_common.py")
    params = {p["name"] for p in job["parameters"]}
    assert params <= widgets, f"parameters without a widget: {sorted(params - widgets)}"


def test_only_one_run_at_a_time_and_a_schedule(job):
    assert job["max_concurrent_runs"] == 1
    assert job["schedule"]["quartz_cron_expression"]
