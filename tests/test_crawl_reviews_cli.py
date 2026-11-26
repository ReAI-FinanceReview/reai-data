"""Tests for scripts/crawl_reviews.py (Issue #127)

The DAG runs this script once per store (``--store appstore`` / ``--store playstore``) and reads
only its exit code: 0 when the step succeeded, 1 when it failed; 2 stays argparse's usage error.
``main()`` runs against the real test database and the local RSS server from conftest; the Play
app-id file stays empty, so PlayStoreCrawler raises before any request reaches the live store.

``scripts`` is not a package, so the module is loaded by file path, as in
``tests/test_backfill_app_version_cli.py``.
"""

import importlib.util
import json
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts" / "crawl_reviews.py"
CONFIG = yaml.safe_load((ROOT / "config" / "crawler_config.yml").read_text(encoding="utf-8"))
PAGE_SIZE = CONFIG["appstore"]["rss_page_size"]
CUTOFF = CONFIG["appstore"]["max_consecutive_empty_feeds"]


def _load_cli():
    spec = importlib.util.spec_from_file_location("_crawl_reviews_cli", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def cli():
    return _load_cli()


def _printed_result(capsys) -> dict:
    return json.loads(capsys.readouterr().out.strip().splitlines()[-1])


@pytest.mark.requires_db
def test_exit_0_when_the_app_store_crawl_succeeds(cli, crawl_config, appstore_rss_server, rss_review_entry, capsys):
    appstore_rss_server.set_pages("1001", [rss_review_entry(f"r1001-{i}", "7.0.0") for i in range(PAGE_SIZE)])
    crawl_config.set_app_ids("appstore", "1001")

    assert cli.main(["--store", "appstore", "--config", crawl_config.path]) == 0
    result = _printed_result(capsys)
    assert (result["status"], list(result["validations"])) == ("success", ["appstore"])


@pytest.mark.requires_db
def test_exit_1_when_consecutive_empty_feeds_reach_the_cutoff(cli, crawl_config, appstore_rss_server, capsys):
    empty = [f"200{i}" for i in range(CUTOFF)]
    crawl_config.set_app_ids("appstore", *empty)

    assert cli.main(["--store", "appstore", "--config", crawl_config.path]) == 1
    result = _printed_result(capsys)
    assert result["status"] == "failed" and result["message"].startswith("crawl failed: appstore: App Store RSS:")
    for store_id in empty:
        assert appstore_rss_server.requested_pages(store_id) == list(range(1, CONFIG["appstore"]["pages_to_crawl"] + 1))


@pytest.mark.requires_db
def test_store_defaults_to_all_and_fails_when_either_store_fails(
    cli, crawl_config, appstore_rss_server, rss_review_entry, capsys
):
    appstore_rss_server.set_pages("1003", [rss_review_entry(f"r1003-{i}", "7.0.0") for i in range(PAGE_SIZE)])
    crawl_config.set_app_ids("appstore", "1003")

    assert cli.main(["--config", crawl_config.path]) == 1
    result = _printed_result(capsys)
    assert list(result["validations"]) == ["appstore", "playstore"]
    assert result["validations"]["appstore"]["status"] == "success"
    assert result["message"].startswith("crawl failed: playstore: ")


def test_rejects_an_unknown_store_with_argparse_usage_error(cli):
    with pytest.raises(SystemExit) as exc_info:
        cli.build_arg_parser().parse_args(["--store", "unified"])

    assert exc_info.value.code == 2
