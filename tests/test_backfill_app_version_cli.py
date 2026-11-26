"""Tests for scripts/backfill_app_version.py (Issue #125)

The script decides two things on its own: which stores ``--platform`` selects, and the exit code
(0 everything confirmed, 1 a store request failed, 2 some targets unverified). ``main()`` runs
against the real test database and the local RSS server from conftest; no Play rows are seeded,
so no request reaches the live Play store.

``scripts`` is not a package, so the module is loaded by file path, as in
``tests/test_assign_dept_cli.py``.
"""

import importlib.util
from pathlib import Path

import pytest
from sqlalchemy import text

from src.crawlers.app_version_backfill import load_settings
from src.models.enums import PlatformType

ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts" / "backfill_app_version.py"
APPSTORE = PlatformType.APPSTORE
PAGE_SIZE = load_settings(str(ROOT / "config" / "crawler_config.yml")).appstore_rss_page_size


def _load_cli():
    spec = importlib.util.spec_from_file_location("_backfill_app_version_cli", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def cli():
    return _load_cli()


def _serve_versions(server, rss_review_entry, store_id, *targets):
    """Page 1 of ``store_id`` holds the (review_id, version) targets, padded to a full page."""
    entries = [rss_review_entry(rid, version) for rid, version in targets]
    entries += [rss_review_entry(f"{store_id}-other-{i}", "7.0.0") for i in range(PAGE_SIZE - len(entries))]
    server.set_pages(store_id, entries)


@pytest.mark.requires_db
def test_exit_0_when_every_target_is_confirmed(
    cli, backfill_db, backfill_config, appstore_rss_server, rss_review_entry, capsys
):
    app_id = backfill_db.seed_app(APPSTORE, "3000001", [("a", None), ("b", None)])
    _serve_versions(appstore_rss_server, rss_review_entry, "3000001", ("a", "7.3.16"), ("b", "7.3.15"))

    assert cli.main(["--config", backfill_config]) == cli.EXIT_CONFIRMED == 0
    assert "mode: dry-run (no writes)" in capsys.readouterr().out
    assert backfill_db.versions(app_id) == {"a": None, "b": None}


@pytest.mark.requires_db
def test_apply_flag_writes(cli, backfill_db, backfill_config, appstore_rss_server, rss_review_entry):
    app_id = backfill_db.seed_app(APPSTORE, "3000001", [("a", None)])
    _serve_versions(appstore_rss_server, rss_review_entry, "3000001", ("a", "7.3.16"))

    assert cli.main(["--config", backfill_config, "--apply"]) == 0
    assert backfill_db.versions(app_id) == {"a": "7.3.16"}


@pytest.mark.requires_db
def test_exit_2_when_targets_stay_unverified(cli, backfill_db, backfill_config, appstore_rss_server):
    backfill_db.seed_app(APPSTORE, "3000001", [("a", None)])  # the RSS serves only empty pages

    assert cli.main(["--config", backfill_config]) == cli.EXIT_UNVERIFIED == 2


@pytest.mark.requires_db
def test_exit_1_when_a_request_failed_even_with_unverified_left(
    cli, backfill_db, backfill_config, appstore_rss_server
):
    backfill_db.seed_app(APPSTORE, "3000001", [("a", None)])
    backfill_db.seed_app(APPSTORE, "3000002", [("b", None)])  # empty feed: unverified
    appstore_rss_server.fail("3000001", 1, status=500)

    assert cli.main(["--config", backfill_config]) == cli.EXIT_ERROR == 1


@pytest.mark.requires_db
def test_exit_1_when_one_app_fails_in_the_database(
    cli, backfill_db, backfill_config, appstore_rss_server, rss_review_entry
):
    """Another transaction holds the first app's rows; the second app is still written."""
    locked = backfill_db.seed_app(APPSTORE, "3000001", [("a", None)])
    written = backfill_db.seed_app(APPSTORE, "3000002", [("b", None)])
    _serve_versions(appstore_rss_server, rss_review_entry, "3000001", ("a", "7.3.16"))
    _serve_versions(appstore_rss_server, rss_review_entry, "3000002", ("b", "7.3.15"))

    with backfill_db.engine.begin() as other_writer:
        other_writer.execute(
            text("SELECT 1 FROM app_reviews WHERE app_id = :app_id FOR UPDATE"), {"app_id": locked}
        )
        exit_code = cli.main(["--config", backfill_config, "--apply"])

    assert exit_code == cli.EXIT_ERROR == 1
    assert backfill_db.versions(locked) == {"a": None}
    assert backfill_db.versions(written) == {"b": "7.3.15"}


@pytest.mark.requires_db
@pytest.mark.parametrize("argv, appstore_requested", [
    ([], True),
    (["--platform", "all"], True),
    (["--platform", "appstore"], True),
    (["--platform", "playstore"], False),
])
def test_platform_selects_the_stores(
    argv, appstore_requested, cli, backfill_db, backfill_config, appstore_rss_server, rss_review_entry
):
    backfill_db.seed_app(APPSTORE, "3000001", [("a", None)])
    _serve_versions(appstore_rss_server, rss_review_entry, "3000001", ("a", "7.3.16"))

    assert cli.main(["--config", backfill_config, *argv]) == 0
    assert bool(appstore_rss_server.requested_pages("3000001")) is appstore_requested


def test_rejects_an_unknown_platform(cli):
    with pytest.raises(SystemExit):
        cli.build_arg_parser().parse_args(["--platform", "web"])


def test_help_documents_the_exit_codes(cli):
    help_text = cli.build_arg_parser().format_help()

    for code in (cli.EXIT_CONFIRMED, cli.EXIT_ERROR, cli.EXIT_UNVERIFIED):
        assert f"\n  {code}  " in help_text
