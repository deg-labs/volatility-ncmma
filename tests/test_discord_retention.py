import importlib.util
import logging
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import Mock

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "ncmma"))
spec = importlib.util.spec_from_file_location("monitor_app", ROOT / "ncmma/ncmma.py")
app = importlib.util.module_from_spec(spec)
spec.loader.exec_module(app)


def response(status, body=None):
    result = Mock(status_code=status)
    result.json.return_value = body
    return result


@pytest.fixture
def monitor(tmp_path, monkeypatch):
    cls = getattr(app, "CmmaFundingRateMonitor", None) or app.CmmaPriceMonitor
    obj = cls.__new__(cls)
    obj.db_path = tmp_path / "messages.db"
    obj.logger = logging.getLogger("test-monitor")
    obj.discord_webhook_url = "https://discord.invalid/api/webhooks/333/new"
    obj.discord_keep_messages = 1
    obj.discord_message_max_age_hours = 0
    obj.discord_cleanup_interval_hours = 24
    obj.discord_delete_interval_seconds = 0
    obj._init_db()
    now = datetime.now(timezone.utc)
    with sqlite3.connect(obj.db_path) as db:
        db.executemany(
            "INSERT INTO discord_messages VALUES (?,?,?)",
            [
                (
                    "100",
                    (now - timedelta(hours=25)).isoformat(),
                    "https://discord.invalid/api/webhooks/222/old",
                ),
                ("101", now.isoformat(), obj.discord_webhook_url),
            ],
        )
    monkeypatch.setattr(app.time, "sleep", lambda *a: None)
    monkeypatch.setattr(
        app.requests, "get", Mock(side_effect=AssertionError("unexpected HTTP GET"))
    )
    return obj


def ids(monitor):
    with sqlite3.connect(monitor.db_path) as db:
        return [
            row[0]
            for row in db.execute(
                "SELECT message_id FROM discord_messages ORDER BY message_id"
            )
        ]


def test_uses_original_webhook_after_rotation(monitor, monkeypatch):
    delete = Mock(return_value=response(204))
    monkeypatch.setattr(app.requests, "delete", delete)
    monitor._cleanup_old_discord_messages()
    assert ids(monitor) == ["101"]
    assert "/webhooks/222/old/messages/100" in delete.call_args.args[0]


@pytest.mark.parametrize(
    "status,body",
    [(404, {"code": 10015}), (403, {"code": 50013}), (503, None), (404, None)],
)
def test_failures_preserve_ids_and_retry_next_cycle(monitor, monkeypatch, status, body):
    delete = Mock(return_value=response(status, body))
    monkeypatch.setattr(app.requests, "delete", delete)
    monitor._cleanup_old_discord_messages()
    first = delete.call_count
    assert ids(monitor) == ["100", "101"]
    assert monitor._get_meta("last_discord_cleanup") is None
    monitor._cleanup_old_discord_messages()
    assert delete.call_count > first


def test_unknown_message_in_original_route_is_already_gone(monitor, monkeypatch):
    monkeypatch.setattr(
        app.requests, "delete", Mock(return_value=response(404, {"code": 10008}))
    )
    monitor._cleanup_old_discord_messages()
    assert ids(monitor) == ["101"]


def test_legacy_row_is_not_deleted_without_positive_route_check(monitor, monkeypatch):
    with sqlite3.connect(monitor.db_path) as db:
        db.execute(
            'UPDATE discord_messages SET webhook_url=NULL WHERE message_id="100"'
        )
    monkeypatch.setattr(
        app.requests, "get", Mock(return_value=response(404, {"code": 10008}))
    )
    delete = Mock()
    monkeypatch.setattr(app.requests, "delete", delete)
    monitor._cleanup_old_discord_messages()
    assert ids(monitor) == ["100", "101"]
    delete.assert_not_called()


def test_legacy_row_is_migrated_after_matching_get(monitor, monkeypatch):
    with sqlite3.connect(monitor.db_path) as db:
        db.execute(
            'UPDATE discord_messages SET webhook_url=NULL WHERE message_id="100"'
        )
    monkeypatch.setattr(
        app.requests,
        "get",
        Mock(return_value=response(200, {"id": "100", "webhook_id": "333"})),
    )
    delete = Mock(return_value=response(204))
    monkeypatch.setattr(app.requests, "delete", delete)
    monitor._cleanup_old_discord_messages()
    assert ids(monitor) == ["101"]
    assert "/webhooks/333/new/messages/100" in delete.call_args.args[0]


def test_expiry_does_not_wait_for_daily_count_cleanup(monitor, monkeypatch):
    monitor.discord_keep_messages = 50
    monitor.discord_message_max_age_hours = 24
    monitor._set_meta(
        "last_discord_cleanup",
        (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat(),
    )
    monkeypatch.setattr(app.requests, "delete", Mock(return_value=response(204)))
    monitor._cleanup_old_discord_messages()
    assert ids(monitor) == ["101"]
