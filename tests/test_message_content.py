"""Tests for the message_content stream."""

from __future__ import annotations

import datetime
import io
import json
from contextlib import redirect_stdout
from unittest import mock

import pytest
import requests

from tap_mandrill import streams
from tap_mandrill.tap import TapMandrill

NOW = datetime.datetime.now(datetime.timezone.utc)
ALLOWLIST = ["^Your Pulumi Receipt$", "^Your Pulumi stack update for .+ failed$"]


def hours_ago(hours: float) -> str:
    return (NOW - datetime.timedelta(hours=hours)).isoformat()


def activity_row(message_id: str, subject: str, ts: str) -> dict:
    return {
        "message_id": message_id,
        "ts": ts,
        "email": "user@example.com",
        "sender": "billing@pulumi.com",
        "subject": subject,
        "status": "sent",
        "opens": 0,
        "clicks": 0,
    }


class FakeResponse:
    def __init__(self, status_code: int, body: object) -> None:
        self.status_code = status_code
        self.ok = status_code < 400  # noqa: PLR2004
        self._body = body

    def json(self) -> object:
        if isinstance(self._body, Exception):
            raise self._body
        return self._body


def content_response(message_id: str) -> FakeResponse:
    return FakeResponse(
        200,
        {"html": f"<p>{message_id}</p>", "subject": "From Mandrill", "from_email": "billing@pulumi.com"},
    )


def run_tap(
    rows: list[dict],
    post: mock.Mock,
    *,
    config: dict | None = None,
    state: dict | None = None,
) -> tuple[list[dict], dict]:
    """Sync the tap over fixed activity rows; return content records and final state."""
    tap_config = {"auth_token": "test-key", "content_subject_allowlist": ALLOWLIST, **(config or {})}
    tap = TapMandrill(config=tap_config, state=state)
    out = io.StringIO()
    with (
        mock.patch.object(streams.ActivityExportStream, "get_records", lambda self, context: iter(rows)),  # noqa: ARG005
        mock.patch.object(streams.requests, "post", post),
        mock.patch.object(streams.time, "sleep"),
        redirect_stdout(out),
    ):
        tap.sync_all()

    messages = [json.loads(line) for line in out.getvalue().splitlines() if line.strip()]
    records = [m["record"] for m in messages if m["type"] == "RECORD" and m["stream"] == "message_content"]
    states = [m["value"] for m in messages if m["type"] == "STATE"]
    return records, (states[-1] if states else {})


def content_bookmark(state: dict) -> str | None:
    return state.get("bookmarks", {}).get("message_content", {}).get("content_fetched_through")


def posted_ids(post: mock.Mock) -> list[str]:
    return [call.kwargs["json"]["id"] for call in post.call_args_list]


def test_fetches_content_only_for_allowlisted_subjects() -> None:
    post = mock.Mock(side_effect=lambda *_args, **kwargs: content_response(kwargs["json"]["id"]))
    rows = [
        activity_row("receipt-1", "Your Pulumi Receipt", hours_ago(5)),
        activity_row("reset-1", "Reset your Pulumi password", hours_ago(4)),
        activity_row("stack-1", "your pulumi stack update for acme/prod failed", hours_ago(3)),
        activity_row("generated-123", "Your Pulumi Receipt", hours_ago(2)),
    ]

    records, state = run_tap(rows, post)

    assert posted_ids(post) == ["receipt-1", "stack-1"]
    assert [r["message_id"] for r in records] == ["receipt-1", "stack-1"]
    assert records[0]["html"] == "<p>receipt-1</p>"
    assert records[0]["ts"] == rows[0]["ts"]
    assert records[0]["from_email"] == "billing@pulumi.com"
    assert content_bookmark(state) == rows[2]["ts"]
    # The API key travels in the request body and must not be logged or emitted.
    assert post.call_args_list[0].kwargs["json"]["key"] == "test-key"
    assert "test-key" not in json.dumps(records)


def test_fetches_nothing_without_an_allowlist() -> None:
    post = mock.Mock()
    rows = [activity_row("receipt-1", "Your Pulumi Receipt", hours_ago(5))]

    records, state = run_tap(rows, post, config={"content_subject_allowlist": []})

    post.assert_not_called()
    assert records == []
    assert content_bookmark(state) is None


def test_fetches_each_message_once_across_runs() -> None:
    post = mock.Mock(side_effect=lambda *_args, **kwargs: content_response(kwargs["json"]["id"]))
    first_rows = [
        activity_row("a", "Your Pulumi Receipt", hours_ago(30)),
        activity_row("b", "Your Pulumi Receipt", hours_ago(20)),
    ]
    _, state = run_tap(first_rows, post)
    assert posted_ids(post) == ["a", "b"]

    # The activity stream re-reads the last 7 days, so the next run sees a and b again.
    post.reset_mock()
    second_rows = [*first_rows, activity_row("c", "Your Pulumi Receipt", hours_ago(1))]
    records, state = run_tap(second_rows, post, state=state)

    # b sits exactly on the bookmark, so it is fetched again; a is not.
    assert posted_ids(post) == ["b", "c"]
    assert [r["message_id"] for r in records] == ["b", "c"]
    assert content_bookmark(state) == second_rows[2]["ts"]


def test_order_within_a_run_does_not_matter() -> None:
    post = mock.Mock(side_effect=lambda *_args, **kwargs: content_response(kwargs["json"]["id"]))
    rows = [
        activity_row("newer", "Your Pulumi Receipt", hours_ago(1)),
        activity_row("older", "Your Pulumi Receipt", hours_ago(10)),
        activity_row("newer", "Your Pulumi Receipt", hours_ago(1)),
    ]

    records, state = run_tap(rows, post)

    assert posted_ids(post) == ["newer", "older"]
    assert len(records) == 2  # noqa: PLR2004
    assert content_bookmark(state) == rows[0]["ts"]


def test_first_run_only_looks_back_the_configured_days() -> None:
    post = mock.Mock(side_effect=lambda *_args, **kwargs: content_response(kwargs["json"]["id"]))
    rows = [
        activity_row("old", "Your Pulumi Receipt", hours_ago(24 * 5)),
        activity_row("recent", "Your Pulumi Receipt", hours_ago(24)),
    ]

    run_tap(rows, post, config={"content_lookback_days": 2})

    assert posted_ids(post) == ["recent"]


def test_expired_content_is_skipped_and_still_bookmarked() -> None:
    post = mock.Mock(return_value=FakeResponse(500, {"status": "error", "name": "Unknown_Message"}))
    rows = [activity_row("gone", "Your Pulumi Receipt", hours_ago(5))]

    records, state = run_tap(rows, post)

    assert records == []
    assert post.call_count == 1
    assert content_bookmark(state) == rows[0]["ts"]


def test_retries_transient_failures() -> None:
    post = mock.Mock(
        side_effect=[
            FakeResponse(503, ValueError("not json")),
            requests.ConnectionError("boom"),
            content_response("receipt-1"),
        ],
    )
    rows = [activity_row("receipt-1", "Your Pulumi Receipt", hours_ago(5))]

    records, _ = run_tap(rows, post)

    assert post.call_count == 3  # noqa: PLR2004
    assert [r["message_id"] for r in records] == ["receipt-1"]


@pytest.mark.parametrize("previous_bookmark", [None, hours_ago(48)])
def test_persistent_failure_stops_the_run_and_keeps_the_old_bookmark(previous_bookmark: str | None) -> None:
    def respond(*_args: object, **kwargs: dict) -> FakeResponse:
        if kwargs["json"]["id"] == "bad":
            return FakeResponse(500, {"status": "error", "name": "GeneralError"})
        return content_response(kwargs["json"]["id"])

    post = mock.Mock(side_effect=respond)
    rows = [
        activity_row("ok", "Your Pulumi Receipt", hours_ago(6)),
        activity_row("bad", "Your Pulumi Receipt", hours_ago(5)),
        activity_row("never-tried", "Your Pulumi Receipt", hours_ago(4)),
    ]
    state = {"bookmarks": {"message_content": {"content_fetched_through": previous_bookmark}}} if previous_bookmark else None

    records, final_state = run_tap(rows, post, state=state)

    # The activity export is not failed by a content error.
    assert [r["message_id"] for r in records] == ["ok"]
    assert posted_ids(post) == ["ok", "bad", "bad", "bad"]
    # The bookmark does not move, so the next run retries all three.
    assert content_bookmark(final_state) == previous_bookmark


def test_client_errors_are_not_retried() -> None:
    post = mock.Mock(return_value=FakeResponse(401, {"status": "error", "name": "Invalid_Key"}))
    rows = [activity_row("receipt-1", "Your Pulumi Receipt", hours_ago(5))]

    records, _ = run_tap(rows, post)

    assert records == []
    assert post.call_count == 1
