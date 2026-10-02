"""Mandrill tap implementation."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_mandrill import streams


class TapMandrill(Tap):
    """Mandrill tap class."""

    name = "tap-mandrill"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "auth_token",
            th.StringType(nullable=False),
            required=True,
            secret=True,  # Flag config as protected.
            title="Auth Token",
            description="The Mandrill API key used for authentication",
        ),
        th.Property(
            "start_date",
            th.DateTimeType(nullable=True),
            description="The earliest record date to sync (ISO format)",
        ),
        th.Property(
            "api_url",
            th.StringType(nullable=False),
            title="API URL",
            default="https://mandrillapp.com/api/1.0",
            description="The Mandrill API base URL",
        ),
        th.Property(
            "content_subject_allowlist",
            th.ArrayType(th.StringType),
            title="Content Subject Allowlist",
            description=(
                "Regular expressions (case-insensitive) matched against message "
                "subjects. The message_content stream fetches the HTML body only for "
                "messages whose subject matches one of them. Leave unset to fetch no "
                "content; some emails carry account-action links that should not be "
                "stored."
            ),
        ),
        th.Property(
            "content_lookback_days",
            th.IntegerType,
            title="Content Lookback Days",
            default=3,
            description=(
                "On the first run of the message_content stream, how many days back "
                "to fetch content for. Later runs continue from the stream's bookmark."
            ),
        ),
        th.Property(
            "user_agent",
            th.StringType(nullable=True),
            description=(
                "A custom User-Agent header to send with each request. Default is "
                "'<tap_name>/<tap_version>'"
            ),
        ),
    ).to_dict()

    def discover_streams(self) -> list[streams.MandrillStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.ActivityExportStream(self),
            streams.MessageContentStream(self),
        ]


if __name__ == "__main__":
    TapMandrill.cli()
