# tap-mandrill

`tap-mandrill` is a Singer tap for Mandrill (Mailchimp's transactional email service) that exports and processes activity history.

Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

## Installation

Install from the local repository:

```bash
pip install -e .
```

## Configuration

### Accepted Config Options

A full list of supported settings and capabilities for this tap is available by running:

```bash
tap-mandrill --about
```

| Setting | Required | Default | Description |
|:--------|:--------:|:-------:|:------------|
| auth_token | True | None | The Mandrill API key used for authentication |
| start_date | False | None | The earliest record date to sync (ISO format) |
| api_url | False | https://mandrillapp.com/api/1.0 | The Mandrill API base URL |
| content_subject_allowlist | False | None | Regular expressions (case-insensitive) matched against message subjects. The `message_content` stream fetches the HTML body only for matching messages. Unset means no content is fetched. |
| content_lookback_days | False | 3 | On the first run of `message_content`, how many days back to fetch content for |

## Streams

### `activity_export`

One row per sent message, from Mandrill's activity export: recipient, subject, status, opens and clicks. Each run re-reads the last 7 days so opens and clicks stay current.

### `message_content`

The HTML body of sent messages, one record per message (`message_id`, `ts`, `subject`, `from_email`, `html`, `fetched_at`). Mandrill only keeps message content for a limited time, so this stream archives it.

- It is a child of `activity_export`: for each exported message it calls `/messages/content` once.
- **Nothing is fetched unless `content_subject_allowlist` is set.** Some emails carry links that act on the recipient's account, such as password resets and invitations. Only list subjects that are safe to store.
- It keeps its own bookmark (`content_fetched_through`), so the 7-day re-read of `activity_export` does not refetch content.
- A message can be emitted more than once, after a failed run or at the boundary between two runs. Deduplicate on `message_id` downstream.
- A content request that keeps failing does not fail the sync. Content fetching stops for that run, the bookmark stays where it was, and the next run retries.

```json
{
  "auth_token": "YOUR_MANDRILL_API_KEY",
  "content_subject_allowlist": ["^Your Receipt$", "^Your trial ends on .+$"]
}
```

### Configure using environment variables

This Singer tap will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

### Source Authentication and Authorization

This tap uses the Mandrill API key for authentication. You can get your API key from your Mailchimp/Mandrill account settings.

## Usage

You can easily run `tap-mandrill` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Tap Directly

Create a config file:

```json
{
  "auth_token": "YOUR_MANDRILL_API_KEY",
  "start_date": "2023-01-01T00:00:00Z"
}
```

Then run:

```bash
# Discover mode
tap-mandrill --config config.json --discover > catalog.json

# Edit the catalog to select the streams you want
# Then run the tap in sync mode
tap-mandrill --config config.json --catalog catalog.json 
```

To pipe the output to a target:

```bash
tap-mandrill --config config.json --catalog catalog.json | target-jsonl --config target-config.json
```

### Testing with Meltano

You can also use Meltano to test the tap:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd tap-mandrill
meltano install

# Configure the tap
meltano config tap-mandrill set auth_token YOUR_MANDRILL_API_KEY

# Run the ELT pipeline
meltano run tap-mandrill target-jsonl
```

## About Streams

### activity_export

This stream exports activity history from Mandrill using the `/exports/activity` endpoint, which returns CSV data with email activity information. The stream:

1. Requests an export for activities in daily batches from the start date up to yesterday
2. Polls the export status until it's complete
3. Downloads the ZIP file and extracts the CSV content
4. Maps the CSV fields to the stream schema fields
5. Updates the state to yesterday's date for the next sync

#### Date Range Handling

The tap follows these rules for date ranges:
- First run: Uses `start_date` from config up to yesterday (not including today)
- Subsequent runs: Uses the last state bookmark up to yesterday
- Always uses yesterday as the end date (not today) to ensure complete data

#### Field Mapping

The stream maps these CSV fields from the Mandrill export:
- "Message ID" → "message_id"  
- "Date" → "ts" (timestamp)
- "Email Address" → "email"
- "Sender" → "sender"
- "Subject" → "subject"
- "Status" → "status"
- "Tags" → "tags"
- "Subaccount" → "subaccount"
- "Opens" → "opens"
- "Clicks" → "clicks"
- "Bounce Detail" → "bounce_detail"

#### Error Handling and Performance

- The tap handles network errors and retries when necessary
- It supports direct S3 URL download when provided by the Mandrill API
- For large exports, it processes data in daily batches to improve reliability

## Development

### Initialize your Development Environment

Prerequisites:

- Python 3.9+
- [uv](https://docs.astral.sh/uv/)

```bash
uv sync
```

### Create and Run Tests

Create tests within the `tests` subfolder and then run:

```bash
uv run pytest
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to
develop your own taps and targets.
