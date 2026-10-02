"""Stream type classes for tap-mandrill."""

from __future__ import annotations

import typing as t
import csv
import datetime
import io
import re
import time
from importlib import resources

import requests
from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_mandrill.client import MandrillStream

# Directory for schema definitions if using JSON schema files
SCHEMAS_DIR = resources.files(__package__) / "schemas"


class ActivityExportStream(MandrillStream):
    """Mandrill Activity Export stream.
    
    This stream exports activity history from Mandrill and processes the CSV output.
    It handles state management to only process new data since the last run.
    """

    name = "activity_export"
    path = "/exports/activity"
    primary_keys: t.ClassVar[list[str]] = ["ts", "message_id"]
    replication_key = "ts"
    rest_method = "POST"
    
    schema = th.PropertiesList(
        # CSV fields based on actual Mandrill export format
        th.Property("ts", th.DateTimeType, description="The timestamp of the event"),
        th.Property("message_id", th.StringType, description="The message ID"),
        th.Property("email", th.StringType, description="The email address"),
        th.Property("sender", th.StringType, description="The sender address"),
        th.Property("subject", th.StringType, description="The subject line"),
        th.Property("status", th.StringType, description="The status of the message"),
        th.Property("channel", th.StringType, description="The delivery channel (email or sms)"),
        th.Property("tags", th.StringType, description="The tags associated with the message"),
        th.Property("subaccount", th.StringType, description="The subaccount the message belongs to"),
        th.Property("opens", th.IntegerType, description="Number of opens"),
        th.Property("clicks", th.IntegerType, description="Number of clicks"),
        th.Property("bounce_detail", th.StringType, description="Bounce description if applicable"),
    ).to_dict()
    
    def generate_child_contexts(
        self,
        record: dict,
        context: dict | None,  # noqa: ARG002
    ) -> t.Iterable[dict | None]:
        """Yield a context for each message whose content should be fetched.

        Most rows yield nothing: the content stream only wants messages that match
        its subject allowlist and that it has not fetched on an earlier run.

        Args:
            record: An activity export row.
            context: Stream partition or context dictionary.

        Yields:
            A context for the ``message_content`` stream.
        """
        for child in self.child_streams:
            if isinstance(child, MessageContentStream) and child.wants(record):
                yield {
                    "message_id": record.get("message_id"),
                    "ts": record.get("ts"),
                    "subject": record.get("subject") or "",
                }

    def get_records(self, context: dict | None) -> t.Iterable[dict]:
        """Get records from the Mandrill activity export API.
        
        This method:
        1. Requests exports for date ranges in monthly batches
        2. Waits for each export to complete
        3. Downloads and processes each CSV
        4. Updates the state with the latest date processed
        
        Args:
            context: Stream partition or context dictionary.
            
        Yields:
            Each record from the CSV exports.
        """
        import time
        
        # For incremental syncs, get the last bookmark from state
        bookmark_date = self.get_starting_timestamp(context)
        now = datetime.datetime.now(datetime.timezone.utc)
        yesterday = now - datetime.timedelta(days=1)
        
        # Calculate the date 7 days ago to implement the last-week-full-sync logic
        seven_days_ago = now - datetime.timedelta(days=7)
        
        # Determine start date based on state or config
        if bookmark_date:
            # If the bookmark is more recent than seven days ago, 
            # always fetch the full last 7 days
            if bookmark_date > seven_days_ago:
                start_date = seven_days_ago
                self.logger.info(f"Bookmark is within last 7 days, fetching full week from: {start_date.isoformat()}")
            else:
                # Use the bookmark date for older data
                start_date = bookmark_date
                self.logger.info(f"Using bookmark date: {start_date.isoformat()}")
        else:
            # Use default from config if no state
            config_start_date = self.config.get("start_date")
            if config_start_date:
                start_date = datetime.datetime.fromisoformat(config_start_date.replace("Z", "+00:00"))
                # If the config start_date is after yesterday, use yesterday
                if start_date > yesterday:
                    start_date = yesterday
                    self.logger.info(f"Config start_date is in the future, using yesterday instead: {start_date.isoformat()}")
            else:
                # Default to 7 days ago if no start date is provided
                start_date = seven_days_ago
                self.logger.info(f"No start date provided, using 7 days ago: {start_date.isoformat()}")
                
        # End date is always yesterday (to ensure complete data)
        end_date = yesterday
        self.logger.info(f"Date range: {start_date.isoformat()} to {end_date.isoformat()}")
        
        # Calculate date ranges for processing (divide into daily chunks)
        days_range = []
        
        # Always process from start_date up to yesterday (not including today)
        current_start = start_date
        
        self.logger.info(f"Processing data from {current_start.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        
        # Create daily ranges
        while current_start <= end_date:
            # For each day, use the NEXT day as the end date (inclusive range for Mandrill API)
            current_end = current_start + datetime.timedelta(days=1)
            days_range.append((current_start, current_end))
            self.logger.info(f"Added date range: {current_start.strftime('%Y-%m-%d')} to {current_end.strftime('%Y-%m-%d')}")
            current_start = current_start + datetime.timedelta(days=1)
            
        self.logger.info(f"Processing {len(days_range)} day(s) of data")
        
        # If no days in range, don't process anything
        if not days_range:
            self.logger.info("No days to process, skipping.")
            return
            
        # Save original path and method to restore later
        original_path = self.path
        original_method = self.rest_method
        
        # Track the latest processed date for state management
        latest_processed_date = None
        
        # Process each daily range
        for i, (batch_start, batch_end) in enumerate(days_range):
            # Format dates for Mandrill API (YYYY-MM-DD)
            # For Mandrill API, the end date should be the next day to get full day data
            date_from = batch_start.strftime("%Y-%m-%d")
            date_to = batch_end.strftime("%Y-%m-%d")
            
            self.logger.info(f"Processing batch {i+1}/{len(days_range)}: {date_from} to {date_to} (inclusive range for Mandrill API)")
            
            # Reset path and method for each batch
            self.path = "/exports/activity"
            self.rest_method = "POST"
            
            # Store the export request data
            self._request_data = {
                "date_from": date_from,
                "date_to": date_to,
            }
            
            # Request the export
            records = list(self.request_records(context))
            
            # Process the first record to get the export ID
            if not records or "id" not in records[0]:
                self.logger.error(f"Failed to get export ID for batch {date_from} to {date_to}")
                continue  # Skip to next batch instead of failing completely
                
            export_id = records[0]["id"]
            self.logger.info(f"Got export ID: {export_id} for batch {date_from} to {date_to}")
            
            # Now we need to wait for the export to complete
            # Set the path for the status check
            self.path = "/exports/info"
            self._request_data = {"id": export_id}
            
            # Poll until the export is complete with increased timeout
            max_retries = 60  # Doubled from 30 to 60 retries
            retry_count = 0
            export_complete = False
            result_url = None  # Variable to store the S3 URL
            
            while retry_count < max_retries:
                # Get export status
                status_records = list(self.request_records(context))
                
                if status_records:
                    # Check for both status and state fields
                    status = status_records[0].get("status", "unknown")
                    state = status_records[0].get("state", "")
                    
                    # If state is present, use it instead of status
                    if state:
                        status = state
                        self.logger.info(f"Using 'state' field value: {state}")
                    
                    # Check if there's a result_url for direct S3 download
                    result_url = status_records[0].get("result_url", "")
                    if result_url:
                        self.logger.info(f"Found direct S3 URL for download: {result_url}")
                    
                    # Get more detailed status information
                    created_rows = status_records[0].get("created_rows", 0)
                    created_rows_total = status_records[0].get("created_rows_total", 0)
                    percent = 0
                    if created_rows_total:
                        percent = (created_rows / created_rows_total) * 100
                        
                    # Log detailed progress information
                    progress_str = f" [{created_rows}/{created_rows_total} rows - {percent:.1f}%]"
                    self.logger.info(f"Export status: {status}{progress_str}")
                    
                    # Check if export is ready for download
                    if status == "complete":
                        self.logger.info(f"Export complete! Ready to download data")
                        export_complete = True
                        break
                        
                    # If we have a result_url but status is not complete, we can still proceed
                    if result_url and retry_count > 3:
                        self.logger.info("Found download URL even though status is not 'complete', proceeding with download")
                        export_complete = True
                        break
                        
                # Not ready yet, wait and retry with longer delay
                retry_count += 1
                self.logger.info(f"Export not ready, retrying {retry_count}/{max_retries}")
                
                # Wait 300 seconds between retries (5 minutes)
                time.sleep(300)
                
            if not export_complete:
                self.logger.error(f"Export timed out for batch {date_from} to {date_to}")
                continue  # Skip to next batch instead of failing completely
                
            # Get CSV data using the best available method
            csv_data = ""
            
            # If we have a direct S3 URL, use it for download
            if result_url:
                self.logger.info(f"Downloading directly from S3 URL: {result_url}")
                import requests
                
                try:
                    # Log that we're starting the download
                    self.logger.info(f"Starting download from S3 for batch {date_from} to {date_to}")
                    
                    # Download the ZIP file
                    download_start = datetime.datetime.now()
                    download_response = requests.get(result_url)
                    download_end = datetime.datetime.now()
                    download_time = (download_end - download_start).total_seconds()
                    
                    # Check for HTTP errors
                    download_response.raise_for_status()
                    
                    # Get content length
                    content_length = len(download_response.content)
                    self.logger.info(f"Downloaded {content_length} bytes in {download_time:.1f} seconds ({content_length/download_time:.0f} bytes/s)")
                    
                    # Handle ZIP file
                    import io
                    import zipfile
                    
                    # Try to extract CSV from ZIP
                    try:
                        zip_data = io.BytesIO(download_response.content)
                        with zipfile.ZipFile(zip_data) as zip_file:
                            # List files in the ZIP
                            file_list = zip_file.namelist()
                            self.logger.info(f"ZIP contains {len(file_list)} files: {file_list}")
                            
                            # Look for CSV file
                            csv_files = [f for f in file_list if f.endswith('.csv')]
                            if csv_files:
                                # Extract the first CSV file
                                csv_filename = csv_files[0]
                                self.logger.info(f"Extracting CSV file: {csv_filename}")
                                csv_data = zip_file.read(csv_filename).decode('utf-8')
                                self.logger.info(f"Extracted CSV data: {len(csv_data)} bytes")
                                
                                # Log a sample of the CSV content for debugging
                                preview_size = min(500, len(csv_data))
                                self.logger.info(f"CSV preview (first {preview_size} chars): {csv_data[:preview_size]}")
                            else:
                                # If no CSV files, try any text file
                                text_files = [f for f in file_list if not f.endswith('/')]
                                if text_files:
                                    # Extract the first text file
                                    text_filename = text_files[0]
                                    self.logger.info(f"No CSV files found. Trying to extract text file: {text_filename}")
                                    csv_data = zip_file.read(text_filename).decode('utf-8', errors='replace')
                                    self.logger.info(f"Extracted text data: {len(csv_data)} bytes")
                                else:
                                    self.logger.error("No files found in ZIP")
                                    # Try to log all ZIP contents for debugging
                                    for file_info in zip_file.infolist():
                                        self.logger.info(f"ZIP entry: {file_info.filename}, size: {file_info.file_size}")
                    except zipfile.BadZipFile:
                        self.logger.error("Not a valid ZIP file, trying to process as CSV directly")
                        csv_data = download_response.text
                        # Log the content type and a preview
                        self.logger.info(f"Response content type: {download_response.headers.get('Content-Type')}")
                        preview_size = min(500, len(csv_data))
                        self.logger.info(f"Response content preview: {csv_data[:preview_size]}")
                except Exception as e:
                    self.logger.error(f"Error downloading from S3: {str(e)}")
                    # Continue with the standard API method as fallback
            
            # If S3 download failed or no URL was provided, try the standard API method
            if not csv_data:
                self.logger.info("Using standard API method to download CSV")
                self.path = f"/exports/download/{export_id}"
                self.rest_method = "GET"
                self._request_data = None
                
                # Log that we're starting the download
                self.logger.info(f"Starting download of CSV data for batch {date_from} to {date_to}")
                
                # Get the CSV data
                download_start = datetime.datetime.now()
                csv_records = list(self.request_records(context))
                download_end = datetime.datetime.now()
                download_time = (download_end - download_start).total_seconds()
                
                if not csv_records or "_response_text" not in csv_records[0]:
                    self.logger.error(f"Failed to get CSV content for batch {date_from} to {date_to}")
                    continue  # Skip to next batch
                    
                csv_data = csv_records[0]["_response_text"]
                csv_size = len(csv_data)
                
                # Log download statistics
                self.logger.info(f"Downloaded {csv_size} bytes in {download_time:.1f} seconds ({csv_size/download_time:.0f} bytes/s)")
            
            # Check if we got empty data
            if not csv_data.strip():
                self.logger.warning(f"Empty CSV data for batch {date_from} to {date_to}")
                continue  # Skip to next batch
                
            # Process the CSV data
            csv_file = io.StringIO(csv_data)
            reader = csv.DictReader(csv_file)
            
            # Dump CSV content for debugging if it's small enough
            if len(csv_data) < 1000:
                self.logger.info(f"CSV content preview: {csv_data}")
            else:
                self.logger.info(f"CSV content length: {len(csv_data)} bytes")
                # Show the first few lines
                first_lines = csv_data.split('\n')[:5]
                self.logger.info(f"First {len(first_lines)} lines: {first_lines}")
                
            # Get the CSV headers
            headers = reader.fieldnames
            self.logger.info(f"CSV headers: {headers}")

            # Convert CSV rows to properly typed records
            row_count = 0
            processed_count = 0
            
            # CSV field mapping from Mandrill export to our schema.
            # NOTE: Mandrill renamed the recipient column "Email Address" -> "Recipient"
            # and added "Channel" around Oct-Nov 2025. The old guard silently skipped
            # absent columns, so the rename nulled every recipient with no error.
            field_mapping = {
                "Message ID": "message_id",
                "Date": "ts",
                "Recipient": "email",
                "Sender": "sender",
                "Subject": "subject",
                "Status": "status",
                "Channel": "channel",
                "Tags": "tags",
                "Subaccount": "subaccount",
                "Opens": "opens",
                "Clicks": "clicks",
                "Bounce Detail": "bounce_detail"
            }

            # Fail loud if Mandrill drops or renames a column we depend on, instead
            # of silently emitting NULLs. Skip the check for an empty export (no
            # headers), which is a legitimate result for a quiet date range.
            required_source_columns = ("Message ID", "Date", "Recipient")
            if headers:
                missing = [c for c in required_source_columns if c not in headers]
                if missing:
                    raise RuntimeError(
                        f"Mandrill activity export is missing expected column(s) "
                        f"{missing}; got headers {headers}. The export schema changed "
                        f"again - update field_mapping in streams.py."
                    )

            for row in reader:
                row_count += 1
                
                # Skip empty rows
                if not row or all(not v for v in row.values()):
                    self.logger.debug(f"Skipping empty row #{row_count}")
                    continue
                
                # Log the original row for debugging
                if row_count <= 5:
                    self.logger.info(f"Original CSV row #{row_count}: {row}")
                
                # Create a new row with mapped field names
                mapped_row = {}
                for csv_field, schema_field in field_mapping.items():
                    if csv_field in row:
                        mapped_row[schema_field] = row[csv_field]
                
                # Log the mapped row for debugging
                if row_count <= 5:
                    self.logger.info(f"Mapped row #{row_count}: {mapped_row}")
                
                # Ensure the timestamp field exists (required for replication key)
                if "ts" not in mapped_row or not mapped_row["ts"]:
                    # If the row doesn't have a timestamp, use the batch_end time
                    self.logger.warning(f"Row missing 'ts' field, using batch end time: {mapped_row}")
                    mapped_row["ts"] = batch_end.isoformat()
                else:
                    # Convert timestamp to datetime
                    try:
                        # Try to parse the date in the format provided by Mandrill
                        ts = datetime.datetime.strptime(mapped_row["ts"], "%Y-%m-%d %H:%M:%S")
                        mapped_row["ts"] = ts.replace(tzinfo=datetime.timezone.utc).isoformat()
                    except (ValueError, TypeError):
                        try:
                            # Try alternate format with timezone
                            ts = datetime.datetime.fromisoformat(mapped_row["ts"].replace("Z", "+00:00"))
                            mapped_row["ts"] = ts.isoformat()
                        except (ValueError, TypeError):
                            self.logger.warning(f"Invalid timestamp format: {mapped_row['ts']}, using batch end time")
                            mapped_row["ts"] = batch_end.isoformat()
            
                # Convert numeric fields
                for field in ["opens", "clicks"]:
                    if field in mapped_row and mapped_row[field]:
                        try:
                            mapped_row[field] = int(mapped_row[field])
                        except (ValueError, TypeError):
                            mapped_row[field] = 0
                    else:
                        # Default to 0 if field is missing
                        mapped_row[field] = 0
                
                # Double check that all required fields exist before yielding
                if "ts" not in mapped_row:
                    self.logger.error(f"Critical error: Row still missing 'ts' field after mapping: {mapped_row}")
                    continue  # Skip this row
                    
                if "message_id" not in mapped_row or not mapped_row["message_id"]:
                    self.logger.warning(f"Row missing 'message_id' field: {mapped_row}")
                    # Generate a placeholder message_id based on other fields to avoid primary key issues
                    identifier = f"{mapped_row.get('email', '')}-{mapped_row['ts']}"
                    mapped_row["message_id"] = f"generated-{hash(identifier)}"
                    
                # Increment processed count and yield the mapped row
                processed_count += 1
                yield mapped_row
            
            self.logger.info(f"Processed {processed_count} of {row_count} rows for batch {date_from} to {date_to}")
            
            # If no rows were processed but we had data, log a warning
            if row_count > 0 and processed_count == 0:
                self.logger.warning("No rows were processed despite having CSV data. Check the CSV format.")
                # Dump headers and first row again for debugging
                if len(csv_data) > 0:
                    csv_lines = csv_data.split('\n')
                    if len(csv_lines) > 0:
                        self.logger.info(f"CSV header line: {csv_lines[0]}")
                        if len(csv_lines) > 1:
                            self.logger.info(f"CSV first data line: {csv_lines[1]}")
            elif row_count == 0:
                self.logger.warning("CSV file was empty or had no data rows.")
            
            # Update the latest processed date
            latest_processed_date = batch_end
            
            # After each batch, update the state
            if latest_processed_date:
                self._write_replication_key_signpost(context=None, value=latest_processed_date.isoformat())
                self.logger.info(f"Updated state to {latest_processed_date.isoformat()}")
        
        # Reset the path and method for next time
        self.path = original_path
        self.rest_method = original_method
        
        # Final state update - intentionally set bookmark to 7 days ago
        # This ensures we always reprocess the last 7 days to catch updates
        seven_days_ago = now - datetime.timedelta(days=7)
        
        if latest_processed_date:
            # For any data older than 7 days ago, we can safely use that as bookmark
            if latest_processed_date < seven_days_ago:
                self._write_replication_key_signpost(context=None, value=latest_processed_date.isoformat())
                self.logger.info(f"Updated state to last processed date: {latest_processed_date.isoformat()}")
            else:
                # For recent data, always set bookmark to 7 days ago
                self._write_replication_key_signpost(context=None, value=seven_days_ago.isoformat())
                self.logger.info(f"Set state to 7 days ago to ensure full week sync: {seven_days_ago.isoformat()}")
        else:
            # No data was processed, set state to 7 days ago
            self._write_replication_key_signpost(context=None, value=seven_days_ago.isoformat())
            self.logger.info(f"No data processed, set state to 7 days ago: {seven_days_ago.isoformat()}")



class MessageContentStream(MandrillStream):
    """The rendered HTML of sent messages, one record per message.

    Mandrill only keeps message content for a limited time, so this stream archives
    it. It is a child of the activity export: for each exported message that matches
    ``content_subject_allowlist`` and has not been fetched before, it calls
    ``/messages/content`` once.

    Nothing is fetched unless ``content_subject_allowlist`` is set. Some emails carry
    links that act on the recipient's account (password resets, invitations), and
    the allowlist is what keeps those out of the warehouse.

    A message can be emitted more than once, when a run stops fetching after a
    failure and the next run retries, so consumers should deduplicate on
    ``message_id``.
    """

    name = "message_content"
    path = "/messages/content"
    rest_method = "POST"
    parent_stream_type = ActivityExportStream
    primary_keys: t.ClassVar[list[str]] = ["message_id"]
    replication_key = None
    # One bookmark for the whole stream, rather than a state partition per message.
    state_partitioning_keys: t.ClassVar[list[str]] = []

    # The activity stream re-reads the last 7 days on every run, so this stream keeps
    # its own bookmark: the newest send time whose content has been fetched, plus
    # the IDs of the messages handled at exactly that time. Send times have
    # one-second resolution, so the time alone cannot tell a message that was
    # already fetched from a new one sent in the same second.
    STATE_KEY = "content_fetched_through"
    STATE_IDS_KEY = "content_fetched_ids"
    GENERATED_ID_PREFIX = "generated-"

    request_timeout_seconds = 30
    max_attempts = 3
    retry_wait_seconds = 5

    schema = th.PropertiesList(
        th.Property("message_id", th.StringType, description="The message ID"),
        th.Property("ts", th.DateTimeType, description="When the message was sent"),
        th.Property("subject", th.StringType, description="The subject line"),
        th.Property("from_email", th.StringType, description="The sender address"),
        th.Property("html", th.StringType, description="The HTML body as sent"),
        th.Property("fetched_at", th.DateTimeType, description="When the content was fetched"),
    ).to_dict()

    def __init__(self, *args: t.Any, **kwargs: t.Any) -> None:
        """Initialize the stream."""
        super().__init__(*args, **kwargs)
        self._allowlist = [
            re.compile(pattern, re.IGNORECASE)
            for pattern in self.config.get("content_subject_allowlist") or []
        ]
        self._run_started = False
        self._previous_bookmark: str | None = None
        self._previous_ids: list[str] = []
        self._cutoff: datetime.datetime | None = None
        self._newest_fetched: datetime.datetime | None = None
        self._newest_ids: set[str] = set()
        self._seen_message_ids: set[str] = set()
        self._halted = False

    def _start_run(self) -> None:
        """Freeze the cutoff for this run from the bookmark the last run left."""
        if self._run_started:
            return
        self._run_started = True
        self._previous_bookmark = self.stream_state.get(self.STATE_KEY)
        if self._previous_bookmark:
            self._cutoff = _parse_timestamp(self._previous_bookmark)
        if self._cutoff is not None:
            # Carry the bookmark into this run, so a new message sent in the same
            # second as the bookmark is added to its IDs rather than replacing them.
            self._previous_ids = list(self.stream_state.get(self.STATE_IDS_KEY) or [])
            self._newest_fetched = self._cutoff
            self._newest_ids = set(self._previous_ids)
        else:
            # First run: older content has usually expired from Mandrill already.
            lookback_days = self.config["content_lookback_days"]
            self._cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=lookback_days)
        self.logger.info(f"Fetching content for allowlisted messages sent since {self._cutoff.isoformat()}")

    def wants(self, record: dict) -> bool:
        """Return whether the content of this activity row should be fetched.

        Args:
            record: An activity export row.

        Returns:
            True when the message is allowlisted and not yet fetched.
        """
        if not self._allowlist or self._halted:
            return False

        message_id = record.get("message_id") or ""
        if not message_id or message_id.startswith(self.GENERATED_ID_PREFIX):
            return False
        if message_id in self._seen_message_ids:
            return False

        subject = " ".join((record.get("subject") or "").split())
        if not any(pattern.search(subject) for pattern in self._allowlist):
            return False

        self._start_run()
        sent_at = _parse_timestamp(record.get("ts"))
        if sent_at is None or self._cutoff is None or sent_at < self._cutoff:
            return False
        # At exactly the bookmark time, only messages not handled before are new.
        return sent_at > self._cutoff or message_id not in self._previous_ids

    def get_records(self, context: dict | None) -> t.Iterable[dict]:
        """Fetch the content of the one message named by the parent's context.

        Args:
            context: The child context built by the activity stream.

        Yields:
            At most one record, holding the message's HTML.
        """
        if not context or self._halted:
            return

        message_id = context["message_id"]
        self._seen_message_ids.add(message_id)
        try:
            content = self._fetch_content(message_id)
        except Exception as e:  # noqa: BLE001
            # Content is an extra. Losing it must not fail the activity export, so stop
            # fetching for this run and leave the bookmark where the last run put it.
            # The next run then retries everything this run attempted.
            self.logger.error(f"Stopping content fetch for this run after message {message_id} failed: {e}")
            self._halt()
            return

        self._advance(context["ts"], message_id)
        if content is None:
            self.logger.warning(f"Mandrill has no content for message {message_id}, skipping")
            return

        yield {
            "message_id": message_id,
            "ts": context["ts"],
            "subject": content.get("subject") or context.get("subject") or "",
            "from_email": content.get("from_email") or "",
            "html": content["html"],
            "fetched_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }

    def _fetch_content(self, message_id: str) -> dict | None:
        """Call ``/messages/content``.

        Args:
            message_id: The Mandrill message ID.

        Returns:
            The response body, or None when Mandrill no longer has the content.

        Raises:
            RuntimeError: When the request keeps failing.
        """
        payload = {**(self.prepare_request_payload(None, None) or {}), "id": message_id}
        url = f"{self.url_base}{self.path}"
        last_error = "no attempt made"

        for attempt in range(1, self.max_attempts + 1):
            try:
                response = requests.post(
                    url,
                    json=payload,
                    headers=self.http_headers,
                    timeout=self.request_timeout_seconds,
                )
            except requests.RequestException as e:
                # The exception text can include the request, so log only its type.
                last_error = type(e).__name__
            else:
                body = _json_object(response)
                if response.ok:
                    if body is None:
                        last_error = "response was not a JSON object"
                    elif not body.get("html"):
                        return None
                    else:
                        return body
                elif body is not None and body.get("name") == "Unknown_Message":
                    # Mandrill reports both an unknown ID and expired content this way.
                    return None
                else:
                    last_error = f"HTTP {response.status_code}"
                    if body is not None and body.get("name"):
                        last_error += f" ({body['name']})"
                    retryable = response.status_code == 429 or response.status_code >= 500
                    if not retryable:
                        break

            if attempt < self.max_attempts:
                time.sleep(self.retry_wait_seconds * attempt)

        msg = f"Mandrill content request failed: {last_error}"
        raise RuntimeError(msg)

    def _advance(self, ts: str, message_id: str) -> None:
        """Move the bookmark to the newest send time handled so far."""
        sent_at = _parse_timestamp(ts)
        if sent_at is None:
            return
        if self._newest_fetched is None or sent_at > self._newest_fetched:
            self._newest_fetched = sent_at
            self._newest_ids = {message_id}
        elif sent_at == self._newest_fetched:
            self._newest_ids.add(message_id)
        else:
            return
        self.stream_state[self.STATE_KEY] = self._newest_fetched.isoformat()
        self.stream_state[self.STATE_IDS_KEY] = sorted(self._newest_ids)

    def _halt(self) -> None:
        """Stop fetching for this run and restore the bookmark the run started with."""
        self._halted = True
        if self._previous_bookmark:
            self.stream_state[self.STATE_KEY] = self._previous_bookmark
            self.stream_state[self.STATE_IDS_KEY] = self._previous_ids
        else:
            self.stream_state.pop(self.STATE_KEY, None)
            self.stream_state.pop(self.STATE_IDS_KEY, None)


def _parse_timestamp(value: str | None) -> datetime.datetime | None:
    """Parse an ISO timestamp, treating a naive value as UTC."""
    if not value:
        return None
    try:
        parsed = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=datetime.timezone.utc)
    return parsed


def _json_object(response: requests.Response) -> dict | None:
    """Return the response body when it is a JSON object, else None."""
    try:
        body = response.json()
    except ValueError:
        return None
    return body if isinstance(body, dict) else None
