"""REST client handling, including mandrillStream base class."""

from __future__ import annotations

import decimal
import typing as t
import json
import os
import requests
from importlib import resources

# Add dotenv support for local testing
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from singer_sdk.authenticators import APIKeyAuthenticator
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BaseAPIPaginator  # noqa: TC002
from singer_sdk.streams import RESTStream

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context


# Directory for schema definitions if using JSON schema files
SCHEMAS_DIR = resources.files(__package__) / "schemas"


class MandrillStream(RESTStream):
    """Mandrill stream base class."""

    # Update this value if necessary or override `parse_response`.
    records_jsonpath = "$[*]"

    # Update this value if necessary or override `get_new_paginator`.
    next_page_token_jsonpath = "$.next_page"  # noqa: S105

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return self.config.get("api_url", "https://mandrillapp.com/api/1.0")

    @property
    def authenticator(self) -> APIKeyAuthenticator | None:
        """Return a new authenticator object.

        Returns:
            An authenticator instance.
        """
        # Mandrill API requires the key parameter in the request body
        # We'll handle this in prepare_request_payload
        return None

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed.

        Returns:
            A dictionary of HTTP headers.
        """
        headers = {}
        
        # Set content type for POST requests
        if self.rest_method == "POST":
            headers["Content-Type"] = "application/json"
            
        # Add custom user agent if provided
        if self.config.get("user_agent"):
            headers["User-Agent"] = self.config.get("user_agent")
            
        return headers

    def get_new_paginator(self) -> BaseAPIPaginator:
        """Create a new pagination helper instance.

        If the source API can make use of the `next_page_token_jsonpath`
        attribute, or it contains a `X-Next-Page` header in the response
        then you can remove this method.

        If you need custom pagination that uses page numbers, "next" links, or
        other approaches, please read the guide: https://sdk.meltano.com/en/v0.25.0/guides/pagination-classes.html.

        Returns:
            A pagination helper instance.
        """
        return super().get_new_paginator()

    def get_url_params(
        self,
        context: Context | None,  # noqa: ARG002
        next_page_token: t.Any | None,  # noqa: ANN401
    ) -> dict[str, t.Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        params: dict = {}
        if next_page_token:
            params["page"] = next_page_token
        if self.replication_key:
            params["sort"] = "asc"
            params["order_by"] = self.replication_key
        return params

    def prepare_request_payload(
        self,
        context: Context | None,  # noqa: ARG002
        next_page_token: t.Any | None,  # noqa: ARG002, ANN401
    ) -> dict | None:
        """Prepare the data payload for the REST API request.

        For Mandrill API, we need to include the API key in the request body.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary with the JSON body for a POST requests.
        """
        # Return None for GET requests
        if self.rest_method == "GET":
            return None
            
        # Try to get the API key from different sources
        auth_token = self.config.get("auth_token", "")
        
        # If not in config, check environment variables
        if not auth_token:
            # Check for TAP_MANDRILL_AUTH_TOKEN environment variable
            auth_token = os.environ.get("TAP_MANDRILL_AUTH_TOKEN", "")
            if auth_token and (auth_token.startswith("'") or auth_token.startswith('"')):
                # Remove quotes if present
                auth_token = auth_token.strip("'\"")
            
            self.logger.debug(f"Using auth token from environment: {auth_token[:5]}...")
            
        # For POST requests, always include the API key
        payload = {
            "key": auth_token,
        }
        
        # If there's additional request data from a subclass, merge it
        if hasattr(self, "_request_data") and self._request_data:
            payload.update(self._request_data)
            
        return payload

    # Override the request method completely to handle our custom API needs
    def request_records(self, context: dict | None) -> t.Iterable[dict]:
        """Request records from REST endpoint(s), returning response records.

        If pagination is detected, pages will be recursed automatically.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            An item for every record in the response.
        """
        # Initialize paging parameters
        next_page_token = None
        finished = False
        
        # Get records until no more pages are available
        while not finished:
            # Get the URL parameters (used for pagination)
            params = self.get_url_params(context, next_page_token)
            
            # Get the request payload
            data = self.prepare_request_payload(context, next_page_token)
            
            # Make the API request
            self.logger.debug(f"Making request to {self.path}")
            
            url = f"{self.url_base}{self.path}"
            
            # For POST requests, include the auth token in the body
            if self.rest_method.upper() == "POST":
                # Make the POST request
                resp = requests.post(
                    url,
                    json=data,
                    headers=self.http_headers,
                    params=params,
                )
            else:
                # Make the GET request
                resp = requests.get(
                    url,
                    headers=self.http_headers,
                    params=params,
                )
            
            # Check for HTTP errors
            resp.raise_for_status()
            
            # Check if the response is a CSV or text
            content_type = resp.headers.get("Content-Type", "")
            if "text/csv" in content_type or "text/plain" in content_type:
                # For CSV/text responses, yield a special record
                yield {"_response_text": resp.text}
            else:
                # For JSON responses, parse and yield records
                try:
                    data = resp.json(parse_float=decimal.Decimal)
                    
                    # For export API endpoints, get the ID directly
                    if self.path == "/exports/activity" and isinstance(data, dict) and "id" in data:
                        yield data
                    # Check if the endpoint is /exports/info
                    elif self.path == "/exports/info" and isinstance(data, dict):
                        yield data
                    # For all other endpoints, use the JsonPath extraction
                    else:
                        for record in extract_jsonpath(self.records_jsonpath, input=data):
                            yield record
                            
                except json.JSONDecodeError as e:
                    self.logger.error(f"Failed to parse JSON response: {e}")
                    self.logger.debug(f"Response content: {resp.text[:1000]}")
                    
            # No pagination for Mandrill API in our implementation
            finished = True

    def parse_response(self, response: requests.Response) -> t.Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: The HTTP ``requests.Response`` object.

        Yields:
            Each record from the source.
        """
        # This method is overridden by request_records and should not be called
        # However, in case it is called, handle it properly
        
        # Check if response is a string (special case from our implementation)
        if isinstance(response, str):
            yield {"_response_text": response}
            return
            
        # Check if response is JSON or not
        content_type = response.headers.get("Content-Type", "")
        
        # For CSV/text responses, just pass the text content instead of the response object
        if "text/csv" in content_type or "text/plain" in content_type:
            # Return the response text content as a special record that will be handled
            # by the specific stream implementation
            yield {"_response_text": response.text}
            return
            
        # For JSON responses, use JsonPath extraction
        try:
            json_data = response.json(parse_float=decimal.Decimal)
            yield from extract_jsonpath(
                self.records_jsonpath,
                input=json_data,
            )
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse JSON response: {e}")
            self.logger.debug(f"Response content: {response.text[:1000]}")
            yield {}

    def post_process(
        self,
        row: dict,
        context: Context | None = None,  # noqa: ARG002
    ) -> dict | None:
        """As needed, append or transform raw data to match expected structure.

        Args:
            row: An individual record from the stream.
            context: The stream context.

        Returns:
            The updated record dictionary, or ``None`` to skip the record.
        """
        return row
