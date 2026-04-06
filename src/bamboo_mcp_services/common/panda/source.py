"""PanDA data source utilities for fetching and validating snapshots."""
from __future__ import annotations
from dataclasses import dataclass
from typing import Any
import hashlib
import json
import requests
from pathlib import Path


@dataclass
class RawSnapshot:
    """Container for a raw data snapshot from a PanDA source.

    Attributes:
        source: Origin identifier (file path or URL).
        raw: Raw data payload (typically a parsed JSON object).
        fetched_utc: UTC timestamp of fetch (ISO 8601 format).
        content_hash: SHA-256 hash of the raw content for deduplication.
    """
    source: str
    raw: Any
    fetched_utc: str
    content_hash: str


class BaseSource:
    """Base class for fetching PanDA data from files or URLs."""

    def fetch_from_file(self, path: str) -> RawSnapshot:
        """Fetch and parse a JSON snapshot from a local file.

        Args:
            path: File system path to the JSON file.

        Returns:
            RawSnapshot containing the parsed data and metadata.

        Raises:
            FileNotFoundError: If the file does not exist.
            json.JSONDecodeError: If the file is not valid JSON.
        """
        p = Path(path)
        text = p.read_text()
        h = hashlib.sha256(text.encode('utf-8')).hexdigest()
        return RawSnapshot(source=str(path), raw=json.loads(text), fetched_utc=str(None), content_hash=h)

    def fetch_from_url(self, url: str) -> RawSnapshot:
        """Fetch and parse a JSON snapshot from a remote URL.

        Args:
            url: HTTP/HTTPS URL to fetch from.

        Returns:
            RawSnapshot containing the parsed data and metadata.

        Raises:
            requests.RequestException: If the HTTP request fails.
            json.JSONDecodeError: If the response is not valid JSON.
        """
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        text = r.text
        h = hashlib.sha256(text.encode('utf-8')).hexdigest()
        return RawSnapshot(source=url, raw=r.json(), fetched_utc=str(None), content_hash=h)
