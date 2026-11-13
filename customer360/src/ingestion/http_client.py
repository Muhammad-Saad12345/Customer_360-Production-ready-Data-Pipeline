"""Small HTTP client used by every API connector.

* Retries 429 and 5xx responses and network errors with exponential backoff,
  honouring ``Retry-After`` (capped) on 429.
* After the last attempt it raises ``TransientError`` so Step Functions can
  retry the whole task later; 4xx responses (bad credentials, bad request)
  raise ``SourceRequestError`` immediately and are not retried.
* Never logs headers, auth or query values that could carry secrets.
"""
from __future__ import annotations

import logging
import time
from typing import Any, Callable, Dict, Optional

import requests

from src.errors import SourceRequestError, TransientError

logger = logging.getLogger("customer360.http")

RETRYABLE_STATUS = {429, 500, 502, 503, 504}


class SourceHttpClient:
    def __init__(
        self,
        source: str,
        headers: Optional[Dict[str, str]] = None,
        auth: Any = None,
        timeout: float = 30.0,
        max_attempts: int = 4,
        backoff_seconds: float = 1.0,
        max_retry_after_seconds: float = 30.0,
        session: Optional[requests.Session] = None,
        sleep: Callable[[float], None] = time.sleep,
    ):
        self.source = source
        self.headers = headers or {}
        self.auth = auth
        self.timeout = timeout
        self.max_attempts = max_attempts
        self.backoff_seconds = backoff_seconds
        self.max_retry_after_seconds = max_retry_after_seconds
        self.session = session or requests.Session()
        self._sleep = sleep
        self.request_count = 0

    def _wait(self, attempt: int, response: Optional[requests.Response]) -> float:
        delay = self.backoff_seconds * (2 ** (attempt - 1))
        if response is not None and response.status_code == 429:
            retry_after = response.headers.get("Retry-After")
            try:
                if retry_after is not None:
                    delay = float(retry_after)
            except ValueError:
                pass
        return min(delay, self.max_retry_after_seconds)

    def request_json(self, method: str, url: str, **kwargs: Any) -> Dict[str, Any]:
        last_error: str = ""
        for attempt in range(1, self.max_attempts + 1):
            response: Optional[requests.Response] = None
            try:
                self.request_count += 1
                response = self.session.request(
                    method,
                    url,
                    headers=self.headers,
                    auth=self.auth,
                    timeout=self.timeout,
                    **kwargs,
                )
            except (requests.ConnectionError, requests.Timeout) as exc:
                last_error = f"{type(exc).__name__}"
            else:
                if response.status_code < 400:
                    try:
                        return response.json()
                    except ValueError as exc:
                        raise SourceRequestError(f"{self.source}: non-JSON response from {url.split('?')[0]}") from exc
                if response.status_code not in RETRYABLE_STATUS:
                    raise SourceRequestError(
                        f"{self.source}: HTTP {response.status_code} from {url.split('?')[0]}"
                    )
                last_error = f"HTTP {response.status_code}"

            if attempt < self.max_attempts:
                delay = self._wait(attempt, response)
                logger.warning(
                    "%s request failed (%s), attempt %d/%d, retrying in %.1fs",
                    self.source, last_error, attempt, self.max_attempts, delay,
                )
                self._sleep(delay)

        raise TransientError(f"{self.source}: {last_error} after {self.max_attempts} attempts")

    def get_json(self, url: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return self.request_json("GET", url, params=params)

    def post_json(self, url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self.request_json("POST", url, json=payload)
