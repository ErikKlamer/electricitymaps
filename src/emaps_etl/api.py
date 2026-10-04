"""Electricity Maps API client with retries for rate limits and server errors."""

import logging

import requests
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

log = logging.getLogger(__name__)

RETRY_STATUS_CODES = {429, 500, 502, 503, 504}


def _is_retryable(error: BaseException) -> bool:
    if isinstance(error, requests.HTTPError):
        return error.response is not None and error.response.status_code in RETRY_STATUS_CODES
    return isinstance(error, requests.ConnectionError | requests.Timeout)


@retry(
    retry=retry_if_exception(_is_retryable),
    wait=wait_exponential(multiplier=2, max=60),
    stop=stop_after_attempt(5),
    before_sleep=before_sleep_log(log, logging.WARNING),
    reraise=True,
)
def get_json(url: str, params: dict, api_key: str) -> tuple[str, dict]:
    """GET a JSON endpoint. Returns the full request URL (without the key) and the JSON body."""
    response = requests.get(url, params=params, headers={"auth-token": api_key}, timeout=30)
    response.raise_for_status()
    return response.url, response.json()
