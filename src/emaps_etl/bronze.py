"""Bronze layer: store raw API responses as received, with minimal ingestion metadata.

One JSON file per API call, partitioned by ingestion date:
    bronze/<stream>/year=YYYY/month=MM/day=DD/<ingested_at>.json
"""

import logging
from datetime import UTC, datetime

from emaps_etl import storage
from emaps_etl.api import get_json
from emaps_etl.config import Settings, get_api_key

log = logging.getLogger(__name__)

# Bronze stream name -> API endpoint. The trial API key only allows the `history` endpoints
# (last 24 hours, hourly); see "Limitations" in the README.
STREAMS = {
    "electricity_mix": "electricity-mix/history",
    "electricity_flows": "electricity-flows/history",
}


def ingest(stream: str, settings: Settings) -> dict:
    """Fetch one stream from the API and store it in Bronze. Returns the stored record."""
    url, response = get_json(
        url=f"{settings.api_base_url}/{STREAMS[stream]}",
        params={
            "zone": settings.zone,
            "temporalGranularity": "hourly",
            "disableCallerLookup": "true",
        },
        api_key=get_api_key(settings),
    )
    ingested_at = datetime.now(UTC)
    record = {
        "ingested_at": ingested_at.isoformat(),
        "source_url": url,
        "response": response,
    }

    file_path = storage.path(
        settings,
        "bronze",
        stream,
        f"year={ingested_at:%Y}",
        f"month={ingested_at:%m}",
        f"day={ingested_at:%d}",
        f"{ingested_at:%Y%m%dT%H%M%S%fZ}.json",
    )
    storage.write_json(file_path, record, settings)
    log.info("Stored %s (%d hours) in %s", stream, len(response.get("history", [])), file_path)
    return record
