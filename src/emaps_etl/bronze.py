"""Bronze layer: store raw API responses as received, with minimal ingestion metadata.

One JSON file per API call, partitioned by ingestion date:
    bronze/<stream>/year=YYYY/month=MM/day=DD/<ingested_at>.json
"""

import logging
from datetime import UTC, datetime, timedelta

from emaps_etl import reader, writer
from emaps_etl.api import get_json
from emaps_etl.config import Settings, get_api_key

log = logging.getLogger(__name__)

# Bronze file names start with the ingestion timestamp in this format (22 characters).
FILE_TIMESTAMP = "%Y%m%dT%H%M%S%fZ"

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

    file_path = writer.path(
        settings,
        "bronze",
        stream,
        f"year={ingested_at:%Y}",
        f"month={ingested_at:%m}",
        f"day={ingested_at:%d}",
        f"{ingested_at.strftime(FILE_TIMESTAMP)}.json",
    )
    writer.write_json(file_path, record, settings)
    log.info("Stored %s (%d hours) in %s", stream, len(response.get("history", [])), file_path)
    return record


def records_since(stream: str, after: datetime | None, settings: Settings) -> list[dict]:
    """Bronze records of a stream ingested after `after`; all records when `after` is None.

    Bronze is partitioned by ingestion date, so only the day folders from `after` until today
    are listed, and file names start with the ingestion timestamp, so only new files are read.
    """
    root = writer.path(settings, "bronze", stream)
    if after is None:
        folders = [root]
    else:
        days = (datetime.now(UTC).date() - after.date()).days
        dates = [after.date() + timedelta(days=n) for n in range(days + 1)]
        folders = [f"{root}/year={d:%Y}/month={d:%m}/day={d:%d}" for d in dates]

    uris = [uri for folder in folders for uri in reader.list_files(folder, settings.aws_region)]
    if after is not None:
        uris = [uri for uri in uris if ingested_at_of(uri) > after]
    return [reader.read_json(uri, settings.aws_region) for uri in uris]


def ingested_at_of(uri: str) -> datetime:
    name = uri.rsplit("/", 1)[-1]
    return datetime.strptime(name[:22], FILE_TIMESTAMP).replace(tzinfo=UTC)
