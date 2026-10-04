"""Run the whole pipeline once without Dagster: python -m emaps_etl"""

import logging

from emaps_etl import bronze, gold, silver
from emaps_etl.config import get_settings


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    settings = get_settings()

    mix = bronze.ingest("electricity_mix", settings)
    flows = bronze.ingest("electricity_flows", settings)

    silver.update_mix([mix], settings)
    silver.update_flows([flows], settings)

    gold.build_daily_relative_mix(settings)
    gold.build_daily_imports(settings)
    gold.build_daily_exports(settings)


if __name__ == "__main__":
    main()
