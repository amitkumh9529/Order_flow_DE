"""CLI script to publish validated Gold datasets into the Supabase analytics schema."""

import argparse
import sys
from pathlib import Path
from typing import Dict

from config.logging_config import setup_logger
from config.settings import settings
from orderflow.database.connection import db_manager
from orderflow.publishing.analytics_upsert import AnalyticsPublisher
from orderflow.publishing.gold_loader import GoldLoader
from scripts.validate_gold_export import validate_gold_export

logger = setup_logger("orderflow.publish")


def publish_gold(export_dir: Path | str | None = None) -> Dict[str, int]:
    """Validates and publishes Gold models into Supabase analytics tables."""
    gold_path = Path(export_dir or settings.GOLD_EXPORT_DIR)
    logger.info(f"=== Starting Gold Analytics Publishing from {gold_path} ===")

    # Step 1: Pre-publish validation gate
    valid = validate_gold_export(gold_path)
    if not valid:
        logger.error("Pre-publish validation failed. Aborting publishing.")
        raise RuntimeError("Validation gate failed.")

    # Step 2: Load tables from Parquet
    loader = GoldLoader(gold_dir=gold_path)
    tables = loader.load_all_tables()
    logger.info(f"Loaded {len(tables)} tables from {gold_path}")

    # Step 3: Publish to database
    publisher = AnalyticsPublisher(manager=db_manager)
    summary = publisher.publish_bundle(tables)

    logger.info("=== Gold Publishing Completed Successfully ===")
    for tbl, count in summary.items():
        logger.info(f"  - analytics.{tbl}: {count:,} rows")

    return summary


def main():
    parser = argparse.ArgumentParser(description="Publish Gold datasets to analytics schema.")
    parser.add_argument("--dir", default=None, help="Directory containing gold export Parquet files")
    args = parser.parse_args()

    try:
        publish_gold(args.dir)
    except Exception as e:
        logger.error(f"Publishing failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
