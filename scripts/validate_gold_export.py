"""Gold Export Validation CLI.

Validates exported Gold Parquet datasets against manifest, schema completeness,
referential integrity, and financial reconciliation before publishing.
"""

import argparse
import sys
from pathlib import Path

import pandas as pd

from config.logging_config import setup_logger
from config.settings import settings
from orderflow.quality.base_checks import QualitySuite
from orderflow.quality.financial_checks import (
    check_line_items_arithmetic,
    check_order_total_reconciliation,
)
from orderflow.quality.referential_checks import check_foreign_key
from orderflow.quality.schema_checks import check_not_null, check_uniqueness
from orderflow.storage.manifest import ManifestManager

logger = setup_logger("orderflow.validate_gold")


def validate_gold_export(gold_dir: Path | str | None = None) -> bool:
    """Validates all exported Gold datasets before publishing to Supabase analytics."""
    export_path = Path(gold_dir or settings.GOLD_EXPORT_DIR)
    manifest_file = export_path / "export_manifest.json"

    logger.info(f"=== Starting Gold Export Quality Validation: {export_path} ===")

    if not manifest_file.exists():
        logger.error(f"Export manifest missing at {manifest_file}. Run Lakehouse transformation first.")
        return False

    manifest_mgr = ManifestManager()
    manifest = manifest_mgr.load_manifest(manifest_file)

    # 1. Verify file existence and checksums
    valid_files = manifest_mgr.verify_manifest_files(manifest, root_dir=export_path)
    if not valid_files:
        logger.error("Cryptographic file verification failed for Gold exports.")
        return False

    suite = QualitySuite(run_id=manifest.get("export_id"))

    # Load core dataframes
    try:
        customers_df = pd.read_parquet(export_path / "dim_customer.parquet")
        orders_df = pd.read_parquet(export_path / "fact_orders.parquet")
        items_df = pd.read_parquet(export_path / "fact_order_items.parquet")
        products_df = pd.read_parquet(export_path / "dim_product.parquet")
    except Exception as e:
        logger.error(f"Failed to read exported Parquet files: {e}")
        return False

    # 2. Uniqueness Checks
    suite.add_result(check_uniqueness(customers_df, "customer_id", "dim_customer", suite.run_id))
    suite.add_result(check_uniqueness(orders_df, "order_id", "fact_orders", suite.run_id))
    suite.add_result(check_uniqueness(products_df, "product_id", "dim_product", suite.run_id))

    # 3. Completeness Checks
    suite.add_result(check_not_null(orders_df, ["order_id", "customer_id", "order_total"], "fact_orders", suite.run_id))

    # 4. Financial Reconciliation Checks
    suite.add_result(check_order_total_reconciliation(orders_df, suite.run_id, tolerance=0.02))
    suite.add_result(check_line_items_arithmetic(items_df, suite.run_id, tolerance=0.02))

    # 5. Referential Integrity Checks
    suite.add_result(
        check_foreign_key(
            parent_df=customers_df,
            parent_key="customer_id",
            child_df=orders_df,
            child_foreign_key="customer_id",
            parent_table_name="dim_customer",
            child_table_name="fact_orders",
            run_id=suite.run_id,
        )
    )
    suite.add_result(
        check_foreign_key(
            parent_df=products_df,
            parent_key="product_id",
            child_df=items_df,
            child_foreign_key="product_id",
            parent_table_name="dim_product",
            child_table_name="fact_order_items",
            run_id=suite.run_id,
        )
    )

    if suite.has_critical_failures():
        logger.error("CRITICAL QUALITY GATES FAILED. Publishing is BLOCKED.")
        return False

    logger.info("ALL CRITICAL DATA QUALITY CHECKS PASSED. Dataset is verified for publishing.")
    return True


def main():
    parser = argparse.ArgumentParser(description="Validate Gold Parquet exports.")
    parser.add_argument("--dir", default=None, help="Directory containing gold export Parquet files")
    args = parser.parse_args()

    success = validate_gold_export(args.dir)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
