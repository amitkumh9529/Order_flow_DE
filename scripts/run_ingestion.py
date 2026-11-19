"""OrderFlow Ingestion Runner.

Extracts data incrementally or full snapshot from operational tables, enriches records
with audit metadata, writes partitioned Parquet, generates manifests, and updates watermarks.
"""

import argparse
import uuid
from pathlib import Path

import yaml
from sqlalchemy import text

from config.logging_config import setup_logger
from orderflow.database.connection import db_manager
from orderflow.database.queries import INSERT_PIPELINE_RUN_QUERY, UPDATE_PIPELINE_RUN_QUERY
from orderflow.ingestion.full_extractor import FullExtractor
from orderflow.ingestion.incremental_extractor import IncrementalExtractor
from orderflow.ingestion.watermark_manager import WatermarkManager
from orderflow.storage.manifest import ManifestManager
from orderflow.storage.parquet_writer import ParquetWriter
from orderflow.utils.dates import utc_now

logger = setup_logger("orderflow.ingestion")


def load_pipeline_config() -> dict:
    config_path = Path("config/pipeline_config.yaml")
    if config_path.exists():
        with open(config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)
    return {}


def parse_args():
    parser = argparse.ArgumentParser(description="Run OrderFlow Ingestion Pipeline.")
    parser.add_argument(
        "--mode",
        choices=["incremental", "full"],
        default="incremental",
        help="Extraction mode: incremental (default) or full snapshot",
    )
    parser.add_argument(
        "--table",
        type=str,
        default=None,
        help="Specific table to extract (default: all configured tables)",
    )
    return parser.parse_args()


def run_ingestion(mode: str = "incremental", table: str | None = None) -> str:
    """Executes the ingestion lifecycle across configured entities."""
    run_id = str(uuid.uuid4())
    pipeline_name = "orderflow_ingestion"
    started_at = utc_now()

    logger.info(f"=== Starting Ingestion Run: run_id={run_id}, mode={mode}, table={table or 'ALL'} ===")

    # Initialize ops.pipeline_runs
    is_sqlite = db_manager.db_url.startswith("sqlite")
    try:
        with db_manager.connect() as conn:
            if is_sqlite:
                conn.execute(
                    text("""
                    CREATE TABLE IF NOT EXISTS pipeline_runs (
                        run_id TEXT PRIMARY KEY,
                        pipeline_name TEXT,
                        source_table TEXT,
                        run_status TEXT,
                        started_at TEXT,
                        completed_at TEXT,
                        rows_extracted INTEGER,
                        rows_rejected INTEGER,
                        files_created INTEGER,
                        error_message TEXT
                    );
                    """)
                )
                conn.execute(
                    text("""
                    CREATE TABLE IF NOT EXISTS pipeline_watermarks (
                        pipeline_name TEXT,
                        source_table TEXT,
                        last_successful_watermark TEXT,
                        last_successful_run_id TEXT,
                        updated_at TEXT,
                        PRIMARY KEY (pipeline_name, source_table)
                    );
                    """)
                )
                conn.execute(
                    text("""
                    INSERT INTO pipeline_runs (run_id, pipeline_name, source_table, run_status, started_at)
                    VALUES (:run_id, :pipeline_name, :source_table, :run_status, :started_at);
                    """),
                    {
                        "run_id": run_id,
                        "pipeline_name": pipeline_name,
                        "source_table": table or "ALL",
                        "run_status": "started",
                        "started_at": started_at.isoformat(),
                    },
                )
            else:
                conn.execute(
                    text(INSERT_PIPELINE_RUN_QUERY),
                    {
                        "run_id": run_id,
                        "pipeline_name": pipeline_name,
                        "source_table": table or "ALL",
                        "run_status": "started",
                        "started_at": started_at,
                    },
                )
            conn.commit()
    except Exception as e:
        logger.warning(f"Could not initialize pipeline_runs record: {e}")

    config = load_pipeline_config()
    sources = config.get("sources", {})
    tables_to_run = [table] if table else list(sources.keys())

    watermark_mgr = WatermarkManager(manager=db_manager)
    writer = ParquetWriter()
    manifest_mgr = ManifestManager()

    all_batches = []
    watermark_updates = []
    total_extracted_rows = 0

    try:
        for t_name in tables_to_run:
            if t_name not in sources:
                logger.warning(f"Table '{t_name}' not found in pipeline_config.yaml. Skipping.")
                continue

            t_cfg = sources[t_name]
            schema = t_cfg.get("schema", "app")
            pk = t_cfg.get("primary_key", f"{t_name[:-1]}_id")
            wm_col = t_cfg.get("watermark_column", "updated_at")
            strategy = t_cfg.get("strategy", "incremental")

            use_incremental = (mode == "incremental") and (strategy != "full")

            if use_incremental:
                extractor = IncrementalExtractor(
                    schema=schema,
                    table=t_name,
                    primary_key=pk,
                    watermark_column=wm_col,
                    watermark_mgr=watermark_mgr,
                    writer=writer,
                    pipeline_name=pipeline_name,
                )
                batches, candidate_wm = extractor.extract(run_id=run_id)
                all_batches.extend(batches)
                watermark_updates.append((t_name, candidate_wm))
                rows = sum(b.get("row_count", 0) for b in batches)
                total_extracted_rows += rows
            else:
                extractor = FullExtractor(
                    schema=schema,
                    table=t_name,
                    primary_key=pk,
                    writer=writer,
                )
                batches = extractor.extract(run_id=run_id)
                all_batches.extend(batches)
                rows = sum(b.get("row_count", 0) for b in batches)
                total_extracted_rows += rows

        completed_at = utc_now()

        # Step 5: Create and save ingestion manifest
        manifest = manifest_mgr.create_ingestion_manifest(
            run_id=run_id,
            pipeline_name=pipeline_name,
            batches=all_batches,
            started_at=started_at,
            completed_at=completed_at,
            extra_metadata={"mode": mode, "tables": tables_to_run},
        )
        manifest_file = manifest_mgr.save_manifest(manifest, file_prefix="ingestion")
        logger.info(f"Generated manifest: {manifest_file.name}")

        # Step 6: Verify manifest integrity before updating watermarks
        is_valid = manifest_mgr.verify_manifest_files(manifest)
        if not is_valid and all_batches:
            raise RuntimeError("Ingestion manifest validation failed. Aborting watermark advancement.")

        # Step 7: Advance watermarks atomically for incrementally extracted tables
        for t_name, wm in watermark_updates:
            watermark_mgr.update_watermark(
                pipeline_name=pipeline_name,
                source_table=t_name,
                watermark=wm,
                run_id=run_id,
            )

        # Update run status to completed
        try:
            with db_manager.connect() as conn:
                if is_sqlite:
                    conn.execute(
                        text("""
                        UPDATE pipeline_runs
                        SET run_status = 'completed', completed_at = :completed_at,
                            rows_extracted = :rows_extracted, files_created = :files_created
                        WHERE run_id = :run_id;
                        """),
                        {
                            "run_id": run_id,
                            "completed_at": completed_at.isoformat(),
                            "rows_extracted": total_extracted_rows,
                            "files_created": len(all_batches),
                        },
                    )
                else:
                    conn.execute(
                        text(UPDATE_PIPELINE_RUN_QUERY),
                        {
                            "run_id": run_id,
                            "run_status": "completed",
                            "completed_at": completed_at,
                            "rows_extracted": total_extracted_rows,
                            "rows_rejected": 0,
                            "files_created": len(all_batches),
                            "error_message": None,
                        },
                    )
                conn.commit()
        except Exception as e:
            logger.warning(f"Could not update pipeline_runs status: {e}")

        logger.info(
            f"=== Ingestion Run Completed: run_id={run_id}, rows={total_extracted_rows:,}, files={len(all_batches)} ==="
        )
        return run_id

    except Exception as e:
        logger.error(f"Ingestion run failed: {e}", exc_info=True)
        # Record failure in pipeline_runs
        try:
            with db_manager.connect() as conn:
                if is_sqlite:
                    conn.execute(
                        text("""
                        UPDATE pipeline_runs
                        SET run_status = 'failed', completed_at = :completed_at, error_message = :err
                        WHERE run_id = :run_id;
                        """),
                        {
                            "run_id": run_id,
                            "completed_at": utc_now().isoformat(),
                            "err": str(e),
                        },
                    )
                else:
                    conn.execute(
                        text(UPDATE_PIPELINE_RUN_QUERY),
                        {
                            "run_id": run_id,
                            "run_status": "failed",
                            "completed_at": utc_now(),
                            "rows_extracted": total_extracted_rows,
                            "rows_rejected": 0,
                            "files_created": len(all_batches),
                            "error_message": str(e),
                        },
                    )
                conn.commit()
        except Exception:
            pass
        raise


def main():
    args = parse_args()
    run_ingestion(mode=args.mode, table=args.table)


if __name__ == "__main__":
    main()
