"""Prepares and packages landed ingestion files for Databricks Lakehouse processing.

Packages local/S3 Parquet batches into the handoff structure for Databricks Volumes
or direct S3 landing, generating an upload manifest.
"""

import argparse
import json
import shutil
import uuid
from pathlib import Path
from typing import Any, Dict, List

from config.logging_config import setup_logger
from config.settings import settings
from orderflow.utils.dates import utc_now
from orderflow.utils.hashing import compute_file_checksum

logger = setup_logger("orderflow.handoff")


def prepare_databricks_handoff(
    raw_dir: Path | str | None = None,
    output_dir: Path | str | None = None,
    manifest_id: str | None = None,
) -> Dict[str, Any]:
    """Packages landed ingestion files into the Databricks handoff structure."""
    if raw_dir:
        source_path = Path(raw_dir)
    else:
        # Default to artifacts/raw, fallback to demo_artifacts/raw if empty
        default_raw = settings.ARTIFACTS_DIR / "raw"
        demo_raw = settings.BASE_DIR / "demo_artifacts" / "raw"
        if default_raw.exists() and any(default_raw.rglob("*.parquet")):
            source_path = default_raw
        elif demo_raw.exists() and any(demo_raw.rglob("*.parquet")):
            source_path = demo_raw
        else:
            source_path = default_raw

    target_path = Path(output_dir or settings.DATABRICKS_UPLOAD_DIR)
    target_path.mkdir(parents=True, exist_ok=True)

    handoff_run_id = str(uuid.uuid4())
    logger.info(f"Preparing Databricks handoff: run_id={handoff_run_id}, from={source_path} to={target_path}")

    # Discover Parquet files in source directory
    parquet_files = list(source_path.rglob("*.parquet"))
    if not parquet_files:
        logger.warning(f"No Parquet files found in {source_path}. Ensure ingestion has executed.")
        return {"status": "EMPTY", "run_id": handoff_run_id, "files_copied": 0}

    copied_files: List[Dict[str, Any]] = []
    total_bytes = 0

    for p_file in parquet_files:
        # Preserve relative path structure: {table}/{partition}/{run_id}/{file_name}
        rel_path = p_file.relative_to(source_path)
        dest_file = target_path / rel_path
        dest_file.parent.mkdir(parents=True, exist_ok=True)

        shutil.copy2(p_file, dest_file)
        file_size = dest_file.stat().st_size
        sha256 = compute_file_checksum(dest_file)
        total_bytes += file_size

        copied_files.append({
            "source_path": str(p_file),
            "destination_path": str(dest_file),
            "relative_path": str(rel_path).replace("\\", "/"),
            "file_size_bytes": file_size,
            "sha256_checksum": sha256,
        })

    handoff_manifest = {
        "handoff_id": handoff_run_id,
        "created_at": utc_now().isoformat(),
        "target_volume": f"{settings.DATABRICKS_CATALOG}.landing.incoming",
        "total_files": len(copied_files),
        "total_bytes": total_bytes,
        "status": "AWAITING_DATABRICKS_INGESTION",
        "files": copied_files,
    }

    manifest_file = target_path / "handoff_manifest.json"
    with open(manifest_file, "w", encoding="utf-8") as f:
        json.dump(handoff_manifest, f, indent=2)

    logger.info(
        f"Databricks handoff ready: {len(copied_files)} files ({total_bytes:,} bytes). Manifest: {manifest_file.name}"
    )
    return handoff_manifest


def main():
    parser = argparse.ArgumentParser(description="Prepare landed data for Databricks Lakehouse.")
    parser.add_argument("--raw-dir", default=None, help="Directory containing raw landed Parquet files")
    parser.add_argument("--output-dir", default=None, help="Target directory for Databricks handoff")
    args = parser.parse_args()
    prepare_databricks_handoff(raw_dir=args.raw_dir, output_dir=args.output_dir)


if __name__ == "__main__":
    main()
