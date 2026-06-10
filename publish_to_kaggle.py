"""
publish_to_kaggle.py — Cloud Run Job: export BigQuery → Kaggle dataset.

Exports two tables to parquet, then creates or versions the Kaggle dataset:
  - UFC_data.UFC_features        → ufc_features.parquet
  - UFC_data.full_data_silver_plus → full_data_silver_plus.parquet

Auth:
  KAGGLE_USERNAME and KAGGLE_KEY must be set in the environment.
  On GCP: inject via Secret Manager bindings on the Cloud Run Job.
  Locally: export in shell before running.

Usage:
  python publish_to_kaggle.py
"""

import json
import logging
import os
import subprocess
import sys
import tempfile
from datetime import date
from pathlib import Path

import pandas as pd
from google.cloud import bigquery

from config import (
    DATASET,
    KAGGLE_DATASET_SLUG,
    PROJECT_ID,
    TABLE_FEATURES,
    VIEW_FULL_DATA_SILVER_PLUS,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

client = bigquery.Client(project=PROJECT_ID)
DS = f"{PROJECT_ID}.{DATASET}"

# Files written inside the temp export directory
_EXPORTS: list[tuple[str, str]] = [
    (TABLE_FEATURES,          "ufc_features.parquet"),
    (VIEW_FULL_DATA_SILVER_PLUS, "full_data_silver_plus.parquet"),
]


# ── Auth ───────────────────────────────────────────────────────────────────────

def _check_and_configure_auth() -> tuple[str, str]:
    """
    Resolve Kaggle credentials from environment variables.  Two schemes:

    Scheme A — KAGGLE_API_TOKEN (newer, GCP-friendly single secret):
        export KAGGLE_API_TOKEN='{"username":"user","key":"KGAT_..."}'

    Scheme B — split vars (legacy, still supported):
        export KAGGLE_USERNAME=user
        export KAGGLE_KEY=KGAT_...

    Writes ~/.kaggle/kaggle.json for the CLI if not already present.
    Exits 1 with a clear message when credentials are missing or malformed.
    Returns (username, key).
    """
    # ── Scheme A: single JSON token ────────────────────────────────────────
    api_token_raw = os.environ.get("KAGGLE_API_TOKEN", "").strip()
    if api_token_raw:
        try:
            creds = json.loads(api_token_raw)
            username = creds.get("username", "").strip()
            key      = creds.get("key",      "").strip()
            if not username or not key:
                raise ValueError("JSON must have non-empty 'username' and 'key' fields")
        except (json.JSONDecodeError, ValueError) as exc:
            logging.error(
                "KAGGLE_API_TOKEN is set but is not valid JSON.\n"
                "  Error: %s\n"
                "  Expected: KAGGLE_API_TOKEN='{\"username\":\"your_user\",\"key\":\"KGAT_...\"}'\n"
                "  Got a bare key?  Wrap it:\n"
                "      export KAGGLE_API_TOKEN='{\"username\":\"your_user\",\"key\":\"<the key>\"}'\n"
                "  Or use the split form instead:\n"
                "      export KAGGLE_USERNAME=your_user\n"
                "      export KAGGLE_KEY=<the key>",
                exc,
            )
            sys.exit(1)
    # ── Scheme B: split vars ───────────────────────────────────────────────
    else:
        username = os.environ.get("KAGGLE_USERNAME", "").strip()
        key      = os.environ.get("KAGGLE_KEY",      "").strip()
        missing  = [n for n, v in [("KAGGLE_USERNAME", username), ("KAGGLE_KEY", key)] if not v]
        if missing:
            logging.error(
                "Missing Kaggle credentials: %s\n"
                "  Set either a single JSON token:\n"
                "      export KAGGLE_API_TOKEN='{\"username\":\"your_user\",\"key\":\"KGAT_...\"}'\n"
                "  Or both split vars:\n"
                "      export KAGGLE_USERNAME=your_user\n"
                "      export KAGGLE_KEY=KGAT_...\n"
                "  Get a token at https://www.kaggle.com/settings → Account → API\n"
                "  On GCP: bind KAGGLE_API_TOKEN via Secret Manager on the Cloud Run Job.",
                ", ".join(missing),
            )
            sys.exit(1)

    kaggle_dir = Path.home() / ".kaggle"
    kaggle_dir.mkdir(mode=0o700, parents=True, exist_ok=True)

    # CLI v2: ~/.kaggle/access_token contains just the bare key
    access_token_path = kaggle_dir / "access_token"
    access_token_path.write_text(key)
    access_token_path.chmod(0o600)

    # CLI v1 legacy: ~/.kaggle/kaggle.json
    cfg_path = kaggle_dir / "kaggle.json"
    cfg_path.write_text(json.dumps({"username": username, "key": key}))
    cfg_path.chmod(0o600)

    # CLI v2 also reads KAGGLE_API_TOKEN as a bare key (not JSON)
    os.environ["KAGGLE_API_TOKEN"] = key
    os.environ["KAGGLE_USERNAME"]  = username
    os.environ["KAGGLE_KEY"]       = key

    logging.info("Kaggle auth OK  (user: %s)", username)
    return username, key


# ── Export ─────────────────────────────────────────────────────────────────────

def _export_table(table_or_view: str, filename: str, export_dir: Path) -> None:
    """Query a BigQuery table/view and write it as a parquet file."""
    sql = f"SELECT * FROM `{DS}.{table_or_view}`"
    logging.info("Exporting %s ...", table_or_view)
    df = client.query(sql).to_dataframe()

    # Drop any internal/temporary columns
    df = df[[c for c in df.columns if not c.startswith("_")]]

    out_path = export_dir / filename
    df.to_parquet(out_path, index=False)

    size_mb = out_path.stat().st_size / 1_048_576
    logging.info("  → %s: %d rows × %d cols  (%.1f MB)", filename, len(df), len(df.columns), size_mb)


def _write_dataset_metadata(export_dir: Path) -> None:
    """Write the dataset-metadata.json Kaggle requires for create/version."""
    meta = {
        "title": "UFC Betting Odds & Fight Stats — Daily",
        "id": KAGGLE_DATASET_SLUG,
        "licenses": [{"name": "CC0-1.0"}],
    }
    (export_dir / "dataset-metadata.json").write_text(json.dumps(meta, indent=2))
    logging.info("Wrote dataset-metadata.json  (slug: %s)", KAGGLE_DATASET_SLUG)


# ── Kaggle CLI wrapper ─────────────────────────────────────────────────────────

def _kaggle(*args: str) -> tuple[int, str]:
    """Run a kaggle CLI command and return (returncode, combined_output)."""
    cmd = ["kaggle", *args]
    result = subprocess.run(cmd, capture_output=True, text=True)
    return result.returncode, (result.stdout + result.stderr).strip()


def _publish(export_dir: Path) -> None:
    """
    Version an existing dataset, or create it if this is the first push.
    The kaggle CLI call order:
      1. datasets version  — succeeds if dataset already exists on Kaggle
      2. datasets create   — first-time publish fallback
    """
    today        = date.today().isoformat()
    version_note = f"auto update {today}"

    logging.info("Versioning dataset %s ...", KAGGLE_DATASET_SLUG)
    rc, out = _kaggle(
        "datasets", "version",
        "-p", str(export_dir),
        "-m", version_note,
        "--dir-mode", "skip",
    )
    if rc == 0:
        logging.info("Dataset versioned successfully.")
        if out:
            logging.info(out)
        return

    # First-ever push or stale metadata — try create
    logging.info("Version failed (rc=%d); attempting first-time create ...", rc)
    if out:
        logging.info(out)

    rc2, out2 = _kaggle(
        "datasets", "create",
        "-p", str(export_dir),
        "--dir-mode", "skip",
    )
    if rc2 != 0:
        logging.error("kaggle datasets create failed (rc=%d):\n%s", rc2, out2)
        sys.exit(1)

    logging.info("Dataset created successfully.")
    if out2:
        logging.info(out2)


# ── Entry ──────────────────────────────────────────────────────────────────────

def main() -> None:
    logging.info("=" * 60)
    logging.info("publish_to_kaggle  →  %s", KAGGLE_DATASET_SLUG)
    logging.info("=" * 60)

    _check_and_configure_auth()  # exits 1 on missing/malformed creds

    with tempfile.TemporaryDirectory() as tmp:
        export_dir = Path(tmp)

        for table_or_view, filename in _EXPORTS:
            _export_table(table_or_view, filename, export_dir)

        _write_dataset_metadata(export_dir)
        _publish(export_dir)

    logging.info("=" * 60)
    logging.info("publish_to_kaggle complete.")
    sys.exit(0)


if __name__ == "__main__":
    main()
