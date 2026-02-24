"""
Fetches card_metrics and set_metrics from BigQuery and writes them as
static JSON files for the Hugo site.

Required environment variables:
  GCP_PROJECT_ID  - GCP project that owns the BigQuery dataset
  BQ_DATASET      - BigQuery dataset name (default: tcg_price_oracle)
"""

import json
import os
import sys
from datetime import datetime, timezone
from decimal import Decimal

from google.cloud import bigquery

PROJECT_ID = os.environ["GCP_PROJECT_ID"]
DATASET = os.environ.get("BQ_DATASET", "tcg_price_oracle")

OUTPUT_DIR = "static/data"


# ---------------------------------------------------------------------------
# Queries
# ---------------------------------------------------------------------------

CARD_METRICS_QUERY = f"""
SELECT
  set_id,
  month,
  card_number,
  card_name,
  rarity,
  raw_price,
  psa_10_price,
  psa_9_price,
  tag_10_price,
  ace_10_price,
  cgc_10_price,
  bgs_10_price,
  bgs_10_black_label_price,
  cgc_10_pristine_price
FROM
  `{PROJECT_ID}.{DATASET}.card_metrics`
ORDER BY
  month DESC,
  set_id,
  card_number
"""

SET_METRICS_QUERY = f"""
SELECT
  set_id,
  month,
  ev,
  set_value,
  top_5_value,
  top_5_ratio
FROM
  `{PROJECT_ID}.{DATASET}.set_metrics`
ORDER BY
  month DESC,
  set_id
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _BQEncoder(json.JSONEncoder):
    """Handle types that BigQuery returns that are not JSON-serialisable by default."""

    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, (datetime,)):
            return obj.isoformat()
        if hasattr(obj, "isoformat"):  # date, time
            return obj.isoformat()
        return super().default(obj)


def run_query(client: bigquery.Client, sql: str) -> list[dict]:
    print(f"Running query:\n{sql.strip()}\n")
    rows = client.query(sql).result()
    return [dict(row) for row in rows]


def write_json(payload: dict, filename: str) -> None:
    path = f"{OUTPUT_DIR}/{filename}"
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, cls=_BQEncoder)
    print(f"Written {payload['record_count']} records → {path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    client = bigquery.Client(project=PROJECT_ID)
    now = datetime.now(timezone.utc).isoformat()

    # card_metrics
    card_rows = run_query(client, CARD_METRICS_QUERY)
    write_json(
        {"last_updated": now, "record_count": len(card_rows), "data": card_rows},
        "card_metrics.json",
    )

    # set_metrics
    set_rows = run_query(client, SET_METRICS_QUERY)
    write_json(
        {"last_updated": now, "record_count": len(set_rows), "data": set_rows},
        "set_metrics.json",
    )

    print("Done.")


if __name__ == "__main__":
    try:
        main()
    except KeyError as exc:
        print(f"ERROR: missing required environment variable {exc}", file=sys.stderr)
        sys.exit(1)
