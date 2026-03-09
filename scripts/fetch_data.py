"""
Fetches card_market_history, set_market_history, tcgplayer_market_snapshots,
and booster_box_ml_features from BigQuery and writes them as static JSON files
for the Hugo site.

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

CARD_MARKET_HISTORY_QUERY = f"""
SELECT
  card_id,
  month,
  grade_id,
  market_price,
  volume
FROM
  `{PROJECT_ID}.{DATASET}.card_market_history`
WHERE
  month IS NOT NULL
ORDER BY
  card_id,
  month DESC
"""

SET_MARKET_HISTORY_QUERY = f"""
SELECT
  game,
  set_id,
  month,
  ev,
  set_value,
  top_5_value,
  top_5_ratio
FROM
  `{PROJECT_ID}.{DATASET}.set_market_history`
WHERE
  month IS NOT NULL
ORDER BY
  game,
  set_id,
  month DESC
"""

TCGPLAYER_MARKET_SNAPSHOTS_QUERY = f"""
SELECT
  snapshot_date,
  tcg,
  set_id,
  product_type,
  tcgplayer_id,
  seller_count,
  product_count,
  median_ask_price,
  avg_sold_30d,
  sales_to_inventory_ratio
FROM
  `{PROJECT_ID}.{DATASET}.tcgplayer_market_snapshots`
WHERE
  snapshot_date IS NOT NULL
ORDER BY
  tcg,
  set_id,
  product_type,
  snapshot_date DESC
"""

BOOSTER_BOX_ML_FEATURES_QUERY = f"""
SELECT
  game,
  set_id,
  snapshot_date,
  release_date,
  era,
  product_type,
  is_special_set,
  is_standard_legal,
  pack_count,
  months_since_release,
  msrp,
  market_price,
  ev,
  ev_to_market_ratio,
  set_value,
  top_5_value,
  top_5_ratio,
  median_ask_price,
  seller_count,
  product_count,
  avg_sold_30d,
  sales_to_inventory_ratio,
  price_change_90d_pct,
  worst_rarity_master_exp_packs,
  second_worst_rarity_master_exp_packs,
  label_90d_price_change_pct,
  label_365d_price_change_pct,
  label_2y_price_change_pct,
  label_5y_price_change_pct
FROM
  `{PROJECT_ID}.{DATASET}.booster_box_ml_features`
WHERE
  snapshot_date IS NOT NULL
ORDER BY
  game,
  era,
  set_id,
  product_type,
  snapshot_date DESC
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
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    client = bigquery.Client(project=PROJECT_ID)
    now = datetime.now(timezone.utc).isoformat()

    # card_market_history — split into per-set files: card_market_history/{game}/{set_id}.json
    # card_id format: "{game}_{set_id}_{card_number}"
    card_rows = run_query(client, CARD_MARKET_HISTORY_QUERY)
    card_groups: dict[tuple[str, str], list[dict]] = {}
    for row in card_rows:
        parts = row["card_id"].split("_", 2)
        game, set_id = parts[0], parts[1]
        card_groups.setdefault((game, set_id), []).append(row)

    for (game, set_id), rows in sorted(card_groups.items()):
        out_dir = f"{OUTPUT_DIR}/card_market_history/{game}"
        os.makedirs(out_dir, exist_ok=True)
        write_json(
            {"last_updated": now, "record_count": len(rows), "data": rows},
            f"card_market_history/{game}/{set_id}.json",
        )

    # set_market_history
    set_rows = run_query(client, SET_MARKET_HISTORY_QUERY)
    write_json(
        {"last_updated": now, "record_count": len(set_rows), "data": set_rows},
        "set_market_history.json",
    )

    # tcgplayer_market_snapshots
    tcgplayer_rows = run_query(client, TCGPLAYER_MARKET_SNAPSHOTS_QUERY)
    write_json(
        {"last_updated": now, "record_count": len(tcgplayer_rows), "data": tcgplayer_rows},
        "tcgplayer_market_snapshots.json",
    )

    # booster_box_ml_features
    ml_rows = run_query(client, BOOSTER_BOX_ML_FEATURES_QUERY)
    write_json(
        {"last_updated": now, "record_count": len(ml_rows), "data": ml_rows},
        "booster_box_ml_features.json",
    )

    print("Done.")


if __name__ == "__main__":
    try:
        main()
    except KeyError as exc:
        print(f"ERROR: missing required environment variable {exc}", file=sys.stderr)
        sys.exit(1)
