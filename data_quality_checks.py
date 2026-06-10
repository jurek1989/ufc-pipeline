"""
data_quality_checks.py — bramki jakości danych dla pipeline UFC.

Checks:
  1 (CRITICAL): Każda walka w UFC_fights_stats_data ma dokładnie 2 wiersze (jeden per zawodnik).
  2 (INFO):     % walk bez implied_prob od 2020 w full_data_silver_plus.
  3 (INFO):     % NULL ranking dla zawodników obecnych w UFC_rankings.
  4 (CRITICAL): Liczba wierszy full_data_silver_plus == UFC_fights_data.
  5 (CRITICAL): Orphan fights — walki bez wpisu w UFC_fights_urls.

Exit codes:
  0 — wszystkie krytyczne OK (info checks mogą mieć ostrzeżenia)
  1 — co najmniej jedna krytyczna asercja nie przeszła
"""

import sys
import logging

from google.cloud import bigquery

from config import PROJECT_ID, DATASET

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

client = bigquery.Client(project=PROJECT_ID)

DS = f"`{PROJECT_ID}.{DATASET}`"


def run_query(sql: str):
    return client.query(sql).result()


# ── Check 1 (CRITICAL): każda walka ma dokładnie 2 wiersze stats ─────────────

def check_stats_rows() -> bool:
    """Returns True if check passes (no violations)."""
    sql = f"""
    SELECT COUNT(*) AS bad_fights
    FROM (
      SELECT fight_url, COUNT(*) AS row_count
      FROM {DS}.UFC_fights_stats_data
      GROUP BY fight_url
      HAVING row_count != 2
    )
    """
    rows = list(run_query(sql))
    bad = rows[0].bad_fights

    if bad == 0:
        logging.info("[CHECK 1] PASS — stats: all fights have exactly 2 rows")
        return True
    else:
        logging.error(f"[CHECK 1] FAIL — stats: {bad} fight(s) do NOT have exactly 2 rows")
        return False


# ── Check 2 (INFO): % walk bez implied_prob od 2020 ─────────────────────────

def check_odds_coverage() -> bool:
    """Returns True always (info only), logs warning if coverage < 80%."""
    sql = f"""
    SELECT
      COUNT(*) AS total,
      COUNTIF(f_1_implied_prob IS NULL AND f_2_implied_prob IS NULL) AS missing_odds
    FROM {DS}.full_data_silver_plus
    WHERE event_date >= '2020-01-01'
    """
    rows = list(run_query(sql))
    row = rows[0]
    total = row.total
    missing = row.missing_odds
    pct_missing = round(100.0 * missing / total, 1) if total > 0 else 0.0

    if pct_missing > 20.0:
        logging.warning(
            f"[CHECK 2] WARN — odds coverage: {pct_missing}% of fights since 2020 "
            f"have no implied_prob ({missing}/{total})"
        )
    else:
        logging.info(
            f"[CHECK 2] OK   — odds coverage: {pct_missing}% missing since 2020 "
            f"({missing}/{total})"
        )
    return True


# ── Check 3 (INFO): % NULL ranking dla znanych zawodników ───────────────────

def check_rankings_coverage() -> bool:
    """Returns True always (info only), logs warning if >30% ranked fighters are NULL."""
    sql = f"""
    WITH ranked_fighters AS (
      SELECT DISTINCT fighter
      FROM {DS}.UFC_rankings
    ),
    in_fights AS (
      SELECT DISTINCT f_1 AS fighter FROM {DS}.UFC_fights_data
      UNION DISTINCT
      SELECT DISTINCT f_2 FROM {DS}.UFC_fights_data
    )
    SELECT
      COUNT(*) AS total_ranked,
      COUNTIF(inf.fighter IS NULL) AS not_in_fights
    FROM ranked_fighters rf
    LEFT JOIN in_fights inf USING (fighter)
    """
    rows = list(run_query(sql))
    row = rows[0]
    total = row.total_ranked
    not_found = row.not_in_fights
    pct = round(100.0 * not_found / total, 1) if total > 0 else 0.0

    if pct > 30.0:
        logging.warning(
            f"[CHECK 3] WARN — rankings: {pct}% of ranked fighters not found in fights "
            f"({not_found}/{total})"
        )
    else:
        logging.info(
            f"[CHECK 3] OK   — rankings: {pct}% of ranked fighters not in fights "
            f"({not_found}/{total})"
        )
    return True


# ── Check 4 (CRITICAL): full_data_silver_plus row count == UFC_fights_data ──

def check_view_row_count() -> bool:
    """Returns True if counts match."""
    sql = f"""
    SELECT
      (SELECT COUNT(*) FROM {DS}.UFC_fights_data)       AS fights_count,
      (SELECT COUNT(*) FROM {DS}.full_data_silver_plus) AS view_count
    """
    rows = list(run_query(sql))
    row = rows[0]
    fights = row.fights_count
    view = row.view_count

    if fights == view:
        logging.info(f"[CHECK 4] PASS — row counts match: {fights} rows")
        return True
    else:
        logging.error(
            f"[CHECK 4] FAIL — row count mismatch: "
            f"UFC_fights_data={fights}, full_data_silver_plus={view} "
            f"(diff={abs(fights - view)})"
        )
        return False


# ── Check 5 (CRITICAL): orphan fights bez wpisu w UFC_fights_urls ───────────

def check_orphan_fights() -> bool:
    """Returns True if no orphans found."""
    sql = f"""
    SELECT COUNT(*) AS orphan_count
    FROM {DS}.UFC_fights_data fd
    LEFT JOIN {DS}.UFC_fights_urls fu
      ON fd.fight_url = fu.fight_url
    WHERE fu.fight_url IS NULL
    """
    rows = list(run_query(sql))
    orphans = rows[0].orphan_count

    if orphans == 0:
        logging.info("[CHECK 5] PASS — no orphan fights (all fights have a UFC_fights_urls entry)")
        return True
    else:
        logging.error(
            f"[CHECK 5] FAIL — {orphans} fight(s) in UFC_fights_data "
            f"have no corresponding entry in UFC_fights_urls"
        )
        return False


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    logging.info("=" * 60)
    logging.info("UFC Pipeline — Data Quality Checks")
    logging.info("=" * 60)

    critical_failures = []

    # Critical checks
    if not check_stats_rows():
        critical_failures.append(1)

    # Info checks (always pass at script level)
    check_odds_coverage()
    check_rankings_coverage()

    # Critical checks
    if not check_view_row_count():
        critical_failures.append(4)

    if not check_orphan_fights():
        critical_failures.append(5)

    logging.info("=" * 60)
    if critical_failures:
        logging.error(f"RESULT: FAILED — critical checks failed: {critical_failures}")
        sys.exit(1)
    else:
        logging.info("RESULT: PASSED — all critical checks OK")
        sys.exit(0)


if __name__ == "__main__":
    main()
