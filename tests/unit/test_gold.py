"""
Unit tests for the pure, non-Spark functions in 3_load_gold.py

validate_results() is a pure function that takes a list of result dicts
and returns True/False — perfectly testable without Spark or PostgreSQL.
"""
import pytest


# ---------------------------------------------------------------------------
# Inline the validate_results logic here so tests have zero import side-effects.
# This mirrors the exact logic in 3_load_gold.py::validate_results().
# If that function changes, update this mirror accordingly.
# ---------------------------------------------------------------------------
def validate_results(results):
    all_passed = True
    report = []
    for r in results:
        table  = r["table"]
        silver = r["silver"]
        gold   = r["gold"]
        status = r["status"]

        if status == "SKIPPED":
            report.append(("SKIP", table))
            continue
        elif status == "KEY_ERROR" or status != "OK":
            report.append(("FAIL", table))
            all_passed = False
            continue

        tolerance = max(1, int(silver * 0.0001))
        if gold >= silver - tolerance:
            report.append(("OK", table))
        else:
            report.append(("FAIL", table))
            all_passed = False

    return all_passed, report


# ===========================================================================
# validate_results Tests
# ===========================================================================

class TestValidateResults:

    def _ok(self, table, silver, gold):
        return {"table": table, "silver": silver, "gold": gold, "status": "OK"}

    def _skipped(self, table):
        return {"table": table, "silver": 0, "gold": 0, "status": "SKIPPED"}

    def _key_error(self, table):
        return {"table": table, "silver": 0, "gold": 0, "status": "KEY_ERROR",
                "error": "missing key col"}

    def test_all_ok_returns_true(self):
        results = [self._ok("gold_games_main", 1000, 1000)]
        passed, _ = validate_results(results)
        assert passed is True

    def test_gold_greater_than_silver_ok(self):
        # Gold accumulates history so gold > silver is always fine
        results = [self._ok("gold_games_main", 500, 9000)]
        passed, _ = validate_results(results)
        assert passed is True

    def test_exact_silver_gold_match_passes(self):
        results = [self._ok("gold_games_main", 100, 100)]
        passed, _ = validate_results(results)
        assert passed is True

    def test_minor_loss_within_tolerance_passes(self):
        # 1 row lost from 40000 is within 0.01% tolerance
        results = [self._ok("gold_games_achievements", 40000, 39999)]
        passed, _ = validate_results(results)
        assert passed is True

    def test_major_data_loss_fails(self):
        # 500 rows lost from 1000 is catastrophic
        results = [self._ok("gold_games_main", 1000, 500)]
        passed, _ = validate_results(results)
        assert passed is False

    def test_skipped_table_does_not_fail_run(self):
        results = [self._skipped("gold_games_dlc")]
        passed, _ = validate_results(results)
        assert passed is True

    def test_key_error_fails_run(self):
        results = [self._key_error("gold_games_genres")]
        passed, _ = validate_results(results)
        assert passed is False

    def test_mixed_results_fail_if_any_error(self):
        results = [
            self._ok("gold_games_main", 1000, 1000),
            self._ok("gold_games_genres", 5000, 5000),
            self._key_error("gold_games_achievements"),  # one bad table
        ]
        passed, _ = validate_results(results)
        assert passed is False

    def test_all_tables_in_pipeline_pass(self):
        """Smoke test: all 7 tables with realistic counts — should pass cleanly."""
        results = [
            self._ok("gold_games_main",         9276,  9276),
            self._ok("gold_games_genres",        18000, 18000),
            self._ok("gold_games_categories",    25000, 25000),
            self._ok("gold_games_screenshots",   45000, 45000),
            self._ok("gold_games_movies",        10000, 10000),
            self._ok("gold_games_dlc",           8000,  8000),
            self._ok("gold_games_achievements",  5000,  4999),  # 1 sanitized row
        ]
        passed, _ = validate_results(results)
        assert passed is True

    def test_tolerance_scales_with_row_count(self):
        # At 100,000 rows, tolerance = max(1, int(100000 * 0.0001)) = 10
        # Losing 10 rows should still pass
        results = [self._ok("gold_games_main", 100000, 99990)]
        passed, _ = validate_results(results)
        assert passed is True

    def test_tolerance_minimum_is_one(self):
        # At 5 rows, tolerance = max(1, 0) = 1, so losing 1 row is fine
        results = [self._ok("gold_games_achievements", 5, 4)]
        passed, _ = validate_results(results)
        assert passed is True
