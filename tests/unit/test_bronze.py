"""
Unit tests for 1_extract_bronze.py

All tests run fully offline — no Steam API calls, no file system side effects
(tmp directories are used and cleaned up automatically by pytest's tmp_path fixture).
"""
import json
import pytest
from datetime import datetime, timedelta
from pathlib import Path
import sys
import os

# ---------------------------------------------------------------------------
# Import the functions under test directly from the scripts directory.
# We add scripts/ to sys.path and use importlib to avoid the top-level
# API_KEY guard from crashing on import.
# ---------------------------------------------------------------------------
import importlib
import unittest.mock as mock

# Patch the environment variable BEFORE importing the module
@pytest.fixture(scope="module", autouse=True)
def patch_env():
    with mock.patch.dict(os.environ, {"STEAM_API_KEY": "test_key_000"}):
        yield

# Lazy import inside each test to avoid module-level side effects
def get_bronze_funcs():
    with mock.patch.dict(os.environ, {"STEAM_API_KEY": "test_key_000"}):
        spec = importlib.util.spec_from_file_location(
            "extract_bronze",
            Path(__file__).parents[2] / "scripts" / "1_extract_bronze.py"
        )
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
    return mod


# ===========================================================================
# Registry Tests
# ===========================================================================

class TestRegistry:
    def test_load_registry_missing_file_returns_empty(self, tmp_path, monkeypatch):
        mod = get_bronze_funcs()
        monkeypatch.setattr(mod, "REGISTRY_PATH", str(tmp_path / "missing.json"))
        assert mod.load_registry() == {}

    def test_save_and_load_registry_roundtrip(self, tmp_path, monkeypatch):
        mod = get_bronze_funcs()
        path = str(tmp_path / "registry.json")
        monkeypatch.setattr(mod, "REGISTRY_PATH", path)

        data = {"12345": {"name": "Test Game", "rank": 1, "peak_players": 500,
                           "last_updated": datetime.utcnow().isoformat()}}
        mod.save_registry(data)
        loaded = mod.load_registry()
        assert loaded == data

    def test_save_registry_creates_parent_dir(self, tmp_path, monkeypatch):
        mod = get_bronze_funcs()
        nested = str(tmp_path / "deep" / "dir" / "registry.json")
        monkeypatch.setattr(mod, "REGISTRY_PATH", nested)
        mod.save_registry({"1": {"name": "x"}})
        assert Path(nested).exists()


# ===========================================================================
# save_bronze Tests
# ===========================================================================

class TestSaveBronze:
    def test_output_is_valid_json_array(self, tmp_path):
        mod = get_bronze_funcs()
        path = str(tmp_path / "out.json")
        records = [{"appid": 1, "name": "Game A"}, {"appid": 2, "name": "Game B"}]
        mod.save_bronze(records, path)
        with open(path, encoding="utf-8") as f:
            loaded = json.load(f)
        assert loaded == records

    def test_empty_list_produces_valid_empty_array(self, tmp_path):
        mod = get_bronze_funcs()
        path = str(tmp_path / "empty.json")
        mod.save_bronze([], path)
        with open(path, encoding="utf-8") as f:
            loaded = json.load(f)
        assert loaded == []

    def test_unicode_characters_preserved(self, tmp_path):
        mod = get_bronze_funcs()
        path = str(tmp_path / "unicode.json")
        records = [{"appid": 1, "name": "ゲーム: Café & Straße"}]
        mod.save_bronze(records, path)
        with open(path, encoding="utf-8") as f:
            loaded = json.load(f)
        assert loaded[0]["name"] == "ゲーム: Café & Straße"

    def test_large_batch_produces_correct_count(self, tmp_path):
        mod = get_bronze_funcs()
        path = str(tmp_path / "large.json")
        records = [{"appid": i, "name": f"Game {i}"} for i in range(500)]
        mod.save_bronze(records, path)
        with open(path, encoding="utf-8") as f:
            loaded = json.load(f)
        assert len(loaded) == 500


# ===========================================================================
# build_priority_queue Tests
# ===========================================================================

class TestBuildPriorityQueue:
    """Tests for the pure priority-queue logic — no I/O involved."""

    def _make_registry(self, appids, hours_old=None):
        """Helper: create a registry with given appids, optionally aged."""
        if hours_old is None:
            last_updated = datetime.utcnow().isoformat()
        else:
            last_updated = (datetime.utcnow() - timedelta(hours=hours_old)).isoformat()
        return {
            str(a): {"name": f"Game {a}", "rank": 0, "peak_players": 0,
                     "last_updated": last_updated}
            for a in appids
        }

    def test_new_chart_game_gets_priority_zero(self):
        mod = get_bronze_funcs()
        charts = [{"appid": 100, "peak_in_game": 1000}]
        queue = mod.build_priority_queue(charts, {}, [])
        assert len(queue) == 1
        assert queue[0]["priority"] == 0
        assert queue[0]["appid"] == 100

    def test_fresh_chart_game_is_skipped(self):
        mod = get_bronze_funcs()
        registry = self._make_registry([100], hours_old=1)  # 1h old, STALE_HOURS=24
        charts = [{"appid": 100, "peak_in_game": 1000}]
        queue = mod.build_priority_queue(charts, registry, [])
        assert queue == []

    def test_stale_chart_game_gets_priority_one(self):
        mod = get_bronze_funcs()
        registry = self._make_registry([100], hours_old=48)  # 48h old > STALE_HOURS
        charts = [{"appid": 100, "peak_in_game": 1000}]
        queue = mod.build_priority_queue(charts, registry, [])
        assert len(queue) == 1
        assert queue[0]["priority"] == 1

    def test_discovery_game_gets_priority_two(self):
        mod = get_bronze_funcs()
        queue = mod.build_priority_queue([], {}, [999])
        assert len(queue) == 1
        assert queue[0]["priority"] == 2
        assert queue[0]["appid"] == 999

    def test_discovery_id_already_in_charts_is_not_duplicated(self):
        mod = get_bronze_funcs()
        charts = [{"appid": 100, "peak_in_game": 500}]
        # 100 is in both charts AND discovery — should only appear once (as charts)
        queue = mod.build_priority_queue(charts, {}, [100, 200])
        appids = [q["appid"] for q in queue]
        assert appids.count(100) == 1
        assert 200 in appids

    def test_queue_is_sorted_priority_ascending(self):
        mod = get_bronze_funcs()
        stale_registry = self._make_registry([50], hours_old=48)
        charts = [{"appid": 1, "peak_in_game": 0}, {"appid": 50, "peak_in_game": 0}]
        queue = mod.build_priority_queue(charts, stale_registry, [999])
        priorities = [q["priority"] for q in queue]
        assert priorities == sorted(priorities)
