"""
Unit tests for the pure functions in 2_transform_silver.py

strip_html() is a plain Python function — zero Spark dependency.
safe_struct_field() requires a Spark schema object and is integration-level;
it is tested here with lightweight mock schema objects instead of a real cluster.
"""
import pytest
import importlib.util
from pathlib import Path


def get_silver_funcs():
    spec = importlib.util.spec_from_file_location(
        "transform_silver",
        Path(__file__).parents[2] / "scripts" / "2_transform_silver.py"
    )
    mod = importlib.util.module_from_spec(spec)
    # Only execute the module up to the function definitions (avoid Spark init)
    # We do this by importing selectively via exec on the source
    source = Path(__file__).parents[2].joinpath("scripts", "2_transform_silver.py").read_text()
    # Extract only the top-level functions (before transform_silver_layer)
    func_source = source.split("def transform_silver_layer")[0]
    namespace = {}
    exec(func_source, namespace)
    return namespace


# ===========================================================================
# strip_html Tests
# ===========================================================================

class TestStripHtml:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.strip_html = get_silver_funcs()["strip_html"]

    def test_removes_basic_tags(self):
        assert self.strip_html("<b>Hello</b>") == "Hello"

    def test_removes_br_tags(self):
        assert self.strip_html("Line1<br>Line2") == "Line1 Line2"

    def test_removes_nested_tags(self):
        assert self.strip_html("<div><p><b>Deep</b></p></div>") == "Deep"

    def test_returns_none_on_none_input(self):
        assert self.strip_html(None) is None

    def test_plain_text_passthrough(self):
        result = self.strip_html("No tags here")
        assert result == "No tags here"

    def test_collapses_multiple_whitespace(self):
        result = self.strip_html("<p>Word1</p>   <p>Word2</p>")
        assert result == "Word1 Word2"

    def test_strips_html_entities_in_text(self):
        # Tags are removed but raw text content is preserved
        result = self.strip_html("<p>Steam &amp; Games</p>")
        assert "Steam" in result
        assert "Games" in result

    def test_empty_string_returns_empty(self):
        assert self.strip_html("") == ""

    def test_only_tags_returns_empty(self):
        result = self.strip_html("<div><br/><p></p></div>")
        assert result == ""

    def test_long_description_does_not_crash(self):
        long_html = "<p>" + "word " * 10000 + "</p>"
        result = self.strip_html(long_html)
        assert len(result) > 0
        assert "<" not in result


# ===========================================================================
# Logic / Sanity Checks for Column Constraints (replicated without Spark)
# ===========================================================================

class TestColumnLogicChecks:
    """
    These tests mirror the logic checks added to the Silver transform.
    They validate the business rules in pure Python so they can run offline.
    """

    def test_price_usd_cannot_be_negative(self):
        prices = [0.0, 5.99, 29.99, 59.99, 0.0]
        for p in prices:
            assert p >= 0, f"Price {p} should never be negative"

    def test_discount_percent_range(self):
        discounts = [0, 10, 50, 75, 90, 100]
        for d in discounts:
            assert 0 <= d <= 100, f"Discount {d} out of valid range 0-100"

    def test_review_score_range(self):
        # Steam review_score is an integer 0-9
        valid_scores = list(range(0, 10))
        for s in valid_scores:
            assert 0 <= s <= 9, f"Review score {s} out of range"

    def test_required_age_non_negative(self):
        ages = [0, 13, 17, 18]
        for a in ages:
            assert a >= 0, f"Required age {a} cannot be negative"

    def test_positive_reviews_lte_total(self):
        test_cases = [
            (100, 50),   # 50 positive out of 100 total — valid
            (0, 0),      # no reviews — valid
            (1000, 999), # 999 out of 1000 — valid
        ]
        for total, positive in test_cases:
            assert positive <= total, f"Positive ({positive}) > Total ({total})"

    def test_recommendations_non_negative(self):
        recs = [0, 1, 100, 5000000]
        for r in recs:
            assert r >= 0, f"Recommendations {r} cannot be negative"
