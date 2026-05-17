"""Tests for result comparison helpers."""

from pathlib import Path

from duckcluster.compare import assert_result_matches, load_expected


def test_load_expected_parses_out_file(tmp_path: Path) -> None:
    """Tab-separated .out fixtures expose columns and rows for comparison."""
    fixture = tmp_path / "sample.out"
    fixture.write_text(
        "# columns: a,b\n"
        "1\t2\n"
        "3\t4\n",
        encoding="utf-8",
    )

    expected = load_expected(fixture)
    assert expected["columns"] == ["a", "b"]
    assert expected["rows"] == [["1", "2"], ["3", "4"]]


def test_assert_result_matches_ignores_row_order() -> None:
    """Row order differences do not fail an otherwise identical result set."""
    actual = {"columns": ["x"], "rows": [["2"], ["1"]]}
    expected = {"columns": ["x"], "rows": [["1"], ["2"]]}
    assert_result_matches(actual, expected)
