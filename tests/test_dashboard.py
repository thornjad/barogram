import dashboard


# --- _rolling_mean ---

def test_rolling_mean_basic():
    result = dashboard._rolling_mean([1.0, 2.0, 3.0, 4.0, 5.0], window=3)
    assert result[0] == 1.0
    assert result[1] == 1.5
    assert result[2] == 2.0
    assert result[3] == 3.0
    assert result[4] == 4.0


def test_rolling_mean_window_larger_than_input():
    result = dashboard._rolling_mean([2.0, 4.0, 6.0], window=10)
    assert result == [2.0, 3.0, 4.0]


def test_rolling_mean_skips_none():
    result = dashboard._rolling_mean([1.0, None, 3.0], window=3)
    assert result[0] == 1.0
    assert result[1] == 1.0   # only [1.0] valid in window [1.0, None]
    assert result[2] == 2.0   # mean of [1.0, 3.0]


def test_rolling_mean_all_none():
    result = dashboard._rolling_mean([None, None, None])
    assert result == [None, None, None]


def test_rolling_mean_empty():
    assert dashboard._rolling_mean([]) == []


# --- _score_summary_table ---

def _make_rows(model_id, model, model_type, avg_mae):
    """Minimal summary_rows entry usable by _compute_model_summary."""
    return {
        "model_id": model_id,
        "model": model,
        "type": model_type,
        "member_id": 0,
        "variable": "temperature",
        "lead_hours": 24,
        "n": 5,
        "avg_mae": avg_mae,
        "avg_bias": 0.0,
    }


def test_climo_ratio_is_one():
    rows = [
        _make_rows(1, "persistence", "base", 2.0),
        _make_rows(2, "climatological_mean", "base", 1.5),
    ]
    html = dashboard._score_summary_table(rows, "test window")
    # climo ratio cell should show 1.00
    assert 'data-ratio="1.00"' in html


def test_persistence_ratio_vs_climo():
    rows = [
        _make_rows(1, "persistence", "base", 2.0),
        _make_rows(2, "climatological_mean", "base", 1.0),
    ]
    html = dashboard._score_summary_table(rows, "test window")
    # persistence ratio = 2.0 / 1.0 = 2.00
    assert 'data-ratio="2.00"' in html


def test_other_model_ratio_vs_climo():
    rows = [
        _make_rows(1, "persistence", "base", 2.0),
        _make_rows(2, "climatological_mean", "base", 1.0),
        _make_rows(3, "pressure_tendency", "base", 0.8),
    ]
    html = dashboard._score_summary_table(rows, "test window")
    # pressure_tendency ratio = 0.8 / 1.0 = 0.80
    assert 'data-ratio="0.80"' in html


def test_baseline_badge_on_climo():
    rows = [
        _make_rows(1, "persistence", "base", 2.0),
        _make_rows(2, "climatological_mean", "base", 1.0),
    ]
    html = dashboard._score_summary_table(rows, "test window")
    assert 'class="baseline-badge"' in html
    # climo row should have baseline badge, not persistence
    climo_idx = html.index("climatological_mean")
    pers_idx = html.index("persistence")
    badge_idx = html.index('class="baseline-badge"')
    assert climo_idx < badge_idx  # badge comes after climo model name in table


def test_persistence_row_is_grey():
    rows = [
        _make_rows(1, "persistence", "base", 2.0),
        _make_rows(2, "climatological_mean", "base", 1.0),
    ]
    html = dashboard._score_summary_table(rows, "test window")
    # persistence row gets grey baseline-row styling, no badge
    assert 'baseline-row' in html
    assert 'pers-badge' not in html


def test_worse_than_persistence_amber_class():
    rows = [
        _make_rows(1, "persistence", "base", 1.0),
        _make_rows(2, "climatological_mean", "base", 0.8),
        _make_rows(3, "pressure_tendency", "base", 1.5),  # worse than both
    ]
    html = dashboard._score_summary_table(rows, "test window")
    # pressure_tendency ratio = 1.5/0.8 = 1.875; persistence ratio = 1.0/0.8 = 1.25
    # 1.875 > 1.25 → amber
    assert 'mae-worse-pers' in html


def test_no_amber_when_better_than_persistence():
    rows = [
        _make_rows(1, "persistence", "base", 2.0),
        _make_rows(2, "climatological_mean", "base", 1.0),
        _make_rows(3, "pressure_tendency", "base", 0.5),  # better than both
    ]
    html = dashboard._score_summary_table(rows, "test window")
    # pressure_tendency ratio = 0.5/1.0 = 0.5; persistence ratio = 2.0/1.0 = 2.0
    # 0.5 < 2.0 → no amber
    assert 'mae-worse-pers' not in html


def test_column_headers_say_vs_climo():
    rows = [
        _make_rows(1, "persistence", "base", 2.0),
        _make_rows(2, "climatological_mean", "base", 1.0),
    ]
    html = dashboard._score_summary_table(rows, "test window")
    assert "vs climo" in html


# --- _mae_timeseries_data ---

def _make_ts_row(model_id, model, model_type, issued_at, variable, avg_mae):
    return {
        "model_id": model_id,
        "model": model,
        "type": model_type,
        "member_id": 0,
        "variable": variable,
        "lead_hours": 24,
        "issued_at": issued_at,
        "avg_mae": avg_mae,
    }


_T0 = 1_700_000_000
_T1 = _T0 + 21600


def test_timeseries_climo_ratio_is_one():
    rows = [
        _make_ts_row(2, "climatological_mean", "base", _T0, "pressure", 1.5),
        _make_ts_row(1, "persistence", "base", _T0, "pressure", 2.0),
    ]
    result = dashboard._mae_timeseries_data(rows)
    climo = result["24"]["climatological_mean"]
    assert climo["series"]["pressure"]["y_ratio"] == [1.0]


def test_timeseries_persistence_ratio_vs_climo():
    rows = [
        _make_ts_row(2, "climatological_mean", "base", _T0, "pressure", 1.0),
        _make_ts_row(1, "persistence", "base", _T0, "pressure", 2.0),
    ]
    result = dashboard._mae_timeseries_data(rows)
    pers = result["24"]["persistence"]
    assert pers["series"]["pressure"]["y_ratio"] == [2.0]


def test_timeseries_is_baseline_flag():
    rows = [
        _make_ts_row(2, "climatological_mean", "base", _T0, "pressure", 1.0),
        _make_ts_row(1, "persistence", "base", _T0, "pressure", 2.0),
    ]
    result = dashboard._mae_timeseries_data(rows)
    assert result["24"]["climatological_mean"]["is_baseline"] is True
    assert result["24"]["persistence"]["is_baseline"] is False


def test_timeseries_is_persistence_flag():
    rows = [
        _make_ts_row(2, "climatological_mean", "base", _T0, "pressure", 1.0),
        _make_ts_row(1, "persistence", "base", _T0, "pressure", 2.0),
    ]
    result = dashboard._mae_timeseries_data(rows)
    assert result["24"]["persistence"]["is_persistence"] is True
    assert result["24"]["climatological_mean"]["is_persistence"] is False


def test_timeseries_rolling_present():
    rows = [
        _make_ts_row(2, "climatological_mean", "base", _T0, "pressure", 1.0),
        _make_ts_row(2, "climatological_mean", "base", _T1, "pressure", 1.0),
        _make_ts_row(1, "persistence", "base", _T0, "pressure", 2.0),
        _make_ts_row(1, "persistence", "base", _T1, "pressure", 2.0),
    ]
    result = dashboard._mae_timeseries_data(rows)
    pers = result["24"]["persistence"]["series"]["pressure"]
    assert "y_ratio_rolling" in pers
    assert len(pers["y_ratio_rolling"]) == len(pers["y_ratio"])
