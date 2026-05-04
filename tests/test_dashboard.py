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


# --- _overall_accuracy_html precip gating ---

def _make_acc_row(model_id, model, model_type, variable, lead_hours, avg_mae):
    return {
        "model_id": model_id,
        "model": model,
        "type": model_type,
        "variable": variable,
        "lead_hours": lead_hours,
        "avg_mae": avg_mae,
    }


def _base_rows():
    """Minimal rows: climo + one other model, temperature + precip_prob."""
    return [
        _make_acc_row(2, "climatological_mean", "base", "temperature", 24, 5.0),
        _make_acc_row(2, "climatological_mean", "base", "precip_prob", 24, 0.001),
        _make_acc_row(200, "nws", "external", "temperature", 24, 2.0),
        _make_acc_row(200, "nws", "external", "precip_prob", 24, 0.05),
    ]


def test_overall_accuracy_excludes_precip_below_threshold():
    html = dashboard._overall_accuracy_html(_base_rows(), precip_events=0)
    # skill computed only from temperature; precip_prob row absent from average
    assert "nws" in html
    # 2.0/5.0 = 60% skill for temperature only
    assert "60%" in html


def test_overall_accuracy_excludes_precip_at_threshold_minus_one():
    html = dashboard._overall_accuracy_html(_base_rows(), precip_events=4)
    assert "60%" in html


def test_overall_accuracy_includes_precip_at_threshold():
    # with precip_events=5, precip_prob is also included
    # temperature skill = (1 - 2.0/5.0)*100 = 60%
    # precip_prob skill = (1 - 0.05/0.001)*100 = -4900%
    # average = (60 + (-4900)) / 2 = -2420%
    html = dashboard._overall_accuracy_html(_base_rows(), precip_events=5)
    assert "-2420%" in html
