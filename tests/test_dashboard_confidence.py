"""Task 15: confidence rendering in the hero forecast cards and the
per-member detail table."""
import dashboard


def _ens_row(variable, lead_hours, value, spread, confidence, issued_at=1_700_000_000):
    return {
        "model": "barogram_ensemble",
        "member_id": 0,
        "variable": variable,
        "lead_hours": lead_hours,
        "value": value,
        "spread": spread,
        "confidence": confidence,
        "issued_at": issued_at,
        "valid_at": issued_at + lead_hours * 3600,
    }


def test_ensemble_forecast_section_renders_confidence_percentage():
    mean_rows = [_ens_row("temperature", 6, 20.0, 1.0, 0.73)]
    html = dashboard._ensemble_forecast_section(mean_rows, tempest=None)
    assert "73%" in html
    assert "fcst-confidence" in html


def test_ensemble_forecast_section_handles_missing_confidence():
    mean_rows = [_ens_row("temperature", 6, 20.0, 1.0, None)]
    html = dashboard._ensemble_forecast_section(mean_rows, tempest=None)
    # renders without crashing, empty confidence div rather than "None%"
    assert "None%" not in html
    assert '<div class="fcst-confidence"></div>' in html


def test_member_detail_js_includes_avg_confidence_column():
    member_rows = [
        {"model": "analog", "member_id": 1, "member_name": "k3", "variable": "temperature",
         "lead_hours": 6, "avg_mae": 1.5, "avg_confidence": 0.6, "n": 10},
    ]
    js = dashboard._member_detail_js(member_rows)
    assert "avg_confidence" in js
    assert "Avg Confidence" in js


def test_member_detail_js_handles_null_confidence():
    member_rows = [
        {"model": "analog", "member_id": 1, "member_name": "k3", "variable": "temperature",
         "lead_hours": 6, "avg_mae": 1.5, "avg_confidence": None, "n": 10},
    ]
    js = dashboard._member_detail_js(member_rows)
    assert "avg_confidence" in js  # data still flows through, JS handles null at render time
