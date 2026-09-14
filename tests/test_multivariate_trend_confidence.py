"""Confidence wiring for multivariate_trend.py: real weights + non-uniform
confidence should shift member_id=0's value versus weights alone, and a
missing weight should drop only that member, not the whole group."""
import models.multivariate_trend as mvt_mod
from tests.conftest import make_input_db, make_obs

_ISSUED_AT = 1_700_000_000  # fixed epoch for determinism
_LEAD = 6
_VALID_AT = _ISSUED_AT + _LEAD * 3600
_SHORT_MID = 1   # linear-1h
_LONG_MID = 12   # linear-24h


def _row(ts, air_temp):
    return {
        "timestamp": ts,
        "air_temp": air_temp,
        "dew_point": 10.0,
        "station_pressure": 1013.0,
    }


def _seed_all_obs():
    """24h of flat history (50.0) followed by a sharp ramp in the final hour
    (50 -> 55 -> 60). A 1h-window linear fit sees only the steep ramp; a
    24h-window fit sees mostly flat history, so the two members extrapolate
    to genuinely different values at lead=6."""
    rows = [_row(_ISSUED_AT - h * 3600, 50.0) for h in range(24, 1, -1)]
    rows.append(_row(_ISSUED_AT - 3600, 50.0))       # t = -1h
    rows.append(_row(_ISSUED_AT - 1800, 55.0))        # t = -0.5h
    rows.append(_row(_ISSUED_AT, 60.0))               # t = 0h (now)
    return rows


def test_short_and_long_window_members_differ():
    conn_in = make_input_db()
    all_obs = _seed_all_obs()
    obs = make_obs(ts=_ISSUED_AT)

    rows = mvt_mod.run(obs, _ISSUED_AT, conn_in=conn_in, all_obs=all_obs)
    short_val = next(r["value"] for r in rows if r["member_id"] == _SHORT_MID and r["lead_hours"] == _LEAD and r["variable"] == "temperature")
    long_val = next(r["value"] for r in rows if r["member_id"] == _LONG_MID and r["lead_hours"] == _LEAD and r["variable"] == "temperature")

    assert short_val is not None and long_val is not None
    assert short_val != long_val


def test_confidence_shifts_value_versus_weights_alone():
    conn_in = make_input_db()
    all_obs = _seed_all_obs()
    obs = make_obs(ts=_ISSUED_AT)
    sector = mvt_mod._sector(_VALID_AT)

    weights = {(_SHORT_MID, "temperature", _LEAD, sector): 1.0,
               (_LONG_MID, "temperature", _LEAD, sector): 1.0}
    rows_no_confidence = mvt_mod.run(obs, _ISSUED_AT, conn_in=conn_in, all_obs=all_obs, weights=weights)
    mean_no_confidence = next(
        r["value"] for r in rows_no_confidence
        if r["member_id"] == 0 and r["lead_hours"] == _LEAD and r["variable"] == "temperature"
    )

    matched_days = [10_000_000 + d * 86400 for d in range(10)]
    baseline_days = [10_000_000 + d * 86400 for d in range(50, 80)]  # not in default_matches

    def _history(matched_mae, baseline_mae):
        rows = [{"variable": "temperature", "lead_hours": _LEAD, "issued_at": ts, "mae": matched_mae} for ts in matched_days]
        rows += [{"variable": "temperature", "lead_hours": _LEAD, "issued_at": ts, "mae": baseline_mae} for ts in baseline_days]
        return rows

    member_history = {
        _SHORT_MID: _history(matched_mae=0.1, baseline_mae=5.0),
        _LONG_MID: _history(matched_mae=10.0, baseline_mae=5.0),
    }
    default_matches = matched_days

    rows_with_confidence = mvt_mod.run(
        obs, _ISSUED_AT, conn_in=conn_in, all_obs=all_obs, weights=weights,
        member_history=member_history, default_matches=default_matches,
    )
    mean_with_confidence = next(
        r["value"] for r in rows_with_confidence
        if r["member_id"] == 0 and r["lead_hours"] == _LEAD and r["variable"] == "temperature"
    )

    assert mean_with_confidence != mean_no_confidence


def test_missing_weight_drops_only_that_member():
    conn_in = make_input_db()
    all_obs = _seed_all_obs()
    obs = make_obs(ts=_ISSUED_AT)
    sector = mvt_mod._sector(_VALID_AT)

    # only the short-window member gets a weight; the long-window member is dropped
    weights = {(_SHORT_MID, "temperature", _LEAD, sector): 1.0}
    rows = mvt_mod.run(obs, _ISSUED_AT, conn_in=conn_in, all_obs=all_obs, weights=weights)
    mean_row = next(r for r in rows if r["member_id"] == 0 and r["lead_hours"] == _LEAD and r["variable"] == "temperature")
    short_val = next(r["value"] for r in rows if r["member_id"] == _SHORT_MID and r["lead_hours"] == _LEAD and r["variable"] == "temperature")
    long_val = next(r["value"] for r in rows if r["member_id"] == _LONG_MID and r["lead_hours"] == _LEAD and r["variable"] == "temperature")

    assert abs(mean_row["value"] - short_val) < 1e-6
    assert abs(mean_row["value"] - (short_val + long_val) / 2) > 1e-6
