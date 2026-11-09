"""_build_conditionals is a vectorized rewrite of a per-timestamp python scan;
this checks it against that scan, kept here as the reference."""
import math
import random

import models.synoptic_state_machine as ssm_mod
from models.surface_signs import _FUTURE_LOOKUP_SEC, _find_nearest_ts

_BASE = 1_700_000_000


def _obs(rng, ts):
    return {
        "timestamp": ts,
        "air_temp": rng.uniform(-5, 30) if rng.random() > 0.05 else None,
        "dew_point": rng.uniform(-10, 20) if rng.random() > 0.05 else None,
        "station_pressure": rng.uniform(1000, 1025),
        "wind_avg": rng.uniform(0, 8),
        "wind_gust": rng.uniform(0, 14),
        "wind_direction": rng.uniform(0, 360),
        "solar_radiation": rng.uniform(0, 600),
        "uv_index": 1.0,
        "precip_accum_day": rng.choice([0.0, 0.0, 0.5, 1.0]),
        "lightning_count": rng.choice([0, 0, 0, 2]),
        "relative_humidity": rng.uniform(30, 100),
    }


def _reference(all_obs, solar_climo):
    by_ts = {row["timestamp"]: row for row in all_obs}
    sorted_ts = sorted(by_ts)
    accum = {}
    for ts in sorted_ts:
        states = ssm_mod._signals_at(ts, sorted_ts, by_ts, solar_climo)
        for lead in ssm_mod.LEAD_HOURS:
            ts_fut = _find_nearest_ts(sorted_ts, ts + lead * 3600, _FUTURE_LOOKUP_SEC)
            if ts_fut is None:
                continue
            row_fut = by_ts[ts_fut]
            for mid, state in states.items():
                if state is None:
                    continue
                for col in ssm_mod.VARIABLES.values():
                    v_now, v_fut = by_ts[ts][col], row_fut[col]
                    if v_now is not None and v_fut is not None:
                        accum.setdefault((mid, state, col, lead), []).append(v_fut - v_now)
    return {k: sum(v) / len(v) for k, v in accum.items() if len(v) >= ssm_mod._MIN_SAMPLES}


def test_build_conditionals_matches_reference_scan():
    rng = random.Random(11)
    ts, all_obs = _BASE, []
    for _ in range(700):
        # irregular spacing with gaps, so some future lookups miss and some tie
        ts += rng.choice([300, 300, 300, 600, 900, 1800, 7200])
        all_obs.append(_obs(rng, ts))
    solar = ssm_mod._build_solar_climo(all_obs)

    conds, by_ts, sorted_ts = ssm_mod._build_conditionals(all_obs, solar)
    expected = _reference(all_obs, solar)

    assert expected, "fixture too thin to exercise any cell"
    assert set(conds) == set(expected)
    for key, value in expected.items():
        assert math.isclose(conds[key], value, rel_tol=1e-9, abs_tol=1e-9), key
    assert sorted_ts == sorted(by_ts)


def test_build_conditionals_empty_obs():
    assert ssm_mod._build_conditionals([], {})[0] == {}
