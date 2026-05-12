# dry_airmass_diurnal: uses the persistence of a dewpoint-depression anomaly to
# scale the diurnal temperature amplitude. In a sustained dry airmass (large
# positive DD anomaly), afternoon highs run above the climatological diurnal
# curve and overnight lows run below it. Members vary the lookback window (24h /
# 48h / 72h) and whether a pressure-ridge boost is also applied.
#
# members:
#   1  24h-amp          24h window, amplitude scaling only
#   2  48h-amp          48h window, amplitude scaling only
#   3  72h-amp          72h window, amplitude scaling only
#   4  24h-amp-ridge    24h window + pressure-departure daytime boost
#   5  48h-amp-ridge    48h window + pressure-departure daytime boost
#   6  72h-amp-ridge    72h window + pressure-departure daytime boost

import datetime as dt
import math
import statistics

import db
from models._utils import _sector

MODEL_ID = 15
MODEL_NAME = "dry_airmass_diurnal"
NEEDS_CONN_IN = True
NEEDS_WEIGHTS = True

LEAD_HOURS = [6, 12, 18, 24]

VAR_COL = {
    "temperature": "air_temp",
    "dewpoint":    "dew_point",
}

# members 1-3: amplitude only; members 4-6: amplitude + pressure ridge boost
_MEMBERS = [
    (1, 24, False),
    (2, 48, False),
    (3, 72, False),
    (4, 24, True),
    (5, 48, True),
    (6, 72, True),
]
_ALL_MEMBER_IDS = [mid for mid, _, _ in _MEMBERS]

_AMP_SENSITIVITY = 0.07   # diurnal-dev fraction per 1°C of DD anomaly
_P_SENSITIVITY   = 0.015  # °C per hPa pressure departure (afternoon hours only)
_TD_DECAY_K      = 0.04   # dewpoint anomaly e-folding per lead hour


def _local_hour_float(ts: int) -> float:
    d = dt.datetime.fromtimestamp(ts)
    return d.hour + d.minute / 60.0 + d.second / 3600.0


def _hour_means(
    obs_rows: list,
    col: str,
    min_obs: int = 3,
    min_buckets: int = 12,
) -> dict[int, float] | None:
    buckets: dict[int, list[float]] = {}
    for row in obs_rows:
        v = row[col]
        if v is None:
            continue
        h = dt.datetime.fromtimestamp(row["timestamp"]).hour
        buckets.setdefault(h, []).append(v)
    populated = {h: vals for h, vals in buckets.items() if len(vals) >= min_obs}
    if len(populated) < min_buckets:
        return None
    return {h: sum(vals) / len(vals) for h, vals in populated.items()}


def _interp_hm(hm: dict[int, float], hour: float) -> float | None:
    """Linear interpolation between integer-hour buckets, wrapping midnight."""
    if not hm:
        return None
    h0 = int(hour) % 24
    h1 = (h0 + 1) % 24
    if h0 in hm and h1 in hm:
        frac = hour - int(hour)
        return hm[h0] * (1 - frac) + hm[h1] * frac
    nearest = min(hm, key=lambda h: min(abs(h - h0), 24 - abs(h - h0)))
    return hm[nearest]


def _window_stats(obs_72h: list, issued_at: int, window_hours: int) -> dict | None:
    """Compute mean dewpoint depression and mean dewpoint over a lookback window.

    Returns None if the window has no usable obs.
    """
    cutoff = issued_at - window_hours * 3600
    window = [
        o for o in obs_72h
        if o["timestamp"] >= cutoff
        and o["air_temp"] is not None
        and o["dew_point"] is not None
    ]
    if not window:
        return None
    mean_dd = sum(o["air_temp"] - o["dew_point"] for o in window) / len(window)
    mean_td = sum(o["dew_point"] for o in window) / len(window)
    return {"mean_dd": mean_dd, "mean_td": mean_td}


def _null_rows(issued_at: int) -> list[dict]:
    """Return all-None rows for every (member, lead, variable) — used when
    historical data is insufficient to compute hour means."""
    rows = []
    for lead in LEAD_HOURS:
        valid_at = issued_at + lead * 3600
        for mid in [0] + _ALL_MEMBER_IDS:
            for variable in VAR_COL:
                rows.append({
                    "model_id": MODEL_ID, "model": MODEL_NAME, "member_id": mid,
                    "issued_at": issued_at, "valid_at": valid_at,
                    "lead_hours": lead, "variable": variable, "value": None,
                })
    return rows


def run(obs, issued_at: int, *, conn_in, weights=None) -> list[dict]:
    t_now = _local_hour_float(obs["timestamp"])

    raw_30d = db.tempest_obs_in_range(conn_in, issued_at - 30 * 86400, issued_at)
    raw_72h = db.tempest_obs_in_range(conn_in, issued_at - 72 * 3600, issued_at)

    hm_T  = _hour_means(raw_30d, "air_temp")
    hm_Td = _hour_means(raw_30d, "dew_point")

    if hm_T is None or hm_Td is None:
        return _null_rows(issued_at)

    daily_T_mean = sum(hm_T.values()) / len(hm_T)

    climo_T_now  = _interp_hm(hm_T,  t_now)
    climo_Td_now = _interp_hm(hm_Td, t_now)
    if climo_T_now is None or climo_Td_now is None:
        return _null_rows(issued_at)
    climo_DD_now = climo_T_now - climo_Td_now

    # pressure departure from 30d mean
    p_vals = [r["station_pressure"] for r in raw_30d if r["station_pressure"] is not None]
    p_baseline = sum(p_vals) / len(p_vals) if p_vals else None
    obs_pressure = obs.get("station_pressure")
    p_dep = (
        obs_pressure - p_baseline
        if obs_pressure is not None and p_baseline is not None
        else None
    )

    obs_T  = obs.get("air_temp")
    obs_Td = obs.get("dew_point")

    # precompute window stats for each lookback length
    window_cache: dict[int, dict | None] = {}
    for _, window_hours, _ in _MEMBERS:
        if window_hours not in window_cache:
            window_cache[window_hours] = _window_stats(raw_72h, issued_at, window_hours)

    rows = []
    for lead in LEAD_HOURS:
        valid_at = issued_at + lead * 3600
        t_valid = _local_hour_float(valid_at)

        T_base = _interp_hm(hm_T, t_valid)
        Td_base_valid = _interp_hm(hm_Td, t_valid)
        t_now_interp = _interp_hm(hm_T, t_now)
        anchor_T = (obs_T - t_now_interp) if obs_T is not None and t_now_interp is not None else None

        dev = (T_base - daily_T_mean) if T_base is not None else None
        td_decay = math.exp(-_TD_DECAY_K * lead)

        member_vals: dict[int, dict[str, float | None]] = {
            mid: {"temperature": None, "dewpoint": None} for mid in _ALL_MEMBER_IDS
        }

        for mid, window_hours, use_pressure in _MEMBERS:
            ws = window_cache[window_hours]
            if ws is None or T_base is None or anchor_T is None or dev is None:
                continue

            dd_anom = ws["mean_dd"] - climo_DD_now
            td_anom = ws["mean_td"] - climo_Td_now

            amp_adj = dd_anom * _AMP_SENSITIVITY * dev
            if use_pressure and p_dep is not None:
                p_adj = p_dep * _P_SENSITIVITY * max(0.0, dev)
            else:
                p_adj = 0.0

            member_vals[mid]["temperature"] = T_base + anchor_T + amp_adj + p_adj

            if Td_base_valid is not None:
                member_vals[mid]["dewpoint"] = Td_base_valid + td_anom * td_decay

        for mid, _, _ in _MEMBERS:
            for variable in VAR_COL:
                rows.append({
                    "model_id": MODEL_ID, "model": MODEL_NAME, "member_id": mid,
                    "issued_at": issued_at, "valid_at": valid_at,
                    "lead_hours": lead, "variable": variable,
                    "value": member_vals[mid][variable],
                })

        # member_id=0: weighted mean + spread
        for variable in VAR_COL:
            valid_pairs = [
                (mid, member_vals[mid][variable])
                for mid in _ALL_MEMBER_IDS
                if member_vals[mid][variable] is not None
            ]
            if not valid_pairs:
                mean = None
            elif weights:
                w_pairs = [
                    (weights.get((mid, variable, lead, _sector(valid_at)), None), v)
                    for mid, v in valid_pairs
                ]
                if any(w is None for w, _ in w_pairs):
                    mean = sum(v for _, v in valid_pairs) / len(valid_pairs)
                else:
                    total_w = sum(w for w, _ in w_pairs)
                    mean = sum(w * v for w, v in w_pairs) / total_w
            else:
                mean = sum(v for _, v in valid_pairs) / len(valid_pairs)
            spread = (
                statistics.pstdev([v for _, v in valid_pairs])
                if len(valid_pairs) > 1
                else None
            )
            rows.append({
                "model_id": MODEL_ID, "model": MODEL_NAME, "member_id": 0,
                "issued_at": issued_at, "valid_at": valid_at,
                "lead_hours": lead, "variable": variable,
                "value": mean, "spread": spread,
            })

    return rows
