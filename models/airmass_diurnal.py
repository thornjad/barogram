# airmass_diurnal: scales the climatological diurnal curve by solar clearness
# index and airmass signals derived from Tempest observations only.
# member_id=0 is the performance-weighted mean of all members when weights are
# available, otherwise equal-weighted.
#
# members:
#   1  clearness-only              k persisted, scales diurnal amplitude
#   2  clearness+dewpoint          k × normalized dewpoint depression
#   3  clearness-pressure-projected k adjusted forward via pressure tendency
#   4  wind-sector-only            8-sector wind direction temperature offset
#   5  wind+clearness              sector offset + k combined
#   6  morning-warmup-rate         recent T rise rate scales afternoon amplitude
#   7  dewpoint-only               dewpoint depression, afternoon hours only
#   8  combined-full               k×dewpoint + sector offset

import datetime as dt
import math
import statistics

import db

MODEL_ID = 7
MODEL_NAME = "airmass_diurnal"
NEEDS_CONN_IN = True
NEEDS_WEIGHTS = True

LEAD_HOURS = [6, 12, 18, 24]

# pressure intentionally omitted
VAR_COL = {
    "temperature": "air_temp",
    "dewpoint":    "dew_point",
    "wind_speed":  "wind_avg",
}

_K_MEAN = 0.55          # typical mean clearness across all-weather days
_K_SENSITIVITY = 2.0    # amplitude multiplier per (k − K_MEAN) per unit diurnal dev
_TD_SENSITIVITY = 0.08  # °C temp adj per °C of dewpoint-depression anomaly
_P_K_SENSITIVITY = 0.03 # k change per hPa/h per lead hour (pressure→k projection)

# temperature offsets (°C) by 8-point wind sector: 0=N 1=NE 2=E 3=SE 4=S 5=SW 6=W 7=NW
_SECTOR_TEMP = {0: -1.5, 1: -1.0, 2: -0.5, 3: 0.5, 4: 1.5, 5: 2.0, 6: 0.5, 7: -0.5}

_MEMBER_NAMES = [
    (1, "clearness-only"),
    (2, "clearness+dewpoint"),
    (3, "clearness-pressure-projected"),
    (4, "wind-sector-only"),
    (5, "wind+clearness"),
    (6, "morning-warmup-rate"),
    (7, "dewpoint-only"),
    (8, "combined-full"),
]
_ALL_MEMBER_IDS = [mid for mid, _ in _MEMBER_NAMES]


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
    # fall back to nearest available hour
    nearest = min(hm, key=lambda h: min(abs(h - h0), 24 - abs(h - h0)))
    return hm[nearest]


def _clear_sky_irr(lat_deg: float, ts: int) -> float | None:
    """Theoretical clear-sky surface irradiance (W/m²)."""
    d = dt.datetime.fromtimestamp(ts)
    doy = d.timetuple().tm_yday
    hour = d.hour + d.minute / 60.0
    decl = math.radians(-23.45 * math.cos(math.radians(360 / 365 * (doy + 10))))
    lat = math.radians(lat_deg)
    ha = math.radians(15 * (hour - 12))
    sin_alt = (
        math.sin(lat) * math.sin(decl)
        + math.cos(lat) * math.cos(decl) * math.cos(ha)
    )
    if sin_alt <= 0.05:
        return None
    et_irr = 1361 * (1 + 0.033 * math.cos(math.radians(360 * doy / 365)))
    return et_irr * sin_alt * 0.75


def clearness_index(solar_rad: float | None, lat_deg: float, ts: int) -> float | None:
    """Observed / theoretical clear-sky ratio clamped [0, 1]; None if nighttime."""
    if solar_rad is None:
        return None
    cs = _clear_sky_irr(lat_deg, ts)
    if cs is None or cs <= 0:
        return None
    return max(0.0, min(1.0, solar_rad / cs))


def run(obs, issued_at: int, *, conn_in, weights=None) -> list[dict]:
    location = db.tempest_station_location(conn_in)
    lat = location[0] if location else None

    t_now = _local_hour_float(obs["timestamp"])

    # 30-day historical obs for climatology
    raw_30d = db.tempest_obs_in_range(conn_in, issued_at - 30 * 86400, issued_at)

    hm: dict[str, dict[int, float] | None] = {
        variable: _hour_means(raw_30d, col) for variable, col in VAR_COL.items()
    }
    t_hm = hm["temperature"]
    t_daily_mean = sum(t_hm.values()) / len(t_hm) if t_hm else None

    # clearness index at issued time
    k = clearness_index(obs.get("solar_radiation"), lat, obs["timestamp"]) if lat else None
    k_adj = (k - _K_MEAN) if k is not None else None

    # 3-hour obs window for pressure tendency (member 3) and warmup rate (member 6)
    recent_3h = db.tempest_obs_in_range(
        conn_in, obs["timestamp"] - 3 * 3600, obs["timestamp"]
    )
    p_vals = [r["station_pressure"] for r in recent_3h if r["station_pressure"] is not None]
    dp_dt = (p_vals[-1] - p_vals[0]) / 3.0 if len(p_vals) >= 2 else 0.0

    t_vals = [r["air_temp"] for r in recent_3h if r["air_temp"] is not None]
    rise_rate = (t_vals[-1] - t_vals[0]) / 3.0 if len(t_vals) >= 2 else 0.0
    if t_hm is not None:
        h0, h3 = int(t_now) % 24, (int(t_now) - 3) % 24
        climo_rise = (
            (t_hm[h0] - t_hm[h3]) / 3.0
            if h0 in t_hm and h3 in t_hm
            else None
        )
    else:
        climo_rise = None
    if climo_rise and abs(climo_rise) > 0.1:
        warmup_factor = max(0.5, min(2.5, rise_rate / climo_rise))
    else:
        warmup_factor = 1.0

    # dewpoint depression for members 2, 7, 8
    td_dep = (
        obs["air_temp"] - obs["dew_point"]
        if obs["air_temp"] is not None and obs["dew_point"] is not None
        else None
    )
    td_deps = [
        r["air_temp"] - r["dew_point"]
        for r in raw_30d
        if r["air_temp"] is not None and r["dew_point"] is not None
    ]
    td_dep_mean = sum(td_deps) / len(td_deps) if td_deps else None
    td_factor = (
        max(0.5, min(2.0, td_dep / td_dep_mean))
        if td_dep is not None and td_dep_mean
        else 1.0
    )

    # wind sector for members 4, 5, 8
    wind_dir = obs.get("wind_direction")
    sector = int((wind_dir + 22.5) / 45) % 8 if wind_dir is not None else None
    sector_temp_adj = _SECTOR_TEMP.get(sector, 0.0) if sector is not None else 0.0

    rows = []
    for lead in LEAD_HOURS:
        valid_at = obs["timestamp"] + lead * 3600
        t_valid = _local_hour_float(valid_at)

        # projected k for member 3
        if k is not None:
            kp = max(0.0, min(1.0, k + dp_dt * _P_K_SENSITIVITY * lead))
            k_proj_adj = kp - _K_MEAN
        else:
            k_proj_adj = None

        member_vals: dict[int, dict[str, float | None]] = {}

        for mid, _name in _MEMBER_NAMES:
            member_vals[mid] = {}
            for variable, col in VAR_COL.items():
                hm_v = hm[variable]
                if hm_v is None:
                    member_vals[mid][variable] = None
                    rows.append({
                        "model_id": MODEL_ID, "model": MODEL_NAME, "member_id": mid,
                        "issued_at": issued_at, "valid_at": valid_at,
                        "lead_hours": lead, "variable": variable, "value": None,
                    })
                    continue

                T_base_valid = _interp_hm(hm_v, t_valid)
                T_base_now = _interp_hm(hm_v, t_now)
                if T_base_valid is None or T_base_now is None:
                    member_vals[mid][variable] = None
                    rows.append({
                        "model_id": MODEL_ID, "model": MODEL_NAME, "member_id": mid,
                        "issued_at": issued_at, "valid_at": valid_at,
                        "lead_hours": lead, "variable": variable, "value": None,
                    })
                    continue

                obs_val = obs[col]
                anchor = (obs_val - T_base_now) if obs_val is not None else 0.0

                T_adj = 0.0
                if variable == "temperature" and t_daily_mean is not None:
                    dev = T_base_valid - t_daily_mean
                    v_hour = int(t_valid) % 24

                    if mid == 1:
                        if k_adj is not None:
                            T_adj = dev * k_adj * _K_SENSITIVITY
                    elif mid == 2:
                        if k_adj is not None:
                            T_adj = dev * (k_adj * td_factor) * _K_SENSITIVITY
                    elif mid == 3:
                        if k_proj_adj is not None:
                            T_adj = dev * k_proj_adj * _K_SENSITIVITY
                    elif mid == 4:
                        T_adj = sector_temp_adj
                    elif mid == 5:
                        T_adj = sector_temp_adj
                        if k_adj is not None:
                            T_adj += dev * k_adj * _K_SENSITIVITY
                    elif mid == 6:
                        if 9 <= v_hour <= 20:
                            T_adj = dev * (warmup_factor - 1.0)
                    elif mid == 7:
                        if td_dep is not None and td_dep_mean is not None and 10 <= v_hour <= 20:
                            T_adj = (td_dep - td_dep_mean) * _TD_SENSITIVITY
                    elif mid == 8:
                        T_adj = sector_temp_adj
                        if k_adj is not None:
                            T_adj += dev * (k_adj * td_factor) * _K_SENSITIVITY

                value = T_base_valid + anchor + T_adj
                member_vals[mid][variable] = value
                rows.append({
                    "model_id": MODEL_ID, "model": MODEL_NAME, "member_id": mid,
                    "issued_at": issued_at, "valid_at": valid_at,
                    "lead_hours": lead, "variable": variable, "value": value,
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
                    (weights.get((mid, variable, lead), None), v)
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
