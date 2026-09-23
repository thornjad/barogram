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
#   9  clearness-trend             dk/dt projected k (slope over 3h window)
#  10  clearness-trend+dewpoint    projected k × dewpoint depression factor
#  11  clearness-trend+press-proj  projected k further adjusted by dP/dt
#  12  pressure-departure          station pressure departure from 30d mean → T offset
#  13  pressure-dep+clearness-trend  pressure departure + projected-k clearness
#  14  wind-veer                   veering/backing rate from 3h direction history
#  15  clearness-stability         k dampened by solar radiation CV (cloud character)
#  16  veer+clearness              member 14 + member 15 combined
#  17  clearsky-envelope-trend     trend of solar_radiation vs own 30d hourly max envelope
#  18  uv-solar-divergence         uv_index vs solar_radiation ratio, divergence from own history
#  19  early-ramp-steepness        solar_radiation ramp rate in first 2h after sunrise
#  20  snow-cover-proxy            sub-freezing run-length + precip infers snow-covered ground
#  21  self_correction             standard self-correction member (models/_self_correction.py)
#                                   -- member_id=0 minus this model's own learned bias
#
# guardrail (not a member, applied to every member's temperature value): a
# locally-learned seasonal diurnal-swing ceiling caps how far any member can
# push a value from the day's climatological mean. See _seasonal_swing_ceiling.

import datetime as dt
import math
import statistics
import time

import db
import models._confidence as _confidence
import models._self_correction as _self_correction
from models._utils import _sector

MODEL_ID = 7
MODEL_NAME = "airmass_diurnal"
NEEDS_CONN_IN = True
NEEDS_CONN_OUT = True
NEEDS_WEIGHTS = True
NEEDS_LOCATION = True
NEEDS_MATCH_HISTORY = True

_SELF_CORRECTION_MEMBER = 21

from models._climo_weights import LEAD_HOURS

# pressure intentionally omitted
VAR_COL = {
    "temperature": "air_temp",
    "dewpoint":    "dew_point",
}

_K_MEAN = 0.55          # typical mean clearness across all-weather days
_K_SENSITIVITY = 2.0    # amplitude multiplier per (k − K_MEAN) per unit diurnal dev
_TD_SENSITIVITY = 0.08  # °C temp adj per °C of dewpoint-depression anomaly
_P_K_SENSITIVITY = 0.03 # k change per hPa/h per lead hour (pressure→k projection)
_P_DEP_SENSITIVITY = 0.7  # °C temp adj per hPa below 30d mean pressure (warm sector signal)
_VEER_SENSITIVITY = 0.015 # °C per (°/hour of veer × lead_hours), capped at ±4°C
_CV_DAMPEN = 0.4          # fraction by which high solar CV reduces k_adj amplitude
_CV_MIN_OBS = 4           # minimum daytime solar obs to compute CV
_ENV_K_SENSITIVITY = 1.5   # amplitude multiplier for envelope-trend projected index (member 17)
_UV_DIV_SENSITIVITY = 15.0 # °C adj per unit of uv/solar ratio divergence from own history (member 18)
_RAMP_SENSITIVITY = 1.5    # amplitude multiplier for early solar-ramp factor (member 19)
_SNOW_SUPPRESSION = 1.5    # °C daytime warming suppression when snow cover is inferred (member 20)
_SNOW_MIN_RUN_DAYS = 3     # consecutive sub-freezing days before snow cover is inferred
_SNOW_RUN_CAP = 10         # run length (days) at which suppression reaches full _SNOW_SUPPRESSION
_SWING_HISTORY_DAYS = 400    # cross-year window for the seasonal swing ceiling, matches
                             # _confidence.py's own cross-year window convention
_SWING_CEILING_MIN_DAYS = 5  # minimum same-month history days before the ceiling activates

# temperature offsets (°C) by 8-point wind sector: 0=N 1=NE 2=E 3=SE 4=S 5=SW 6=W 7=NW
_SECTOR_TEMP = {0: -1.5, 1: -1.0, 2: -0.5, 3: 0.5, 4: 1.5, 5: 2.0, 6: 0.5, 7: -0.5}

_MEMBER_NAMES = [
    (1,  "clearness-only"),
    (2,  "clearness+dewpoint"),
    (3,  "clearness-pressure-projected"),
    (4,  "wind-sector-only"),
    (5,  "wind+clearness"),
    (6,  "morning-warmup-rate"),
    (7,  "dewpoint-only"),
    (8,  "combined-full"),
    (9,  "clearness-trend"),
    (10, "clearness-trend+dewpoint"),
    (11, "clearness-trend+pressure-proj"),
    (12, "pressure-departure"),
    (13, "pressure-dep+clearness-trend"),
    (14, "wind-veer"),
    (15, "clearness-stability"),
    (16, "veer+clearness"),
    (17, "clearsky-envelope-trend"),
    (18, "uv-solar-divergence"),
    (19, "early-ramp-steepness"),
    (20, "snow-cover-proxy"),
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

def _compute_dk_dt(solar_obs: list, lat: float) -> float | None:
    """Linear slope of clearness index (k/hour) from recent daytime obs.

    Filters to obs where clearness_index returns non-None (daytime only).
    Requires at least 2 qualifying points; returns None otherwise.
    """
    k_points = []
    for r in solar_obs:
        k = clearness_index(r["solar_radiation"], lat, r["timestamp"])
        if k is not None:
            k_points.append((r["timestamp"], k))
    if len(k_points) < 2:
        return None
    elapsed = (k_points[-1][0] - k_points[0][0]) / 3600.0
    if elapsed <= 0:
        return None
    return (k_points[-1][1] - k_points[0][1]) / elapsed

def _angular_diff(a: float, b: float) -> float:
    """Signed angular difference b − a in [−180, 180]. Positive = CW (veering)."""
    return ((b - a) + 180) % 360 - 180

def _compute_veer_rate(wind_obs: list) -> float | None:
    """Net veering rate (°/hour) from obs rows with wind_direction.
    Positive = clockwise = warm advection. None if < 2 valid points."""
    dirs = [
        (r["timestamp"], r["wind_direction"])
        for r in wind_obs
        if r["wind_direction"] is not None
    ]
    if len(dirs) < 2:
        return None
    elapsed = (dirs[-1][0] - dirs[0][0]) / 3600.0
    if elapsed <= 0:
        return None
    total = sum(_angular_diff(dirs[i][1], dirs[i + 1][1]) for i in range(len(dirs) - 1))
    return total / elapsed

def _solar_cv(solar_obs: list) -> float | None:
    """Coefficient of variation of solar radiation from daytime obs (> 10 W/m²).
    None if fewer than _CV_MIN_OBS qualifying readings."""
    vals = [
        r["solar_radiation"]
        for r in solar_obs
        if r["solar_radiation"] is not None and r["solar_radiation"] > 10
    ]
    if len(vals) < _CV_MIN_OBS:
        return None
    mean = sum(vals) / len(vals)
    if mean <= 0:
        return None
    return statistics.pstdev(vals) / mean

def _hour_max(
    obs_rows: list,
    col: str,
    min_obs: int = 3,
    min_buckets: int = 12,
) -> dict[int, float] | None:
    """Own-history envelope: per-local-hour max of col, distinct from the
    astronomical clear-sky formula _clear_sky_irr uses (member 17)."""
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
    return {h: max(vals) for h, vals in populated.items()}

def _compute_envelope_trend(obs_rows: list, env_hm: dict[int, float]) -> float | None:
    """Slope (per hour) of solar_radiation / envelope(hour) over obs_rows.
    None if envelope is missing or fewer than 2 daytime points qualify."""
    if not env_hm:
        return None
    points = []
    for r in obs_rows:
        if r["solar_radiation"] is None:
            continue
        env_val = _interp_hm(env_hm, _local_hour_float(r["timestamp"]))
        if env_val is None or env_val <= 0:
            continue
        ratio = max(0.0, min(1.3, r["solar_radiation"] / env_val))
        points.append((r["timestamp"], ratio))
    if len(points) < 2:
        return None
    elapsed = (points[-1][0] - points[0][0]) / 3600.0
    if elapsed <= 0:
        return None
    return (points[-1][1] - points[0][1]) / elapsed

def _sunrise_ts(lat_deg: float, day_ts: int) -> int | None:
    """First timestamp on day_ts's local date where the sun is up, per the
    same threshold _clear_sky_irr already uses. 5-minute resolution."""
    midnight = dt.datetime.fromtimestamp(day_ts).replace(hour=0, minute=0, second=0, microsecond=0)
    for minute in range(0, 12 * 60, 5):
        ts = int((midnight + dt.timedelta(minutes=minute)).timestamp())
        if _clear_sky_irr(lat_deg, ts) is not None:
            return ts
    return None

def _consecutive_subfreezing_run(obs_rows: list, today: dt.date) -> tuple[int, bool]:
    """Run-length (days, ending yesterday) of daily-max air_temp < 0°C, plus
    whether any day in that run recorded precip_accum_day > 0 (member 20).
    today's own partial date is excluded. A gap or a day at/above freezing
    ends the run."""
    daily_max: dict[dt.date, float] = {}
    daily_precip: dict[dt.date, float] = {}
    for row in obs_rows:
        d = dt.datetime.fromtimestamp(row["timestamp"]).date()
        if d == today:
            continue
        if row["air_temp"] is not None:
            daily_max[d] = max(daily_max.get(d, row["air_temp"]), row["air_temp"])
        if row["precip_accum_day"] is not None:
            daily_precip[d] = max(daily_precip.get(d, 0.0), row["precip_accum_day"])
    run_length = 0
    had_precip = False
    d = today - dt.timedelta(days=1)
    while d in daily_max and daily_max[d] < 0.0:
        run_length += 1
        if daily_precip.get(d, 0.0) > 0.0:
            had_precip = True
        d -= dt.timedelta(days=1)
    return run_length, had_precip

def _seasonal_swing_ceiling(hist_rows: list, month: int, min_days: int = _SWING_CEILING_MIN_DAYS) -> float | None:
    """95th-percentile daily (max − min air_temp) among hist_rows falling in
    calendar `month`, across whatever years of history hist_rows spans.
    None if fewer than min_days qualifying days exist yet (guardrail
    inactive until then)."""
    daily: dict[dt.date, tuple[float, float]] = {}
    for row in hist_rows:
        if row["air_temp"] is None:
            continue
        ts_dt = dt.datetime.fromtimestamp(row["timestamp"])
        if ts_dt.month != month:
            continue
        date = ts_dt.date()
        lo, hi = daily.get(date, (row["air_temp"], row["air_temp"]))
        daily[date] = (min(lo, row["air_temp"]), max(hi, row["air_temp"]))
    ranges = sorted(hi - lo for lo, hi in daily.values())
    if len(ranges) < min_days:
        return None
    idx = max(0, min(len(ranges) - 1, round(0.95 * (len(ranges) - 1))))
    return ranges[idx]

def run(obs, issued_at: int, *, conn_in, conn_out=None, weights=None, location=None, member_history=None,
        default_matches=None) -> list[dict]:
    if location is None:
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

    # pressure departure from 30d mean (member 12, 13)
    p_vals_30d = [r["station_pressure"] for r in raw_30d if r["station_pressure"] is not None]
    p_30d_mean = sum(p_vals_30d) / len(p_vals_30d) if p_vals_30d else None
    p_dep = (
        obs["station_pressure"] - p_30d_mean
        if obs["station_pressure"] is not None and p_30d_mean is not None
        else None
    )

    # clearness index at issued time
    k = clearness_index(obs["solar_radiation"], lat, obs["timestamp"]) if lat else None
    k_adj = (k - _K_MEAN) if k is not None else None

    # 3-hour obs window for pressure tendency (member 3) and warmup rate (member 6)
    recent_3h = db.tempest_obs_in_range(
        conn_in, obs["timestamp"] - 3 * 3600, obs["timestamp"]
    )
    p_vals = [r["station_pressure"] for r in recent_3h if r["station_pressure"] is not None]
    dp_dt = (p_vals[-1] - p_vals[0]) / 3.0 if len(p_vals) >= 2 else 0.0

    # clearness slope over 3h window (members 9, 10, 11, 13)
    dk_dt = _compute_dk_dt(recent_3h, lat) if lat is not None else None

    # veering/backing rate from 3h direction history (member 14, 16)
    veer_rate = _compute_veer_rate(recent_3h)

    # solar radiation coefficient of variation from 3h window (member 15, 16)
    solar_cv_val = _solar_cv(recent_3h)

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
    wind_dir = obs["wind_direction"]
    sector = int((wind_dir + 22.5) / 45) % 8 if wind_dir is not None else None
    sector_temp_adj = _SECTOR_TEMP.get(sector, 0.0) if sector is not None else 0.0

    obs_date = dt.datetime.fromtimestamp(obs["timestamp"]).date()

    # own-history clear-sky envelope + its trend (member 17)
    env_hm = _hour_max(raw_30d, "solar_radiation")
    env_idx_now = None
    if env_hm is not None and obs["solar_radiation"] is not None:
        env_val_now = _interp_hm(env_hm, t_now)
        if env_val_now is not None and env_val_now > 0:
            env_idx_now = max(0.0, min(1.3, obs["solar_radiation"] / env_val_now))
    env_dk_dt = _compute_envelope_trend(recent_3h, env_hm) if env_hm is not None else None

    # UV-vs-solar divergence from the station's own history (member 18)
    uv_hist_ratios = [
        r["uv_index"] / r["solar_radiation"]
        for r in raw_30d
        if r["solar_radiation"] is not None and r["solar_radiation"] > 50 and r["uv_index"] is not None
    ]
    uv_ratio_hist = sum(uv_hist_ratios) / len(uv_hist_ratios) if uv_hist_ratios else None
    uv_ratio_now = (
        obs.get("uv_index") / obs["solar_radiation"]
        if obs["solar_radiation"] is not None and obs["solar_radiation"] > 50 and obs.get("uv_index") is not None
        else None
    )
    uv_div = (
        uv_ratio_now - uv_ratio_hist
        if uv_ratio_now is not None and uv_ratio_hist is not None
        else None
    )

    # early solar-ramp steepness vs the theoretical clear-sky ramp (member 19)
    ramp_factor = None
    if lat is not None:
        sunrise_ts = _sunrise_ts(lat, obs["timestamp"])
        if sunrise_ts is not None and obs["timestamp"] >= sunrise_ts + 2 * 3600:
            ramp_obs = db.tempest_obs_in_range(conn_in, sunrise_ts, sunrise_ts + 2 * 3600)
            ramp_vals = [
                (r["timestamp"], r["solar_radiation"])
                for r in ramp_obs
                if r["solar_radiation"] is not None
            ]
            if len(ramp_vals) >= 2:
                elapsed_h = (ramp_vals[-1][0] - ramp_vals[0][0]) / 3600.0
                expected_irr = _clear_sky_irr(lat, sunrise_ts + 2 * 3600)
                if elapsed_h > 0 and expected_irr and expected_irr > 0:
                    actual_ramp = (ramp_vals[-1][1] - ramp_vals[0][1]) / elapsed_h
                    expected_ramp = expected_irr / 2.0
                    ramp_factor = max(0.0, min(2.5, actual_ramp / expected_ramp))

    # inferred snow cover from a sub-freezing run + any precip in it (member 20)
    snow_run_length, snow_had_precip = _consecutive_subfreezing_run(raw_30d, obs_date)
    snow_likely = snow_run_length >= _SNOW_MIN_RUN_DAYS and snow_had_precip

    # continental diurnal-swing ceiling: guardrail on every member's temperature
    # value, not a forecasting member of its own
    swing_hist = db.tempest_obs_in_range(conn_in, issued_at - _SWING_HISTORY_DAYS * 86400, issued_at)
    swing_ceiling = _seasonal_swing_ceiling(swing_hist, obs_date.month)
    half_swing_ceiling = swing_ceiling / 2.0 if swing_ceiling is not None else None

    rows = []
    for lead in LEAD_HOURS:
        valid_at = obs["timestamp"] + lead * 3600
        t_valid = _local_hour_float(valid_at)

        # projected k for member 3 (pressure-tendency adjustment to snapshot k)
        if k is not None:
            kp = max(0.0, min(1.0, k + dp_dt * _P_K_SENSITIVITY * lead))
            k_proj_adj = kp - _K_MEAN
        else:
            k_proj_adj = None

        # trend-projected k for members 9, 10, 11, 13 (dk/dt slope × lead)
        if dk_dt is not None and k is not None:
            k_trend = max(0.0, min(1.0, k + dk_dt * lead))
        else:
            k_trend = None
        k_trend_adj = (k_trend - _K_MEAN) if k_trend is not None else None

        # projected envelope index for member 17 (env_dk_dt slope × lead)
        if env_idx_now is not None and env_dk_dt is not None:
            env_idx_proj = max(0.0, min(1.3, env_idx_now + env_dk_dt * lead))
            env_adj = env_idx_proj - 1.0
        else:
            env_adj = None

        daytime_valid = _clear_sky_irr(lat, valid_at) is not None if lat is not None else False
        same_day_valid = dt.datetime.fromtimestamp(valid_at).date() == obs_date

        member_vals: dict[int, dict[str, float | None]] = {}

        variable_confidences = {
            variable: _confidence.member_confidences(
                member_history, default_matches, _ALL_MEMBER_IDS + [_SELF_CORRECTION_MEMBER], variable, lead
            )
            for variable in VAR_COL
        }

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
                        "confidence": variable_confidences[variable].get(mid),
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
                        "confidence": variable_confidences[variable].get(mid),
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
                    elif mid == 9:
                        if k_trend_adj is not None:
                            T_adj = dev * k_trend_adj * _K_SENSITIVITY
                    elif mid == 10:
                        if k_trend_adj is not None:
                            T_adj = dev * (k_trend_adj * td_factor) * _K_SENSITIVITY
                    elif mid == 11:
                        if k_trend is not None:
                            kp_trend = max(0.0, min(1.0, k_trend + dp_dt * _P_K_SENSITIVITY * lead))
                            T_adj = dev * (kp_trend - _K_MEAN) * _K_SENSITIVITY
                    elif mid == 12:
                        if p_dep is not None:
                            T_adj = -_P_DEP_SENSITIVITY * p_dep
                    elif mid == 13:
                        if p_dep is not None:
                            T_adj = -_P_DEP_SENSITIVITY * p_dep
                        if k_trend_adj is not None:
                            T_adj += dev * k_trend_adj * _K_SENSITIVITY
                    elif mid == 14:
                        if veer_rate is not None:
                            capped = max(-60.0, min(60.0, veer_rate))
                            T_adj = max(-4.0, min(4.0, capped * _VEER_SENSITIVITY * lead))
                    elif mid == 15:
                        if k_adj is not None:
                            if solar_cv_val is not None:
                                k_eff_adj = k_adj * (1.0 - _CV_DAMPEN * min(1.0, solar_cv_val))
                            else:
                                k_eff_adj = k_adj
                            T_adj = dev * k_eff_adj * _K_SENSITIVITY
                    elif mid == 16:
                        if veer_rate is not None:
                            capped = max(-60.0, min(60.0, veer_rate))
                            T_adj += max(-4.0, min(4.0, capped * _VEER_SENSITIVITY * lead))
                        if k_adj is not None:
                            if solar_cv_val is not None:
                                k_eff_adj = k_adj * (1.0 - _CV_DAMPEN * min(1.0, solar_cv_val))
                            else:
                                k_eff_adj = k_adj
                            T_adj += dev * k_eff_adj * _K_SENSITIVITY
                    elif mid == 17:
                        if env_adj is not None and lead <= 4 and daytime_valid:
                            T_adj = dev * env_adj * _ENV_K_SENSITIVITY
                    elif mid == 18:
                        if uv_div is not None and daytime_valid:
                            T_adj = dev * uv_div * _UV_DIV_SENSITIVITY
                    elif mid == 19:
                        if ramp_factor is not None and 4 <= lead <= 10 and same_day_valid:
                            T_adj = dev * (ramp_factor - 1.0) * _RAMP_SENSITIVITY
                    elif mid == 20:
                        if snow_likely and 9 <= v_hour <= 16:
                            T_adj = -_SNOW_SUPPRESSION * min(snow_run_length, _SNOW_RUN_CAP) / _SNOW_RUN_CAP

                value = T_base_valid + anchor + T_adj
                if variable == "temperature" and half_swing_ceiling is not None and t_daily_mean is not None:
                    value = max(t_daily_mean - half_swing_ceiling, min(t_daily_mean + half_swing_ceiling, value))
                member_vals[mid][variable] = value
                rows.append({
                    "model_id": MODEL_ID, "model": MODEL_NAME, "member_id": mid,
                    "issued_at": issued_at, "valid_at": valid_at,
                    "lead_hours": lead, "variable": variable, "value": value,
                    "confidence": variable_confidences[variable].get(mid),
                })

        # member_id=0: weighted mean + spread
        for variable in VAR_COL:
            cell_confidences = variable_confidences[variable]
            valid_pairs = [
                (mid, member_vals[mid][variable])
                for mid in _ALL_MEMBER_IDS
                if member_vals[mid][variable] is not None
            ]
            if not valid_pairs:
                mean, group_confidence = None, None
            elif weights:
                member_weights = {
                    mid: weights.get((mid, variable, lead, _sector(valid_at)))
                    for mid, _ in valid_pairs
                }
                confidences = {mid: cell_confidences.get(mid) for mid, _ in valid_pairs}
                mean, group_confidence = _confidence.combine_pattern(
                    valid_pairs, member_weights, confidences
                )
            else:
                mean = sum(v for _, v in valid_pairs) / len(valid_pairs)
                group_confidence = _confidence.average_confidence(
                    [cell_confidences.get(mid) for mid, _ in valid_pairs]
                )
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
                "confidence": group_confidence,
            })

            corrected = _self_correction.corrected_value(
                conn_out, MODEL_ID, variable, lead, mean, issued_at
            )
            rows.append({
                "model_id": MODEL_ID, "model": MODEL_NAME, "member_id": _SELF_CORRECTION_MEMBER,
                "issued_at": issued_at, "valid_at": valid_at,
                "lead_hours": lead, "variable": variable,
                "value": corrected,
                "confidence": cell_confidences.get(_SELF_CORRECTION_MEMBER),
            })

    return rows
