# bogo: 70-member silly forecast ensemble.
# Each member uses a different flavor of wrongness.
# member_id=0 is the ensemble mean+spread; every other member_id is named
# (see the members table / docs/012_bogo.md for the full roster).
# Scored purely for entertainment; expected to perform poorly. Still, its
# members are genuinely skill-weighted now like every other model -- being
# a joke doesn't exempt it from tune's real accounting.

import datetime
import math
import random
import statistics

import db
import models._confidence as _confidence
import models._self_correction as _self_correction

MODEL_ID = 12
MODEL_NAME = "bogo"
NEEDS_CONN_IN = True
NEEDS_CONN_OUT = True
NEEDS_WEIGHTS = True
NEEDS_MATCH_HISTORY = True

from models._climo_weights import LEAD_HOURS
from models._utils import _sector
MIN_OBS = 30

# standard self-correction member (models/_self_correction.py): member_id=0's
# own mean minus this model's learned bias. Kept out of mr/_ensemble_mean --
# it's derived from the mean and must never feed back into it.
_SELF_CORRECTION_MEMBER = 70

_STEP = {
    "temperature": 5.0,
    "dewpoint": 3.0,
    "pressure": 3.0,
}

# hardcoded world records — we don't care if they're updated
_WORLD_RECORDS = {
    "temperature": (-89.2, 56.7),   # Vostok 1983, Death Valley 2013
    "dewpoint":    (-60.0, 35.5),   # dry arctic, Dhahran SA 2003
    "pressure":    (870.0, 1083.8), # Typhoon Tip 1979, Tosontsengel 2001
}

# approximate Mercury retrograde windows
_MERCURY_RETROGRADES = [
    ((2024, 1,  1), (2024, 1, 14)),
    ((2024, 4,  1), (2024, 4, 25)),
    ((2024, 8,  5), (2024, 8, 28)),
    ((2024, 11, 25), (2024, 12, 15)),
    ((2025, 1, 15), (2025, 2,  4)),
    ((2025, 5, 15), (2025, 6,  7)),
    ((2025, 9,  9), (2025, 10, 2)),
    ((2025, 12, 24), (2026, 1, 14)),
    ((2026, 4,  9), (2026, 5,  3)),
    ((2026, 8, 11), (2026, 9,  4)),
    ((2026, 11, 27), (2026, 12, 21)),
    ((2027, 3, 14), (2027, 4,  7)),
    ((2027, 7, 15), (2027, 8,  8)),
    ((2027, 11, 10), (2027, 12, 3)),
]

# 2024-01-01 00:00:00 UTC
_ASTROTURFED_EPOCH = 1704067200

# temp_offset_°C by weekday (0=Monday)
_DAY_BIAS = {
    0: -2.0,
    1: -1.0,
    2:  0.0,
    3:  0.5,
    4:  2.0,
    5:  0.0,
    6:  0.0,
}


def _is_mercury_retrograde(ts: int) -> bool:
    d = datetime.date.fromtimestamp(ts)
    for (sy, sm, sd), (ey, em, ed) in _MERCURY_RETROGRADES:
        if datetime.date(sy, sm, sd) <= d <= datetime.date(ey, em, ed):
            return True
    return False


def _c(val, lo, hi):
    return None if val is None else max(lo, min(hi, val))


def _cdp(dp, temp):
    if dp is None or temp is None:
        return dp
    return min(dp, temp)


def _clamp(result: dict) -> dict:
    clamped = {}
    for (lead, var), val in result.items():
        lo, hi = _WORLD_RECORDS.get(var, (-9999.0, 9999.0))
        v = _c(val, lo, hi)
        if var == "dewpoint":
            v = _cdp(v, result.get((lead, "temperature")))
        clamped[(lead, var)] = v
    return clamped


def _seasonal_extremes(conn_in, month: int) -> dict:
    row = conn_in.execute(
        """
        select min(t.air_temp) as temp_min, max(t.air_temp) as temp_max,
               min(t.dew_point) as dew_min, max(t.dew_point) as dew_max,
               min(t.station_pressure) as pres_min, max(t.station_pressure) as pres_max
        from tempest_obs t
        join stations s on s.station_id = t.station_id
        where s.source = 'tempest'
          and cast(strftime('%m', datetime(t.timestamp, 'unixepoch', 'localtime')) as integer) = ?
        """,
        (month,),
    ).fetchone()
    if row is None:
        return {}
    return {
        "temperature": (row["temp_min"], row["temp_max"]),
        "dewpoint":    (row["dew_min"],  row["dew_max"]),
        "pressure":    (row["pres_min"], row["pres_max"]),
    }


def _all_time_extremes(conn_in) -> dict:
    row = conn_in.execute(
        """
        select min(t.air_temp) as temp_min, max(t.air_temp) as temp_max,
               min(t.dew_point) as dew_min, max(t.dew_point) as dew_max,
               min(t.station_pressure) as pres_min, max(t.station_pressure) as pres_max
        from tempest_obs t
        join stations s on s.station_id = t.station_id
        where s.source = 'tempest'
        """
    ).fetchone()
    if row is None or row["temp_min"] is None:
        return {}
    return {
        "temperature": (row["temp_min"], row["temp_max"]),
        "dewpoint":    (row["dew_min"],  row["dew_max"]),
        "pressure":    (row["pres_min"], row["pres_max"]),
    }


def _climo_day_extremes(conn_in, month: int) -> dict:
    # diurnal range of the climo mean itself: min/max of the (month, hour)
    # bucket means across all 24 hours of the day
    temps, dews, press = [], [], []
    for hour in range(24):
        means = db.climo_bucket_means(conn_in, month, hour, MIN_OBS)
        if means.get("temperature") is not None:
            temps.append(means["temperature"])
        if means.get("dewpoint") is not None:
            dews.append(means["dewpoint"])
        if means.get("pressure") is not None:
            press.append(means["pressure"])
    if not temps or not dews or not press:
        return {}
    return {
        "temperature": (min(temps), max(temps)),
        "dewpoint":    (min(dews), max(dews)),
        "pressure":    (min(press), max(press)),
    }


def _extremes_in_range(conn_in, start_ts: int, end_ts: int) -> dict:
    row = conn_in.execute(
        """
        select min(t.air_temp) as temp_min, max(t.air_temp) as temp_max,
               min(t.dew_point) as dew_min, max(t.dew_point) as dew_max,
               min(t.station_pressure) as pres_min, max(t.station_pressure) as pres_max
        from tempest_obs t
        join stations s on s.station_id = t.station_id
        where s.source = 'tempest'
          and t.timestamp between ? and ?
        """,
        (start_ts, end_ts),
    ).fetchone()
    if row is None or row["temp_min"] is None:
        return {}
    return {
        "temperature": (row["temp_min"], row["temp_max"]),
        "dewpoint":    (row["dew_min"],  row["dew_max"]),
        "pressure":    (row["pres_min"], row["pres_max"]),
    }


def _earliest_tempest_ts(conn_in) -> int | None:
    row = conn_in.execute(
        """
        select min(t.timestamp) as ts
        from tempest_obs t
        join stations s on s.station_id = t.station_id
        where s.source = 'tempest'
        """
    ).fetchone()
    return row["ts"] if row is not None and row["ts"] is not None else None


def _random_window_extremes(conn_in, now_ts: int, window_sec: int) -> dict:
    # picks one random window_sec-wide slice of history instead of the most
    # recent one -- used by the "random 7 days" sine variant
    earliest = _earliest_tempest_ts(conn_in)
    if earliest is None or now_ts - window_sec <= earliest:
        return _extremes_in_range(conn_in, now_ts - window_sec, now_ts)
    start = random.uniform(earliest, now_ts - window_sec)
    return _extremes_in_range(conn_in, int(start), int(start) + window_sec)


def _safe_uniform(lo, hi):
    if lo is None or hi is None:
        return None
    if lo >= hi:
        return lo
    return random.uniform(lo, hi)


def _extremes_roulette(extremes: dict) -> dict:
    # fresh independent random draw per variable, per lead hour
    if not extremes:
        return {}
    t_lo, t_hi = extremes["temperature"]
    d_lo, d_hi = extremes["dewpoint"]
    p_lo, p_hi = extremes["pressure"]
    result = {}
    for lead in LEAD_HOURS:
        temp = _safe_uniform(t_lo, t_hi)
        dew  = _safe_uniform(d_lo, min(d_hi, temp) if temp is not None else d_hi)
        pres = _safe_uniform(p_lo, p_hi)
        result[(lead, "temperature")] = temp
        result[(lead, "dewpoint")]    = dew
        result[(lead, "pressure")]    = pres
    return result


def _extremes_climb(extremes: dict) -> dict:
    # each lead draws random between the prior lead's value and the high
    if not extremes:
        return {}
    t_lo, t_hi = extremes["temperature"]
    d_lo, d_hi = extremes["dewpoint"]
    p_lo, p_hi = extremes["pressure"]
    temp = _safe_uniform(t_lo, t_hi)
    dew  = _safe_uniform(d_lo, min(d_hi, temp) if temp is not None else d_hi)
    pres = _safe_uniform(p_lo, p_hi)
    result = {}
    for lead in LEAD_HOURS:
        result[(lead, "temperature")] = temp
        result[(lead, "dewpoint")]    = dew
        result[(lead, "pressure")]    = pres
        temp = _safe_uniform(temp, t_hi)
        dew  = _safe_uniform(dew, min(d_hi, temp) if temp is not None else d_hi)
        pres = _safe_uniform(pres, p_hi)
    return result


def _extremes_drop(extremes: dict) -> dict:
    # each lead draws random between the low and the prior lead's value
    if not extremes:
        return {}
    t_lo, t_hi = extremes["temperature"]
    d_lo, d_hi = extremes["dewpoint"]
    p_lo, p_hi = extremes["pressure"]
    temp = _safe_uniform(t_lo, t_hi)
    dew  = _safe_uniform(d_lo, min(d_hi, temp) if temp is not None else d_hi)
    pres = _safe_uniform(p_lo, p_hi)
    result = {}
    for lead in LEAD_HOURS:
        result[(lead, "temperature")] = temp
        result[(lead, "dewpoint")]    = dew
        result[(lead, "pressure")]    = pres
        temp = _safe_uniform(t_lo, temp)
        dew  = _safe_uniform(d_lo, min(dew, temp) if dew is not None and temp is not None else dew)
        pres = _safe_uniform(p_lo, pres)
    return result


def _random_past_obs(conn_in):
    return conn_in.execute(
        """
        select t.air_temp as temperature, t.dew_point as dewpoint,
               t.station_pressure as pressure
        from tempest_obs t
        join stations s on s.station_id = t.station_id
        where s.source = 'tempest' and t.air_temp is not null
        order by random()
        limit 1
        """
    ).fetchone()


def _precompute_climos(obs, conn_in) -> dict:
    climos = {}
    for lead in LEAD_HOURS:
        valid_at = obs["timestamp"] + lead * 3600
        t = datetime.datetime.fromtimestamp(valid_at)
        means = db.climo_bucket_means(conn_in, t.month, t.hour, MIN_OBS)
        climos[lead] = {
            "temperature": means.get("temperature"),
            "dewpoint":    means.get("dewpoint"),
            "pressure":    means.get("pressure"),
            "valid_at":    valid_at,
        }
    return climos


# ---- member implementations ----
# each returns dict[(lead_hours, variable) -> float | None]

def _drunkard(climos: dict) -> dict:
    result = {}
    state = None
    for lead in LEAD_HOURS:
        c = climos[lead]
        if state is None:
            state = {v: c[v] for v in ["temperature", "dewpoint", "pressure"]}
        for var, bound in _STEP.items():
            prev = state[var]
            val = None if prev is None else prev + random.uniform(-bound, bound)
            state[var] = val
            result[(lead, var)] = val
    return _clamp(result)


def _blind_drunkard(climos: dict) -> dict:
    result = {}
    for lead in LEAD_HOURS:
        c = climos[lead]
        for var, bound in _STEP.items():
            base = c[var]
            val = None if base is None else base + random.uniform(-bound, bound)
            result[(lead, var)] = val
    return _clamp(result)


def _chaos(climos: dict) -> dict:
    result = {}
    state = None
    for lead in LEAD_HOURS:
        c = climos[lead]
        if state is None:
            state = {v: c[v] for v in ["temperature", "dewpoint", "pressure"]}
        for var, bound in _STEP.items():
            b3 = bound * 3.0
            prev = state[var]
            val = None if prev is None else prev + random.uniform(-b3, b3)
            state[var] = val
            result[(lead, var)] = val
    return _clamp(result)


def _vibes(climos: dict, extremes: dict) -> dict:
    result = {}
    for lead in LEAD_HOURS:
        c = climos[lead]
        for var in ["temperature", "dewpoint", "pressure"]:
            if var in extremes and extremes[var] and None not in extremes[var]:
                lo, hi = extremes[var]
                val = random.uniform(lo, hi)
            else:
                base = c[var]
                fallback = {"temperature": 15.0, "dewpoint": 10.0, "pressure": 15.0}.get(var, 10.0)
                val = None if base is None else random.uniform(base - fallback, base + fallback)
            result[(lead, var)] = val
    return _clamp(result)


def _contrarian(obs, climos: dict, conn_in) -> dict:
    obs_dt = datetime.datetime.fromtimestamp(obs["timestamp"])
    now_means = db.climo_bucket_means(conn_in, obs_dt.month, obs_dt.hour, MIN_OBS)
    deviations = {
        "temperature": (obs["air_temp"] or 0.0) - (now_means.get("temperature") or obs["air_temp"] or 0.0),
        "dewpoint":    (obs["dew_point"] or 0.0) - (now_means.get("dewpoint") or obs["dew_point"] or 0.0),
        "pressure":    (obs["station_pressure"] or 0.0) - (now_means.get("pressure") or obs["station_pressure"] or 0.0),
    }
    result = {}
    for lead in LEAD_HOURS:
        c = climos[lead]
        for var in ["temperature", "dewpoint", "pressure"]:
            base = c[var]
            result[(lead, var)] = None if base is None else base - deviations[var]
    return _clamp(result)


def _hype_train(obs, climos: dict, obs_6h_ago) -> dict:
    if obs_6h_ago is None:
        return {(lead, var): climos[lead][var] for lead in LEAD_HOURS
                for var in ["temperature", "dewpoint", "pressure"]}
    trends = {
        "temperature": (obs["air_temp"] or 0.0) - (obs_6h_ago["air_temp"] or obs["air_temp"] or 0.0),
        "dewpoint":    (obs["dew_point"] or 0.0) - (obs_6h_ago["dew_point"] or obs["dew_point"] or 0.0),
        "pressure":    (obs["station_pressure"] or 0.0) - (obs_6h_ago["station_pressure"] or obs["station_pressure"] or 0.0),
    }
    result = {}
    for i, lead in enumerate(LEAD_HOURS):
        mult = i + 1
        for var in ["temperature", "dewpoint", "pressure"]:
            base = obs["air_temp" if var == "temperature" else "dew_point" if var == "dewpoint" else "station_pressure"]
            jitter = random.uniform(-0.3, 0.3) * _STEP[var]
            result[(lead, var)] = None if base is None else base + trends[var] * mult + jitter
    return _clamp(result)


def _mercury_retrograde(climos: dict, ts: int) -> dict:
    mult = 10.0 if _is_mercury_retrograde(ts) else 0.5
    result = {}
    state = None
    for lead in LEAD_HOURS:
        c = climos[lead]
        if state is None:
            state = {v: c[v] for v in ["temperature", "dewpoint", "pressure"]}
        for var, bound in _STEP.items():
            bm = bound * mult
            prev = state[var]
            val = None if prev is None else prev + random.uniform(-bm, bm)
            state[var] = val
            result[(lead, var)] = val
    return _clamp(result)


def _weatherperson(climos: dict) -> dict:
    result = {}
    for lead in LEAD_HOURS:
        c = climos[lead]
        result[(lead, "temperature")] = c["temperature"]
        result[(lead, "dewpoint")]    = c["dewpoint"]
        result[(lead, "pressure")]    = c["pressure"]
    return result


def _crowd_sourced(rand_obs, climos: dict) -> dict:
    result = {}
    for lead in LEAD_HOURS:
        c = climos[lead]
        if rand_obs is not None:
            temp = rand_obs["temperature"]
            dp   = _cdp(rand_obs["dewpoint"], temp)
            result[(lead, "temperature")] = temp
            result[(lead, "dewpoint")]    = dp
            result[(lead, "pressure")]    = rand_obs["pressure"]
        else:
            result[(lead, "temperature")] = c["temperature"]
            result[(lead, "dewpoint")]    = c["dewpoint"]
            result[(lead, "pressure")]    = c["pressure"]
    return result


def _groundhog_day(obs_24h_ago, climos: dict) -> dict:
    result = {}
    for lead in LEAD_HOURS:
        c = climos[lead]
        if obs_24h_ago is not None:
            temp = obs_24h_ago["air_temp"]
            dp   = _cdp(obs_24h_ago["dew_point"], temp)
            result[(lead, "temperature")] = temp
            result[(lead, "dewpoint")]    = dp
            result[(lead, "pressure")]    = obs_24h_ago["station_pressure"]
        else:
            result[(lead, "temperature")] = c["temperature"]
            result[(lead, "dewpoint")]    = c["dewpoint"]
            result[(lead, "pressure")]    = c["pressure"]
    return result


def _cg(obs, climos: dict) -> dict:
    result = {}
    for lead in LEAD_HOURS:
        c = climos[lead]
        result[(lead, "temperature")] = c["temperature"]
        result[(lead, "dewpoint")]    = c["dewpoint"]
        result[(lead, "pressure")]    = c["pressure"]
    return result


def _climate_anxiety(climos: dict) -> dict:
    result = {}
    for lead in LEAD_HOURS:
        c = climos[lead]
        temp = None if c["temperature"] is None else c["temperature"] + 3.0
        dew  = None if c["dewpoint"] is None else _cdp(c["dewpoint"] + 3.0, temp)
        result[(lead, "temperature")] = temp
        result[(lead, "dewpoint")]    = dew
        result[(lead, "pressure")]    = c["pressure"]
    return result


def _too_early(obs_6h_ago, climos: dict) -> dict:
    result = {}
    for lead in LEAD_HOURS:
        c = climos[lead]
        if obs_6h_ago is not None:
            temp = obs_6h_ago["air_temp"]
            dp   = _cdp(obs_6h_ago["dew_point"], temp)
            result[(lead, "temperature")] = temp
            result[(lead, "dewpoint")]    = dp
            result[(lead, "pressure")]    = obs_6h_ago["station_pressure"]
        else:
            result[(lead, "temperature")] = c["temperature"]
            result[(lead, "dewpoint")]    = c["dewpoint"]
            result[(lead, "pressure")]    = c["pressure"]
    return result


def _monday(climos: dict) -> dict:
    result = {}
    for lead in LEAD_HOURS:
        c = climos[lead]
        weekday = datetime.datetime.fromtimestamp(c["valid_at"]).weekday()
        temp_off = _DAY_BIAS.get(weekday, 0.0)
        temp = None if c["temperature"] is None else c["temperature"] + temp_off
        dew  = None if c["dewpoint"] is None else _cdp(c["dewpoint"] + temp_off * 0.5, temp)
        result[(lead, "temperature")] = temp
        result[(lead, "dewpoint")]    = dew
        result[(lead, "pressure")]    = c["pressure"]
    return result


def _grant_funded(climos: dict) -> dict:
    result = {}
    state = None
    for lead in LEAD_HOURS:
        c = climos[lead]
        if state is None:
            state = {v: c[v] for v in ["temperature", "dewpoint", "pressure"]}
        for var, bound in _STEP.items():
            if random.random() < 0.20:
                state[var] = None
                result[(lead, var)] = None
                continue
            prev = state[var]
            bh = bound * 0.5
            val = None if prev is None else prev + random.uniform(-bh, bh)
            state[var] = val
            result[(lead, var)] = val
    return _clamp(result)


def _the_algorithm(obs, climos: dict) -> dict:
    c6 = climos[6]
    temp_climo = c6["temperature"] or obs["air_temp"] or 0.0
    temp_obs   = obs["air_temp"] or temp_climo
    deviation  = temp_obs - temp_climo
    direction  = 1 if deviation >= 0 else -1
    magnitude  = abs(deviation)
    result = {}
    for lead in LEAD_HOURS:
        c = climos[lead]
        temp = None if c["temperature"] is None else c["temperature"] + 2.0 * magnitude * direction
        dew  = None if c["dewpoint"] is None else c["dewpoint"] + 1.5 * magnitude * direction
        pres = None if c["pressure"] is None else c["pressure"] - 3.0 * magnitude * direction
        dew  = _cdp(dew, temp)
        result[(lead, "temperature")] = temp
        result[(lead, "dewpoint")]    = dew
        result[(lead, "pressure")]    = pres
    return _clamp(result)


def _peer_review(other_members: dict) -> dict:
    noise = {"temperature": 0.5, "dewpoint": 0.3, "pressure": 0.5}
    result = {}
    for lead in LEAD_HOURS:
        for var in ["temperature", "dewpoint", "pressure"]:
            vals = [m[(lead, var)] for m in other_members.values()
                    if (lead, var) in m and m[(lead, var)] is not None]
            if vals:
                mean = statistics.mean(vals)
                result[(lead, var)] = mean + random.uniform(-noise[var], noise[var])
            else:
                result[(lead, var)] = None
    return _clamp(result)


def _dew_denier(climos: dict) -> dict:
    result = {}
    for lead in LEAD_HOURS:
        c = climos[lead]
        result[(lead, "temperature")] = c["temperature"]
        result[(lead, "dewpoint")]    = c["temperature"]  # same as temp: 100% RH always
        result[(lead, "pressure")]    = c["pressure"]
    return result


def _breaking_news(obs, climos: dict, extremes: dict) -> dict:
    c6 = climos[6]
    _obs_col = {"temperature": "air_temp", "dewpoint": "dew_point", "pressure": "station_pressure"}
    result = {}
    for lead in LEAD_HOURS:
        c = climos[lead]
        for var in ["temperature", "dewpoint", "pressure"]:
            if var not in extremes or not extremes[var] or None in extremes[var]:
                result[(lead, var)] = c[var]
                continue
            lo, hi = extremes[var]
            obs_val   = obs[_obs_col[var]] or c6[var] or 0.0
            climo_val = c6[var] or obs_val
            result[(lead, var)] = hi if obs_val >= climo_val else lo
    return _clamp(result)


def _engagement_bait(climos: dict) -> dict:
    result = {}
    for lead in LEAD_HOURS:
        c = climos[lead]
        temp = None if c["temperature"] is None else float(round(c["temperature"]))
        dew  = None if c["dewpoint"] is None else float(round(c["dewpoint"]))
        pres = None if c["pressure"] is None else round(c["pressure"], 1)
        dew  = _cdp(dew, temp)
        result[(lead, "temperature")] = temp
        result[(lead, "dewpoint")]    = dew
        result[(lead, "pressure")]    = pres
    return result


def _both_sides(climos: dict, extremes: dict) -> dict:
    result = {}
    for i, lead in enumerate(LEAD_HOURS):
        c = climos[lead]
        is_hot = (i % 2 == 0)
        for var in ["temperature", "dewpoint", "pressure"]:
            if var not in extremes or not extremes[var] or None in extremes[var]:
                result[(lead, var)] = c[var]
                continue
            lo, hi = extremes[var]
            result[(lead, var)] = hi if is_hot else lo
    return _clamp(result)


def _sponsored_content() -> dict:
    result = {}
    for lead in LEAD_HOURS:
        result[(lead, "temperature")] = 22.0
        result[(lead, "dewpoint")]    = 15.0
        result[(lead, "pressure")]    = 1013.0
    return result


def _influencer(climos: dict) -> dict:
    aesthetic = random.choice(["golden", "storm"])
    result = {}
    for lead in LEAD_HOURS:
        c = climos[lead]
        if aesthetic == "golden":
            temp = None if c["temperature"] is None else c["temperature"] + 6.0
            dew  = c["dewpoint"]
            pres = None if c["pressure"] is None else c["pressure"] + 3.0
        else:
            temp = None if c["temperature"] is None else c["temperature"] - 6.0
            dew  = None if c["dewpoint"] is None else c["dewpoint"] + 2.0
            pres = None if c["pressure"] is None else c["pressure"] - 15.0
        dew = _cdp(dew, temp)
        result[(lead, "temperature")] = temp
        result[(lead, "dewpoint")]    = dew
        result[(lead, "pressure")]    = pres
    return _clamp(result)


def _panic(obs, climos: dict, obs_6h_ago) -> dict:
    if (obs_6h_ago is None
            or obs["station_pressure"] is None
            or obs_6h_ago["station_pressure"] is None):
        return {(lead, var): climos[lead][var] for lead in LEAD_HOURS
                for var in ["temperature", "dewpoint", "pressure"]}
    trend = obs["station_pressure"] - obs_6h_ago["station_pressure"]
    result = {}
    for lead in LEAD_HOURS:
        c = climos[lead]
        if trend < 0:
            temp = None if c["temperature"] is None else c["temperature"] - 15.0
            dew  = None if c["dewpoint"] is None else c["dewpoint"] + 5.0
            pres = None if c["pressure"] is None else c["pressure"] - 25.0
        elif trend > 0:
            temp = None if c["temperature"] is None else c["temperature"] + 15.0
            dew  = None if c["dewpoint"] is None else c["dewpoint"] - 5.0
            pres = None if c["pressure"] is None else c["pressure"] + 20.0
        else:
            temp = c["temperature"]
            dew  = c["dewpoint"]
            pres = c["pressure"]
        dew = _cdp(dew, temp)
        result[(lead, "temperature")] = temp
        result[(lead, "dewpoint")]    = dew
        result[(lead, "pressure")]    = pres
    return _clamp(result)


def _nostalgia(obs_1yr_ago, climos: dict) -> dict:
    result = {}
    for lead in LEAD_HOURS:
        c = climos[lead]
        if obs_1yr_ago is not None:
            temp = obs_1yr_ago["air_temp"]
            dp   = _cdp(obs_1yr_ago["dew_point"], temp)
            result[(lead, "temperature")] = temp
            result[(lead, "dewpoint")]    = dp
            result[(lead, "pressure")]    = obs_1yr_ago["station_pressure"]
        else:
            result[(lead, "temperature")] = c["temperature"]
            result[(lead, "dewpoint")]    = c["dewpoint"]
            result[(lead, "pressure")]    = c["pressure"]
    return result


def _astroturfed(climos: dict, issued_at: int) -> dict:
    months_elapsed = (issued_at - _ASTROTURFED_EPOCH) / (30 * 86400)
    drift = months_elapsed * 0.1
    result = {}
    for lead in LEAD_HOURS:
        c = climos[lead]
        temp = None if c["temperature"] is None else c["temperature"] + drift
        dew  = None if c["dewpoint"] is None else _cdp(c["dewpoint"] + drift * 0.8, temp)
        result[(lead, "temperature")] = temp
        result[(lead, "dewpoint")]    = dew
        result[(lead, "pressure")]    = c["pressure"]
    return result


def _record_breaker(obs, climos: dict, conn_in) -> dict:
    obs_dt   = datetime.datetime.fromtimestamp(obs["timestamp"])
    now_means = db.climo_bucket_means(conn_in, obs_dt.month, obs_dt.hour, MIN_OBS)
    _obs_col = {
        "temperature": "air_temp",
        "dewpoint":    "dew_point",
        "pressure":    "station_pressure",
    }
    result = {}
    for lead in LEAD_HOURS:
        c = climos[lead]
        for var, col in _obs_col.items():
            obs_val   = obs[col]
            climo_val = now_means.get(var)
            lo, hi = _WORLD_RECORDS[var]
            if obs_val is None or climo_val is None:
                result[(lead, var)] = c[var]
            elif obs_val >= climo_val:
                result[(lead, var)] = hi
            else:
                result[(lead, var)] = lo
    return _clamp(result)


def _mirror_time(obs, conn_in) -> dict:
    # forecast for +N hours is whatever conditions actually were N hours ago
    result = {}
    for lead in LEAD_HOURS:
        past = db.nearest_tempest_obs(conn_in, obs["timestamp"] - lead * 3600)
        if past is not None:
            temp = past["air_temp"]
            result[(lead, "temperature")] = temp
            result[(lead, "dewpoint")]    = _cdp(past["dew_point"], temp)
            result[(lead, "pressure")]    = past["station_pressure"]
        else:
            result[(lead, "temperature")] = None
            result[(lead, "dewpoint")]    = None
            result[(lead, "pressure")]    = None
    return result


def _random_aggregate(conn_in, climos: dict, n: int) -> dict:
    # averages n randomly chosen historical obs from the same (month, hour)
    # bucket as each lead's valid_at -- n is picked once per member per run
    result = {}
    for lead in LEAD_HOURS:
        c = climos[lead]
        dt = datetime.datetime.fromtimestamp(c["valid_at"])
        rows = db.climo_bucket_obs(conn_in, dt.month, dt.hour)
        sample = random.sample(rows, min(n, len(rows))) if rows else []
        vals_t = [r["air_temp"] for r in sample if r["air_temp"] is not None]
        vals_d = [r["dew_point"] for r in sample if r["dew_point"] is not None]
        vals_p = [r["station_pressure"] for r in sample if r["station_pressure"] is not None]
        temp = statistics.mean(vals_t) if vals_t else c["temperature"]
        dew  = statistics.mean(vals_d) if vals_d else c["dewpoint"]
        pres = statistics.mean(vals_p) if vals_p else c["pressure"]
        dew  = _cdp(dew, temp)
        result[(lead, "temperature")] = temp
        result[(lead, "dewpoint")]    = dew
        result[(lead, "pressure")]    = pres
    return _clamp(result)


def _reinterpret_temp(value: float | None, unit: str) -> float | None:
    # takes a value that is actually degrees Celsius and reinterprets its
    # numeral as if it were degrees `unit`, converting back to Celsius under
    # that wrong assumption
    if value is None:
        return None
    if unit == "fahrenheit":
        return (value - 32.0) * 5.0 / 9.0
    if unit == "kelvin":
        return value - 273.15
    if unit == "rankine":
        return (value - 491.67) * 5.0 / 9.0
    raise ValueError(unit)


def _unit_confusion(climos: dict, unit: str) -> dict:
    result = {}
    for lead in LEAD_HOURS:
        c = climos[lead]
        temp = _reinterpret_temp(c["temperature"], unit)
        dew  = _cdp(_reinterpret_temp(c["dewpoint"], unit), temp)
        result[(lead, "temperature")] = temp
        result[(lead, "dewpoint")]    = dew
        result[(lead, "pressure")]    = c["pressure"]
    return _clamp(result)


# rank value: number cards at face, face cards at 10, ace at 1 (low)
_CARD_RANKS = {
    "2": 2, "3": 3, "4": 4, "5": 5, "6": 6, "7": 7, "8": 8, "9": 9, "10": 10,
    "J": 10, "Q": 10, "K": 10, "A": 1,
}
_CARD_SUIT_COLOR = {"hearts": "red", "diamonds": "red", "clubs": "black", "spades": "black"}


def _hand_delta() -> float:
    deck = [(rank, suit) for rank in _CARD_RANKS for suit in _CARD_SUIT_COLOR]
    hand = random.sample(deck, 5)
    total = sum(_CARD_RANKS[rank] for rank, _ in hand)
    reds = sum(1 for _, suit in hand if _CARD_SUIT_COLOR[suit] == "red")
    delta = total / 10.0
    return -delta if reds >= 3 else delta


def _play_cards(climos: dict) -> dict:
    # each lead, each variable draws its own 5-card hand and adds/subtracts
    # the running total -- a card-game random walk
    result = {}
    state = None
    for lead in LEAD_HOURS:
        c = climos[lead]
        if state is None:
            state = {v: c[v] for v in ["temperature", "dewpoint", "pressure"]}
        for var in ["temperature", "dewpoint", "pressure"]:
            prev = state[var]
            val = None if prev is None else prev + _hand_delta()
            state[var] = val
            result[(lead, var)] = val
    return _clamp(result)


def _random_past_obs_n(conn_in, n: int) -> list:
    return conn_in.execute(
        """
        select t.air_temp as temperature, t.dew_point as dewpoint,
               t.station_pressure as pressure
        from tempest_obs t
        join stations s on s.station_id = t.station_id
        where s.source = 'tempest' and t.air_temp is not null
        order by random()
        limit ?
        """,
        (n,),
    ).fetchall()


def _obs_distance(candidate: dict, obs) -> float:
    d = 0.0
    if candidate["temperature"] is not None and obs["air_temp"] is not None:
        d += abs(candidate["temperature"] - obs["air_temp"])
    if candidate["dewpoint"] is not None and obs["dew_point"] is not None:
        d += abs(candidate["dewpoint"] - obs["dew_point"])
    if candidate["pressure"] is not None and obs["station_pressure"] is not None:
        d += abs(candidate["pressure"] - obs["station_pressure"])
    return d


def _bracket_winner(candidates: list, obs) -> dict | None:
    # single-elimination bracket: each round pairs candidates up and keeps
    # whichever of the pair is closer to current conditions; an odd one out
    # gets a bye straight to the next round
    round_ = list(candidates)
    if not round_:
        return None
    while len(round_) > 1:
        next_round = []
        for i in range(0, len(round_) - 1, 2):
            a, b = round_[i], round_[i + 1]
            next_round.append(a if _obs_distance(a, obs) <= _obs_distance(b, obs) else b)
        if len(round_) % 2 == 1:
            next_round.append(round_[-1])
        round_ = next_round
    return round_[0]


def _head_to_head(obs, climos: dict, conn_in) -> dict:
    # worse than an analog day on purpose: 50 random historical obs fight a
    # single-elimination bracket against current conditions, repeated fresh
    # for each lead hour
    result = {}
    for lead in LEAD_HOURS:
        c = climos[lead]
        candidates = [dict(r) for r in _random_past_obs_n(conn_in, 50)]
        winner = _bracket_winner(candidates, obs)
        if winner is not None:
            temp = winner["temperature"]
            result[(lead, "temperature")] = temp
            result[(lead, "dewpoint")]    = _cdp(winner["dewpoint"], temp)
            result[(lead, "pressure")]    = winner["pressure"]
        else:
            result[(lead, "temperature")] = c["temperature"]
            result[(lead, "dewpoint")]    = c["dewpoint"]
            result[(lead, "pressure")]    = c["pressure"]
    return result


def _sine_value(mode: str, current: float | None, lo: float | None, hi: float | None,
                 lead_hours: int) -> float | None:
    if current is None or lo is None or hi is None:
        return None
    amp = (hi - lo) / 2.0
    mid = (hi + lo) / 2.0
    phase = 2 * math.pi * lead_hours / 24.0
    if mode == "zero":
        # starts at current conditions, rises, falls, and returns to current
        # conditions after a full 24h period
        return current + amp * math.sin(phase)
    if mode == "high":
        # starts at the window high, falls, rises, returns to the high
        return mid + amp * math.cos(phase)
    if mode == "low":
        # starts at the window low, rises, falls, returns to the low
        return mid - amp * math.cos(phase)
    raise ValueError(mode)


def _lookback_sine(obs, extremes: dict, mode: str) -> dict:
    # one 24h-period sine (or cosine) wave per variable, amplitude set by the
    # variable's high/low over some lookback window. pressure isn't part of
    # the original idea (only temp/dewpoint were specified) but gets the same
    # treatment for consistency -- every bogo member outputs all 3 variables
    _obs_col = {"temperature": "air_temp", "dewpoint": "dew_point", "pressure": "station_pressure"}
    result = {}
    for lead in LEAD_HOURS:
        for var, col in _obs_col.items():
            if var not in extremes or not extremes[var] or None in extremes[var]:
                result[(lead, var)] = None
                continue
            lo, hi = extremes[var]
            result[(lead, var)] = _sine_value(mode, obs[col], lo, hi, lead)
    for lead in LEAD_HOURS:
        t = result.get((lead, "temperature"))
        result[(lead, "dewpoint")] = _cdp(result.get((lead, "dewpoint")), t)
    return _clamp(result)


def _fast_sine(extremes: dict) -> dict:
    # not an actual sine curve -- each successive lead just alternates
    # between the window high and low
    result = {}
    for i, lead in enumerate(LEAD_HOURS):
        use_high = (i % 2 == 0)
        for var in ["temperature", "dewpoint", "pressure"]:
            if var not in extremes or not extremes[var] or None in extremes[var]:
                result[(lead, var)] = None
                continue
            lo, hi = extremes[var]
            result[(lead, var)] = hi if use_high else lo
    for lead in LEAD_HOURS:
        t = result.get((lead, "temperature"))
        result[(lead, "dewpoint")] = _cdp(result.get((lead, "dewpoint")), t)
    return _clamp(result)


_TRUE_RANDOM_LO_F = -459.67  # absolute zero
_TRUE_RANDOM_HI_F = 60000.0


def _f_to_c(f: float) -> float:
    return (f - 32.0) * 5.0 / 9.0


def _true_random() -> dict:
    result = {}
    for lead in LEAD_HOURS:
        temp = _f_to_c(random.uniform(_TRUE_RANDOM_LO_F, _TRUE_RANDOM_HI_F))
        dew  = _f_to_c(random.uniform(_TRUE_RANDOM_LO_F, _TRUE_RANDOM_HI_F))
        # pressure has no natural Fahrenheit analog -- reuses the same absurd
        # numeric range directly in hPa, since ignoring units is the whole joke
        pres = random.uniform(_TRUE_RANDOM_LO_F, _TRUE_RANDOM_HI_F)
        dew  = _cdp(dew, temp)
        result[(lead, "temperature")] = temp
        result[(lead, "dewpoint")]    = dew
        result[(lead, "pressure")]    = pres
    return _clamp(result)


def _persistence_flicker(obs, climos: dict) -> dict:
    # one chance-of-persistence value is drawn per run and reused for every
    # lead hour; each lead independently rolls against that same chance
    chance = random.random()
    result = {}
    for lead in LEAD_HOURS:
        c = climos[lead]
        if random.random() < chance:
            temp = obs["air_temp"]
            result[(lead, "temperature")] = temp
            result[(lead, "dewpoint")]    = _cdp(obs["dew_point"], temp)
            result[(lead, "pressure")]    = obs["station_pressure"]
        else:
            result[(lead, "temperature")] = c["temperature"]
            result[(lead, "dewpoint")]    = c["dewpoint"]
            result[(lead, "pressure")]    = c["pressure"]
    return result


def _angry_peer_review(other_members: dict) -> dict:
    # same idea as peer-review, but with much wider noise
    noise = {"temperature": 8.0, "dewpoint": 5.0, "pressure": 8.0}
    result = {}
    for lead in LEAD_HOURS:
        for var in ["temperature", "dewpoint", "pressure"]:
            vals = [m[(lead, var)] for m in other_members.values()
                    if (lead, var) in m and m[(lead, var)] is not None]
            if vals:
                mean = statistics.mean(vals)
                result[(lead, var)] = mean + random.uniform(-noise[var], noise[var])
            else:
                result[(lead, var)] = None
    return _clamp(result)


# the "original 26" members, i.e. everything before the extremes-roulette
# family arrived, minus peer-review itself (which is a mean of everyone else
# and would be circular/redundant to fold back in here)
_OG_MEMBER_IDS = [i for i in range(1, 27) if i != 17]


def _og_only(members: dict) -> dict:
    result = {}
    for lead in LEAD_HOURS:
        for var in ["temperature", "dewpoint", "pressure"]:
            vals = [members[i][(lead, var)] for i in _OG_MEMBER_IDS
                    if i in members and (lead, var) in members[i]
                    and members[i][(lead, var)] is not None]
            result[(lead, var)] = statistics.mean(vals) if vals else None
    return _clamp(result)


# every lookback-sine and fast-sine member id (see run() for assignment)
_SINE_MEMBER_IDS = list(range(45, 64))


def _sine_reviewer(members: dict) -> dict:
    result = {}
    for lead in LEAD_HOURS:
        for var in ["temperature", "dewpoint", "pressure"]:
            vals = [members[i][(lead, var)] for i in _SINE_MEMBER_IDS
                    if i in members and (lead, var) in members[i]
                    and members[i][(lead, var)] is not None]
            result[(lead, var)] = statistics.mean(vals) if vals else None
    return _clamp(result)


def _sine_integrator(members: dict) -> dict:
    # literal arithmetic sum rather than a mean: every sine member here
    # samples the same fixed lead hours off a 24h-period sine/cosine curve, so
    # summing the sampled values at each lead is equivalent to superposing the
    # underlying waves and sampling the result -- no continuous math needed
    result = {}
    for lead in LEAD_HOURS:
        for var in ["temperature", "dewpoint", "pressure"]:
            vals = [members[i][(lead, var)] for i in _SINE_MEMBER_IDS
                    if i in members and (lead, var) in members[i]
                    and members[i][(lead, var)] is not None]
            result[(lead, var)] = sum(vals) if vals else None
    return _clamp(result)


def _ensemble_mean(members: dict, member_confidence: dict, weights, current_ts: int) -> tuple[dict, dict]:
    result = {}
    result_confidence = {}
    for lead in LEAD_HOURS:
        valid_at = current_ts + lead * 3600  # compute once per lead, matching this file's own existing convention elsewhere
        for var in ["temperature", "dewpoint", "pressure"]:
            pairs = [(mid, v) for mid, m in members.items() if (v := m.get((lead, var))) is not None]
            if not pairs:
                result[(lead, var)] = (None, None)
                result_confidence[(lead, var)] = None
                continue
            confidences = {mid: member_confidence.get(mid, {}).get((lead, var)) for mid, _ in pairs}
            if weights:
                member_weights = {mid: weights.get((mid, var, lead, _sector(valid_at))) for mid, _ in pairs}
                mean, group_confidence = _confidence.combine_pattern(pairs, member_weights, confidences)
            else:
                mean = sum(v for _, v in pairs) / len(pairs)
                group_confidence = _confidence.average_confidence(list(confidences.values()))
            vals = [v for _, v in pairs]
            spread = statistics.stdev(vals) if len(vals) >= 2 else 0.0
            result[(lead, var)] = (mean, spread)
            result_confidence[(lead, var)] = group_confidence
    # enforce dewpoint <= temperature on mean
    for lead in LEAD_HOURS:
        mean_t, _  = result.get((lead, "temperature"), (None, None))
        mean_d, sd = result.get((lead, "dewpoint"), (None, None))
        if mean_d is not None and mean_t is not None:
            result[(lead, "dewpoint")] = (_cdp(mean_d, mean_t), sd)
    return result, result_confidence


def run(obs, issued_at: int, *, conn_in, conn_out=None, weights=None, member_history=None,
        default_matches=None) -> list[dict]:
    climos  = _precompute_climos(obs, conn_in)
    month   = datetime.datetime.fromtimestamp(obs["timestamp"]).month
    extremes = _seasonal_extremes(conn_in, month)
    record_extremes = _all_time_extremes(conn_in)
    climo_day_extremes = _climo_day_extremes(conn_in, month)

    obs_6h_ago  = db.nearest_tempest_obs(conn_in, obs["timestamp"] - 21600)
    obs_24h_ago = db.nearest_tempest_obs(conn_in, obs["timestamp"] - 86400)
    obs_1yr_ago = db.nearest_tempest_obs(conn_in, obs["timestamp"] - 365 * 86400)
    rand_obs    = _random_past_obs(conn_in)

    now_ts = obs["timestamp"]
    extremes_24h       = _extremes_in_range(conn_in, now_ts - 86400, now_ts)
    extremes_7d        = _extremes_in_range(conn_in, now_ts - 7 * 86400, now_ts)
    extremes_7d_random = _random_window_extremes(conn_in, now_ts, 7 * 86400)
    extremes_100d      = _extremes_in_range(conn_in, now_ts - 100 * 86400, now_ts)
    extremes_alltime   = record_extremes

    mr = {}
    mr[1]  = _drunkard(climos)
    mr[2]  = _blind_drunkard(climos)
    mr[3]  = _chaos(climos)
    mr[4]  = _vibes(climos, extremes)
    mr[5]  = _contrarian(obs, climos, conn_in)
    mr[6]  = _hype_train(obs, climos, obs_6h_ago)
    mr[7]  = _mercury_retrograde(climos, obs["timestamp"])
    mr[8]  = _weatherperson(climos)
    mr[9]  = _crowd_sourced(rand_obs, climos)
    mr[10] = _groundhog_day(obs_24h_ago, climos)
    mr[11] = _cg(obs, climos)
    mr[12] = _climate_anxiety(climos)
    mr[13] = _too_early(obs_6h_ago, climos)
    mr[14] = _monday(climos)
    mr[15] = _grant_funded(climos)
    mr[16] = _the_algorithm(obs, climos)
    mr[18] = _dew_denier(climos)
    mr[19] = _breaking_news(obs, climos, extremes)
    mr[20] = _engagement_bait(climos)
    mr[21] = _both_sides(climos, extremes)
    mr[22] = _sponsored_content()
    mr[23] = _influencer(climos)
    mr[24] = _panic(obs, climos, obs_6h_ago)
    mr[25] = _nostalgia(obs_1yr_ago, climos)
    mr[26] = _astroturfed(climos, issued_at)
    mr[27] = _record_breaker(obs, climos, conn_in)
    mr[28] = _clamp(_extremes_roulette(record_extremes))
    mr[29] = _clamp(_extremes_climb(record_extremes))
    mr[30] = _clamp(_extremes_drop(record_extremes))
    mr[31] = _clamp(_extremes_roulette(climo_day_extremes))
    mr[32] = _clamp(_extremes_climb(climo_day_extremes))
    mr[33] = _clamp(_extremes_drop(climo_day_extremes))
    mr[34] = _mirror_time(obs, conn_in)
    mr[35] = _random_aggregate(conn_in, climos, random.randint(5, 100))
    mr[36] = _random_aggregate(conn_in, climos, random.randint(5, 100))
    mr[37] = _random_aggregate(conn_in, climos, random.randint(5, 100))
    mr[38] = _random_aggregate(conn_in, climos, random.randint(5, 100))
    mr[39] = _random_aggregate(conn_in, climos, random.randint(5, 100))
    mr[40] = _unit_confusion(climos, "fahrenheit")
    mr[41] = _unit_confusion(climos, "kelvin")
    mr[42] = _unit_confusion(climos, "rankine")
    mr[43] = _play_cards(climos)
    mr[44] = _head_to_head(obs, climos, conn_in)
    mr[45] = _lookback_sine(obs, extremes_24h, "zero")
    mr[46] = _lookback_sine(obs, extremes_24h, "high")
    mr[47] = _lookback_sine(obs, extremes_24h, "low")
    mr[48] = _lookback_sine(obs, extremes_7d, "zero")
    mr[49] = _lookback_sine(obs, extremes_7d, "high")
    mr[50] = _lookback_sine(obs, extremes_7d, "low")
    mr[51] = _lookback_sine(obs, extremes_7d_random, "zero")
    mr[52] = _lookback_sine(obs, extremes_7d_random, "high")
    mr[53] = _lookback_sine(obs, extremes_7d_random, "low")
    mr[54] = _lookback_sine(obs, extremes_100d, "zero")
    mr[55] = _lookback_sine(obs, extremes_100d, "high")
    mr[56] = _lookback_sine(obs, extremes_100d, "low")
    mr[57] = _lookback_sine(obs, extremes_alltime, "zero")
    mr[58] = _lookback_sine(obs, extremes_alltime, "high")
    mr[59] = _lookback_sine(obs, extremes_alltime, "low")
    mr[60] = _fast_sine(extremes_24h)
    mr[61] = _fast_sine(extremes_7d)
    mr[62] = _fast_sine(extremes_100d)
    mr[63] = _fast_sine(extremes_alltime)
    mr[64] = _true_random()
    mr[65] = _persistence_flicker(obs, climos)
    mr[17] = _peer_review(mr)  # depends on all others
    mr[66] = _angry_peer_review(mr)
    mr[67] = _og_only(mr)
    mr[68] = _sine_reviewer(mr)
    mr[69] = _sine_integrator(mr)

    member_confidence = {
        mid: {
            (lead, var): _confidence.confidence_for_cell(
                (member_history or {}).get(mid, []), var, lead, (default_matches or [])
            )
            for lead in LEAD_HOURS for var in ["temperature", "dewpoint", "pressure"]
        }
        for mid in mr
    }

    mean_data, mean_confidence = _ensemble_mean(mr, member_confidence, weights, obs["timestamp"])

    self_correction_confidence = {
        (lead, var): _confidence.confidence_for_cell(
            (member_history or {}).get(_SELF_CORRECTION_MEMBER, []), var, lead, (default_matches or [])
        )
        for lead in LEAD_HOURS for var in ["temperature", "dewpoint", "pressure"]
    }

    rows = []

    for member_id, forecasts in mr.items():
        for lead in LEAD_HOURS:
            valid_at = obs["timestamp"] + lead * 3600
            for var in ["temperature", "dewpoint", "pressure"]:
                rows.append({
                    "model_id":  MODEL_ID,
                    "model":     MODEL_NAME,
                    "member_id": member_id,
                    "issued_at": issued_at,
                    "valid_at":  valid_at,
                    "lead_hours": lead,
                    "variable":  var,
                    "value":     forecasts.get((lead, var)),
                    "confidence": member_confidence[member_id][(lead, var)],
                })

    for lead in LEAD_HOURS:
        valid_at = obs["timestamp"] + lead * 3600
        for var in ["temperature", "dewpoint", "pressure"]:
            mean, spread = mean_data.get((lead, var), (None, None))
            rows.append({
                "model_id":  MODEL_ID,
                "model":     MODEL_NAME,
                "member_id": 0,
                "issued_at": issued_at,
                "valid_at":  valid_at,
                "lead_hours": lead,
                "variable":  var,
                "value":     mean,
                "spread":    spread,
                "confidence": mean_confidence.get((lead, var)),
            })

            corrected = _self_correction.corrected_value(conn_out, MODEL_ID, var, lead, mean, issued_at)
            rows.append({
                "model_id":  MODEL_ID,
                "model":     MODEL_NAME,
                "member_id": _SELF_CORRECTION_MEMBER,
                "issued_at": issued_at,
                "valid_at":  valid_at,
                "lead_hours": lead,
                "variable":  var,
                "value":     corrected,
                "confidence": self_correction_confidence.get((lead, var)),
            })

    return rows
