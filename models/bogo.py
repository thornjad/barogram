# bogo: 27-member silly forecast ensemble.
# Each member uses a different flavor of wrongness.
# member_id=0 is the ensemble mean+spread; members 1-27 are named.
# Scored purely for entertainment; expected to perform poorly.

import datetime
import random
import statistics

import db

MODEL_ID = 12
MODEL_NAME = "bogo"
NEEDS_CONN_IN = True

LEAD_HOURS = [6, 12, 18, 24]
MIN_OBS = 30

_STEP = {
    "temperature": 5.0,
    "dewpoint": 3.0,
    "pressure": 3.0,
    "precip_prob": 0.20,
}

# hardcoded world records — we don't care if they're updated
_WORLD_RECORDS = {
    "temperature": (-89.2, 56.7),   # Vostok 1983, Death Valley 2013
    "dewpoint":    (-60.0, 35.5),   # dry arctic, Dhahran SA 2003
    "pressure":    (870.0, 1083.8), # Typhoon Tip 1979, Tosontsengel 2001
    "precip_prob": (0.0,   1.0),
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

# (temp_offset_°C, precip_offset) by weekday (0=Monday)
_DAY_BIAS = {
    0: (-2.0,  0.15),
    1: (-1.0,  0.05),
    2: ( 0.0,  0.00),
    3: ( 0.5, -0.05),
    4: ( 2.0, -0.15),
    5: ( 0.0,  0.00),
    6: ( 0.0,  0.00),
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
        pp = db.climo_precip_probability(conn_in, t.month, t.hour, MIN_OBS)
        climos[lead] = {
            "temperature": means.get("temperature"),
            "dewpoint":    means.get("dewpoint"),
            "pressure":    means.get("pressure"),
            "precip_prob": pp if pp is not None else 0.1,
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
            state = {v: c[v] for v in ["temperature", "dewpoint", "pressure", "precip_prob"]}
        for var, bound in _STEP.items():
            prev = state[var]
            if var == "precip_prob":
                val = None if prev is None else _c(prev + random.uniform(-bound, bound), 0.0, 1.0)
            else:
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
            if var == "precip_prob":
                val = None if base is None else _c(base + random.uniform(-bound, bound), 0.0, 1.0)
            else:
                val = None if base is None else base + random.uniform(-bound, bound)
            result[(lead, var)] = val
    return _clamp(result)


def _chaos(climos: dict) -> dict:
    result = {}
    state = None
    for lead in LEAD_HOURS:
        c = climos[lead]
        if state is None:
            state = {v: c[v] for v in ["temperature", "dewpoint", "pressure", "precip_prob"]}
        for var, bound in _STEP.items():
            b3 = bound * 3.0
            prev = state[var]
            if var == "precip_prob":
                val = None if prev is None else _c(prev + random.uniform(-b3, b3), 0.0, 1.0)
            else:
                val = None if prev is None else prev + random.uniform(-b3, b3)
            state[var] = val
            result[(lead, var)] = val
    return _clamp(result)


def _vibes(climos: dict, extremes: dict) -> dict:
    result = {}
    for lead in LEAD_HOURS:
        c = climos[lead]
        for var in ["temperature", "dewpoint", "pressure", "precip_prob"]:
            if var == "precip_prob":
                val = random.random()
            elif var in extremes and extremes[var] and None not in extremes[var]:
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
        result[(lead, "precip_prob")] = c["precip_prob"]
    return _clamp(result)


def _hype_train(obs, climos: dict, obs_6h_ago) -> dict:
    if obs_6h_ago is None:
        return {(lead, var): climos[lead][var] for lead in LEAD_HOURS
                for var in ["temperature", "dewpoint", "pressure", "precip_prob"]}
    trends = {
        "temperature": (obs["air_temp"] or 0.0) - (obs_6h_ago["air_temp"] or obs["air_temp"] or 0.0),
        "dewpoint":    (obs["dew_point"] or 0.0) - (obs_6h_ago["dew_point"] or obs["dew_point"] or 0.0),
        "pressure":    (obs["station_pressure"] or 0.0) - (obs_6h_ago["station_pressure"] or obs["station_pressure"] or 0.0),
    }
    result = {}
    for i, lead in enumerate(LEAD_HOURS):
        c = climos[lead]
        mult = i + 1
        for var in ["temperature", "dewpoint", "pressure"]:
            base = obs["air_temp" if var == "temperature" else "dew_point" if var == "dewpoint" else "station_pressure"]
            jitter = random.uniform(-0.3, 0.3) * _STEP[var]
            result[(lead, var)] = None if base is None else base + trends[var] * mult + jitter
        result[(lead, "precip_prob")] = c["precip_prob"]
    return _clamp(result)


def _mercury_retrograde(climos: dict, ts: int) -> dict:
    mult = 10.0 if _is_mercury_retrograde(ts) else 0.5
    result = {}
    state = None
    for lead in LEAD_HOURS:
        c = climos[lead]
        if state is None:
            state = {v: c[v] for v in ["temperature", "dewpoint", "pressure", "precip_prob"]}
        for var, bound in _STEP.items():
            bm = bound * mult
            prev = state[var]
            if var == "precip_prob":
                val = None if prev is None else _c(prev + random.uniform(-bm, bm), 0.0, 1.0)
            else:
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
        result[(lead, "precip_prob")] = 0.30
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
        result[(lead, "precip_prob")] = random.random()
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
        result[(lead, "precip_prob")] = c["precip_prob"]
    return result


def _cg(obs, climos: dict) -> dict:
    has_lightning = (obs["lightning_count"] or 0) > 0
    result = {}
    for lead in LEAD_HOURS:
        c = climos[lead]
        result[(lead, "temperature")] = c["temperature"]
        result[(lead, "dewpoint")]    = c["dewpoint"]
        result[(lead, "pressure")]    = c["pressure"]
        result[(lead, "precip_prob")] = 1.0 if has_lightning else c["precip_prob"]
    return result


def _climate_anxiety(climos: dict) -> dict:
    result = {}
    for lead in LEAD_HOURS:
        c = climos[lead]
        temp = None if c["temperature"] is None else c["temperature"] + 3.0
        dew  = None if c["dewpoint"] is None else _cdp(c["dewpoint"] + 3.0, temp)
        pp   = None if c["precip_prob"] is None else _c(c["precip_prob"] * 1.1, 0.0, 1.0)
        result[(lead, "temperature")] = temp
        result[(lead, "dewpoint")]    = dew
        result[(lead, "pressure")]    = c["pressure"]
        result[(lead, "precip_prob")] = pp
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
        result[(lead, "precip_prob")] = c["precip_prob"]
    return result


def _monday(climos: dict) -> dict:
    result = {}
    for lead in LEAD_HOURS:
        c = climos[lead]
        weekday = datetime.datetime.fromtimestamp(c["valid_at"]).weekday()
        temp_off, precip_off = _DAY_BIAS.get(weekday, (0.0, 0.0))
        temp = None if c["temperature"] is None else c["temperature"] + temp_off
        dew  = None if c["dewpoint"] is None else _cdp(c["dewpoint"] + temp_off * 0.5, temp)
        pp   = _c((c["precip_prob"] or 0.1) + precip_off, 0.0, 1.0)
        result[(lead, "temperature")] = temp
        result[(lead, "dewpoint")]    = dew
        result[(lead, "pressure")]    = c["pressure"]
        result[(lead, "precip_prob")] = pp
    return result


def _grant_funded(climos: dict) -> dict:
    result = {}
    state = None
    for lead in LEAD_HOURS:
        c = climos[lead]
        if state is None:
            state = {v: c[v] for v in ["temperature", "dewpoint", "pressure", "precip_prob"]}
        for var, bound in _STEP.items():
            if random.random() < 0.20:
                state[var] = None
                result[(lead, var)] = None
                continue
            prev = state[var]
            bh = bound * 0.5
            if var == "precip_prob":
                val = None if prev is None else _c(prev + random.uniform(-bh, bh), 0.0, 1.0)
            else:
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
        pp   = _c((c["precip_prob"] or 0.1) + (0.3 if direction < 0 else -0.3), 0.0, 1.0)
        dew  = _cdp(dew, temp)
        result[(lead, "temperature")] = temp
        result[(lead, "dewpoint")]    = dew
        result[(lead, "pressure")]    = pres
        result[(lead, "precip_prob")] = pp
    return _clamp(result)


def _peer_review(other_members: dict) -> dict:
    noise = {"temperature": 0.5, "dewpoint": 0.3, "pressure": 0.5, "precip_prob": 0.03}
    result = {}
    for lead in LEAD_HOURS:
        for var in ["temperature", "dewpoint", "pressure", "precip_prob"]:
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
        result[(lead, "precip_prob")] = c["precip_prob"]
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
        temp_obs   = obs["air_temp"] or c6["temperature"] or 0.0
        temp_climo = c6["temperature"] or temp_obs
        result[(lead, "precip_prob")] = 1.0 if temp_obs < temp_climo else 0.0
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
        result[(lead, "precip_prob")] = 0.51
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
        result[(lead, "precip_prob")] = 0.0 if is_hot else 1.0
    return _clamp(result)


def _sponsored_content() -> dict:
    result = {}
    for lead in LEAD_HOURS:
        result[(lead, "temperature")] = 22.0
        result[(lead, "dewpoint")]    = 15.0
        result[(lead, "pressure")]    = 1013.0
        result[(lead, "precip_prob")] = 0.0
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
            pp   = 0.0
        else:
            temp = None if c["temperature"] is None else c["temperature"] - 6.0
            dew  = None if c["dewpoint"] is None else c["dewpoint"] + 2.0
            pres = None if c["pressure"] is None else c["pressure"] - 15.0
            pp   = 0.85
        dew = _cdp(dew, temp)
        result[(lead, "temperature")] = temp
        result[(lead, "dewpoint")]    = dew
        result[(lead, "pressure")]    = pres
        result[(lead, "precip_prob")] = pp
    return _clamp(result)


def _panic(obs, climos: dict, obs_6h_ago) -> dict:
    if (obs_6h_ago is None
            or obs["station_pressure"] is None
            or obs_6h_ago["station_pressure"] is None):
        return {(lead, var): climos[lead][var] for lead in LEAD_HOURS
                for var in ["temperature", "dewpoint", "pressure", "precip_prob"]}
    trend = obs["station_pressure"] - obs_6h_ago["station_pressure"]
    result = {}
    for lead in LEAD_HOURS:
        c = climos[lead]
        if trend < 0:
            temp = None if c["temperature"] is None else c["temperature"] - 15.0
            dew  = None if c["dewpoint"] is None else c["dewpoint"] + 5.0
            pres = None if c["pressure"] is None else c["pressure"] - 25.0
            pp   = 1.0
        elif trend > 0:
            temp = None if c["temperature"] is None else c["temperature"] + 15.0
            dew  = None if c["dewpoint"] is None else c["dewpoint"] - 5.0
            pres = None if c["pressure"] is None else c["pressure"] + 20.0
            pp   = 0.0
        else:
            temp = c["temperature"]
            dew  = c["dewpoint"]
            pres = c["pressure"]
            pp   = c["precip_prob"]
        dew = _cdp(dew, temp)
        result[(lead, "temperature")] = temp
        result[(lead, "dewpoint")]    = dew
        result[(lead, "pressure")]    = pres
        result[(lead, "precip_prob")] = pp
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
        result[(lead, "precip_prob")] = c["precip_prob"]
    return result


def _astroturfed(climos: dict, issued_at: int) -> dict:
    months_elapsed = (issued_at - _ASTROTURFED_EPOCH) / (30 * 86400)
    drift = months_elapsed * 0.1
    result = {}
    for lead in LEAD_HOURS:
        c = climos[lead]
        temp = None if c["temperature"] is None else c["temperature"] + drift
        dew  = None if c["dewpoint"] is None else _cdp(c["dewpoint"] + drift * 0.8, temp)
        pp   = None if c["precip_prob"] is None else _c(c["precip_prob"] + drift * 0.005, 0.0, 1.0)
        result[(lead, "temperature")] = temp
        result[(lead, "dewpoint")]    = dew
        result[(lead, "pressure")]    = c["pressure"]
        result[(lead, "precip_prob")] = pp
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
        pres_obs   = obs["station_pressure"]
        pres_climo = now_means.get("pressure")
        if pres_obs is None or pres_climo is None:
            result[(lead, "precip_prob")] = c["precip_prob"]
        elif pres_obs < pres_climo:
            result[(lead, "precip_prob")] = 1.0
        else:
            result[(lead, "precip_prob")] = 0.0
    return _clamp(result)


def _ensemble_mean(members: dict) -> dict:
    result = {}
    for lead in LEAD_HOURS:
        for var in ["temperature", "dewpoint", "pressure", "precip_prob"]:
            vals = [m[(lead, var)] for m in members.values()
                    if (lead, var) in m and m[(lead, var)] is not None]
            if vals:
                mean   = statistics.mean(vals)
                spread = statistics.stdev(vals) if len(vals) >= 2 else 0.0
            else:
                mean   = None
                spread = None
            result[(lead, var)] = (mean, spread)
    # enforce dewpoint <= temperature on mean
    for lead in LEAD_HOURS:
        mean_t, _  = result.get((lead, "temperature"), (None, None))
        mean_d, sd = result.get((lead, "dewpoint"), (None, None))
        if mean_d is not None and mean_t is not None:
            result[(lead, "dewpoint")] = (_cdp(mean_d, mean_t), sd)
    return result


def run(obs, issued_at: int, *, conn_in) -> list[dict]:
    climos  = _precompute_climos(obs, conn_in)
    month   = datetime.datetime.fromtimestamp(obs["timestamp"]).month
    extremes = _seasonal_extremes(conn_in, month)

    obs_6h_ago  = db.nearest_tempest_obs(conn_in, obs["timestamp"] - 21600)
    obs_24h_ago = db.nearest_tempest_obs(conn_in, obs["timestamp"] - 86400)
    obs_1yr_ago = db.nearest_tempest_obs(conn_in, obs["timestamp"] - 365 * 86400)
    rand_obs    = _random_past_obs(conn_in)

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
    mr[17] = _peer_review(mr)  # depends on all others

    mean_data = _ensemble_mean(mr)

    rows = []

    for member_id, forecasts in mr.items():
        for lead in LEAD_HOURS:
            valid_at = obs["timestamp"] + lead * 3600
            for var in ["temperature", "dewpoint", "pressure", "precip_prob"]:
                rows.append({
                    "model_id":  MODEL_ID,
                    "model":     MODEL_NAME,
                    "member_id": member_id,
                    "issued_at": issued_at,
                    "valid_at":  valid_at,
                    "lead_hours": lead,
                    "variable":  var,
                    "value":     forecasts.get((lead, var)),
                })

    for lead in LEAD_HOURS:
        valid_at = obs["timestamp"] + lead * 3600
        for var in ["temperature", "dewpoint", "pressure", "precip_prob"]:
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
            })

    return rows
