# external_corrected: bias-corrected NWS and Tempest forecast.
# learns systematic error patterns from historical scored rows and applies
# corrections conditioned on time-of-day, season, and current airmass state.
# type='external' — excluded from the barogram ensemble, for comparison only.
#
# members 1-5: nws + flat/diurnal/seasonal/airmass/joint correction
# members 6-10: tempest_forecast + same five correction strategies
# member 0: ensemble mean of all members that produced values

import datetime
import statistics
from collections import defaultdict

import db
from models.nws import _fetch as _fetch_nws, _nearest as _snap_nearest
from models.tempest_forecast import _fetch as _fetch_tempest
from models.surface_signs import _find_nearest_ts

MODEL_ID = 202
MODEL_NAME = "external_corrected"
NEEDS_CONN_IN = True
# NEEDS_CONN_OUT used here to read historical scored external forecasts — an exception to
# the convention that only the ensemble model uses this flag.
NEEDS_CONN_OUT = True
NEEDS_CONF = True

LEAD_HOURS = [6, 12, 18, 24]
_VARIABLES = ["temperature", "dewpoint", "precip_prob"]
_MIN_SAMPLES = 3
_OBS_WINDOW = 600  # ±10 min for matching historical obs to issued_at
_NWS_MEMBERS = frozenset({1, 2, 3, 4, 5})
_TEMPEST_MEMBERS = frozenset({6, 7, 8, 9, 10})
_SOURCE_FLOOR = 0.10  # minimum weight fraction for the weaker source

_SEASON_MAP = {12: 0, 1: 0, 2: 0, 3: 1, 4: 1, 5: 1,
               6: 2, 7: 2, 8: 2, 9: 3, 10: 3, 11: 3}

# (member_id, source_model_name, conditioning_strategy)
_MEMBERS = [
    (1, "nws", "flat"),
    (2, "nws", "diurnal"),
    (3, "nws", "seasonal"),
    (4, "nws", "airmass"),
    (5, "nws", "joint"),
    (6, "tempest_forecast", "flat"),
    (7, "tempest_forecast", "diurnal"),
    (8, "tempest_forecast", "seasonal"),
    (9, "tempest_forecast", "airmass"),
    (10, "tempest_forecast", "joint"),
]


def _hour_bucket(ts: int) -> int:
    return datetime.datetime.fromtimestamp(ts).hour // 6


def _season(ts: int) -> int:
    return _SEASON_MAP[datetime.datetime.fromtimestamp(ts).month]


def _airmass_cat(obs) -> str | None:
    if obs is None:
        return None
    t = obs["air_temp"]
    d = obs["dew_point"]
    if t is None or d is None:
        return None
    spread = t - d
    if spread < 3.0:
        return "moist"
    if spread > 8.0:
        return "dry"
    return "moderate"


def _load_errors(conn_out, model_name: str) -> list:
    return conn_out.execute(
        """
        select issued_at, valid_at, lead_hours, variable, error
        from forecasts
        where model = ? and scored_at is not null and member_id = 0
          and error is not null
        """,
        (model_name,),
    ).fetchall()


def _build_tables(errors: list, obs_by_ts: dict, sorted_ts: list) -> dict:
    flat = defaultdict(list)
    diurnal = defaultdict(list)
    seasonal = defaultdict(list)
    airmass = defaultdict(list)
    joint = defaultdict(list)

    for row in errors:
        issued_at = row["issued_at"]
        valid_at = row["valid_at"]
        lead = row["lead_hours"]
        variable = row["variable"]
        error = row["error"]
        hb = _hour_bucket(valid_at)
        sea = _season(valid_at)

        flat[(variable, lead)].append(error)
        diurnal[(variable, lead, hb)].append(error)
        seasonal[(variable, lead, sea)].append(error)
        joint[(variable, lead, hb, sea)].append(error)

        nearest_ts = _find_nearest_ts(sorted_ts, issued_at, max_delta=_OBS_WINDOW)
        if nearest_ts is not None:
            cat = _airmass_cat(obs_by_ts[nearest_ts])
            if cat is not None:
                airmass[(variable, lead, cat)].append(error)

    def to_means(d):
        return {k: statistics.mean(v) for k, v in d.items() if len(v) >= _MIN_SAMPLES}

    return {
        "flat": to_means(flat),
        "diurnal": to_means(diurnal),
        "seasonal": to_means(seasonal),
        "airmass": to_means(airmass),
        "joint": to_means(joint),
    }


def _get_correction(t: dict, cond: str, variable: str, lead: int,
                    hb: int, sea: int, airmass_cat: str | None) -> float:
    flat = t["flat"].get((variable, lead), 0.0)
    if cond == "flat":
        return flat
    if cond == "diurnal":
        return t["diurnal"].get((variable, lead, hb), flat)
    if cond == "seasonal":
        return t["seasonal"].get((variable, lead, sea), flat)
    if cond == "airmass":
        if airmass_cat is None:
            return flat
        return t["airmass"].get((variable, lead, airmass_cat), flat)
    if cond == "joint":
        c = t["joint"].get((variable, lead, hb, sea))
        if c is None:
            c = t["diurnal"].get((variable, lead, hb), flat)
        return c
    return 0.0


def _load_group_mae(conn_out) -> tuple[dict, dict]:
    """Return (nws_mae, tempest_mae) keyed by (variable, lead_hours, hour_bucket)."""
    rows = conn_out.execute(
        """
        select
            case when member_id between 1 and 5 then 'nws' else 'tempest' end as src,
            variable,
            lead_hours,
            cast(strftime('%H', datetime(valid_at, 'unixepoch', 'localtime'))
                 as integer) / 6 as hour_bucket,
            avg(mae) as avg_mae,
            count(*) as n
        from forecasts
        where model_id = ? and member_id between 1 and 10
          and scored_at is not null and mae is not null
        group by
            case when member_id between 1 and 5 then 'nws' else 'tempest' end,
            variable, lead_hours, hour_bucket
        """,
        (MODEL_ID,),
    ).fetchall()

    nws_mae: dict = {}
    tempest_mae: dict = {}
    for row in rows:
        if row["n"] < _MIN_SAMPLES:
            continue
        key = (row["variable"], row["lead_hours"], row["hour_bucket"])
        if row["src"] == "nws":
            nws_mae[key] = row["avg_mae"]
        else:
            tempest_mae[key] = row["avg_mae"]
    return nws_mae, tempest_mae


def _source_weights(
    nws_mae: dict, tempest_mae: dict, variable: str, lead: int, hb: int
) -> tuple[float, float]:
    """Return (nws_w, tempest_w) via inverse-MAE weighting with a floor."""
    key = (variable, lead, hb)
    nm = nws_mae.get(key)
    tm = tempest_mae.get(key)
    if nm is None and tm is None:
        return 0.5, 0.5
    if nm is None:
        return 0.0, 1.0
    if tm is None:
        return 1.0, 0.0
    ni = 1.0 / nm if nm > 0 else 1e9
    ti = 1.0 / tm if tm > 0 else 1e9
    total = ni + ti
    nw, tw = ni / total, ti / total
    if nw < _SOURCE_FLOOR:
        nw = _SOURCE_FLOOR
        tw = 1.0 - nw
    elif tw < _SOURCE_FLOOR:
        tw = _SOURCE_FLOOR
        nw = 1.0 - tw
    return nw, tw


def _make_row(member_id: int, lead: int, valid_at: int, variable: str,
              value: float | None, issued_at: int) -> dict:
    return {
        "model_id": MODEL_ID,
        "model": MODEL_NAME,
        "member_id": member_id,
        "issued_at": issued_at,
        "valid_at": valid_at,
        "lead_hours": lead,
        "variable": variable,
        "value": value,
    }


def run(obs, issued_at: int, *, conn_in, conn_out, conf) -> list[dict]:
    all_obs = db.tempest_obs_in_range(conn_in, 0, issued_at)
    obs_by_ts = {r["timestamp"]: r for r in all_obs}
    sorted_ts = sorted(obs_by_ts)

    nws_errors = _load_errors(conn_out, "nws")
    tempest_errors = _load_errors(conn_out, "tempest_forecast")
    tables = {
        "nws": _build_tables(nws_errors, obs_by_ts, sorted_ts),
        "tempest_forecast": _build_tables(tempest_errors, obs_by_ts, sorted_ts),
    }
    nws_mae, tempest_mae = _load_group_mae(conn_out)

    location = db.tempest_station_location(conn_in)
    nws_hourly = _fetch_nws(location[0], location[1]) if location else {}
    tempest_hourly = {}
    if conf and conf.tempest_token and conf.tempest_station_id:
        tempest_hourly = _fetch_tempest(conf.tempest_station_id, conf.tempest_token)

    current_airmass = _airmass_cat(obs)

    rows = []
    for lead in LEAD_HOURS:
        valid_at = issued_at + lead * 3600
        hb = _hour_bucket(valid_at)
        sea = _season(valid_at)

        source_entries = {
            "nws": _snap_nearest(nws_hourly, valid_at),
            "tempest_forecast": _snap_nearest(tempest_hourly, valid_at),
        }

        for variable in _VARIABLES:
            nws_vals = []
            tempest_vals = []
            for member_id, source, cond in _MEMBERS:
                entry = source_entries[source]
                raw_val = entry.get(variable) if entry else None
                if raw_val is None:
                    continue
                correction = _get_correction(
                    tables[source], cond, variable, lead, hb, sea, current_airmass
                )
                corrected = raw_val - correction
                rows.append(_make_row(member_id, lead, valid_at, variable, corrected, issued_at))
                if member_id in _NWS_MEMBERS:
                    nws_vals.append(corrected)
                else:
                    tempest_vals.append(corrected)

            nws_mean = statistics.mean(nws_vals) if nws_vals else None
            tempest_mean = statistics.mean(tempest_vals) if tempest_vals else None
            if nws_mean is not None and tempest_mean is not None:
                nws_w, tempest_w = _source_weights(nws_mae, tempest_mae, variable, lead, hb)
                mean_val = nws_w * nws_mean + tempest_w * tempest_mean
            elif nws_mean is not None:
                mean_val = nws_mean
            elif tempest_mean is not None:
                mean_val = tempest_mean
            else:
                mean_val = None
            rows.append(_make_row(0, lead, valid_at, variable, mean_val, issued_at))

    return rows
