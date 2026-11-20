"""history_fill.py: a member's scored history gets synthetic rows at the lead hours it never
forecast, by straight-line interpolation between the neighbouring 0/6/12/18/24 forecasts of
the same run, with lead 0 anchored to the observation at issue time."""
import models._confidence as _confidence
import history_fill
from tests.conftest import make_input_db, make_output_db

_T0 = 1_700_000_000  # issue time
_MODEL = 1
_VAR = "temperature"


def _obs(conn_in, ts, air_temp, dew_point=None, pressure=None):
    conn_in.execute(
        "insert into tempest_obs (station_id, timestamp, air_temp, dew_point, station_pressure)"
        " values ('KTEST', ?, ?, ?, ?)", (ts, air_temp, dew_point, pressure))


def _hourly_obs(conn_in, issued, temps):
    """temps[h] is the air_temp observed h hours after issue."""
    for h, t in enumerate(temps):
        _obs(conn_in, issued + h * 3600, t)


def _forecast(conn_out, issued, lead, value, observed, *, pruned=False, var=_VAR, member=0):
    error = value - observed
    conn_out.execute(
        "insert into forecasts (model_id, model, member_id, issued_at, valid_at, lead_hours,"
        " variable, value, observed, error, mae, scored_at) values (?, 'm', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (_MODEL, member, issued, issued + lead * 3600, lead, var,
         None if pruned else value, None if pruned else observed, error, abs(error), issued + 99999))


def _hist(conn_in, conn_out, member=0):
    obs = history_fill.ObsIndex.load(conn_in, _T0 - 86400)
    return history_fill.filled_history(conn_out, _MODEL, member, 0, obs)


def _maes(hist, lead, var=_VAR):
    cell = hist.cell(var, lead)
    return None if cell is None else sorted(cell[1].tolist())


def _setup(lead6_value=16.0, obs_path=None):
    """One run at _T0: obs path (default 10, 11, ... 28 degrees), lead 6 forecast at 16."""
    conn_in, conn_out = make_input_db(), make_output_db()
    path = obs_path or [10.0 + h for h in range(25)]
    _hourly_obs(conn_in, _T0, path)
    _forecast(conn_out, _T0, 6, lead6_value, path[6])
    return conn_in, conn_out, path


def test_leads_below_six_interpolate_from_the_observation_at_issue_time():
    conn_in, conn_out, path = _setup(lead6_value=16.0, obs_path=[10, 10, 10, 13, 10, 10, 10] + [10] * 18)
    hist = _hist(conn_in, conn_out)
    # forecast line goes 10 (now) to 16 (+6h): +3h forecast is 13, observed 13 -> error 0
    assert _maes(hist, 3) == [0.0]
    # +1h forecast 11, observed 10 -> error 1
    assert _maes(hist, 1) == [1.0]


def test_leads_between_anchors_interpolate_between_neighbouring_forecasts():
    conn_in, conn_out, path = _setup(obs_path=[0.0] * 25)
    _forecast(conn_out, _T0, 12, 22.0, 0.0)
    hist = _hist(conn_in, conn_out)
    # line from 16 at +6h to 22 at +12h: +9h is 19, observed 0
    assert _maes(hist, 9) == [19.0]
    assert _maes(hist, 7) == [17.0]


def test_anchor_leads_are_never_synthesized():
    conn_in, conn_out, _ = _setup()
    _forecast(conn_out, _T0, 12, 20.0, 22.0)
    hist = _hist(conn_in, conn_out)
    assert len(_maes(hist, 6)) == 1  # only the real row
    assert len(_maes(hist, 12)) == 1
    assert hist.cell(_VAR, 24) is None  # no real or synthetic row at 24


def test_a_real_hourly_row_is_kept_and_not_duplicated():
    conn_in, conn_out, path = _setup(obs_path=[0.0] * 25)
    _forecast(conn_out, _T0, 3, 5.0, 0.0)
    hist = _hist(conn_in, conn_out)
    assert _maes(hist, 3) == [5.0]  # real error, not the synthetic 8.0 from the 0 to 16 line
    assert len(_maes(hist, 2)) == 1  # neighbours still filled


def test_pruned_values_are_recovered_from_error_and_observation():
    conn_in, conn_out = make_input_db(), make_output_db()
    _hourly_obs(conn_in, _T0, [10.0] * 25)
    _forecast(conn_out, _T0, 6, 16.0, 10.0, pruned=True)
    hist = _hist(conn_in, conn_out)
    # recovered value is error + observed = 6 + 10 = 16; +3h line is 13 against observed 10
    assert _maes(hist, 3) == [3.0]


def test_no_synthetic_row_without_both_anchors():
    conn_in, conn_out, _ = _setup()
    hist = _hist(conn_in, conn_out)
    for lead in range(7, 12):
        assert hist.cell(_VAR, lead) is None  # no lead 12 forecast, so nothing to interpolate to


def test_no_lead_zero_observation_means_no_short_leads():
    conn_in, conn_out = make_input_db(), make_output_db()
    for h in range(1, 25):
        _obs(conn_in, _T0 + h * 3600, 10.0)  # nothing within 30 min of issue time
    _forecast(conn_out, _T0, 6, 16.0, 10.0)
    hist = _hist(conn_in, conn_out)
    for lead in range(1, 6):
        assert hist.cell(_VAR, lead) is None


def test_no_observation_at_the_target_time_means_no_synthetic_row():
    conn_in, conn_out = make_input_db(), make_output_db()
    _obs(conn_in, _T0, 10.0)
    _obs(conn_in, _T0 + 6 * 3600, 10.0)
    _forecast(conn_out, _T0, 6, 16.0, 10.0)
    hist = _hist(conn_in, conn_out)
    for lead in range(1, 6):
        assert hist.cell(_VAR, lead) is None  # no observation near the target time to score against


def test_variables_and_members_stay_separate():
    conn_in, conn_out, _ = _setup(obs_path=[0.0] * 25)
    conn_in.execute("update tempest_obs set dew_point = 1.0, station_pressure = 1000.0")
    _forecast(conn_out, _T0, 6, 5.0, 1.0, var="dewpoint")
    _forecast(conn_out, _T0, 6, 1004.0, 1000.0, var="pressure", member=1)
    hist0 = _hist(conn_in, conn_out, member=0)
    assert hist0.cell("dewpoint", 3) is not None
    assert hist0.cell("pressure", 3) is None  # that row belongs to member 1
    hist1 = _hist(conn_in, conn_out, member=1)
    assert hist1.cell("pressure", 3) is not None
    # pressure +3h: line 1000 -> 1004 is 1002 against observed 1000
    assert _maes(hist1, 3, "pressure") == [2.0]


def test_runs_are_interpolated_independently():
    conn_in, conn_out = make_input_db(), make_output_db()
    t1 = _T0 + 86400
    for t in (_T0, t1):
        _hourly_obs(conn_in, t, [0.0] * 25)
    _forecast(conn_out, _T0, 6, 6.0, 0.0)
    _forecast(conn_out, t1, 6, 12.0, 0.0)
    hist = _hist(conn_in, conn_out)
    assert _maes(hist, 3) == [3.0, 6.0]
    assert hist.cell(_VAR, 3)[2] == 2  # two distinct days


def test_hourly_cells_are_usable_by_confidence_with_only_six_hour_history():
    conn_in, conn_out = make_input_db(), make_output_db()
    matches = []
    for d in range(10):
        t = _T0 + d * 86400
        _hourly_obs(conn_in, t, [10.0] * 25)
        _forecast(conn_out, t, 6, 10.2, 10.0)
        matches.append((t, 1.0))
    hist = _hist(conn_in, conn_out)
    assert _confidence.confidence_for_cell(hist, _VAR, 3, matches) > 0.0
