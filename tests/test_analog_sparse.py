"""Integration tests for analog model behavior with sparse/empty data."""
import time

import pytest

import models.analog as analog_mod
from tests.conftest import make_input_db, make_obs, make_output_db


def _insert_obs(conn, ts: int, air_temp=20.0, dew_point=12.0,
                station_pressure=1013.0, wind_avg=3.0):
    conn.execute(
        """
        insert into tempest_obs
            (station_id, timestamp, air_temp, dew_point, station_pressure,
             wind_avg, wind_gust, wind_direction, precip_accum_day,
             solar_radiation, uv_index, lightning_count)
        values ('KTEST', ?, ?, ?, ?, ?, null, null, 0.0, 0.0, 0.0, 0)
        """,
        (ts, air_temp, dew_point, station_pressure, wind_avg),
    )


class TestNoHistoricalData:
    def test_returns_rows_not_empty(self):
        conn_in = make_input_db()
        obs = make_obs()
        rows = analog_mod.run(obs, obs["timestamp"], conn_in=conn_in)
        # 4 leads × 4 variables × 9 members (0-8) = 144 rows
        assert len(rows) == 144

    def test_all_values_none(self):
        conn_in = make_input_db()
        obs = make_obs()
        rows = analog_mod.run(obs, obs["timestamp"], conn_in=conn_in)
        assert all(row["value"] is None for row in rows)

    def test_all_required_keys_present(self):
        required = {"model_id", "model", "issued_at", "valid_at", "lead_hours", "variable", "value"}
        conn_in = make_input_db()
        obs = make_obs()
        rows = analog_mod.run(obs, obs["timestamp"], conn_in=conn_in)
        for row in rows:
            assert required.issubset(row.keys()), f"missing keys: {required - row.keys()}"

    def test_no_duplicate_triples(self):
        conn_in = make_input_db()
        obs = make_obs()
        rows = analog_mod.run(obs, obs["timestamp"], conn_in=conn_in)
        triples = [(r["lead_hours"], r["variable"], r.get("member_id", 0)) for r in rows]
        assert len(triples) == len(set(triples))


class TestFewerCandidatesThanK:
    def test_two_candidates_k_ten_does_not_crash(self):
        """With only 2 historical days, K=10 members use all 2 without crashing."""
        conn_in = make_input_db()
        now = int(time.time())

        # insert obs for 2 historical days, same time of day as now
        one_day = 86400
        _insert_obs(conn_in, now - one_day, air_temp=18.0)
        # also insert the future obs for those days (at +6h, +12h, +18h, +24h)
        for lead in [6, 12, 18, 24]:
            _insert_obs(conn_in, now - one_day + lead * 3600, air_temp=19.0 + lead * 0.5)

        two_days = 2 * one_day
        _insert_obs(conn_in, now - two_days, air_temp=22.0)
        for lead in [6, 12, 18, 24]:
            _insert_obs(conn_in, now - two_days + lead * 3600, air_temp=21.0 + lead * 0.5)

        obs = make_obs(ts=now)
        rows = analog_mod.run(obs, now, conn_in=conn_in)
        assert len(rows) == 144

    def test_two_candidates_member_values_not_all_none(self):
        """Futures are available, so at least some member values should be non-None."""
        conn_in = make_input_db()
        now = int(time.time())
        one_day = 86400

        _insert_obs(conn_in, now - one_day, air_temp=18.0)
        for lead in [6, 12, 18, 24]:
            _insert_obs(conn_in, now - one_day + lead * 3600, air_temp=19.0)

        _insert_obs(conn_in, now - 2 * one_day, air_temp=22.0)
        for lead in [6, 12, 18, 24]:
            _insert_obs(conn_in, now - 2 * one_day + lead * 3600, air_temp=21.0)

        obs = make_obs(ts=now)
        rows = analog_mod.run(obs, now, conn_in=conn_in)
        temp_rows = [r for r in rows if r["variable"] == "temperature" and r.get("member_id", 0) == 2]
        assert any(r["value"] is not None for r in temp_rows)


class TestSingleCandidate:
    def test_single_candidate_returns_rows(self):
        conn_in = make_input_db()
        now = int(time.time())
        _insert_obs(conn_in, now - 86400, air_temp=20.0)
        for lead in [6, 12, 18, 24]:
            _insert_obs(conn_in, now - 86400 + lead * 3600, air_temp=21.0)

        obs = make_obs(ts=now)
        rows = analog_mod.run(obs, now, conn_in=conn_in)
        assert len(rows) == 144

    def test_single_candidate_spread_is_none(self):
        """spread requires at least 2 values; with 1 analog it must be None."""
        conn_in = make_input_db()
        now = int(time.time())
        _insert_obs(conn_in, now - 86400, air_temp=20.0)
        for lead in [6, 12, 18, 24]:
            _insert_obs(conn_in, now - 86400 + lead * 3600, air_temp=21.0)

        obs = make_obs(ts=now)
        rows = analog_mod.run(obs, now, conn_in=conn_in)
        mean_rows = [r for r in rows if r.get("member_id", 0) == 0]
        assert all(r.get("spread") is None for r in mean_rows)


class TestNoFutureObsForAnalog:
    def test_analog_with_no_future_obs_gives_none(self):
        """If an analog day has no future obs at +lead, that value is None."""
        conn_in = make_input_db()
        now = int(time.time())
        # insert one historical obs but NO future obs for it
        _insert_obs(conn_in, now - 86400, air_temp=20.0)

        obs = make_obs(ts=now)
        rows = analog_mod.run(obs, now, conn_in=conn_in)
        assert all(r["value"] is None for r in rows)


class TestModelConstants:
    def test_model_id(self):
        assert analog_mod.MODEL_ID == 8

    def test_model_name(self):
        assert analog_mod.MODEL_NAME == "analog"

    def test_needs_conn_in(self):
        assert analog_mod.NEEDS_CONN_IN is True

    def test_needs_weights(self):
        assert analog_mod.NEEDS_WEIGHTS is True

    def test_member_count(self):
        # 8 named members + member_id=0 = 9 total
        assert len(analog_mod._MEMBERS) == 8
        assert all(mid in {m[0] for m in analog_mod._MEMBERS} for mid in range(1, 9))
