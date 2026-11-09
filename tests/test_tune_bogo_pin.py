import argparse
import sqlite3
import time

import pytest

import barogram as bg
import db
from tests.conftest import make_output_db

_RUNS = 40
_LEAD = 6


class _FakeConf:
    def __init__(self, output_db):
        self.output_db = str(output_db)


def _args():
    return argparse.Namespace(
        min_runs=3, min_total_runs=30, floor=0.08, pool_alpha=0.10,
        subfloor_fraction=0.35, huber_percentile=80.0, dry_run=False,
    )


def _seed(conn, model_id, member_id, error, runs=_RUNS):
    now = int(time.time())
    for i in range(runs):
        issued_at = now - (i + 2) * 86400
        valid_at = issued_at + _LEAD * 3600
        conn.execute(
            """
            insert into forecasts
                (model_id, model, member_id, issued_at, valid_at, lead_hours, variable,
                 value, observed, error, mae, scored_at)
            values (?, 'm', ?, ?, ?, ?, 'temperature', 0.0, 0.0, ?, ?, ?)
            """,
            (model_id, member_id, issued_at, valid_at, _LEAD, error, abs(error), valid_at + 1),
        )


def _on_disk(tmp_path, seed):
    path = tmp_path / "barogram.db"
    mem = make_output_db()
    seed(mem)
    disk = sqlite3.connect(str(path))
    mem.backup(disk)
    disk.close()
    return path


def _weights(path, model_id):
    conn = db.open_output_db(str(path))
    try:
        return db.load_weights(conn, model_id)
    finally:
        conn.close()


def _mean_weight(weights, member_id):
    vals = [w for (mid, *_), w in weights.items() if mid == member_id]
    return sum(vals) / len(vals)


def test_bogo_member_id_is_not_pinned_to_subfloor(tmp_path, monkeypatch):
    # member 12 is bogo's id inside the ensemble, and the same id in any other model;
    # it must be weighted by skill like every other member
    def seed(conn):
        _seed(conn, 2, 0, 10.0)      # reference model (climatological_mean)
        _seed(conn, 100, 12, 1.0)    # clearly skilled
        _seed(conn, 100, 5, 5.0)     # less skilled

    path = _on_disk(tmp_path, seed)
    monkeypatch.setattr(bg, "_sync_check", lambda: None)

    bg.cmd_tune(_args(), _FakeConf(path))

    weights = _weights(path, 100)
    assert _mean_weight(weights, 12) > _mean_weight(weights, 5)
