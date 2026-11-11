import models._calibration as cal


def _fine(variable="temperature", lead=6, n_bin=1, form_bin=1, cells=None):
    """cells: {score_bin: (days, p_hat)} -> fine dict keyed like build_table expects."""
    return {(variable, lead, n_bin, form_bin, b): v for b, v in (cells or {}).items()}


def _coarse(variable="temperature", lead=6, cells=None):
    return {(variable, lead, b): v for b, v in (cells or {}).items()}


# --- hit and tolerance ---

def test_is_hit_boundary_is_inclusive():
    assert cal.is_hit("temperature", 6, cal.tolerance("temperature", 6))
    assert not cal.is_hit("temperature", 6, cal.tolerance("temperature", 6) + 1e-6)


def test_tolerance_matches_plan_defaults_at_every_lead():
    for lead in (1, 12, 24):
        assert cal.tolerance("temperature", lead) == 1.11
        assert cal.tolerance("dewpoint", lead) == 1.11
        assert cal.tolerance("pressure", lead) == 1.5


# --- bins ---

def test_n_bin_edges():
    assert [cal.n_bin(n) for n in (1, 3, 4, 10, 11, 50)] == [0, 0, 1, 1, 2, 2]


def test_score_bin_edges():
    assert cal.score_bin(0.0) == 0
    assert cal.score_bin(0.999) == 9
    assert cal.score_bin(1.0) == 9


def test_pack_unpack_roundtrip_leaf_and_aggregate():
    packed = cal.pack(2, 0, 7)
    assert cal.unpack(packed) == (2, 0, 7)
    assert cal.unpack(cal.aggregate_bin(4)) == (3, 1, 4)


# --- activation and value ---

def test_cell_with_fewer_than_min_days_is_inactive():
    table = cal.build_table(_fine(cells={5: (cal.MIN_DAYS - 1, 1.0)}), {})
    assert table.lookup("temperature", 6, 1, 1, 5) is None


def test_perfect_record_needs_98_days_for_99_percent():
    at_98 = cal.build_table(_fine(cells={9: (98, 1.0)}), {})
    at_97 = cal.build_table(_fine(cells={9: (97, 1.0)}), {})
    assert at_98.lookup("temperature", 6, 1, 1, 9) >= 0.99
    assert at_97.lookup("temperature", 6, 1, 1, 9) < 0.99


def test_fourteen_perfect_days_do_not_reach_99():
    table = cal.build_table(_fine(cells={9: (14, 1.0)}), {})
    assert abs(table.lookup("temperature", 6, 1, 1, 9) - 15 / 16) < 1e-9


# --- monotone merge and fill ---

def test_pav_pools_thin_high_bin_with_lower_neighbours():
    # bin 3 is a thin 16/17 hit; bins 4 and 5 have lots of days at 0.85
    cells = {3: (14, 1.0), 4: (150, 0.85), 5: (150, 0.85)}
    table = cal.build_table(_fine(cells=cells), {})
    v3, v4, v5 = (table.lookup("temperature", 6, 1, 1, b) for b in (3, 4, 5))
    assert v3 <= v4 <= v5
    # the thin estimate is dragged down toward its neighbours, not carried up
    assert v4 < 0.90
    assert v3 < 0.9375


def test_violation_case_pools_instead_of_taking_max():
    # 0.70, 0.60, 0.80 pattern: the middle bin must never be reported as 0.70
    cells = {2: (100, 0.70), 3: (100, 0.60), 4: (100, 0.80)}
    table = cal.build_table(_fine(cells=cells), {})
    vals = [table.lookup("temperature", 6, 1, 1, b) for b in (2, 3, 4)]
    assert vals[0] <= vals[1] <= vals[2]
    assert vals[1] < 0.70


def test_inactive_bin_takes_nearest_lower_active_value():
    cells = {3: (100, 0.50), 6: (100, 0.70)}
    table = cal.build_table(_fine(cells=cells), {})
    low = table.lookup("temperature", 6, 1, 1, 3)
    assert table.lookup("temperature", 6, 1, 1, 4) == low
    assert table.lookup("temperature", 6, 1, 1, 5) == low
    assert table.lookup("temperature", 6, 1, 1, 9) == table.lookup("temperature", 6, 1, 1, 6)


def test_thin_top_bin_never_exceeds_nearest_lower_active_value():
    cells = {5: (100, 0.60), 9: (3, 1.0)}
    table = cal.build_table(_fine(cells=cells), {})
    assert table.lookup("temperature", 6, 1, 1, 9) == table.lookup("temperature", 6, 1, 1, 5)


def test_low_bins_below_lowest_active_use_coarse_but_never_exceed_lowest_fine():
    fine = _fine(cells={4: (100, 0.40)})
    coarse = _coarse(cells={0: (100, 0.10), 1: (100, 0.20), 2: (100, 0.90), 4: (100, 0.50)})
    table = cal.build_table(fine, coarse)
    lowest_fine = table.lookup("temperature", 6, 1, 1, 4)
    assert table.lookup("temperature", 6, 1, 1, 0) < lowest_fine
    assert table.lookup("temperature", 6, 1, 1, 1) < lowest_fine
    # coarse bin 2 is high, but a low bin may not exceed the lowest active fine value
    assert table.lookup("temperature", 6, 1, 1, 2) <= lowest_fine


def test_empty_fine_falls_back_to_coarse():
    coarse = _coarse(cells={2: (100, 0.30), 6: (100, 0.70)})
    table = cal.build_table({}, coarse)
    assert table.lookup("temperature", 6, 1, 1, 6) is not None
    assert table.lookup("temperature", 6, 1, 1, 6) > table.lookup("temperature", 6, 1, 1, 2)


def test_both_empty_returns_none():
    table = cal.build_table({}, _coarse(cells={3: (3, 1.0)}))
    assert table.lookup("temperature", 6, 1, 1, 3) is None
    assert cal.build_table({}, {}).lookup("pressure", 1, 0, 0, 0) is None


def test_aggregate_cell_never_uses_coarse():
    coarse = _coarse(cells={5: (200, 0.90)})
    table = cal.build_table({}, coarse)
    assert table.lookup("temperature", 6, 3, 1, 5) is None
    fine = _fine(n_bin=3, form_bin=1, cells={5: (100, 0.6)})
    assert cal.build_table(fine, coarse).lookup("temperature", 6, 3, 1, 5) is not None


def test_values_are_clamped_to_unit_interval():
    table = cal.build_table(_fine(cells={9: (1000, 1.0)}), {})
    v = table.lookup("temperature", 6, 1, 1, 9)
    assert 0.0 <= v <= 1.0


def test_lookup_exception_in_vector_build_returns_none_not_raise():
    table = cal.build_table({("temperature", 6, 1, 1, 99): (100, 0.5)}, {})
    # a malformed score bin must not blow up the forecast run
    assert table.lookup("temperature", 6, 1, 1, 3) is None
