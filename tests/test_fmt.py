import fmt


def test_wind_dir_none():
    assert fmt.wind_dir(None) == "\u2014"


def test_wind_dir_north_zero():
    assert fmt.wind_dir(0) == "N"


def test_wind_dir_north_360():
    # round(360/22.5) % 16 = 16 % 16 = 0 -> N
    assert fmt.wind_dir(360) == "N"


def test_wind_dir_east():
    assert fmt.wind_dir(90) == "E"


def test_wind_dir_west():
    assert fmt.wind_dir(270) == "W"


def test_wind_dir_rounds_down_to_north():
    # 11/22.5 = 0.489 -> rounds to 0 -> N
    assert fmt.wind_dir(11) == "N"


def test_wind_dir_rounds_up_to_nne():
    # 12/22.5 = 0.533 -> rounds to 1 -> NNE
    assert fmt.wind_dir(12) == "NNE"


def test_temp_none():
    assert fmt.temp(None) == "\u2014"


def test_temp_freezing():
    assert fmt.temp(0.0) == "32.0\u00b0F"


def test_temp_boiling():
    assert fmt.temp(100.0) == "212.0\u00b0F"


def test_temp_intersection():
    # -40°C == -40°F
    assert fmt.temp(-40.0) == "-40.0\u00b0F"


def test_val_none():
    assert fmt.val(None) == "\u2014"


def test_val_with_unit():
    assert fmt.val(3.14159, ".2f", " mb") == "3.14 mb"


def test_val_default_spec():
    assert fmt.val(3.14159, ".1f") == "3.1"
