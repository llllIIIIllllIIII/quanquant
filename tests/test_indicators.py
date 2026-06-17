from quanquant.indicators.core import bias, sma, value_at, wr


def test_sma():
    assert sma([1, 2, 3, 4, 5], 3) == 4.0
    assert sma([10, 20], 2) == 15.0
    assert sma([1, 2], 3) is None       # insufficient
    assert sma([1, 2, 3], 0) is None


def test_wr():
    # HHV=12, LLV=7, C=10 -> (12-10)/(12-7)*-100 = -40
    assert wr([10, 12, 11], [8, 9, 7], [9, 11, 10], 3) == -40.0
    # at the high -> 0; at the low -> -100
    assert wr([10, 12], [8, 7], [12, 12], 2) == 0.0
    assert wr([12, 12], [7, 7], [7, 7], 2) == -100.0
    # flat range -> 0 (no div-by-zero)
    assert wr([10, 10], [10, 10], [10, 10], 2) == 0.0
    assert wr([1], [1], [1], 3) is None


def test_bias():
    # sma(last3 of [10,10,10,13]) = 11; C=13 -> (13-11)/11*100
    assert abs(bias([10, 10, 10, 13], 3) - (2 / 11 * 100)) < 1e-9
    assert bias([5], 3) is None


def test_value_at_dispatch():
    bars = [
        {"high": 10, "low": 8, "close": 9},
        {"high": 12, "low": 9, "close": 11},
        {"high": 11, "low": 7, "close": 10},
    ]
    assert value_at("ma", 3, bars) == 10.0           # (9+11+10)/3
    assert value_at("wr", 3, bars) == -40.0
    assert value_at("bias", 3, bars) is not None
    assert value_at("unknown", 3, bars) is None
