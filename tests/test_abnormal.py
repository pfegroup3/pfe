import numpy as np
import pandas as pd
from pfe.transform.abnormal import aggregate_car_test, event_car


def _data(shock=0.0, seed=1):
    rng = np.random.default_rng(seed)
    idx = pd.date_range("2024-05-01 13:30", periods=4000, freq="min", tz="UTC")
    m = pd.Series(rng.normal(0, 5e-4, len(idx)), index=idx)
    r = 0.00001 + 1.2 * m + rng.normal(0, 3e-4, len(idx))
    ev = idx[3000]
    pos = idx.get_loc(ev)
    r.iloc[pos : pos + 60] += shock
    return r, m, ev


def test_beta_recupere_et_signal_detecte():
    r, m, ev = _data(shock=0.0008)
    res = event_car(r, m, ev)
    assert abs(res["beta"] - 1.2) < 0.05 and res["car"] > 0.03 and res["p_value"] < 0.01


def test_sans_choc_non_significatif():
    r, m, ev = _data(shock=0.0)
    assert event_car(r, m, ev)["p_value"] > 0.01


def test_donnees_insuffisantes():
    r, m, ev = _data()
    assert event_car(r.iloc[:100], m.iloc[:100], ev) is None


def test_agregat():
    assert aggregate_car_test(pd.Series(np.random.default_rng(0).normal(0.01, 0.005, 50)))["t_p"] < 0.001
