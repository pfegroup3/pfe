"""Étude d'événements : modèle de marché, rendement anormal cumulé (CAR) et significativité."""
from __future__ import annotations

import numpy as np
import pandas as pd
from scipy import stats


def event_car(
    stock_ret: pd.Series, mkt_ret: pd.Series, event_ts: pd.Timestamp,
    *, pre: int = 15, post: int = 60, est_len: int = 1950, min_est: int = 300,
) -> dict | None:
    """CAR sur [-pre, +post] minutes (en nombre de barres) autour de `event_ts`.

    Estimation (alpha, beta) par MCO sur les `est_len` barres précédant la fenêtre
    (≈ 5 séances) -> aucune fuite d'information de l'événement dans le modèle.
    """
    df = pd.concat({"r": stock_ret, "m": mkt_ret}, axis=1).dropna().sort_index()
    pos = df.index.searchsorted(event_ts)
    lo, hi = pos - pre, pos + post + 1
    if lo - min_est < 0 or hi > len(df):
        return None
    est = df.iloc[max(0, lo - est_len) : lo]
    if len(est) < min_est or est["m"].var() == 0:
        return None
    beta, alpha = np.polyfit(est["m"], est["r"], 1)
    resid = est["r"] - (alpha + beta * est["m"])
    sigma = resid.std(ddof=2)
    win = df.iloc[lo:hi]
    ar = win["r"] - (alpha + beta * win["m"])
    car = float(ar.sum())
    t = car / (sigma * np.sqrt(len(ar))) if sigma > 0 else np.nan
    p = float(2 * stats.t.sf(abs(t), df=len(est) - 2)) if np.isfinite(t) else np.nan
    return {"car": car, "t_stat": float(t), "p_value": p, "alpha": float(alpha), "beta": float(beta),
            "sigma": float(sigma), "n_est": int(len(est)), "n_win": int(len(ar))}


def aggregate_car_test(cars: pd.Series) -> dict:
    """Test transversal : CAR moyen ≠ 0 (t de Student) + Wilcoxon (robuste aux queues épaisses)."""
    c = cars.dropna()
    if len(c) < 5:
        return {"n": len(c), "mean": float(c.mean()) if len(c) else np.nan, "t_p": np.nan, "wilcoxon_p": np.nan}
    return {"n": len(c), "mean": float(c.mean()), "t_p": float(stats.ttest_1samp(c, 0).pvalue),
            "wilcoxon_p": float(stats.wilcoxon(c).pvalue)}
