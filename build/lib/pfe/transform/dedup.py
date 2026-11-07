"""Regroupement des articles quasi identiques : 1re publication = signal, reprises = bruit."""
from __future__ import annotations

import re

import numpy as np
import pandas as pd


def _shingles(text: str, k: int = 3) -> set[str]:
    toks = re.findall(r"\w+", (text or "").lower())
    if len(toks) <= k:
        return {" ".join(toks)}
    return {" ".join(toks[i : i + k]) for i in range(len(toks) - k + 1)}


def cluster_articles(
    df: pd.DataFrame, *, id_col="hk_article", text_col="text", ts_col="published_utc",
    threshold: float = 0.8, window: str = "48h", num_perm: int = 128,
) -> pd.DataFrame:
    """MinHash-LSH, parcours chronologique. Ajoute cluster_id, is_first, dup_rank, cluster_size.

    Les candidats hors fenêtre temporelle sont ignorés (une reprise 2 semaines plus tard est une nouvelle info).
    À l'échelle : appeler via groupBy(date).applyInPandas sur une fenêtre glissante de 48 h.
    """
    from datasketch import MinHash, MinHashLSH

    d = df.sort_values(ts_col, kind="stable").reset_index(drop=True)
    ts = pd.to_datetime(d[ts_col], utc=True).to_numpy()
    win = pd.Timedelta(window).to_timedelta64()
    lsh = MinHashLSH(threshold=threshold, num_perm=num_perm)
    cluster = np.empty(len(d), dtype=object)
    for i, text in enumerate(d[text_col].fillna("")):
        m = MinHash(num_perm=num_perm)
        for sh in _shingles(text):
            m.update(sh.encode())
        cands = [int(c) for c in lsh.query(m) if ts[i] - ts[int(c)] <= win]
        cluster[i] = cluster[min(cands)] if cands else d.at[i, id_col]
        lsh.insert(str(i), m)
    d["cluster_id"] = cluster
    d["dup_rank"] = d.groupby("cluster_id").cumcount()
    d["is_first"] = d["dup_rank"] == 0
    d["cluster_size"] = d.groupby("cluster_id")[id_col].transform("size")
    return d
