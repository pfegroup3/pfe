"""
    Modèle 1 : 
    sentiment (FinBERT) + catégorie d'événement ; évaluation vs échantillon annoté et vs Finnhub.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import re


CATEGORIES = ["résultats", "fusion-acquisition", "direction", "réglementaire-juridique",
              "produit", "analyste-notation", "macro", "autre"]

# 8-K : items SEC -> catégorie (règle déterministe, prioritaire sur le zero-shot)
ITEM_8K = {"2.02": "résultats", "1.01": "fusion-acquisition", "2.01": "fusion-acquisition",
           "5.02": "direction", "8.01": "autre", "7.01": "autre", "3.01": "réglementaire-juridique",
           "4.02": "réglementaire-juridique", "1.03": "réglementaire-juridique"}

_KEYWORDS = {
    "résultats": ["earnings", "revenue", "profit", "eps", "quarterly", "guidance", "results"],
    "fusion-acquisition": ["acquire", "acquires", "acquisition", "merger", "buyout", "takeover"],
    "direction": ["ceo", "cfo", "resigns", "appoints", "steps down", "chairman"],
    "réglementaire-juridique": ["sec", "lawsuit", "fine", "probe", "investigation", "antitrust", "settlement"],
    "produit": ["launch", "launches", "unveils", "unveil", "recall", "new product"],
    "analyste-notation": ["upgrade", "downgrade", "price target", "analyst", "rating", "outperform"],
    "macro": ["fed", "inflation", "interest rate", "tariff", "tariffs", "gdp", "opec"],
}
_PATTERNS = {c: re.compile(r"\b(?:" + "|".join(map(re.escape, w)) + r")\b", re.I) for c, w in _KEYWORDS.items()}


def categorize_rules(text: str) -> str:
    """
        Cette méthode catégorise par mots-clés. Elle prend la première catégorie qui correspond, sinon 'autre'.
    """
    for cat, pat in _PATTERNS.items():
        if pat.search(text or ""):
            return cat
    return "autre"

class FinBertScorer:
    def __init__(self, model: str = "ProsusAI/finbert", device: int = -1, batch_size: int = 32):
        from transformers import pipeline

        # Chargement de FinBERT et gestion de la tokenisation
        self.pipe = pipeline(
                            "text-classification", 
                            model      = model, 
                            top_k      = None, 
                            truncation = True,
                            max_length = 256, 
                            device     = device, 
                            batch_size = batch_size
                            )


    def score(self, texts: list[str]) -> pd.DataFrame:
        """
            Cette methode appelle le modèle sur la liste des textes et calcule sentiment_score : positive - negative 
        """
        res  = self.pipe([t or "" for t in texts])
        rows = [{d["label"].lower(): d["score"] for d in r} for r in res]
        out  = pd.DataFrame(rows).reindex(columns = ["positive", "negative", "neutral"]).fillna(0.0)

        out["sentiment_score"] = out["positive"] - out["negative"]
        out["sentiment_label"] = out[["positive", "negative", "neutral"]].idxmax(axis = 1)

        return out


class ZeroShotCategorizer:
    def __init__(self, model: str = "facebook/bart-large-mnli", device: int = -1):
        from transformers import pipeline

        self.pipe = pipeline("zero-shot-classification", model = model, device = device)

    def predict(self, texts: list[str]) -> list[str]:
        return [r["labels"][0] for r in self.pipe(list(texts), candidate_labels = CATEGORIES, truncation = True)]


def finnhub_to_label(score: pd.Series, band: float = 0.15) -> pd.Series:
    """Finnhub bullishPercent/score centré sur 0 -> positive/negative/neutral."""
    return pd.Series(np.select([score > band, score < -band], ["positive", "negative"], "neutral"), index = score.index)


def evaluate_sentiment(gold: pd.Series, finbert: pd.Series, finnhub: pd.Series | None = None) -> dict:
    """Compare FinBERT et Finnhub à l'échantillon annoté (gold)."""
    from sklearn.metrics import cohen_kappa_score, f1_score

    m = {"finbert_macro_f1": f1_score(gold, finbert, average = "macro"),
         "finbert_kappa": cohen_kappa_score(gold, finbert)}
    if finnhub is not None:
        m |= {"finnhub_macro_f1": f1_score(gold, finnhub, average = "macro"),
              "finnhub_kappa": cohen_kappa_score(gold, finnhub)}
    return {k: float(v) for k, v in m.items()}

_ITEMS_BLOCK = re.compile(r"8-K Items ([\d.,\s]+?)\s-")

def categorize_8k(headline: str) -> str | None:
    """Catégorie d'après les items du 8-K ; la première correspondance utile gagne, sinon None."""
    m = _ITEMS_BLOCK.search(headline or "")
    if not m:
        return None
    for item in re.findall(r"\d\.\d{2}", m.group(1)):
        cat = ITEM_8K.get(item)
        if cat and cat != "autre":
            return cat
    return None
