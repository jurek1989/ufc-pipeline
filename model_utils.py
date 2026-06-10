"""
model_utils.py — shared model classes for train_model.py and predict_upcoming.py.

Keeping IsotonicCalibratedClassifier in a dedicated importable module ensures
joblib serializes it as 'model_utils.IsotonicCalibratedClassifier' rather than
'__main__.IsotonicCalibratedClassifier', which breaks cross-script loading.
"""

import numpy as np
from sklearn.isotonic import IsotonicRegression
from xgboost import XGBClassifier


class IsotonicCalibratedClassifier:
    """
    IsotonicRegression stacked on a pre-fitted XGBClassifier.
    Equivalent to CalibratedClassifierCV(cv='prefit', method='isotonic')
    (sklearn ≥ 1.6 dropped the 'prefit' string).

    prob_clip: optional (lo, hi) tuple to prevent certainty outputs.
    """

    def __init__(self, base: XGBClassifier, prob_clip: tuple[float, float] | None = None):
        self.base      = base
        self.ir        = IsotonicRegression(out_of_bounds="clip")
        self.prob_clip = prob_clip

    def fit(self, X, y):
        raw = self.base.predict_proba(X)[:, 1]
        self.ir.fit(raw, y)
        return self

    def predict_proba(self, X) -> np.ndarray:
        raw  = self.base.predict_proba(X)[:, 1]
        cal  = self.ir.transform(raw)
        clip = getattr(self, "prob_clip", None)   # safe for old pickles
        if clip:
            cal = np.clip(cal, clip[0], clip[1])
        return np.column_stack([1.0 - cal, cal])

    def predict(self, X) -> np.ndarray:
        return (self.predict_proba(X)[:, 1] >= 0.5).astype(int)
