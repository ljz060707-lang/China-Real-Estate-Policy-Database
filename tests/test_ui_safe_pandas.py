import sys
from pathlib import Path

import pandas as pd
import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.ui import safe_pandas


def test_safe_pandas_disables_arrow_string_inference_at_frame_boundary():
    with pd.option_context("future.infer_string", True):
        frame = safe_pandas(pl.DataFrame({"city_name": ["南京市"], "complete_rate": [0.5]}))
    assert str(frame["city_name"].dtype) == "object"
    assert str(frame.columns.dtype) == "object"
    assert frame.to_dict(orient="records") == [
        {"city_name": "南京市", "complete_rate": 0.5}
    ]