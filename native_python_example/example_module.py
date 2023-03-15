from typing import Iterable
import pandas as pd

def add_scalar_to_series(series: pd.Series, scalar_to_add: int) -> pd.Series:
    return series + scalar_to_add

def sum_series(*series: Iterable[pd.Series]) -> pd.Series:
    return sum(series)