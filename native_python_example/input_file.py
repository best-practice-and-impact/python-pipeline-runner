import sys
import pandas as pd

_this_module = sys.modules[__name__]

_df = pd.DataFrame({
        "column_1": [1, 2, 3],
        "column_2": [4, 5, 6],
    })
    
for column_name in _df.columns:
    setattr(_this_module, column_name, _df[column_name])
