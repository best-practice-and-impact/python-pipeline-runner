import pandas as pd

# How can we get the input columns represented as separate objects here?
# This works but doesn't offer much benefit over a string representation because you can't find the original object
# .. import * also works, but makes the editor highlight all of the missing imports 
# Could instead references them in the steps using df.column_name to make it clear which input they are from
from input_file import column_1, column_2

from example_module import add_scalar_to_series, sum_series
from constants import SOME_SCALAR

# Although the methods are trivial, this approach keeps the logic defined separately and testable
# It also doesn't care if these are series, scalar values, numpy arrays etc
column_3 = sum_series(column_1, column_2)
column_4 = add_scalar_to_series(column_3, SOME_SCALAR)

# Would move this to a runner that uses getattr and executes the pipeline
# However, python/pandas would still evaluate every object in this module even if it's not included in the output, unless the driver uses lazy eval (PySpark or polars)
print(pd.DataFrame(zip(column_1, column_3, column_4), columns=["column_1", "column_3", "column_4"]))