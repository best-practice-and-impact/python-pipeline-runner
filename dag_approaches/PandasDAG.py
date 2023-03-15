"""
Written in collab with ChatGPT
Doesn't work, but has the rough outline of what I was looking for
"""

import pandas as pd
from typing import Any, Dict, List, Tuple, Union

class PandasDAG:
    def __init__(self, input_data: pd.DataFrame) -> None:
        self.input_data = input_data
        self.nodes = {}
        self.dependencies = {}
        self.scalars = {}

        # Create initial nodes from input_data
        for col in input_data.columns:
            self.nodes[col] = (input_data[col], None, {} )
            self.dependencies[col] = set()

    def add_scalar(self, name: str, value: Any) -> None:
        self.scalars[name] = value

    def add_task(self, func: Any, output_col: str, **kwargs: Any) -> None:
        dependencies = []
        for arg_name, arg_type in func.__annotations__.items():
            if arg_type in (pd.Series, int, float):
                dependencies.append(arg_name)

        self.nodes[output_col] = (None, func, kwargs)
        self.dependencies[output_col] = set(dependencies)

    def run(self) -> pd.DataFrame:
        while len(self.nodes) > len(self.input_data.columns):
            for node, (value, func, kwargs) in self.nodes.items():
                if value is not None:
                    continue

                if all(self.nodes[col][0] is not None for col in self.dependencies[node]):
                    inputs = []
                    for col in self.dependencies[node]:
                        if col in self.scalars:
                            inputs.append(self.scalars[col])
                        else:
                            inputs.append(self.nodes[col][0])
                    output = func(*inputs, **kwargs)
                    self.nodes[node] = (output, func, kwargs)

        output_data = {}
        for col in self.nodes:
            if col not in self.input_data.columns:
                output_data[col] = self.nodes[col][0]

        return pd.DataFrame(output_data)


if __name__ == "__main__":
    # Define some transformation functions
    def add_scalar(series: pd.Series, scalar_to_add: int) -> pd.Series:
        return series + scalar_to_add

    def multiply_series(series1: pd.Series, series2: pd.Series) -> pd.Series:
        return series1 * series2

    def subtract_scalar(series: pd.Series, scalar_to_subtract: float) -> pd.Series:
        return series - scalar_to_subtract

    # Create a sample input DataFrame
    input_data = pd.DataFrame({
        "A": [1, 2, 3],
        "B": [4, 5, 6],
        "C": [7, 8, 9]
    })

    # Create a DAG and add tasks
    dag = PandasDAG(input_data)
    dag.add_scalar("S", 10)
    dag.add_scalar("S2", 5)
    dag.add_task(add_scalar, "D", series="A", scalar_to_add="S")
    dag.add_task(multiply_series, "E", series1="B", series2="D")
    dag.add_task(subtract_scalar, "F", series="C", scalar_to_subtract="S2")
    dag.add_task(multiply_series, "G", series1="E", series2="F")

    # Execute the DAG
    output_data = dag.run()

    # Display the results
    print(output_data)