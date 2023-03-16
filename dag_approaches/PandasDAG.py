"""
Delayed evaluation of a set of pandas transformations. Lends itself to a config-driven pipeline definition.

Improvements might include:
- supporting methods that take parameters that are not nodes (e.g. bool flag). However, not having this does encourage simple function definitions.
- if the above is implemented, we might want to consider more explicit definition of nodes as parameters
- using the list of desired output columns to filter the DAG and only execute necessary transformations
- better error reporting
"""


from graphlib import TopologicalSorter
import pandas as pd
from typing import Any, get_type_hints
from collections import namedtuple

Node = namedtuple("Node", ["source_data", "fn", "parameters"])

class PandasDAG:
    def __init__(self, input_data: pd.DataFrame) -> None:
        self.input_data = input_data
        self.nodes = {}
        self.edges = {}

        # Create initial nodes from input_data
        for col in input_data.columns:
            self.nodes[col] = Node(input_data[col], None, {})

    def add_scalar(self, name: str, value: Any) -> None:
        self.nodes[name] = Node(value, None, {})

    def add_task(self, func: Any, output_column: str, **kwargs: Any) -> None:
        dependencies = []
        for arg_name, arg_type in get_type_hints(func).items():
            if arg_name == "return":
                continue
            if arg_type in (pd.Series, int, float):
                dependencies.append(kwargs[arg_name])
            else:
                raise ValueError("Parameter is an unsupported node type. {arg_name} is type {arg_type}")

        self.nodes[output_column] = (None, func, kwargs)
        self.edges[output_column] = set(dependencies)

    def run(self) -> pd.DataFrame:
        graph = TopologicalSorter(self.edges)
        orderded_nodes = list(graph.static_order())

        for node_name in orderded_nodes:
            if node_name not in self.nodes:
                raise ValueError(f"Parameter is not a node: {node_name}")

            source_data, fn, parameters = self.nodes[node_name]
            if source_data is not None:
                continue

            parsed_parameters = dict()
            for keyword, value in parameters.items():
                if value not in self.nodes:
                    raise ValueError("No node named {value}")
                parsed_parameters[keyword] = self.nodes[value][0]
        
            output = fn(**parsed_parameters)
            self.nodes[node_name] = Node(output, None, {})

        
        output_data = {}
        for col in self.nodes:
            if col not in self.input_data.columns:
                output_data[col] = self.nodes[col][0]
            else:
                output_data[col] = input_data[col]

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