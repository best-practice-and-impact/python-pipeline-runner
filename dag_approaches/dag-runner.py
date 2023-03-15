"""
First attempt at building a config-driven DAG runner. Incomplete.


Requirements:
- nodes/columns are assigned python objects
- logic is reused between common derivations
- code is concise


Usage options

F.col("col_3") = sum_cols(F.col("col_1"), F.col("col_2"))

PySpark's solution, so must be a reasonably logical approach
Not assigned objects, so more difficult to trace in code
Lots of repetition of the Col function call


col_3 = sum_cols(col_1, col_2)
output(col_1, col_2, col_3)
Would need to create an assigned object for every col in source data, so we can reference them as parameters
Could lead to some very nice semantic definitions - e.g. average_height = average(height)
Might depends on order in definition?



Node(name="col_3", method=sum_cols, parameters={"columns_to_sum":["col_1", "col_2"]})
Not named objects, so more difficult to trace in code
Config-driven
Quite verbose to define if you're not using a config



def col_3(col_1, col_2):
    return col_1, col_2
Everything is an assigned object
Not DRY and would be a pain to change tests to a generic method
Method and data nodes tightly coupled
Output columns are still selected/specified as strings in Hamilton
Column names are fixed by the method, so everyone using that method would use the same name




@does(sum_cols)
def col_3(col_1, col_2):
    pass
Misleading because it's not a function
Everything is an assigned object
Output columns are still selected/specified as strings in Hamilton
"""

from graphlib import TopologicalSorter
from dataclasses import dataclass
from typing import Optional, get_type_hints

import pandas as pd

@dataclass
class Node:
    """
    Class to store enough information to evaluate the creation of a column once the DAG is
    created and validated.

    This would require either the source dataframe or the method and parameters to derive the column.
    Assuming that users pass column names as strings, we would need to parse these into pd.Series for the evaluation.
    """
    name: str
    method: Optional[callable] = None
    parameters: Optional[dict] = None
    source: Optional[pd.DataFrame] = None

    def validate(self):
        pass
    
    def parse_parameters(self):
        """
        Parse column names into pandas.Series objects, inferred from type hints.
        
        This needs to be recursive to catch Series nested inside other objects.
        """
        type_hints = get_type_hints(self.method)
        parsed_parameters = {}
        for key, value in self.parameters.items():
            if key not in type_hints:
                raise ValueError(f"{self.method.__name__} has no argument named {key}")
            parsed_value = self.source["value"] if type_hints[key] == pd.Series else value
            parsed_parameters[key] = parsed_value
        return parsed_parameters
    
    def evaluate(self):
        return self.method(**self.parse_parameters()) if self.method is not None else self.source[self.name]


def nodes_from_dataframe(df: pd.DataFrame) -> list[Node]:
    "Create a list of Nodes from source dataframe columns."
    return [Node(name=column_name, source=df) for column_name in df.column]

def DAG_from_node_list():
    pass

class DAG(TopologicalSorter):
    def validate(self):
        try:
            self.static_order()
        except Exception as e:
            raise e("DAG contains cycles.")

    def evaluate(self):
        for node in self.static_order():
            node.source[node.name] = node.evaluate()


# A couple of functions that we would need to support
def sum_cols(columns_to_sum: list[pd.Series]) -> pd.Series:
    return sum(columns_to_sum)

def multiply_by_integer(column_to_double: pd.Series, multiplier: int) -> pd.Series:
    return column_to_double * multiplier


if __name__ == "__main__":
    df = pd.DataFrame({"col_1": [0, 1, 2], "col_2": [3, 4, 5]})
    nodes = nodes_from_dataframe(df)
    nodes += [Node(name="col_3", method=sum_cols, parameters={"columns_to_sum":["col_1", "col_2"]}), Node(name="col_3", method=multiply_by_integer, parameters={"column_to_double": "col_1", "multiplier": 2})]
    
    
    # graph = {"D": ["B", "C"], "C": {"A"}, "B": {"A"}}
    # Need a method to build a DAG from list of Nodes
    # DAG = DAG_from_node_list()
    dag = DAG(graph)
    dag_order = tuple(dag.static_order())
    print(dag_order)
