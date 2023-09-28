#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import numpy as np
import pandas as pd
from typing import Any, Union, List, Dict

from pyspark.sql import DataFrame
from pyspark.ml.connect.util import aggregate_dataframe


class SummarizerAggState:
    def __init__(self, input_array: "np.ndarray") -> None:
        self.min_values = input_array.copy()
        self.max_values = input_array.copy()
        self.count = 1
        self.sum_values = np.array(input_array.copy())
        self.square_sum_values = np.square(input_array.copy())

    def update(self, input_array: "np.ndarray") -> None:
        self.count += 1
        self.sum_values += input_array
        self.square_sum_values += np.square(input_array)
        self.min_values = np.minimum(self.min_values, input_array)
        self.max_values = np.maximum(self.max_values, input_array)

    def merge(self, state: "SummarizerAggState") -> "SummarizerAggState":
        self.count += state.count
        self.sum_values += state.sum_values
        self.square_sum_values += state.square_sum_values
        self.min_values = np.minimum(self.min_values, state.min_values)
        self.max_values = np.maximum(self.max_values, state.max_values)
        return self

    def to_result(self, metrics: List[str]) -> Dict[str, Any]:
        result = {}

        for metric in metrics:
            if metric == "min":
                result["min"] = self.min_values.copy()
            if metric == "max":
                result["max"] = self.max_values.copy()
            if metric == "sum":
                result["sum"] = self.sum_values.copy()
            if metric == "mean":
                result["mean"] = self.sum_values / self.count
            if metric == "std":
                if self.count <= 1:
                    raise ValueError(
                        "Standard deviation evaluation requires more than one row data."
                    )
                result["std"] = np.sqrt(
                    (
                        (self.square_sum_values / self.count)
                        - np.square(self.sum_values / self.count)
                    )
                    * (self.count / (self.count - 1))
                )
            if metric == "count":
                result["count"] = self.count  # type: ignore[assignment]

        return result


def summarize_dataframe(
    dataframe: Union["DataFrame", "pd.DataFrame"], column: str, metrics: List[str]
) -> Dict[str, Any]:
    """
    Summarize an array type column over a spark dataframe or a pandas dataframe

    Parameters
    ----------
    dataframe : :py:class:`pyspark.sql.DataFrame` or py:class:`pandas.DataFrame`
        input dataset, it can be either pandas dataframe or spark dataframe.

    column:
        The name of the column to be summarized, it must be an array type column
        and all values in the column must have the same length.
    metrics:
        The metrics to be summarized, available metrics are:
        "min", "max",  "sum", "mean", "count"

    Returns
    -------
    Summary results as a dict, the keys in the dict are the metrics being summarized.
    """

    def local_agg_fn(pandas_df: "pd.DataFrame") -> Any:
        state = None
        for _, value_array in pandas_df[column].items():
            if state is None:
                state = SummarizerAggState(value_array)
            else:
                state.update(value_array)

        return state

    def merge_agg_state(state1: Any, state2: Any) -> Any:
        return state1.merge(state2)

    def agg_state_to_result(state: Any) -> Any:
        return state.to_result(metrics)

    return aggregate_dataframe(
        dataframe, [column], local_agg_fn, merge_agg_state, agg_state_to_result
    )
