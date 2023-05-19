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
from pyspark.mlv2.util import aggregate_dataframe


class SummarizerAggStatus:

    def __init__(self, input_array):
        self.min_values = input_array.copy()
        self.max_values = input_array.copy()
        self.count = 1
        self.sum_values = np.array(input_array.copy())
        self.square_sum_values = np.square(input_array)

    def update(self, input_array):
        self.count += 1
        self.sum_values += input_array
        self.square_sum_values += np.square(input_array)
        self.min_values = np.minimum(self.min_values, input_array)
        self.max_values = np.maximum(self.max_values, input_array)

    def merge(self, status):
        self.count += status.count
        self.sum_values += status.sum_values
        self.square_sum_values += status.square_sum_values
        self.min_values = np.minimum(self.min_values, status.min_values)
        self.max_values = np.maximum(self.max_values, status.max_values)
        return self

    def to_result(self, metrics):
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
                    raise ValueError("Standard deviation evaluation requires more than one row data.")
                result["std"] = np.sqrt(
                    ((self.square_sum_values / self.count) - np.square(self.sum_values / self.count))
                    * (self.count / (self.count - 1))
                )

        return result


def summarize_dataframe(dataframe, column, metrics):
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
        "min", "max",  "sum", "mean"

    Returns
    -------
    Summary results as a dict, the keys in the dict are the metrics being summarized.
    """

    def local_agg_fn(pandas_df):
        status = None
        for value_array in pandas_df[column].values:
            if status is None:
                status = SummarizerAggStatus(value_array)
            else:
                status.update(value_array)

        return status

    def merge_agg_status(status1, status2):
        return status1.merge(status2)

    def agg_status_to_result(status):
        return status.to_result(metrics)

    return aggregate_dataframe(
        dataframe, [column], local_agg_fn, merge_agg_status, agg_status_to_result
    )
