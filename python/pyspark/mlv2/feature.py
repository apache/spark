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
from typing import Any, Union, List, Tuple, Callable

from pyspark.sql import DataFrame
from pyspark.mlv2.base import Estimator, Model
from pyspark.mlv2.summarizer import summarize_dataframe
from pyspark.ml.param.shared import HasInputCol, HasOutputCol


class MaxAbsScaler(Estimator, HasInputCol, HasOutputCol):
    """
    Rescale each feature individually to range [-1, 1] by dividing through the largest maximum
    absolute value in each feature. It does not shift/center the data, and thus does not destroy
    any sparsity.
    """

    def __init__(self, inputCol: str, outputCol: str) -> None:
        super().__init__()
        self.set(self.inputCol, inputCol)
        self.set(self.outputCol, outputCol)

    def _fit(self, dataset: Union["pd.DataFrame", "DataFrame"]) -> "MaxAbsScalerModel":
        input_col = self.getInputCol()

        min_max_res = summarize_dataframe(dataset, input_col, ["min", "max"])
        min_values = min_max_res["min"]
        max_values = min_max_res["max"]

        max_abs_values = np.maximum(np.abs(min_values), np.abs(max_values))

        model = MaxAbsScalerModel(max_abs_values)
        model._resetUid(self.uid)
        return self._copyValues(model)


class MaxAbsScalerModel(Model, HasInputCol, HasOutputCol):
    def __init__(self, max_abs_values: "np.ndarray") -> None:
        super().__init__()
        self.max_abs_values = max_abs_values

    def _input_columns(self) -> List[str]:
        return [self.getInputCol()]

    def _output_columns(self) -> List[Tuple[str, str]]:
        return [(self.getOutputCol(), "array<double>")]

    def _get_transform_fn(self) -> Callable[..., Any]:
        max_abs_values = self.max_abs_values
        max_abs_values_zero_cond = max_abs_values == 0.0

        def transform_fn(series: Any) -> Any:
            def map_value(x: "np.ndarray") -> "np.ndarray":
                return np.where(max_abs_values_zero_cond, 0.0, x / max_abs_values)

            return series.apply(map_value)

        return transform_fn


class StandardScaler(Estimator, HasInputCol, HasOutputCol):
    """
    Standardizes features by removing the mean and scaling to unit variance using column summary
    statistics on the samples in the training set.
    """

    def __init__(self, inputCol: str, outputCol: str) -> None:
        super().__init__()
        self.set(self.inputCol, inputCol)
        self.set(self.outputCol, outputCol)

    def _fit(self, dataset: Union[DataFrame, pd.DataFrame]) -> "StandardScalerModel":
        input_col = self.getInputCol()

        min_max_res = summarize_dataframe(dataset, input_col, ["mean", "std"])
        mean_values = min_max_res["mean"]
        std_values = min_max_res["std"]

        model = StandardScalerModel(mean_values, std_values)
        model._resetUid(self.uid)
        return self._copyValues(model)


class StandardScalerModel(Model, HasInputCol, HasOutputCol):
    def __init__(self, mean_values: "np.ndarray", std_values: "np.ndarray") -> None:
        super().__init__()
        self.mean_values = mean_values
        self.std_values = std_values

    def _input_columns(self) -> List[str]:
        return [self.getInputCol()]

    def _output_columns(self) -> List[Tuple[str, str]]:
        return [(self.getOutputCol(), "array<double>")]

    def _get_transform_fn(self) -> Callable[..., Any]:
        mean_values = self.mean_values
        std_values = self.std_values

        def transform_fn(series: Any) -> Any:
            def map_value(x: "np.ndarray") -> "np.ndarray":
                return (x - mean_values) / std_values

            return series.apply(map_value)

        return transform_fn
