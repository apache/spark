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
import pickle
from typing import Any, Union, List, Tuple, Callable, Dict, Optional

from pyspark import keyword_only
from pyspark.sql import DataFrame
from pyspark.ml.param.shared import HasInputCol, HasOutputCol
from pyspark.ml.connect.base import Estimator, Model
from pyspark.ml.connect.io_utils import ParamsReadWrite, CoreModelReadWrite
from pyspark.ml.connect.summarizer import summarize_dataframe


class MaxAbsScaler(Estimator, HasInputCol, HasOutputCol, ParamsReadWrite):
    """
    Rescale each feature individually to range [-1, 1] by dividing through the largest maximum
    absolute value in each feature. It does not shift/center the data, and thus does not destroy
    any sparsity.
    """

    _input_kwargs: Dict[str, Any]

    @keyword_only
    def __init__(self, *, inputCol: Optional[str] = None, outputCol: Optional[str] = None) -> None:
        """
        __init__(self, \\*, inputCol=None, outputCol=None)
        """
        super().__init__()
        kwargs = self._input_kwargs
        self._set(**kwargs)

    def _fit(self, dataset: Union["pd.DataFrame", "DataFrame"]) -> "MaxAbsScalerModel":
        input_col = self.getInputCol()

        stat_res = summarize_dataframe(dataset, input_col, ["min", "max", "count"])
        min_values = stat_res["min"]
        max_values = stat_res["max"]
        n_samples_seen = stat_res["count"]

        max_abs_values = np.maximum(np.abs(min_values), np.abs(max_values))

        model = MaxAbsScalerModel(max_abs_values, n_samples_seen)
        model._resetUid(self.uid)
        return self._copyValues(model)


class MaxAbsScalerModel(Model, HasInputCol, HasOutputCol, ParamsReadWrite, CoreModelReadWrite):
    def __init__(
        self, max_abs_values: Optional["np.ndarray"] = None, n_samples_seen: Optional[int] = None
    ) -> None:
        super().__init__()
        self.max_abs_values = max_abs_values
        if max_abs_values is not None:
            # if scale value is zero, replace it with 1.0 (for preventing division by zero)
            self.scale_values = np.where(max_abs_values == 0.0, 1.0, max_abs_values)
        self.n_samples_seen = n_samples_seen

    def _input_columns(self) -> List[str]:
        return [self.getInputCol()]

    def _output_columns(self) -> List[Tuple[str, str]]:
        return [(self.getOutputCol(), "array<double>")]

    def _get_transform_fn(self) -> Callable[..., Any]:
        scale_values = self.scale_values

        def transform_fn(series: Any) -> Any:
            def map_value(x: "np.ndarray") -> "np.ndarray":
                return x / scale_values

            return series.apply(map_value)

        return transform_fn

    def _get_core_model_filename(self) -> str:
        return self.__class__.__name__ + ".sklearn.pkl"

    def _save_core_model(self, path: str) -> None:
        from sklearn.preprocessing import MaxAbsScaler as sk_MaxAbsScaler

        sk_model = sk_MaxAbsScaler()
        sk_model.scale_ = self.scale_values
        sk_model.max_abs_ = self.max_abs_values
        sk_model.n_features_in_ = len(self.max_abs_values)  # type: ignore[arg-type]
        sk_model.n_samples_seen_ = self.n_samples_seen

        with open(path, "wb") as fp:
            pickle.dump(sk_model, fp)

    def _load_core_model(self, path: str) -> None:
        with open(path, "rb") as fp:
            sk_model = pickle.load(fp)

        self.max_abs_values = sk_model.max_abs_
        self.scale_values = sk_model.scale_
        self.n_samples_seen = sk_model.n_samples_seen_


class StandardScaler(Estimator, HasInputCol, HasOutputCol, ParamsReadWrite):
    """
    Standardizes features by removing the mean and scaling to unit variance using column summary
    statistics on the samples in the training set.
    """

    _input_kwargs: Dict[str, Any]

    @keyword_only
    def __init__(self, inputCol: Optional[str] = None, outputCol: Optional[str] = None) -> None:
        """
        __init__(self, \\*, inputCol=None, outputCol=None)
        """
        super().__init__()
        kwargs = self._input_kwargs
        self._set(**kwargs)

    def _fit(self, dataset: Union[DataFrame, pd.DataFrame]) -> "StandardScalerModel":
        input_col = self.getInputCol()

        stat_result = summarize_dataframe(dataset, input_col, ["mean", "std", "count"])
        mean_values = stat_result["mean"]
        std_values = stat_result["std"]
        n_samples_seen = stat_result["count"]

        model = StandardScalerModel(mean_values, std_values, n_samples_seen)
        model._resetUid(self.uid)
        return self._copyValues(model)


class StandardScalerModel(Model, HasInputCol, HasOutputCol, ParamsReadWrite, CoreModelReadWrite):
    def __init__(
        self,
        mean_values: Optional["np.ndarray"] = None,
        std_values: Optional["np.ndarray"] = None,
        n_samples_seen: Optional[int] = None,
    ) -> None:
        super().__init__()
        self.mean_values = mean_values
        self.std_values = std_values
        if std_values is not None:
            # if scale value is zero, replace it with 1.0 (for preventing division by zero)
            self.scale_values = np.where(std_values == 0.0, 1.0, std_values)
        self.n_samples_seen = n_samples_seen

    def _input_columns(self) -> List[str]:
        return [self.getInputCol()]

    def _output_columns(self) -> List[Tuple[str, str]]:
        return [(self.getOutputCol(), "array<double>")]

    def _get_transform_fn(self) -> Callable[..., Any]:
        mean_values = self.mean_values
        scale_values = self.scale_values

        def transform_fn(series: Any) -> Any:
            def map_value(x: "np.ndarray") -> "np.ndarray":
                return (x - mean_values) / scale_values

            return series.apply(map_value)

        return transform_fn

    def _get_core_model_filename(self) -> str:
        return self.__class__.__name__ + ".sklearn.pkl"

    def _save_core_model(self, path: str) -> None:
        from sklearn.preprocessing import StandardScaler as sk_StandardScaler

        sk_model = sk_StandardScaler(with_mean=True, with_std=True)
        sk_model.scale_ = self.scale_values
        sk_model.var_ = self.std_values * self.std_values  # type: ignore[operator]
        sk_model.mean_ = self.mean_values
        sk_model.n_features_in_ = len(self.std_values)  # type: ignore[arg-type]
        sk_model.n_samples_seen_ = self.n_samples_seen

        with open(path, "wb") as fp:
            pickle.dump(sk_model, fp)

    def _load_core_model(self, path: str) -> None:
        with open(path, "rb") as fp:
            sk_model = pickle.load(fp)

        self.std_values = np.sqrt(sk_model.var_)
        self.scale_values = sk_model.scale_
        self.mean_values = sk_model.mean_
        self.n_samples_seen = sk_model.n_samples_seen_
