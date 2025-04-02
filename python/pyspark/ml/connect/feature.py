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

import pickle
from typing import Any, Union, List, Tuple, Callable, Dict, Optional

import numpy as np
import pandas as pd

from pyspark import keyword_only
from pyspark.sql import DataFrame
from pyspark.ml.param.shared import (
    HasInputCol,
    HasInputCols,
    HasOutputCol,
    HasFeatureSizes,
    HasHandleInvalid,
    Param,
    Params,
    TypeConverters,
)
from pyspark.ml.connect.base import Estimator, Model, Transformer
from pyspark.ml.connect.io_utils import ParamsReadWrite, CoreModelReadWrite
from pyspark.ml.connect.summarizer import summarize_dataframe


class MaxAbsScaler(Estimator, HasInputCol, HasOutputCol, ParamsReadWrite):
    """
    Rescale each feature individually to range [-1, 1] by dividing through the largest maximum
    absolute value in each feature. It does not shift/center the data, and thus does not destroy
    any sparsity.

    .. versionadded:: 3.5.0

    .. deprecated:: 4.0.0

    Examples
    --------
    >>> from pyspark.ml.connect.feature import MaxAbsScaler
    >>> scaler = MaxAbsScaler(inputCol='features', outputCol='scaled_features')
    >>> dataset = spark.createDataFrame([
    ...     ([1.0, 2.0],),
    ...     ([2.0, -1.0],),
    ...     ([-3.0, -2.0],),
    ... ], schema=['features'])
    >>> scaler_model = scaler.fit(dataset)
    >>> transformed_dataset = scaler_model.transform(dataset)
    >>> transformed_dataset.show(truncate=False)
    +------------+--------------------------+
    |features    |scaled_features           |
    +------------+--------------------------+
    |[1.0, 2.0]  |[0.3333333333333333, 1.0] |
    |[2.0, -1.0] |[0.6666666666666666, -0.5]|
    |[-3.0, -2.0]|[-1.0, -1.0]              |
    +------------+--------------------------+
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
    """
    Model fitted by MaxAbsScaler.

    .. versionadded:: 3.5.0

    .. deprecated:: 4.0.0
    """

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

    .. versionadded:: 3.5.0

    .. deprecated:: 4.0.0

    Examples
    --------
    >>> from pyspark.ml.connect.feature import StandardScaler
    >>> scaler = StandardScaler(inputCol='features', outputCol='scaled_features')
    >>> dataset = spark.createDataFrame([
    ...     ([1.0, 2.0],),
    ...     ([2.0, -1.0],),
    ...     ([-3.0, -2.0],),
    ... ], schema=['features'])
    >>> scaler_model = scaler.fit(dataset)
    >>> transformed_dataset = scaler_model.transform(dataset)
    >>> transformed_dataset.show(truncate=False)
    +------------+------------------------------------------+
    |features    |scaled_features                           |
    +------------+------------------------------------------+
    |[1.0, 2.0]  |[0.3779644730092272, 1.1208970766356101]  |
    |[2.0, -1.0] |[0.7559289460184544, -0.3202563076101743] |
    |[-3.0, -2.0]|[-1.1338934190276817, -0.8006407690254358]|
    +------------+------------------------------------------+
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
    """
    Model fitted by StandardScaler.

    .. versionadded:: 3.5.0

    .. deprecated:: 4.0.0
    """

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


class ArrayAssembler(
    Transformer,
    HasInputCols,
    HasOutputCol,
    HasFeatureSizes,
    HasHandleInvalid,
    ParamsReadWrite,
):
    """
    A feature transformer that merges multiple input columns into an array type column.

    Parameters
    ----------
    You need to set param `inputCols` for specifying input column names,
    and set param `featureSizes` for specifying corresponding input column
    feature size, for scalar type input column, corresponding feature size must be set to 1,
    otherwise, set corresponding feature size to feature array length.
    Output column is "array<double"> type and contains array of assembled features.
    All elements in input feature columns must be convertible to double type.

    You can set 'handler_invalid' param to specify how to handle invalid input value
    (None or NaN), if it is set to 'error', error is thrown for invalid input value,
    if it is set to 'keep', it returns relevant number of NaN in the output.

    .. versionadded:: 4.0.0

    .. deprecated:: 4.0.0

    Examples
    --------
    >>> from pyspark.ml.connect.feature import ArrayAssembler
    >>> import numpy as np
    >>>
    >>> spark_df = spark.createDataFrame(
    ...     [
    ...         ([2.0, 3.5, 1.5], 3.0, True, 1),
    ...         ([-3.0, np.nan, -2.5], 4.0, False, 2),
    ...     ],
    ...     schema=["f1", "f2", "f3", "f4"],
    ... )
    >>> assembler = ArrayAssembler(
    ...     inputCols=["f1", "f2", "f3", "f4"],
    ...     outputCol="out",
    ...     featureSizes=[3, 1, 1, 1],
    ...     handleInvalid="keep",
    ... )
    >>> assembler.transform(spark_df).select("out").show(truncate=False)
    """

    _input_kwargs: Dict[str, Any]

    # Override doc of handleInvalid param.
    handleInvalid: Param[str] = Param(
        Params._dummy(),
        "handleInvalid",
        "how to handle invalid entries. Options are 'error' (throw an error), "
        "or 'keep' (return relevant number of NaN in the output). Default value "
        "is 'error'",
        typeConverter=TypeConverters.toString,
    )

    @keyword_only
    def __init__(
        self,
        *,
        inputCols: Optional[List[str]] = None,
        outputCol: Optional[str] = None,
        featureSizes: Optional[List[int]] = None,
        handleInvalid: Optional[str] = "error",
    ) -> None:
        """
        __init__(
            self, \\*, inputCols=None, outputCol=None, featureSizes=None, handleInvalid="error"
        )
        """
        super().__init__()
        kwargs = self._input_kwargs
        self._set(**kwargs)
        self._setDefault(handleInvalid="error")

    def _input_columns(self) -> List[str]:
        return self.getInputCols()

    def _output_columns(self) -> List[Tuple[str, str]]:
        return [(self.getOutputCol(), "array<double>")]

    def _get_transform_fn(self) -> Callable[..., Any]:
        feature_size_list = self.getFeatureSizes()
        if feature_size_list is None or len(feature_size_list) != len(self.getInputCols()):
            raise ValueError(
                "'feature_size_list' param must be set with an array of integer, and"
                "its length must be equal to number of input columns."
            )
        for feature_size in feature_size_list:
            if feature_size <= 0:
                raise ValueError("All input feature sizes must be an positive integer.")

        assembled_feature_size = sum(feature_size_list)
        handler_invalid = self.getHandleInvalid()

        if handler_invalid not in ["error", "keep"]:
            raise ValueError("'handler_invalid' param must be set with 'error' or 'keep' value.")

        keep_invalid = handler_invalid == "keep"

        def assemble_features(*feature_list: Any) -> Any:
            assembled_array = np.empty(assembled_feature_size, dtype=np.float64)
            pos = 0
            for index, feature in enumerate(feature_list):
                feature_size = feature_size_list[index]

                if feature is not None:
                    if np.isscalar(feature) and feature_size != 1:
                        raise ValueError(
                            f"The {index + 1}th input feature is a scalar value, but provided "
                            f"feature size is {feature_size}."
                        )
                    if not np.isscalar(feature) and len(feature) != feature_size:
                        raise ValueError(
                            f"The {index + 1}th input feature size does not match "
                            f"with provided feature size {feature_size}."
                        )
                if keep_invalid:
                    if feature is None:
                        assembled_array[pos : pos + feature_size] = np.nan
                    else:
                        assembled_array[pos : pos + feature_size] = feature
                else:
                    if feature is None or np.isnan(feature).any():
                        raise ValueError(
                            f"The input features contains invalid value: {str(feature)}"
                        )
                    else:
                        assembled_array[pos : pos + feature_size] = feature

                pos += feature_size

            return assembled_array

        def transform_fn(*series_list: Any) -> Any:
            return pd.Series(assemble_features(*feature_list) for feature_list in zip(*series_list))

        return transform_fn
