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

from abc import ABCMeta, abstractmethod

import pandas as pd

from typing import (
    Any,
    Generic,
    List,
    Optional,
    TypeVar,
    Union,
    TYPE_CHECKING,
    Tuple,
    Callable,
)

from pyspark import since
from pyspark.ml.common import inherit_doc
from pyspark.sql.dataframe import DataFrame
from pyspark.ml.param import Params
from pyspark.ml.param.shared import (
    HasLabelCol,
    HasFeaturesCol,
    HasPredictionCol,
)
from pyspark.ml.connect.util import transform_dataframe_column

if TYPE_CHECKING:
    from pyspark.ml._typing import ParamMap

M = TypeVar("M", bound="Transformer")


@inherit_doc
class Estimator(Params, Generic[M], metaclass=ABCMeta):
    """
    Abstract class for estimators that fit models to data.

    .. versionadded:: 3.5.0
    """

    @abstractmethod
    def _fit(self, dataset: Union[DataFrame, pd.DataFrame]) -> M:
        """
        Fits a model to the input dataset. This is called by the default implementation of fit.


        Parameters
        ----------
        dataset : :py:class:`pyspark.sql.DataFrame`
            input dataset

        Returns
        -------
        :class:`Transformer`
            fitted model
        """
        raise NotImplementedError()

    def fit(
        self,
        dataset: Union[DataFrame, pd.DataFrame],
        params: Optional["ParamMap"] = None,
    ) -> Union[M, List[M]]:
        """
        Fits a model to the input dataset with optional parameters.

        .. versionadded:: 3.5.0

        Parameters
        ----------
        dataset : :py:class:`pyspark.sql.DataFrame` or py:class:`pandas.DataFrame`
            input dataset, it can be either pandas dataframe or spark dataframe.
        params : a dict of param values, optional
            an optional param map that overrides embedded params.

        Returns
        -------
        :py:class:`Transformer`
            fitted model
        """
        if params is None:
            params = dict()

        if isinstance(params, dict):
            if params:
                return self.copy(params)._fit(dataset)
            else:
                return self._fit(dataset)
        else:
            raise TypeError(
                "Params must be either a param map or a list/tuple of param maps, "
                "but got %s." % type(params)
            )


_SPARKML_TRANSFORMER_TMP_OUTPUT_COLNAME = "_sparkML_transformer_tmp_output"


@inherit_doc
class Transformer(Params, metaclass=ABCMeta):
    """
    Abstract class for transformers that transform one dataset into another.

    .. versionadded:: 3.5.0
    """

    def _input_columns(self) -> List[str]:
        """
        Return a list of input column names which are used as inputs of transformation.
        """
        raise NotImplementedError()

    def _output_columns(self) -> List[Tuple[str, str]]:
        """
        Return a list of output transformed columns, each elements in the list
        is a tuple of (column_name, column_spark_type)
        """
        raise NotImplementedError()

    def _get_transform_fn(self) -> Callable[["pd.Series"], Any]:
        """
        Return a transformation function that accepts an instance of `pd.Series` as input and
        returns transformed result as an instance of `pd.Series` or `pd.DataFrame`.
        If there's only one output column, the transformed result must be an
        instance of `pd.Series`, if there are multiple output columns, the transformed result
        must be an instance of `pd.DataFrame` with column names matching output schema
        returned by  `_output_columns` interface.
        """
        raise NotImplementedError()

    def transform(
        self, dataset: Union[DataFrame, pd.DataFrame], params: Optional["ParamMap"] = None
    ) -> Union[DataFrame, pd.DataFrame]:
        """
        Transforms the input dataset.
        The dataset can be either pandas dataframe or spark dataframe,
        if it is a spark DataFrame, the result of transformation is a new spark DataFrame
        that contains all existing columns and output columns with names.
        if it is a pandas DataFrame, the input pandas dataframe is appended with output
        columns in place.

        Note: Transformers does not allow output column having the same name with
        existing columns.

        Parameters
        ----------
        dataset : :py:class:`pyspark.sql.DataFrame` or py:class:`pandas.DataFrame`
            input dataset.

        params : dict, optional
            an optional param map that overrides embedded params.

        Returns
        -------
        :py:class:`pyspark.sql.DataFrame` or py:class:`pandas.DataFrame`
            transformed dataset, the type of output dataframe is consistent with
            input dataframe.
        """
        if params is None:
            params = dict()
        if isinstance(params, dict):
            if params:
                return self.copy(params)._transform(dataset)
            else:
                return self._transform(dataset)

    def _transform(self, dataset: Union[DataFrame, pd.DataFrame]) -> Union[DataFrame, pd.DataFrame]:
        input_cols = self._input_columns()
        transform_fn = self._get_transform_fn()
        output_cols = self._output_columns()

        existing_cols = list(dataset.columns)
        for col_name, _ in output_cols:
            if col_name in existing_cols:
                raise ValueError(
                    "Transformers does not allow output column having the same name with "
                    "existing columns."
                )

        return transform_dataframe_column(
            dataset,
            input_cols=input_cols,
            transform_fn=transform_fn,
            output_cols=output_cols,
        )


@inherit_doc
class Evaluator(Params, metaclass=ABCMeta):
    """
    Base class for evaluators that compute metrics from predictions.

    .. versionadded:: 3.5.0
    """

    @abstractmethod
    def _evaluate(self, dataset: Union["DataFrame", "pd.DataFrame"]) -> float:
        """
        Evaluates the output.

        Parameters
        ----------
        dataset : :py:class:`pyspark.sql.DataFrame`
            a dataset that contains labels/observations and predictions

        Returns
        -------
        float
            metric
        """
        raise NotImplementedError()

    def evaluate(self, dataset: DataFrame, params: Optional["ParamMap"] = None) -> float:
        """
        Evaluates the output with optional parameters.

        .. versionadded:: 3.5.0

        Parameters
        ----------
        dataset : :py:class:`pyspark.sql.DataFrame`
            a dataset that contains labels/observations and predictions
        params : dict, optional
            an optional param map that overrides embedded params

        Returns
        -------
        float
            metric
        """
        if params is None:
            params = dict()
        if isinstance(params, dict):
            if params:
                return self.copy(params)._evaluate(dataset)
            else:
                return self._evaluate(dataset)
        else:
            raise TypeError("Params must be a param map but got %s." % type(params))

    @since("1.5.0")
    def isLargerBetter(self) -> bool:
        """
        Indicates whether the metric returned by :py:meth:`evaluate` should be maximized
        (True, default) or minimized (False).
        A given evaluator may support multiple metrics which may be maximized or minimized.
        """
        raise NotImplementedError()


@inherit_doc
class Model(Transformer, metaclass=ABCMeta):
    """
    Abstract class for models that are fitted by estimators.

    .. versionadded:: 3.5.0
    """

    pass


@inherit_doc
class _PredictorParams(HasLabelCol, HasFeaturesCol, HasPredictionCol):
    """
    Params for :py:class:`Predictor` and :py:class:`PredictorModel`.

    .. versionadded:: 3.5.0
    """

    pass


@inherit_doc
class Predictor(Estimator[M], _PredictorParams, metaclass=ABCMeta):
    """
    Estimator for prediction tasks (regression and classification).
    """

    @since("3.5.0")
    def setLabelCol(self, value: str) -> "Predictor":
        """
        Sets the value of :py:attr:`labelCol`.
        """
        return self._set(labelCol=value)

    @since("3.5.0")
    def setFeaturesCol(self, value: str) -> "Predictor":
        """
        Sets the value of :py:attr:`featuresCol`.
        """
        return self._set(featuresCol=value)

    @since("3.5.0")
    def setPredictionCol(self, value: str) -> "Predictor":
        """
        Sets the value of :py:attr:`predictionCol`.
        """
        return self._set(predictionCol=value)


@inherit_doc
class PredictionModel(Model, _PredictorParams, metaclass=ABCMeta):
    """
    Model for prediction tasks (regression and classification).
    """

    @since("3.5.0")
    def setFeaturesCol(self, value: str) -> "PredictionModel":
        """
        Sets the value of :py:attr:`featuresCol`.
        """
        return self._set(featuresCol=value)

    @since("3.5.0")
    def setPredictionCol(self, value: str) -> "PredictionModel":
        """
        Sets the value of :py:attr:`predictionCol`.
        """
        return self._set(predictionCol=value)

    @property
    @abstractmethod
    @since("3.5.0")
    def numFeatures(self) -> int:
        """
        Returns the number of features the model was trained on. If unknown, returns -1
        """
        raise NotImplementedError()
