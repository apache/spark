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

import copy
import threading
import pandas as pd

from typing import (
    Any,
    Callable,
    Generic,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
    Union,
    cast,
    overload,
    TYPE_CHECKING,
)

from pyspark import since
from pyspark.ml.param import P
from pyspark.ml.common import inherit_doc
from pyspark.ml.param.shared import (
    HasInputCols,
    HasOutputCols,
    HasLabelCol,
    HasFeaturesCol,
    HasPredictionCol,
)
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import DataType, StructField, StructType
from pyspark.ml.param import Param, Params, TypeConverters
from pyspark.sql.functions import col, pandas_udf, struct
import pickle

from pyspark.mlv2.util import transform_dataframe_column

if TYPE_CHECKING:
    from pyspark.ml._typing import ParamMap

T = TypeVar("T")
M = TypeVar("M", bound="Transformer")


@inherit_doc
class Estimator(Params, Generic[M], metaclass=ABCMeta):
    """
    Abstract class for estimators that fit models to data.

    .. versionadded:: 1.3.0
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

        .. versionadded:: 1.3.0

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
    """

    def _input_column_name(self):
        """
        Return the name of the input column that is transformed.
        """
        raise NotImplementedError()

    def _output_columns(self):
        """
        Return a list of output transformed columns, each elements in the list
        is a tuple of (column_name, column_spark_type)
        """
        raise NotImplementedError()

    def _get_transform_fn(self):
        """
        Return a transformation function that accepts an instance of `pd.Series` as input and returns
        transformed result as an instance of `pd.Series` or `pd.DataFrame`
        """
        raise NotImplementedError()

    def transform(self, dataset: Union[DataFrame, pd.DataFrame]) -> Union[DataFrame, pd.DataFrame]:
        """
        Transforms the input dataset.
        The dataset can be either pandas dataframe or spark dataframe,
        if it is pandas dataframe, transforms the dataframe locally without creating spark jobs.

        Parameters
        ----------
        dataset : :py:class:`pyspark.sql.DataFrame` or py:class:`pandas.DataFrame`
            input dataset.

        Returns
        -------
        :py:class:`pyspark.sql.DataFrame` or py:class:`pandas.DataFrame`
            transformed dataset, the type of output dataframe is consistent with
            input dataframe.
        """
        input_col_name = self._input_column_name()
        transform_fn = self._get_transform_fn()
        output_cols = self._output_columns()

        return transform_dataframe_column(
            dataset,
            input_col_name=input_col_name,
            transform_fn=transform_fn,
            output_cols=output_cols
        )


@inherit_doc
class Evaluator(Params, metaclass=ABCMeta):
    """
    Base class for evaluators that compute metrics from predictions.

    .. versionadded:: 3.5.0
    """

    @abstractmethod
    def _evaluate(self, dataset: DataFrame) -> float:
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

        .. versionadded:: 1.4.0

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
        return True


@inherit_doc
class Model(Transformer, metaclass=ABCMeta):
    """
    Abstract class for models that are fitted by estimators.

    .. versionadded:: 1.4.0
    """

    pass
