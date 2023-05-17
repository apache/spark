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
class Transformer(Params, HasInputCols, HasOutputCols, metaclass=ABCMeta):
    """
    Abstract class for transformers that transform one dataset into another.
    """
    def _get_transform_func_and_output_schema(self):
        """
        Returns a function defined like:
        `transform_func(input: pd.DataFrame) -> pd.DataFrame`
        the transforming function accepts a pandas DataFrame input,
        the input columns must be consistent with "inputCols" param,
        and generate a new pandas DataFrame as transformation results,
        output columns must be consistent with "outputCols" param.
        and returns an output pyspark schema for the output columns.
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
        transform_func, output_spark_schema = self._get_transform_func_and_output_schema()

        if isinstance(dataset, pd.DataFrame):
            input_cols = self.getInputCols()
            selected_dataset = dataset[input_cols]
            return transform_func(selected_dataset)

        @pandas_udf(returnType=output_spark_schema)
        def pyspark_transform_udf(s: pd.DataFrame) -> pd.DataFrame:
            return transform_func(s)

        return dataset.withColumn(
            _SPARKML_TRANSFORMER_TMP_OUTPUT_COLNAME,
            pyspark_transform_udf(struct(*self.getInputCols()))
        ).withColumn(
            [f"{_SPARKML_TRANSFORMER_TMP_OUTPUT_COLNAME}.{output_col}"]
            for output_col in self.getOutputCols()
        ).drop(_SPARKML_TRANSFORMER_TMP_OUTPUT_COLNAME)


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
