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
    Abstract class for Evaluators
    """
    def _get_input_col_map(self):
        """
        Return a dict that maps raw input column name to column name that metric
        function needed.

        e.g., for metric like `accuracy`, it requires `label` column and `prediction` column as
        input, supposing user input DataFrame contains 3 columns:
         abc_features, def_label, xyz_prediction
        then this function should return a dict of:
        {"def_label": "label", "xyz_prediction", "prediction"
        and the `update_status` function returned by `_get_metric_functions` must accept a
        pandas DataFrame containing columns of "label" and "prediction".
        """
        raise NotImplementedError()

    def _get_metric_functions(self):
        """
        Returns a tuple of functions: (init_status, update_status, merge_status, status_to_metric),
        the `init_status` function must have signature like:
        `def init_status() -> XX_METRIC_EVAL_STATUS`

        the `update_status` function must have signature like:
        `def update_status(status: XX_METRIC_EVAL_STATUS, input: pd.DataFrame)`
        `update_status` function is an in-place op

        the `merge_status` function must have signature like:
        `def merge_status(x: XX_METRIC_EVAL_STATUS, y: XX_METRIC_EVAL_STATUS)
        `merge_status` function is an in-place op, i.e., status `y` is merged to status `x`.
        and status must be and pickle-able.

        the `status_to_metric` function must have signature like:
        `def status_to_metric(status: XX_METRIC_EVAL_STATUS) -> METRIC_VALUE_TYPE`
        """
        raise NotImplementedError()

    def evaluate(self, dataset: Union[DataFrame, pd.DataFrame]) -> Any:
        """
        Evaluate metric over a spark DataFrame.
        return metric value.
        """
        if isinstance(dataset, pd.DataFrame):
            input_col_map = self._get_input_col_map()
            input_cols = list(input_col_map.keys())

            dataset = dataset[input_cols].rename(input_col_map)

            init_status, update_status, merge_status, status_to_metric = self._get_metric_functions()

            status = init_status()
            update_status(status, dataset)
            return status_to_metric(status)

        dataset = dataset.select(
            **[col(raw_input_col).alias(input_col)
               for raw_input_col, input_col in self._get_input_col_map()]
        )

        init_status, update_status, merge_status, status_to_metric = self._get_metric_functions()

        def compute_status(iterator):
            status = init_status()

            for pdf in iterator:
                update_status(status, pdf)

            return pd.DataFrame({'status': [pickle.dumps(status)]})

        result_pdf = dataset.mapInPandas(compute_status, schema='status binary').toPandas()

        merged_status = None
        for s in result_pdf.status:
            s = pickle.loads(s)
            if merged_status is None:
                merged_status = s
            else:
                merge_status(merged_status, s)

        return status_to_metric(merged_status)


@inherit_doc
class Model(Transformer, metaclass=ABCMeta):
    """
    Abstract class for models that are fitted by estimators.

    .. versionadded:: 1.4.0
    """

    pass
