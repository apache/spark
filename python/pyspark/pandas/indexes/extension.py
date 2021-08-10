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

from typing import Any, Callable, Optional, Union

import pandas as pd
import numpy as np

from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DataType

from pyspark.pandas.indexes.base import Index
from pyspark.pandas.internal import SPARK_DEFAULT_INDEX_NAME
from pyspark.pandas.typedef.typehints import as_spark_type, infer_pd_series_spark_type


# TODO: Implement na_action similar functionality to pandas
# NB: Passing return_type into class cause Serialisation errors; instead pass at method level
class MapExtension:
    def __init__(self, index: Index, na_action: Optional[str] = None):
        self._index = index
        if na_action is not None:
            raise NotImplementedError("Currently do not support na_action functionality")
        else:
            self._na_action = na_action

    def map(self, mapper: Union[dict, Callable[[Any], Any], pd.Series]) -> Index:
        """
        Single callable/entry point to map Index values.

        Parameters
        ----------
        mapper: function, dict, or pd.Series
            Mapping correspondence.

        Returns
        -------
        Index
        """
        if isinstance(mapper, dict):
            return self._map_dict(mapper)
        elif isinstance(mapper, pd.Series):
            return self._map_series(mapper)
        elif isinstance(mapper, Callable):
            return self._map_lambda(mapper)
        else:
            raise TypeError("mapper can only be function, dict, or pd.Series.")

    def _map_dict(self, mapper: dict) -> Index:
        """
        Helper method that maps Index values when mapper is in dict type.

        .. note:: Default return value for missing elements is np.nan

        Parameters
        ----------
        mapper: dict
            Key-value pairs that are used to instruct mapping from index value
            to new value

        Returns
        -------
        Index
        """
        return_type = self._mapper_return_type(mapper)

        @pandas_udf(return_type)
        def pyspark_mapper(col: pd.Index) -> pd.Index:
            return col.apply(lambda i: mapper.get(i, np.nan))  # type: ignore

        return self._index._with_new_scol(pyspark_mapper(SPARK_DEFAULT_INDEX_NAME))

    def _map_series(self, mapper: pd.Series) -> Index:
        """
        Helper method that maps Index values when mapper is in pd.Series type.

        Parameters
        ----------
        mapper: pandas.Series
            Series that is used to instruct mapping from index value to new value

        Returns
        -------
        Index
        """
        return_type = self._mapper_return_type(mapper)

        def getOrElse(input: pd.Series, pos):
            try:
                return input.loc[pos]
            except:
                return None

        @pandas_udf(return_type)
        def pyspark_mapper(col: pd.Index) -> pd.Index:
            return col.apply(lambda i: getOrElse(mapper, i))

        return self._index._with_new_scol(pyspark_mapper(SPARK_DEFAULT_INDEX_NAME))

    def _map_lambda(self, mapper: Callable[[Any], Any]) -> Index:
        """
        Helper method that maps Index values when mapper is a generic lambda function.

        Parameters
        ----------
        mapper: function
            Generic lambda function to apply to index

        Returns
        -------
        Index
        """
        return_type = self._mapper_return_type(mapper)

        @pandas_udf(return_type)
        def pyspark_mapper(col: pd.Index) -> pd.Index:
            return col.apply(mapper)

        return self._index._with_new_scol(scol=pyspark_mapper(SPARK_DEFAULT_INDEX_NAME))

    def _mapper_return_type(self, mapper: Union[dict, Callable[[Any], Any], pd.Series]) -> DataType:
        """
        Helper method to get the mapper's return type. The return type is required for
        the pandas_udf.

        Parameters
        ----------
        mapper: function, dict, or pd.Series

        Returns
        -------
        Spark DataType
        """
        if isinstance(mapper, dict):
            return as_spark_type(type(list(mapper.values())[0]))
        elif isinstance(mapper, pd.Series):
            return infer_pd_series_spark_type(mapper, mapper.dtype)
        else:
            return as_spark_type(type(mapper(self._index.min())))
