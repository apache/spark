#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from typing import overload
from typing import Any, Iterable, List, Optional, Tuple, Union

from pyspark.sql.pandas._typing import DataFrameLike
from pyspark import since as since
from pyspark.rdd import RDD
import pyspark.sql.dataframe
from pyspark.sql.pandas.serializers import (
    ArrowCollectSerializer as ArrowCollectSerializer,
)
from pyspark.sql.types import *
from pyspark.traceback_utils import SCCallSiteSync as SCCallSiteSync

class PandasConversionMixin:
    def toPandas(self) -> DataFrameLike: ...

class SparkConversionMixin:
    @overload
    def createDataFrame(
        self, data: DataFrameLike, samplingRatio: Optional[float] = ...
    ) -> pyspark.sql.dataframe.DataFrame: ...
    @overload
    def createDataFrame(
        self,
        data: DataFrameLike,
        schema: Union[StructType, str],
        verifySchema: bool = ...,
    ) -> pyspark.sql.dataframe.DataFrame: ...
