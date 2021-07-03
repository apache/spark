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
from typing import Optional, Union

from pyspark.sql.pandas._typing import DataFrameLike
from pyspark import since as since  # noqa: F401
from pyspark.rdd import RDD  # noqa: F401
import pyspark.sql.dataframe
from pyspark.sql.pandas.serializers import (  # noqa: F401
    ArrowCollectSerializer as ArrowCollectSerializer,
)
from pyspark.sql.types import (  # noqa: F401
    BooleanType as BooleanType,
    ByteType as ByteType,
    DataType as DataType,
    DoubleType as DoubleType,
    FloatType as FloatType,
    IntegerType as IntegerType,
    IntegralType as IntegralType,
    LongType as LongType,
    ShortType as ShortType,
    StructType as StructType,
    TimestampType as TimestampType,
)
from pyspark.traceback_utils import SCCallSiteSync as SCCallSiteSync  # noqa: F401

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
