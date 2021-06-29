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

from typing import Union

from pyspark.sql.pandas._typing import (
    GroupedMapPandasUserDefinedFunction,
    PandasGroupedMapFunction,
    PandasCogroupedMapFunction,
)

from pyspark import since as since  # noqa: F401
from pyspark.rdd import PythonEvalType as PythonEvalType  # noqa: F401
from pyspark.sql.column import Column as Column  # noqa: F401
from pyspark.sql.context import SQLContext
import pyspark.sql.group
from pyspark.sql.dataframe import DataFrame as DataFrame
from pyspark.sql.types import StructType

class PandasGroupedOpsMixin:
    def cogroup(self, other: pyspark.sql.group.GroupedData) -> PandasCogroupedOps: ...
    def apply(self, udf: GroupedMapPandasUserDefinedFunction) -> DataFrame: ...
    def applyInPandas(
        self, func: PandasGroupedMapFunction, schema: Union[StructType, str]
    ) -> DataFrame: ...

class PandasCogroupedOps:
    sql_ctx: SQLContext
    def __init__(
        self, gd1: pyspark.sql.group.GroupedData, gd2: pyspark.sql.group.GroupedData
    ) -> None: ...
    def applyInPandas(
        self, func: PandasCogroupedMapFunction, schema: Union[StructType, str]
    ) -> DataFrame: ...
