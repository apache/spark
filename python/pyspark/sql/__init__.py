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

"""
Important classes of Spark SQL and DataFrames:

    - :class:`pyspark.sql.SparkSession`
      Main entry point for :class:`DataFrame` and SQL functionality.
    - :class:`pyspark.sql.DataFrame`
      A distributed collection of data grouped into named columns.
    - :class:`pyspark.sql.Column`
      A column expression in a :class:`DataFrame`.
    - :class:`pyspark.sql.Row`
      A row of data in a :class:`DataFrame`.
    - :class:`pyspark.sql.GroupedData`
      Aggregation methods, returned by :func:`DataFrame.groupBy`.
    - :class:`pyspark.sql.DataFrameNaFunctions`
      Methods for handling missing data (null values).
    - :class:`pyspark.sql.DataFrameStatFunctions`
      Methods for statistics functionality.
    - :class:`pyspark.sql.functions`
      List of built-in functions available for :class:`DataFrame`.
    - :class:`pyspark.sql.types`
      List of data types available.
    - :class:`pyspark.sql.Window`
      For working with window functions.
"""
from pyspark.sql.types import Row, VariantVal
from pyspark.sql.context import SQLContext, HiveContext, UDFRegistration, UDTFRegistration
from pyspark.sql.session import SparkSession
from pyspark.sql.column import Column
from pyspark.sql.catalog import Catalog
from pyspark.sql.dataframe import DataFrame, DataFrameNaFunctions, DataFrameStatFunctions
from pyspark.sql.group import GroupedData
from pyspark.sql.observation import Observation
from pyspark.sql.readwriter import DataFrameReader, DataFrameWriter, DataFrameWriterV2
from pyspark.sql.merge import MergeIntoWriter
from pyspark.sql.window import Window, WindowSpec
from pyspark.sql.pandas.group_ops import PandasCogroupedOps
from pyspark.sql.utils import is_remote


__all__ = [
    "SparkSession",
    "SQLContext",
    "HiveContext",
    "UDFRegistration",
    "UDTFRegistration",
    "DataFrame",
    "GroupedData",
    "Column",
    "Catalog",
    "Observation",
    "Row",
    "DataFrameNaFunctions",
    "DataFrameStatFunctions",
    "VariantVal",
    "Window",
    "WindowSpec",
    "DataFrameReader",
    "DataFrameWriter",
    "DataFrameWriterV2",
    "MergeIntoWriter",
    "PandasCogroupedOps",
    "is_remote",
]
