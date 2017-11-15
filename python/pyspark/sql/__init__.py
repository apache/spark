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
from __future__ import absolute_import


from pyspark.sql.types import Row
from pyspark.sql.context import SQLContext, HiveContext, UDFRegistration
from pyspark.sql.session import SparkSession
from pyspark.sql.column import Column
from pyspark.sql.catalog import Catalog
from pyspark.sql.dataframe import DataFrame, DataFrameNaFunctions, DataFrameStatFunctions
from pyspark.sql.group import GroupedData
from pyspark.sql.readwriter import DataFrameReader, DataFrameWriter
from pyspark.sql.window import Window, WindowSpec


__all__ = [
    'SparkSession', 'SQLContext', 'HiveContext', 'UDFRegistration',
    'DataFrame', 'GroupedData', 'Column', 'Catalog', 'Row',
    'DataFrameNaFunctions', 'DataFrameStatFunctions', 'Window', 'WindowSpec',
    'DataFrameReader', 'DataFrameWriter'
]
