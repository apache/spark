---
layout: global
title: PySpark Usage Guide for Pandas with Apache Arrow
displayTitle: PySpark Usage Guide for Pandas with Apache Arrow
license: |
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at
 
     http://www.apache.org/licenses/LICENSE-2.0
 
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
---

* Table of contents
{:toc}

## Apache Arrow in PySpark

Apache Arrow is an in-memory columnar data format that is used in Spark to efficiently transfer
data between JVM and Python processes. This currently is most beneficial to Python users that
work with Pandas/NumPy data. Its usage is not automatic and might require some minor
changes to configuration or code to take full advantage and ensure compatibility. This guide will
give a high-level description of how to use Arrow in Spark and highlight any differences when
working with Arrow-enabled data.

### Ensure PyArrow Installed

If you install PySpark using pip, then PyArrow can be brought in as an extra dependency of the
SQL module with the command `pip install pyspark[sql]`. Otherwise, you must ensure that PyArrow
is installed and available on all cluster nodes. The current supported version is 0.12.1.
You can install using pip or conda from the conda-forge channel. See PyArrow
[installation](https://arrow.apache.org/docs/python/install.html) for details.

## Enabling for Conversion to/from Pandas

Arrow is available as an optimization when converting a Spark DataFrame to a Pandas DataFrame
using the call `toPandas()` and when creating a Spark DataFrame from a Pandas DataFrame with
`createDataFrame(pandas_df)`. To use Arrow when executing these calls, users need to first set
the Spark configuration 'spark.sql.execution.arrow.enabled' to 'true'. This is disabled by default.

In addition, optimizations enabled by 'spark.sql.execution.arrow.enabled' could fallback automatically
to non-Arrow optimization implementation if an error occurs before the actual computation within Spark.
This can be controlled by 'spark.sql.execution.arrow.fallback.enabled'.

<div class="codetabs">
<div data-lang="python" markdown="1">
{% include_example dataframe_with_arrow python/sql/arrow.py %}
</div>
</div>

Using the above optimizations with Arrow will produce the same results as when Arrow is not
enabled. Note that even with Arrow, `toPandas()` results in the collection of all records in the
DataFrame to the driver program and should be done on a small subset of the data. Not all Spark
data types are currently supported and an error can be raised if a column has an unsupported type,
see [Supported SQL Types](#supported-sql-types). If an error occurs during `createDataFrame()`,
Spark will fall back to create the DataFrame without Arrow.

## Pandas UDFs (a.k.a. Vectorized UDFs)

Pandas UDFs are user defined functions that are executed by Spark using Arrow to transfer data and
Pandas to work with the data. A Pandas UDF is defined using the keyword `pandas_udf` as a decorator
or to wrap the function, no additional configuration is required. Currently, there are two types of
Pandas UDF: Scalar and Grouped Map.

### Scalar

Scalar Pandas UDFs are used for vectorizing scalar operations. They can be used with functions such
as `select` and `withColumn`. The Python function should take `pandas.Series` as inputs and return
a `pandas.Series` of the same length. Internally, Spark will execute a Pandas UDF by splitting
columns into batches and calling the function for each batch as a subset of the data, then
concatenating the results together.

The following example shows how to create a scalar Pandas UDF that computes the product of 2 columns.

<div class="codetabs">
<div data-lang="python" markdown="1">
{% include_example scalar_pandas_udf python/sql/arrow.py %}
</div>
</div>

### Grouped Map
Grouped map Pandas UDFs are used with `groupBy().apply()` which implements the "split-apply-combine" pattern.
Split-apply-combine consists of three steps:
* Split the data into groups by using `DataFrame.groupBy`.
* Apply a function on each group. The input and output of the function are both `pandas.DataFrame`. The
  input data contains all the rows and columns for each group.
* Combine the results into a new `DataFrame`.

To use `groupBy().apply()`, the user needs to define the following:
* A Python function that defines the computation for each group.
* A `StructType` object or a string that defines the schema of the output `DataFrame`.

The column labels of the returned `pandas.DataFrame` must either match the field names in the
defined output schema if specified as strings, or match the field data types by position if not
strings, e.g. integer indices. See [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.html#pandas.DataFrame)
on how to label columns when constructing a `pandas.DataFrame`.

Note that all data for a group will be loaded into memory before the function is applied. This can
lead to out of memory exceptions, especially if the group sizes are skewed. The configuration for
[maxRecordsPerBatch](#setting-arrow-batch-size) is not applied on groups and it is up to the user
to ensure that the grouped data will fit into the available memory.

The following example shows how to use `groupby().apply()` to subtract the mean from each value in the group.

<div class="codetabs">
<div data-lang="python" markdown="1">
{% include_example grouped_map_pandas_udf python/sql/arrow.py %}
</div>
</div>

For detailed usage, please see [`pyspark.sql.functions.pandas_udf`](api/python/pyspark.sql.html#pyspark.sql.functions.pandas_udf) and
[`pyspark.sql.GroupedData.apply`](api/python/pyspark.sql.html#pyspark.sql.GroupedData.apply).

### Grouped Aggregate

Grouped aggregate Pandas UDFs are similar to Spark aggregate functions. Grouped aggregate Pandas UDFs are used with `groupBy().agg()` and
[`pyspark.sql.Window`](api/python/pyspark.sql.html#pyspark.sql.Window). It defines an aggregation from one or more `pandas.Series`
to a scalar value, where each `pandas.Series` represents a column within the group or window.

Note that this type of UDF does not support partial aggregation and all data for a group or window will be loaded into memory. Also,
only unbounded window is supported with Grouped aggregate Pandas UDFs currently.

The following example shows how to use this type of UDF to compute mean with groupBy and window operations:

<div class="codetabs">
<div data-lang="python" markdown="1">
{% include_example grouped_agg_pandas_udf python/sql/arrow.py %}
</div>
</div>

For detailed usage, please see [`pyspark.sql.functions.pandas_udf`](api/python/pyspark.sql.html#pyspark.sql.functions.pandas_udf)

## Usage Notes

### Supported SQL Types

Currently, all Spark SQL data types are supported by Arrow-based conversion except `MapType`,
`ArrayType` of `TimestampType`, and nested `StructType`.

### Setting Arrow Batch Size

Data partitions in Spark are converted into Arrow record batches, which can temporarily lead to
high memory usage in the JVM. To avoid possible out of memory exceptions, the size of the Arrow
record batches can be adjusted by setting the conf "spark.sql.execution.arrow.maxRecordsPerBatch"
to an integer that will determine the maximum number of rows for each batch. The default value is
10,000 records per batch. If the number of columns is large, the value should be adjusted
accordingly. Using this limit, each data partition will be made into 1 or more record batches for
processing.

### Timestamp with Time Zone Semantics

Spark internally stores timestamps as UTC values, and timestamp data that is brought in without
a specified time zone is converted as local time to UTC with microsecond resolution. When timestamp
data is exported or displayed in Spark, the session time zone is used to localize the timestamp
values. The session time zone is set with the configuration 'spark.sql.session.timeZone' and will
default to the JVM system local time zone if not set. Pandas uses a `datetime64` type with nanosecond
resolution, `datetime64[ns]`, with optional time zone on a per-column basis.

When timestamp data is transferred from Spark to Pandas it will be converted to nanoseconds
and each column will be converted to the Spark session time zone then localized to that time
zone, which removes the time zone and displays values as local time. This will occur
when calling `toPandas()` or `pandas_udf` with timestamp columns.

When timestamp data is transferred from Pandas to Spark, it will be converted to UTC microseconds. This
occurs when calling `createDataFrame` with a Pandas DataFrame or when returning a timestamp from a
`pandas_udf`. These conversions are done automatically to ensure Spark will have data in the
expected format, so it is not necessary to do any of these conversions yourself. Any nanosecond
values will be truncated.

Note that a standard UDF (non-Pandas) will load timestamp data as Python datetime objects, which is
different than a Pandas timestamp. It is recommended to use Pandas time series functionality when
working with timestamps in `pandas_udf`s to get the best performance, see
[here](https://pandas.pydata.org/pandas-docs/stable/timeseries.html) for details.
