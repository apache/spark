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

To use Apache Arrow in PySpark, [the recommended version of PyArrow](#recommended-pandas-and-pyarrow-versions)
should be installed.
If you install PySpark using pip, then PyArrow can be brought in as an extra dependency of the
SQL module with the command `pip install pyspark[sql]`. Otherwise, you must ensure that PyArrow
is installed and available on all cluster nodes.
You can install using pip or conda from the conda-forge channel. See PyArrow
[installation](https://arrow.apache.org/docs/python/install.html) for details.

## Enabling for Conversion to/from Pandas

Arrow is available as an optimization when converting a Spark DataFrame to a Pandas DataFrame
using the call `toPandas()` and when creating a Spark DataFrame from a Pandas DataFrame with
`createDataFrame(pandas_df)`. To use Arrow when executing these calls, users need to first set
the Spark configuration `spark.sql.execution.arrow.pyspark.enabled` to `true`. This is disabled by default.

In addition, optimizations enabled by `spark.sql.execution.arrow.pyspark.enabled` could fallback automatically
to non-Arrow optimization implementation if an error occurs before the actual computation within Spark.
This can be controlled by `spark.sql.execution.arrow.pyspark.fallback.enabled`.

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

Pandas UDFs are user defined functions that are executed by Spark using
Arrow to transfer data and Pandas to work with the data, which allows vectorized operations. A Pandas
UDF is defined using the `pandas_udf` as a decorator or to wrap the function, and no additional
configuration is required. A Pandas UDF behaves as a regular PySpark function API in general.

Before Spark 3.0, Pandas UDFs used to be defined with `PandasUDFType`. From Spark 3.0
with Python 3.6+, you can also use [Python type hints](https://www.python.org/dev/peps/pep-0484).
Using Python type hints are preferred and using `PandasUDFType` will be deprecated in
the future release.

Note that the type hint should use `pandas.Series` in all cases but there is one variant
that `pandas.DataFrame` should be used for its input or output type hint instead when the input
or output column is of `StructType`. The following example shows a Pandas UDF which takes long
column, string column and struct column, and outputs a struct column. It requires the function to
specify the type hints of `pandas.Series` and `pandas.DataFrame` as below:

<p>
<div class="codetabs">
<div data-lang="python" markdown="1">
{% include_example ser_to_frame_pandas_udf python/sql/arrow.py %}
</div>
</div>
</p>

In the following sections, it describes the combinations of the supported type hints. For simplicity,
`pandas.DataFrame` variant is omitted.

### Series to Series

The type hint can be expressed as `pandas.Series`, ... -> `pandas.Series`.

By using `pandas_udf` with the function having such type hints above, it creates a Pandas UDF where the given
function takes one or more `pandas.Series` and outputs one `pandas.Series`. The output of the function should
always be of the same length as the input. Internally, PySpark will execute a Pandas UDF by splitting
columns into batches and calling the function for each batch as a subset of the data, then concatenating
the results together.

The following example shows how to create this Pandas UDF that computes the product of 2 columns.

<div class="codetabs">
<div data-lang="python" markdown="1">
{% include_example ser_to_ser_pandas_udf python/sql/arrow.py %}
</div>
</div>

For detailed usage, please see [`pyspark.sql.functions.pandas_udf`](api/python/pyspark.sql.html#pyspark.sql.functions.pandas_udf)

### Iterator of Series to Iterator of Series

The type hint can be expressed as `Iterator[pandas.Series]` -> `Iterator[pandas.Series]`.

By using `pandas_udf` with the function having such type hints above, it creates a Pandas UDF where the given
function takes an iterator of `pandas.Series` and outputs an iterator of `pandas.Series`. The
length of the entire output from the function should be the same length of the entire input; therefore, it can
prefetch the data from the input iterator as long as the lengths are the same.
In this case, the created Pandas UDF requires one input column when the Pandas UDF is called. To use
multiple input columns, a different type hint is required. See Iterator of Multiple Series to Iterator
of Series.

It is also useful when the UDF execution requires initializing some states although internally it works
identically as Series to Series case. The pseudocode below illustrates the example.

{% highlight python %}
@pandas_udf("long")
def calculate(iterator: Iterator[pd.Series]) -> Iterator[pd.Series]:
    # Do some expensive initialization with a state
    state = very_expensive_initialization()
    for x in iterator:
        # Use that state for whole iterator.
        yield calculate_with_state(x, state)

df.select(calculate("value")).show()
{% endhighlight %}

The following example shows how to create this Pandas UDF:

<div class="codetabs">
<div data-lang="python" markdown="1">
{% include_example iter_ser_to_iter_ser_pandas_udf python/sql/arrow.py %}
</div>
</div>

For detailed usage, please see [`pyspark.sql.functions.pandas_udf`](api/python/pyspark.sql.html#pyspark.sql.functions.pandas_udf)

### Iterator of Multiple Series to Iterator of Series

The type hint can be expressed as `Iterator[Tuple[pandas.Series, ...]]` -> `Iterator[pandas.Series]`.

By using `pandas_udf` with the function having such type hints above, it creates a Pandas UDF where the
given function takes an iterator of a tuple of multiple `pandas.Series` and outputs an iterator of `pandas.Series`.
In this case, the created pandas UDF requires multiple input columns as many as the series in the tuple
when the Pandas UDF is called. Otherwise, it has the same characteristics and restrictions as Iterator of Series
to Iterator of Series case.

The following example shows how to create this Pandas UDF:

<div class="codetabs">
<div data-lang="python" markdown="1">
{% include_example iter_sers_to_iter_ser_pandas_udf python/sql/arrow.py %}
</div>
</div>

For detailed usage, please see [`pyspark.sql.functions.pandas_udf`](api/python/pyspark.sql.html#pyspark.sql.functions.pandas_udf)

### Series to Scalar

The type hint can be expressed as `pandas.Series`, ... -> `Any`.

By using `pandas_udf` with the function having such type hints above, it creates a Pandas UDF similar
to PySpark's aggregate functions. The given function takes `pandas.Series` and returns a scalar value.
The return type should be a primitive data type, and the returned scalar can be either a python
primitive type, e.g., `int` or `float` or a numpy data type, e.g., `numpy.int64` or `numpy.float64`.
`Any` should ideally be a specific scalar type accordingly.

This UDF can be also used with `groupBy().agg()` and [`pyspark.sql.Window`](api/python/pyspark.sql.html#pyspark.sql.Window).
It defines an aggregation from one or more `pandas.Series` to a scalar value, where each `pandas.Series`
represents a column within the group or window.

Note that this type of UDF does not support partial aggregation and all data for a group or window
will be loaded into memory. Also, only unbounded window is supported with Grouped aggregate Pandas
UDFs currently. The following example shows how to use this type of UDF to compute mean with a group-by
and window operations:

<div class="codetabs">
<div data-lang="python" markdown="1">
{% include_example ser_to_scalar_pandas_udf python/sql/arrow.py %}
</div>
</div>

For detailed usage, please see [`pyspark.sql.functions.pandas_udf`](api/python/pyspark.sql.html#pyspark.sql.functions.pandas_udf)


## Pandas Function APIs

Pandas Function APIs can directly apply a Python native function against the whole `DataFrame` by
using Pandas instances. Internally it works similarly with Pandas UDFs by using Arrow to transfer
data and Pandas to work with the data, which allows vectorized operations. However, A Pandas Function
API behaves as a regular API under PySpark `DataFrame` instead of `Column`, and Python type hints in Pandas
Functions APIs are optional and do not affect how it works internally at this moment although they
might be required in the future.

From Spark 3.0, grouped map pandas UDF is now categorized as a separate Pandas Function API,
`DataFrame.groupby().applyInPandas()`. It is still possible to use it with `PandasUDFType`
and `DataFrame.groupby().apply()` as it was; however, it is preferred to use
`DataFrame.groupby().applyInPandas()` directly. Using `PandasUDFType` will be deprecated
in the future.

### Grouped Map

Grouped map operations with Pandas instances are supported by `DataFrame.groupby().applyInPandas()`
which requires a Python function that takes a `pandas.DataFrame` and return another `pandas.DataFrame`.
It maps each group to each `pandas.DataFrame` in the Python function.

This API implements the "split-apply-combine" pattern which consists of three steps:
* Split the data into groups by using `DataFrame.groupBy`.
* Apply a function on each group. The input and output of the function are both `pandas.DataFrame`. The
  input data contains all the rows and columns for each group.
* Combine the results into a new PySpark `DataFrame`.

To use `groupBy().applyInPandas()`, the user needs to define the following:
* A Python function that defines the computation for each group.
* A `StructType` object or a string that defines the schema of the output PySpark `DataFrame`.

The column labels of the returned `pandas.DataFrame` must either match the field names in the
defined output schema if specified as strings, or match the field data types by position if not
strings, e.g. integer indices. See [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.html#pandas.DataFrame)
on how to label columns when constructing a `pandas.DataFrame`.

Note that all data for a group will be loaded into memory before the function is applied. This can
lead to out of memory exceptions, especially if the group sizes are skewed. The configuration for
[maxRecordsPerBatch](#setting-arrow-batch-size) is not applied on groups and it is up to the user
to ensure that the grouped data will fit into the available memory.

The following example shows how to use `groupby().applyInPandas()` to subtract the mean from each value
in the group.

<div class="codetabs">
<div data-lang="python" markdown="1">
{% include_example grouped_apply_in_pandas python/sql/arrow.py %}
</div>
</div>

For detailed usage, please see [`pyspark.sql.GroupedData.applyInPandas`](api/python/pyspark.sql.html#pyspark.sql.GroupedData.applyInPandas).

### Map

Map operations with Pandas instances are supported by `DataFrame.mapInPandas()` which maps an iterator
of `pandas.DataFrame`s to another iterator of `pandas.DataFrame`s that represents the current
PySpark `DataFrame` and returns the result as a PySpark `DataFrame`. The functions takes and outputs
an iterator of `pandas.DataFrame`. It can return the output of arbitrary length in contrast to some
Pandas UDFs although internally it works similarly with Series to Series Pandas UDF.

The following example shows how to use `mapInPandas()`:

<div class="codetabs">
<div data-lang="python" markdown="1">
{% include_example map_in_pandas python/sql/arrow.py %}
</div>
</div>

For detailed usage, please see [`pyspark.sql.DataFrame.mapsInPandas`](api/python/pyspark.sql.html#pyspark.sql.DataFrame.mapInPandas).

### Co-grouped Map

Co-grouped map operations with Pandas instances are supported by `DataFrame.groupby().cogroup().applyInPandas()` which
allows two PySpark `DataFrame`s to be cogrouped by a common key and then a Python function applied to each
cogroup. It consists of the following steps:
* Shuffle the data such that the groups of each dataframe which share a key are cogrouped together.
* Apply a function to each cogroup. The input of the function is two `pandas.DataFrame` (with an optional tuple
representing the key). The output of the function is a `pandas.DataFrame`.
* Combine the `pandas.DataFrame`s from all groups into a new PySpark `DataFrame`. 

To use `groupBy().cogroup().applyInPandas()`, the user needs to define the following:
* A Python function that defines the computation for each cogroup.
* A `StructType` object or a string that defines the schema of the output PySpark `DataFrame`.

The column labels of the returned `pandas.DataFrame` must either match the field names in the
defined output schema if specified as strings, or match the field data types by position if not
strings, e.g. integer indices. See [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.html#pandas.DataFrame)
on how to label columns when constructing a `pandas.DataFrame`.

Note that all data for a cogroup will be loaded into memory before the function is applied. This can lead to out of
memory exceptions, especially if the group sizes are skewed. The configuration for [maxRecordsPerBatch](#setting-arrow-batch-size)
is not applied and it is up to the user to ensure that the cogrouped data will fit into the available memory.

The following example shows how to use `groupby().cogroup().applyInPandas()` to perform an asof join between two datasets.

<div class="codetabs">
<div data-lang="python" markdown="1">
{% include_example cogrouped_apply_in_pandas python/sql/arrow.py %}
</div>
</div>

For detailed usage, please see [`pyspark.sql.PandasCogroupedOps.applyInPandas()`](api/python/pyspark.sql.html#pyspark.sql.PandasCogroupedOps.applyInPandas).


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

### Recommended Pandas and PyArrow Versions

For usage with pyspark.sql, the supported versions of Pandas is 0.24.2 and PyArrow is 0.15.1. Higher
versions may be used, however, compatibility and data correctness can not be guaranteed and should
be verified by the user.

### Compatibility Setting for PyArrow >= 0.15.0 and Spark 2.3.x, 2.4.x

Since Arrow 0.15.0, a change in the binary IPC format requires an environment variable to be
compatible with previous versions of Arrow <= 0.14.1. This is only necessary to do for PySpark
users with versions 2.3.x and 2.4.x that have manually upgraded PyArrow to 0.15.0. The following
can be added to `conf/spark-env.sh` to use the legacy Arrow IPC format:

```
ARROW_PRE_0_15_IPC_FORMAT=1
```

This will instruct PyArrow >= 0.15.0 to use the legacy IPC format with the older Arrow Java that
is in Spark 2.3.x and 2.4.x. Not setting this environment variable will lead to a similar error as
described in [SPARK-29367](https://issues.apache.org/jira/browse/SPARK-29367) when running
`pandas_udf`s or `toPandas()` with Arrow enabled. More information about the Arrow IPC change can
be read on the Arrow 0.15.0 release [blog](http://arrow.apache.org/blog/2019/10/06/0.15.0-release/#columnar-streaming-protocol-change-since-0140).
