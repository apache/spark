..  Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

..    http://www.apache.org/licenses/LICENSE-2.0

..  Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.


=================================
Type Hints in Pandas API on Spark
=================================

.. currentmodule:: pyspark.pandas

Pandas API on Spark, by default, infers the schema by taking some top records from the output,
in particular, when you use APIs that allow users to apply a function against pandas-on-Spark DataFrame
such as :func:`DataFrame.transform`, :func:`DataFrame.apply`, :func:`DataFrame.pandas_on_spark.apply_batch`,
:func:`DataFrame.pandas_on_spark.apply_batch`, :func:`Series.pandas_on_spark.apply_batch`, etc.

However, this is potentially expensive. If there are several expensive operations such as a shuffle
in the upstream of the execution plan, pandas API on Spark will end up with executing the Spark job twice, once
for schema inference, and once for processing actual data with the schema.

To avoid the consequences, pandas API on Spark has its own type hinting style to specify the schema to avoid
schema inference. Pandas API on Spark understands the type hints specified in the return type and converts it
as a Spark schema for pandas UDFs used internally. The way of type hinting has evolved over time.

This chapter covers the recommended way and the supported ways in detail.

.. note::
    The variadic generics support is experimental and unstable in pandas API on Spark.
    The way of typing can change between minor releases without a warning.
    See also `PEP 646 <https://www.python.org/dev/peps/pep-0646/>`_ for variadic generics in Python.


pandas-on-Spark DataFrame and Pandas DataFrame
----------------------------------------------

In the early pandas-on-Spark version, it was introduced to specify a type hint in the function in order to use
it as a Spark schema. As an example, you can specify the return type hint as below by using pandas-on-Spark
:class:`DataFrame`.

.. code-block:: python

    >>> def pandas_div(pdf) -> ps.DataFrame[float, float]:
    ...    # pdf is a pandas DataFrame.
    ...    return pdf[['B', 'C']] / pdf[['B', 'C']]
    ...
    >>> df = ps.DataFrame({'A': ['a', 'a', 'b'], 'B': [1, 2, 3], 'C': [4, 6, 5]})
    >>> df.groupby('A').apply(pandas_div)

Notice that the function ``pandas_div`` actually takes and outputs a pandas DataFrame instead of
pandas-on-Spark :class:`DataFrame`. So, technically the correct types should be of pandas.

With Python 3.9+, you can specify the type hints by using pandas instances as follows:

.. code-block:: python

    >>> def pandas_div(pdf) -> pd.DataFrame[float, float]:
    ...    # pdf is a pandas DataFrame.
    ...    return pdf[['B', 'C']] / pdf[['B', 'C']]
    ...
    >>> df = ps.DataFrame({'A': ['a', 'a', 'b'], 'B': [1, 2, 3], 'C': [4, 6, 5]})
    >>> df.groupby('A').apply(pandas_div)

Likewise, pandas Series can be also used as a type hints:

.. code-block:: python

    >>> def sqrt(x) -> pd.Series[float]:
    ...     return np.sqrt(x)
    ...
    >>> df = ps.DataFrame([[4, 9]] * 3, columns=['A', 'B'])
    >>> df.apply(sqrt, axis=0)

Currently, both pandas API on Spark and pandas instances can be used to specify the type hints; however, pandas-on-Spark
plans to move gradually towards using pandas instances only as the stability becomes proven.


Type Hinting with Names
-----------------------

This approach is to overcome the limitations in the existing type
hinting especially for DataFrame. When you use a DataFrame as the return type hint, for example,
``DataFrame[int, int]``, there is no way to specify the names of each Series. In the old way, pandas API on Spark just generates
the column names as ``c#`` and this easily leads users to lose or forget the Series mappings. See the example below:

.. code-block:: python

    >>> def transform(pdf) -> pd.DataFrame[int, int]:
    ...     pdf['A'] = pdf.id + 1
    ...     return pdf
    ...
    >>> ps.range(5).pandas_on_spark.apply_batch(transform)

.. code-block:: bash

       c0  c1
    0   0   1
    1   1   2
    2   2   3
    3   3   4
    4   4   5

The new style of type hinting in pandas API on Spark is similar to the regular Python type hints in variables. The Series name
is specified as a string, and the type is specified after a colon. The following example shows a simple case with
the Series names, ``id`` and ``A``, and ``int`` types respectively.

.. code-block:: python

    >>> def transform(pdf) -> pd.DataFrame["id": int, "A": int]:
    ...     pdf['A'] = pdf.id + 1
    ...     return pdf
    ...
    >>> ps.range(5).pandas_on_spark.apply_batch(transform)

.. code-block:: bash

       id   A
    0   0   1
    1   1   2
    2   2   3
    3   3   4
    4   4   5

In addition, pandas API on Spark also dynamically supports ``dtype`` instances and the column index in pandas so that users can
programmatically generate the return type and schema.

.. code-block:: python

    >>> def transform(pdf) -> pd.DataFrame[
    ..         zip(sample.columns, sample.dtypes)]:
    ...    return pdf + 1
    ...
    >>> psdf.pandas_on_spark.apply_batch(transform)

Likewise, ``dtype`` instances from pandas DataFrame can be used alone and let pandas API on Spark generate column names.

.. code-block:: python

    >>> def transform(pdf) -> pd.DataFrame[sample.dtypes]:
    ...     return pdf + 1
    ...
    >>> psdf.pandas_on_spark.apply_batch(transform)


Type Hinting with Index
-----------------------

When you omit index types in the type hints, pandas API on Spark attaches the default index (`compute.default_index_type`),
and it loses the index column and information from the original data. The default index sometimes requires to have an
expensive computation such as shuffle so it is best to specify the index type together.


Index
~~~~~

With the pandas DataFrames below:

.. code-block:: python

    >>> pdf = pd.DataFrame({'id': range(5)})
    >>> sample = pdf.copy()
    >>> sample["a"] = sample.id + 1

The ways below are allowed for a regular index:

.. code-block:: python

    >>> def transform(pdf) -> pd.DataFrame[int, [int, int]]:
    ...     pdf["a"] = pdf.id + 1
    ...     return pdf
    ...
    >>> ps.from_pandas(pdf).pandas_on_spark.apply_batch(transform)

.. code-block:: python

    >>> def transform(pdf) -> pd.DataFrame[
    ...         sample.index.dtype, sample.dtypes]:
    ...     pdf["a"] = pdf.id + 1
    ...     return pdf
    ...
    >>> ps.from_pandas(pdf).pandas_on_spark.apply_batch(transform)

.. code-block:: python

    >>> def transform(pdf) -> pd.DataFrame[
    ...         ("idxA", int), [("id", int), ("a", int)]]:
    ...     pdf["a"] = pdf.id + 1
    ...     return pdf
    ...
    >>> ps.from_pandas(pdf).pandas_on_spark.apply_batch(transform)

.. code-block:: python

    >>> def transform(pdf) -> pd.DataFrame[
    ...         (sample.index.name, sample.index.dtype),
    ...         zip(sample.columns, sample.dtypes)]:
    ...     pdf["a"] = pdf.id + 1
    ...     return pdf
    ...
    >>> ps.from_pandas(pdf).pandas_on_spark.apply_batch(transform)


MultiIndex
~~~~~~~~~~

With the pandas DataFrames below:

    >>> midx = pd.MultiIndex.from_arrays(
    ...     [(1, 1, 2), (1.5, 4.5, 7.5)],
    ...     names=("int", "float"))
    >>> pdf = pd.DataFrame(range(3), index=midx, columns=["id"])
    >>> sample = pdf.copy()
    >>> sample["a"] = sample.id + 1

The ways below are allowed for multi-index:

.. code-block:: python

    >>> def transform(pdf) -> pd.DataFrame[[int, float], [int, int]]:
    ...     pdf["a"] = pdf.id + 1
    ...     return pdf
    ...
    >>> ps.from_pandas(pdf).pandas_on_spark.apply_batch(transform)

.. code-block:: python

    >>> def transform(pdf) -> pd.DataFrame[
    ...         sample.index.dtypes, sample.dtypes]:
    ...     pdf["a"] = pdf.id + 1
    ...     return pdf
    ...
    >>> ps.from_pandas(pdf).pandas_on_spark.apply_batch(transform)

.. code-block:: python

    >>> def transform(pdf) -> pd.DataFrame[
    ...         [("int", int), ("float", float)],
    ...         [("id", int), ("a", int)]]:
    ...     pdf["a"] = pdf.id + 1
    ...     return pdf
    ...
    >>> ps.from_pandas(pdf).pandas_on_spark.apply_batch(transform)

.. code-block:: python

    >>> def transform(pdf) -> pd.DataFrame[
    ...         zip(sample.index.names, sample.index.dtypes),
    ...         zip(sample.columns, sample.dtypes)]:
    ...     pdf["A"] = pdf.id + 1
    ...     return pdf
    ...
    >>> ps.from_pandas(pdf).pandas_on_spark.apply_batch(transform)
