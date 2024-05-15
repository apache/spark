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

==================
Upgrading PySpark
==================

Upgrading from PySpark 3.5 to 4.0
---------------------------------

* In Spark 4.0, Python 3.8 support was dropped in PySpark.
* In Spark 4.0, the minimum supported version for Pandas has been raised from 1.0.5 to 2.0.0 in PySpark.
* In Spark 4.0, the minimum supported version for Numpy has been raised from 1.15 to 1.21 in PySpark.
* In Spark 4.0, the minimum supported version for PyArrow has been raised from 4.0.0 to 10.0.0 in PySpark.
* In Spark 4.0, ``Int64Index`` and ``Float64Index`` have been removed from pandas API on Spark, ``Index`` should be used directly.
* In Spark 4.0, ``DataFrame.iteritems`` has been removed from pandas API on Spark, use ``DataFrame.items`` instead.
* In Spark 4.0, ``Series.iteritems`` has been removed from pandas API on Spark, use ``Series.items`` instead.
* In Spark 4.0, ``DataFrame.append`` has been removed from pandas API on Spark, use ``ps.concat`` instead.
* In Spark 4.0, ``Series.append`` has been removed from pandas API on Spark, use ``ps.concat`` instead.
* In Spark 4.0, ``DataFrame.mad`` has been removed from pandas API on Spark.
* In Spark 4.0, ``Series.mad`` has been removed from pandas API on Spark.
* In Spark 4.0, ``na_sentinel`` parameter from ``Index.factorize`` and ``Series.factorize`` has been removed from pandas API on Spark, use ``use_na_sentinel`` instead.
* In Spark 4.0, ``inplace`` parameter from ``Categorical.add_categories``, ``Categorical.remove_categories``, ``Categorical.set_categories``, ``Categorical.rename_categories``, ``Categorical.reorder_categories``, ``Categorical.as_ordered``, ``Categorical.as_unordered`` have been removed from pandas API on Spark.
* In Spark 4.0, ``inplace`` parameter from ``CategoricalIndex.add_categories``, ``CategoricalIndex.remove_categories``, ``CategoricalIndex.remove_unused_categories``, ``CategoricalIndex.set_categories``, ``CategoricalIndex.rename_categories``, ``CategoricalIndex.reorder_categories``, ``CategoricalIndex.as_ordered``, ``CategoricalIndex.as_unordered`` have been removed from pandas API on Spark.
* In Spark 4.0, ``closed`` parameter from ``ps.date_range`` has been removed from pandas API on Spark.
* In Spark 4.0, ``include_start`` and ``include_end`` parameters from ``DataFrame.between_time`` have been removed from pandas API on Spark, use ``inclusive`` instead.
* In Spark 4.0, ``include_start`` and ``include_end`` parameters from ``Series.between_time`` have been removed from pandas API on Spark, use ``inclusive`` instead.
* In Spark 4.0, the various datetime attributes of ``DatetimeIndex`` (``day``, ``month``, ``year`` etc.) are now ``int32`` instead of ``int64`` from pandas API on Spark.
* In Spark 4.0, ``sort_columns`` parameter from ``DataFrame.plot`` and `Series.plot`` has been removed from pandas API on Spark.
* In Spark 4.0, the default value of ``regex`` parameter for ``Series.str.replace`` has been changed from ``True`` to ``False`` from pandas API on Spark. Additionally, a single character ``pat`` with ``regex=True`` is now treated as a regular expression instead of a string literal.
* In Spark 4.0, the resulting name from ``value_counts`` for all objects sets to ``'count'`` (or ``'proportion'`` if ``normalize=True`` was passed) from pandas API on Spark, and the index will be named after the original object.
* In Spark 4.0, ``squeeze`` parameter from ``ps.read_csv`` and ``ps.read_excel`` has been removed from pandas API on Spark.
* In Spark 4.0, ``null_counts`` parameter from ``DataFrame.info`` has been removed from pandas API on Spark, use ``show_counts`` instead.
* In Spark 4.0, the result of ``MultiIndex.append`` does not keep the index names from pandas API on Spark.
* In Spark 4.0, ``DataFrameGroupBy.agg`` with lists respecting ``as_index=False`` from pandas API on Spark.
* In Spark 4.0, ``DataFrame.stack`` guarantees the order of existing columns instead of sorting them lexicographically from pandas API on Spark.
* In Spark 4.0, ``True`` or ``False`` to ``inclusive`` parameter from ``Series.between`` has been removed from pandas API on Spark, use ``both`` or ``neither`` instead respectively.
* In Spark 4.0, ``Index.asi8`` has been removed from pandas API on Spark, use ``Index.astype`` instead.
* In Spark 4.0, ``Index.is_type_compatible`` has been removed from pandas API on Spark, use ``Index.isin`` instead.
* In Spark 4.0, ``col_space`` parameter from ``DataFrame.to_latex`` and ``Series.to_latex`` has been removed from pandas API on Spark.
* In Spark 4.0, ``DataFrame.to_spark_io`` has been removed from pandas API on Spark, use ``DataFrame.spark.to_spark_io`` instead.
* In Spark 4.0, ``Series.is_monotonic`` and ``Index.is_monotonic`` have been removed from pandas API on Spark, use ``Series.is_monotonic_increasing`` or ``Index.is_monotonic_increasing`` instead respectively.
* In Spark 4.0, ``DataFrame.get_dtype_counts`` has been removed from pandas API on Spark, use ``DataFrame.dtypes.value_counts()`` instead.
* In Spark 4.0, ``encoding`` parameter from ``DataFrame.to_excel`` and ``Series.to_excel`` have been removed from pandas API on Spark.
* In Spark 4.0, ``verbose`` parameter from ``DataFrame.to_excel`` and ``Series.to_excel`` have been removed from pandas API on Spark.
* In Spark 4.0, ``mangle_dupe_cols`` parameter from ``read_csv`` has been removed from pandas API on Spark.
* In Spark 4.0, ``DataFrameGroupBy.backfill`` has been removed from pandas API on Spark, use ``DataFrameGroupBy.bfill`` instead.
* In Spark 4.0, ``DataFrameGroupBy.pad`` has been removed from pandas API on Spark, use ``DataFrameGroupBy.ffill`` instead.
* In Spark 4.0, ``Index.is_all_dates`` has been removed from pandas API on Spark.
* In Spark 4.0, ``convert_float`` parameter from ``read_excel`` has been removed from pandas API on Spark.
* In Spark 4.0, ``mangle_dupe_cols`` parameter from ``read_excel`` has been removed from pandas API on Spark.
* In Spark 4.0, ``DataFrame.koalas`` has been removed from pandas API on Spark, use ``DataFrame.pandas_on_spark`` instead.
* In Spark 4.0, ``DataFrame.to_koalas`` has been removed from PySpark, use ``DataFrame.pandas_api`` instead.
* In Spark 4.0, ``DataFrame.to_pandas_on_spark`` has been removed from PySpark, use ``DataFrame.pandas_api`` instead.
* In Spark 4.0, ``DatatimeIndex.week`` and ``DatatimeIndex.weekofyear`` have been removed from Pandas API on Spark, use ``DatetimeIndex.isocalendar().week`` instead.
* In Spark 4.0, ``Series.dt.week`` and ``Series.dt.weekofyear`` have been removed from Pandas API on Spark, use ``Series.dt.isocalendar().week`` instead.
* In Spark 4.0, when applying ``astype`` to a decimal type object, the existing missing value is changed to ``True`` instead of ``False`` from Pandas API on Spark.
* In Spark 4.0, ``pyspark.testing.assertPandasOnSparkEqual`` has been removed from Pandas API on Spark, use ``pyspark.pandas.testing.assert_frame_equal`` instead.
* In Spark 4.0, the aliases ``Y``, ``M``, ``H``, ``T``, ``S`` have been deprecated from Pandas API on Spark, use ``YE``, ``ME``, ``h``, ``min``, ``s`` instead respectively.
* In Spark 4.0, the schema of a map column is inferred by merging the schemas of all pairs in the map. To restore the previous behavior where the schema is only inferred from the first non-null pair, you can set ``spark.sql.pyspark.legacy.inferMapTypeFromFirstPair.enabled`` to ``true``.



Upgrading from PySpark 3.3 to 3.4
---------------------------------

* In Spark 3.4, the schema of an array column is inferred by merging the schemas of all elements in the array. To restore the previous behavior where the schema is only inferred from the first element, you can set ``spark.sql.pyspark.legacy.inferArrayTypeFromFirstElement.enabled`` to ``true``.
* In Spark 3.4, if Pandas on Spark API ``Groupby.apply``'s ``func`` parameter return type is not specified and ``compute.shortcut_limit`` is set to 0, the sampling rows will be set to 2 (ensure sampling rows always >= 2) to make sure infer schema is accurate.
* In Spark 3.4, if Pandas on Spark API ``Index.insert`` is out of bounds, will raise IndexError with ``index {} is out of bounds for axis 0 with size {}`` to follow pandas 1.4 behavior.
* In Spark 3.4, the series name will be preserved in Pandas on Spark API ``Series.mode`` to follow pandas 1.4 behavior.
* In Spark 3.4, the Pandas on Spark API ``Index.__setitem__`` will first to check ``value`` type is ``Column`` type to avoid raising unexpected ``ValueError`` in ``is_list_like`` like `Cannot convert column into bool: please use '&' for 'and', '|' for 'or', '~' for 'not' when building DataFrame boolean expressions.`.
* In Spark 3.4, the Pandas on Spark API ``astype('category')`` will also refresh ``categories.dtype`` according to original data ``dtype`` to follow pandas 1.4 behavior.
* In Spark 3.4, the Pandas on Spark API supports groupby positional indexing in ``GroupBy.head`` and ``GroupBy.tail`` to follow pandas 1.4. Negative arguments now work correctly and result in ranges relative to the end and start of each group, Previously, negative arguments returned empty frames.
* In Spark 3.4, the infer schema process of ``groupby.apply`` in Pandas on Spark, will first infer the pandas type to ensure the accuracy of the pandas ``dtype`` as much as possible.
* In Spark 3.4, the ``Series.concat`` sort parameter will be respected to follow pandas 1.4 behaviors.
* In Spark 3.4, the ``DataFrame.__setitem__`` will make a copy and replace pre-existing arrays, which will NOT be over-written to follow pandas 1.4 behaviors.
* In Spark 3.4, the ``SparkSession.sql`` and the Pandas on Spark API ``sql`` have got new parameter ``args`` which provides binding of named parameters to their SQL literals.
* In Spark 3.4, Pandas API on Spark follows for the pandas 2.0, and some APIs were deprecated or removed in Spark 3.4 according to the changes made in pandas 2.0. Please refer to the [release notes of pandas](https://pandas.pydata.org/docs/dev/whatsnew/) for more details.
* In Spark 3.4, the custom monkey-patch of ``collections.namedtuple`` was removed, and ``cloudpickle`` was used by default. To restore the previous behavior for any relevant pickling issue of ``collections.namedtuple``, set ``PYSPARK_ENABLE_NAMEDTUPLE_PATCH`` environment variable to ``1``.


Upgrading from PySpark 3.2 to 3.3
---------------------------------

* In Spark 3.3, the ``pyspark.pandas.sql`` method follows [the standard Python string formatter](https://docs.python.org/3/library/string.html#format-string-syntax). To restore the previous behavior, set ``PYSPARK_PANDAS_SQL_LEGACY`` environment variable to ``1``.
* In Spark 3.3, the ``drop`` method of pandas API on Spark DataFrame supports dropping rows by ``index``, and sets dropping by index instead of column by default.
* In Spark 3.3, PySpark upgrades Pandas version, the new minimum required version changes from 0.23.2 to 1.0.5.
* In Spark 3.3, the ``repr`` return values of SQL DataTypes have been changed to yield an object with the same value when passed to ``eval``.


Upgrading from PySpark 3.1 to 3.2
---------------------------------

* In Spark 3.2, the PySpark methods from sql, ml, spark_on_pandas modules raise the ``TypeError`` instead of ``ValueError`` when are applied to a param of inappropriate type.
* In Spark 3.2, the traceback from Python UDFs, pandas UDFs and pandas function APIs are simplified by default without the traceback from the internal Python workers. In Spark 3.1 or earlier, the traceback from Python workers was printed out. To restore the behavior before Spark 3.2, you can set ``spark.sql.execution.pyspark.udf.simplifiedTraceback.enabled`` to ``false``.
* In Spark 3.2, pinned thread mode is enabled by default to map each Python thread to the corresponding JVM thread. Previously,
  one JVM thread could be reused for multiple Python threads, which resulted in one JVM thread local being shared to multiple Python threads.
  Also, note that now ``pyspark.InheritableThread`` or ``pyspark.inheritable_thread_target`` is recommended to use together for a Python thread
  to properly inherit the inheritable attributes such as local properties in a JVM thread, and to avoid a potential resource leak issue.
  To restore the behavior before Spark 3.2, you can set ``PYSPARK_PIN_THREAD`` environment variable to ``false``.


Upgrading from PySpark 2.4 to 3.0
---------------------------------

* In Spark 3.0, PySpark requires a pandas version of 0.23.2 or higher to use pandas related functionality, such as ``toPandas``, ``createDataFrame`` from pandas DataFrame, and so on.
* In Spark 3.0, PySpark requires a PyArrow version of 0.12.1 or higher to use PyArrow related functionality, such as ``pandas_udf``, ``toPandas`` and ``createDataFrame`` with "spark.sql.execution.arrow.enabled=true", etc.
* In PySpark, when creating a ``SparkSession`` with ``SparkSession.builder.getOrCreate()``, if there is an existing ``SparkContext``, the builder was trying to update the ``SparkConf`` of the existing ``SparkContext`` with configurations specified to the builder, but the ``SparkContext`` is shared by all ``SparkSession`` s, so we should not update them. In 3.0, the builder comes to not update the configurations. This is the same behavior as Java/Scala API in 2.3 and above. If you want to update them, you need to update them prior to creating a ``SparkSession``.
* In PySpark, when Arrow optimization is enabled, if Arrow version is higher than 0.11.0, Arrow can perform safe type conversion when converting pandas.Series to an Arrow array during serialization. Arrow raises errors when detecting unsafe type conversions like overflow. You enable it by setting ``spark.sql.execution.pandas.convertToArrowArraySafely`` to true. The default setting is false. PySpark behavior for Arrow versions is illustrated in the following table:

    =======================================  ================  =========================
    PyArrow version                          Integer overflow  Floating point truncation
    =======================================  ================  =========================
    0.11.0 and below                         Raise error       Silently allows
    > 0.11.0, arrowSafeTypeConversion=false  Silent overflow   Silently allows
    > 0.11.0, arrowSafeTypeConversion=true   Raise error       Raise error
    =======================================  ================  =========================

* In Spark 3.0, ``createDataFrame(..., verifySchema=True)`` validates LongType as well in PySpark. Previously, LongType was not verified and resulted in None in case the value overflows. To restore this behavior, verifySchema can be set to False to disable the validation.
* As of Spark 3.0, ``Row`` field names are no longer sorted alphabetically when constructing with named arguments for Python versions 3.6 and above, and the order of fields will match that as entered. To enable sorted fields by default, as in Spark 2.4, set the environment variable ``PYSPARK_ROW_FIELD_SORTING_ENABLED`` to true for both executors and driver - this environment variable must be consistent on all executors and driver; otherwise, it may cause failures or incorrect answers. For Python versions less than 3.6, the field names will be sorted alphabetically as the only option.
* In Spark 3.0, ``pyspark.ml.param.shared.Has*`` mixins do not provide any ``set*(self, value)`` setter methods anymore, use the respective ``self.set(self.*, value)`` instead. See `SPARK-29093 <https://issues.apache.org/jira/browse/SPARK-29093>`_ for details.


Upgrading from PySpark 2.3 to 2.4
---------------------------------

* In PySpark, when Arrow optimization is enabled, previously ``toPandas`` just failed when Arrow optimization is unable to be used whereas ``createDataFrame`` from Pandas DataFrame allowed the fallback to non-optimization. Now, both ``toPandas`` and ``createDataFrame`` from Pandas DataFrame allow the fallback by default, which can be switched off by ``spark.sql.execution.arrow.fallback.enabled``.


Upgrading from PySpark 2.3.0 to 2.3.1 and above
-----------------------------------------------

* As of version 2.3.1 Arrow functionality, including ``pandas_udf`` and ``toPandas()``/``createDataFrame()`` with ``spark.sql.execution.arrow.enabled`` set to ``True``, has been marked as experimental. These are still evolving and not currently recommended for use in production.


Upgrading from PySpark 2.2 to 2.3
---------------------------------

* In PySpark, now we need Pandas 0.19.2 or upper if you want to use Pandas related functionalities, such as ``toPandas``, ``createDataFrame`` from Pandas DataFrame, etc.
* In PySpark, the behavior of timestamp values for Pandas related functionalities was changed to respect session timezone. If you want to use the old behavior, you need to set a configuration ``spark.sql.execution.pandas.respectSessionTimeZone`` to False. See `SPARK-22395 <https://issues.apache.org/jira/browse/SPARK-22395>`_ for details.
* In PySpark, ``na.fill()`` or ``fillna`` also accepts boolean and replaces nulls with booleans. In prior Spark versions, PySpark just ignores it and returns the original Dataset/DataFrame.
* In PySpark, ``df.replace`` does not allow to omit value when ``to_replace`` is not a dictionary. Previously, value could be omitted in the other cases and had None by default, which is counterintuitive and error-prone.


Upgrading from PySpark 1.4 to 1.5
---------------------------------

* Resolution of strings to columns in Python now supports using dots (.) to qualify the column or access nested values. For example ``df['table.column.nestedField']``. However, this means that if your column name contains any dots you must now escape them using backticks (e.g., ``table.`column.with.dots`.nested``).
* DataFrame.withColumn method in PySpark supports adding a new column or replacing existing columns of the same name.


Upgrading from PySpark 1.0-1.2 to 1.3
-------------------------------------

* When using DataTypes in Python you will need to construct them (i.e. ``StringType()``) instead of referencing a singleton.
