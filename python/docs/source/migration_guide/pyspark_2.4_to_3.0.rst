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
Upgrading from PySpark 2.4 to 3.0
=================================

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

