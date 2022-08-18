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
Upgrading from PySpark 3.3 to 3.4
=================================

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
