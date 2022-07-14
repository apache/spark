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
Upgrading from PySpark 3.2 to 3.3
=================================

* In Spark 3.3, the ``pyspark.pandas.sql`` method follows [the standard Python string formatter](https://docs.python.org/3/library/string.html#format-string-syntax). To restore the previous behavior, set ``PYSPARK_PANDAS_SQL_LEGACY`` environment variable to ``1``.
* In Spark 3.3, the ``drop`` method of pandas API on Spark DataFrame supports dropping rows by ``index``, and sets dropping by index instead of column by default.
* In Spark 3.3, PySpark upgrades Pandas version, the new minimum required version changes from 0.23.2 to 1.0.5.
* In Spark 3.3, the ``repr`` return values of SQL DataTypes have been changed to yield an object with the same value when passed to ``eval``.
