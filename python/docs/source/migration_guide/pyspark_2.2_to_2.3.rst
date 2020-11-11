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
Upgrading from PySpark 2.2 to 2.3
=================================

* In PySpark, now we need Pandas 0.19.2 or upper if you want to use Pandas related functionalities, such as ``toPandas``, ``createDataFrame`` from Pandas DataFrame, etc.

* In PySpark, the behavior of timestamp values for Pandas related functionalities was changed to respect session timezone. If you want to use the old behavior, you need to set a configuration ``spark.sql.execution.pandas.respectSessionTimeZone`` to False. See `SPARK-22395 <https://issues.apache.org/jira/browse/SPARK-22395>`_ for details.

* In PySpark, ``na.fill()`` or ``fillna`` also accepts boolean and replaces nulls with booleans. In prior Spark versions, PySpark just ignores it and returns the original Dataset/DataFrame.

* In PySpark, ``df.replace`` does not allow to omit value when ``to_replace`` is not a dictionary. Previously, value could be omitted in the other cases and had None by default, which is counterintuitive and error-prone.

