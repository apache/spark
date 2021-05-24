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
Upgrading from PySpark 3.1 to 3.2
=================================

* In Spark 3.2, the PySpark methods from sql, ml, spark_on_pandas modules raise the ``TypeError`` instead of ``ValueError`` when are applied to an param of inappropriate type.

* In Spark 3.2, the traceback from Python UDFs, pandas UDFs and pandas function APIs are simplified by default without the traceback from the internal Python workers. In Spark 3.1 or earlier, the traceback from Python workers was printed out. To restore the behavior before Spark 3.2, you can set ``spark.sql.execution.pyspark.udf.simplifiedTraceback.enabled`` to ``false``.
