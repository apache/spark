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


============================================
Migrating from Koalas to pandas API on Spark
============================================

* The package name to import should be changed to ``pyspark.pandas`` from ``databricks.koalas``.

   .. code-block:: python
   
      # import databricks.koalas as ks
      import pyspark.pandas as ps

* ``DataFrame.koalas`` in Koalas DataFrame was renamed to ``DataFrame.pandas_on_spark`` in pandas-on-Spark DataFrame. ``DataFrame.koalas`` was kept for compatibility reason but deprecated as of Spark 3.2.
  ``DataFrame.koalas`` will be removed in the future releases.

* Monkey-patched ``DataFrame.to_koalas`` in PySpark DataFrame was renamed to ``DataFrame.to_pandas_on_spark`` in PySpark DataFrame. ``DataFrame.to_koalas`` was kept for compatibility reason but deprecated as of Spark 3.2.
  ``DataFrame.to_koalas`` will be removed in the future releases.

* ``databricks.koalas.__version__`` was removed. ``pyspark.__version__`` should be used instead.
