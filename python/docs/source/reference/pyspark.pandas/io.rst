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


.. _api.io:

============
Input/Output
============
.. currentmodule:: pyspark.pandas


Data Generator
--------------
.. autosummary::
   :toctree: api/

   range

Spark Metastore Table
---------------------
.. autosummary::
   :toctree: api/

   read_table
   DataFrame.to_table

Delta Lake
----------
.. autosummary::
   :toctree: api/

   read_delta
   DataFrame.to_delta

Parquet
-------
.. autosummary::
   :toctree: api/

   read_parquet
   DataFrame.to_parquet

ORC
-------
.. autosummary::
   :toctree: api/

   read_orc
   DataFrame.to_orc

Generic Spark I/O
-----------------
.. autosummary::
   :toctree: api/

   read_spark_io
   DataFrame.to_spark_io

Flat File / CSV
---------------
.. autosummary::
   :toctree: api/

   read_csv
   DataFrame.to_csv

Clipboard
---------
.. autosummary::
   :toctree: api/

   read_clipboard
   DataFrame.to_clipboard

Excel
-----
.. autosummary::
   :toctree: api/

   read_excel
   DataFrame.to_excel

JSON
----
.. autosummary::
   :toctree: api/

   read_json
   DataFrame.to_json

HTML
----
.. autosummary::
   :toctree: api/

   read_html
   DataFrame.to_html

SQL
---
.. autosummary::
   :toctree: api/

   read_sql_table
   read_sql_query
   read_sql
