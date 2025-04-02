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


.. _api.general_functions:

=================
General functions
=================
.. currentmodule:: pyspark.pandas

Data manipulations and SQL
--------------------------
.. autosummary::
   :toctree: api/

   melt
   merge
   merge_asof
   get_dummies
   concat
   sql
   broadcast

Top-level missing data
----------------------

.. autosummary::
   :toctree: api/

   isna
   isnull
   notna
   notnull

Top-level dealing with numeric data
-----------------------------------

.. autosummary::
   :toctree: api/

   to_numeric

Top-level dealing with datetimelike data
----------------------------------------
.. autosummary::
   :toctree: api/

   to_datetime
   date_range
   to_timedelta
   timedelta_range
