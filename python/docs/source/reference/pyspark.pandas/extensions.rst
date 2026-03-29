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


.. _api.extensions:

==========
Extensions
==========
.. currentmodule:: pyspark.pandas.extensions

Accessors
---------

Accessors can be written and registered with pandas-on-Spark Dataframes, Series, and
Index objects. Accessors allow developers to extend the functionality of
pandas-on-Spark objects seamlessly by writing arbitrary classes and methods which are
then wrapped in one of the following decorators.

.. autosummary::
   :toctree: api/

   register_dataframe_accessor
   register_series_accessor
   register_index_accessor
