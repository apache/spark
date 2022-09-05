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


======
Window
======
.. currentmodule:: pyspark.pandas.window

Rolling objects are returned by ``.rolling`` calls: :func:`pandas_on_spark.DataFrame.rolling`, :func:`pandas_on_spark.Series.rolling`, etc.
Expanding objects are returned by ``.expanding`` calls: :func:`pandas_on_spark.DataFrame.expanding`, :func:`pandas_on_spark.Series.expanding`, etc.
ExponentialMoving objects are returned by ``.ewm`` calls: :func:`pandas_on_spark.DataFrame.ewm`, :func:`pandas_on_spark.Series.ewm`, etc.

Standard moving window functions
--------------------------------

.. autosummary::
   :toctree: api/

   Rolling.count
   Rolling.sum
   Rolling.min
   Rolling.max
   Rolling.mean

Standard expanding window functions
-----------------------------------

.. autosummary::
   :toctree: api/

   Expanding.count
   Expanding.sum
   Expanding.min
   Expanding.max
   Expanding.mean

Exponential moving window functions
-----------------------------------

.. autosummary::
   :toctree: api/

   ExponentialMoving.mean
