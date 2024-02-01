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


.. _api.groupby:

=======
GroupBy
=======
.. currentmodule:: pyspark.pandas

GroupBy objects are returned by groupby calls: :func:`DataFrame.groupby`, :func:`Series.groupby`, etc.

.. currentmodule:: pyspark.pandas.groupby


Indexing, iteration
-------------------
.. autosummary::
   :toctree: api/

   GroupBy.get_group

Function application
--------------------
.. autosummary::
   :toctree: api/

   GroupBy.apply
   GroupBy.transform

The following methods are available only for `DataFrameGroupBy` objects.

.. autosummary::
   :toctree: api/

   DataFrameGroupBy.agg
   DataFrameGroupBy.aggregate

Computations / Descriptive Stats
--------------------------------
.. autosummary::
   :toctree: api/

   GroupBy.all
   GroupBy.any
   GroupBy.count
   GroupBy.cumcount
   GroupBy.cummax
   GroupBy.cummin
   GroupBy.cumprod
   GroupBy.cumsum
   GroupBy.ewm
   GroupBy.filter
   GroupBy.first
   GroupBy.last
   GroupBy.max
   GroupBy.mean
   GroupBy.median
   GroupBy.min
   GroupBy.nth
   GroupBy.prod
   GroupBy.rank
   GroupBy.sem
   GroupBy.std
   GroupBy.sum
   GroupBy.var
   GroupBy.nunique
   GroupBy.quantile
   GroupBy.size
   GroupBy.diff
   GroupBy.idxmax
   GroupBy.idxmin
   GroupBy.fillna
   GroupBy.bfill
   GroupBy.ffill
   GroupBy.head
   GroupBy.shift
   GroupBy.tail

The following methods are available only for `DataFrameGroupBy` objects.

.. autosummary::
   :toctree: api/

   DataFrameGroupBy.describe

The following methods are available only for `SeriesGroupBy` objects.

.. autosummary::
   :toctree: api/

   SeriesGroupBy.nsmallest
   SeriesGroupBy.nlargest
   SeriesGroupBy.value_counts
   SeriesGroupBy.unique
