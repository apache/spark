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


.. _api.indexing:

=============
Index objects
=============

Index
-----
.. currentmodule:: pyspark.pandas

.. autosummary::
   :toctree: api/

   Index

Properties
~~~~~~~~~~
.. autosummary::
   :toctree: api/

   Index.is_monotonic
   Index.is_monotonic_increasing
   Index.is_monotonic_decreasing
   Index.is_unique
   Index.has_duplicates
   Index.hasnans
   Index.dtype
   Index.inferred_type
   Index.is_all_dates
   Index.shape
   Index.name
   Index.names
   Index.ndim
   Index.size
   Index.nlevels
   Index.empty
   Index.T
   Index.values

Modifying and computations
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: api/

   Index.all
   Index.any
   Index.argmin
   Index.argmax
   Index.copy
   Index.delete
   Index.equals
   Index.factorize
   Index.identical
   Index.insert
   Index.is_boolean
   Index.is_categorical
   Index.is_floating
   Index.is_integer
   Index.is_interval
   Index.is_numeric
   Index.is_object
   Index.drop
   Index.drop_duplicates
   Index.min
   Index.max
   Index.map
   Index.rename
   Index.repeat
   Index.take
   Index.unique
   Index.nunique
   Index.value_counts

Compatibility with MultiIndex
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: api/

   Index.set_names
   Index.droplevel

Missing Values
~~~~~~~~~~~~~~
.. autosummary::
   :toctree: api/

   Index.fillna
   Index.dropna
   Index.isna
   Index.notna

Conversion
~~~~~~~~~~
.. autosummary::
   :toctree: api/

   Index.astype
   Index.item
   Index.to_list
   Index.to_series
   Index.to_frame
   Index.view
   Index.to_numpy

Spark-related
-------------
``Index.spark`` provides features that does not exist in pandas but
in Spark. These can be accessed by ``Index.spark.<function/property>``.

.. autosummary::
   :toctree: api/

   Index.spark.column
   Index.spark.transform

Sorting
~~~~~~~
.. autosummary::
   :toctree: api/

   Index.sort_values

Time-specific operations
~~~~~~~~~~~~~~~~~~~~~~~~
.. autosummary::
   :toctree: api/

   Index.shift

Combining / joining / set operations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. autosummary::
   :toctree: api/

   Index.append
   Index.intersection
   Index.union
   Index.difference
   Index.symmetric_difference

Selecting
~~~~~~~~~
.. autosummary::
   :toctree: api/

   Index.asof
   Index.isin

.. _api.numeric:

Numeric Index
-------------
.. autosummary::
   :toctree: api/

   Int64Index
   Float64Index

.. _api.categorical:

CategoricalIndex
----------------
.. autosummary::
   :toctree: api/

   CategoricalIndex

Categorical components
~~~~~~~~~~~~~~~~~~~~~~
.. autosummary::
   :toctree: api/

   CategoricalIndex.codes
   CategoricalIndex.categories
   CategoricalIndex.ordered
   CategoricalIndex.rename_categories
   CategoricalIndex.reorder_categories
   CategoricalIndex.add_categories
   CategoricalIndex.remove_categories
   CategoricalIndex.remove_unused_categories
   CategoricalIndex.set_categories
   CategoricalIndex.as_ordered
   CategoricalIndex.as_unordered
   CategoricalIndex.map

.. _api.multiindex:

MultiIndex
----------
.. autosummary::
   :toctree: api/

   MultiIndex

MultiIndex Constructors
~~~~~~~~~~~~~~~~~~~~~~~
.. autosummary::
   :toctree: api/

   MultiIndex.from_arrays
   MultiIndex.from_tuples
   MultiIndex.from_product
   MultiIndex.from_frame

MultiIndex Properties
~~~~~~~~~~~~~~~~~~~~~
.. autosummary::
   :toctree: api/

   MultiIndex.has_duplicates
   MultiIndex.hasnans
   MultiIndex.inferred_type
   MultiIndex.is_all_dates
   MultiIndex.shape
   MultiIndex.names
   MultiIndex.ndim
   MultiIndex.empty
   MultiIndex.T
   MultiIndex.size
   MultiIndex.nlevels
   MultiIndex.levshape
   MultiIndex.values
   MultiIndex.dtypes

MultiIndex components
~~~~~~~~~~~~~~~~~~~~~
.. autosummary::
   :toctree: api/

   MultiIndex.swaplevel

MultiIndex components
~~~~~~~~~~~~~~~~~~~~~
.. autosummary::
   :toctree: api/

   MultiIndex.droplevel

MultiIndex Missing Values
~~~~~~~~~~~~~~~~~~~~~~~~~
.. autosummary::
   :toctree: api/

   MultiIndex.fillna
   MultiIndex.dropna

MultiIndex Modifying and computations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. autosummary::
   :toctree: api/

   MultiIndex.equals
   MultiIndex.equal_levels
   MultiIndex.identical
   MultiIndex.insert
   MultiIndex.drop
   MultiIndex.copy
   MultiIndex.delete
   MultiIndex.rename
   MultiIndex.repeat
   MultiIndex.take
   MultiIndex.unique
   MultiIndex.min
   MultiIndex.max
   MultiIndex.value_counts

MultiIndex Combining / joining / set operations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. autosummary::
   :toctree: api/

   MultiIndex.append
   MultiIndex.intersection
   MultiIndex.union
   MultiIndex.difference
   MultiIndex.symmetric_difference

MultiIndex Conversion
~~~~~~~~~~~~~~~~~~~~~
.. autosummary::
   :toctree: api/

   MultiIndex.astype
   MultiIndex.item
   MultiIndex.to_list
   MultiIndex.to_series
   MultiIndex.to_frame
   MultiIndex.view
   MultiIndex.to_numpy

MultiIndex Spark-related
------------------------
``MultiIndex.spark`` provides features that does not exist in pandas but
in Spark. These can be accessed by ``MultiIndex.spark.<function/property>``.

.. autosummary::
   :toctree: api/

   MultiIndex.spark.data_type
   MultiIndex.spark.column
   MultiIndex.spark.transform

MultiIndex Sorting
~~~~~~~~~~~~~~~~~~
.. autosummary::
   :toctree: api/

   MultiIndex.sort_values

.. _api.datetimes:

DatatimeIndex
-------------
.. autosummary::
   :toctree: api/

   DatetimeIndex

Time/date components
~~~~~~~~~~~~~~~~~~~~
.. autosummary::
   :toctree: api/

   DatetimeIndex.year
   DatetimeIndex.month
   DatetimeIndex.day
   DatetimeIndex.hour
   DatetimeIndex.minute
   DatetimeIndex.second
   DatetimeIndex.microsecond
   DatetimeIndex.week
   DatetimeIndex.weekofyear
   DatetimeIndex.dayofweek
   DatetimeIndex.day_of_week
   DatetimeIndex.weekday
   DatetimeIndex.dayofyear
   DatetimeIndex.day_of_year
   DatetimeIndex.quarter
   DatetimeIndex.is_month_start
   DatetimeIndex.is_month_end
   DatetimeIndex.is_quarter_start
   DatetimeIndex.is_quarter_end
   DatetimeIndex.is_year_start
   DatetimeIndex.is_year_end
   DatetimeIndex.is_leap_year
   DatetimeIndex.daysinmonth
   DatetimeIndex.days_in_month

Selecting
~~~~~~~~~~~~~~~~~~~~~~~~
.. autosummary::
   :toctree: api/

   DatetimeIndex.indexer_between_time
   DatetimeIndex.indexer_at_time

Time-specific operations
~~~~~~~~~~~~~~~~~~~~~~~~
.. autosummary::
   :toctree: api/

   DatetimeIndex.normalize
   DatetimeIndex.strftime
   DatetimeIndex.round
   DatetimeIndex.floor
   DatetimeIndex.ceil
   DatetimeIndex.month_name
   DatetimeIndex.day_name
