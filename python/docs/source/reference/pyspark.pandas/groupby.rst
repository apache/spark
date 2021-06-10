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
   GroupBy.filter
   GroupBy.first
   GroupBy.last
   GroupBy.max
   GroupBy.mean
   GroupBy.median
   GroupBy.min
   GroupBy.rank
   GroupBy.std
   GroupBy.sum
   GroupBy.var
   GroupBy.nunique
   GroupBy.size
   GroupBy.diff
   GroupBy.idxmax
   GroupBy.idxmin
   GroupBy.fillna
   GroupBy.bfill
   GroupBy.ffill
   GroupBy.head
   GroupBy.backfill
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
