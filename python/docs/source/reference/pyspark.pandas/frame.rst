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


.. _api.dataframe:

=========
DataFrame
=========
.. currentmodule:: pyspark.pandas

Constructor
-----------
.. autosummary::
   :toctree: api/

   DataFrame

Attributes and underlying data
------------------------------

.. autosummary::
   :toctree: api/

   DataFrame.index
   DataFrame.columns
   DataFrame.empty

.. autosummary::
   :toctree: api/

   DataFrame.dtypes
   DataFrame.shape
   DataFrame.axes
   DataFrame.ndim
   DataFrame.size
   DataFrame.select_dtypes
   DataFrame.values

Conversion
----------
.. autosummary::
   :toctree: api/

   DataFrame.copy
   DataFrame.isna
   DataFrame.astype
   DataFrame.isnull
   DataFrame.notna
   DataFrame.notnull
   DataFrame.pad
   DataFrame.bool

Indexing, iteration
-------------------
.. autosummary::
   :toctree: api/

   DataFrame.at
   DataFrame.iat
   DataFrame.head
   DataFrame.idxmax
   DataFrame.idxmin
   DataFrame.loc
   DataFrame.iloc
   DataFrame.items
   DataFrame.iteritems
   DataFrame.iterrows
   DataFrame.itertuples
   DataFrame.keys
   DataFrame.pop
   DataFrame.tail
   DataFrame.xs
   DataFrame.get
   DataFrame.where
   DataFrame.mask
   DataFrame.query

Binary operator functions
-------------------------
.. autosummary::
   :toctree: api/

   DataFrame.add
   DataFrame.radd
   DataFrame.div
   DataFrame.rdiv
   DataFrame.truediv
   DataFrame.rtruediv
   DataFrame.mul
   DataFrame.rmul
   DataFrame.sub
   DataFrame.rsub
   DataFrame.pow
   DataFrame.rpow
   DataFrame.mod
   DataFrame.rmod
   DataFrame.floordiv
   DataFrame.rfloordiv
   DataFrame.lt
   DataFrame.gt
   DataFrame.le
   DataFrame.ge
   DataFrame.ne
   DataFrame.eq
   DataFrame.dot
   DataFrame.combine_first

Function application, GroupBy & Window
--------------------------------------
.. autosummary::
   :toctree: api/

   DataFrame.apply
   DataFrame.applymap
   DataFrame.pipe
   DataFrame.agg
   DataFrame.aggregate
   DataFrame.groupby
   DataFrame.rolling
   DataFrame.expanding
   DataFrame.transform

.. _api.dataframe.stats:

Computations / Descriptive Stats
--------------------------------
.. autosummary::
   :toctree: api/

   DataFrame.abs
   DataFrame.all
   DataFrame.any
   DataFrame.clip
   DataFrame.corr
   DataFrame.corrwith
   DataFrame.count
   DataFrame.cov
   DataFrame.describe
   DataFrame.ewm
   DataFrame.kurt
   DataFrame.kurtosis
   DataFrame.mad
   DataFrame.max
   DataFrame.mean
   DataFrame.min
   DataFrame.median
   DataFrame.mode
   DataFrame.pct_change
   DataFrame.prod
   DataFrame.product
   DataFrame.quantile
   DataFrame.nunique
   DataFrame.sem
   DataFrame.skew
   DataFrame.sum
   DataFrame.std
   DataFrame.var
   DataFrame.cummin
   DataFrame.cummax
   DataFrame.cumsum
   DataFrame.cumprod
   DataFrame.round
   DataFrame.diff
   DataFrame.eval

Reindexing / Selection / Label manipulation
-------------------------------------------
.. autosummary::
   :toctree: api/

   DataFrame.add_prefix
   DataFrame.add_suffix
   DataFrame.align
   DataFrame.at_time
   DataFrame.between_time
   DataFrame.drop
   DataFrame.droplevel
   DataFrame.drop_duplicates
   DataFrame.duplicated
   DataFrame.equals
   DataFrame.filter
   DataFrame.first
   DataFrame.head
   DataFrame.last
   DataFrame.rename
   DataFrame.rename_axis
   DataFrame.reset_index
   DataFrame.set_index
   DataFrame.swapaxes
   DataFrame.swaplevel
   DataFrame.take
   DataFrame.isin
   DataFrame.sample
   DataFrame.truncate

.. _api.dataframe.missing:

Missing data handling
---------------------
.. autosummary::
   :toctree: api/

   DataFrame.backfill
   DataFrame.dropna
   DataFrame.fillna
   DataFrame.replace
   DataFrame.bfill
   DataFrame.ffill
   DataFrame.interpolate

Reshaping, sorting, transposing
-------------------------------
.. autosummary::
   :toctree: api/

   DataFrame.pivot_table
   DataFrame.pivot
   DataFrame.sort_index
   DataFrame.sort_values
   DataFrame.nlargest
   DataFrame.nsmallest
   DataFrame.stack
   DataFrame.unstack
   DataFrame.melt
   DataFrame.explode
   DataFrame.squeeze
   DataFrame.T
   DataFrame.transpose
   DataFrame.reindex
   DataFrame.reindex_like
   DataFrame.rank

Combining / joining / merging
-----------------------------
.. autosummary::
   :toctree: api/

   DataFrame.append
   DataFrame.assign
   DataFrame.merge
   DataFrame.join
   DataFrame.update
   DataFrame.insert

Time series-related
-------------------
.. autosummary::
   :toctree: api/

   DataFrame.resample
   DataFrame.shift
   DataFrame.first_valid_index
   DataFrame.last_valid_index

Serialization / IO / Conversion
-------------------------------
.. autosummary::
   :toctree: api/

   DataFrame.from_records
   DataFrame.info
   DataFrame.to_table
   DataFrame.to_delta
   DataFrame.to_parquet
   DataFrame.to_spark_io
   DataFrame.to_csv
   DataFrame.to_pandas
   DataFrame.to_html
   DataFrame.to_numpy
   DataFrame.to_spark
   DataFrame.to_string
   DataFrame.to_json
   DataFrame.to_dict
   DataFrame.to_excel
   DataFrame.to_clipboard
   DataFrame.to_markdown
   DataFrame.to_records
   DataFrame.to_latex
   DataFrame.style

Spark-related
-------------
``DataFrame.spark`` provides features that does not exist in pandas but
in Spark. These can be accessed by ``DataFrame.spark.<function/property>``.

.. autosummary::
   :toctree: api/

   DataFrame.spark.frame
   DataFrame.spark.cache
   DataFrame.spark.persist
   DataFrame.spark.hint
   DataFrame.spark.to_table
   DataFrame.spark.to_spark_io
   DataFrame.spark.apply
   DataFrame.spark.repartition
   DataFrame.spark.coalesce

.. _api.dataframe.plot:

Plotting
--------
``DataFrame.plot`` is both a callable method and a namespace attribute for
specific plotting methods of the form ``DataFrame.plot.<kind>``.

.. autosummary::
   :toctree: api/

   DataFrame.plot
   DataFrame.plot.area
   DataFrame.plot.barh
   DataFrame.plot.bar
   DataFrame.plot.hist
   DataFrame.plot.box
   DataFrame.plot.line
   DataFrame.plot.pie
   DataFrame.plot.scatter
   DataFrame.plot.density
   DataFrame.hist
   DataFrame.boxplot
   DataFrame.kde

Pandas-on-Spark specific
------------------------
``DataFrame.pandas_on_spark`` provides pandas-on-Spark specific features that exists only in pandas API on Spark.
These can be accessed by ``DataFrame.pandas_on_spark.<function/property>``.

.. autosummary::
   :toctree: api/

   DataFrame.pandas_on_spark.apply_batch
   DataFrame.pandas_on_spark.transform_batch
