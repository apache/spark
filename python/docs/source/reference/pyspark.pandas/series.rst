.. _api.series:

======
Series
======
.. currentmodule:: pyspark.pandas

Constructor
-----------
.. autosummary::
   :toctree: api/

   Series

Attributes
----------

.. autosummary::
   :toctree: api/

   Series.index
   Series.dtype
   Series.dtypes
   Series.ndim
   Series.name
   Series.shape
   Series.axes
   Series.size
   Series.empty
   Series.T
   Series.hasnans
   Series.values

Conversion
----------
.. autosummary::
   :toctree: api/

   Series.astype
   Series.copy
   Series.bool

Indexing, iteration
-------------------
.. autosummary::
   :toctree: api/

   Series.at
   Series.iat
   Series.loc
   Series.iloc
   Series.keys
   Series.pop
   Series.items
   Series.iteritems
   Series.item
   Series.xs
   Series.get

Binary operator functions
-------------------------

.. autosummary::
   :toctree: api/

   Series.add
   Series.div
   Series.mul
   Series.radd
   Series.rdiv
   Series.rmul
   Series.rsub
   Series.rtruediv
   Series.sub
   Series.truediv
   Series.pow
   Series.rpow
   Series.mod
   Series.rmod
   Series.floordiv
   Series.rfloordiv
   Series.divmod
   Series.rdivmod
   Series.combine_first
   Series.lt
   Series.gt
   Series.le
   Series.ge
   Series.ne
   Series.eq
   Series.product
   Series.dot

Function application, GroupBy & Window
--------------------------------------
.. autosummary::
   :toctree: api/

   Series.apply
   Series.agg
   Series.aggregate
   Series.transform
   Series.map
   Series.groupby
   Series.rolling
   Series.expanding
   Series.pipe

.. _api.series.stats:

Computations / Descriptive Stats
--------------------------------
.. autosummary::
   :toctree: api/

   Series.abs
   Series.all
   Series.any
   Series.between
   Series.clip
   Series.corr
   Series.count
   Series.cummax
   Series.cummin
   Series.cumsum
   Series.cumprod
   Series.describe
   Series.filter
   Series.kurt
   Series.mad
   Series.max
   Series.mean
   Series.min
   Series.mode
   Series.nlargest
   Series.nsmallest
   Series.pct_change
   Series.prod
   Series.nunique
   Series.is_unique
   Series.quantile
   Series.rank
   Series.sem
   Series.skew
   Series.std
   Series.sum
   Series.median
   Series.var
   Series.kurtosis
   Series.unique
   Series.value_counts
   Series.round
   Series.diff
   Series.is_monotonic
   Series.is_monotonic_increasing
   Series.is_monotonic_decreasing

Reindexing / Selection / Label manipulation
-------------------------------------------
.. autosummary::
   :toctree: api/

   Series.align
   Series.drop
   Series.droplevel
   Series.drop_duplicates
   Series.equals
   Series.add_prefix
   Series.add_suffix
   Series.first
   Series.head
   Series.idxmax
   Series.idxmin
   Series.isin
   Series.last
   Series.rename
   Series.rename_axis
   Series.reindex
   Series.reindex_like
   Series.reset_index
   Series.sample
   Series.swaplevel
   Series.swapaxes
   Series.take
   Series.tail
   Series.where
   Series.mask
   Series.truncate

Missing data handling
---------------------
.. autosummary::
   :toctree: api/

   Series.backfill
   Series.bfill
   Series.isna
   Series.isnull
   Series.notna
   Series.notnull
   Series.pad
   Series.dropna
   Series.fillna

Reshaping, sorting, transposing
-------------------------------
.. autosummary::
   :toctree: api/

   Series.argsort
   Series.argmin
   Series.argmax
   Series.sort_index
   Series.sort_values
   Series.unstack
   Series.explode
   Series.repeat
   Series.squeeze
   Series.factorize

Combining / joining / merging
-----------------------------
.. autosummary::
   :toctree: api/

   Series.append
   Series.compare
   Series.replace
   Series.update

Time series-related
-------------------

.. autosummary::
   :toctree: api/

   Series.asof
   Series.shift
   Series.first_valid_index
   Series.last_valid_index
   Series.at_time
   Series.between_time

Spark-related
-------------
``Series.spark`` provides features that does not exist in pandas but
in Spark. These can be accessed by ``Series.spark.<function/property>``.

.. autosummary::
   :toctree: api/

   Series.spark.column
   Series.spark.transform
   Series.spark.apply

Accessors
---------

Pandas API on Spark provides dtype-specific methods under various accessors.
These are separate namespaces within :class:`Series` that only apply
to specific data types.

=========== ===========================
Data Type                      Accessor
=========== ===========================
Datetime    :ref:`dt <api.series.dt>`
String      :ref:`str <api.series.str>`
Categorical :ref:`cat <api.series.cat>`
=========== ===========================

.. _api.series.dt:

Date Time Handling
------------------

``Series.dt`` can be used to access the values of the series as
datetimelike and return several properties.
These can be accessed like ``Series.dt.<property>``.

Datetime Properties
~~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: api/

   Series.dt.date
   Series.dt.year
   Series.dt.month
   Series.dt.day
   Series.dt.hour
   Series.dt.minute
   Series.dt.second
   Series.dt.microsecond
   Series.dt.week
   Series.dt.weekofyear
   Series.dt.dayofweek
   Series.dt.weekday
   Series.dt.dayofyear
   Series.dt.quarter
   Series.dt.is_month_start
   Series.dt.is_month_end
   Series.dt.is_quarter_start
   Series.dt.is_quarter_end
   Series.dt.is_year_start
   Series.dt.is_year_end
   Series.dt.is_leap_year
   Series.dt.daysinmonth
   Series.dt.days_in_month

Datetime Methods
~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: api/

   Series.dt.normalize
   Series.dt.strftime
   Series.dt.round
   Series.dt.floor
   Series.dt.ceil
   Series.dt.month_name
   Series.dt.day_name

.. _api.series.str:

String Handling
---------------

``Series.str`` can be used to access the values of the series as
strings and apply several methods to it. These can be accessed
like ``Series.str.<function/property>``.

.. autosummary::
   :toctree: api/

   Series.str.capitalize
   Series.str.cat
   Series.str.center
   Series.str.contains
   Series.str.count
   Series.str.decode
   Series.str.encode
   Series.str.endswith
   Series.str.extract
   Series.str.extractall
   Series.str.find
   Series.str.findall
   Series.str.get
   Series.str.get_dummies
   Series.str.index
   Series.str.isalnum
   Series.str.isalpha
   Series.str.isdigit
   Series.str.isspace
   Series.str.islower
   Series.str.isupper
   Series.str.istitle
   Series.str.isnumeric
   Series.str.isdecimal
   Series.str.join
   Series.str.len
   Series.str.ljust
   Series.str.lower
   Series.str.lstrip
   Series.str.match
   Series.str.normalize
   Series.str.pad
   Series.str.partition
   Series.str.repeat
   Series.str.replace
   Series.str.rfind
   Series.str.rindex
   Series.str.rjust
   Series.str.rpartition
   Series.str.rsplit
   Series.str.rstrip
   Series.str.slice
   Series.str.slice_replace
   Series.str.split
   Series.str.startswith
   Series.str.strip
   Series.str.swapcase
   Series.str.title
   Series.str.translate
   Series.str.upper
   Series.str.wrap
   Series.str.zfill

.. _api.series.cat:

Categorical accessor
--------------------
Categorical-dtype specific methods and attributes are available under
the ``Series.cat`` accessor.

.. autosummary::
   :toctree: api/

   Series.cat.categories
   Series.cat.ordered
   Series.cat.codes
   Series.cat.rename_categories
   Series.cat.reorder_categories
   Series.cat.add_categories
   Series.cat.remove_categories
   Series.cat.remove_unused_categories
   Series.cat.set_categories
   Series.cat.as_ordered
   Series.cat.as_unordered

.. _api.series.plot:

Plotting
-------------------------------
``Series.plot`` is both a callable method and a namespace attribute for
specific plotting methods of the form ``Series.plot.<kind>``.

.. autosummary::
   :toctree: api/

   Series.plot
   Series.plot.area
   Series.plot.bar
   Series.plot.barh
   Series.plot.box
   Series.plot.density
   Series.plot.hist
   Series.plot.line
   Series.plot.pie
   Series.plot.kde
   Series.hist

Serialization / IO / Conversion
-------------------------------
.. autosummary::
   :toctree: api/

   Series.to_pandas
   Series.to_numpy
   Series.to_list
   Series.to_string
   Series.to_dict
   Series.to_clipboard
   Series.to_latex
   Series.to_markdown
   Series.to_json
   Series.to_csv
   Series.to_excel
   Series.to_frame

Pandas-on-Spark specific
------------------------
``Series.pandas_on_spark`` provides pandas-on-Spark specific features that exists only in pandas API on Spark.
These can be accessed by ``Series.pandas_on_spark.<function/property>``.

.. autosummary::
   :toctree: api/

   Series.pandas_on_spark.transform_batch

