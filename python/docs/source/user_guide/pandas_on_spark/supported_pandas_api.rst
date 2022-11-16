
=====================
Supported pandas API
=====================

.. currentmodule:: pyspark.pandas

The following table shows the pandas APIs that implemented or non-implemented from pandas API on
Spark. Some pandas API do not implement full parameters, so the third column shows missing
parameters for each API.

* 'Y' in the second column means it's implemented including its whole parameter.
* 'N' means it's not implemented yet.
* 'P' means it's partially implemented with the missing of some parameters.

All API in the list below computes the data with distributed execution except the ones that require
the local execution by design. For example, `DataFrame.to_numpy() <https://spark.apache.org/docs/
latest/api/python/reference/pyspark.pandas/api/pyspark.pandas.DataFrame.to_numpy.html>`__
requires to collect the data to the driver side.

If there is non-implemented pandas API or parameter you want, you can create an `Apache Spark
JIRA <https://issues.apache.org/jira/projects/SPARK/summary>`__ to request or to contribute by
your own.

The API list is updated based on the `latest pandas official API reference
<https://pandas.pydata.org/docs/reference/index.html#>`__.

CategoricalIndex API
----------------------------------------------------------------------------------------------------
.. currentmodule:: pyspark.pandas.CategoricalIndex

.. list-table::
    :header-rows: 1

    * - API
      - Implemented
      - Missing parameters
    * - :func:`add_categories`
      - Y
      - 
    * - argsort
      - N
      - 
    * - :func:`as_ordered`
      - Y
      - 
    * - :func:`as_unordered`
      - Y
      - 
    * - astype
      - N
      - 
    * - equals
      - N
      - 
    * - is_dtype_equal
      - N
      - 
    * - :func:`map`
      - Y
      - 
    * - max
      - N
      - 
    * - min
      - N
      - 
    * - reindex
      - N
      - 
    * - :func:`remove_categories`
      - Y
      - 
    * - :func:`remove_unused_categories`
      - Y
      - 
    * - :func:`rename_categories`
      - Y
      - 
    * - :func:`reorder_categories`
      - Y
      - 
    * - searchsorted
      - N
      - 
    * - :func:`set_categories`
      - Y
      - 
    * - take_nd
      - N
      - 
    * - tolist
      - N
      - 

DataFrame API
----------------------------------------------------------------------------------------------------
.. currentmodule:: pyspark.pandas.DataFrame

.. list-table::
    :header-rows: 1

    * - API
      - Implemented
      - Missing parameters
    * - :func:`add`
      - P
      - ``axis`` , ``fill_value`` , ``level``
    * - :func:`agg`
      - P
      - ``axis``
    * - :func:`aggregate`
      - P
      - ``axis``
    * - :func:`align`
      - P
      - ``broadcast_axis`` , ``fill_axis`` , ``fill_value`` , ``level`` , ``limit`` and more. See the `pandas.DataFrame.align <https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.align.html>`__ and `pyspark.pandas.DataFrame.align <https://spark.apache.org/docs/latest/api/python/reference/pyspark.pandas/api/pyspark.pandas.DataFrame.align.html>`__ for detail.
    * - :func:`all`
      - P
      - ``level``
    * - :func:`any`
      - P
      - ``level`` , ``skipna``
    * - :func:`append`
      - Y
      - 
    * - :func:`apply`
      - P
      - ``raw`` , ``result_type``
    * - :func:`applymap`
      - P
      - ``na_action``
    * - asfreq
      - N
      - 
    * - :func:`assign`
      - Y
      - 
    * - bfill
      - N
      - 
    * - :func:`boxplot`
      - P
      - ``ax`` , ``backend`` , ``by`` , ``column`` , ``figsize`` and more. See the `pandas.DataFrame.boxplot <https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.boxplot.html>`__ and `pyspark.pandas.DataFrame.boxplot <https://spark.apache.org/docs/latest/api/python/reference/pyspark.pandas/api/pyspark.pandas.DataFrame.boxplot.html>`__ for detail.
    * - :func:`clip`
      - P
      - ``axis`` , ``inplace``
    * - combine
      - N
      - 
    * - :func:`combine_first`
      - Y
      - 
    * - compare
      - N
      - 
    * - :func:`corr`
      - P
      - ``numeric_only``
    * - :func:`corrwith`
      - P
      - ``numeric_only``
    * - count
      - N
      - 
    * - :func:`cov`
      - P
      - ``numeric_only``
    * - cummax
      - N
      - 
    * - cummin
      - N
      - 
    * - cumprod
      - N
      - 
    * - cumsum
      - N
      - 
    * - :func:`diff`
      - Y
      - 
    * - :func:`div`
      - P
      - ``axis`` , ``fill_value`` , ``level``
    * - :func:`divide`
      - P
      - ``axis`` , ``fill_value`` , ``level``
    * - :func:`dot`
      - Y
      - 
    * - :func:`drop`
      - P
      - ``errors`` , ``inplace`` , ``level``
    * - :func:`drop_duplicates`
      - Y
      - 
    * - :func:`dropna`
      - Y
      - 
    * - :func:`duplicated`
      - Y
      - 
    * - :func:`eq`
      - P
      - ``axis`` , ``level``
    * - :func:`eval`
      - Y
      - 
    * - :func:`explode`
      - Y
      - 
    * - ffill
      - N
      - 
    * - :func:`fillna`
      - P
      - ``downcast``
    * - :func:`floordiv`
      - P
      - ``axis`` , ``fill_value`` , ``level``
    * - :func:`ge`
      - P
      - ``axis`` , ``level``
    * - :func:`groupby`
      - P
      - ``group_keys`` , ``level`` , ``observed`` , ``sort`` , ``squeeze``
    * - :func:`gt`
      - P
      - ``axis`` , ``level``
    * - :func:`hist`
      - P
      - ``ax`` , ``backend`` , ``by`` , ``column`` , ``data`` and more. See the `pandas.DataFrame.hist <https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.hist.html>`__ and `pyspark.pandas.DataFrame.hist <https://spark.apache.org/docs/latest/api/python/reference/pyspark.pandas/api/pyspark.pandas.DataFrame.hist.html>`__ for detail.
    * - :func:`idxmax`
      - P
      - ``numeric_only`` , ``skipna``
    * - :func:`idxmin`
      - P
      - ``numeric_only`` , ``skipna``
    * - :func:`info`
      - P
      - ``memory_usage`` , ``show_counts``
    * - :func:`insert`
      - Y
      - 
    * - :func:`interpolate`
      - P
      - ``axis`` , ``downcast`` , ``inplace``
    * - isetitem
      - N
      - 
    * - :func:`isin`
      - Y
      - 
    * - :func:`isna`
      - Y
      - 
    * - :func:`isnull`
      - Y
      - 
    * - :func:`items`
      - Y
      - 
    * - :func:`iteritems`
      - Y
      - 
    * - :func:`iterrows`
      - Y
      - 
    * - :func:`itertuples`
      - Y
      - 
    * - :func:`join`
      - P
      - ``other`` , ``sort`` , ``validate``
    * - kurt
      - N
      - 
    * - kurtosis
      - N
      - 
    * - :func:`le`
      - P
      - ``axis`` , ``level``
    * - lookup
      - N
      - 
    * - :func:`lt`
      - P
      - ``axis`` , ``level``
    * - :func:`mad`
      - P
      - ``level`` , ``skipna``
    * - :func:`mask`
      - P
      - ``axis`` , ``errors`` , ``inplace`` , ``level`` , ``try_cast``
    * - max
      - N
      - 
    * - mean
      - N
      - 
    * - median
      - N
      - 
    * - :func:`melt`
      - P
      - ``col_level`` , ``ignore_index``
    * - memory_usage
      - N
      - 
    * - :func:`merge`
      - P
      - ``copy`` , ``indicator`` , ``sort`` , ``validate``
    * - min
      - N
      - 
    * - :func:`mod`
      - P
      - ``axis`` , ``fill_value`` , ``level``
    * - :func:`mode`
      - Y
      - 
    * - :func:`mul`
      - P
      - ``axis`` , ``fill_value`` , ``level``
    * - :func:`multiply`
      - P
      - ``axis`` , ``fill_value`` , ``level``
    * - :func:`ne`
      - P
      - ``axis`` , ``level``
    * - :func:`nlargest`
      - Y
      - 
    * - :func:`notna`
      - Y
      - 
    * - :func:`notnull`
      - Y
      - 
    * - :func:`nsmallest`
      - Y
      - 
    * - :func:`nunique`
      - Y
      - 
    * - :func:`pivot`
      - Y
      - 
    * - :func:`pivot_table`
      - P
      - ``dropna`` , ``margins`` , ``margins_name`` , ``observed`` , ``sort``
    * - :func:`pop`
      - Y
      - 
    * - :func:`pow`
      - P
      - ``axis`` , ``fill_value`` , ``level``
    * - prod
      - N
      - 
    * - product
      - N
      - 
    * - :func:`quantile`
      - P
      - ``interpolation`` , ``method``
    * - :func:`query`
      - Y
      - 
    * - :func:`radd`
      - P
      - ``axis`` , ``fill_value`` , ``level``
    * - :func:`rdiv`
      - P
      - ``axis`` , ``fill_value`` , ``level``
    * - :func:`reindex`
      - P
      - ``level`` , ``limit`` , ``method`` , ``tolerance``
    * - :func:`rename`
      - P
      - ``copy``
    * - reorder_levels
      - N
      - 
    * - :func:`replace`
      - Y
      - 
    * - :func:`resample`
      - P
      - ``axis`` , ``base`` , ``convention`` , ``group_keys`` , ``kind`` and more. See the `pandas.DataFrame.resample <https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.resample.html>`__ and `pyspark.pandas.DataFrame.resample <https://spark.apache.org/docs/latest/api/python/reference/pyspark.pandas/api/pyspark.pandas.DataFrame.resample.html>`__ for detail.
    * - :func:`reset_index`
      - P
      - ``allow_duplicates`` , ``names``
    * - :func:`rfloordiv`
      - P
      - ``axis`` , ``fill_value`` , ``level``
    * - :func:`rmod`
      - P
      - ``axis`` , ``fill_value`` , ``level``
    * - :func:`rmul`
      - P
      - ``axis`` , ``fill_value`` , ``level``
    * - :func:`round`
      - Y
      - 
    * - :func:`rpow`
      - P
      - ``axis`` , ``fill_value`` , ``level``
    * - :func:`rsub`
      - P
      - ``axis`` , ``fill_value`` , ``level``
    * - :func:`rtruediv`
      - P
      - ``axis`` , ``fill_value`` , ``level``
    * - :func:`select_dtypes`
      - Y
      - 
    * - sem
      - N
      - 
    * - set_axis
      - N
      - 
    * - :func:`set_index`
      - P
      - ``verify_integrity``
    * - :func:`shift`
      - P
      - ``axis`` , ``freq``
    * - skew
      - N
      - 
    * - :func:`sort_index`
      - P
      - ``key`` , ``sort_remaining``
    * - :func:`sort_values`
      - P
      - ``axis`` , ``key`` , ``kind``
    * - :func:`stack`
      - P
      - ``dropna`` , ``level``
    * - std
      - N
      - 
    * - :func:`sub`
      - P
      - ``axis`` , ``fill_value`` , ``level``
    * - :func:`subtract`
      - P
      - ``axis`` , ``fill_value`` , ``level``
    * - sum
      - N
      - 
    * - :func:`swaplevel`
      - Y
      - 
    * - :func:`to_dict`
      - Y
      - 
    * - to_feather
      - N
      - 
    * - to_gbq
      - N
      - 
    * - :func:`to_html`
      - P
      - ``encoding``
    * - to_markdown
      - N
      - 
    * - to_numpy
      - N
      - 
    * - :func:`to_orc`
      - P
      - ``engine`` , ``engine_kwargs`` , ``index``
    * - :func:`to_parquet`
      - P
      - ``engine`` , ``index`` , ``storage_options``
    * - to_period
      - N
      - 
    * - :func:`to_records`
      - Y
      - 
    * - to_stata
      - N
      - 
    * - :func:`to_string`
      - P
      - ``encoding`` , ``max_colwidth`` , ``min_rows``
    * - to_timestamp
      - N
      - 
    * - to_xml
      - N
      - 
    * - :func:`transform`
      - Y
      - 
    * - :func:`transpose`
      - P
      - ``copy``
    * - :func:`truediv`
      - P
      - ``axis`` , ``fill_value`` , ``level``
    * - :func:`unstack`
      - P
      - ``fill_value`` , ``level``
    * - :func:`update`
      - P
      - ``errors`` , ``filter_func``
    * - value_counts
      - N
      - 
    * - var
      - N
      - 
    * - :func:`where`
      - P
      - ``errors`` , ``inplace`` , ``level`` , ``try_cast``

DatetimeIndex API
----------------------------------------------------------------------------------------------------
.. currentmodule:: pyspark.pandas.DatetimeIndex

.. list-table::
    :header-rows: 1

    * - API
      - Implemented
      - Missing parameters
    * - :func:`ceil`
      - Y
      - 
    * - :func:`day_name`
      - Y
      - 
    * - :func:`floor`
      - Y
      - 
    * - get_loc
      - N
      - 
    * - :func:`indexer_at_time`
      - Y
      - 
    * - :func:`indexer_between_time`
      - Y
      - 
    * - isocalendar
      - N
      - 
    * - :func:`month_name`
      - Y
      - 
    * - :func:`normalize`
      - Y
      - 
    * - :func:`round`
      - Y
      - 
    * - slice_indexer
      - N
      - 
    * - snap
      - N
      - 
    * - std
      - N
      - 
    * - :func:`strftime`
      - Y
      - 
    * - to_julian_date
      - N
      - 
    * - to_period
      - N
      - 
    * - to_perioddelta
      - N
      - 
    * - to_pydatetime
      - N
      - 
    * - to_series
      - N
      - 
    * - tz_convert
      - N
      - 
    * - tz_localize
      - N
      - 
    * - union_many
      - N
      - 

Index API
----------------------------------------------------------------------------------------------------
.. currentmodule:: pyspark.pandas.Index

.. list-table::
    :header-rows: 1

    * - API
      - Implemented
      - Missing parameters
    * - all
      - N
      - 
    * - any
      - N
      - 
    * - :func:`append`
      - Y
      - 
    * - :func:`argmax`
      - P
      - ``axis`` , ``skipna``
    * - :func:`argmin`
      - P
      - ``axis`` , ``skipna``
    * - argsort
      - N
      - 
    * - :func:`asof`
      - Y
      - 
    * - asof_locs
      - N
      - 
    * - astype
      - N
      - 
    * - :func:`copy`
      - P
      - ``dtype`` , ``names``
    * - :func:`delete`
      - Y
      - 
    * - :func:`difference`
      - Y
      - 
    * - :func:`drop`
      - P
      - ``errors``
    * - :func:`drop_duplicates`
      - Y
      - 
    * - :func:`droplevel`
      - Y
      - 
    * - :func:`dropna`
      - Y
      - 
    * - duplicated
      - N
      - 
    * - :func:`equals`
      - Y
      - 
    * - :func:`fillna`
      - P
      - ``downcast``
    * - format
      - N
      - 
    * - get_indexer
      - N
      - 
    * - get_indexer_for
      - N
      - 
    * - get_indexer_non_unique
      - N
      - 
    * - :func:`get_level_values`
      - Y
      - 
    * - get_loc
      - N
      - 
    * - get_slice_bound
      - N
      - 
    * - get_value
      - N
      - 
    * - groupby
      - N
      - 
    * - :func:`holds_integer`
      - Y
      - 
    * - :func:`identical`
      - Y
      - 
    * - :func:`insert`
      - Y
      - 
    * - :func:`intersection`
      - P
      - ``sort``
    * - is\_
      - N
      - 
    * - :func:`is_boolean`
      - Y
      - 
    * - :func:`is_categorical`
      - Y
      - 
    * - :func:`is_floating`
      - Y
      - 
    * - :func:`is_integer`
      - Y
      - 
    * - :func:`is_interval`
      - Y
      - 
    * - is_mixed
      - N
      - 
    * - :func:`is_numeric`
      - Y
      - 
    * - :func:`is_object`
      - Y
      - 
    * - :func:`is_type_compatible`
      - Y
      - 
    * - isin
      - N
      - 
    * - isna
      - N
      - 
    * - isnull
      - N
      - 
    * - join
      - N
      - 
    * - :func:`map`
      - Y
      - 
    * - :func:`max`
      - P
      - ``axis`` , ``skipna``
    * - memory_usage
      - N
      - 
    * - :func:`min`
      - P
      - ``axis`` , ``skipna``
    * - notna
      - N
      - 
    * - notnull
      - N
      - 
    * - putmask
      - N
      - 
    * - ravel
      - N
      - 
    * - reindex
      - N
      - 
    * - :func:`rename`
      - Y
      - 
    * - :func:`repeat`
      - P
      - ``axis``
    * - :func:`set_names`
      - Y
      - 
    * - set_value
      - N
      - 
    * - shift
      - N
      - 
    * - slice_indexer
      - N
      - 
    * - slice_locs
      - N
      - 
    * - :func:`sort`
      - Y
      - 
    * - :func:`sort_values`
      - P
      - ``key`` , ``na_position``
    * - sortlevel
      - N
      - 
    * - :func:`symmetric_difference`
      - Y
      - 
    * - take
      - N
      - 
    * - to_flat_index
      - N
      - 
    * - :func:`to_frame`
      - Y
      - 
    * - to_native_types
      - N
      - 
    * - :func:`to_series`
      - P
      - ``index``
    * - :func:`union`
      - Y
      - 
    * - :func:`unique`
      - Y
      - 
    * - :func:`view`
      - Y
      - 
    * - where
      - N
      - 

MultiIndex API
----------------------------------------------------------------------------------------------------
.. currentmodule:: pyspark.pandas.MultiIndex

.. list-table::
    :header-rows: 1

    * - API
      - Implemented
      - Missing parameters
    * - append
      - N
      - 
    * - argsort
      - N
      - 
    * - astype
      - N
      - 
    * - :func:`copy`
      - P
      - ``codes`` , ``dtype`` , ``levels`` , ``name`` , ``names``
    * - delete
      - N
      - 
    * - :func:`drop`
      - P
      - ``errors``
    * - :func:`drop_duplicates`
      - Y
      - 
    * - dropna
      - N
      - 
    * - duplicated
      - N
      - 
    * - :func:`equal_levels`
      - Y
      - 
    * - equals
      - N
      - 
    * - fillna
      - N
      - 
    * - format
      - N
      - 
    * - :func:`get_level_values`
      - Y
      - 
    * - get_loc
      - N
      - 
    * - get_loc_level
      - N
      - 
    * - get_locs
      - N
      - 
    * - get_slice_bound
      - N
      - 
    * - :func:`insert`
      - Y
      - 
    * - is_lexsorted
      - N
      - 
    * - isin
      - N
      - 
    * - memory_usage
      - N
      - 
    * - remove_unused_levels
      - N
      - 
    * - rename
      - N
      - 
    * - reorder_levels
      - N
      - 
    * - repeat
      - N
      - 
    * - set_codes
      - N
      - 
    * - set_levels
      - N
      - 
    * - set_names
      - N
      - 
    * - slice_locs
      - N
      - 
    * - sortlevel
      - N
      - 
    * - :func:`swaplevel`
      - Y
      - 
    * - take
      - N
      - 
    * - to_flat_index
      - N
      - 
    * - :func:`to_frame`
      - P
      - ``allow_duplicates``
    * - truncate
      - N
      - 
    * - unique
      - N
      - 
    * - view
      - N
      - 

Series API
----------------------------------------------------------------------------------------------------
.. currentmodule:: pyspark.pandas.Series

.. list-table::
    :header-rows: 1

    * - API
      - Implemented
      - Missing parameters
    * - :func:`add`
      - P
      - ``axis`` , ``fill_value`` , ``level``
    * - :func:`agg`
      - P
      - ``axis``
    * - :func:`aggregate`
      - P
      - ``axis``
    * - :func:`align`
      - P
      - ``broadcast_axis`` , ``fill_axis`` , ``fill_value`` , ``level`` , ``limit`` and more. See the `pandas.Series.align <https://pandas.pydata.org/docs/reference/api/pandas.Series.align.html>`__ and `pyspark.pandas.Series.align <https://spark.apache.org/docs/latest/api/python/reference/pyspark.pandas/api/pyspark.pandas.Series.align.html>`__ for detail.
    * - all
      - N
      - 
    * - any
      - N
      - 
    * - :func:`append`
      - Y
      - 
    * - :func:`apply`
      - P
      - ``convert_dtype``
    * - :func:`argsort`
      - P
      - ``axis`` , ``kind`` , ``order``
    * - asfreq
      - N
      - 
    * - :func:`autocorr`
      - P
      - ``lag``
    * - :func:`between`
      - Y
      - 
    * - bfill
      - N
      - 
    * - :func:`clip`
      - P
      - ``axis``
    * - combine
      - N
      - 
    * - :func:`combine_first`
      - Y
      - 
    * - :func:`compare`
      - P
      - ``align_axis`` , ``result_names``
    * - :func:`corr`
      - Y
      - 
    * - count
      - N
      - 
    * - :func:`cov`
      - Y
      - 
    * - cummax
      - N
      - 
    * - cummin
      - N
      - 
    * - cumprod
      - N
      - 
    * - cumsum
      - N
      - 
    * - :func:`diff`
      - Y
      - 
    * - :func:`div`
      - P
      - ``axis`` , ``fill_value`` , ``level``
    * - :func:`divide`
      - P
      - ``axis`` , ``fill_value`` , ``level``
    * - :func:`divmod`
      - P
      - ``axis`` , ``fill_value`` , ``level``
    * - :func:`dot`
      - Y
      - 
    * - :func:`drop`
      - P
      - ``axis`` , ``errors``
    * - :func:`drop_duplicates`
      - Y
      - 
    * - :func:`dropna`
      - P
      - ``how``
    * - :func:`duplicated`
      - Y
      - 
    * - :func:`eq`
      - P
      - ``axis`` , ``fill_value`` , ``level``
    * - :func:`explode`
      - P
      - ``ignore_index``
    * - ffill
      - N
      - 
    * - :func:`fillna`
      - P
      - ``downcast``
    * - :func:`floordiv`
      - P
      - ``axis`` , ``fill_value`` , ``level``
    * - :func:`ge`
      - P
      - ``axis`` , ``fill_value`` , ``level``
    * - :func:`groupby`
      - P
      - ``group_keys`` , ``level`` , ``observed`` , ``sort`` , ``squeeze``
    * - :func:`gt`
      - P
      - ``axis`` , ``fill_value`` , ``level``
    * - :func:`hist`
      - P
      - ``ax`` , ``backend`` , ``by`` , ``figsize`` , ``grid`` and more. See the `pandas.Series.hist <https://pandas.pydata.org/docs/reference/api/pandas.Series.hist.html>`__ and `pyspark.pandas.Series.hist <https://spark.apache.org/docs/latest/api/python/reference/pyspark.pandas/api/pyspark.pandas.Series.hist.html>`__ for detail.
    * - :func:`idxmax`
      - P
      - ``axis``
    * - :func:`idxmin`
      - P
      - ``axis``
    * - info
      - N
      - 
    * - :func:`interpolate`
      - P
      - ``axis`` , ``downcast`` , ``inplace``
    * - isin
      - N
      - 
    * - isna
      - N
      - 
    * - isnull
      - N
      - 
    * - :func:`items`
      - Y
      - 
    * - :func:`iteritems`
      - Y
      - 
    * - :func:`keys`
      - Y
      - 
    * - kurt
      - N
      - 
    * - kurtosis
      - N
      - 
    * - :func:`le`
      - P
      - ``axis`` , ``fill_value`` , ``level``
    * - :func:`lt`
      - P
      - ``axis`` , ``fill_value`` , ``level``
    * - :func:`mad`
      - P
      - ``axis`` , ``level`` , ``skipna``
    * - :func:`map`
      - Y
      - 
    * - :func:`mask`
      - P
      - ``axis`` , ``errors`` , ``inplace`` , ``level`` , ``try_cast``
    * - max
      - N
      - 
    * - mean
      - N
      - 
    * - median
      - N
      - 
    * - memory_usage
      - N
      - 
    * - min
      - N
      - 
    * - :func:`mod`
      - P
      - ``axis`` , ``fill_value`` , ``level``
    * - :func:`mode`
      - Y
      - 
    * - :func:`mul`
      - P
      - ``axis`` , ``fill_value`` , ``level``
    * - :func:`multiply`
      - P
      - ``axis`` , ``fill_value`` , ``level``
    * - :func:`ne`
      - P
      - ``axis`` , ``fill_value`` , ``level``
    * - :func:`nlargest`
      - P
      - ``keep``
    * - notna
      - N
      - 
    * - notnull
      - N
      - 
    * - :func:`nsmallest`
      - P
      - ``keep``
    * - :func:`pop`
      - Y
      - 
    * - :func:`pow`
      - P
      - ``axis`` , ``fill_value`` , ``level``
    * - prod
      - N
      - 
    * - product
      - N
      - 
    * - :func:`quantile`
      - P
      - ``interpolation``
    * - :func:`radd`
      - P
      - ``axis`` , ``fill_value`` , ``level``
    * - ravel
      - N
      - 
    * - :func:`rdiv`
      - P
      - ``axis`` , ``fill_value`` , ``level``
    * - :func:`rdivmod`
      - P
      - ``axis`` , ``fill_value`` , ``level``
    * - :func:`reindex`
      - Y
      - 
    * - :func:`rename`
      - P
      - ``axis`` , ``copy`` , ``errors`` , ``inplace`` , ``level``
    * - reorder_levels
      - N
      - 
    * - :func:`repeat`
      - P
      - ``axis``
    * - :func:`replace`
      - P
      - ``inplace`` , ``limit`` , ``method``
    * - :func:`resample`
      - P
      - ``axis`` , ``base`` , ``convention`` , ``group_keys`` , ``kind`` and more. See the `pandas.Series.resample <https://pandas.pydata.org/docs/reference/api/pandas.Series.resample.html>`__ and `pyspark.pandas.Series.resample <https://spark.apache.org/docs/latest/api/python/reference/pyspark.pandas/api/pyspark.pandas.Series.resample.html>`__ for detail.
    * - :func:`reset_index`
      - P
      - ``allow_duplicates``
    * - :func:`rfloordiv`
      - P
      - ``axis`` , ``fill_value`` , ``level``
    * - :func:`rmod`
      - P
      - ``axis`` , ``fill_value`` , ``level``
    * - :func:`rmul`
      - P
      - ``axis`` , ``fill_value`` , ``level``
    * - :func:`round`
      - Y
      - 
    * - :func:`rpow`
      - P
      - ``axis`` , ``fill_value`` , ``level``
    * - :func:`rsub`
      - P
      - ``axis`` , ``fill_value`` , ``level``
    * - :func:`rtruediv`
      - P
      - ``axis`` , ``fill_value`` , ``level``
    * - :func:`searchsorted`
      - P
      - ``sorter``
    * - sem
      - N
      - 
    * - set_axis
      - N
      - 
    * - shift
      - N
      - 
    * - skew
      - N
      - 
    * - :func:`sort_index`
      - P
      - ``key`` , ``sort_remaining``
    * - :func:`sort_values`
      - P
      - ``axis`` , ``key`` , ``kind``
    * - std
      - N
      - 
    * - :func:`sub`
      - P
      - ``axis`` , ``fill_value`` , ``level``
    * - :func:`subtract`
      - P
      - ``axis`` , ``fill_value`` , ``level``
    * - sum
      - N
      - 
    * - :func:`swaplevel`
      - Y
      - 
    * - take
      - N
      - 
    * - :func:`to_dict`
      - Y
      - 
    * - :func:`to_frame`
      - Y
      - 
    * - to_markdown
      - N
      - 
    * - to_period
      - N
      - 
    * - :func:`to_string`
      - P
      - ``min_rows``
    * - to_timestamp
      - N
      - 
    * - :func:`transform`
      - Y
      - 
    * - :func:`truediv`
      - P
      - ``axis`` , ``fill_value`` , ``level``
    * - :func:`unique`
      - Y
      - 
    * - :func:`unstack`
      - P
      - ``fill_value``
    * - :func:`update`
      - Y
      - 
    * - var
      - N
      - 
    * - view
      - N
      - 
    * - :func:`where`
      - P
      - ``axis`` , ``errors`` , ``inplace`` , ``level`` , ``try_cast``

TimedeltaIndex API
----------------------------------------------------------------------------------------------------
.. currentmodule:: pyspark.pandas.TimedeltaIndex

.. list-table::
    :header-rows: 1

    * - API
      - Implemented
      - Missing parameters
    * - ceil
      - N
      - 
    * - floor
      - N
      - 
    * - get_loc
      - N
      - 
    * - median
      - N
      - 
    * - round
      - N
      - 
    * - std
      - N
      - 
    * - sum
      - N
      - 
    * - to_pytimedelta
      - N
      - 
    * - total_seconds
      - N
      - 

General Function API
----------------------------------------------------------------------------------------------------
.. currentmodule:: pyspark.pandas

.. list-table::
    :header-rows: 1

    * - API
      - Implemented
      - Missing parameters
    * - array
      - N
      - 
    * - bdate_range
      - N
      - 
    * - :func:`concat`
      - P
      - ``copy`` , ``keys`` , ``levels`` , ``names`` , ``verify_integrity``
    * - crosstab
      - N
      - 
    * - cut
      - N
      - 
    * - :func:`date_range`
      - P
      - ``inclusive``
    * - eval
      - N
      - 
    * - factorize
      - N
      - 
    * - from_dummies
      - N
      - 
    * - :func:`get_dummies`
      - Y
      - 
    * - infer_freq
      - N
      - 
    * - interval_range
      - N
      - 
    * - :func:`isna`
      - Y
      - 
    * - :func:`isnull`
      - Y
      - 
    * - json_normalize
      - N
      - 
    * - lreshape
      - N
      - 
    * - :func:`melt`
      - P
      - ``col_level`` , ``ignore_index``
    * - :func:`merge`
      - P
      - ``copy`` , ``indicator`` , ``left`` , ``sort`` , ``validate``
    * - :func:`merge_asof`
      - Y
      - 
    * - merge_ordered
      - N
      - 
    * - :func:`notna`
      - Y
      - 
    * - :func:`notnull`
      - Y
      - 
    * - period_range
      - N
      - 
    * - pivot
      - N
      - 
    * - pivot_table
      - N
      - 
    * - qcut
      - N
      - 
    * - :func:`read_clipboard`
      - Y
      - 
    * - :func:`read_csv`
      - P
      - ``cache_dates`` , ``chunksize`` , ``compression`` , ``converters`` , ``date_parser`` and more. See the `pandas.read_csv <https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html>`__ and `pyspark.pandas.read_csv <https://spark.apache.org/docs/latest/api/python/reference/pyspark.pandas/api/pyspark.pandas.read_csv.html>`__ for detail.
    * - :func:`read_excel`
      - P
      - ``decimal`` , ``na_filter`` , ``storage_options``
    * - read_feather
      - N
      - 
    * - read_fwf
      - N
      - 
    * - read_gbq
      - N
      - 
    * - read_hdf
      - N
      - 
    * - :func:`read_html`
      - P
      - ``extract_links``
    * - :func:`read_json`
      - P
      - ``chunksize`` , ``compression`` , ``convert_axes`` , ``convert_dates`` , ``date_unit`` and more. See the `pandas.read_json <https://pandas.pydata.org/docs/reference/api/pandas.read_json.html>`__ and `pyspark.pandas.read_json <https://spark.apache.org/docs/latest/api/python/reference/pyspark.pandas/api/pyspark.pandas.read_json.html>`__ for detail.
    * - :func:`read_orc`
      - Y
      - 
    * - :func:`read_parquet`
      - P
      - ``engine`` , ``storage_options`` , ``use_nullable_dtypes``
    * - read_pickle
      - N
      - 
    * - read_sas
      - N
      - 
    * - read_spss
      - N
      - 
    * - :func:`read_sql`
      - P
      - ``chunksize`` , ``coerce_float`` , ``params`` , ``parse_dates``
    * - :func:`read_sql_query`
      - P
      - ``chunksize`` , ``coerce_float`` , ``dtype`` , ``params`` , ``parse_dates``
    * - :func:`read_sql_table`
      - P
      - ``chunksize`` , ``coerce_float`` , ``parse_dates``
    * - read_stata
      - N
      - 
    * - :func:`read_table`
      - P
      - ``cache_dates`` , ``chunksize`` , ``comment`` , ``compression`` , ``converters`` and more. See the `pandas.read_table <https://pandas.pydata.org/docs/reference/api/pandas.read_table.html>`__ and `pyspark.pandas.read_table <https://spark.apache.org/docs/latest/api/python/reference/pyspark.pandas/api/pyspark.pandas.read_table.html>`__ for detail.
    * - read_xml
      - N
      - 
    * - set_eng_float_format
      - N
      - 
    * - show_versions
      - N
      - 
    * - test
      - N
      - 
    * - :func:`timedelta_range`
      - Y
      - 
    * - :func:`to_datetime`
      - P
      - ``cache`` , ``dayfirst`` , ``exact`` , ``utc`` , ``yearfirst``
    * - :func:`to_numeric`
      - P
      - ``downcast``
    * - to_pickle
      - N
      - 
    * - :func:`to_timedelta`
      - Y
      - 
    * - unique
      - N
      - 
    * - value_counts
      - N
      - 
    * - wide_to_long
      - N
      - 

Expanding API
----------------------------------------------------------------------------------------------------
.. currentmodule:: pyspark.pandas.window.Expanding

.. list-table::
    :header-rows: 1

    * - API
      - Implemented
      - Missing parameters
    * - agg
      - N
      - 
    * - aggregate
      - N
      - 
    * - apply
      - N
      - 
    * - corr
      - N
      - 
    * - :func:`count`
      - P
      - ``numeric_only``
    * - cov
      - N
      - 
    * - :func:`kurt`
      - P
      - ``numeric_only``
    * - :func:`max`
      - P
      - ``engine`` , ``engine_kwargs`` , ``numeric_only``
    * - :func:`mean`
      - P
      - ``engine`` , ``engine_kwargs`` , ``numeric_only``
    * - median
      - N
      - 
    * - :func:`min`
      - P
      - ``engine`` , ``engine_kwargs`` , ``numeric_only``
    * - :func:`quantile`
      - P
      - ``interpolation`` , ``numeric_only``
    * - rank
      - N
      - 
    * - sem
      - N
      - 
    * - :func:`skew`
      - P
      - ``numeric_only``
    * - :func:`std`
      - P
      - ``ddof`` , ``engine`` , ``engine_kwargs`` , ``numeric_only``
    * - :func:`sum`
      - P
      - ``engine`` , ``engine_kwargs`` , ``numeric_only``
    * - :func:`var`
      - P
      - ``ddof`` , ``engine`` , ``engine_kwargs`` , ``numeric_only``

Rolling API
----------------------------------------------------------------------------------------------------
.. currentmodule:: pyspark.pandas.window.Rolling

.. list-table::
    :header-rows: 1

    * - API
      - Implemented
      - Missing parameters
    * - agg
      - N
      - 
    * - aggregate
      - N
      - 
    * - apply
      - N
      - 
    * - corr
      - N
      - 
    * - :func:`count`
      - P
      - ``numeric_only``
    * - cov
      - N
      - 
    * - :func:`kurt`
      - P
      - ``numeric_only``
    * - :func:`max`
      - P
      - ``engine`` , ``engine_kwargs`` , ``numeric_only``
    * - :func:`mean`
      - P
      - ``engine`` , ``engine_kwargs`` , ``numeric_only``
    * - median
      - N
      - 
    * - :func:`min`
      - P
      - ``engine`` , ``engine_kwargs`` , ``numeric_only``
    * - :func:`quantile`
      - P
      - ``interpolation`` , ``numeric_only``
    * - rank
      - N
      - 
    * - sem
      - N
      - 
    * - :func:`skew`
      - P
      - ``numeric_only``
    * - :func:`std`
      - P
      - ``ddof`` , ``engine`` , ``engine_kwargs`` , ``numeric_only``
    * - :func:`sum`
      - P
      - ``engine`` , ``engine_kwargs`` , ``numeric_only``
    * - :func:`var`
      - P
      - ``ddof`` , ``engine`` , ``engine_kwargs`` , ``numeric_only``

Window API
----------------------------------------------------------------------------------------------------
.. currentmodule:: pyspark.pandas.window.Window

.. list-table::
    :header-rows: 1

    * - API
      - Implemented
      - Missing parameters
    * - agg
      - N
      - 
    * - aggregate
      - N
      - 
    * - mean
      - N
      - 
    * - std
      - N
      - 
    * - sum
      - N
      - 
    * - var
      - N
      - 

DataFrameGroupBy API
----------------------------------------------------------------------------------------------------
.. currentmodule:: pyspark.pandas.groupby.DataFrameGroupBy

.. list-table::
    :header-rows: 1

    * - API
      - Implemented
      - Missing parameters
    * - agg
      - N
      - 
    * - aggregate
      - N
      - 
    * - boxplot
      - N
      - 
    * - filter
      - N
      - 
    * - idxmax
      - N
      - 
    * - idxmin
      - N
      - 
    * - nunique
      - N
      - 
    * - transform
      - N
      - 
    * - value_counts
      - N
      - 

GroupBy API
----------------------------------------------------------------------------------------------------
.. currentmodule:: pyspark.pandas.groupby.GroupBy

.. list-table::
    :header-rows: 1

    * - API
      - Implemented
      - Missing parameters
    * - :func:`all`
      - Y
      - 
    * - :func:`any`
      - P
      - ``skipna``
    * - :func:`apply`
      - Y
      - 
    * - :func:`backfill`
      - Y
      - 
    * - :func:`bfill`
      - Y
      - 
    * - :func:`count`
      - Y
      - 
    * - :func:`cumcount`
      - Y
      - 
    * - :func:`cummax`
      - P
      - ``axis`` , ``numeric_only``
    * - :func:`cummin`
      - P
      - ``axis`` , ``numeric_only``
    * - :func:`cumprod`
      - P
      - ``axis``
    * - :func:`cumsum`
      - P
      - ``axis``
    * - describe
      - N
      - 
    * - :func:`diff`
      - P
      - ``axis``
    * - :func:`ewm`
      - Y
      - 
    * - :func:`expanding`
      - Y
      - 
    * - :func:`ffill`
      - Y
      - 
    * - :func:`first`
      - Y
      - 
    * - :func:`head`
      - Y
      - 
    * - :func:`last`
      - P
      - ``min_count``
    * - :func:`max`
      - P
      - ``engine`` , ``engine_kwargs``
    * - :func:`mean`
      - P
      - ``engine`` , ``engine_kwargs``
    * - :func:`median`
      - Y
      - 
    * - :func:`min`
      - P
      - ``engine`` , ``engine_kwargs``
    * - ngroup
      - N
      - 
    * - :func:`nth`
      - P
      - ``dropna``
    * - ohlc
      - N
      - 
    * - :func:`pad`
      - Y
      - 
    * - pct_change
      - N
      - 
    * - :func:`prod`
      - Y
      - 
    * - :func:`quantile`
      - P
      - ``interpolation`` , ``numeric_only``
    * - :func:`rank`
      - P
      - ``axis`` , ``na_option`` , ``pct``
    * - resample
      - N
      - 
    * - :func:`rolling`
      - Y
      - 
    * - sample
      - N
      - 
    * - :func:`sem`
      - P
      - ``numeric_only``
    * - :func:`shift`
      - P
      - ``axis`` , ``freq``
    * - :func:`size`
      - Y
      - 
    * - :func:`std`
      - P
      - ``engine`` , ``engine_kwargs`` , ``numeric_only``
    * - :func:`sum`
      - P
      - ``engine`` , ``engine_kwargs``
    * - :func:`tail`
      - Y
      - 
    * - :func:`var`
      - P
      - ``engine`` , ``engine_kwargs`` , ``numeric_only``

SeriesGroupBy API
----------------------------------------------------------------------------------------------------
.. currentmodule:: pyspark.pandas.groupby.SeriesGroupBy

.. list-table::
    :header-rows: 1

    * - API
      - Implemented
      - Missing parameters
    * - :func:`agg`
      - P
      - ``engine`` , ``engine_kwargs`` , ``func``
    * - :func:`aggregate`
      - P
      - ``engine`` , ``engine_kwargs`` , ``func``
    * - apply
      - N
      - 
    * - describe
      - N
      - 
    * - filter
      - N
      - 
    * - :func:`nlargest`
      - P
      - ``keep``
    * - :func:`nsmallest`
      - P
      - ``keep``
    * - nunique
      - N
      - 
    * - transform
      - N
      - 
    * - :func:`value_counts`
      - P
      - ``bins`` , ``normalize``

