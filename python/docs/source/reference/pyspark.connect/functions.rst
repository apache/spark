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


=========
Functions
=========
.. currentmodule:: pyspark.sql.connect

Normal Functions
----------------
.. autosummary::
    :toctree: api/

    functions.col
    functions.column
    functions.lit
    functions.broadcast
    functions.coalesce
    functions.input_file_name
    functions.isnan
    functions.isnull
    functions.monotonically_increasing_id
    functions.nanvl
    functions.rand
    functions.randn
    functions.spark_partition_id
    functions.when
    functions.bitwise_not
    functions.bitwiseNOT
    functions.expr
    functions.greatest
    functions.least


Math Functions
--------------
.. autosummary::
    :toctree: api/

    functions.sqrt
    functions.abs
    functions.acos
    functions.acosh
    functions.asin
    functions.asinh
    functions.atan
    functions.atanh
    functions.atan2
    functions.bin
    functions.cbrt
    functions.ceil
    functions.conv
    functions.cos
    functions.cosh
    functions.cot
    functions.csc
    functions.exp
    functions.expm1
    functions.factorial
    functions.floor
    functions.hex
    functions.unhex
    functions.hypot
    functions.log
    functions.log10
    functions.log1p
    functions.log2
    functions.pmod
    functions.pow
    functions.rint
    functions.round
    functions.bround
    functions.sec
    functions.shiftleft
    functions.shiftright
    functions.shiftrightunsigned
    functions.signum
    functions.sin
    functions.sinh
    functions.tan
    functions.tanh
    functions.toDegrees
    functions.degrees
    functions.toRadians
    functions.radians


Datetime Functions
------------------
.. autosummary::
    :toctree: api/

    functions.add_months
    functions.current_date
    functions.current_timestamp
    functions.date_add
    functions.date_format
    functions.date_sub
    functions.date_trunc
    functions.datediff
    functions.dayofmonth
    functions.dayofweek
    functions.dayofyear
    functions.second
    functions.weekofyear
    functions.year
    functions.quarter
    functions.month
    functions.last_day
    functions.localtimestamp
    functions.minute
    functions.months_between
    functions.next_day
    functions.hour
    functions.make_date
    functions.from_unixtime
    functions.unix_timestamp
    functions.to_timestamp
    functions.to_date
    functions.trunc
    functions.from_utc_timestamp
    functions.to_utc_timestamp
    functions.window
    functions.session_window
    functions.timestamp_seconds
    functions.window_time


Collection Functions
--------------------
.. autosummary::
    :toctree: api/

    functions.array
    functions.array_contains
    functions.arrays_overlap
    functions.array_join
    functions.create_map
    functions.slice
    functions.concat
    functions.array_position
    functions.element_at
    functions.array_append
    functions.array_sort
    functions.array_insert
    functions.array_remove
    functions.array_distinct
    functions.array_intersect
    functions.array_union
    functions.array_except
    functions.array_compact
    functions.transform
    functions.exists
    functions.forall
    functions.filter
    functions.aggregate
    functions.zip_with
    functions.transform_keys
    functions.transform_values
    functions.map_filter
    functions.map_from_arrays
    functions.map_zip_with
    functions.explode
    functions.explode_outer
    functions.posexplode
    functions.posexplode_outer
    functions.inline
    functions.inline_outer
    functions.get
    functions.get_json_object
    functions.json_tuple
    functions.from_json
    functions.schema_of_json
    functions.to_json
    functions.size
    functions.struct
    functions.sort_array
    functions.array_max
    functions.array_min
    functions.shuffle
    functions.reverse
    functions.flatten
    functions.sequence
    functions.array_repeat
    functions.map_contains_key
    functions.map_keys
    functions.map_values
    functions.map_entries
    functions.map_from_entries
    functions.arrays_zip
    functions.map_concat
    functions.from_csv
    functions.schema_of_csv
    functions.to_csv


Partition Transformation Functions
----------------------------------
.. autosummary::
    :toctree: api/

    functions.years
    functions.months
    functions.days
    functions.hours
    functions.bucket


Aggregate Functions
-------------------
.. autosummary::
    :toctree: api/

    functions.approxCountDistinct
    functions.approx_count_distinct
    functions.avg
    functions.collect_list
    functions.collect_set
    functions.corr
    functions.count
    functions.count_distinct
    functions.countDistinct
    functions.covar_pop
    functions.covar_samp
    functions.first
    functions.grouping
    functions.grouping_id
    functions.kurtosis
    functions.last
    functions.max
    functions.max_by
    functions.mean
    functions.median
    functions.min
    functions.min_by
    functions.mode
    functions.percentile_approx
    functions.product
    functions.skewness
    functions.stddev
    functions.stddev_pop
    functions.stddev_samp
    functions.sum
    functions.sum_distinct
    functions.sumDistinct
    functions.var_pop
    functions.var_samp
    functions.variance


Window Functions
----------------
.. autosummary::
    :toctree: api/

    functions.cume_dist
    functions.dense_rank
    functions.lag
    functions.lead
    functions.nth_value
    functions.ntile
    functions.percent_rank
    functions.rank
    functions.row_number


Sort Functions
--------------
.. autosummary::
    :toctree: api/

    functions.asc
    functions.asc_nulls_first
    functions.asc_nulls_last
    functions.desc
    functions.desc_nulls_first
    functions.desc_nulls_last


String Functions
----------------
.. autosummary::
    :toctree: api/

    functions.ascii
    functions.base64
    functions.bit_length
    functions.concat_ws
    functions.decode
    functions.encode
    functions.format_number
    functions.format_string
    functions.initcap
    functions.instr
    functions.length
    functions.lower
    functions.levenshtein
    functions.locate
    functions.lpad
    functions.ltrim
    functions.octet_length
    functions.regexp_extract
    functions.regexp_replace
    functions.unbase64
    functions.rpad
    functions.repeat
    functions.rtrim
    functions.soundex
    functions.split
    functions.substring
    functions.substring_index
    functions.overlay
    functions.sentences
    functions.translate
    functions.trim
    functions.upper


UDF
---
.. autosummary::
    :toctree: api/

    functions.call_udf
    functions.udf
    functions.unwrap_udt

Misc Functions
--------------
.. autosummary::
    :toctree: api/

    functions.md5
    functions.sha1
    functions.sha2
    functions.crc32
    functions.hash
    functions.xxhash64
    functions.assert_true
    functions.raise_error

