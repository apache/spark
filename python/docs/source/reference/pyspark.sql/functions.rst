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
.. currentmodule:: pyspark.sql.functions

Normal Functions
----------------
.. autosummary::
    :toctree: api/

    col
    column
    lit
    broadcast
    coalesce
    input_file_name
    isnan
    isnull
    monotonically_increasing_id
    nanvl
    rand
    randn
    spark_partition_id
    when
    bitwise_not
    bitwiseNOT
    expr
    greatest
    least


Math Functions
--------------
.. autosummary::
    :toctree: api/

    sqrt
    abs
    acos
    acosh
    asin
    asinh
    atan
    atanh
    atan2
    bin
    cbrt
    ceil
    conv
    cos
    cosh
    cot
    csc
    exp
    expm1
    factorial
    floor
    hex
    unhex
    hypot
    log
    log10
    log1p
    log2
    pmod
    pow
    rint
    round
    bround
    sec
    shiftleft
    shiftright
    shiftrightunsigned
    signum
    sin
    sinh
    tan
    tanh
    toDegrees
    degrees
    toRadians
    radians


Datetime Functions
------------------
.. autosummary::
    :toctree: api/

    add_months
    current_date
    current_timestamp
    date_add
    date_format
    date_sub
    date_trunc
    datediff
    dayofmonth
    dayofweek
    dayofyear
    second
    weekofyear
    year
    quarter
    month
    last_day
    localtimestamp
    minute
    months_between
    next_day
    hour
    make_date
    from_unixtime
    unix_timestamp
    to_timestamp
    to_date
    trunc
    from_utc_timestamp
    to_utc_timestamp
    window
    session_window
    timestamp_seconds
    window_time


Collection Functions
--------------------
.. autosummary::
    :toctree: api/

    array
    array_contains
    arrays_overlap
    array_join
    create_map
    slice
    concat
    array_position
    element_at
    array_append
    array_sort
    array_insert
    array_remove
    array_distinct
    array_intersect
    array_union
    array_except
    array_compact
    transform
    exists
    forall
    filter
    aggregate
    zip_with
    transform_keys
    transform_values
    map_filter
    map_from_arrays
    map_zip_with
    explode
    explode_outer
    posexplode
    posexplode_outer
    inline
    inline_outer
    get
    get_json_object
    json_tuple
    from_json
    schema_of_json
    to_json
    size
    struct
    sort_array
    array_max
    array_min
    shuffle
    reverse
    flatten
    sequence
    array_repeat
    map_contains_key
    map_keys
    map_values
    map_entries
    map_from_entries
    arrays_zip
    map_concat
    from_csv
    schema_of_csv
    to_csv


Partition Transformation Functions
----------------------------------
.. autosummary::
    :toctree: api/

    years
    months
    days
    hours
    bucket


Aggregate Functions
-------------------
.. autosummary::
    :toctree: api/

    approxCountDistinct
    approx_count_distinct
    avg
    collect_list
    collect_set
    corr
    count
    count_distinct
    countDistinct
    covar_pop
    covar_samp
    first
    grouping
    grouping_id
    kurtosis
    last
    max
    max_by
    mean
    median
    min
    min_by
    mode
    percentile_approx
    product
    skewness
    stddev
    stddev_pop
    stddev_samp
    sum
    sum_distinct
    sumDistinct
    var_pop
    var_samp
    variance


Window Functions
----------------
.. autosummary::
    :toctree: api/

    cume_dist
    dense_rank
    lag
    lead
    nth_value
    ntile
    percent_rank
    rank
    row_number


Sort Functions
--------------
.. autosummary::
    :toctree: api/

    asc
    asc_nulls_first
    asc_nulls_last
    desc
    desc_nulls_first
    desc_nulls_last


String Functions
----------------
.. autosummary::
    :toctree: api/

    ascii
    base64
    bit_length
    concat_ws
    decode
    encode
    format_number
    format_string
    initcap
    instr
    length
    lower
    levenshtein
    locate
    lpad
    ltrim
    octet_length
    regexp_extract
    regexp_replace
    unbase64
    rpad
    repeat
    rtrim
    soundex
    split
    substring
    substring_index
    overlay
    sentences
    translate
    trim
    upper


UDF
---
.. autosummary::
    :toctree: api/

    call_udf
    pandas_udf
    udf
    unwrap_udt

Misc Functions
--------------
.. autosummary::
    :toctree: api/

    md5
    sha1
    sha2
    crc32
    hash
    xxhash64
    assert_true
    raise_error

