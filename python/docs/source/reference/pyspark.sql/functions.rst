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
    ceiling
    conv
    cos
    cosh
    cot
    csc
    e
    exp
    expm1
    factorial
    floor
    hex
    unhex
    hypot
    ln
    log
    log10
    log1p
    log2
    negate
    negative
    pi
    pmod
    positive
    pow
    power
    rint
    round
    bround
    sec
    shiftleft
    shiftright
    shiftrightunsigned
    sign
    signum
    sin
    sinh
    tan
    tanh
    toDegrees
    degrees
    toRadians
    radians
    width_bucket


Datetime Functions
------------------
.. autosummary::
    :toctree: api/

    add_months
    convert_timezone
    curdate
    current_date
    current_timestamp
    current_timezone
    date_add
    date_diff
    date_format
    date_from_unix_date
    date_sub
    date_trunc
    dateadd
    datediff
    day
    date_part
    datepart
    dayofmonth
    dayofweek
    dayofyear
    extract
    second
    weekofyear
    year
    quarter
    month
    last_day
    localtimestamp
    make_dt_interval
    make_interval
    make_timestamp
    make_timestamp_ltz
    make_timestamp_ntz
    make_ym_interval
    minute
    months_between
    next_day
    hour
    make_date
    now
    from_unixtime
    unix_timestamp
    to_unix_timestamp
    to_timestamp
    to_timestamp_ltz
    to_timestamp_ntz
    to_date
    trunc
    from_utc_timestamp
    to_utc_timestamp
    weekday
    window
    session_window
    timestamp_micros
    timestamp_millis
    timestamp_seconds
    unix_date
    unix_micros
    unix_millis
    unix_seconds
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
    array_prepend
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
    str_to_map
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

    any_value
    approxCountDistinct
    approx_count_distinct
    approx_percentile
    avg
    bit_and
    bit_or
    bit_xor
    bool_and
    bool_or
    collect_list
    collect_set
    corr
    count
    count_distinct
    countDistinct
    count_if
    covar_pop
    covar_samp
    every
    first
    first_value
    grouping
    grouping_id
    histogram_numeric
    hll_sketch_agg
    hll_union_agg
    kurtosis
    last
    last_value
    max
    max_by
    mean
    median
    min
    min_by
    mode
    percentile
    percentile_approx
    product
    reduce
    regr_avgx
    regr_avgy
    regr_count
    regr_intercept
    regr_r2
    regr_slope
    regr_sxx
    regr_sxy
    regr_syy
    skewness
    some
    std
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
    btrim
    char
    character_length
    char_length
    chr
    concat_ws
    contains
    decode
    elt
    encode
    endswith
    find_in_set
    format_number
    format_string
    ilike
    initcap
    instr
    lcase
    length
    like
    lower
    left
    levenshtein
    locate
    lpad
    ltrim
    octet_length
    parse_url
    position
    printf
    rlike
    regexp
    regexp_like
    regexp_count
    regexp_extract
    regexp_extract_all
    regexp_replace
    regexp_substr
    regexp_instr
    replace
    right
    ucase
    unbase64
    rpad
    repeat
    rtrim
    soundex
    split
    split_part
    startswith
    substr
    substring
    substring_index
    overlay
    sentences
    to_binary
    to_char
    to_number
    translate
    trim
    upper
    url_decode
    url_encode


Bitwise Functions
-----------------
.. autosummary::
    :toctree: api/

    bit_count
    bit_get
    getbit


UDF
---
.. autosummary::
    :toctree: api/

    call_udf
    pandas_udf
    udf
    udtf
    unwrap_udt

Misc Functions
--------------
.. autosummary::
    :toctree: api/

    current_catalog
    current_database
    current_schema
    current_user
    md5
    sha1
    sha2
    crc32
    hash
    xxhash64
    assert_true
    raise_error
    hll_sketch_estimate
    hll_union
    user

Predicate Functions
-------------------
.. autosummary::
    :toctree: api/

    equal_null
    ifnull
    isnotnull
    nullif
    nvl
    nvl2

Xml Functions
--------------
.. autosummary::
    :toctree: api/

    xpath
    xpath_boolean
    xpath_double
    xpath_float
    xpath_int
    xpath_long
    xpath_number
    xpath_short
    xpath_string

