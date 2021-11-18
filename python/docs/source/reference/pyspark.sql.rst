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
Spark SQL
=========

Core Classes
------------

.. currentmodule:: pyspark.sql

.. autosummary::
    :toctree: api/

    SparkSession
    Catalog
    DataFrame
    Column
    Observation
    Row
    GroupedData
    PandasCogroupedOps
    DataFrameNaFunctions
    DataFrameStatFunctions
    Window


Spark Session APIs
------------------

.. currentmodule:: pyspark.sql

The entry point to programming Spark with the Dataset and DataFrame API.
To create a Spark session, you should use ``SparkSession.builder`` attribute.
See also :class:`SparkSession`.

.. autosummary::
    :toctree: api/

    SparkSession.builder.appName
    SparkSession.builder.config
    SparkSession.builder.enableHiveSupport
    SparkSession.builder.getOrCreate
    SparkSession.builder.master
    SparkSession.catalog
    SparkSession.conf
    SparkSession.createDataFrame
    SparkSession.getActiveSession
    SparkSession.newSession
    SparkSession.range
    SparkSession.read
    SparkSession.readStream
    SparkSession.sparkContext
    SparkSession.sql
    SparkSession.stop
    SparkSession.streams
    SparkSession.table
    SparkSession.udf
    SparkSession.version


Configuration
-------------

.. currentmodule:: pyspark.sql.conf

.. autosummary::
    :toctree: api/

    RuntimeConfig


Input and Output
----------------

.. currentmodule:: pyspark.sql

.. autosummary::
    :toctree: api/

    DataFrameReader.csv
    DataFrameReader.format
    DataFrameReader.jdbc
    DataFrameReader.json
    DataFrameReader.load
    DataFrameReader.option
    DataFrameReader.options
    DataFrameReader.orc
    DataFrameReader.parquet
    DataFrameReader.schema
    DataFrameReader.table
    DataFrameWriter.bucketBy
    DataFrameWriter.csv
    DataFrameWriter.format
    DataFrameWriter.insertInto
    DataFrameWriter.jdbc
    DataFrameWriter.json
    DataFrameWriter.mode
    DataFrameWriter.option
    DataFrameWriter.options
    DataFrameWriter.orc
    DataFrameWriter.parquet
    DataFrameWriter.partitionBy
    DataFrameWriter.save
    DataFrameWriter.saveAsTable
    DataFrameWriter.sortBy
    DataFrameWriter.text


DataFrame APIs
--------------

.. currentmodule:: pyspark.sql

.. autosummary::
    :toctree: api/

    DataFrame.agg
    DataFrame.alias
    DataFrame.approxQuantile
    DataFrame.cache
    DataFrame.checkpoint
    DataFrame.coalesce
    DataFrame.colRegex
    DataFrame.collect
    DataFrame.columns
    DataFrame.corr
    DataFrame.count
    DataFrame.cov
    DataFrame.createGlobalTempView
    DataFrame.createOrReplaceGlobalTempView
    DataFrame.createOrReplaceTempView
    DataFrame.createTempView
    DataFrame.crossJoin
    DataFrame.crosstab
    DataFrame.cube
    DataFrame.describe
    DataFrame.distinct
    DataFrame.drop
    DataFrame.dropDuplicates
    DataFrame.drop_duplicates
    DataFrame.dropna
    DataFrame.dtypes
    DataFrame.exceptAll
    DataFrame.explain
    DataFrame.fillna
    DataFrame.filter
    DataFrame.first
    DataFrame.foreach
    DataFrame.foreachPartition
    DataFrame.freqItems
    DataFrame.groupBy
    DataFrame.head
    DataFrame.hint
    DataFrame.inputFiles
    DataFrame.intersect
    DataFrame.intersectAll
    DataFrame.isEmpty
    DataFrame.isLocal
    DataFrame.isStreaming
    DataFrame.join
    DataFrame.limit
    DataFrame.localCheckpoint
    DataFrame.mapInPandas
    DataFrame.mapInArrow
    DataFrame.na
    DataFrame.observe
    DataFrame.orderBy
    DataFrame.persist
    DataFrame.printSchema
    DataFrame.randomSplit
    DataFrame.rdd
    DataFrame.registerTempTable
    DataFrame.repartition
    DataFrame.repartitionByRange
    DataFrame.replace
    DataFrame.rollup
    DataFrame.sameSemantics
    DataFrame.sample
    DataFrame.sampleBy
    DataFrame.schema
    DataFrame.select
    DataFrame.selectExpr
    DataFrame.semanticHash
    DataFrame.show
    DataFrame.sort
    DataFrame.sortWithinPartitions
    DataFrame.stat
    DataFrame.storageLevel
    DataFrame.subtract
    DataFrame.summary
    DataFrame.tail
    DataFrame.take
    DataFrame.toDF
    DataFrame.toJSON
    DataFrame.toLocalIterator
    DataFrame.toPandas
    DataFrame.transform
    DataFrame.union
    DataFrame.unionAll
    DataFrame.unionByName
    DataFrame.unpersist
    DataFrame.where
    DataFrame.withColumn
    DataFrame.withColumnRenamed
    DataFrame.withWatermark
    DataFrame.write
    DataFrame.writeStream
    DataFrame.writeTo
    DataFrame.to_pandas_on_spark
    DataFrameNaFunctions.drop
    DataFrameNaFunctions.fill
    DataFrameNaFunctions.replace
    DataFrameStatFunctions.approxQuantile
    DataFrameStatFunctions.corr
    DataFrameStatFunctions.cov
    DataFrameStatFunctions.crosstab
    DataFrameStatFunctions.freqItems
    DataFrameStatFunctions.sampleBy

Column APIs
-----------

.. currentmodule:: pyspark.sql

.. autosummary::
    :toctree: api/

    Column.alias
    Column.asc
    Column.asc_nulls_first
    Column.asc_nulls_last
    Column.astype
    Column.between
    Column.bitwiseAND
    Column.bitwiseOR
    Column.bitwiseXOR
    Column.cast
    Column.contains
    Column.desc
    Column.desc_nulls_first
    Column.desc_nulls_last
    Column.dropFields
    Column.endswith
    Column.eqNullSafe
    Column.getField
    Column.getItem
    Column.ilike
    Column.isNotNull
    Column.isNull
    Column.isin
    Column.like
    Column.name
    Column.otherwise
    Column.over
    Column.rlike
    Column.startswith
    Column.substr
    Column.when
    Column.withField

Data Types
----------

.. currentmodule:: pyspark.sql.types

.. autosummary::
    :template: autosummary/class_with_docs.rst
    :toctree: api/

    ArrayType
    BinaryType
    BooleanType
    ByteType
    DataType
    DateType
    DecimalType
    DoubleType
    FloatType
    IntegerType
    LongType
    MapType
    NullType
    ShortType
    StringType
    StructField
    StructType
    TimestampNTZType
    TimestampType
    DayTimeIntervalType


Observation
-----------

.. currentmodule:: pyspark.sql

.. autosummary::
    :toctree: api/

    Observation.get


Row
---

.. currentmodule:: pyspark.sql

.. autosummary::
    :toctree: api/

    Row.asDict


Functions
---------

.. currentmodule:: pyspark.sql.functions

.. autosummary::
    :toctree: api/

    abs
    acos
    acosh
    add_months
    aggregate
    approxCountDistinct
    approx_count_distinct
    array
    array_contains
    array_distinct
    array_except
    array_intersect
    array_join
    array_max
    array_min
    array_position
    array_remove
    array_repeat
    array_sort
    array_union
    arrays_overlap
    arrays_zip
    asc
    asc_nulls_first
    asc_nulls_last
    ascii
    asin
    asinh
    assert_true
    atan
    atanh
    atan2
    avg
    base64
    bin
    bit_length
    bitwise_not
    bitwiseNOT
    broadcast
    bround
    bucket
    cbrt
    ceil
    coalesce
    col
    collect_list
    collect_set
    column
    concat
    concat_ws
    conv
    corr
    cos
    cosh
    cot
    count
    count_distinct
    countDistinct
    covar_pop
    covar_samp
    crc32
    create_map
    csc
    cume_dist
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
    days
    decode
    degrees
    dense_rank
    desc
    desc_nulls_first
    desc_nulls_last
    element_at
    encode
    exists
    exp
    explode
    explode_outer
    expm1
    expr
    factorial
    filter
    first
    flatten
    floor
    forall
    format_number
    format_string
    from_csv
    from_json
    from_unixtime
    from_utc_timestamp
    get_json_object
    greatest
    grouping
    grouping_id
    hash
    hex
    hour
    hours
    hypot
    initcap
    input_file_name
    instr
    isnan
    isnull
    json_tuple
    kurtosis
    lag
    last
    last_day
    lead
    least
    length
    levenshtein
    lit
    locate
    log
    log10
    log1p
    log2
    lower
    lpad
    ltrim
    make_date
    map_concat
    map_entries
    map_filter
    map_from_arrays
    map_from_entries
    map_keys
    map_values
    map_zip_with
    max
    max_by
    md5
    mean
    min
    min_by
    minute
    monotonically_increasing_id
    month
    months
    months_between
    nanvl
    next_day
    nth_value
    ntile
    octet_length
    overlay
    pandas_udf
    percent_rank
    percentile_approx
    posexplode
    posexplode_outer
    pow
    product
    quarter
    radians
    raise_error
    rand
    randn
    rank
    regexp_extract
    regexp_replace
    repeat
    reverse
    rint
    round
    row_number
    rpad
    rtrim
    schema_of_csv
    schema_of_json
    sec
    second
    sentences
    sequence
    session_window
    sha1
    sha2
    shiftleft
    shiftright
    shiftrightunsigned
    shuffle
    signum
    sin
    sinh
    size
    skewness
    slice
    sort_array
    soundex
    spark_partition_id
    split
    sqrt
    stddev
    stddev_pop
    stddev_samp
    struct
    substring
    substring_index
    sum
    sum_distinct
    sumDistinct
    tan
    tanh
    timestamp_seconds
    toDegrees
    toRadians
    to_csv
    to_date
    to_json
    to_timestamp
    to_utc_timestamp
    transform
    transform_keys
    transform_values
    translate
    trim
    trunc
    udf
    unbase64
    unhex
    unix_timestamp
    upper
    var_pop
    var_samp
    variance
    weekofyear
    when
    window
    xxhash64
    year
    years
    zip_with


.. currentmodule:: pyspark.sql.avro.functions

.. autosummary::
    :toctree: api/

    from_avro
    to_avro

Window
------

.. currentmodule:: pyspark.sql

.. autosummary::
    :toctree: api/

    Window.currentRow
    Window.orderBy
    Window.partitionBy
    Window.rangeBetween
    Window.rowsBetween
    Window.unboundedFollowing
    Window.unboundedPreceding
    WindowSpec.orderBy
    WindowSpec.partitionBy
    WindowSpec.rangeBetween
    WindowSpec.rowsBetween

Grouping
--------

.. currentmodule:: pyspark.sql

.. autosummary::
    :toctree: api/

    GroupedData.agg
    GroupedData.apply
    GroupedData.applyInPandas
    GroupedData.avg
    GroupedData.cogroup
    GroupedData.count
    GroupedData.max
    GroupedData.mean
    GroupedData.min
    GroupedData.pivot
    GroupedData.sum
    PandasCogroupedOps.applyInPandas

Catalog APIs
------------

.. currentmodule:: pyspark.sql

.. autosummary::
    :toctree: api/

    Catalog.cacheTable
    Catalog.clearCache
    Catalog.createExternalTable
    Catalog.createTable
    Catalog.currentDatabase
    Catalog.databaseExists
    Catalog.dropGlobalTempView
    Catalog.dropTempView
    Catalog.functionExists
    Catalog.isCached
    Catalog.listColumns
    Catalog.listDatabases
    Catalog.listFunctions
    Catalog.listTables
    Catalog.recoverPartitions
    Catalog.refreshByPath
    Catalog.refreshTable
    Catalog.registerFunction
    Catalog.setCurrentDatabase
    Catalog.tableExists
    Catalog.uncacheTable
