/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql

import java.util.Properties

import scala.collection.immutable
import scala.collection.JavaConversions._

import org.apache.spark.sql.catalyst.CatalystConf

private[spark] object SQLConf {

  private val sqlConfEntries = java.util.Collections.synchronizedMap(
    new java.util.HashMap[String, SQLConfEntry[_]]())

  /**
   * An entry contains all meta information for a configuration.
   *
   * @param key the key for the configuration
   * @param defaultValue the default value for the configuration
   * @param valueConverter how to convert a string to the value. It should throw an exception if the
   *                       string does not have the required format.
   * @param stringConverter how to convert a value to a string that the user can use it as a valid
   *                        string value. It's usually `toString`. But sometimes, a custom converter
   *                        is necessary. E.g., if T is List[String], `a, b, c` is better than
   *                        `List(a, b, c)`.
   * @param doc the document for the configuration
   * @param isPublic if this configuration is public to the user. If it's `false`, this
   *                 configuration is only used internally and we should not expose it to the user.
   * @tparam T the value type
   */
  private[sql] class SQLConfEntry[T] private(
      val key: String,
      val defaultValue: Option[T],
      val valueConverter: String => T,
      val stringConverter: T => String,
      val doc: String,
      val isPublic: Boolean) {

    def defaultValueString: String = defaultValue.map(stringConverter).getOrElse("<undefined>")

    override def toString: String = {
      s"SQLConfEntry(key = $key, defaultValue=$defaultValueString, doc=$doc, isPublic = $isPublic)"
    }
  }

  private[sql] object SQLConfEntry {

    private def apply[T](
          key: String,
          defaultValue: Option[T],
          valueConverter: String => T,
          stringConverter: T => String,
          doc: String,
          isPublic: Boolean): SQLConfEntry[T] =
      sqlConfEntries.synchronized {
        if (sqlConfEntries.containsKey(key)) {
          throw new IllegalArgumentException(s"Duplicate SQLConfEntry. $key has been registered")
        }
        val entry =
          new SQLConfEntry[T](key, defaultValue, valueConverter, stringConverter, doc, isPublic)
        sqlConfEntries.put(key, entry)
        entry
      }

    def intConf(
          key: String,
          defaultValue: Option[Int] = None,
          doc: String = "",
          isPublic: Boolean = true): SQLConfEntry[Int] =
      SQLConfEntry(key, defaultValue, { v =>
        try {
          v.toInt
        } catch {
          case _: NumberFormatException =>
            throw new IllegalArgumentException(s"$key should be int, but was $v")
        }
      }, _.toString, doc, isPublic)

    def longConf(
        key: String,
        defaultValue: Option[Long] = None,
        doc: String = "",
        isPublic: Boolean = true): SQLConfEntry[Long] =
      SQLConfEntry(key, defaultValue, { v =>
        try {
          v.toLong
        } catch {
          case _: NumberFormatException =>
            throw new IllegalArgumentException(s"$key should be long, but was $v")
        }
      }, _.toString, doc, isPublic)

    def doubleConf(
        key: String,
        defaultValue: Option[Double] = None,
        doc: String = "",
        isPublic: Boolean = true): SQLConfEntry[Double] =
      SQLConfEntry(key, defaultValue, { v =>
        try {
          v.toDouble
        } catch {
          case _: NumberFormatException =>
            throw new IllegalArgumentException(s"$key should be double, but was $v")
        }
      }, _.toString, doc, isPublic)

    def booleanConf(
        key: String,
        defaultValue: Option[Boolean] = None,
        doc: String = "",
        isPublic: Boolean = true): SQLConfEntry[Boolean] =
      SQLConfEntry(key, defaultValue, { v =>
        try {
          v.toBoolean
        } catch {
          case _: IllegalArgumentException =>
            throw new IllegalArgumentException(s"$key should be boolean, but was $v")
        }
      }, _.toString, doc, isPublic)

    def stringConf(
        key: String,
        defaultValue: Option[String] = None,
        doc: String = "",
        isPublic: Boolean = true): SQLConfEntry[String] =
      SQLConfEntry(key, defaultValue, v => v, v => v, doc, isPublic)

    def enumConf[T](
        key: String,
        valueConverter: String => T,
        validValues: Set[T],
        defaultValue: Option[T] = None,
        doc: String = "",
        isPublic: Boolean = true): SQLConfEntry[T] =
      SQLConfEntry(key, defaultValue, v => {
        if (!validValues.contains(v)) {
          throw new IllegalArgumentException(
            s"The value of $key should be one of ${validValues.mkString(", ")}, but was $v")
        }
        valueConverter(v)
      }, _.toString, doc, isPublic)
  }

  import SQLConfEntry._

  val COMPRESS_CACHED = booleanConf("spark.sql.inMemoryColumnarStorage.compressed",
    defaultValue = Some(true),
    doc = "When set to true Spark SQL will automatically select a compression codec for each " +
      "column based on statistics of the data.")

  val COLUMN_BATCH_SIZE = intConf("spark.sql.inMemoryColumnarStorage.batchSize",
    defaultValue = Some(10000),
    doc = "Controls the size of batches for columnar caching.  Larger batch sizes can improve " +
      "memory utilization and compression, but risk OOMs when caching data.")

  val IN_MEMORY_PARTITION_PRUNING =
    booleanConf("spark.sql.inMemoryColumnarStorage.partitionPruning",
      defaultValue = Some(false),
      doc = "<TODO>")

  val AUTO_BROADCASTJOIN_THRESHOLD = intConf("spark.sql.autoBroadcastJoinThreshold",
    defaultValue = Some(10 * 1024 * 1024),
    doc = "Configures the maximum size in bytes for a table that will be broadcast to all worker " +
      "nodes when performing a join.  By setting this value to -1 broadcasting can be disabled. " +
      "Note that currently statistics are only supported for Hive Metastore tables where the " +
      "command<code>ANALYZE TABLE &lt;tableName&gt; COMPUTE STATISTICS noscan</code> has been run.")

  val DEFAULT_SIZE_IN_BYTES = longConf("spark.sql.defaultSizeInBytes", isPublic = false)

  val SHUFFLE_PARTITIONS = intConf("spark.sql.shuffle.partitions",
    defaultValue = Some(200),
    doc = "Configures the number of partitions to use when shuffling data for joins or " +
      "aggregations.")

  val CODEGEN_ENABLED = booleanConf("spark.sql.codegen",
    defaultValue = Some(false),
    doc = "When true, code will be dynamically generated at runtime for expression evaluation in" +
      " a specific query. For some queries with complicated expression this option can lead to " +
      "significant speed-ups. However, for simple queries this can actually slow down query " +
      "execution.")

  val UNSAFE_ENABLED = booleanConf("spark.sql.unsafe.enabled",
    defaultValue = Some(false),
    doc = "<TDDO>")

  val DIALECT = stringConf("spark.sql.dialect", defaultValue = Some("sql"), doc = "<TODO>")

  val CASE_SENSITIVE = booleanConf("spark.sql.caseSensitive",
    defaultValue = Some(true),
    doc = "<TODO>")

  val PARQUET_BINARY_AS_STRING = booleanConf("spark.sql.parquet.binaryAsString",
    defaultValue = Some(false),
    doc = "Some other Parquet-producing systems, in particular Impala and older versions of " +
      "Spark SQL, do not differentiate between binary data and strings when writing out the " +
      "Parquet schema. This flag tells Spark SQL to interpret binary data as a string to provide " +
      "compatibility with these systems.")

  val PARQUET_INT96_AS_TIMESTAMP = booleanConf("spark.sql.parquet.int96AsTimestamp",
    defaultValue = Some(true),
    doc = "Some Parquet-producing systems, in particular Impala, store Timestamp into INT96. " +
      "Spark would also store Timestamp as INT96 because we need to avoid precision lost of the " +
      "nanoseconds field. This flag tells Spark SQL to interpret INT96 data as a timestamp to " +
      "provide compatibility with these systems.")

  val PARQUET_CACHE_METADATA = booleanConf("spark.sql.parquet.cacheMetadata",
    defaultValue = Some(true),
    doc = "Turns on caching of Parquet schema metadata. Can speed up querying of static data.")

  val PARQUET_COMPRESSION = enumConf("spark.sql.parquet.compression.codec",
    valueConverter = v => v,
    validValues = Set("uncompressed", "snappy", "gzip", "lzo"),
    defaultValue = Some("gzip"),
    doc = "Sets the compression codec use when writing Parquet files. Acceptable values include: " +
      "uncompressed, snappy, gzip, lzo.")

  val PARQUET_FILTER_PUSHDOWN_ENABLED = booleanConf("spark.sql.parquet.filterPushdown",
    defaultValue = Some(false),
    doc = "Turn on Parquet filter pushdown optimization. This feature is turned off by default" +
      " because of a known bug in Paruet 1.6.0rc3 " +
      "(<a href=\"https://issues.apache.org/jira/browse/PARQUET-136\">PARQUET-136</a>). However, " +
      "if your table doesn't contain any nullable string or binary columns, it's still safe to " +
      "turn this feature on.")

  val PARQUET_USE_DATA_SOURCE_API = booleanConf("spark.sql.parquet.useDataSourceApi",
    defaultValue = Some(true),
    doc = "<TODO>")

  val ORC_FILTER_PUSHDOWN_ENABLED = booleanConf("spark.sql.orc.filterPushdown",
    defaultValue = Some(false),
    doc = "<TODO>")

  val HIVE_VERIFY_PARTITIONPATH = booleanConf("spark.sql.hive.verifyPartitionPath",
    defaultValue = Some(true),
    doc = "<TODO>")

  val COLUMN_NAME_OF_CORRUPT_RECORD = stringConf("spark.sql.columnNameOfCorruptRecord",
    defaultValue = Some("_corrupt_record"),
    doc = "<TODO>")

  val BROADCAST_TIMEOUT = intConf("spark.sql.broadcastTimeout",
    defaultValue = Some(5 * 60),
    doc = "<TODO>")

  // Options that control which operators can be chosen by the query planner.  These should be
  // considered hints and may be ignored by future versions of Spark SQL.
  val EXTERNAL_SORT = booleanConf("spark.sql.planner.externalSort",
    defaultValue = Some(true),
    doc = "When true, performs sorts spilling to disk as needed otherwise sort each partition in" +
      " memory.")

  val SORTMERGE_JOIN = booleanConf("spark.sql.planner.sortMergeJoin",
    defaultValue = Some(false),
    doc = "<TODO>")

  // This is only used for the thriftserver
  val THRIFTSERVER_POOL = stringConf("spark.sql.thriftserver.scheduler.pool",
    doc = "Set a Fair Scheduler pool for a JDBC client session")

  val THRIFTSERVER_UI_STATEMENT_LIMIT = intConf("spark.sql.thriftserver.ui.retainedStatements",
    defaultValue = Some(200),
    doc = "<TODO>")

  val THRIFTSERVER_UI_SESSION_LIMIT = intConf("spark.sql.thriftserver.ui.retainedSessions",
    defaultValue = Some(200),
    doc = "<TODO>")

  // This is used to set the default data source
  val DEFAULT_DATA_SOURCE_NAME = stringConf("spark.sql.sources.default",
    defaultValue = Some("org.apache.spark.sql.parquet"),
    doc = "<TODO>")

  // This is used to control the when we will split a schema's JSON string to multiple pieces
  // in order to fit the JSON string in metastore's table property (by default, the value has
  // a length restriction of 4000 characters). We will split the JSON string of a schema
  // to its length exceeds the threshold.
  val SCHEMA_STRING_LENGTH_THRESHOLD = intConf("spark.sql.sources.schemaStringLengthThreshold",
    defaultValue = Some(4000),
    doc = "<TODO>")

  // Whether to perform partition discovery when loading external data sources.  Default to true.
  val PARTITION_DISCOVERY_ENABLED = booleanConf("spark.sql.sources.partitionDiscovery.enabled",
    defaultValue = Some(true),
    doc = "<TODO>")

  // Whether to perform partition column type inference. Default to true.
  val PARTITION_COLUMN_TYPE_INFERENCE =
    booleanConf("spark.sql.sources.partitionColumnTypeInference.enabled",
      defaultValue = Some(true),
      doc = "<TODO>")

  // The output committer class used by FSBasedRelation. The specified class needs to be a
  // subclass of org.apache.hadoop.mapreduce.OutputCommitter.
  // NOTE: This property should be set in Hadoop `Configuration` rather than Spark `SQLConf`
  val OUTPUT_COMMITTER_CLASS =
    stringConf("spark.sql.sources.outputCommitterClass", isPublic = false)

  // Whether to perform eager analysis when constructing a dataframe.
  // Set to false when debugging requires the ability to look at invalid query plans.
  val DATAFRAME_EAGER_ANALYSIS = booleanConf("spark.sql.eagerAnalysis",
    defaultValue = Some(true),
    doc = "<TODO>")

  // Whether to automatically resolve ambiguity in join conditions for self-joins.
  // See SPARK-6231.
  val DATAFRAME_SELF_JOIN_AUTO_RESOLVE_AMBIGUITY =
    booleanConf("spark.sql.selfJoinAutoResolveAmbiguity", defaultValue = Some(true), doc = "<TODO>")

  // Whether to retain group by columns or not in GroupedData.agg.
  val DATAFRAME_RETAIN_GROUP_COLUMNS = booleanConf("spark.sql.retainGroupColumns",
    defaultValue = Some(true),
    doc = "<TODO>")

  val USE_SQL_SERIALIZER2 = booleanConf("spark.sql.useSerializer2",
    defaultValue = Some(true), doc = "<TODO>")

  val USE_JACKSON_STREAMING_API = booleanConf("spark.sql.json.useJacksonStreamingAPI",
    defaultValue = Some(true), doc = "<TODO>")

  object Deprecated {
    val MAPRED_REDUCE_TASKS = "mapred.reduce.tasks"
  }
}

/**
 * A class that enables the setting and getting of mutable config parameters/hints.
 *
 * In the presence of a SQLContext, these can be set and queried by passing SET commands
 * into Spark SQL's query functions (i.e. sql()). Otherwise, users of this class can
 * modify the hints by programmatically calling the setters and getters of this class.
 *
 * SQLConf is thread-safe (internally synchronized, so safe to be used in multiple threads).
 */

private[sql] class SQLConf extends Serializable with CatalystConf {
  import SQLConf._

  /** Only low degree of contention is expected for conf, thus NOT using ConcurrentHashMap. */
  @transient protected[spark] val settings = java.util.Collections.synchronizedMap(
    new java.util.HashMap[String, String]())

  /** ************************ Spark SQL Params/Hints ******************* */
  // TODO: refactor so that these hints accessors don't pollute the name space of SQLContext?

  /**
   * The SQL dialect that is used when parsing queries.  This defaults to 'sql' which uses
   * a simple SQL parser provided by Spark SQL.  This is currently the only option for users of
   * SQLContext.
   *
   * When using a HiveContext, this value defaults to 'hiveql', which uses the Hive 0.12.0 HiveQL
   * parser.  Users can change this to 'sql' if they want to run queries that aren't supported by
   * HiveQL (e.g., SELECT 1).
   *
   * Note that the choice of dialect does not affect things like what tables are available or
   * how query execution is performed.
   */
  private[spark] def dialect: String = getConf(DIALECT)

  /** When true tables cached using the in-memory columnar caching will be compressed. */
  private[spark] def useCompression: Boolean = getConf(COMPRESS_CACHED)

  /** The compression codec for writing to a Parquetfile */
  private[spark] def parquetCompressionCodec: String = getConf(PARQUET_COMPRESSION)

  private[spark] def parquetCacheMetadata: Boolean = getConf(PARQUET_CACHE_METADATA)

  /** The number of rows that will be  */
  private[spark] def columnBatchSize: Int = getConf(COLUMN_BATCH_SIZE)

  /** Number of partitions to use for shuffle operators. */
  private[spark] def numShufflePartitions: Int = getConf(SHUFFLE_PARTITIONS)

  /** When true predicates will be passed to the parquet record reader when possible. */
  private[spark] def parquetFilterPushDown: Boolean = getConf(PARQUET_FILTER_PUSHDOWN_ENABLED)

  /** When true uses Parquet implementation based on data source API */
  private[spark] def parquetUseDataSourceApi: Boolean = getConf(PARQUET_USE_DATA_SOURCE_API)

  private[spark] def orcFilterPushDown: Boolean = getConf(ORC_FILTER_PUSHDOWN_ENABLED)

  /** When true uses verifyPartitionPath to prune the path which is not exists. */
  private[spark] def verifyPartitionPath: Boolean = getConf(HIVE_VERIFY_PARTITIONPATH)

  /** When true the planner will use the external sort, which may spill to disk. */
  private[spark] def externalSortEnabled: Boolean = getConf(EXTERNAL_SORT)

  /**
   * Sort merge join would sort the two side of join first, and then iterate both sides together
   * only once to get all matches. Using sort merge join can save a lot of memory usage compared
   * to HashJoin.
   */
  private[spark] def sortMergeJoinEnabled: Boolean = getConf(SORTMERGE_JOIN)

  /**
   * When set to true, Spark SQL will use the Scala compiler at runtime to generate custom bytecode
   * that evaluates expressions found in queries.  In general this custom code runs much faster
   * than interpreted evaluation, but there are significant start-up costs due to compilation.
   * As a result codegen is only beneficial when queries run for a long time, or when the same
   * expressions are used multiple times.
   *
   * Defaults to false as this feature is currently experimental.
   */
  private[spark] def codegenEnabled: Boolean = getConf(CODEGEN_ENABLED)

  /**
   * caseSensitive analysis true by default
   */
  def caseSensitiveAnalysis: Boolean = getConf(SQLConf.CASE_SENSITIVE)

  /**
   * When set to true, Spark SQL will use managed memory for certain operations.  This option only
   * takes effect if codegen is enabled.
   *
   * Defaults to false as this feature is currently experimental.
   */
  private[spark] def unsafeEnabled: Boolean = getConf(UNSAFE_ENABLED)

  private[spark] def useSqlSerializer2: Boolean = getConf(USE_SQL_SERIALIZER2)

  /**
   * Selects between the new (true) and old (false) JSON handlers, to be removed in Spark 1.5.0
   */
  private[spark] def useJacksonStreamingAPI: Boolean = getConf(USE_JACKSON_STREAMING_API)

  /**
   * Upper bound on the sizes (in bytes) of the tables qualified for the auto conversion to
   * a broadcast value during the physical executions of join operations.  Setting this to -1
   * effectively disables auto conversion.
   *
   * Hive setting: hive.auto.convert.join.noconditionaltask.size, whose default value is 10000.
   */
  private[spark] def autoBroadcastJoinThreshold: Int = getConf(AUTO_BROADCASTJOIN_THRESHOLD)

  /**
   * The default size in bytes to assign to a logical operator's estimation statistics.  By default,
   * it is set to a larger value than `autoBroadcastJoinThreshold`, hence any logical operator
   * without a properly implemented estimation of this statistic will not be incorrectly broadcasted
   * in joins.
   */
  private[spark] def defaultSizeInBytes: Long =
    getConf(DEFAULT_SIZE_IN_BYTES, autoBroadcastJoinThreshold + 1L)

  /**
   * When set to true, we always treat byte arrays in Parquet files as strings.
   */
  private[spark] def isParquetBinaryAsString: Boolean = getConf(PARQUET_BINARY_AS_STRING)

  /**
   * When set to true, we always treat INT96Values in Parquet files as timestamp.
   */
  private[spark] def isParquetINT96AsTimestamp: Boolean = getConf(PARQUET_INT96_AS_TIMESTAMP)

  /**
   * When set to true, partition pruning for in-memory columnar tables is enabled.
   */
  private[spark] def inMemoryPartitionPruning: Boolean = getConf(IN_MEMORY_PARTITION_PRUNING)

  private[spark] def columnNameOfCorruptRecord: String = getConf(COLUMN_NAME_OF_CORRUPT_RECORD)

  /**
   * Timeout in seconds for the broadcast wait time in hash join
   */
  private[spark] def broadcastTimeout: Int = getConf(BROADCAST_TIMEOUT)

  private[spark] def defaultDataSourceName: String = getConf(DEFAULT_DATA_SOURCE_NAME)

  private[spark] def partitionDiscoveryEnabled(): Boolean =
    getConf(SQLConf.PARTITION_DISCOVERY_ENABLED)

  private[spark] def partitionColumnTypeInferenceEnabled(): Boolean =
    getConf(SQLConf.PARTITION_COLUMN_TYPE_INFERENCE)

  // Do not use a value larger than 4000 as the default value of this property.
  // See the comments of SCHEMA_STRING_LENGTH_THRESHOLD above for more information.
  private[spark] def schemaStringLengthThreshold: Int = getConf(SCHEMA_STRING_LENGTH_THRESHOLD)

  private[spark] def dataFrameEagerAnalysis: Boolean = getConf(DATAFRAME_EAGER_ANALYSIS)

  private[spark] def dataFrameSelfJoinAutoResolveAmbiguity: Boolean =
    getConf(DATAFRAME_SELF_JOIN_AUTO_RESOLVE_AMBIGUITY)

  private[spark] def dataFrameRetainGroupColumns: Boolean = getConf(DATAFRAME_RETAIN_GROUP_COLUMNS)

  /** ********************** SQLConf functionality methods ************ */

  /** Set Spark SQL configuration properties. */
  def setConf(props: Properties): Unit = settings.synchronized {
    props.foreach { case (k, v) => setConfString(k, v) }
  }

  /** Set the given Spark SQL configuration property using a `string` value. */
  def setConfString(key: String, value: String): Unit = {
    require(key != null, "key cannot be null")
    require(value != null, s"value cannot be null for key: $key")
    val entry = sqlConfEntries.get(key)
    if (entry != null) {
      // Only verify configs in the SQLConf object
      entry.valueConverter(value)
    }
    settings.put(key, value)
  }

  /** Set the given Spark SQL configuration property. */
  def setConf[T](entry: SQLConfEntry[T], value: T): Unit = {
    require(entry != null, "entry cannot be null")
    require(value != null, s"value cannot be null for key: ${entry.key}")
    require(sqlConfEntries.get(entry.key) == entry, s"$entry is not registered")
    settings.put(entry.key, entry.stringConverter(value))
  }

  /** Return the value of Spark SQL configuration property for the given key. */
  def getConfString(key: String): String = {
    Option(settings.get(key)).getOrElse(throw new NoSuchElementException(key))
  }

  /**
   * Return the value of Spark SQL configuration property for the given key. If the key is not set
   * yet, return `defaultValue`. This is useful when `defaultValue` in SQLConfEntry is not the
   * desired one.
   */
  def getConf[T](entry: SQLConfEntry[T], defaultValue: T): T = {
    require(sqlConfEntries.get(entry.key) == entry, s"$entry is not registered")
    Option(settings.get(entry.key)).map(entry.valueConverter).getOrElse(defaultValue)
  }

  /**
   * Return the value of Spark SQL configuration property for the given key. If the key is not set
   * yet, return `defaultValue` in [[SQLConfEntry]].
   */
  def getConf[T](entry: SQLConfEntry[T]): T = {
    require(sqlConfEntries.get(entry.key) == entry, s"$entry is not registered")
    Option(settings.get(entry.key)).map(entry.valueConverter).orElse(entry.defaultValue).
      getOrElse(throw new NoSuchElementException(entry.key))
  }

  /**
   * Return the `string` value of Spark SQL configuration property for the given key. If the key is
   * not set yet, return `defaultValue`.
   */
  def getConfString(key: String, defaultValue: String): String = {
    val entry = sqlConfEntries.get(key)
    if (entry != null && defaultValue != "<undefined>") {
      // Only verify configs in the SQLConf object
      entry.valueConverter(defaultValue)
    }
    Option(settings.get(key)).getOrElse(defaultValue)
  }

  /**
   * Return all the configuration properties that have been set (i.e. not the default).
   * This creates a new copy of the config properties in the form of a Map.
   */
  def getAllConfs: immutable.Map[String, String] = settings.synchronized { settings.toMap }

  /**
   * Return all the configuration definitions that have been defined in [[SQLConf]]. Each
   * definition contains key, defaultValue and doc.
   */
  def getAllDefinedConfs: Seq[(String, String, String)] = sqlConfEntries.synchronized {
    sqlConfEntries.values.filter(_.isPublic).map { entry =>
      (entry.key, entry.defaultValueString, entry.doc)
    }.toSeq
  }

  private[spark] def unsetConf(key: String) {
    settings -= key
  }

  private[spark] def clear() {
    settings.clear()
  }
}

