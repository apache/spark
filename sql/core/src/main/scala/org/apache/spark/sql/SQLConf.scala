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

import org.apache.parquet.hadoop.ParquetOutputCommitter

import org.apache.spark.sql.catalyst.CatalystConf

////////////////////////////////////////////////////////////////////////////////////////////////////
// This file defines the configuration options for Spark SQL.
////////////////////////////////////////////////////////////////////////////////////////////////////


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
        val _v = valueConverter(v)
        if (!validValues.contains(_v)) {
          throw new IllegalArgumentException(
            s"The value of $key should be one of ${validValues.mkString(", ")}, but was $v")
        }
        _v
      }, _.toString, doc, isPublic)

    def seqConf[T](
        key: String,
        valueConverter: String => T,
        defaultValue: Option[Seq[T]] = None,
        doc: String = "",
        isPublic: Boolean = true): SQLConfEntry[Seq[T]] = {
      SQLConfEntry(
        key, defaultValue, _.split(",").map(valueConverter), _.mkString(","), doc, isPublic)
    }

    def stringSeqConf(
        key: String,
        defaultValue: Option[Seq[String]] = None,
        doc: String = "",
        isPublic: Boolean = true): SQLConfEntry[Seq[String]] = {
      seqConf(key, s => s, defaultValue, doc, isPublic)
    }
  }

  import SQLConfEntry._

  val ALLOW_MULTIPLE_CONTEXTS = booleanConf("spark.sql.allowMultipleContexts",
    defaultValue = Some(true),
    doc = "When set to true, creating multiple SQLContexts/HiveContexts is allowed." +
      "When set to false, only one SQLContext/HiveContext is allowed to be created " +
      "through the constructor. Please note that this conf needs to be set in Spark Conf. Once" +
      "a SQLContext/HiveContext has been created, changing the value of this conf will not" +
      "have effect.",
    isPublic = true)

  val COMPRESS_CACHED = booleanConf("spark.sql.inMemoryColumnarStorage.compressed",
    defaultValue = Some(true),
    doc = "When set to true Spark SQL will automatically select a compression codec for each " +
      "column based on statistics of the data.",
    isPublic = false)

  val COLUMN_BATCH_SIZE = intConf("spark.sql.inMemoryColumnarStorage.batchSize",
    defaultValue = Some(10000),
    doc = "Controls the size of batches for columnar caching.  Larger batch sizes can improve " +
      "memory utilization and compression, but risk OOMs when caching data.",
    isPublic = false)

  val IN_MEMORY_PARTITION_PRUNING =
    booleanConf("spark.sql.inMemoryColumnarStorage.partitionPruning",
      defaultValue = Some(true),
      doc = "When true, enable partition pruning for in-memory columnar tables.",
      isPublic = false)

  val AUTO_BROADCASTJOIN_THRESHOLD = intConf("spark.sql.autoBroadcastJoinThreshold",
    defaultValue = Some(10 * 1024 * 1024),
    doc = "Configures the maximum size in bytes for a table that will be broadcast to all worker " +
      "nodes when performing a join.  By setting this value to -1 broadcasting can be disabled. " +
      "Note that currently statistics are only supported for Hive Metastore tables where the " +
      "command<code>ANALYZE TABLE &lt;tableName&gt; COMPUTE STATISTICS noscan</code> has been run.")

  val DEFAULT_SIZE_IN_BYTES = longConf(
    "spark.sql.defaultSizeInBytes",
    doc = "The default table size used in query planning. By default, it is set to a larger " +
      "value than `spark.sql.autoBroadcastJoinThreshold` to be more conservative. That is to say " +
      "by default the optimizer will not choose to broadcast a table unless it knows for sure its" +
      "size is small enough.",
    isPublic = false)

  val SHUFFLE_PARTITIONS = intConf("spark.sql.shuffle.partitions",
    defaultValue = Some(200),
    doc = "The default number of partitions to use when shuffling data for joins or aggregations.")

  val TUNGSTEN_ENABLED = booleanConf("spark.sql.tungsten.enabled",
    defaultValue = Some(true),
    doc = "When true, use the optimized Tungsten physical execution backend which explicitly " +
          "manages memory and dynamically generates bytecode for expression evaluation.")

  val CODEGEN_ENABLED = booleanConf("spark.sql.codegen",
    defaultValue = Some(true),  // use TUNGSTEN_ENABLED as default
    doc = "When true, code will be dynamically generated at runtime for expression evaluation in" +
      " a specific query.",
    isPublic = false)

  val UNSAFE_ENABLED = booleanConf("spark.sql.unsafe.enabled",
    defaultValue = Some(true),  // use TUNGSTEN_ENABLED as default
    doc = "When true, use the new optimized Tungsten physical execution backend.",
    isPublic = false)

  val DIALECT = stringConf(
    "spark.sql.dialect",
    defaultValue = Some("sql"),
    doc = "The default SQL dialect to use.")

  val CASE_SENSITIVE = booleanConf("spark.sql.caseSensitive",
    defaultValue = Some(true),
    doc = "Whether the query analyzer should be case sensitive or not.")

  val PARQUET_SCHEMA_MERGING_ENABLED = booleanConf("spark.sql.parquet.mergeSchema",
    defaultValue = Some(false),
    doc = "When true, the Parquet data source merges schemas collected from all data files, " +
          "otherwise the schema is picked from the summary file or a random data file " +
          "if no summary file is available.")

  val PARQUET_SCHEMA_RESPECT_SUMMARIES = booleanConf("spark.sql.parquet.respectSummaryFiles",
    defaultValue = Some(false),
    doc = "When true, we make assumption that all part-files of Parquet are consistent with " +
          "summary files and we will ignore them when merging schema. Otherwise, if this is " +
          "false, which is the default, we will merge all part-files. This should be considered " +
          "as expert-only option, and shouldn't be enabled before knowing what it means exactly.")

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
    valueConverter = v => v.toLowerCase,
    validValues = Set("uncompressed", "snappy", "gzip", "lzo"),
    defaultValue = Some("gzip"),
    doc = "Sets the compression codec use when writing Parquet files. Acceptable values include: " +
      "uncompressed, snappy, gzip, lzo.")

  val PARQUET_FILTER_PUSHDOWN_ENABLED = booleanConf("spark.sql.parquet.filterPushdown",
    defaultValue = Some(true),
    doc = "Enables Parquet filter push-down optimization when set to true.")

  val PARQUET_FOLLOW_PARQUET_FORMAT_SPEC = booleanConf(
    key = "spark.sql.parquet.followParquetFormatSpec",
    defaultValue = Some(false),
    doc = "Whether to follow Parquet's format specification when converting Parquet schema to " +
      "Spark SQL schema and vice versa.",
    isPublic = false)

  val PARQUET_OUTPUT_COMMITTER_CLASS = stringConf(
    key = "spark.sql.parquet.output.committer.class",
    defaultValue = Some(classOf[ParquetOutputCommitter].getName),
    doc = "The output committer class used by Parquet. The specified class needs to be a " +
      "subclass of org.apache.hadoop.mapreduce.OutputCommitter.  Typically, it's also a subclass " +
      "of org.apache.parquet.hadoop.ParquetOutputCommitter.  NOTE: 1. Instead of SQLConf, this " +
      "option must be set in Hadoop Configuration.  2. This option overrides " +
      "\"spark.sql.sources.outputCommitterClass\"."
  )

  val ORC_FILTER_PUSHDOWN_ENABLED = booleanConf("spark.sql.orc.filterPushdown",
    defaultValue = Some(false),
    doc = "When true, enable filter pushdown for ORC files.")

  val HIVE_VERIFY_PARTITION_PATH = booleanConf("spark.sql.hive.verifyPartitionPath",
    defaultValue = Some(false),
    doc = "<TODO>")

  val HIVE_METASTORE_PARTITION_PRUNING = booleanConf("spark.sql.hive.metastorePartitionPruning",
    defaultValue = Some(false),
    doc = "When true, some predicates will be pushed down into the Hive metastore so that " +
          "unmatching partitions can be eliminated earlier.")

  val COLUMN_NAME_OF_CORRUPT_RECORD = stringConf("spark.sql.columnNameOfCorruptRecord",
    defaultValue = Some("_corrupt_record"),
    doc = "<TODO>")

  val BROADCAST_TIMEOUT = intConf("spark.sql.broadcastTimeout",
    defaultValue = Some(5 * 60),
    doc = "Timeout in seconds for the broadcast wait time in broadcast joins.")

  // Options that control which operators can be chosen by the query planner.  These should be
  // considered hints and may be ignored by future versions of Spark SQL.
  val EXTERNAL_SORT = booleanConf("spark.sql.planner.externalSort",
    defaultValue = Some(true),
    doc = "When true, performs sorts spilling to disk as needed otherwise sort each partition in" +
      " memory.")

  val SORTMERGE_JOIN = booleanConf("spark.sql.planner.sortMergeJoin",
    defaultValue = Some(true),
    doc = "When true, use sort merge join (as opposed to hash join) by default for large joins.")

  // This is only used for the thriftserver
  val THRIFTSERVER_POOL = stringConf("spark.sql.thriftserver.scheduler.pool",
    doc = "Set a Fair Scheduler pool for a JDBC client session")

  val THRIFTSERVER_UI_STATEMENT_LIMIT = intConf("spark.sql.thriftserver.ui.retainedStatements",
    defaultValue = Some(200),
    doc = "The number of SQL statements kept in the JDBC/ODBC web UI history.")

  val THRIFTSERVER_UI_SESSION_LIMIT = intConf("spark.sql.thriftserver.ui.retainedSessions",
    defaultValue = Some(200),
    doc = "The number of SQL client sessions kept in the JDBC/ODBC web UI history.")

  // This is used to set the default data source
  val DEFAULT_DATA_SOURCE_NAME = stringConf("spark.sql.sources.default",
    defaultValue = Some("org.apache.spark.sql.parquet"),
    doc = "The default data source to use in input/output.")

  // This is used to control the when we will split a schema's JSON string to multiple pieces
  // in order to fit the JSON string in metastore's table property (by default, the value has
  // a length restriction of 4000 characters). We will split the JSON string of a schema
  // to its length exceeds the threshold.
  val SCHEMA_STRING_LENGTH_THRESHOLD = intConf("spark.sql.sources.schemaStringLengthThreshold",
    defaultValue = Some(4000),
    doc = "The maximum length allowed in a single cell when " +
      "storing additional schema information in Hive's metastore.",
    isPublic = false)

  val PARTITION_DISCOVERY_ENABLED = booleanConf("spark.sql.sources.partitionDiscovery.enabled",
    defaultValue = Some(true),
    doc = "When true, automtically discover data partitions.")

  val PARTITION_COLUMN_TYPE_INFERENCE =
    booleanConf("spark.sql.sources.partitionColumnTypeInference.enabled",
      defaultValue = Some(true),
      doc = "When true, automatically infer the data types for partitioned columns.")

  val PARTITION_MAX_FILES =
    intConf("spark.sql.sources.maxConcurrentWrites",
      defaultValue = Some(5),
      doc = "The maximum number of concurent files to open before falling back on sorting when " +
            "writing out files using dynamic partitioning.")

  // The output committer class used by HadoopFsRelation. The specified class needs to be a
  // subclass of org.apache.hadoop.mapreduce.OutputCommitter.
  //
  // NOTE:
  //
  //  1. Instead of SQLConf, this option *must be set in Hadoop Configuration*.
  //  2. This option can be overriden by "spark.sql.parquet.output.committer.class".
  val OUTPUT_COMMITTER_CLASS =
    stringConf("spark.sql.sources.outputCommitterClass", isPublic = false)

  val PARALLEL_PARTITION_DISCOVERY_THRESHOLD = intConf(
    key = "spark.sql.sources.parallelPartitionDiscovery.threshold",
    defaultValue = Some(32),
    doc = "<TODO>")

  // Whether to perform eager analysis when constructing a dataframe.
  // Set to false when debugging requires the ability to look at invalid query plans.
  val DATAFRAME_EAGER_ANALYSIS = booleanConf(
    "spark.sql.eagerAnalysis",
    defaultValue = Some(true),
    doc = "When true, eagerly applies query analysis on DataFrame operations.",
    isPublic = false)

  // Whether to automatically resolve ambiguity in join conditions for self-joins.
  // See SPARK-6231.
  val DATAFRAME_SELF_JOIN_AUTO_RESOLVE_AMBIGUITY = booleanConf(
    "spark.sql.selfJoinAutoResolveAmbiguity",
    defaultValue = Some(true),
    isPublic = false)

  // Whether to retain group by columns or not in GroupedData.agg.
  val DATAFRAME_RETAIN_GROUP_COLUMNS = booleanConf(
    "spark.sql.retainGroupColumns",
    defaultValue = Some(true),
    isPublic = false)

  val USE_SQL_AGGREGATE2 = booleanConf("spark.sql.useAggregate2",
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

  private[spark] def useCompression: Boolean = getConf(COMPRESS_CACHED)

  private[spark] def parquetCompressionCodec: String = getConf(PARQUET_COMPRESSION)

  private[spark] def parquetCacheMetadata: Boolean = getConf(PARQUET_CACHE_METADATA)

  private[spark] def columnBatchSize: Int = getConf(COLUMN_BATCH_SIZE)

  private[spark] def numShufflePartitions: Int = getConf(SHUFFLE_PARTITIONS)

  private[spark] def parquetFilterPushDown: Boolean = getConf(PARQUET_FILTER_PUSHDOWN_ENABLED)

  private[spark] def orcFilterPushDown: Boolean = getConf(ORC_FILTER_PUSHDOWN_ENABLED)

  private[spark] def verifyPartitionPath: Boolean = getConf(HIVE_VERIFY_PARTITION_PATH)

  private[spark] def metastorePartitionPruning: Boolean = getConf(HIVE_METASTORE_PARTITION_PRUNING)

  private[spark] def externalSortEnabled: Boolean = getConf(EXTERNAL_SORT)

  private[spark] def sortMergeJoinEnabled: Boolean = getConf(SORTMERGE_JOIN)

  private[spark] def codegenEnabled: Boolean = getConf(CODEGEN_ENABLED, getConf(TUNGSTEN_ENABLED))

  def caseSensitiveAnalysis: Boolean = getConf(SQLConf.CASE_SENSITIVE)

  private[spark] def unsafeEnabled: Boolean = getConf(UNSAFE_ENABLED, getConf(TUNGSTEN_ENABLED))

  private[spark] def useSqlAggregate2: Boolean = getConf(USE_SQL_AGGREGATE2)

  private[spark] def autoBroadcastJoinThreshold: Int = getConf(AUTO_BROADCASTJOIN_THRESHOLD)

  private[spark] def defaultSizeInBytes: Long =
    getConf(DEFAULT_SIZE_IN_BYTES, autoBroadcastJoinThreshold + 1L)

  private[spark] def isParquetBinaryAsString: Boolean = getConf(PARQUET_BINARY_AS_STRING)

  private[spark] def isParquetINT96AsTimestamp: Boolean = getConf(PARQUET_INT96_AS_TIMESTAMP)

  private[spark] def followParquetFormatSpec: Boolean = getConf(PARQUET_FOLLOW_PARQUET_FORMAT_SPEC)

  private[spark] def inMemoryPartitionPruning: Boolean = getConf(IN_MEMORY_PARTITION_PRUNING)

  private[spark] def columnNameOfCorruptRecord: String = getConf(COLUMN_NAME_OF_CORRUPT_RECORD)

  private[spark] def broadcastTimeout: Int = getConf(BROADCAST_TIMEOUT)

  private[spark] def defaultDataSourceName: String = getConf(DEFAULT_DATA_SOURCE_NAME)

  private[spark] def partitionDiscoveryEnabled(): Boolean =
    getConf(SQLConf.PARTITION_DISCOVERY_ENABLED)

  private[spark] def partitionColumnTypeInferenceEnabled(): Boolean =
    getConf(SQLConf.PARTITION_COLUMN_TYPE_INFERENCE)

  private[spark] def parallelPartitionDiscoveryThreshold: Int =
    getConf(SQLConf.PARALLEL_PARTITION_DISCOVERY_THRESHOLD)

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
    Option(settings.get(key)).
      orElse {
        // Try to use the default value
        Option(sqlConfEntries.get(key)).map(_.defaultValueString)
      }.
      getOrElse(throw new NoSuchElementException(key))
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

  private[spark] def unsetConf(key: String): Unit = {
    settings -= key
  }

  private[spark] def unsetConf(entry: SQLConfEntry[_]): Unit = {
    settings -= entry.key
  }

  private[spark] def clear(): Unit = {
    settings.clear()
  }
}

