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

package org.apache.spark.sql.internal

import java.util.{NoSuchElementException, Properties}

import scala.collection.JavaConverters._
import scala.collection.immutable

import org.apache.parquet.hadoop.ParquetOutputCommitter

import org.apache.spark.Logging
import org.apache.spark.internal.config._
import org.apache.spark.network.util.ByteUnit
import org.apache.spark.sql.catalyst.CatalystConf
import org.apache.spark.sql.catalyst.parser.ParserConf
import org.apache.spark.util.Utils

////////////////////////////////////////////////////////////////////////////////////////////////////
// This file defines the configuration options for Spark SQL.
////////////////////////////////////////////////////////////////////////////////////////////////////


object SQLConf {

  private val sqlConfEntries = java.util.Collections.synchronizedMap(
    new java.util.HashMap[String, ConfigEntry[_]]())

  private def register[T <: ConfigEntry[_]](entry: T): T = sqlConfEntries.synchronized {
    require(!sqlConfEntries.containsKey(entry.key),
      s"Duplicate SQLConfigEntry. ${entry.key} has been registered")
    sqlConfEntries.put(entry.key, entry)
    entry
  }

  private class SQLTypedConfigBuilder[T](parent: TypedConfigBuilder[T])
    extends TypedConfigBuilder(parent.parent, parent.converter, parent.stringConverter) {

    override def transform(fn: T => T): TypedConfigBuilder[T] = {
      new SQLTypedConfigBuilder(super.transform(fn))
    }

    override def toSequence: TypedConfigBuilder[Seq[T]] = {
      new SQLTypedConfigBuilder(super.toSequence)
    }

    override def optional: OptionalConfigEntry[T] = register(super.optional)

    override def withDefault(default: T): ConfigEntry[T] = register(super.withDefault(default))

    override def withDefaultString(default: String): ConfigEntry[T] =
      register(super.withDefaultString(default))

  }

  private[sql] object SQLConfigBuilder {

    def apply(key: String): SQLConfigBuilder = new SQLConfigBuilder(key)

  }

  private[sql] class SQLConfigBuilder(key: String) extends ConfigBuilder(key) {

    override def intConf: TypedConfigBuilder[Int] = {
      new SQLTypedConfigBuilder(super.intConf)
    }

    override def longConf: TypedConfigBuilder[Long] = {
      new SQLTypedConfigBuilder(super.longConf)
    }

    override def booleanConf: TypedConfigBuilder[Boolean] = {
      new SQLTypedConfigBuilder(super.booleanConf)
    }

    override def doubleConf: TypedConfigBuilder[Double] = {
      new SQLTypedConfigBuilder(super.doubleConf)
    }

    override def stringConf: TypedConfigBuilder[String] = {
      new SQLTypedConfigBuilder(super.stringConf)
    }

    override def bytesConf(unit: ByteUnit): TypedConfigBuilder[Long] = {
      new SQLTypedConfigBuilder(super.bytesConf(unit))
    }

  }

  val ALLOW_MULTIPLE_CONTEXTS = SQLConfigBuilder("spark.sql.allowMultipleContexts")
    .doc("When set to true, creating multiple SQLContexts/HiveContexts is allowed." +
      "When set to false, only one SQLContext/HiveContext is allowed to be created " +
      "through the constructor (new SQLContexts/HiveContexts created through newSession " +
      "method is allowed). Please note that this conf needs to be set in Spark Conf. Once" +
      "a SQLContext/HiveContext has been created, changing the value of this conf will not" +
      "have effect.")
    .booleanConf
    .withDefault(true)

  val COMPRESS_CACHED = SQLConfigBuilder("spark.sql.inMemoryColumnarStorage.compressed")
    .internal
    .doc("When set to true Spark SQL will automatically select a compression codec for each " +
      "column based on statistics of the data.")
    .booleanConf
    .withDefault(true)

  val COLUMN_BATCH_SIZE = SQLConfigBuilder("spark.sql.inMemoryColumnarStorage.batchSize")
    .internal
    .doc("Controls the size of batches for columnar caching.  Larger batch sizes can improve " +
      "memory utilization and compression, but risk OOMs when caching data.")
    .intConf
    .withDefault(10000)

  val IN_MEMORY_PARTITION_PRUNING =
    SQLConfigBuilder("spark.sql.inMemoryColumnarStorage.partitionPruning")
      .internal
      .doc("When true, enable partition pruning for in-memory columnar tables.")
      .booleanConf
      .withDefault(true)

  val AUTO_BROADCASTJOIN_THRESHOLD = SQLConfigBuilder("spark.sql.autoBroadcastJoinThreshold")
    .doc("Configures the maximum size in bytes for a table that will be broadcast to all worker " +
      "nodes when performing a join.  By setting this value to -1 broadcasting can be disabled. " +
      "Note that currently statistics are only supported for Hive Metastore tables where the " +
      "command<code>ANALYZE TABLE &lt;tableName&gt; COMPUTE STATISTICS noscan</code> has been run.")
    .intConf
    .withDefault(10 * 1024 * 1024)

  val DEFAULT_SIZE_IN_BYTES = SQLConfigBuilder("spark.sql.defaultSizeInBytes")
    .internal
    .doc("The default table size used in query planning. By default, it is set to a larger " +
      "value than `spark.sql.autoBroadcastJoinThreshold` to be more conservative. That is to say " +
      "by default the optimizer will not choose to broadcast a table unless it knows for sure its" +
      "size is small enough.")
    .longConf
    .withDefault(-1)

  val SHUFFLE_PARTITIONS = SQLConfigBuilder("spark.sql.shuffle.partitions")
    .doc("The default number of partitions to use when shuffling data for joins or aggregations.")
    .intConf
    .withDefault(200)

  val SHUFFLE_TARGET_POSTSHUFFLE_INPUT_SIZE =
    SQLConfigBuilder("spark.sql.adaptive.shuffle.targetPostShuffleInputSize")
      .doc("The target post-shuffle input size in bytes of a task.")
      .bytesConf(ByteUnit.BYTE)
      .withDefault(64 * 1024 * 1024)

  val ADAPTIVE_EXECUTION_ENABLED = SQLConfigBuilder("spark.sql.adaptive.enabled")
    .doc("When true, enable adaptive query execution.")
    .booleanConf
    .withDefault(false)

  val SHUFFLE_MIN_NUM_POSTSHUFFLE_PARTITIONS =
    SQLConfigBuilder("spark.sql.adaptive.minNumPostShufflePartitions")
      .internal
      .doc("The advisory minimal number of post-shuffle partitions provided to " +
        "ExchangeCoordinator. This setting is used in our test to make sure we " +
        "have enough parallelism to expose issues that will not be exposed with a " +
        "single partition. When the value is a non-positive value, this setting will" +
        "not be provided to ExchangeCoordinator.")
      .intConf
      .withDefault(-1)

  val SUBEXPRESSION_ELIMINATION_ENABLED =
    SQLConfigBuilder("spark.sql.subexpressionElimination.enabled")
      .internal
      .doc("When true, common subexpressions will be eliminated.")
      .booleanConf
      .withDefault(true)

  val CASE_SENSITIVE = SQLConfigBuilder("spark.sql.caseSensitive")
    .doc("Whether the query analyzer should be case sensitive or not.")
    .booleanConf
    .withDefault(true)

  val PARQUET_SCHEMA_MERGING_ENABLED = SQLConfigBuilder("spark.sql.parquet.mergeSchema")
    .doc("When true, the Parquet data source merges schemas collected from all data files, " +
         "otherwise the schema is picked from the summary file or a random data file " +
         "if no summary file is available.")
    .booleanConf
    .withDefault(false)

  val PARQUET_SCHEMA_RESPECT_SUMMARIES = SQLConfigBuilder("spark.sql.parquet.respectSummaryFiles")
    .doc("When true, we make assumption that all part-files of Parquet are consistent with " +
         "summary files and we will ignore them when merging schema. Otherwise, if this is " +
         "false, which is the default, we will merge all part-files. This should be considered " +
         "as expert-only option, and shouldn't be enabled before knowing what it means exactly.")
    .booleanConf
    .withDefault(false)

  val PARQUET_BINARY_AS_STRING = SQLConfigBuilder("spark.sql.parquet.binaryAsString")
    .doc("Some other Parquet-producing systems, in particular Impala and older versions of " +
      "Spark SQL, do not differentiate between binary data and strings when writing out the " +
      "Parquet schema. This flag tells Spark SQL to interpret binary data as a string to provide " +
      "compatibility with these systems.")
    .booleanConf
    .withDefault(false)

  val PARQUET_INT96_AS_TIMESTAMP = SQLConfigBuilder("spark.sql.parquet.int96AsTimestamp")
    .doc("Some Parquet-producing systems, in particular Impala, store Timestamp into INT96. " +
      "Spark would also store Timestamp as INT96 because we need to avoid precision lost of the " +
      "nanoseconds field. This flag tells Spark SQL to interpret INT96 data as a timestamp to " +
      "provide compatibility with these systems.")
    .booleanConf
    .withDefault(true)

  val PARQUET_CACHE_METADATA = SQLConfigBuilder("spark.sql.parquet.cacheMetadata")
    .doc("Turns on caching of Parquet schema metadata. Can speed up querying of static data.")
    .booleanConf
    .withDefault(true)

  val PARQUET_COMPRESSION = SQLConfigBuilder("spark.sql.parquet.compression.codec")
    .doc("Sets the compression codec use when writing Parquet files. Acceptable values include: " +
      "uncompressed, snappy, gzip, lzo.")
    .stringConf
    .transform(_.toLowerCase())
    .checkValues(Set("uncompressed", "snappy", "gzip", "lzo"))
    .withDefault("gzip")

  val PARQUET_FILTER_PUSHDOWN_ENABLED = SQLConfigBuilder("spark.sql.parquet.filterPushdown")
    .doc("Enables Parquet filter push-down optimization when set to true.")
    .booleanConf
    .withDefault(true)

  val PARQUET_WRITE_LEGACY_FORMAT = SQLConfigBuilder("spark.sql.parquet.writeLegacyFormat")
    .doc("Whether to follow Parquet's format specification when converting Parquet schema to " +
      "Spark SQL schema and vice versa.")
    .booleanConf
    .withDefault(false)

  val PARQUET_OUTPUT_COMMITTER_CLASS = SQLConfigBuilder("spark.sql.parquet.output.committer.class")
    .doc("The output committer class used by Parquet. The specified class needs to be a " +
      "subclass of org.apache.hadoop.mapreduce.OutputCommitter.  Typically, it's also a subclass " +
      "of org.apache.parquet.hadoop.ParquetOutputCommitter.  NOTE: 1. Instead of SQLConf, this " +
      "option must be set in Hadoop Configuration.  2. This option overrides " +
      "\"spark.sql.sources.outputCommitterClass\".")
    .stringConf
    .withDefault(classOf[ParquetOutputCommitter].getName)

  val PARQUET_UNSAFE_ROW_RECORD_READER_ENABLED =
    SQLConfigBuilder("spark.sql.parquet.enableUnsafeRowRecordReader")
      .doc("Enables using the custom ParquetUnsafeRowRecordReader.")
      .booleanConf
      .withDefault(true)

  val PARQUET_VECTORIZED_READER_ENABLED =
    SQLConfigBuilder("spark.sql.parquet.enableVectorizedReader")
      .doc("Enables vectorized parquet decoding.")
      .booleanConf
      .withDefault(true)

  val ORC_FILTER_PUSHDOWN_ENABLED = SQLConfigBuilder("spark.sql.orc.filterPushdown")
    .doc("When true, enable filter pushdown for ORC files.")
    .booleanConf
    .withDefault(false)

  val HIVE_VERIFY_PARTITION_PATH = SQLConfigBuilder("spark.sql.hive.verifyPartitionPath")
    .doc("When true, check all the partition paths under the table\'s root directory " +
         "when reading data stored in HDFS.")
    .booleanConf
    .withDefault(false)

  val HIVE_METASTORE_PARTITION_PRUNING =
    SQLConfigBuilder("spark.sql.hive.metastorePartitionPruning")
      .doc("When true, some predicates will be pushed down into the Hive metastore so that " +
           "unmatching partitions can be eliminated earlier.")
      .booleanConf
      .withDefault(false)

  val NATIVE_VIEW = SQLConfigBuilder("spark.sql.nativeView")
    .internal
    .doc("When true, CREATE VIEW will be handled by Spark SQL instead of Hive native commands.  " +
         "Note that this function is experimental and should ony be used when you are using " +
         "non-hive-compatible tables written by Spark SQL.  The SQL string used to create " +
         "view should be fully qualified, i.e. use `tbl1`.`col1` instead of `*` whenever " +
         "possible, or you may get wrong result.")
    .booleanConf
    .withDefault(false)

  val CANONICAL_NATIVE_VIEW = SQLConfigBuilder("spark.sql.nativeView.canonical")
    .internal
    .doc("When this option and spark.sql.nativeView are both true, Spark SQL tries to handle " +
         "CREATE VIEW statement using SQL query string generated from view definition logical " +
         "plan.  If the logical plan doesn't have a SQL representation, we fallback to the " +
         "original native view implementation.")
    .booleanConf
    .withDefault(true)

  val COLUMN_NAME_OF_CORRUPT_RECORD = SQLConfigBuilder("spark.sql.columnNameOfCorruptRecord")
    .doc("The name of internal column for storing raw/un-parsed JSON records that fail to parse.")
    .stringConf
    .withDefault("_corrupt_record")

  val BROADCAST_TIMEOUT = SQLConfigBuilder("spark.sql.broadcastTimeout")
    .doc("Timeout in seconds for the broadcast wait time in broadcast joins.")
    .intConf
    .withDefault(5 * 60)

  // This is only used for the thriftserver
  val THRIFTSERVER_POOL = SQLConfigBuilder("spark.sql.thriftserver.scheduler.pool")
    .doc("Set a Fair Scheduler pool for a JDBC client session")
    .stringConf
    .optional

  val THRIFTSERVER_UI_STATEMENT_LIMIT =
    SQLConfigBuilder("spark.sql.thriftserver.ui.retainedStatements")
      .doc("The number of SQL statements kept in the JDBC/ODBC web UI history.")
      .intConf
      .withDefault(200)

  val THRIFTSERVER_UI_SESSION_LIMIT = SQLConfigBuilder("spark.sql.thriftserver.ui.retainedSessions")
    .doc("The number of SQL client sessions kept in the JDBC/ODBC web UI history.")
    .intConf
    .withDefault(200)

  // This is used to set the default data source
  val DEFAULT_DATA_SOURCE_NAME = SQLConfigBuilder("spark.sql.sources.default")
    .doc("The default data source to use in input/output.")
    .stringConf
    .withDefault("org.apache.spark.sql.parquet")

  // This is used to control the when we will split a schema's JSON string to multiple pieces
  // in order to fit the JSON string in metastore's table property (by default, the value has
  // a length restriction of 4000 characters). We will split the JSON string of a schema
  // to its length exceeds the threshold.
  val SCHEMA_STRING_LENGTH_THRESHOLD =
    SQLConfigBuilder("spark.sql.sources.schemaStringLengthThreshold")
      .doc("The maximum length allowed in a single cell when " +
        "storing additional schema information in Hive's metastore.")
      .internal
      .intConf
      .withDefault(4000)

  val PARTITION_DISCOVERY_ENABLED = SQLConfigBuilder("spark.sql.sources.partitionDiscovery.enabled")
    .doc("When true, automatically discover data partitions.")
    .booleanConf
    .withDefault(true)

  val PARTITION_COLUMN_TYPE_INFERENCE =
    SQLConfigBuilder("spark.sql.sources.partitionColumnTypeInference.enabled")
      .doc("When true, automatically infer the data types for partitioned columns.")
      .booleanConf
      .withDefault(true)

  val PARTITION_MAX_FILES =
    SQLConfigBuilder("spark.sql.sources.maxConcurrentWrites")
      .doc("The maximum number of concurrent files to open before falling back on sorting when " +
            "writing out files using dynamic partitioning.")
      .intConf
      .withDefault(1)

  val BUCKETING_ENABLED = SQLConfigBuilder("spark.sql.sources.bucketing.enabled")
    .doc("When false, we will treat bucketed table as normal table")
    .booleanConf
    .withDefault(true)

  // The output committer class used by HadoopFsRelation. The specified class needs to be a
  // subclass of org.apache.hadoop.mapreduce.OutputCommitter.
  //
  // NOTE:
  //
  //  1. Instead of SQLConf, this option *must be set in Hadoop Configuration*.
  //  2. This option can be overriden by "spark.sql.parquet.output.committer.class".
  val OUTPUT_COMMITTER_CLASS =
    SQLConfigBuilder("spark.sql.sources.outputCommitterClass").internal.stringConf.optional

  val PARALLEL_PARTITION_DISCOVERY_THRESHOLD =
    SQLConfigBuilder("spark.sql.sources.parallelPartitionDiscovery.threshold")
      .doc("The degree of parallelism for schema merging and partition discovery of " +
        "Parquet data sources.")
      .intConf
      .withDefault(32)

  // Whether to perform eager analysis when constructing a dataframe.
  // Set to false when debugging requires the ability to look at invalid query plans.
  val DATAFRAME_EAGER_ANALYSIS = SQLConfigBuilder("spark.sql.eagerAnalysis")
    .internal
    .doc("When true, eagerly applies query analysis on DataFrame operations.")
    .booleanConf
    .withDefault(true)

  // Whether to automatically resolve ambiguity in join conditions for self-joins.
  // See SPARK-6231.
  val DATAFRAME_SELF_JOIN_AUTO_RESOLVE_AMBIGUITY =
    SQLConfigBuilder("spark.sql.selfJoinAutoResolveAmbiguity")
      .internal
      .booleanConf
      .withDefault(true)

  // Whether to retain group by columns or not in GroupedData.agg.
  val DATAFRAME_RETAIN_GROUP_COLUMNS = SQLConfigBuilder("spark.sql.retainGroupColumns")
    .internal
    .booleanConf
    .withDefault(true)

  val DATAFRAME_PIVOT_MAX_VALUES = SQLConfigBuilder("spark.sql.pivotMaxValues")
    .doc("When doing a pivot without specifying values for the pivot column this is the maximum " +
      "number of (distinct) values that will be collected without error.")
    .intConf
    .withDefault(10000)

  val RUN_SQL_ON_FILES = SQLConfigBuilder("spark.sql.runSQLOnFiles")
    .internal
    .doc("When true, we could use `datasource`.`path` as table in SQL query")
    .booleanConf
    .withDefault(true)

  val PARSER_SUPPORT_QUOTEDID = SQLConfigBuilder("spark.sql.parser.supportQuotedIdentifiers")
    .internal
    .doc("Whether to use quoted identifier.\n  false: default(past) behavior. Implies only" +
      "alphaNumeric and underscore are valid characters in identifiers.\n" +
      "  true: implies column names can contain any character.")
    .booleanConf
    .withDefault(true)

  val PARSER_SUPPORT_SQL11_RESERVED_KEYWORDS =
    SQLConfigBuilder("spark.sql.parser.supportSQL11ReservedKeywords")
      .internal
      .doc("This flag should be set to true to enable support for SQL2011 reserved keywords.")
      .booleanConf
      .withDefault(false)

  val WHOLESTAGE_CODEGEN_ENABLED = SQLConfigBuilder("spark.sql.codegen.wholeStage")
    .internal
    .doc("When true, the whole stage (of multiple operators) will be compiled into single java" +
      " method")
    .booleanConf
    .withDefault(true)

  object Deprecated {
    val MAPRED_REDUCE_TASKS = "mapred.reduce.tasks"
    val EXTERNAL_SORT = "spark.sql.planner.externalSort"
    val USE_SQL_AGGREGATE2 = "spark.sql.useAggregate2"
    val TUNGSTEN_ENABLED = "spark.sql.tungsten.enabled"
    val CODEGEN_ENABLED = "spark.sql.codegen"
    val UNSAFE_ENABLED = "spark.sql.unsafe.enabled"
    val SORTMERGE_JOIN = "spark.sql.planner.sortMergeJoin"
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
private[sql] class SQLConf extends Serializable with CatalystConf with ParserConf with Logging {
  import SQLConf._

  /** Only low degree of contention is expected for conf, thus NOT using ConcurrentHashMap. */
  @transient protected[spark] val settings = java.util.Collections.synchronizedMap(
    new java.util.HashMap[String, String]())

  /** ************************ Spark SQL Params/Hints ******************* */

  def useCompression: Boolean = getConf(COMPRESS_CACHED)

  def parquetCompressionCodec: String = getConf(PARQUET_COMPRESSION)

  def parquetCacheMetadata: Boolean = getConf(PARQUET_CACHE_METADATA)

  def columnBatchSize: Int = getConf(COLUMN_BATCH_SIZE)

  def numShufflePartitions: Int = getConf(SHUFFLE_PARTITIONS)

  def targetPostShuffleInputSize: Long =
    getConf(SHUFFLE_TARGET_POSTSHUFFLE_INPUT_SIZE)

  def adaptiveExecutionEnabled: Boolean = getConf(ADAPTIVE_EXECUTION_ENABLED)

  def minNumPostShufflePartitions: Int =
    getConf(SHUFFLE_MIN_NUM_POSTSHUFFLE_PARTITIONS)

  def parquetFilterPushDown: Boolean = getConf(PARQUET_FILTER_PUSHDOWN_ENABLED)

  def orcFilterPushDown: Boolean = getConf(ORC_FILTER_PUSHDOWN_ENABLED)

  def verifyPartitionPath: Boolean = getConf(HIVE_VERIFY_PARTITION_PATH)

  def metastorePartitionPruning: Boolean = getConf(HIVE_METASTORE_PARTITION_PRUNING)

  def nativeView: Boolean = getConf(NATIVE_VIEW)

  def wholeStageEnabled: Boolean = getConf(WHOLESTAGE_CODEGEN_ENABLED)

  def canonicalView: Boolean = getConf(CANONICAL_NATIVE_VIEW)

  def caseSensitiveAnalysis: Boolean = getConf(SQLConf.CASE_SENSITIVE)

  def subexpressionEliminationEnabled: Boolean =
    getConf(SUBEXPRESSION_ELIMINATION_ENABLED)

  def autoBroadcastJoinThreshold: Int = getConf(AUTO_BROADCASTJOIN_THRESHOLD)

  def defaultSizeInBytes: Long =
    getConf(DEFAULT_SIZE_IN_BYTES, autoBroadcastJoinThreshold + 1L)

  def isParquetBinaryAsString: Boolean = getConf(PARQUET_BINARY_AS_STRING)

  def isParquetINT96AsTimestamp: Boolean = getConf(PARQUET_INT96_AS_TIMESTAMP)

  def writeLegacyParquetFormat: Boolean = getConf(PARQUET_WRITE_LEGACY_FORMAT)

  def inMemoryPartitionPruning: Boolean = getConf(IN_MEMORY_PARTITION_PRUNING)

  def columnNameOfCorruptRecord: String = getConf(COLUMN_NAME_OF_CORRUPT_RECORD)

  def broadcastTimeout: Int = getConf(BROADCAST_TIMEOUT)

  def defaultDataSourceName: String = getConf(DEFAULT_DATA_SOURCE_NAME)

  def partitionDiscoveryEnabled(): Boolean =
    getConf(SQLConf.PARTITION_DISCOVERY_ENABLED)

  def partitionColumnTypeInferenceEnabled(): Boolean =
    getConf(SQLConf.PARTITION_COLUMN_TYPE_INFERENCE)

  def parallelPartitionDiscoveryThreshold: Int =
    getConf(SQLConf.PARALLEL_PARTITION_DISCOVERY_THRESHOLD)

  def bucketingEnabled(): Boolean = getConf(SQLConf.BUCKETING_ENABLED)

  // Do not use a value larger than 4000 as the default value of this property.
  // See the comments of SCHEMA_STRING_LENGTH_THRESHOLD above for more information.
  def schemaStringLengthThreshold: Int = getConf(SCHEMA_STRING_LENGTH_THRESHOLD)

  def dataFrameEagerAnalysis: Boolean = getConf(DATAFRAME_EAGER_ANALYSIS)

  def dataFrameSelfJoinAutoResolveAmbiguity: Boolean =
    getConf(DATAFRAME_SELF_JOIN_AUTO_RESOLVE_AMBIGUITY)

  def dataFrameRetainGroupColumns: Boolean = getConf(DATAFRAME_RETAIN_GROUP_COLUMNS)

  def runSQLOnFile: Boolean = getConf(RUN_SQL_ON_FILES)

  def supportQuotedId: Boolean = getConf(PARSER_SUPPORT_QUOTEDID)

  def supportSQL11ReservedKeywords: Boolean = getConf(PARSER_SUPPORT_SQL11_RESERVED_KEYWORDS)

  /** ********************** SQLConf functionality methods ************ */

  /** Set Spark SQL configuration properties. */
  def setConf(props: Properties): Unit = settings.synchronized {
    props.asScala.foreach { case (k, v) => setConfString(k, v) }
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
    setConfWithCheck(key, value)
  }

  /** Set the given Spark SQL configuration property. */
  def setConf[T](entry: ConfigEntry[T], value: T): Unit = {
    require(entry != null, "entry cannot be null")
    require(value != null, s"value cannot be null for key: ${entry.key}")
    require(sqlConfEntries.get(entry.key) == entry, s"$entry is not registered")
    setConfWithCheck(entry.key, entry.stringConverter(value))
  }

  /** Return the value of Spark SQL configuration property for the given key. */
  @throws[NoSuchElementException]("if key is not set")
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
   * yet, return `defaultValue`. This is useful when `defaultValue` in ConfigEntry is not the
   * desired one.
   */
  def getConf[T](entry: ConfigEntry[T], defaultValue: T): T = {
    require(sqlConfEntries.get(entry.key) == entry, s"$entry is not registered")
    Option(settings.get(entry.key)).map(entry.valueConverter).getOrElse(defaultValue)
  }

  /**
   * Return the value of Spark SQL configuration property for the given key. If the key is not set
   * yet, return `defaultValue` in [[ConfigEntry]].
   */
  def getConf[T](entry: ConfigEntry[T]): T = {
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
  def getAllConfs: immutable.Map[String, String] =
    settings.synchronized { settings.asScala.toMap }

  /**
   * Return all the configuration definitions that have been defined in [[SQLConf]]. Each
   * definition contains key, defaultValue and doc.
   */
  def getAllDefinedConfs: Seq[(String, String, String)] = sqlConfEntries.synchronized {
    sqlConfEntries.values.asScala.filter(_.isPublic).map { entry =>
      (entry.key, entry.defaultValueString, entry.doc)
    }.toSeq
  }

  private def setConfWithCheck(key: String, value: String): Unit = {
    if (key.startsWith("spark.") && !key.startsWith("spark.sql.")) {
      logWarning(s"Attempt to set non-Spark SQL config in SQLConf: key = $key, value = $value")
    }
    settings.put(key, value)
  }

  def unsetConf(key: String): Unit = {
    settings.remove(key)
  }

  private[spark] def unsetConf(entry: ConfigEntry[_]): Unit = {
    settings.remove(entry.key)
  }

  def clear(): Unit = {
    settings.clear()
  }
}

