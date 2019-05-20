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

import java.util.{Locale, NoSuchElementException, Properties, TimeZone}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import java.util.zip.Deflater

import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.util.matching.Regex

import org.apache.hadoop.fs.Path

import org.apache.spark.{SparkContext, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.network.util.ByteUnit
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator
import org.apache.spark.unsafe.array.ByteArrayMethods
import org.apache.spark.util.Utils

////////////////////////////////////////////////////////////////////////////////////////////////////
// This file defines the configuration options for Spark SQL.
////////////////////////////////////////////////////////////////////////////////////////////////////


object SQLConf {

  private[sql] val sqlConfEntries = java.util.Collections.synchronizedMap(
    new java.util.HashMap[String, ConfigEntry[_]]())

  val staticConfKeys: java.util.Set[String] =
    java.util.Collections.synchronizedSet(new java.util.HashSet[String]())

  private def register(entry: ConfigEntry[_]): Unit = sqlConfEntries.synchronized {
    require(!sqlConfEntries.containsKey(entry.key),
      s"Duplicate SQLConfigEntry. ${entry.key} has been registered")
    sqlConfEntries.put(entry.key, entry)
  }

  // For testing only
  private[sql] def unregister(entry: ConfigEntry[_]): Unit = sqlConfEntries.synchronized {
    sqlConfEntries.remove(entry.key)
  }

  def buildConf(key: String): ConfigBuilder = ConfigBuilder(key).onCreate(register)

  def buildStaticConf(key: String): ConfigBuilder = {
    ConfigBuilder(key).onCreate { entry =>
      staticConfKeys.add(entry.key)
      SQLConf.register(entry)
    }
  }

  /**
   * Default config. Only used when there is no active SparkSession for the thread.
   * See [[get]] for more information.
   */
  private lazy val fallbackConf = new ThreadLocal[SQLConf] {
    override def initialValue: SQLConf = new SQLConf
  }

  /** See [[get]] for more information. */
  def getFallbackConf: SQLConf = fallbackConf.get()

  private lazy val existingConf = new ThreadLocal[SQLConf] {
    override def initialValue: SQLConf = null
  }

  def withExistingConf[T](conf: SQLConf)(f: => T): T = {
    existingConf.set(conf)
    try {
      f
    } finally {
      existingConf.remove()
    }
  }

  /**
   * Defines a getter that returns the SQLConf within scope.
   * See [[get]] for more information.
   */
  private val confGetter = new AtomicReference[() => SQLConf](() => fallbackConf.get())

  /**
   * Sets the active config object within the current scope.
   * See [[get]] for more information.
   */
  def setSQLConfGetter(getter: () => SQLConf): Unit = {
    confGetter.set(getter)
  }

  /**
   * Returns the active config object within the current scope. If there is an active SparkSession,
   * the proper SQLConf associated with the thread's active session is used. If it's called from
   * tasks in the executor side, a SQLConf will be created from job local properties, which are set
   * and propagated from the driver side.
   *
   * The way this works is a little bit convoluted, due to the fact that config was added initially
   * only for physical plans (and as a result not in sql/catalyst module).
   *
   * The first time a SparkSession is instantiated, we set the [[confGetter]] to return the
   * active SparkSession's config. If there is no active SparkSession, it returns using the thread
   * local [[fallbackConf]]. The reason [[fallbackConf]] is a thread local (rather than just a conf)
   * is to support setting different config options for different threads so we can potentially
   * run tests in parallel. At the time this feature was implemented, this was a no-op since we
   * run unit tests (that does not involve SparkSession) in serial order.
   */
  def get: SQLConf = {
    if (TaskContext.get != null) {
      new ReadOnlySQLConf(TaskContext.get())
    } else {
      val isSchedulerEventLoopThread = SparkContext.getActive
        .map(_.dagScheduler.eventProcessLoop.eventThread)
        .exists(_.getId == Thread.currentThread().getId)
      if (isSchedulerEventLoopThread) {
        // DAGScheduler event loop thread does not have an active SparkSession, the `confGetter`
        // will return `fallbackConf` which is unexpected. Here we require the caller to get the
        // conf within `withExistingConf`, otherwise fail the query.
        val conf = existingConf.get()
        if (conf != null) {
          conf
        } else if (Utils.isTesting) {
          throw new RuntimeException("Cannot get SQLConf inside scheduler event loop thread.")
        } else {
          confGetter.get()()
        }
      } else {
        confGetter.get()()
      }
    }
  }

  val OPTIMIZER_EXCLUDED_RULES = buildConf("spark.sql.optimizer.excludedRules")
    .doc("Configures a list of rules to be disabled in the optimizer, in which the rules are " +
      "specified by their rule names and separated by comma. It is not guaranteed that all the " +
      "rules in this configuration will eventually be excluded, as some rules are necessary " +
      "for correctness. The optimizer will log the rules that have indeed been excluded.")
    .stringConf
    .createOptional

  val OPTIMIZER_MAX_ITERATIONS = buildConf("spark.sql.optimizer.maxIterations")
    .internal()
    .doc("The max number of iterations the optimizer and analyzer runs.")
    .intConf
    .createWithDefault(100)

  val OPTIMIZER_INSET_CONVERSION_THRESHOLD =
    buildConf("spark.sql.optimizer.inSetConversionThreshold")
      .internal()
      .doc("The threshold of set size for InSet conversion.")
      .intConf
      .createWithDefault(10)

  val OPTIMIZER_INSET_SWITCH_THRESHOLD =
    buildConf("spark.sql.optimizer.inSetSwitchThreshold")
      .internal()
      .doc("Configures the max set size in InSet for which Spark will generate code with " +
        "switch statements. This is applicable only to bytes, shorts, ints, dates.")
      .intConf
      .checkValue(threshold => threshold >= 0 && threshold <= 600, "The max set size " +
        "for using switch statements in InSet must be non-negative and less than or equal to 600")
      .createWithDefault(400)

  val OPTIMIZER_PLAN_CHANGE_LOG_LEVEL = buildConf("spark.sql.optimizer.planChangeLog.level")
    .internal()
    .doc("Configures the log level for logging the change from the original plan to the new " +
      "plan after a rule or batch is applied. The value can be 'trace', 'debug', 'info', " +
      "'warn', or 'error'. The default log level is 'trace'.")
    .stringConf
    .transform(_.toUpperCase(Locale.ROOT))
    .checkValue(logLevel => Set("TRACE", "DEBUG", "INFO", "WARN", "ERROR").contains(logLevel),
      "Invalid value for 'spark.sql.optimizer.planChangeLog.level'. Valid values are " +
        "'trace', 'debug', 'info', 'warn' and 'error'.")
    .createWithDefault("trace")

  val OPTIMIZER_PLAN_CHANGE_LOG_RULES = buildConf("spark.sql.optimizer.planChangeLog.rules")
    .internal()
    .doc("Configures a list of rules to be logged in the optimizer, in which the rules are " +
      "specified by their rule names and separated by comma.")
    .stringConf
    .createOptional

  val OPTIMIZER_PLAN_CHANGE_LOG_BATCHES = buildConf("spark.sql.optimizer.planChangeLog.batches")
    .internal()
    .doc("Configures a list of batches to be logged in the optimizer, in which the batches " +
      "are specified by their batch names and separated by comma.")
    .stringConf
    .createOptional

  val COMPRESS_CACHED = buildConf("spark.sql.inMemoryColumnarStorage.compressed")
    .doc("When set to true Spark SQL will automatically select a compression codec for each " +
      "column based on statistics of the data.")
    .booleanConf
    .createWithDefault(true)

  val COLUMN_BATCH_SIZE = buildConf("spark.sql.inMemoryColumnarStorage.batchSize")
    .doc("Controls the size of batches for columnar caching.  Larger batch sizes can improve " +
      "memory utilization and compression, but risk OOMs when caching data.")
    .intConf
    .createWithDefault(10000)

  val IN_MEMORY_PARTITION_PRUNING =
    buildConf("spark.sql.inMemoryColumnarStorage.partitionPruning")
      .internal()
      .doc("When true, enable partition pruning for in-memory columnar tables.")
      .booleanConf
      .createWithDefault(true)

  val CACHE_VECTORIZED_READER_ENABLED =
    buildConf("spark.sql.inMemoryColumnarStorage.enableVectorizedReader")
      .doc("Enables vectorized reader for columnar caching.")
      .booleanConf
      .createWithDefault(true)

  val COLUMN_VECTOR_OFFHEAP_ENABLED =
    buildConf("spark.sql.columnVector.offheap.enabled")
      .internal()
      .doc("When true, use OffHeapColumnVector in ColumnarBatch.")
      .booleanConf
      .createWithDefault(false)

  val PREFER_SORTMERGEJOIN = buildConf("spark.sql.join.preferSortMergeJoin")
    .internal()
    .doc("When true, prefer sort merge join over shuffle hash join.")
    .booleanConf
    .createWithDefault(true)

  val RADIX_SORT_ENABLED = buildConf("spark.sql.sort.enableRadixSort")
    .internal()
    .doc("When true, enable use of radix sort when possible. Radix sort is much faster but " +
      "requires additional memory to be reserved up-front. The memory overhead may be " +
      "significant when sorting very small rows (up to 50% more in this case).")
    .booleanConf
    .createWithDefault(true)

  val AUTO_BROADCASTJOIN_THRESHOLD = buildConf("spark.sql.autoBroadcastJoinThreshold")
    .doc("Configures the maximum size in bytes for a table that will be broadcast to all worker " +
      "nodes when performing a join.  By setting this value to -1 broadcasting can be disabled. " +
      "Note that currently statistics are only supported for Hive Metastore tables where the " +
      "command <code>ANALYZE TABLE &lt;tableName&gt; COMPUTE STATISTICS noscan</code> has been " +
      "run, and file-based data source tables where the statistics are computed directly on " +
      "the files of data.")
    .bytesConf(ByteUnit.BYTE)
    .createWithDefault(10L * 1024 * 1024)

  val LIMIT_SCALE_UP_FACTOR = buildConf("spark.sql.limit.scaleUpFactor")
    .internal()
    .doc("Minimal increase rate in number of partitions between attempts when executing a take " +
      "on a query. Higher values lead to more partitions read. Lower values might lead to " +
      "longer execution times as more jobs will be run")
    .intConf
    .createWithDefault(4)

  val ADVANCED_PARTITION_PREDICATE_PUSHDOWN =
    buildConf("spark.sql.hive.advancedPartitionPredicatePushdown.enabled")
      .internal()
      .doc("When true, advanced partition predicate pushdown into Hive metastore is enabled.")
      .booleanConf
      .createWithDefault(true)

  val SHUFFLE_PARTITIONS = buildConf("spark.sql.shuffle.partitions")
    .doc("The default number of partitions to use when shuffling data for joins or aggregations. " +
      "Note: For structured streaming, this configuration cannot be changed between query " +
      "restarts from the same checkpoint location.")
    .intConf
    .checkValue(_ > 0, "The value of spark.sql.shuffle.partitions must be positive")
    .createWithDefault(200)

  val SHUFFLE_TARGET_POSTSHUFFLE_INPUT_SIZE =
    buildConf("spark.sql.adaptive.shuffle.targetPostShuffleInputSize")
      .doc("The target post-shuffle input size in bytes of a task.")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefault(64 * 1024 * 1024)

  val ADAPTIVE_EXECUTION_ENABLED = buildConf("spark.sql.adaptive.enabled")
    .doc("When true, enable adaptive query execution.")
    .booleanConf
    .createWithDefault(false)

  val SHUFFLE_MIN_NUM_POSTSHUFFLE_PARTITIONS =
    buildConf("spark.sql.adaptive.minNumPostShufflePartitions")
      .internal()
      .doc("The advisory minimal number of post-shuffle partitions provided to " +
        "ExchangeCoordinator. This setting is used in our test to make sure we " +
        "have enough parallelism to expose issues that will not be exposed with a " +
        "single partition. When the value is a non-positive value, this setting will " +
        "not be provided to ExchangeCoordinator.")
      .intConf
      .createWithDefault(-1)

  val SUBEXPRESSION_ELIMINATION_ENABLED =
    buildConf("spark.sql.subexpressionElimination.enabled")
      .internal()
      .doc("When true, common subexpressions will be eliminated.")
      .booleanConf
      .createWithDefault(true)

  val CASE_SENSITIVE = buildConf("spark.sql.caseSensitive")
    .internal()
    .doc("Whether the query analyzer should be case sensitive or not. " +
      "Default to case insensitive. It is highly discouraged to turn on case sensitive mode.")
    .booleanConf
    .createWithDefault(false)

  val CONSTRAINT_PROPAGATION_ENABLED = buildConf("spark.sql.constraintPropagation.enabled")
    .internal()
    .doc("When true, the query optimizer will infer and propagate data constraints in the query " +
      "plan to optimize them. Constraint propagation can sometimes be computationally expensive " +
      "for certain kinds of query plans (such as those with a large number of predicates and " +
      "aliases) which might negatively impact overall runtime.")
    .booleanConf
    .createWithDefault(true)

  val ANSI_SQL_PARSER =
    buildConf("spark.sql.parser.ansi.enabled")
      .doc("When true, tries to conform to ANSI SQL syntax.")
      .booleanConf
      .createWithDefault(false)

  val ESCAPED_STRING_LITERALS = buildConf("spark.sql.parser.escapedStringLiterals")
    .internal()
    .doc("When true, string literals (including regex patterns) remain escaped in our SQL " +
      "parser. The default is false since Spark 2.0. Setting it to true can restore the behavior " +
      "prior to Spark 2.0.")
    .booleanConf
    .createWithDefault(false)

  val FILE_COMRESSION_FACTOR = buildConf("spark.sql.sources.fileCompressionFactor")
    .internal()
    .doc("When estimating the output data size of a table scan, multiply the file size with this " +
      "factor as the estimated data size, in case the data is compressed in the file and lead to" +
      " a heavily underestimated result.")
    .doubleConf
    .checkValue(_ > 0, "the value of fileDataSizeFactor must be greater than 0")
    .createWithDefault(1.0)

  val PARQUET_SCHEMA_MERGING_ENABLED = buildConf("spark.sql.parquet.mergeSchema")
    .doc("When true, the Parquet data source merges schemas collected from all data files, " +
         "otherwise the schema is picked from the summary file or a random data file " +
         "if no summary file is available.")
    .booleanConf
    .createWithDefault(false)

  val PARQUET_SCHEMA_RESPECT_SUMMARIES = buildConf("spark.sql.parquet.respectSummaryFiles")
    .doc("When true, we make assumption that all part-files of Parquet are consistent with " +
         "summary files and we will ignore them when merging schema. Otherwise, if this is " +
         "false, which is the default, we will merge all part-files. This should be considered " +
         "as expert-only option, and shouldn't be enabled before knowing what it means exactly.")
    .booleanConf
    .createWithDefault(false)

  val PARQUET_BINARY_AS_STRING = buildConf("spark.sql.parquet.binaryAsString")
    .doc("Some other Parquet-producing systems, in particular Impala and older versions of " +
      "Spark SQL, do not differentiate between binary data and strings when writing out the " +
      "Parquet schema. This flag tells Spark SQL to interpret binary data as a string to provide " +
      "compatibility with these systems.")
    .booleanConf
    .createWithDefault(false)

  val PARQUET_INT96_AS_TIMESTAMP = buildConf("spark.sql.parquet.int96AsTimestamp")
    .doc("Some Parquet-producing systems, in particular Impala, store Timestamp into INT96. " +
      "Spark would also store Timestamp as INT96 because we need to avoid precision lost of the " +
      "nanoseconds field. This flag tells Spark SQL to interpret INT96 data as a timestamp to " +
      "provide compatibility with these systems.")
    .booleanConf
    .createWithDefault(true)

  val PARQUET_INT96_TIMESTAMP_CONVERSION = buildConf("spark.sql.parquet.int96TimestampConversion")
    .doc("This controls whether timestamp adjustments should be applied to INT96 data when " +
      "converting to timestamps, for data written by Impala.  This is necessary because Impala " +
      "stores INT96 data with a different timezone offset than Hive & Spark.")
    .booleanConf
    .createWithDefault(false)

  object ParquetOutputTimestampType extends Enumeration {
    val INT96, TIMESTAMP_MICROS, TIMESTAMP_MILLIS = Value
  }

  val PARQUET_OUTPUT_TIMESTAMP_TYPE = buildConf("spark.sql.parquet.outputTimestampType")
    .doc("Sets which Parquet timestamp type to use when Spark writes data to Parquet files. " +
      "INT96 is a non-standard but commonly used timestamp type in Parquet. TIMESTAMP_MICROS " +
      "is a standard timestamp type in Parquet, which stores number of microseconds from the " +
      "Unix epoch. TIMESTAMP_MILLIS is also standard, but with millisecond precision, which " +
      "means Spark has to truncate the microsecond portion of its timestamp value.")
    .stringConf
    .transform(_.toUpperCase(Locale.ROOT))
    .checkValues(ParquetOutputTimestampType.values.map(_.toString))
    .createWithDefault(ParquetOutputTimestampType.TIMESTAMP_MICROS.toString)

  val PARQUET_INT64_AS_TIMESTAMP_MILLIS = buildConf("spark.sql.parquet.int64AsTimestampMillis")
    .doc(s"(Deprecated since Spark 2.3, please set ${PARQUET_OUTPUT_TIMESTAMP_TYPE.key}.) " +
      "When true, timestamp values will be stored as INT64 with TIMESTAMP_MILLIS as the " +
      "extended type. In this mode, the microsecond portion of the timestamp value will be" +
      "truncated.")
    .booleanConf
    .createWithDefault(false)

  val PARQUET_COMPRESSION = buildConf("spark.sql.parquet.compression.codec")
    .doc("Sets the compression codec used when writing Parquet files. If either `compression` or " +
      "`parquet.compression` is specified in the table-specific options/properties, the " +
      "precedence would be `compression`, `parquet.compression`, " +
      "`spark.sql.parquet.compression.codec`. Acceptable values include: none, uncompressed, " +
      "snappy, gzip, lzo, brotli, lz4, zstd.")
    .stringConf
    .transform(_.toLowerCase(Locale.ROOT))
    .checkValues(Set("none", "uncompressed", "snappy", "gzip", "lzo", "lz4", "brotli", "zstd"))
    .createWithDefault("snappy")

  val PARQUET_FILTER_PUSHDOWN_ENABLED = buildConf("spark.sql.parquet.filterPushdown")
    .doc("Enables Parquet filter push-down optimization when set to true.")
    .booleanConf
    .createWithDefault(true)

  val PARQUET_FILTER_PUSHDOWN_DATE_ENABLED = buildConf("spark.sql.parquet.filterPushdown.date")
    .doc("If true, enables Parquet filter push-down optimization for Date. " +
      s"This configuration only has an effect when '${PARQUET_FILTER_PUSHDOWN_ENABLED.key}' is " +
      "enabled.")
    .internal()
    .booleanConf
    .createWithDefault(true)

  val PARQUET_FILTER_PUSHDOWN_TIMESTAMP_ENABLED =
    buildConf("spark.sql.parquet.filterPushdown.timestamp")
      .doc("If true, enables Parquet filter push-down optimization for Timestamp. " +
        s"This configuration only has an effect when '${PARQUET_FILTER_PUSHDOWN_ENABLED.key}' is " +
        "enabled and Timestamp stored as TIMESTAMP_MICROS or TIMESTAMP_MILLIS type.")
    .internal()
    .booleanConf
    .createWithDefault(true)

  val PARQUET_FILTER_PUSHDOWN_DECIMAL_ENABLED =
    buildConf("spark.sql.parquet.filterPushdown.decimal")
      .doc("If true, enables Parquet filter push-down optimization for Decimal. " +
        s"This configuration only has an effect when '${PARQUET_FILTER_PUSHDOWN_ENABLED.key}' is " +
        "enabled.")
      .internal()
      .booleanConf
      .createWithDefault(true)

  val PARQUET_FILTER_PUSHDOWN_STRING_STARTSWITH_ENABLED =
    buildConf("spark.sql.parquet.filterPushdown.string.startsWith")
    .doc("If true, enables Parquet filter push-down optimization for string startsWith function. " +
      s"This configuration only has an effect when '${PARQUET_FILTER_PUSHDOWN_ENABLED.key}' is " +
      "enabled.")
    .internal()
    .booleanConf
    .createWithDefault(true)

  val PARQUET_FILTER_PUSHDOWN_INFILTERTHRESHOLD =
    buildConf("spark.sql.parquet.pushdown.inFilterThreshold")
      .doc("The maximum number of values to filter push-down optimization for IN predicate. " +
        "Large threshold won't necessarily provide much better performance. " +
        "The experiment argued that 300 is the limit threshold. " +
        "By setting this value to 0 this feature can be disabled. " +
        s"This configuration only has an effect when '${PARQUET_FILTER_PUSHDOWN_ENABLED.key}' is " +
        "enabled.")
      .internal()
      .intConf
      .checkValue(threshold => threshold >= 0, "The threshold must not be negative.")
      .createWithDefault(10)

  val PARQUET_WRITE_LEGACY_FORMAT = buildConf("spark.sql.parquet.writeLegacyFormat")
    .doc("If true, data will be written in a way of Spark 1.4 and earlier. For example, decimal " +
      "values will be written in Apache Parquet's fixed-length byte array format, which other " +
      "systems such as Apache Hive and Apache Impala use. If false, the newer format in Parquet " +
      "will be used. For example, decimals will be written in int-based format. If Parquet " +
      "output is intended for use with systems that do not support this newer format, set to true.")
    .booleanConf
    .createWithDefault(false)

  val PARQUET_OUTPUT_COMMITTER_CLASS = buildConf("spark.sql.parquet.output.committer.class")
    .doc("The output committer class used by Parquet. The specified class needs to be a " +
      "subclass of org.apache.hadoop.mapreduce.OutputCommitter. Typically, it's also a subclass " +
      "of org.apache.parquet.hadoop.ParquetOutputCommitter. If it is not, then metadata summaries" +
      "will never be created, irrespective of the value of parquet.summary.metadata.level")
    .internal()
    .stringConf
    .createWithDefault("org.apache.parquet.hadoop.ParquetOutputCommitter")

  val PARQUET_VECTORIZED_READER_ENABLED =
    buildConf("spark.sql.parquet.enableVectorizedReader")
      .doc("Enables vectorized parquet decoding.")
      .booleanConf
      .createWithDefault(true)

  val PARQUET_RECORD_FILTER_ENABLED = buildConf("spark.sql.parquet.recordLevelFilter.enabled")
    .doc("If true, enables Parquet's native record-level filtering using the pushed down " +
      "filters. " +
      s"This configuration only has an effect when '${PARQUET_FILTER_PUSHDOWN_ENABLED.key}' " +
      "is enabled and the vectorized reader is not used. You can ensure the vectorized reader " +
      s"is not used by setting '${PARQUET_VECTORIZED_READER_ENABLED.key}' to false.")
    .booleanConf
    .createWithDefault(false)

  val PARQUET_VECTORIZED_READER_BATCH_SIZE = buildConf("spark.sql.parquet.columnarReaderBatchSize")
    .doc("The number of rows to include in a parquet vectorized reader batch. The number should " +
      "be carefully chosen to minimize overhead and avoid OOMs in reading data.")
    .intConf
    .createWithDefault(4096)

  val ORC_COMPRESSION = buildConf("spark.sql.orc.compression.codec")
    .doc("Sets the compression codec used when writing ORC files. If either `compression` or " +
      "`orc.compress` is specified in the table-specific options/properties, the precedence " +
      "would be `compression`, `orc.compress`, `spark.sql.orc.compression.codec`." +
      "Acceptable values include: none, uncompressed, snappy, zlib, lzo.")
    .stringConf
    .transform(_.toLowerCase(Locale.ROOT))
    .checkValues(Set("none", "uncompressed", "snappy", "zlib", "lzo"))
    .createWithDefault("snappy")

  val ORC_IMPLEMENTATION = buildConf("spark.sql.orc.impl")
    .doc("When native, use the native version of ORC support instead of the ORC library in Hive " +
      "1.2.1. It is 'hive' by default prior to Spark 2.4.")
    .internal()
    .stringConf
    .checkValues(Set("hive", "native"))
    .createWithDefault("native")

  val ORC_VECTORIZED_READER_ENABLED = buildConf("spark.sql.orc.enableVectorizedReader")
    .doc("Enables vectorized orc decoding.")
    .booleanConf
    .createWithDefault(true)

  val ORC_VECTORIZED_READER_BATCH_SIZE = buildConf("spark.sql.orc.columnarReaderBatchSize")
    .doc("The number of rows to include in a orc vectorized reader batch. The number should " +
      "be carefully chosen to minimize overhead and avoid OOMs in reading data.")
    .intConf
    .createWithDefault(4096)

  val ORC_FILTER_PUSHDOWN_ENABLED = buildConf("spark.sql.orc.filterPushdown")
    .doc("When true, enable filter pushdown for ORC files.")
    .booleanConf
    .createWithDefault(true)

  val HIVE_VERIFY_PARTITION_PATH = buildConf("spark.sql.hive.verifyPartitionPath")
    .doc("When true, check all the partition paths under the table\'s root directory " +
         "when reading data stored in HDFS. This configuration will be deprecated in the future " +
         "releases and replaced by spark.files.ignoreMissingFiles.")
    .booleanConf
    .createWithDefault(false)

  val HIVE_METASTORE_PARTITION_PRUNING =
    buildConf("spark.sql.hive.metastorePartitionPruning")
      .doc("When true, some predicates will be pushed down into the Hive metastore so that " +
           "unmatching partitions can be eliminated earlier. This only affects Hive tables " +
           "not converted to filesource relations (see HiveUtils.CONVERT_METASTORE_PARQUET and " +
           "HiveUtils.CONVERT_METASTORE_ORC for more information).")
      .booleanConf
      .createWithDefault(true)

  val HIVE_MANAGE_FILESOURCE_PARTITIONS =
    buildConf("spark.sql.hive.manageFilesourcePartitions")
      .doc("When true, enable metastore partition management for file source tables as well. " +
           "This includes both datasource and converted Hive tables. When partition management " +
           "is enabled, datasource tables store partition in the Hive metastore, and use the " +
           "metastore to prune partitions during query planning.")
      .booleanConf
      .createWithDefault(true)

  val HIVE_FILESOURCE_PARTITION_FILE_CACHE_SIZE =
    buildConf("spark.sql.hive.filesourcePartitionFileCacheSize")
      .doc("When nonzero, enable caching of partition file metadata in memory. All tables share " +
           "a cache that can use up to specified num bytes for file metadata. This conf only " +
           "has an effect when hive filesource partition management is enabled.")
      .longConf
      .createWithDefault(250 * 1024 * 1024)

  object HiveCaseSensitiveInferenceMode extends Enumeration {
    val INFER_AND_SAVE, INFER_ONLY, NEVER_INFER = Value
  }

  val HIVE_CASE_SENSITIVE_INFERENCE = buildConf("spark.sql.hive.caseSensitiveInferenceMode")
    .internal()
    .doc("Sets the action to take when a case-sensitive schema cannot be read from a Hive Serde " +
      "table's properties when reading the table with Spark native data sources. Valid options " +
      "include INFER_AND_SAVE (infer the case-sensitive schema from the underlying data files " +
      "and write it back to the table properties), INFER_ONLY (infer the schema but don't " +
      "attempt to write it to the table properties) and NEVER_INFER (the default mode-- fallback " +
      "to using the case-insensitive metastore schema instead of inferring).")
    .stringConf
    .transform(_.toUpperCase(Locale.ROOT))
    .checkValues(HiveCaseSensitiveInferenceMode.values.map(_.toString))
    .createWithDefault(HiveCaseSensitiveInferenceMode.NEVER_INFER.toString)

  val OPTIMIZER_METADATA_ONLY = buildConf("spark.sql.optimizer.metadataOnly")
    .internal()
    .doc("When true, enable the metadata-only query optimization that use the table's metadata " +
      "to produce the partition columns instead of table scans. It applies when all the columns " +
      "scanned are partition columns and the query has an aggregate operator that satisfies " +
      "distinct semantics. By default the optimization is disabled, since it may return " +
      "incorrect results when the files are empty.")
    .booleanConf
    .createWithDefault(false)

  val COLUMN_NAME_OF_CORRUPT_RECORD = buildConf("spark.sql.columnNameOfCorruptRecord")
    .doc("The name of internal column for storing raw/un-parsed JSON and CSV records that fail " +
      "to parse.")
    .stringConf
    .createWithDefault("_corrupt_record")

  val FROM_JSON_FORCE_NULLABLE_SCHEMA = buildConf("spark.sql.fromJsonForceNullableSchema")
    .internal()
    .doc("When true, force the output schema of the from_json() function to be nullable " +
      "(including all the fields). Otherwise, the schema might not be compatible with" +
      "actual data, which leads to corruptions. This config will be removed in Spark 3.0.")
    .booleanConf
    .createWithDefault(true)

  val BROADCAST_TIMEOUT = buildConf("spark.sql.broadcastTimeout")
    .doc("Timeout in seconds for the broadcast wait time in broadcast joins.")
    .timeConf(TimeUnit.SECONDS)
    .createWithDefault(5 * 60)

  // This is only used for the thriftserver
  val THRIFTSERVER_POOL = buildConf("spark.sql.thriftserver.scheduler.pool")
    .doc("Set a Fair Scheduler pool for a JDBC client session.")
    .stringConf
    .createOptional

  val THRIFTSERVER_INCREMENTAL_COLLECT =
    buildConf("spark.sql.thriftServer.incrementalCollect")
      .internal()
      .doc("When true, enable incremental collection for execution in Thrift Server.")
      .booleanConf
      .createWithDefault(false)

  val THRIFTSERVER_UI_STATEMENT_LIMIT =
    buildConf("spark.sql.thriftserver.ui.retainedStatements")
      .doc("The number of SQL statements kept in the JDBC/ODBC web UI history.")
      .intConf
      .createWithDefault(200)

  val THRIFTSERVER_UI_SESSION_LIMIT = buildConf("spark.sql.thriftserver.ui.retainedSessions")
    .doc("The number of SQL client sessions kept in the JDBC/ODBC web UI history.")
    .intConf
    .createWithDefault(200)

  // This is used to set the default data source
  val DEFAULT_DATA_SOURCE_NAME = buildConf("spark.sql.sources.default")
    .doc("The default data source to use in input/output.")
    .stringConf
    .createWithDefault("parquet")

  val CONVERT_CTAS = buildConf("spark.sql.hive.convertCTAS")
    .internal()
    .doc("When true, a table created by a Hive CTAS statement (no USING clause) " +
      "without specifying any storage property will be converted to a data source table, " +
      s"using the data source set by ${DEFAULT_DATA_SOURCE_NAME.key}.")
    .booleanConf
    .createWithDefault(false)

  val GATHER_FASTSTAT = buildConf("spark.sql.hive.gatherFastStats")
      .internal()
      .doc("When true, fast stats (number of files and total size of all files) will be gathered" +
        " in parallel while repairing table partitions to avoid the sequential listing in Hive" +
        " metastore.")
      .booleanConf
      .createWithDefault(true)

  val PARTITION_COLUMN_TYPE_INFERENCE =
    buildConf("spark.sql.sources.partitionColumnTypeInference.enabled")
      .doc("When true, automatically infer the data types for partitioned columns.")
      .booleanConf
      .createWithDefault(true)

  val BUCKETING_ENABLED = buildConf("spark.sql.sources.bucketing.enabled")
    .doc("When false, we will treat bucketed table as normal table")
    .booleanConf
    .createWithDefault(true)

  val BUCKETING_MAX_BUCKETS = buildConf("spark.sql.sources.bucketing.maxBuckets")
    .doc("The maximum number of buckets allowed. Defaults to 100000")
    .intConf
    .checkValue(_ > 0, "the value of spark.sql.sources.bucketing.maxBuckets must be greater than 0")
    .createWithDefault(100000)

  val CROSS_JOINS_ENABLED = buildConf("spark.sql.crossJoin.enabled")
    .doc("When false, we will throw an error if a query contains a cartesian product without " +
        "explicit CROSS JOIN syntax.")
    .booleanConf
    .createWithDefault(false)

  val ORDER_BY_ORDINAL = buildConf("spark.sql.orderByOrdinal")
    .doc("When true, the ordinal numbers are treated as the position in the select list. " +
         "When false, the ordinal numbers in order/sort by clause are ignored.")
    .booleanConf
    .createWithDefault(true)

  val GROUP_BY_ORDINAL = buildConf("spark.sql.groupByOrdinal")
    .doc("When true, the ordinal numbers in group by clauses are treated as the position " +
      "in the select list. When false, the ordinal numbers are ignored.")
    .booleanConf
    .createWithDefault(true)

  val GROUP_BY_ALIASES = buildConf("spark.sql.groupByAliases")
    .doc("When true, aliases in a select list can be used in group by clauses. When false, " +
      "an analysis exception is thrown in the case.")
    .booleanConf
    .createWithDefault(true)

  // The output committer class used by data sources. The specified class needs to be a
  // subclass of org.apache.hadoop.mapreduce.OutputCommitter.
  val OUTPUT_COMMITTER_CLASS = buildConf("spark.sql.sources.outputCommitterClass")
    .internal()
    .stringConf
    .createOptional

  val FILE_COMMIT_PROTOCOL_CLASS =
    buildConf("spark.sql.sources.commitProtocolClass")
      .internal()
      .stringConf
      .createWithDefault(
        "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")

  val PARALLEL_PARTITION_DISCOVERY_THRESHOLD =
    buildConf("spark.sql.sources.parallelPartitionDiscovery.threshold")
      .doc("The maximum number of paths allowed for listing files at driver side. If the number " +
        "of detected paths exceeds this value during partition discovery, it tries to list the " +
        "files with another Spark distributed job. This applies to Parquet, ORC, CSV, JSON and " +
        "LibSVM data sources.")
      .intConf
      .checkValue(parallel => parallel >= 0, "The maximum number of paths allowed for listing " +
        "files at driver side must not be negative")
      .createWithDefault(32)

  val PARALLEL_PARTITION_DISCOVERY_PARALLELISM =
    buildConf("spark.sql.sources.parallelPartitionDiscovery.parallelism")
      .doc("The number of parallelism to list a collection of path recursively, Set the " +
        "number to prevent file listing from generating too many tasks.")
      .internal()
      .intConf
      .createWithDefault(10000)

  // Whether to automatically resolve ambiguity in join conditions for self-joins.
  // See SPARK-6231.
  val DATAFRAME_SELF_JOIN_AUTO_RESOLVE_AMBIGUITY =
    buildConf("spark.sql.selfJoinAutoResolveAmbiguity")
      .internal()
      .booleanConf
      .createWithDefault(true)

  // Whether to retain group by columns or not in GroupedData.agg.
  val DATAFRAME_RETAIN_GROUP_COLUMNS = buildConf("spark.sql.retainGroupColumns")
    .internal()
    .booleanConf
    .createWithDefault(true)

  val DATAFRAME_PIVOT_MAX_VALUES = buildConf("spark.sql.pivotMaxValues")
    .doc("When doing a pivot without specifying values for the pivot column this is the maximum " +
      "number of (distinct) values that will be collected without error.")
    .intConf
    .createWithDefault(10000)

  val RUN_SQL_ON_FILES = buildConf("spark.sql.runSQLOnFiles")
    .internal()
    .doc("When true, we could use `datasource`.`path` as table in SQL query.")
    .booleanConf
    .createWithDefault(true)

  val WHOLESTAGE_CODEGEN_ENABLED = buildConf("spark.sql.codegen.wholeStage")
    .internal()
    .doc("When true, the whole stage (of multiple operators) will be compiled into single java" +
      " method.")
    .booleanConf
    .createWithDefault(true)

  val WHOLESTAGE_CODEGEN_USE_ID_IN_CLASS_NAME =
    buildConf("spark.sql.codegen.useIdInClassName")
    .internal()
    .doc("When true, embed the (whole-stage) codegen stage ID into " +
      "the class name of the generated class as a suffix")
    .booleanConf
    .createWithDefault(true)

  val WHOLESTAGE_MAX_NUM_FIELDS = buildConf("spark.sql.codegen.maxFields")
    .internal()
    .doc("The maximum number of fields (including nested fields) that will be supported before" +
      " deactivating whole-stage codegen.")
    .intConf
    .createWithDefault(100)

  val CODEGEN_FACTORY_MODE = buildConf("spark.sql.codegen.factoryMode")
    .doc("This config determines the fallback behavior of several codegen generators " +
      "during tests. `FALLBACK` means trying codegen first and then fallbacking to " +
      "interpreted if any compile error happens. Disabling fallback if `CODEGEN_ONLY`. " +
      "`NO_CODEGEN` skips codegen and goes interpreted path always. Note that " +
      "this config works only for tests.")
    .internal()
    .stringConf
    .checkValues(CodegenObjectFactoryMode.values.map(_.toString))
    .createWithDefault(CodegenObjectFactoryMode.FALLBACK.toString)

  val CODEGEN_FALLBACK = buildConf("spark.sql.codegen.fallback")
    .internal()
    .doc("When true, (whole stage) codegen could be temporary disabled for the part of query that" +
      " fail to compile generated code")
    .booleanConf
    .createWithDefault(true)

  val CODEGEN_LOGGING_MAX_LINES = buildConf("spark.sql.codegen.logging.maxLines")
    .internal()
    .doc("The maximum number of codegen lines to log when errors occur. Use -1 for unlimited.")
    .intConf
    .checkValue(maxLines => maxLines >= -1, "The maximum must be a positive integer, 0 to " +
      "disable logging or -1 to apply no limit.")
    .createWithDefault(1000)

  val WHOLESTAGE_HUGE_METHOD_LIMIT = buildConf("spark.sql.codegen.hugeMethodLimit")
    .internal()
    .doc("The maximum bytecode size of a single compiled Java function generated by whole-stage " +
      "codegen. When the compiled function exceeds this threshold, the whole-stage codegen is " +
      "deactivated for this subtree of the current query plan. The default value is 65535, which " +
      "is the largest bytecode size possible for a valid Java method. When running on HotSpot, " +
      s"it may be preferable to set the value to ${CodeGenerator.DEFAULT_JVM_HUGE_METHOD_LIMIT} " +
      "to match HotSpot's implementation.")
    .intConf
    .createWithDefault(65535)

  val CODEGEN_METHOD_SPLIT_THRESHOLD = buildConf("spark.sql.codegen.methodSplitThreshold")
    .internal()
    .doc("The threshold of source-code splitting in the codegen. When the number of characters " +
      "in a single Java function (without comment) exceeds the threshold, the function will be " +
      "automatically split to multiple smaller ones. We cannot know how many bytecode will be " +
      "generated, so use the code length as metric. When running on HotSpot, a function's " +
      "bytecode should not go beyond 8KB, otherwise it will not be JITted; it also should not " +
      "be too small, otherwise there will be many function calls.")
    .intConf
    .checkValue(threshold => threshold > 0, "The threshold must be a positive integer.")
    .createWithDefault(1024)

  val WHOLESTAGE_SPLIT_CONSUME_FUNC_BY_OPERATOR =
    buildConf("spark.sql.codegen.splitConsumeFuncByOperator")
      .internal()
      .doc("When true, whole stage codegen would put the logic of consuming rows of each " +
        "physical operator into individual methods, instead of a single big method. This can be " +
        "used to avoid oversized function that can miss the opportunity of JIT optimization.")
      .booleanConf
      .createWithDefault(true)

  val FILES_MAX_PARTITION_BYTES = buildConf("spark.sql.files.maxPartitionBytes")
    .doc("The maximum number of bytes to pack into a single partition when reading files.")
    .bytesConf(ByteUnit.BYTE)
    .createWithDefault(128 * 1024 * 1024) // parquet.block.size

  val FILES_OPEN_COST_IN_BYTES = buildConf("spark.sql.files.openCostInBytes")
    .internal()
    .doc("The estimated cost to open a file, measured by the number of bytes could be scanned in" +
      " the same time. This is used when putting multiple files into a partition. It's better to" +
      " over estimated, then the partitions with small files will be faster than partitions with" +
      " bigger files (which is scheduled first).")
    .longConf
    .createWithDefault(4 * 1024 * 1024)

  val IGNORE_CORRUPT_FILES = buildConf("spark.sql.files.ignoreCorruptFiles")
    .doc("Whether to ignore corrupt files. If true, the Spark jobs will continue to run when " +
      "encountering corrupted files and the contents that have been read will still be returned.")
    .booleanConf
    .createWithDefault(false)

  val IGNORE_MISSING_FILES = buildConf("spark.sql.files.ignoreMissingFiles")
    .doc("Whether to ignore missing files. If true, the Spark jobs will continue to run when " +
      "encountering missing files and the contents that have been read will still be returned.")
    .booleanConf
    .createWithDefault(false)

  val MAX_RECORDS_PER_FILE = buildConf("spark.sql.files.maxRecordsPerFile")
    .doc("Maximum number of records to write out to a single file. " +
      "If this value is zero or negative, there is no limit.")
    .longConf
    .createWithDefault(0)

  val EXCHANGE_REUSE_ENABLED = buildConf("spark.sql.exchange.reuse")
    .internal()
    .doc("When true, the planner will try to find out duplicated exchanges and re-use them.")
    .booleanConf
    .createWithDefault(true)

  val SUBQUERY_REUSE_ENABLED = buildConf("spark.sql.subquery.reuse")
    .internal()
    .doc("When true, the planner will try to find out duplicated subqueries and re-use them.")
    .booleanConf
    .createWithDefault(true)

  val STATE_STORE_PROVIDER_CLASS =
    buildConf("spark.sql.streaming.stateStore.providerClass")
      .internal()
      .doc(
        "The class used to manage state data in stateful streaming queries. This class must " +
          "be a subclass of StateStoreProvider, and must have a zero-arg constructor. " +
          "Note: For structured streaming, this configuration cannot be changed between query " +
          "restarts from the same checkpoint location.")
      .stringConf
      .createWithDefault(
        "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider")

  val STATE_STORE_MIN_DELTAS_FOR_SNAPSHOT =
    buildConf("spark.sql.streaming.stateStore.minDeltasForSnapshot")
      .internal()
      .doc("Minimum number of state store delta files that needs to be generated before they " +
        "consolidated into snapshots.")
      .intConf
      .createWithDefault(10)

  val FLATMAPGROUPSWITHSTATE_STATE_FORMAT_VERSION =
    buildConf("spark.sql.streaming.flatMapGroupsWithState.stateFormatVersion")
      .internal()
      .doc("State format version used by flatMapGroupsWithState operation in a streaming query")
      .intConf
      .checkValue(v => Set(1, 2).contains(v), "Valid versions are 1 and 2")
      .createWithDefault(2)

  val CHECKPOINT_LOCATION = buildConf("spark.sql.streaming.checkpointLocation")
    .doc("The default location for storing checkpoint data for streaming queries.")
    .stringConf
    .createOptional

  val FORCE_DELETE_TEMP_CHECKPOINT_LOCATION =
    buildConf("spark.sql.streaming.forceDeleteTempCheckpointLocation")
      .doc("When true, enable temporary checkpoint locations force delete.")
      .booleanConf
      .createWithDefault(false)

  val MIN_BATCHES_TO_RETAIN = buildConf("spark.sql.streaming.minBatchesToRetain")
    .internal()
    .doc("The minimum number of batches that must be retained and made recoverable.")
    .intConf
    .createWithDefault(100)

  val MAX_BATCHES_TO_RETAIN_IN_MEMORY = buildConf("spark.sql.streaming.maxBatchesToRetainInMemory")
    .internal()
    .doc("The maximum number of batches which will be retained in memory to avoid " +
      "loading from files. The value adjusts a trade-off between memory usage vs cache miss: " +
      "'2' covers both success and direct failure cases, '1' covers only success case, " +
      "and '0' covers extreme case - disable cache to maximize memory size of executors.")
    .intConf
    .createWithDefault(2)

  val STREAMING_AGGREGATION_STATE_FORMAT_VERSION =
    buildConf("spark.sql.streaming.aggregation.stateFormatVersion")
      .internal()
      .doc("State format version used by streaming aggregation operations in a streaming query. " +
        "State between versions are tend to be incompatible, so state format version shouldn't " +
        "be modified after running.")
      .intConf
      .checkValue(v => Set(1, 2).contains(v), "Valid versions are 1 and 2")
      .createWithDefault(2)

  val UNSUPPORTED_OPERATION_CHECK_ENABLED =
    buildConf("spark.sql.streaming.unsupportedOperationCheck")
      .internal()
      .doc("When true, the logical plan for streaming query will be checked for unsupported" +
        " operations.")
      .booleanConf
      .createWithDefault(true)

  val VARIABLE_SUBSTITUTE_ENABLED =
    buildConf("spark.sql.variable.substitute")
      .doc("This enables substitution using syntax like ${var} ${system:var} and ${env:var}.")
      .booleanConf
      .createWithDefault(true)

  val VARIABLE_SUBSTITUTE_DEPTH =
    buildConf("spark.sql.variable.substitute.depth")
      .internal()
      .doc("Deprecated: The maximum replacements the substitution engine will do.")
      .intConf
      .createWithDefault(40)

  val ENABLE_TWOLEVEL_AGG_MAP =
    buildConf("spark.sql.codegen.aggregate.map.twolevel.enabled")
      .internal()
      .doc("Enable two-level aggregate hash map. When enabled, records will first be " +
        "inserted/looked-up at a 1st-level, small, fast map, and then fallback to a " +
        "2nd-level, larger, slower map when 1st level is full or keys cannot be found. " +
        "When disabled, records go directly to the 2nd level. Defaults to true.")
      .booleanConf
      .createWithDefault(true)

  val MAX_NESTED_VIEW_DEPTH =
    buildConf("spark.sql.view.maxNestedViewDepth")
      .internal()
      .doc("The maximum depth of a view reference in a nested view. A nested view may reference " +
        "other nested views, the dependencies are organized in a directed acyclic graph (DAG). " +
        "However the DAG depth may become too large and cause unexpected behavior. This " +
        "configuration puts a limit on this: when the depth of a view exceeds this value during " +
        "analysis, we terminate the resolution to avoid potential errors.")
      .intConf
      .checkValue(depth => depth > 0, "The maximum depth of a view reference in a nested view " +
        "must be positive.")
      .createWithDefault(100)

  val STREAMING_FILE_COMMIT_PROTOCOL_CLASS =
    buildConf("spark.sql.streaming.commitProtocolClass")
      .internal()
      .stringConf
      .createWithDefault("org.apache.spark.sql.execution.streaming.ManifestFileCommitProtocol")

  val STREAMING_MULTIPLE_WATERMARK_POLICY =
    buildConf("spark.sql.streaming.multipleWatermarkPolicy")
      .doc("Policy to calculate the global watermark value when there are multiple watermark " +
        "operators in a streaming query. The default value is 'min' which chooses " +
        "the minimum watermark reported across multiple operators. Other alternative value is" +
        "'max' which chooses the maximum across multiple operators." +
        "Note: This configuration cannot be changed between query restarts from the same " +
        "checkpoint location.")
      .stringConf
      .transform(_.toLowerCase(Locale.ROOT))
      .checkValue(
        str => Set("min", "max").contains(str),
        "Invalid value for 'spark.sql.streaming.multipleWatermarkPolicy'. " +
          "Valid values are 'min' and 'max'")
      .createWithDefault("min") // must be same as MultipleWatermarkPolicy.DEFAULT_POLICY_NAME

  val OBJECT_AGG_SORT_BASED_FALLBACK_THRESHOLD =
    buildConf("spark.sql.objectHashAggregate.sortBased.fallbackThreshold")
      .internal()
      .doc("In the case of ObjectHashAggregateExec, when the size of the in-memory hash map " +
        "grows too large, we will fall back to sort-based aggregation. This option sets a row " +
        "count threshold for the size of the hash map.")
      .intConf
      // We are trying to be conservative and use a relatively small default count threshold here
      // since the state object of some TypedImperativeAggregate function can be quite large (e.g.
      // percentile_approx).
      .createWithDefault(128)

  val USE_OBJECT_HASH_AGG = buildConf("spark.sql.execution.useObjectHashAggregateExec")
    .internal()
    .doc("Decides if we use ObjectHashAggregateExec")
    .booleanConf
    .createWithDefault(true)

  val FILE_SINK_LOG_DELETION = buildConf("spark.sql.streaming.fileSink.log.deletion")
    .internal()
    .doc("Whether to delete the expired log files in file stream sink.")
    .booleanConf
    .createWithDefault(true)

  val FILE_SINK_LOG_COMPACT_INTERVAL =
    buildConf("spark.sql.streaming.fileSink.log.compactInterval")
      .internal()
      .doc("Number of log files after which all the previous files " +
        "are compacted into the next log file.")
      .intConf
      .createWithDefault(10)

  val FILE_SINK_LOG_CLEANUP_DELAY =
    buildConf("spark.sql.streaming.fileSink.log.cleanupDelay")
      .internal()
      .doc("How long that a file is guaranteed to be visible for all readers.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefault(TimeUnit.MINUTES.toMillis(10)) // 10 minutes

  val FILE_SOURCE_LOG_DELETION = buildConf("spark.sql.streaming.fileSource.log.deletion")
    .internal()
    .doc("Whether to delete the expired log files in file stream source.")
    .booleanConf
    .createWithDefault(true)

  val FILE_SOURCE_LOG_COMPACT_INTERVAL =
    buildConf("spark.sql.streaming.fileSource.log.compactInterval")
      .internal()
      .doc("Number of log files after which all the previous files " +
        "are compacted into the next log file.")
      .intConf
      .createWithDefault(10)

  val FILE_SOURCE_LOG_CLEANUP_DELAY =
    buildConf("spark.sql.streaming.fileSource.log.cleanupDelay")
      .internal()
      .doc("How long in milliseconds a file is guaranteed to be visible for all readers.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefault(TimeUnit.MINUTES.toMillis(10)) // 10 minutes

  val STREAMING_SCHEMA_INFERENCE =
    buildConf("spark.sql.streaming.schemaInference")
      .internal()
      .doc("Whether file-based streaming sources will infer its own schema")
      .booleanConf
      .createWithDefault(false)

  val STREAMING_POLLING_DELAY =
    buildConf("spark.sql.streaming.pollingDelay")
      .internal()
      .doc("How long to delay polling new data when no data is available")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefault(10L)

  val STREAMING_NO_DATA_PROGRESS_EVENT_INTERVAL =
    buildConf("spark.sql.streaming.noDataProgressEventInterval")
      .internal()
      .doc("How long to wait between two progress events when there is no data")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefault(10000L)

  val STREAMING_NO_DATA_MICRO_BATCHES_ENABLED =
    buildConf("spark.sql.streaming.noDataMicroBatches.enabled")
      .doc(
        "Whether streaming micro-batch engine will execute batches without data " +
          "for eager state management for stateful streaming queries.")
      .booleanConf
      .createWithDefault(true)

  val STREAMING_METRICS_ENABLED =
    buildConf("spark.sql.streaming.metricsEnabled")
      .doc("Whether Dropwizard/Codahale metrics will be reported for active streaming queries.")
      .booleanConf
      .createWithDefault(false)

  val STREAMING_PROGRESS_RETENTION =
    buildConf("spark.sql.streaming.numRecentProgressUpdates")
      .doc("The number of progress updates to retain for a streaming query")
      .intConf
      .createWithDefault(100)

  val STREAMING_CHECKPOINT_FILE_MANAGER_CLASS =
    buildConf("spark.sql.streaming.checkpointFileManagerClass")
      .doc("The class used to write checkpoint files atomically. This class must be a subclass " +
        "of the interface CheckpointFileManager.")
      .internal()
      .stringConf

  val STREAMING_CHECKPOINT_ESCAPED_PATH_CHECK_ENABLED =
    buildConf("spark.sql.streaming.checkpoint.escapedPathCheck.enabled")
      .doc("Whether to detect a streaming query may pick up an incorrect checkpoint path due " +
        "to SPARK-26824.")
      .internal()
      .booleanConf
      .createWithDefault(true)

  val PARALLEL_FILE_LISTING_IN_STATS_COMPUTATION =
    buildConf("spark.sql.statistics.parallelFileListingInStatsComputation.enabled")
      .internal()
      .doc("When true, SQL commands use parallel file listing, " +
        "as opposed to single thread listing." +
        "This usually speeds up commands that need to list many directories.")
      .booleanConf
      .createWithDefault(true)

  val ENABLE_FALL_BACK_TO_HDFS_FOR_STATS = buildConf("spark.sql.statistics.fallBackToHdfs")
    .doc("If the table statistics are not available from table metadata enable fall back to hdfs." +
      " This is useful in determining if a table is small enough to use auto broadcast joins.")
    .booleanConf
    .createWithDefault(false)

  val DEFAULT_SIZE_IN_BYTES = buildConf("spark.sql.defaultSizeInBytes")
    .internal()
    .doc("The default table size used in query planning. By default, it is set to Long.MaxValue " +
      s"which is larger than `${AUTO_BROADCASTJOIN_THRESHOLD.key}` to be more conservative. " +
      "That is to say by default the optimizer will not choose to broadcast a table unless it " +
      "knows for sure its size is small enough.")
    .bytesConf(ByteUnit.BYTE)
    .createWithDefault(Long.MaxValue)

  val NDV_MAX_ERROR =
    buildConf("spark.sql.statistics.ndv.maxError")
      .internal()
      .doc("The maximum estimation error allowed in HyperLogLog++ algorithm when generating " +
        "column level statistics.")
      .doubleConf
      .createWithDefault(0.05)

  val HISTOGRAM_ENABLED =
    buildConf("spark.sql.statistics.histogram.enabled")
      .doc("Generates histograms when computing column statistics if enabled. Histograms can " +
        "provide better estimation accuracy. Currently, Spark only supports equi-height " +
        "histogram. Note that collecting histograms takes extra cost. For example, collecting " +
        "column statistics usually takes only one table scan, but generating equi-height " +
        "histogram will cause an extra table scan.")
      .booleanConf
      .createWithDefault(false)

  val HISTOGRAM_NUM_BINS =
    buildConf("spark.sql.statistics.histogram.numBins")
      .internal()
      .doc("The number of bins when generating histograms.")
      .intConf
      .checkValue(num => num > 1, "The number of bins must be greater than 1.")
      .createWithDefault(254)

  val PERCENTILE_ACCURACY =
    buildConf("spark.sql.statistics.percentile.accuracy")
      .internal()
      .doc("Accuracy of percentile approximation when generating equi-height histograms. " +
        "Larger value means better accuracy. The relative error can be deduced by " +
        "1.0 / PERCENTILE_ACCURACY.")
      .intConf
      .createWithDefault(10000)

  val AUTO_SIZE_UPDATE_ENABLED =
    buildConf("spark.sql.statistics.size.autoUpdate.enabled")
      .doc("Enables automatic update for table size once table's data is changed. Note that if " +
        "the total number of files of the table is very large, this can be expensive and slow " +
        "down data change commands.")
      .booleanConf
      .createWithDefault(false)

  val CBO_ENABLED =
    buildConf("spark.sql.cbo.enabled")
      .doc("Enables CBO for estimation of plan statistics when set true.")
      .booleanConf
      .createWithDefault(false)

  val JOIN_REORDER_ENABLED =
    buildConf("spark.sql.cbo.joinReorder.enabled")
      .doc("Enables join reorder in CBO.")
      .booleanConf
      .createWithDefault(false)

  val JOIN_REORDER_DP_THRESHOLD =
    buildConf("spark.sql.cbo.joinReorder.dp.threshold")
      .doc("The maximum number of joined nodes allowed in the dynamic programming algorithm.")
      .intConf
      .checkValue(number => number > 0, "The maximum number must be a positive integer.")
      .createWithDefault(12)

  val JOIN_REORDER_CARD_WEIGHT =
    buildConf("spark.sql.cbo.joinReorder.card.weight")
      .internal()
      .doc("The weight of cardinality (number of rows) for plan cost comparison in join reorder: " +
        "rows * weight + size * (1 - weight).")
      .doubleConf
      .checkValue(weight => weight >= 0 && weight <= 1, "The weight value must be in [0, 1].")
      .createWithDefault(0.7)

  val JOIN_REORDER_DP_STAR_FILTER =
    buildConf("spark.sql.cbo.joinReorder.dp.star.filter")
      .doc("Applies star-join filter heuristics to cost based join enumeration.")
      .booleanConf
      .createWithDefault(false)

  val STARSCHEMA_DETECTION = buildConf("spark.sql.cbo.starSchemaDetection")
    .doc("When true, it enables join reordering based on star schema detection. ")
    .booleanConf
    .createWithDefault(false)

  val STARSCHEMA_FACT_TABLE_RATIO = buildConf("spark.sql.cbo.starJoinFTRatio")
    .internal()
    .doc("Specifies the upper limit of the ratio between the largest fact tables" +
      " for a star join to be considered. ")
    .doubleConf
    .createWithDefault(0.9)

  val SESSION_LOCAL_TIMEZONE =
    buildConf("spark.sql.session.timeZone")
      .doc("""The ID of session local timezone, e.g. "GMT", "America/Los_Angeles", etc.""")
      .stringConf
      .createWithDefaultFunction(() => TimeZone.getDefault.getID)

  val WINDOW_EXEC_BUFFER_IN_MEMORY_THRESHOLD =
    buildConf("spark.sql.windowExec.buffer.in.memory.threshold")
      .internal()
      .doc("Threshold for number of rows guaranteed to be held in memory by the window operator")
      .intConf
      .createWithDefault(4096)

  val WINDOW_EXEC_BUFFER_SPILL_THRESHOLD =
    buildConf("spark.sql.windowExec.buffer.spill.threshold")
      .internal()
      .doc("Threshold for number of rows to be spilled by window operator")
      .intConf
      .createWithDefault(SHUFFLE_SPILL_NUM_ELEMENTS_FORCE_SPILL_THRESHOLD.defaultValue.get)

  val SORT_MERGE_JOIN_EXEC_BUFFER_IN_MEMORY_THRESHOLD =
    buildConf("spark.sql.sortMergeJoinExec.buffer.in.memory.threshold")
      .internal()
      .doc("Threshold for number of rows guaranteed to be held in memory by the sort merge " +
        "join operator")
      .intConf
      .createWithDefault(ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH)

  val SORT_MERGE_JOIN_EXEC_BUFFER_SPILL_THRESHOLD =
    buildConf("spark.sql.sortMergeJoinExec.buffer.spill.threshold")
      .internal()
      .doc("Threshold for number of rows to be spilled by sort merge join operator")
      .intConf
      .createWithDefault(SHUFFLE_SPILL_NUM_ELEMENTS_FORCE_SPILL_THRESHOLD.defaultValue.get)

  val CARTESIAN_PRODUCT_EXEC_BUFFER_IN_MEMORY_THRESHOLD =
    buildConf("spark.sql.cartesianProductExec.buffer.in.memory.threshold")
      .internal()
      .doc("Threshold for number of rows guaranteed to be held in memory by the cartesian " +
        "product operator")
      .intConf
      .createWithDefault(4096)

  val CARTESIAN_PRODUCT_EXEC_BUFFER_SPILL_THRESHOLD =
    buildConf("spark.sql.cartesianProductExec.buffer.spill.threshold")
      .internal()
      .doc("Threshold for number of rows to be spilled by cartesian product operator")
      .intConf
      .createWithDefault(SHUFFLE_SPILL_NUM_ELEMENTS_FORCE_SPILL_THRESHOLD.defaultValue.get)

  val SUPPORT_QUOTED_REGEX_COLUMN_NAME = buildConf("spark.sql.parser.quotedRegexColumnNames")
    .doc("When true, quoted Identifiers (using backticks) in SELECT statement are interpreted" +
      " as regular expressions.")
    .booleanConf
    .createWithDefault(false)

  val RANGE_EXCHANGE_SAMPLE_SIZE_PER_PARTITION =
    buildConf("spark.sql.execution.rangeExchange.sampleSizePerPartition")
      .internal()
      .doc("Number of points to sample per partition in order to determine the range boundaries" +
          " for range partitioning, typically used in global sorting (without limit).")
      .intConf
      .createWithDefault(100)

  val ARROW_EXECUTION_ENABLED =
    buildConf("spark.sql.execution.arrow.enabled")
      .doc("When true, make use of Apache Arrow for columnar data transfers." +
        "In case of PySpark, " +
        "1. pyspark.sql.DataFrame.toPandas " +
        "2. pyspark.sql.SparkSession.createDataFrame when its input is a Pandas DataFrame " +
        "The following data types are unsupported: " +
        "BinaryType, MapType, ArrayType of TimestampType, and nested StructType." +

        "In case of SparkR," +
        "1. createDataFrame when its input is an R DataFrame " +
        "2. collect " +
        "3. dapply " +
        "4. gapply " +
        "The following data types are unsupported: " +
        "FloatType, BinaryType, ArrayType, StructType and MapType.")
      .booleanConf
      .createWithDefault(false)

  val ARROW_FALLBACK_ENABLED =
    buildConf("spark.sql.execution.arrow.fallback.enabled")
      .doc(s"When true, optimizations enabled by '${ARROW_EXECUTION_ENABLED.key}' will " +
        "fallback automatically to non-optimized implementations if an error occurs.")
      .booleanConf
      .createWithDefault(true)

  val ARROW_EXECUTION_MAX_RECORDS_PER_BATCH =
    buildConf("spark.sql.execution.arrow.maxRecordsPerBatch")
      .doc("When using Apache Arrow, limit the maximum number of records that can be written " +
        "to a single ArrowRecordBatch in memory. If set to zero or negative there is no limit.")
      .intConf
      .createWithDefault(10000)

  val PANDAS_RESPECT_SESSION_LOCAL_TIMEZONE =
    buildConf("spark.sql.execution.pandas.respectSessionTimeZone")
      .internal()
      .doc("When true, make Pandas DataFrame with timestamp type respecting session local " +
        "timezone when converting to/from Pandas DataFrame. This configuration will be " +
        "deprecated in the future releases.")
      .booleanConf
      .createWithDefault(true)

  val PANDAS_GROUPED_MAP_ASSIGN_COLUMNS_BY_NAME =
    buildConf("spark.sql.legacy.execution.pandas.groupedMap.assignColumnsByName")
      .internal()
      .doc("When true, columns will be looked up by name if labeled with a string and fallback " +
        "to use position if not. When false, a grouped map Pandas UDF will assign columns from " +
        "the returned Pandas DataFrame based on position, regardless of column label type. " +
        "This configuration will be deprecated in future releases.")
      .booleanConf
      .createWithDefault(true)

  val PANDAS_ARROW_SAFE_TYPE_CONVERSION =
    buildConf("spark.sql.execution.pandas.arrowSafeTypeConversion")
      .internal()
      .doc("When true, Arrow will perform safe type conversion when converting " +
        "Pandas.Series to Arrow array during serialization. Arrow will raise errors " +
        "when detecting unsafe type conversion like overflow. When false, disabling Arrow's type " +
        "check and do type conversions anyway. This config only works for Arrow 0.11.0+.")
      .booleanConf
      .createWithDefault(false)

  val REPLACE_EXCEPT_WITH_FILTER = buildConf("spark.sql.optimizer.replaceExceptWithFilter")
    .internal()
    .doc("When true, the apply function of the rule verifies whether the right node of the" +
      " except operation is of type Filter or Project followed by Filter. If yes, the rule" +
      " further verifies 1) Excluding the filter operations from the right (as well as the" +
      " left node, if any) on the top, whether both the nodes evaluates to a same result." +
      " 2) The left and right nodes don't contain any SubqueryExpressions. 3) The output" +
      " column names of the left node are distinct. If all the conditions are met, the" +
      " rule will replace the except operation with a Filter by flipping the filter" +
      " condition(s) of the right node.")
    .booleanConf
    .createWithDefault(true)

  val DECIMAL_OPERATIONS_ALLOW_PREC_LOSS =
    buildConf("spark.sql.decimalOperations.allowPrecisionLoss")
      .internal()
      .doc("When true (default), establishing the result type of an arithmetic operation " +
        "happens according to Hive behavior and SQL ANSI 2011 specification, ie. rounding the " +
        "decimal part of the result if an exact representation is not possible. Otherwise, NULL " +
        "is returned in those cases, as previously.")
      .booleanConf
      .createWithDefault(true)

  val LITERAL_PICK_MINIMUM_PRECISION =
    buildConf("spark.sql.legacy.literal.pickMinimumPrecision")
      .internal()
      .doc("When integral literal is used in decimal operations, pick a minimum precision " +
        "required by the literal if this config is true, to make the resulting precision and/or " +
        "scale smaller. This can reduce the possibility of precision lose and/or overflow.")
      .booleanConf
      .createWithDefault(true)

  val SQL_OPTIONS_REDACTION_PATTERN =
    buildConf("spark.sql.redaction.options.regex")
      .doc("Regex to decide which keys in a Spark SQL command's options map contain sensitive " +
        "information. The values of options whose names that match this regex will be redacted " +
        "in the explain output. This redaction is applied on top of the global redaction " +
        s"configuration defined by ${SECRET_REDACTION_PATTERN.key}.")
    .regexConf
    .createWithDefault("(?i)url".r)

  val SQL_STRING_REDACTION_PATTERN =
    buildConf("spark.sql.redaction.string.regex")
      .doc("Regex to decide which parts of strings produced by Spark contain sensitive " +
        "information. When this regex matches a string part, that string part is replaced by a " +
        "dummy value. This is currently used to redact the output of SQL explain commands. " +
        "When this conf is not set, the value from `spark.redaction.string.regex` is used.")
      .fallbackConf(org.apache.spark.internal.config.STRING_REDACTION_PATTERN)

  val CONCAT_BINARY_AS_STRING = buildConf("spark.sql.function.concatBinaryAsString")
    .doc("When this option is set to false and all inputs are binary, `functions.concat` returns " +
      "an output as binary. Otherwise, it returns as a string. ")
    .booleanConf
    .createWithDefault(false)

  val ELT_OUTPUT_AS_STRING = buildConf("spark.sql.function.eltOutputAsString")
    .doc("When this option is set to false and all inputs are binary, `elt` returns " +
      "an output as binary. Otherwise, it returns as a string. ")
    .booleanConf
    .createWithDefault(false)

  val ALLOW_CREATING_MANAGED_TABLE_USING_NONEMPTY_LOCATION =
    buildConf("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation")
    .internal()
    .doc("When this option is set to true, creating managed tables with nonempty location " +
      "is allowed. Otherwise, an analysis exception is thrown. ")
    .booleanConf
    .createWithDefault(false)

  val VALIDATE_PARTITION_COLUMNS =
    buildConf("spark.sql.sources.validatePartitionColumns")
      .internal()
      .doc("When this option is set to true, partition column values will be validated with " +
        "user-specified schema. If the validation fails, a runtime exception is thrown." +
        "When this option is set to false, the partition column value will be converted to null " +
        "if it can not be casted to corresponding user-specified schema.")
      .booleanConf
      .createWithDefault(true)

  val CONTINUOUS_STREAMING_EPOCH_BACKLOG_QUEUE_SIZE =
    buildConf("spark.sql.streaming.continuous.epochBacklogQueueSize")
      .doc("The max number of entries to be stored in queue to wait for late epochs. " +
        "If this parameter is exceeded by the size of the queue, stream will stop with an error.")
      .intConf
      .createWithDefault(10000)

  val CONTINUOUS_STREAMING_EXECUTOR_QUEUE_SIZE =
    buildConf("spark.sql.streaming.continuous.executorQueueSize")
    .internal()
    .doc("The size (measured in number of rows) of the queue used in continuous execution to" +
      " buffer the results of a ContinuousDataReader.")
    .intConf
    .createWithDefault(1024)

  val CONTINUOUS_STREAMING_EXECUTOR_POLL_INTERVAL_MS =
    buildConf("spark.sql.streaming.continuous.executorPollIntervalMs")
      .internal()
      .doc("The interval at which continuous execution readers will poll to check whether" +
        " the epoch has advanced on the driver.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefault(100)

  val USE_V1_SOURCE_READER_LIST = buildConf("spark.sql.sources.read.useV1SourceList")
    .internal()
    .doc("A comma-separated list of data source short names or fully qualified data source" +
      " register class names for which data source V2 read paths are disabled. Reads from these" +
      " sources will fall back to the V1 sources.")
    .stringConf
    .createWithDefault("")

  val USE_V1_SOURCE_WRITER_LIST = buildConf("spark.sql.sources.write.useV1SourceList")
    .internal()
    .doc("A comma-separated list of data source short names or fully qualified data source" +
      " register class names for which data source V2 write paths are disabled. Writes from these" +
      " sources will fall back to the V1 sources.")
    .stringConf
    .createWithDefault("csv,json,orc,text")

  val DISABLED_V2_STREAMING_WRITERS = buildConf("spark.sql.streaming.disabledV2Writers")
    .doc("A comma-separated list of fully qualified data source register class names for which" +
      " StreamWriteSupport is disabled. Writes to these sources will fall back to the V1 Sinks.")
    .stringConf
    .createWithDefault("")

  val DISABLED_V2_STREAMING_MICROBATCH_READERS =
    buildConf("spark.sql.streaming.disabledV2MicroBatchReaders")
      .internal()
      .doc(
        "A comma-separated list of fully qualified data source register class names for which " +
          "MicroBatchReadSupport is disabled. Reads from these sources will fall back to the " +
          "V1 Sources.")
      .stringConf
      .createWithDefault("")

  object PartitionOverwriteMode extends Enumeration {
    val STATIC, DYNAMIC = Value
  }

  val PARTITION_OVERWRITE_MODE =
    buildConf("spark.sql.sources.partitionOverwriteMode")
      .doc("When INSERT OVERWRITE a partitioned data source table, we currently support 2 modes: " +
        "static and dynamic. In static mode, Spark deletes all the partitions that match the " +
        "partition specification(e.g. PARTITION(a=1,b)) in the INSERT statement, before " +
        "overwriting. In dynamic mode, Spark doesn't delete partitions ahead, and only overwrite " +
        "those partitions that have data written into it at runtime. By default we use static " +
        "mode to keep the same behavior of Spark prior to 2.3. Note that this config doesn't " +
        "affect Hive serde tables, as they are always overwritten with dynamic mode. This can " +
        "also be set as an output option for a data source using key partitionOverwriteMode " +
        "(which takes precedence over this setting), e.g. " +
        "dataframe.write.option(\"partitionOverwriteMode\", \"dynamic\").save(path)."
      )
      .stringConf
      .transform(_.toUpperCase(Locale.ROOT))
      .checkValues(PartitionOverwriteMode.values.map(_.toString))
      .createWithDefault(PartitionOverwriteMode.STATIC.toString)

  val SORT_BEFORE_REPARTITION =
    buildConf("spark.sql.execution.sortBeforeRepartition")
      .internal()
      .doc("When perform a repartition following a shuffle, the output row ordering would be " +
        "nondeterministic. If some downstream stages fail and some tasks of the repartition " +
        "stage retry, these tasks may generate different data, and that can lead to correctness " +
        "issues. Turn on this config to insert a local sort before actually doing repartition " +
        "to generate consistent repartition results. The performance of repartition() may go " +
        "down since we insert extra local sort before it.")
        .booleanConf
        .createWithDefault(true)

  val NESTED_SCHEMA_PRUNING_ENABLED =
    buildConf("spark.sql.optimizer.nestedSchemaPruning.enabled")
      .internal()
      .doc("Prune nested fields from a logical relation's output which are unnecessary in " +
        "satisfying a query. This optimization allows columnar file format readers to avoid " +
        "reading unnecessary nested column data. Currently Parquet and ORC are the " +
        "data sources that implement this optimization.")
      .booleanConf
      .createWithDefault(false)

  val SERIALIZER_NESTED_SCHEMA_PRUNING_ENABLED =
    buildConf("spark.sql.optimizer.serializer.nestedSchemaPruning.enabled")
      .internal()
      .doc("Prune nested fields from object serialization operator which are unnecessary in " +
        "satisfying a query. This optimization allows object serializers to avoid " +
        "executing unnecessary nested expressions.")
      .booleanConf
      .createWithDefault(false)

  val TOP_K_SORT_FALLBACK_THRESHOLD =
    buildConf("spark.sql.execution.topKSortFallbackThreshold")
      .internal()
      .doc("In SQL queries with a SORT followed by a LIMIT like " +
          "'SELECT x FROM t ORDER BY y LIMIT m', if m is under this threshold, do a top-K sort" +
          " in memory, otherwise do a global sort which spills to disk if necessary.")
      .intConf
      .createWithDefault(ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH)

  object Deprecated {
    val MAPRED_REDUCE_TASKS = "mapred.reduce.tasks"
  }

  object Replaced {
    val MAPREDUCE_JOB_REDUCES = "mapreduce.job.reduces"
  }

  val CSV_PARSER_COLUMN_PRUNING = buildConf("spark.sql.csv.parser.columnPruning.enabled")
    .internal()
    .doc("If it is set to true, column names of the requested schema are passed to CSV parser. " +
      "Other column values can be ignored during parsing even if they are malformed.")
    .booleanConf
    .createWithDefault(true)

  val REPL_EAGER_EVAL_ENABLED = buildConf("spark.sql.repl.eagerEval.enabled")
    .doc("Enables eager evaluation or not. When true, the top K rows of Dataset will be " +
      "displayed if and only if the REPL supports the eager evaluation. Currently, the " +
      "eager evaluation is supported in PySpark and SparkR. In PySpark, for the notebooks like " +
      "Jupyter, the HTML table (generated by _repr_html_) will be returned. For plain Python " +
      "REPL, the returned outputs are formatted like dataframe.show(). In SparkR, the returned " +
      "outputs are showed similar to R data.frame would.")
    .booleanConf
    .createWithDefault(false)

  val REPL_EAGER_EVAL_MAX_NUM_ROWS = buildConf("spark.sql.repl.eagerEval.maxNumRows")
    .doc("The max number of rows that are returned by eager evaluation. This only takes " +
      s"effect when ${REPL_EAGER_EVAL_ENABLED.key} is set to true. The valid range of this " +
      "config is from 0 to (Int.MaxValue - 1), so the invalid config like negative and " +
      "greater than (Int.MaxValue - 1) will be normalized to 0 and (Int.MaxValue - 1).")
    .intConf
    .createWithDefault(20)

  val REPL_EAGER_EVAL_TRUNCATE = buildConf("spark.sql.repl.eagerEval.truncate")
    .doc("The max number of characters for each cell that is returned by eager evaluation. " +
      s"This only takes effect when ${REPL_EAGER_EVAL_ENABLED.key} is set to true.")
    .intConf
    .createWithDefault(20)

  val FAST_HASH_AGGREGATE_MAX_ROWS_CAPACITY_BIT =
    buildConf("spark.sql.codegen.aggregate.fastHashMap.capacityBit")
      .internal()
      .doc("Capacity for the max number of rows to be held in memory " +
        "by the fast hash aggregate product operator. The bit is not for actual value, " +
        "but the actual numBuckets is determined by loadFactor " +
        "(e.g: default bit value 16 , the actual numBuckets is ((1 << 16) / 0.5).")
      .intConf
      .checkValue(bit => bit >= 10 && bit <= 30, "The bit value must be in [10, 30].")
      .createWithDefault(16)

  val AVRO_COMPRESSION_CODEC = buildConf("spark.sql.avro.compression.codec")
    .doc("Compression codec used in writing of AVRO files. Supported codecs: " +
      "uncompressed, deflate, snappy, bzip2 and xz. Default codec is snappy.")
    .stringConf
    .checkValues(Set("uncompressed", "deflate", "snappy", "bzip2", "xz"))
    .createWithDefault("snappy")

  val AVRO_DEFLATE_LEVEL = buildConf("spark.sql.avro.deflate.level")
    .doc("Compression level for the deflate codec used in writing of AVRO files. " +
      "Valid value must be in the range of from 1 to 9 inclusive or -1. " +
      "The default value is -1 which corresponds to 6 level in the current implementation.")
    .intConf
    .checkValues((1 to 9).toSet + Deflater.DEFAULT_COMPRESSION)
    .createWithDefault(Deflater.DEFAULT_COMPRESSION)

  val COMPARE_DATE_TIMESTAMP_IN_TIMESTAMP =
    buildConf("spark.sql.legacy.compareDateTimestampInTimestamp")
      .internal()
      .doc("When true (default), compare Date with Timestamp after converting both sides to " +
        "Timestamp. This behavior is compatible with Hive 2.2 or later. See HIVE-15236. " +
        "When false, restore the behavior prior to Spark 2.4. Compare Date with Timestamp after " +
        "converting both sides to string. This config will be removed in Spark 3.0.")
      .booleanConf
      .createWithDefault(true)

  val LEGACY_SIZE_OF_NULL = buildConf("spark.sql.legacy.sizeOfNull")
    .doc("If it is set to true, size of null returns -1. This behavior was inherited from Hive. " +
      "The size function returns null for null input if the flag is disabled.")
    .booleanConf
    .createWithDefault(true)

  val LEGACY_REPLACE_DATABRICKS_SPARK_AVRO_ENABLED =
    buildConf("spark.sql.legacy.replaceDatabricksSparkAvro.enabled")
      .doc("If it is set to true, the data source provider com.databricks.spark.avro is mapped " +
        "to the built-in but external Avro data source module for backward compatibility.")
      .booleanConf
      .createWithDefault(true)

  val LEGACY_SETOPS_PRECEDENCE_ENABLED =
    buildConf("spark.sql.legacy.setopsPrecedence.enabled")
      .internal()
      .doc("When set to true and the order of evaluation is not specified by parentheses, the " +
        "set operations are performed from left to right as they appear in the query. When set " +
        "to false and order of evaluation is not specified by parentheses, INTERSECT operations " +
        "are performed before any UNION, EXCEPT and MINUS operations.")
      .booleanConf
      .createWithDefault(false)

  val LEGACY_INTEGRALDIVIDE_RETURN_LONG = buildConf("spark.sql.legacy.integralDivide.returnBigint")
    .doc("If it is set to true, the div operator returns always a bigint. This behavior was " +
      "inherited from Hive. Otherwise, the return type is the data type of the operands.")
    .internal()
    .booleanConf
    .createWithDefault(false)

  val LEGACY_HAVING_WITHOUT_GROUP_BY_AS_WHERE =
    buildConf("spark.sql.legacy.parser.havingWithoutGroupByAsWhere")
      .internal()
      .doc("If it is set to true, the parser will treat HAVING without GROUP BY as a normal " +
        "WHERE, which does not follow SQL standard.")
      .booleanConf
      .createWithDefault(false)

  val LEGACY_PASS_PARTITION_BY_AS_OPTIONS =
    buildConf("spark.sql.legacy.sources.write.passPartitionByAsOptions")
      .internal()
      .doc("Whether to pass the partitionBy columns as options in DataFrameWriter. " +
        "Data source V1 now silently drops partitionBy columns for non-file-format sources; " +
        "turning the flag on provides a way for these sources to see these partitionBy columns.")
      .booleanConf
      .createWithDefault(false)

  val NAME_NON_STRUCT_GROUPING_KEY_AS_VALUE =
    buildConf("spark.sql.legacy.dataset.nameNonStructGroupingKeyAsValue")
      .internal()
      .doc("When set to true, the key attribute resulted from running `Dataset.groupByKey` " +
        "for non-struct key type, will be named as `value`, following the behavior of Spark " +
        "version 2.4 and earlier.")
      .booleanConf
      .createWithDefault(false)

  val MAX_TO_STRING_FIELDS = buildConf("spark.sql.debug.maxToStringFields")
    .doc("Maximum number of fields of sequence-like entries can be converted to strings " +
      "in debug output. Any elements beyond the limit will be dropped and replaced by a" +
      """ "... N more fields" placeholder.""")
    .intConf
    .createWithDefault(25)

  val MAX_PLAN_STRING_LENGTH = buildConf("spark.sql.maxPlanStringLength")
    .doc("Maximum number of characters to output for a plan string.  If the plan is " +
      "longer, further output will be truncated.  The default setting always generates a full " +
      "plan.  Set this to a lower value such as 8k if plan strings are taking up too much " +
      "memory or are causing OutOfMemory errors in the driver or UI processes.")
    .bytesConf(ByteUnit.BYTE)
    .checkValue(i => i >= 0 && i <= ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH, "Invalid " +
      "value for 'spark.sql.maxPlanStringLength'.  Length must be a valid string length " +
      "(nonnegative and shorter than the maximum size).")
    .createWithDefault(ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH)

  val SET_COMMAND_REJECTS_SPARK_CORE_CONFS =
    buildConf("spark.sql.legacy.setCommandRejectsSparkCoreConfs")
      .internal()
      .doc("If it is set to true, SET command will fail when the key is registered as " +
        "a SparkConf entry.")
      .booleanConf
      .createWithDefault(true)

  val DATETIME_JAVA8API_ENABLED = buildConf("spark.sql.datetime.java8API.enabled")
    .doc("If the configuration property is set to true, java.time.Instant and " +
      "java.time.LocalDate classes of Java 8 API are used as external types for " +
      "Catalyst's TimestampType and DateType. If it is set to false, java.sql.Timestamp " +
      "and java.sql.Date are used for the same purpose.")
    .booleanConf
    .createWithDefault(false)

  val UTC_TIMESTAMP_FUNC_ENABLED = buildConf("spark.sql.legacy.utcTimestampFunc.enabled")
    .doc("The configuration property enables the to_utc_timestamp() " +
         "and from_utc_timestamp() functions.")
    .booleanConf
    .createWithDefault(false)

  val SOURCES_BINARY_FILE_MAX_LENGTH = buildConf("spark.sql.sources.binaryFile.maxLength")
    .doc("The max length of a file that can be read by the binary file data source. " +
      "Spark will fail fast and not attempt to read the file if its length exceeds this value. " +
      "The theoretical max is Int.MaxValue, though VMs might implement a smaller max.")
    .internal()
    .intConf
    .createWithDefault(Int.MaxValue)

  val LEGACY_CAST_DATETIME_TO_STRING =
    buildConf("spark.sql.legacy.typeCoercion.datetimeToString")
      .doc("If it is set to true, date/timestamp will cast to string in binary comparisons " +
        "with String")
    .booleanConf
    .createWithDefault(false)

  val DEFAULT_V2_CATALOG = buildConf("spark.sql.default.catalog")
    .doc("Name of the default v2 catalog, used when a catalog is not identified in queries")
    .stringConf
    .createOptional

  val LEGACY_LOOSE_UPCAST = buildConf("spark.sql.legacy.looseUpcast")
    .doc("When true, the upcast will be loose and allows string to atomic types.")
    .booleanConf
    .createWithDefault(false)
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
class SQLConf extends Serializable with Logging {
  import SQLConf._

  /** Only low degree of contention is expected for conf, thus NOT using ConcurrentHashMap. */
  @transient protected[spark] val settings = java.util.Collections.synchronizedMap(
    new java.util.HashMap[String, String]())

  @transient protected val reader = new ConfigReader(settings)

  /** ************************ Spark SQL Params/Hints ******************* */

  def optimizerExcludedRules: Option[String] = getConf(OPTIMIZER_EXCLUDED_RULES)

  def optimizerMaxIterations: Int = getConf(OPTIMIZER_MAX_ITERATIONS)

  def optimizerInSetConversionThreshold: Int = getConf(OPTIMIZER_INSET_CONVERSION_THRESHOLD)

  def optimizerInSetSwitchThreshold: Int = getConf(OPTIMIZER_INSET_SWITCH_THRESHOLD)

  def optimizerPlanChangeLogLevel: String = getConf(OPTIMIZER_PLAN_CHANGE_LOG_LEVEL)

  def optimizerPlanChangeRules: Option[String] = getConf(OPTIMIZER_PLAN_CHANGE_LOG_RULES)

  def optimizerPlanChangeBatches: Option[String] = getConf(OPTIMIZER_PLAN_CHANGE_LOG_BATCHES)

  def stateStoreProviderClass: String = getConf(STATE_STORE_PROVIDER_CLASS)

  def stateStoreMinDeltasForSnapshot: Int = getConf(STATE_STORE_MIN_DELTAS_FOR_SNAPSHOT)

  def checkpointLocation: Option[String] = getConf(CHECKPOINT_LOCATION)

  def isUnsupportedOperationCheckEnabled: Boolean = getConf(UNSUPPORTED_OPERATION_CHECK_ENABLED)

  def streamingFileCommitProtocolClass: String = getConf(STREAMING_FILE_COMMIT_PROTOCOL_CLASS)

  def fileSinkLogDeletion: Boolean = getConf(FILE_SINK_LOG_DELETION)

  def fileSinkLogCompactInterval: Int = getConf(FILE_SINK_LOG_COMPACT_INTERVAL)

  def fileSinkLogCleanupDelay: Long = getConf(FILE_SINK_LOG_CLEANUP_DELAY)

  def fileSourceLogDeletion: Boolean = getConf(FILE_SOURCE_LOG_DELETION)

  def fileSourceLogCompactInterval: Int = getConf(FILE_SOURCE_LOG_COMPACT_INTERVAL)

  def fileSourceLogCleanupDelay: Long = getConf(FILE_SOURCE_LOG_CLEANUP_DELAY)

  def streamingSchemaInference: Boolean = getConf(STREAMING_SCHEMA_INFERENCE)

  def streamingPollingDelay: Long = getConf(STREAMING_POLLING_DELAY)

  def streamingNoDataProgressEventInterval: Long =
    getConf(STREAMING_NO_DATA_PROGRESS_EVENT_INTERVAL)

  def streamingNoDataMicroBatchesEnabled: Boolean =
    getConf(STREAMING_NO_DATA_MICRO_BATCHES_ENABLED)

  def streamingMetricsEnabled: Boolean = getConf(STREAMING_METRICS_ENABLED)

  def streamingProgressRetention: Int = getConf(STREAMING_PROGRESS_RETENTION)

  def filesMaxPartitionBytes: Long = getConf(FILES_MAX_PARTITION_BYTES)

  def filesOpenCostInBytes: Long = getConf(FILES_OPEN_COST_IN_BYTES)

  def ignoreCorruptFiles: Boolean = getConf(IGNORE_CORRUPT_FILES)

  def ignoreMissingFiles: Boolean = getConf(IGNORE_MISSING_FILES)

  def maxRecordsPerFile: Long = getConf(MAX_RECORDS_PER_FILE)

  def useCompression: Boolean = getConf(COMPRESS_CACHED)

  def orcCompressionCodec: String = getConf(ORC_COMPRESSION)

  def orcVectorizedReaderEnabled: Boolean = getConf(ORC_VECTORIZED_READER_ENABLED)

  def orcVectorizedReaderBatchSize: Int = getConf(ORC_VECTORIZED_READER_BATCH_SIZE)

  def parquetCompressionCodec: String = getConf(PARQUET_COMPRESSION)

  def parquetVectorizedReaderEnabled: Boolean = getConf(PARQUET_VECTORIZED_READER_ENABLED)

  def parquetVectorizedReaderBatchSize: Int = getConf(PARQUET_VECTORIZED_READER_BATCH_SIZE)

  def columnBatchSize: Int = getConf(COLUMN_BATCH_SIZE)

  def cacheVectorizedReaderEnabled: Boolean = getConf(CACHE_VECTORIZED_READER_ENABLED)

  def numShufflePartitions: Int = getConf(SHUFFLE_PARTITIONS)

  def targetPostShuffleInputSize: Long =
    getConf(SHUFFLE_TARGET_POSTSHUFFLE_INPUT_SIZE)

  def adaptiveExecutionEnabled: Boolean = getConf(ADAPTIVE_EXECUTION_ENABLED)

  def minNumPostShufflePartitions: Int =
    getConf(SHUFFLE_MIN_NUM_POSTSHUFFLE_PARTITIONS)

  def minBatchesToRetain: Int = getConf(MIN_BATCHES_TO_RETAIN)

  def maxBatchesToRetainInMemory: Int = getConf(MAX_BATCHES_TO_RETAIN_IN_MEMORY)

  def parquetFilterPushDown: Boolean = getConf(PARQUET_FILTER_PUSHDOWN_ENABLED)

  def parquetFilterPushDownDate: Boolean = getConf(PARQUET_FILTER_PUSHDOWN_DATE_ENABLED)

  def parquetFilterPushDownTimestamp: Boolean = getConf(PARQUET_FILTER_PUSHDOWN_TIMESTAMP_ENABLED)

  def parquetFilterPushDownDecimal: Boolean = getConf(PARQUET_FILTER_PUSHDOWN_DECIMAL_ENABLED)

  def parquetFilterPushDownStringStartWith: Boolean =
    getConf(PARQUET_FILTER_PUSHDOWN_STRING_STARTSWITH_ENABLED)

  def parquetFilterPushDownInFilterThreshold: Int =
    getConf(PARQUET_FILTER_PUSHDOWN_INFILTERTHRESHOLD)

  def orcFilterPushDown: Boolean = getConf(ORC_FILTER_PUSHDOWN_ENABLED)

  def verifyPartitionPath: Boolean = getConf(HIVE_VERIFY_PARTITION_PATH)

  def metastorePartitionPruning: Boolean = getConf(HIVE_METASTORE_PARTITION_PRUNING)

  def manageFilesourcePartitions: Boolean = getConf(HIVE_MANAGE_FILESOURCE_PARTITIONS)

  def filesourcePartitionFileCacheSize: Long = getConf(HIVE_FILESOURCE_PARTITION_FILE_CACHE_SIZE)

  def caseSensitiveInferenceMode: HiveCaseSensitiveInferenceMode.Value =
    HiveCaseSensitiveInferenceMode.withName(getConf(HIVE_CASE_SENSITIVE_INFERENCE))

  def compareDateTimestampInTimestamp : Boolean = getConf(COMPARE_DATE_TIMESTAMP_IN_TIMESTAMP)

  def gatherFastStats: Boolean = getConf(GATHER_FASTSTAT)

  def optimizerMetadataOnly: Boolean = getConf(OPTIMIZER_METADATA_ONLY)

  def wholeStageEnabled: Boolean = getConf(WHOLESTAGE_CODEGEN_ENABLED)

  def wholeStageUseIdInClassName: Boolean = getConf(WHOLESTAGE_CODEGEN_USE_ID_IN_CLASS_NAME)

  def wholeStageMaxNumFields: Int = getConf(WHOLESTAGE_MAX_NUM_FIELDS)

  def codegenFallback: Boolean = getConf(CODEGEN_FALLBACK)

  def codegenComments: Boolean = getConf(StaticSQLConf.CODEGEN_COMMENTS)

  def loggingMaxLinesForCodegen: Int = getConf(CODEGEN_LOGGING_MAX_LINES)

  def hugeMethodLimit: Int = getConf(WHOLESTAGE_HUGE_METHOD_LIMIT)

  def methodSplitThreshold: Int = getConf(CODEGEN_METHOD_SPLIT_THRESHOLD)

  def wholeStageSplitConsumeFuncByOperator: Boolean =
    getConf(WHOLESTAGE_SPLIT_CONSUME_FUNC_BY_OPERATOR)

  def tableRelationCacheSize: Int =
    getConf(StaticSQLConf.FILESOURCE_TABLE_RELATION_CACHE_SIZE)

  def codegenCacheMaxEntries: Int = getConf(StaticSQLConf.CODEGEN_CACHE_MAX_ENTRIES)

  def exchangeReuseEnabled: Boolean = getConf(EXCHANGE_REUSE_ENABLED)

  def subqueryReuseEnabled: Boolean = getConf(SUBQUERY_REUSE_ENABLED)

  def caseSensitiveAnalysis: Boolean = getConf(SQLConf.CASE_SENSITIVE)

  def constraintPropagationEnabled: Boolean = getConf(CONSTRAINT_PROPAGATION_ENABLED)

  def ansiParserEnabled: Boolean = getConf(ANSI_SQL_PARSER)

  def escapedStringLiterals: Boolean = getConf(ESCAPED_STRING_LITERALS)

  def fileCompressionFactor: Double = getConf(FILE_COMRESSION_FACTOR)

  def stringRedactionPattern: Option[Regex] = getConf(SQL_STRING_REDACTION_PATTERN)

  def sortBeforeRepartition: Boolean = getConf(SORT_BEFORE_REPARTITION)

  def topKSortFallbackThreshold: Int = getConf(TOP_K_SORT_FALLBACK_THRESHOLD)

  def fastHashAggregateRowMaxCapacityBit: Int = getConf(FAST_HASH_AGGREGATE_MAX_ROWS_CAPACITY_BIT)

  def datetimeJava8ApiEnabled: Boolean = getConf(DATETIME_JAVA8API_ENABLED)

  def utcTimestampFuncEnabled: Boolean = getConf(UTC_TIMESTAMP_FUNC_ENABLED)

  /**
   * Returns the [[Resolver]] for the current configuration, which can be used to determine if two
   * identifiers are equal.
   */
  def resolver: Resolver = {
    if (caseSensitiveAnalysis) {
      org.apache.spark.sql.catalyst.analysis.caseSensitiveResolution
    } else {
      org.apache.spark.sql.catalyst.analysis.caseInsensitiveResolution
    }
  }

  def subexpressionEliminationEnabled: Boolean =
    getConf(SUBEXPRESSION_ELIMINATION_ENABLED)

  def autoBroadcastJoinThreshold: Long = getConf(AUTO_BROADCASTJOIN_THRESHOLD)

  def limitScaleUpFactor: Int = getConf(LIMIT_SCALE_UP_FACTOR)

  def advancedPartitionPredicatePushdownEnabled: Boolean =
    getConf(ADVANCED_PARTITION_PREDICATE_PUSHDOWN)

  def preferSortMergeJoin: Boolean = getConf(PREFER_SORTMERGEJOIN)

  def enableRadixSort: Boolean = getConf(RADIX_SORT_ENABLED)

  def isParquetSchemaMergingEnabled: Boolean = getConf(PARQUET_SCHEMA_MERGING_ENABLED)

  def isParquetSchemaRespectSummaries: Boolean = getConf(PARQUET_SCHEMA_RESPECT_SUMMARIES)

  def parquetOutputCommitterClass: String = getConf(PARQUET_OUTPUT_COMMITTER_CLASS)

  def isParquetBinaryAsString: Boolean = getConf(PARQUET_BINARY_AS_STRING)

  def isParquetINT96AsTimestamp: Boolean = getConf(PARQUET_INT96_AS_TIMESTAMP)

  def isParquetINT96TimestampConversion: Boolean = getConf(PARQUET_INT96_TIMESTAMP_CONVERSION)

  def isParquetINT64AsTimestampMillis: Boolean = getConf(PARQUET_INT64_AS_TIMESTAMP_MILLIS)

  def parquetOutputTimestampType: ParquetOutputTimestampType.Value = {
    val isOutputTimestampTypeSet = settings.containsKey(PARQUET_OUTPUT_TIMESTAMP_TYPE.key)
    if (!isOutputTimestampTypeSet && isParquetINT64AsTimestampMillis) {
      // If PARQUET_OUTPUT_TIMESTAMP_TYPE is not set and PARQUET_INT64_AS_TIMESTAMP_MILLIS is set,
      // respect PARQUET_INT64_AS_TIMESTAMP_MILLIS and use TIMESTAMP_MILLIS. Otherwise,
      // PARQUET_OUTPUT_TIMESTAMP_TYPE has higher priority.
      ParquetOutputTimestampType.TIMESTAMP_MILLIS
    } else {
      ParquetOutputTimestampType.withName(getConf(PARQUET_OUTPUT_TIMESTAMP_TYPE))
    }
  }

  def writeLegacyParquetFormat: Boolean = getConf(PARQUET_WRITE_LEGACY_FORMAT)

  def parquetRecordFilterEnabled: Boolean = getConf(PARQUET_RECORD_FILTER_ENABLED)

  def inMemoryPartitionPruning: Boolean = getConf(IN_MEMORY_PARTITION_PRUNING)

  def offHeapColumnVectorEnabled: Boolean = getConf(COLUMN_VECTOR_OFFHEAP_ENABLED)

  def columnNameOfCorruptRecord: String = getConf(COLUMN_NAME_OF_CORRUPT_RECORD)

  def broadcastTimeout: Long = {
    val timeoutValue = getConf(BROADCAST_TIMEOUT)
    if (timeoutValue < 0) Long.MaxValue else timeoutValue
  }

  def defaultDataSourceName: String = getConf(DEFAULT_DATA_SOURCE_NAME)

  def convertCTAS: Boolean = getConf(CONVERT_CTAS)

  def partitionColumnTypeInferenceEnabled: Boolean =
    getConf(SQLConf.PARTITION_COLUMN_TYPE_INFERENCE)

  def fileCommitProtocolClass: String = getConf(SQLConf.FILE_COMMIT_PROTOCOL_CLASS)

  def parallelPartitionDiscoveryThreshold: Int =
    getConf(SQLConf.PARALLEL_PARTITION_DISCOVERY_THRESHOLD)

  def parallelPartitionDiscoveryParallelism: Int =
    getConf(SQLConf.PARALLEL_PARTITION_DISCOVERY_PARALLELISM)

  def bucketingEnabled: Boolean = getConf(SQLConf.BUCKETING_ENABLED)

  def bucketingMaxBuckets: Int = getConf(SQLConf.BUCKETING_MAX_BUCKETS)

  def dataFrameSelfJoinAutoResolveAmbiguity: Boolean =
    getConf(DATAFRAME_SELF_JOIN_AUTO_RESOLVE_AMBIGUITY)

  def dataFrameRetainGroupColumns: Boolean = getConf(DATAFRAME_RETAIN_GROUP_COLUMNS)

  def dataFramePivotMaxValues: Int = getConf(DATAFRAME_PIVOT_MAX_VALUES)

  def runSQLonFile: Boolean = getConf(RUN_SQL_ON_FILES)

  def enableTwoLevelAggMap: Boolean = getConf(ENABLE_TWOLEVEL_AGG_MAP)

  def useObjectHashAggregation: Boolean = getConf(USE_OBJECT_HASH_AGG)

  def objectAggSortBasedFallbackThreshold: Int = getConf(OBJECT_AGG_SORT_BASED_FALLBACK_THRESHOLD)

  def variableSubstituteEnabled: Boolean = getConf(VARIABLE_SUBSTITUTE_ENABLED)

  def variableSubstituteDepth: Int = getConf(VARIABLE_SUBSTITUTE_DEPTH)

  def warehousePath: String = new Path(getConf(StaticSQLConf.WAREHOUSE_PATH)).toString

  def hiveThriftServerSingleSession: Boolean =
    getConf(StaticSQLConf.HIVE_THRIFT_SERVER_SINGLESESSION)

  def orderByOrdinal: Boolean = getConf(ORDER_BY_ORDINAL)

  def groupByOrdinal: Boolean = getConf(GROUP_BY_ORDINAL)

  def groupByAliases: Boolean = getConf(GROUP_BY_ALIASES)

  def crossJoinEnabled: Boolean = getConf(SQLConf.CROSS_JOINS_ENABLED)

  def sessionLocalTimeZone: String = getConf(SQLConf.SESSION_LOCAL_TIMEZONE)

  def parallelFileListingInStatsComputation: Boolean =
    getConf(SQLConf.PARALLEL_FILE_LISTING_IN_STATS_COMPUTATION)

  def fallBackToHdfsForStatsEnabled: Boolean = getConf(ENABLE_FALL_BACK_TO_HDFS_FOR_STATS)

  def defaultSizeInBytes: Long = getConf(DEFAULT_SIZE_IN_BYTES)

  def ndvMaxError: Double = getConf(NDV_MAX_ERROR)

  def histogramEnabled: Boolean = getConf(HISTOGRAM_ENABLED)

  def histogramNumBins: Int = getConf(HISTOGRAM_NUM_BINS)

  def percentileAccuracy: Int = getConf(PERCENTILE_ACCURACY)

  def cboEnabled: Boolean = getConf(SQLConf.CBO_ENABLED)

  def autoSizeUpdateEnabled: Boolean = getConf(SQLConf.AUTO_SIZE_UPDATE_ENABLED)

  def joinReorderEnabled: Boolean = getConf(SQLConf.JOIN_REORDER_ENABLED)

  def joinReorderDPThreshold: Int = getConf(SQLConf.JOIN_REORDER_DP_THRESHOLD)

  def joinReorderCardWeight: Double = getConf(SQLConf.JOIN_REORDER_CARD_WEIGHT)

  def joinReorderDPStarFilter: Boolean = getConf(SQLConf.JOIN_REORDER_DP_STAR_FILTER)

  def windowExecBufferInMemoryThreshold: Int = getConf(WINDOW_EXEC_BUFFER_IN_MEMORY_THRESHOLD)

  def windowExecBufferSpillThreshold: Int = getConf(WINDOW_EXEC_BUFFER_SPILL_THRESHOLD)

  def sortMergeJoinExecBufferInMemoryThreshold: Int =
    getConf(SORT_MERGE_JOIN_EXEC_BUFFER_IN_MEMORY_THRESHOLD)

  def sortMergeJoinExecBufferSpillThreshold: Int =
    getConf(SORT_MERGE_JOIN_EXEC_BUFFER_SPILL_THRESHOLD)

  def cartesianProductExecBufferInMemoryThreshold: Int =
    getConf(CARTESIAN_PRODUCT_EXEC_BUFFER_IN_MEMORY_THRESHOLD)

  def cartesianProductExecBufferSpillThreshold: Int =
    getConf(CARTESIAN_PRODUCT_EXEC_BUFFER_SPILL_THRESHOLD)

  def maxNestedViewDepth: Int = getConf(SQLConf.MAX_NESTED_VIEW_DEPTH)

  def starSchemaDetection: Boolean = getConf(STARSCHEMA_DETECTION)

  def starSchemaFTRatio: Double = getConf(STARSCHEMA_FACT_TABLE_RATIO)

  def supportQuotedRegexColumnName: Boolean = getConf(SUPPORT_QUOTED_REGEX_COLUMN_NAME)

  def rangeExchangeSampleSizePerPartition: Int = getConf(RANGE_EXCHANGE_SAMPLE_SIZE_PER_PARTITION)

  def arrowEnabled: Boolean = getConf(ARROW_EXECUTION_ENABLED)

  def arrowFallbackEnabled: Boolean = getConf(ARROW_FALLBACK_ENABLED)

  def arrowMaxRecordsPerBatch: Int = getConf(ARROW_EXECUTION_MAX_RECORDS_PER_BATCH)

  def pandasRespectSessionTimeZone: Boolean = getConf(PANDAS_RESPECT_SESSION_LOCAL_TIMEZONE)

  def pandasGroupedMapAssignColumnsByName: Boolean =
    getConf(SQLConf.PANDAS_GROUPED_MAP_ASSIGN_COLUMNS_BY_NAME)

  def arrowSafeTypeConversion: Boolean = getConf(SQLConf.PANDAS_ARROW_SAFE_TYPE_CONVERSION)

  def replaceExceptWithFilter: Boolean = getConf(REPLACE_EXCEPT_WITH_FILTER)

  def decimalOperationsAllowPrecisionLoss: Boolean = getConf(DECIMAL_OPERATIONS_ALLOW_PREC_LOSS)

  def literalPickMinimumPrecision: Boolean = getConf(LITERAL_PICK_MINIMUM_PRECISION)

  def continuousStreamingEpochBacklogQueueSize: Int =
    getConf(CONTINUOUS_STREAMING_EPOCH_BACKLOG_QUEUE_SIZE)

  def continuousStreamingExecutorQueueSize: Int = getConf(CONTINUOUS_STREAMING_EXECUTOR_QUEUE_SIZE)

  def continuousStreamingExecutorPollIntervalMs: Long =
    getConf(CONTINUOUS_STREAMING_EXECUTOR_POLL_INTERVAL_MS)

  def useV1SourceReaderList: String = getConf(USE_V1_SOURCE_READER_LIST)

  def useV1SourceWriterList: String = getConf(USE_V1_SOURCE_WRITER_LIST)

  def disabledV2StreamingWriters: String = getConf(DISABLED_V2_STREAMING_WRITERS)

  def disabledV2StreamingMicroBatchReaders: String =
    getConf(DISABLED_V2_STREAMING_MICROBATCH_READERS)

  def concatBinaryAsString: Boolean = getConf(CONCAT_BINARY_AS_STRING)

  def eltOutputAsString: Boolean = getConf(ELT_OUTPUT_AS_STRING)

  def allowCreatingManagedTableUsingNonemptyLocation: Boolean =
    getConf(ALLOW_CREATING_MANAGED_TABLE_USING_NONEMPTY_LOCATION)

  def validatePartitionColumns: Boolean = getConf(VALIDATE_PARTITION_COLUMNS)

  def partitionOverwriteMode: PartitionOverwriteMode.Value =
    PartitionOverwriteMode.withName(getConf(PARTITION_OVERWRITE_MODE))

  def nestedSchemaPruningEnabled: Boolean = getConf(NESTED_SCHEMA_PRUNING_ENABLED)

  def serializerNestedSchemaPruningEnabled: Boolean =
    getConf(SERIALIZER_NESTED_SCHEMA_PRUNING_ENABLED)

  def csvColumnPruning: Boolean = getConf(SQLConf.CSV_PARSER_COLUMN_PRUNING)

  def legacySizeOfNull: Boolean = getConf(SQLConf.LEGACY_SIZE_OF_NULL)

  def isReplEagerEvalEnabled: Boolean = getConf(SQLConf.REPL_EAGER_EVAL_ENABLED)

  def replEagerEvalMaxNumRows: Int = getConf(SQLConf.REPL_EAGER_EVAL_MAX_NUM_ROWS)

  def replEagerEvalTruncate: Int = getConf(SQLConf.REPL_EAGER_EVAL_TRUNCATE)

  def avroCompressionCodec: String = getConf(SQLConf.AVRO_COMPRESSION_CODEC)

  def avroDeflateLevel: Int = getConf(SQLConf.AVRO_DEFLATE_LEVEL)

  def replaceDatabricksSparkAvroEnabled: Boolean =
    getConf(SQLConf.LEGACY_REPLACE_DATABRICKS_SPARK_AVRO_ENABLED)

  def setOpsPrecedenceEnforced: Boolean = getConf(SQLConf.LEGACY_SETOPS_PRECEDENCE_ENABLED)

  def integralDivideReturnLong: Boolean = getConf(SQLConf.LEGACY_INTEGRALDIVIDE_RETURN_LONG)

  def nameNonStructGroupingKeyAsValue: Boolean =
    getConf(SQLConf.NAME_NON_STRUCT_GROUPING_KEY_AS_VALUE)

  def maxToStringFields: Int = getConf(SQLConf.MAX_TO_STRING_FIELDS)

  def maxPlanStringLength: Int = getConf(SQLConf.MAX_PLAN_STRING_LENGTH).toInt

  def setCommandRejectsSparkCoreConfs: Boolean =
    getConf(SQLConf.SET_COMMAND_REJECTS_SPARK_CORE_CONFS)

  def castDatetimeToString: Boolean = getConf(SQLConf.LEGACY_CAST_DATETIME_TO_STRING)

  def defaultV2Catalog: Option[String] = getConf(DEFAULT_V2_CATALOG)

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
        Option(sqlConfEntries.get(key)).map { e => e.stringConverter(e.readFrom(reader)) }
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
    entry.readFrom(reader)
  }

  /**
   * Return the value of an optional Spark SQL configuration property for the given key. If the key
   * is not set yet, returns None.
   */
  def getConf[T](entry: OptionalConfigEntry[T]): Option[T] = {
    require(sqlConfEntries.get(entry.key) == entry, s"$entry is not registered")
    entry.readFrom(reader)
  }

  /**
   * Return the `string` value of Spark SQL configuration property for the given key. If the key is
   * not set yet, return `defaultValue`.
   */
  def getConfString(key: String, defaultValue: String): String = {
    if (defaultValue != null && defaultValue != ConfigEntry.UNDEFINED) {
      val entry = sqlConfEntries.get(key)
      if (entry != null) {
        // Only verify configs in the SQLConf object
        entry.valueConverter(defaultValue)
      }
    }
    Option(settings.get(key)).getOrElse {
      // If the key is not set, need to check whether the config entry is registered and is
      // a fallback conf, so that we can check its parent.
      sqlConfEntries.get(key) match {
        case e: FallbackConfigEntry[_] => getConfString(e.fallback.key, defaultValue)
        case _ => defaultValue
      }
    }
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
      val displayValue = Option(getConfString(entry.key, null)).getOrElse(entry.defaultValueString)
      (entry.key, displayValue, entry.doc)
    }.toSeq
  }

  /**
   * Redacts the given option map according to the description of SQL_OPTIONS_REDACTION_PATTERN.
   */
  def redactOptions(options: Map[String, String]): Map[String, String] = {
    val regexes = Seq(
      getConf(SQL_OPTIONS_REDACTION_PATTERN),
      SECRET_REDACTION_PATTERN.readFrom(reader))

    regexes.foldLeft(options.toSeq) { case (opts, r) => Utils.redact(Some(r), opts) }.toMap
  }

  /**
   * Return whether a given key is set in this [[SQLConf]].
   */
  def contains(key: String): Boolean = {
    settings.containsKey(key)
  }

  protected def setConfWithCheck(key: String, value: String): Unit = {
    settings.put(key, value)
  }

  def unsetConf(key: String): Unit = {
    settings.remove(key)
  }

  def unsetConf(entry: ConfigEntry[_]): Unit = {
    settings.remove(entry.key)
  }

  def clear(): Unit = {
    settings.clear()
  }

  override def clone(): SQLConf = {
    val result = new SQLConf
    getAllConfs.foreach {
      case(k, v) => if (v ne null) result.setConfString(k, v)
    }
    result
  }

  // For test only
  def copy(entries: (ConfigEntry[_], Any)*): SQLConf = {
    val cloned = clone()
    entries.foreach {
      case (entry, value) => cloned.setConfString(entry.key, value.toString)
    }
    cloned
  }

  def isModifiable(key: String): Boolean = {
    sqlConfEntries.containsKey(key) && !staticConfKeys.contains(key)
  }
}
