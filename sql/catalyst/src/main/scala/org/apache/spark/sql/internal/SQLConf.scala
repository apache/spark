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

import scala.collection.JavaConverters._
import scala.collection.immutable

import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.network.util.ByteUnit
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.util.collection.unsafe.sort.UnsafeExternalSorter

////////////////////////////////////////////////////////////////////////////////////////////////////
// This file defines the configuration options for Spark SQL.
////////////////////////////////////////////////////////////////////////////////////////////////////


object SQLConf {

  private val sqlConfEntries = java.util.Collections.synchronizedMap(
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

  val COMPRESS_CACHED = buildConf("spark.sql.inMemoryColumnarStorage.compressed")
    .internal()
    .doc("When set to true Spark SQL will automatically select a compression codec for each " +
      "column based on statistics of the data.")
    .booleanConf
    .createWithDefault(true)

  val COLUMN_BATCH_SIZE = buildConf("spark.sql.inMemoryColumnarStorage.batchSize")
    .internal()
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
    .longConf
    .createWithDefault(10L * 1024 * 1024)

  val LIMIT_SCALE_UP_FACTOR = buildConf("spark.sql.limit.scaleUpFactor")
    .internal()
    .doc("Minimal increase rate in number of partitions between attempts when executing a take " +
      "on a query. Higher values lead to more partitions read. Lower values might lead to " +
      "longer execution times as more jobs will be run")
    .intConf
    .createWithDefault(4)

  val ENABLE_FALL_BACK_TO_HDFS_FOR_STATS =
    buildConf("spark.sql.statistics.fallBackToHdfs")
    .doc("If the table statistics are not available from table metadata enable fall back to hdfs." +
      " This is useful in determining if a table is small enough to use auto broadcast joins.")
    .booleanConf
    .createWithDefault(false)

  val DEFAULT_SIZE_IN_BYTES = buildConf("spark.sql.defaultSizeInBytes")
    .internal()
    .doc("The default table size used in query planning. By default, it is set to Long.MaxValue " +
      "which is larger than `spark.sql.autoBroadcastJoinThreshold` to be more conservative. " +
      "That is to say by default the optimizer will not choose to broadcast a table unless it " +
      "knows for sure its size is small enough.")
    .longConf
    .createWithDefault(Long.MaxValue)

  val SHUFFLE_PARTITIONS = buildConf("spark.sql.shuffle.partitions")
    .doc("The default number of partitions to use when shuffling data for joins or aggregations.")
    .intConf
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
      "plan to optimize them. Constraint propagation can sometimes be computationally expensive" +
      "for certain kinds of query plans (such as those with a large number of predicates and " +
      "aliases) which might negatively impact overall runtime.")
    .booleanConf
    .createWithDefault(true)

  val ESCAPED_STRING_LITERALS = buildConf("spark.sql.parser.escapedStringLiterals")
    .internal()
    .doc("When true, string literals (including regex patterns) remain escaped in our SQL " +
      "parser. The default is false since Spark 2.0. Setting it to true can restore the behavior " +
      "prior to Spark 2.0.")
    .booleanConf
    .createWithDefault(false)

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

  val PARQUET_INT64_AS_TIMESTAMP_MILLIS = buildConf("spark.sql.parquet.int64AsTimestampMillis")
    .doc("When true, timestamp values will be stored as INT64 with TIMESTAMP_MILLIS as the " +
      "extended type. In this mode, the microsecond portion of the timestamp value will be" +
      "truncated.")
    .booleanConf
    .createWithDefault(false)

  val PARQUET_CACHE_METADATA = buildConf("spark.sql.parquet.cacheMetadata")
    .doc("Turns on caching of Parquet schema metadata. Can speed up querying of static data.")
    .booleanConf
    .createWithDefault(true)

  val PARQUET_COMPRESSION = buildConf("spark.sql.parquet.compression.codec")
    .doc("Sets the compression codec use when writing Parquet files. Acceptable values include: " +
      "uncompressed, snappy, gzip, lzo.")
    .stringConf
    .transform(_.toLowerCase(Locale.ROOT))
    .checkValues(Set("uncompressed", "snappy", "gzip", "lzo"))
    .createWithDefault("snappy")

  val PARQUET_FILTER_PUSHDOWN_ENABLED = buildConf("spark.sql.parquet.filterPushdown")
    .doc("Enables Parquet filter push-down optimization when set to true.")
    .booleanConf
    .createWithDefault(true)

  val PARQUET_WRITE_LEGACY_FORMAT = buildConf("spark.sql.parquet.writeLegacyFormat")
    .doc("Whether to follow Parquet's format specification when converting Parquet schema to " +
      "Spark SQL schema and vice versa.")
    .booleanConf
    .createWithDefault(false)

  val PARQUET_OUTPUT_COMMITTER_CLASS = buildConf("spark.sql.parquet.output.committer.class")
    .doc("The output committer class used by Parquet. The specified class needs to be a " +
      "subclass of org.apache.hadoop.mapreduce.OutputCommitter.  Typically, it's also a subclass " +
      "of org.apache.parquet.hadoop.ParquetOutputCommitter.")
    .internal()
    .stringConf
    .createWithDefault("org.apache.parquet.hadoop.ParquetOutputCommitter")

  val PARQUET_VECTORIZED_READER_ENABLED =
    buildConf("spark.sql.parquet.enableVectorizedReader")
      .doc("Enables vectorized parquet decoding.")
      .booleanConf
      .createWithDefault(true)

  val ORC_FILTER_PUSHDOWN_ENABLED = buildConf("spark.sql.orc.filterPushdown")
    .doc("When true, enable filter pushdown for ORC files.")
    .booleanConf
    .createWithDefault(false)

  val HIVE_VERIFY_PARTITION_PATH = buildConf("spark.sql.hive.verifyPartitionPath")
    .doc("When true, check all the partition paths under the table\'s root directory " +
         "when reading data stored in HDFS.")
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
    .doc("Sets the action to take when a case-sensitive schema cannot be read from a Hive " +
      "table's properties. Although Spark SQL itself is not case-sensitive, Hive compatible file " +
      "formats such as Parquet are. Spark SQL must use a case-preserving schema when querying " +
      "any table backed by files containing case-sensitive field names or queries may not return " +
      "accurate results. Valid options include INFER_AND_SAVE (the default mode-- infer the " +
      "case-sensitive schema from the underlying data files and write it back to the table " +
      "properties), INFER_ONLY (infer the schema but don't attempt to write it to the table " +
      "properties) and NEVER_INFER (fallback to using the case-insensitive metastore schema " +
      "instead of inferring).")
    .stringConf
    .transform(_.toUpperCase(Locale.ROOT))
    .checkValues(HiveCaseSensitiveInferenceMode.values.map(_.toString))
    .createWithDefault(HiveCaseSensitiveInferenceMode.INFER_AND_SAVE.toString)

  val OPTIMIZER_METADATA_ONLY = buildConf("spark.sql.optimizer.metadataOnly")
    .doc("When true, enable the metadata-only query optimization that use the table's metadata " +
      "to produce the partition columns instead of table scans. It applies when all the columns " +
      "scanned are partition columns and the query has an aggregate operator that satisfies " +
      "distinct semantics.")
    .booleanConf
    .createWithDefault(true)

  val COLUMN_NAME_OF_CORRUPT_RECORD = buildConf("spark.sql.columnNameOfCorruptRecord")
    .doc("The name of internal column for storing raw/un-parsed JSON and CSV records that fail " +
      "to parse.")
    .stringConf
    .createWithDefault("_corrupt_record")

  val BROADCAST_TIMEOUT = buildConf("spark.sql.broadcastTimeout")
    .doc("Timeout in seconds for the broadcast wait time in broadcast joins.")
    .intConf
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
      "using the data source set by spark.sql.sources.default.")
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
  val OUTPUT_COMMITTER_CLASS =
    buildConf("spark.sql.sources.outputCommitterClass").internal().stringConf.createOptional

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

  val WHOLESTAGE_MAX_NUM_FIELDS = buildConf("spark.sql.codegen.maxFields")
    .internal()
    .doc("The maximum number of fields (including nested fields) that will be supported before" +
      " deactivating whole-stage codegen.")
    .intConf
    .createWithDefault(100)

  val WHOLESTAGE_FALLBACK = buildConf("spark.sql.codegen.fallback")
    .internal()
    .doc("When true, whole stage codegen could be temporary disabled for the part of query that" +
      " fail to compile generated code")
    .booleanConf
    .createWithDefault(true)

  val MAX_CASES_BRANCHES = buildConf("spark.sql.codegen.maxCaseBranches")
    .internal()
    .doc("The maximum number of switches supported with codegen.")
    .intConf
    .createWithDefault(20)

  val FILES_MAX_PARTITION_BYTES = buildConf("spark.sql.files.maxPartitionBytes")
    .doc("The maximum number of bytes to pack into a single partition when reading files.")
    .longConf
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

  val STATE_STORE_MIN_DELTAS_FOR_SNAPSHOT =
    buildConf("spark.sql.streaming.stateStore.minDeltasForSnapshot")
      .internal()
      .doc("Minimum number of state store delta files that needs to be generated before they " +
        "consolidated into snapshots.")
      .intConf
      .createWithDefault(10)

  val CHECKPOINT_LOCATION = buildConf("spark.sql.streaming.checkpointLocation")
    .doc("The default location for storing checkpoint data for streaming queries.")
    .stringConf
    .createOptional

  val MIN_BATCHES_TO_RETAIN = buildConf("spark.sql.streaming.minBatchesToRetain")
    .internal()
    .doc("The minimum number of batches that must be retained and made recoverable.")
    .intConf
    .createWithDefault(100)

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
    buildConf("spark.sql.codegen.aggregate.map.twolevel.enable")
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

  val NDV_MAX_ERROR =
    buildConf("spark.sql.statistics.ndv.maxError")
      .internal()
      .doc("The maximum estimation error allowed in HyperLogLog++ algorithm when generating " +
        "column level statistics.")
      .doubleConf
      .createWithDefault(0.05)

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

  val WINDOW_EXEC_BUFFER_SPILL_THRESHOLD =
    buildConf("spark.sql.windowExec.buffer.spill.threshold")
      .internal()
      .doc("Threshold for number of rows buffered in window operator")
      .intConf
      .createWithDefault(4096)

  val SORT_MERGE_JOIN_EXEC_BUFFER_SPILL_THRESHOLD =
    buildConf("spark.sql.sortMergeJoinExec.buffer.spill.threshold")
      .internal()
      .doc("Threshold for number of rows buffered in sort merge join operator")
      .intConf
      .createWithDefault(Int.MaxValue)

  val CARTESIAN_PRODUCT_EXEC_BUFFER_SPILL_THRESHOLD =
    buildConf("spark.sql.cartesianProductExec.buffer.spill.threshold")
      .internal()
      .doc("Threshold for number of rows buffered in cartesian product operator")
      .intConf
      .createWithDefault(UnsafeExternalSorter.DEFAULT_NUM_ELEMENTS_FOR_SPILL_THRESHOLD.toInt)

  object Deprecated {
    val MAPRED_REDUCE_TASKS = "mapred.reduce.tasks"
  }

  object Replaced {
    val MAPREDUCE_JOB_REDUCES = "mapreduce.job.reduces"
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
class SQLConf extends Serializable with Logging {
  import SQLConf._

  /** Only low degree of contention is expected for conf, thus NOT using ConcurrentHashMap. */
  @transient protected[spark] val settings = java.util.Collections.synchronizedMap(
    new java.util.HashMap[String, String]())

  @transient private val reader = new ConfigReader(settings)

  /** ************************ Spark SQL Params/Hints ******************* */

  def optimizerMaxIterations: Int = getConf(OPTIMIZER_MAX_ITERATIONS)

  def optimizerInSetConversionThreshold: Int = getConf(OPTIMIZER_INSET_CONVERSION_THRESHOLD)

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

  def streamingMetricsEnabled: Boolean = getConf(STREAMING_METRICS_ENABLED)

  def streamingProgressRetention: Int = getConf(STREAMING_PROGRESS_RETENTION)

  def filesMaxPartitionBytes: Long = getConf(FILES_MAX_PARTITION_BYTES)

  def filesOpenCostInBytes: Long = getConf(FILES_OPEN_COST_IN_BYTES)

  def ignoreCorruptFiles: Boolean = getConf(IGNORE_CORRUPT_FILES)

  def maxRecordsPerFile: Long = getConf(MAX_RECORDS_PER_FILE)

  def useCompression: Boolean = getConf(COMPRESS_CACHED)

  def parquetCompressionCodec: String = getConf(PARQUET_COMPRESSION)

  def parquetCacheMetadata: Boolean = getConf(PARQUET_CACHE_METADATA)

  def parquetVectorizedReaderEnabled: Boolean = getConf(PARQUET_VECTORIZED_READER_ENABLED)

  def columnBatchSize: Int = getConf(COLUMN_BATCH_SIZE)

  def numShufflePartitions: Int = getConf(SHUFFLE_PARTITIONS)

  def targetPostShuffleInputSize: Long =
    getConf(SHUFFLE_TARGET_POSTSHUFFLE_INPUT_SIZE)

  def adaptiveExecutionEnabled: Boolean = getConf(ADAPTIVE_EXECUTION_ENABLED)

  def minNumPostShufflePartitions: Int =
    getConf(SHUFFLE_MIN_NUM_POSTSHUFFLE_PARTITIONS)

  def minBatchesToRetain: Int = getConf(MIN_BATCHES_TO_RETAIN)

  def parquetFilterPushDown: Boolean = getConf(PARQUET_FILTER_PUSHDOWN_ENABLED)

  def orcFilterPushDown: Boolean = getConf(ORC_FILTER_PUSHDOWN_ENABLED)

  def verifyPartitionPath: Boolean = getConf(HIVE_VERIFY_PARTITION_PATH)

  def metastorePartitionPruning: Boolean = getConf(HIVE_METASTORE_PARTITION_PRUNING)

  def manageFilesourcePartitions: Boolean = getConf(HIVE_MANAGE_FILESOURCE_PARTITIONS)

  def filesourcePartitionFileCacheSize: Long = getConf(HIVE_FILESOURCE_PARTITION_FILE_CACHE_SIZE)

  def caseSensitiveInferenceMode: HiveCaseSensitiveInferenceMode.Value =
    HiveCaseSensitiveInferenceMode.withName(getConf(HIVE_CASE_SENSITIVE_INFERENCE))

  def gatherFastStats: Boolean = getConf(GATHER_FASTSTAT)

  def optimizerMetadataOnly: Boolean = getConf(OPTIMIZER_METADATA_ONLY)

  def wholeStageEnabled: Boolean = getConf(WHOLESTAGE_CODEGEN_ENABLED)

  def wholeStageMaxNumFields: Int = getConf(WHOLESTAGE_MAX_NUM_FIELDS)

  def wholeStageFallback: Boolean = getConf(WHOLESTAGE_FALLBACK)

  def maxCaseBranchesForCodegen: Int = getConf(MAX_CASES_BRANCHES)

  def tableRelationCacheSize: Int =
    getConf(StaticSQLConf.FILESOURCE_TABLE_RELATION_CACHE_SIZE)

  def exchangeReuseEnabled: Boolean = getConf(EXCHANGE_REUSE_ENABLED)

  def caseSensitiveAnalysis: Boolean = getConf(SQLConf.CASE_SENSITIVE)

  def constraintPropagationEnabled: Boolean = getConf(CONSTRAINT_PROPAGATION_ENABLED)

  def escapedStringLiterals: Boolean = getConf(ESCAPED_STRING_LITERALS)

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

  def fallBackToHdfsForStatsEnabled: Boolean = getConf(ENABLE_FALL_BACK_TO_HDFS_FOR_STATS)

  def preferSortMergeJoin: Boolean = getConf(PREFER_SORTMERGEJOIN)

  def enableRadixSort: Boolean = getConf(RADIX_SORT_ENABLED)

  def defaultSizeInBytes: Long = getConf(DEFAULT_SIZE_IN_BYTES)

  def isParquetSchemaMergingEnabled: Boolean = getConf(PARQUET_SCHEMA_MERGING_ENABLED)

  def isParquetSchemaRespectSummaries: Boolean = getConf(PARQUET_SCHEMA_RESPECT_SUMMARIES)

  def parquetOutputCommitterClass: String = getConf(PARQUET_OUTPUT_COMMITTER_CLASS)

  def isParquetBinaryAsString: Boolean = getConf(PARQUET_BINARY_AS_STRING)

  def isParquetINT96AsTimestamp: Boolean = getConf(PARQUET_INT96_AS_TIMESTAMP)

  def isParquetINT64AsTimestampMillis: Boolean = getConf(PARQUET_INT64_AS_TIMESTAMP_MILLIS)

  def writeLegacyParquetFormat: Boolean = getConf(PARQUET_WRITE_LEGACY_FORMAT)

  def inMemoryPartitionPruning: Boolean = getConf(IN_MEMORY_PARTITION_PRUNING)

  def columnNameOfCorruptRecord: String = getConf(COLUMN_NAME_OF_CORRUPT_RECORD)

  def broadcastTimeout: Int = getConf(BROADCAST_TIMEOUT)

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

  def ndvMaxError: Double = getConf(NDV_MAX_ERROR)

  def cboEnabled: Boolean = getConf(SQLConf.CBO_ENABLED)

  def joinReorderEnabled: Boolean = getConf(SQLConf.JOIN_REORDER_ENABLED)

  def joinReorderDPThreshold: Int = getConf(SQLConf.JOIN_REORDER_DP_THRESHOLD)

  def joinReorderCardWeight: Double = getConf(SQLConf.JOIN_REORDER_CARD_WEIGHT)

  def joinReorderDPStarFilter: Boolean = getConf(SQLConf.JOIN_REORDER_DP_STAR_FILTER)

  def windowExecBufferSpillThreshold: Int = getConf(WINDOW_EXEC_BUFFER_SPILL_THRESHOLD)

  def sortMergeJoinExecBufferSpillThreshold: Int =
    getConf(SORT_MERGE_JOIN_EXEC_BUFFER_SPILL_THRESHOLD)

  def cartesianProductExecBufferSpillThreshold: Int =
    getConf(CARTESIAN_PRODUCT_EXEC_BUFFER_SPILL_THRESHOLD)

  def maxNestedViewDepth: Int = getConf(SQLConf.MAX_NESTED_VIEW_DEPTH)

  def starSchemaDetection: Boolean = getConf(STARSCHEMA_DETECTION)

  def starSchemaFTRatio: Double = getConf(STARSCHEMA_FACT_TABLE_RATIO)

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
      (entry.key, getConfString(entry.key, entry.defaultValueString), entry.doc)
    }.toSeq
  }

  /**
   * Return whether a given key is set in this [[SQLConf]].
   */
  def contains(key: String): Boolean = {
    settings.containsKey(key)
  }

  private def setConfWithCheck(key: String, value: String): Unit = {
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
}
