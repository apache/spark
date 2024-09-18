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

import java.util.{Locale, Properties, TimeZone}
import java.util
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import java.util.zip.Deflater

import scala.collection.immutable
import scala.jdk.CollectionConverters._
import scala.util.Try
import scala.util.control.NonFatal
import scala.util.matching.Regex

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.OutputCommitter

import org.apache.spark.{ErrorMessageFormat, SparkConf, SparkContext, SparkException, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.io.CompressionCodec
import org.apache.spark.network.util.ByteUnit
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.analysis.{HintErrorLogger, Resolver}
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator
import org.apache.spark.sql.catalyst.plans.logical.HintErrorHandler
import org.apache.spark.sql.catalyst.util.{CollationFactory, DateTimeUtils}
import org.apache.spark.sql.connector.catalog.CatalogManager.SESSION_CATALOG_NAME
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.types.{AtomicType, StringType, TimestampNTZType, TimestampType}
import org.apache.spark.storage.{StorageLevel, StorageLevelMapper}
import org.apache.spark.unsafe.array.ByteArrayMethods
import org.apache.spark.util.{Utils, VersionUtils}

////////////////////////////////////////////////////////////////////////////////////////////////////
// This file defines the configuration options for Spark SQL.
////////////////////////////////////////////////////////////////////////////////////////////////////


object SQLConf {

  private[this] val sqlConfEntriesUpdateLock = new Object

  @volatile
  private[this] var sqlConfEntries: util.Map[String, ConfigEntry[_]] = util.Collections.emptyMap()

  private[this] val staticConfKeysUpdateLock = new Object

  @volatile
  private[this] var staticConfKeys: java.util.Set[String] = util.Collections.emptySet()

  private def register(entry: ConfigEntry[_]): Unit = sqlConfEntriesUpdateLock.synchronized {
    require(!sqlConfEntries.containsKey(entry.key),
      s"Duplicate SQLConfigEntry. ${entry.key} has been registered")
    val updatedMap = new java.util.HashMap[String, ConfigEntry[_]](sqlConfEntries)
    updatedMap.put(entry.key, entry)
    sqlConfEntries = updatedMap
  }

  // For testing only
  private[sql] def unregister(entry: ConfigEntry[_]): Unit = sqlConfEntriesUpdateLock.synchronized {
    val updatedMap = new java.util.HashMap[String, ConfigEntry[_]](sqlConfEntries)
    updatedMap.remove(entry.key)
    sqlConfEntries = updatedMap
  }

  private[internal] def getConfigEntry(key: String): ConfigEntry[_] = {
    sqlConfEntries.get(key)
  }

  private[internal] def getConfigEntries(): util.Collection[ConfigEntry[_]] = {
    sqlConfEntries.values()
  }

  private[internal] def containsConfigEntry(entry: ConfigEntry[_]): Boolean = {
    getConfigEntry(entry.key) == entry
  }

  private[sql] def containsConfigKey(key: String): Boolean = {
    sqlConfEntries.containsKey(key)
  }

  def registerStaticConfigKey(key: String): Unit = staticConfKeysUpdateLock.synchronized {
    val updated = new util.HashSet[String](staticConfKeys)
    updated.add(key)
    staticConfKeys = updated
  }

  def isStaticConfigKey(key: String): Boolean = staticConfKeys.contains(key)

  def buildConf(key: String): ConfigBuilder = ConfigBuilder(key).onCreate(register)

  def buildStaticConf(key: String): ConfigBuilder = {
    ConfigBuilder(key).onCreate { entry =>
      SQLConf.registerStaticConfigKey(entry.key)
      SQLConf.register(entry)
    }
  }

  /**
   * Merge all non-static configs to the SQLConf. For example, when the 1st [[SparkSession]] and
   * the global [[SharedState]] have been initialized, all static configs have taken affect and
   * should not be set to other values. Other later created sessions should respect all static
   * configs and only be able to change non-static configs.
   */
  private[sql] def mergeNonStaticSQLConfigs(
      sqlConf: SQLConf,
      configs: Map[String, String]): Unit = {
    for ((k, v) <- configs if !staticConfKeys.contains(k)) {
      sqlConf.setConfString(k, v)
    }
  }

  /**
   * Extract entries from `SparkConf` and put them in the `SQLConf`
   */
  private[sql] def mergeSparkConf(sqlConf: SQLConf, sparkConf: SparkConf): Unit = {
    sparkConf.getAll.foreach { case (k, v) =>
      sqlConf.setConfString(k, v)
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
    val old = existingConf.get()
    existingConf.set(conf)
    try {
      f
    } finally {
      if (old != null) {
        existingConf.set(old)
      } else {
        existingConf.remove()
      }
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

  // Make sure SqlApiConf is always in sync with SQLConf. SqlApiConf will always try to
  // load SqlConf to make sure both classes are in sync from the get go.
  SqlApiConfHelper.setConfGetter(() => SQLConf.get)

  /**
   * Returns the active config object within the current scope. If there is an active SparkSession,
   * the proper SQLConf associated with the thread's active session is used. If it's called from
   * tasks in the executor side, a SQLConf will be created from job local properties, which are set
   * and propagated from the driver side, unless a `SQLConf` has been set in the scope by
   * `withExistingConf` as done for propagating SQLConf for operations performed on RDDs created
   * from DataFrames.
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
    if (Utils.isInRunningSparkTask) {
      val conf = existingConf.get()
      if (conf != null) {
        conf
      } else {
        new ReadOnlySQLConf(TaskContext.get())
      }
    } else {
      val isSchedulerEventLoopThread = SparkContext.getActive
        .flatMap { sc => Option(sc.dagScheduler) }
        .map(_.eventProcessLoop.eventThread)
        .exists(_.getId == Thread.currentThread().getId)
      if (isSchedulerEventLoopThread) {
        // DAGScheduler event loop thread does not have an active SparkSession, the `confGetter`
        // will return `fallbackConf` which is unexpected. Here we require the caller to get the
        // conf within `withExistingConf`, otherwise fail the query.
        val conf = existingConf.get()
        if (conf != null) {
          conf
        } else if (Utils.isTesting) {
          throw QueryExecutionErrors.cannotGetSQLConfInSchedulerEventLoopThreadError()
        } else {
          confGetter.get()()
        }
      } else {
        val conf = existingConf.get()
        if (conf != null) {
          conf
        } else {
          confGetter.get()()
        }
      }
    }
  }

  val ANALYZER_MAX_ITERATIONS = buildConf("spark.sql.analyzer.maxIterations")
    .internal()
    .doc("The max number of iterations the analyzer runs.")
    .version("3.0.0")
    .intConf
    .createWithDefault(100)

  val MULTI_COMMUTATIVE_OP_OPT_THRESHOLD =
    buildConf("spark.sql.analyzer.canonicalization.multiCommutativeOpMemoryOptThreshold")
      .internal()
      .doc("The minimum number of operands in a commutative expression tree to" +
        " invoke the MultiCommutativeOp memory optimization during canonicalization.")
      .version("3.4.0")
      .intConf
      .createWithDefault(3)

  val OPTIMIZER_EXCLUDED_RULES = buildConf("spark.sql.optimizer.excludedRules")
    .doc("Configures a list of rules to be disabled in the optimizer, in which the rules are " +
      "specified by their rule names and separated by comma. It is not guaranteed that all the " +
      "rules in this configuration will eventually be excluded, as some rules are necessary " +
      "for correctness. The optimizer will log the rules that have indeed been excluded.")
    .version("2.4.0")
    .stringConf
    .createOptional

  val OPTIMIZER_MAX_ITERATIONS = buildConf("spark.sql.optimizer.maxIterations")
    .internal()
    .doc("The max number of iterations the optimizer runs.")
    .version("2.0.0")
    .intConf
    .createWithDefault(100)

  val OPTIMIZER_INSET_CONVERSION_THRESHOLD =
    buildConf("spark.sql.optimizer.inSetConversionThreshold")
      .internal()
      .doc("The threshold of set size for InSet conversion.")
      .version("2.0.0")
      .intConf
      .createWithDefault(10)

  val OPTIMIZER_INSET_SWITCH_THRESHOLD =
    buildConf("spark.sql.optimizer.inSetSwitchThreshold")
      .internal()
      .doc("Configures the max set size in InSet for which Spark will generate code with " +
        "switch statements. This is applicable only to bytes, shorts, ints, dates.")
      .version("3.0.0")
      .intConf
      .checkValue(threshold => threshold >= 0 && threshold <= 600, "The max set size " +
        "for using switch statements in InSet must be non-negative and less than or equal to 600")
      .createWithDefault(400)

  val PLAN_CHANGE_LOG_LEVEL = buildConf("spark.sql.planChangeLog.level")
    .internal()
    .doc("Configures the log level for logging the change from the original plan to the new " +
      "plan after a rule or batch is applied. The value can be 'trace', 'debug', 'info', " +
      "'warn', or 'error'. The default log level is 'trace'.")
    .version("3.1.0")
    .stringConf
    .transform(_.toUpperCase(Locale.ROOT))
    .checkValue(logLevel => Set("TRACE", "DEBUG", "INFO", "WARN", "ERROR").contains(logLevel),
      "Invalid value for 'spark.sql.planChangeLog.level'. Valid values are " +
        "'trace', 'debug', 'info', 'warn' and 'error'.")
    .createWithDefault("trace")

  val PLAN_CHANGE_LOG_RULES = buildConf("spark.sql.planChangeLog.rules")
    .internal()
    .doc("Configures a list of rules for logging plan changes, in which the rules are " +
      "specified by their rule names and separated by comma.")
    .version("3.1.0")
    .stringConf
    .createOptional

  val PLAN_CHANGE_LOG_BATCHES = buildConf("spark.sql.planChangeLog.batches")
    .internal()
    .doc("Configures a list of batches for logging plan changes, in which the batches " +
      "are specified by their batch names and separated by comma.")
    .version("3.1.0")
    .stringConf
    .createOptional

  val PLAN_CHANGE_VALIDATION = buildConf("spark.sql.planChangeValidation")
    .internal()
    .doc("If true, Spark will validate all the plan changes made by analyzer/optimizer and other " +
      "catalyst rules, to make sure every rule returns a valid plan")
    .version("3.4.0")
    .booleanConf
    .createWithDefault(false)

  val ALLOW_NAMED_FUNCTION_ARGUMENTS = buildConf("spark.sql.allowNamedFunctionArguments")
    .doc("If true, Spark will turn on support for named parameters for all functions that has" +
      " it implemented.")
    .version("3.5.0")
    .booleanConf
    .createWithDefault(true)

  val EXTENDED_EXPLAIN_PROVIDERS = buildConf("spark.sql.extendedExplainProviders")
    .doc("A comma-separated list of classes that implement the" +
      " org.apache.spark.sql.ExtendedExplainGenerator trait. If provided, Spark will print" +
      " extended plan information from the providers in explain plan and in the UI")
    .version("4.0.0")
    .stringConf
    .toSequence
    .createOptional

  val DYNAMIC_PARTITION_PRUNING_ENABLED =
    buildConf("spark.sql.optimizer.dynamicPartitionPruning.enabled")
      .doc("When true, we will generate predicate for partition column when it's used as join key")
      .version("3.0.0")
      .booleanConf
      .createWithDefault(true)

  val DYNAMIC_PARTITION_PRUNING_USE_STATS =
    buildConf("spark.sql.optimizer.dynamicPartitionPruning.useStats")
      .internal()
      .doc("When true, distinct count statistics will be used for computing the data size of the " +
        "partitioned table after dynamic partition pruning, in order to evaluate if it is worth " +
        "adding an extra subquery as the pruning filter if broadcast reuse is not applicable.")
      .version("3.0.0")
      .booleanConf
      .createWithDefault(true)

  val DYNAMIC_PARTITION_PRUNING_FALLBACK_FILTER_RATIO =
    buildConf("spark.sql.optimizer.dynamicPartitionPruning.fallbackFilterRatio")
      .internal()
      .doc("When statistics are not available or configured not to be used, this config will be " +
        "used as the fallback filter ratio for computing the data size of the partitioned table " +
        "after dynamic partition pruning, in order to evaluate if it is worth adding an extra " +
        "subquery as the pruning filter if broadcast reuse is not applicable.")
      .version("3.0.0")
      .doubleConf
      .createWithDefault(0.5)

  val DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY =
    buildConf("spark.sql.optimizer.dynamicPartitionPruning.reuseBroadcastOnly")
      .internal()
      .doc("When true, dynamic partition pruning will only apply when the broadcast exchange of " +
        "a broadcast hash join operation can be reused as the dynamic pruning filter.")
      .version("3.0.0")
      .booleanConf
      .createWithDefault(true)

  val RUNTIME_FILTER_NUMBER_THRESHOLD =
    buildConf("spark.sql.optimizer.runtimeFilter.number.threshold")
      .doc("The total number of injected runtime filters (non-DPP) for a single " +
        "query. This is to prevent driver OOMs with too many Bloom filters.")
      .version("3.3.0")
      .intConf
      .checkValue(threshold => threshold >= 0, "The threshold should be >= 0")
      .createWithDefault(10)

  val RUNTIME_BLOOM_FILTER_ENABLED =
    buildConf("spark.sql.optimizer.runtime.bloomFilter.enabled")
      .doc("When true and if one side of a shuffle join has a selective predicate, we attempt " +
        "to insert a bloom filter in the other side to reduce the amount of shuffle data.")
      .version("3.3.0")
      .booleanConf
      .createWithDefault(true)

  val RUNTIME_BLOOM_FILTER_CREATION_SIDE_THRESHOLD =
    buildConf("spark.sql.optimizer.runtime.bloomFilter.creationSideThreshold")
      .doc("Size threshold of the bloom filter creation side plan. Estimated size needs to be " +
        "under this value to try to inject bloom filter.")
      .version("3.3.0")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("10MB")

  val RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD =
    buildConf("spark.sql.optimizer.runtime.bloomFilter.applicationSideScanSizeThreshold")
      .doc("Byte size threshold of the Bloom filter application side plan's aggregated scan " +
        "size. Aggregated scan byte size of the Bloom filter application side needs to be over " +
        "this value to inject a bloom filter.")
      .version("3.3.0")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("10GB")

  val RUNTIME_BLOOM_FILTER_EXPECTED_NUM_ITEMS =
    buildConf("spark.sql.optimizer.runtime.bloomFilter.expectedNumItems")
      .doc("The default number of expected items for the runtime bloomfilter")
      .version("3.3.0")
      .longConf
      .createWithDefault(1000000L)

  val RUNTIME_BLOOM_FILTER_MAX_NUM_ITEMS =
    buildConf("spark.sql.optimizer.runtime.bloomFilter.maxNumItems")
      .doc("The max allowed number of expected items for the runtime bloom filter")
      .version("3.3.0")
      .longConf
      .createWithDefault(4000000L)


  val RUNTIME_BLOOM_FILTER_NUM_BITS =
    buildConf("spark.sql.optimizer.runtime.bloomFilter.numBits")
      .doc("The default number of bits to use for the runtime bloom filter")
      .version("3.3.0")
      .longConf
      .createWithDefault(8388608L)

  val RUNTIME_BLOOM_FILTER_MAX_NUM_BITS =
    buildConf("spark.sql.optimizer.runtime.bloomFilter.maxNumBits")
      .doc("The max number of bits to use for the runtime bloom filter")
      .version("3.3.0")
      .longConf
      .createWithDefault(67108864L)

  val RUNTIME_ROW_LEVEL_OPERATION_GROUP_FILTER_ENABLED =
    buildConf("spark.sql.optimizer.runtime.rowLevelOperationGroupFilter.enabled")
      .doc("Enables runtime group filtering for group-based row-level operations. " +
        "Data sources that replace groups of data (e.g. files, partitions) may prune entire " +
        "groups using provided data source filters when planning a row-level operation scan. " +
        "However, such filtering is limited as not all expressions can be converted into data " +
        "source filters and some expressions can only be evaluated by Spark (e.g. subqueries). " +
        "Since rewriting groups is expensive, Spark can execute a query at runtime to find what " +
        "records match the condition of the row-level operation. The information about matching " +
        "records will be passed back to the row-level operation scan, allowing data sources to " +
        "discard groups that don't have to be rewritten.")
      .version("3.4.0")
      .booleanConf
      .createWithDefault(true)

  val PLANNED_WRITE_ENABLED = buildConf("spark.sql.optimizer.plannedWrite.enabled")
    .internal()
    .doc("When set to true, Spark optimizer will add logical sort operators to V1 write commands " +
      "if needed so that `FileFormatWriter` does not need to insert physical sorts.")
    .version("3.4.0")
    .booleanConf
    .createWithDefault(true)

  val EXPRESSION_PROJECTION_CANDIDATE_LIMIT =
    buildConf("spark.sql.optimizer.expressionProjectionCandidateLimit")
      .doc("The maximum number of the candidate of output expressions whose alias are replaced." +
        " It can preserve the output partitioning and ordering." +
        " Negative value means disable this optimization.")
      .internal()
      .version("3.4.0")
      .intConf
      .createWithDefault(100)

  val COMPRESS_CACHED = buildConf("spark.sql.inMemoryColumnarStorage.compressed")
    .doc("When set to true Spark SQL will automatically select a compression codec for each " +
      "column based on statistics of the data.")
    .version("1.0.1")
    .booleanConf
    .createWithDefault(true)

  val COLUMN_BATCH_SIZE = buildConf("spark.sql.inMemoryColumnarStorage.batchSize")
    .doc("Controls the size of batches for columnar caching.  Larger batch sizes can improve " +
      "memory utilization and compression, but risk OOMs when caching data.")
    .version("1.1.1")
    .intConf
    .createWithDefault(10000)

  val VECTORIZED_HUGE_VECTOR_RESERVE_RATIO =
    buildConf("spark.sql.inMemoryColumnarStorage.hugeVectorReserveRatio")
      .doc("When spark.sql.inMemoryColumnarStorage.hugeVectorThreshold <= 0 or the required " +
        "memory is smaller than spark.sql.inMemoryColumnarStorage.hugeVectorThreshold, spark " +
        "reserves required memory * 2 memory; otherwise, spark reserves " +
        "required memory * this ratio memory, and will release this column vector memory before " +
        "reading the next batch rows.")
      .version("4.0.0")
      .doubleConf
      .createWithDefault(1.2)

  val VECTORIZED_HUGE_VECTOR_THRESHOLD =
    buildConf("spark.sql.inMemoryColumnarStorage.hugeVectorThreshold")
      .doc("When the required memory is larger than this, spark reserves required memory * " +
        s"${VECTORIZED_HUGE_VECTOR_RESERVE_RATIO.key} memory next time and release this column " +
        s"vector memory before reading the next batch rows. -1 means disabling the optimization.")
      .version("4.0.0")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefault(-1)

  val IN_MEMORY_PARTITION_PRUNING =
    buildConf("spark.sql.inMemoryColumnarStorage.partitionPruning")
      .internal()
      .doc("When true, enable partition pruning for in-memory columnar tables.")
      .version("1.2.0")
      .booleanConf
      .createWithDefault(true)

  val IN_MEMORY_TABLE_SCAN_STATISTICS_ENABLED =
    buildConf("spark.sql.inMemoryTableScanStatistics.enable")
      .internal()
      .doc("When true, enable in-memory table scan accumulators.")
      .version("3.0.0")
      .booleanConf
      .createWithDefault(false)

  val CACHE_VECTORIZED_READER_ENABLED =
    buildConf("spark.sql.inMemoryColumnarStorage.enableVectorizedReader")
      .doc("Enables vectorized reader for columnar caching.")
      .version("2.3.1")
      .booleanConf
      .createWithDefault(true)

  val COLUMN_VECTOR_OFFHEAP_ENABLED =
    buildConf("spark.sql.columnVector.offheap.enabled")
      .internal()
      .doc("When true, use OffHeapColumnVector in ColumnarBatch.")
      .version("2.3.0")
      .fallbackConf(MEMORY_OFFHEAP_ENABLED)

  val PREFER_SORTMERGEJOIN = buildConf("spark.sql.join.preferSortMergeJoin")
    .internal()
    .doc("When true, prefer sort merge join over shuffled hash join. " +
      "Sort merge join consumes less memory than shuffled hash join and it works efficiently " +
      "when both join tables are large. On the other hand, shuffled hash join can improve " +
      "performance (e.g., of full outer joins) when one of join tables is much smaller.")
    .version("2.0.0")
    .booleanConf
    .createWithDefault(true)

  val REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION =
    buildConf("spark.sql.requireAllClusterKeysForCoPartition")
      .internal()
      .doc("When true, the planner requires all the clustering keys as the hash partition keys " +
        "of the children, to eliminate the shuffles for the operator that needs its children to " +
        "be co-partitioned, such as JOIN node. This is to avoid data skews which can lead to " +
        "significant performance regression if shuffles are eliminated.")
      .version("3.3.0")
      .booleanConf
      .createWithDefault(true)

  val REQUIRE_ALL_CLUSTER_KEYS_FOR_DISTRIBUTION =
    buildConf("spark.sql.requireAllClusterKeysForDistribution")
      .internal()
      .doc("When true, the planner requires all the clustering keys as the partition keys " +
        "(with same ordering) of the children, to eliminate the shuffle for the operator that " +
        "requires its children be clustered distributed, such as AGGREGATE and WINDOW node. " +
        "This is to avoid data skews which can lead to significant performance regression if " +
        "shuffle is eliminated.")
      .version("3.3.0")
      .booleanConf
      .createWithDefault(false)

  val MAX_SINGLE_PARTITION_BYTES = buildConf("spark.sql.maxSinglePartitionBytes")
    .doc("The maximum number of bytes allowed for a single partition. Otherwise, The planner " +
      "will introduce shuffle to improve parallelism.")
    .version("3.4.0")
    .bytesConf(ByteUnit.BYTE)
    .createWithDefaultString("128m")

  val RADIX_SORT_ENABLED = buildConf("spark.sql.sort.enableRadixSort")
    .internal()
    .doc("When true, enable use of radix sort when possible. Radix sort is much faster but " +
      "requires additional memory to be reserved up-front. The memory overhead may be " +
      "significant when sorting very small rows (up to 50% more in this case).")
    .version("2.0.0")
    .booleanConf
    .createWithDefault(true)

  val AUTO_BROADCASTJOIN_THRESHOLD = buildConf("spark.sql.autoBroadcastJoinThreshold")
    .doc("Configures the maximum size in bytes for a table that will be broadcast to all worker " +
      "nodes when performing a join.  By setting this value to -1 broadcasting can be disabled.")
    .version("1.1.0")
    .bytesConf(ByteUnit.BYTE)
    .createWithDefaultString("10MB")

  val SHUFFLE_HASH_JOIN_FACTOR = buildConf("spark.sql.shuffledHashJoinFactor")
    .doc("The shuffle hash join can be selected if the data size of small" +
      " side multiplied by this factor is still smaller than the large side.")
    .version("3.3.0")
    .intConf
    .checkValue(_ >= 1, "The shuffle hash join factor cannot be negative.")
    .createWithDefault(3)

  val LIMIT_INITIAL_NUM_PARTITIONS = buildConf("spark.sql.limit.initialNumPartitions")
    .internal()
    .doc("Initial number of partitions to try when executing a take on a query. Higher values " +
      "lead to more partitions read. Lower values might lead to longer execution times as more" +
      "jobs will be run")
    .version("3.4.0")
    .intConf
    .checkValue(_ > 0, "value should be positive")
    .createWithDefault(1)

  val LIMIT_SCALE_UP_FACTOR = buildConf("spark.sql.limit.scaleUpFactor")
    .internal()
    .doc("Minimal increase rate in number of partitions between attempts when executing a take " +
      "on a query. Higher values lead to more partitions read. Lower values might lead to " +
      "longer execution times as more jobs will be run")
    .version("2.1.1")
    .intConf
    .createWithDefault(4)

  val ADVANCED_PARTITION_PREDICATE_PUSHDOWN =
    buildConf("spark.sql.hive.advancedPartitionPredicatePushdown.enabled")
      .internal()
      .doc("When true, advanced partition predicate pushdown into Hive metastore is enabled.")
      .version("2.3.0")
      .booleanConf
      .createWithDefault(true)

  val LEAF_NODE_DEFAULT_PARALLELISM = buildConf("spark.sql.leafNodeDefaultParallelism")
    .doc("The default parallelism of Spark SQL leaf nodes that produce data, such as the file " +
      "scan node, the local data scan node, the range node, etc. The default value of this " +
      "config is 'SparkContext#defaultParallelism'.")
    .version("3.2.0")
    .intConf
    .checkValue(_ > 0, "The value of spark.sql.leafNodeDefaultParallelism must be positive.")
    .createOptional

  val SHUFFLE_PARTITIONS = buildConf("spark.sql.shuffle.partitions")
    .doc("The default number of partitions to use when shuffling data for joins or aggregations. " +
      "Note: For structured streaming, this configuration cannot be changed between query " +
      "restarts from the same checkpoint location.")
    .version("1.1.0")
    .intConf
    .checkValue(_ > 0, "The value of spark.sql.shuffle.partitions must be positive")
    .createWithDefault(200)

  val SHUFFLE_TARGET_POSTSHUFFLE_INPUT_SIZE =
    buildConf("spark.sql.adaptive.shuffle.targetPostShuffleInputSize")
      .internal()
      .doc("(Deprecated since Spark 3.0)")
      .version("1.6.0")
      .bytesConf(ByteUnit.BYTE)
      .checkValue(_ > 0, "advisoryPartitionSizeInBytes must be positive")
      .createWithDefaultString("64MB")

  val ADAPTIVE_EXECUTION_ENABLED = buildConf("spark.sql.adaptive.enabled")
    .doc("When true, enable adaptive query execution, which re-optimizes the query plan in the " +
      "middle of query execution, based on accurate runtime statistics.")
    .version("1.6.0")
    .booleanConf
    .createWithDefault(true)

  val ADAPTIVE_EXECUTION_FORCE_APPLY = buildConf("spark.sql.adaptive.forceApply")
    .internal()
    .doc("Adaptive query execution is skipped when the query does not have exchanges or " +
      "sub-queries. By setting this config to true (together with " +
      s"'${ADAPTIVE_EXECUTION_ENABLED.key}' set to true), Spark will force apply adaptive query " +
      "execution for all supported queries.")
    .version("3.0.0")
    .booleanConf
    .createWithDefault(false)

  val ADAPTIVE_EXECUTION_APPLY_FINAL_STAGE_SHUFFLE_OPTIMIZATIONS =
    buildConf("spark.sql.adaptive.applyFinalStageShuffleOptimizations")
      .internal()
      .doc("Configures whether adaptive query execution (if enabled) should apply shuffle " +
        "coalescing and local shuffle read optimization for the final query stage.")
      .version("3.4.2")
      .booleanConf
      .createWithDefault(true)

  val ADAPTIVE_EXECUTION_LOG_LEVEL = buildConf("spark.sql.adaptive.logLevel")
    .internal()
    .doc("Configures the log level for adaptive execution logging of plan changes. The value " +
      "can be 'trace', 'debug', 'info', 'warn', or 'error'. The default log level is 'debug'.")
    .version("3.0.0")
    .stringConf
    .transform(_.toUpperCase(Locale.ROOT))
    .checkValues(Set("TRACE", "DEBUG", "INFO", "WARN", "ERROR"))
    .createWithDefault("debug")

  val ADVISORY_PARTITION_SIZE_IN_BYTES =
    buildConf("spark.sql.adaptive.advisoryPartitionSizeInBytes")
      .doc("The advisory size in bytes of the shuffle partition during adaptive optimization " +
        s"(when ${ADAPTIVE_EXECUTION_ENABLED.key} is true). It takes effect when Spark " +
        "coalesces small shuffle partitions or splits skewed shuffle partition.")
      .version("3.0.0")
      .fallbackConf(SHUFFLE_TARGET_POSTSHUFFLE_INPUT_SIZE)

  val COALESCE_PARTITIONS_ENABLED =
    buildConf("spark.sql.adaptive.coalescePartitions.enabled")
      .doc(s"When true and '${ADAPTIVE_EXECUTION_ENABLED.key}' is true, Spark will coalesce " +
        "contiguous shuffle partitions according to the target size (specified by " +
        s"'${ADVISORY_PARTITION_SIZE_IN_BYTES.key}'), to avoid too many small tasks.")
      .version("3.0.0")
      .booleanConf
      .createWithDefault(true)

  val COALESCE_PARTITIONS_PARALLELISM_FIRST =
    buildConf("spark.sql.adaptive.coalescePartitions.parallelismFirst")
      .doc("When true, Spark does not respect the target size specified by " +
        s"'${ADVISORY_PARTITION_SIZE_IN_BYTES.key}' (default 64MB) when coalescing contiguous " +
        "shuffle partitions, but adaptively calculate the target size according to the default " +
        "parallelism of the Spark cluster. The calculated size is usually smaller than the " +
        "configured target size. This is to maximize the parallelism and avoid performance " +
        "regressions when enabling adaptive query execution. It's recommended to set this " +
        "config to false on a busy cluster to make resource utilization more efficient (not many " +
        "small tasks).")
      .version("3.2.0")
      .booleanConf
      .createWithDefault(true)

  val COALESCE_PARTITIONS_MIN_PARTITION_SIZE =
    buildConf("spark.sql.adaptive.coalescePartitions.minPartitionSize")
      .doc("The minimum size of shuffle partitions after coalescing. This is useful when the " +
        "adaptively calculated target size is too small during partition coalescing.")
      .version("3.2.0")
      .bytesConf(ByteUnit.BYTE)
      .checkValue(_ > 0, "minPartitionSize must be positive")
      .createWithDefaultString("1MB")

  val COALESCE_PARTITIONS_MIN_PARTITION_NUM =
    buildConf("spark.sql.adaptive.coalescePartitions.minPartitionNum")
      .internal()
      .doc("(deprecated) The suggested (not guaranteed) minimum number of shuffle partitions " +
        "after coalescing. If not set, the default value is the default parallelism of the " +
        "Spark cluster. This configuration only has an effect when " +
        s"'${ADAPTIVE_EXECUTION_ENABLED.key}' and " +
        s"'${COALESCE_PARTITIONS_ENABLED.key}' are both true.")
      .version("3.0.0")
      .intConf
      .checkValue(_ > 0, "The minimum number of partitions must be positive.")
      .createOptional

  val COALESCE_PARTITIONS_INITIAL_PARTITION_NUM =
    buildConf("spark.sql.adaptive.coalescePartitions.initialPartitionNum")
      .doc("The initial number of shuffle partitions before coalescing. If not set, it equals to " +
        s"${SHUFFLE_PARTITIONS.key}. This configuration only has an effect when " +
        s"'${ADAPTIVE_EXECUTION_ENABLED.key}' and '${COALESCE_PARTITIONS_ENABLED.key}' " +
        "are both true.")
      .version("3.0.0")
      .intConf
      .checkValue(_ > 0, "The initial number of partitions must be positive.")
      .createOptional

  val DEFAULT_COLLATION =
    buildConf(SqlApiConfHelper.DEFAULT_COLLATION)
      .doc("Sets default collation to use for string literals, parameter markers or the string" +
        " produced by a builtin function such as to_char or CAST")
      .version("4.0.0")
      .stringConf
      .checkValue(
        collationName => {
          try {
            CollationFactory.fetchCollation(collationName)
            true
          } catch {
            case e: SparkException if e.getErrorClass == "COLLATION_INVALID_NAME" => false
          }
        },
        "DEFAULT_COLLATION",
        collationName => Map(
          "proposals" -> CollationFactory.getClosestSuggestionsOnInvalidName(collationName, 3)))
      .createWithDefault("UTF8_BINARY")

  val ICU_CASE_MAPPINGS_ENABLED =
    buildConf("spark.sql.icu.caseMappings.enabled")
      .doc("When enabled we use the ICU library (instead of the JVM) to implement case mappings" +
        " for strings under UTF8_BINARY collation.")
      .version("4.0.0")
      .booleanConf
      .createWithDefault(true)

  val FETCH_SHUFFLE_BLOCKS_IN_BATCH =
    buildConf("spark.sql.adaptive.fetchShuffleBlocksInBatch")
      .internal()
      .doc("Whether to fetch the contiguous shuffle blocks in batch. Instead of fetching blocks " +
        "one by one, fetching contiguous shuffle blocks for the same map task in batch can " +
        "reduce IO and improve performance. Note, multiple contiguous blocks exist in single " +
        s"fetch request only happen when '${ADAPTIVE_EXECUTION_ENABLED.key}' and " +
        s"'${COALESCE_PARTITIONS_ENABLED.key}' are both true. This feature also depends " +
        "on a relocatable serializer, the concatenation support codec in use, the new version " +
        "shuffle fetch protocol and io encryption is disabled.")
      .version("3.0.0")
      .booleanConf
      .createWithDefault(true)

  val LOCAL_SHUFFLE_READER_ENABLED =
    buildConf("spark.sql.adaptive.localShuffleReader.enabled")
      .doc(s"When true and '${ADAPTIVE_EXECUTION_ENABLED.key}' is true, Spark tries to use local " +
        "shuffle reader to read the shuffle data when the shuffle partitioning is not needed, " +
        "for example, after converting sort-merge join to broadcast-hash join.")
      .version("3.0.0")
      .booleanConf
      .createWithDefault(true)

  val SKEW_JOIN_ENABLED =
    buildConf("spark.sql.adaptive.skewJoin.enabled")
      .doc(s"When true and '${ADAPTIVE_EXECUTION_ENABLED.key}' is true, Spark dynamically " +
        "handles skew in shuffled join (sort-merge and shuffled hash) by splitting (and " +
        "replicating if needed) skewed partitions.")
      .version("3.0.0")
      .booleanConf
      .createWithDefault(true)

  val SKEW_JOIN_SKEWED_PARTITION_FACTOR =
    buildConf("spark.sql.adaptive.skewJoin.skewedPartitionFactor")
      .doc("A partition is considered as skewed if its size is larger than this factor " +
        "multiplying the median partition size and also larger than " +
        "'spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes'")
      .version("3.0.0")
      .doubleConf
      .checkValue(_ >= 0, "The skew factor cannot be negative.")
      .createWithDefault(5.0)

  val SKEW_JOIN_SKEWED_PARTITION_THRESHOLD =
    buildConf("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes")
      .doc("A partition is considered as skewed if its size in bytes is larger than this " +
        s"threshold and also larger than '${SKEW_JOIN_SKEWED_PARTITION_FACTOR.key}' " +
        "multiplying the median partition size. Ideally this config should be set larger " +
        s"than '${ADVISORY_PARTITION_SIZE_IN_BYTES.key}'.")
      .version("3.0.0")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("256MB")

  val NON_EMPTY_PARTITION_RATIO_FOR_BROADCAST_JOIN =
    buildConf("spark.sql.adaptive.nonEmptyPartitionRatioForBroadcastJoin")
      .internal()
      .doc("The relation with a non-empty partition ratio lower than this config will not be " +
        "considered as the build side of a broadcast-hash join in adaptive execution regardless " +
        "of its size.This configuration only has an effect when " +
        s"'${ADAPTIVE_EXECUTION_ENABLED.key}' is true.")
      .version("3.0.0")
      .doubleConf
      .checkValue(_ >= 0, "The non-empty partition ratio must be positive number.")
      .createWithDefault(0.2)

  val ADAPTIVE_OPTIMIZER_EXCLUDED_RULES =
    buildConf("spark.sql.adaptive.optimizer.excludedRules")
      .doc("Configures a list of rules to be disabled in the adaptive optimizer, in which the " +
        "rules are specified by their rule names and separated by comma. The optimizer will log " +
        "the rules that have indeed been excluded.")
      .version("3.1.0")
      .stringConf
      .createOptional

  val ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD =
    buildConf("spark.sql.adaptive.autoBroadcastJoinThreshold")
      .doc("Configures the maximum size in bytes for a table that will be broadcast to all " +
        "worker nodes when performing a join. By setting this value to -1 broadcasting can be " +
        s"disabled. The default value is same with ${AUTO_BROADCASTJOIN_THRESHOLD.key}. " +
        "Note that, this config is used only in adaptive framework.")
      .version("3.2.0")
      .bytesConf(ByteUnit.BYTE)
      .createOptional

  val ADAPTIVE_MAX_SHUFFLE_HASH_JOIN_LOCAL_MAP_THRESHOLD =
    buildConf("spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold")
      .doc("Configures the maximum size in bytes per partition that can be allowed to build " +
        "local hash map. If this value is not smaller than " +
        s"${ADVISORY_PARTITION_SIZE_IN_BYTES.key} and all the partition size are not larger " +
        "than this config, join selection prefer to use shuffled hash join instead of " +
        s"sort merge join regardless of the value of ${PREFER_SORTMERGEJOIN.key}.")
      .version("3.2.0")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefault(0L)

  val ADAPTIVE_OPTIMIZE_SKEWS_IN_REBALANCE_PARTITIONS_ENABLED =
    buildConf("spark.sql.adaptive.optimizeSkewsInRebalancePartitions.enabled")
      .doc(s"When true and '${ADAPTIVE_EXECUTION_ENABLED.key}' is true, Spark will optimize the " +
        "skewed shuffle partitions in RebalancePartitions and split them to smaller ones " +
        s"according to the target size (specified by '${ADVISORY_PARTITION_SIZE_IN_BYTES.key}'), " +
        "to avoid data skew.")
      .version("3.2.0")
      .booleanConf
      .createWithDefault(true)

  val ADAPTIVE_REBALANCE_PARTITIONS_SMALL_PARTITION_FACTOR =
    buildConf("spark.sql.adaptive.rebalancePartitionsSmallPartitionFactor")
      .doc(s"A partition will be merged during splitting if its size is small than this factor " +
        s"multiply ${ADVISORY_PARTITION_SIZE_IN_BYTES.key}.")
      .version("3.3.0")
      .doubleConf
      .checkValue(v => v > 0 && v < 1, "the factor must be in (0, 1)")
      .createWithDefault(0.2)

  val ADAPTIVE_FORCE_OPTIMIZE_SKEWED_JOIN =
    buildConf("spark.sql.adaptive.forceOptimizeSkewedJoin")
      .doc("When true, force enable OptimizeSkewedJoin even if it introduces extra shuffle.")
      .version("3.3.0")
      .booleanConf
      .createWithDefault(false)

  val ADAPTIVE_CUSTOM_COST_EVALUATOR_CLASS =
    buildConf("spark.sql.adaptive.customCostEvaluatorClass")
      .doc("The custom cost evaluator class to be used for adaptive execution. If not being set," +
        " Spark will use its own SimpleCostEvaluator by default.")
      .version("3.2.0")
      .stringConf
      .createOptional

  val SUBEXPRESSION_ELIMINATION_ENABLED =
    buildConf("spark.sql.subexpressionElimination.enabled")
      .internal()
      .doc("When true, common subexpressions will be eliminated.")
      .version("1.6.0")
      .booleanConf
      .createWithDefault(true)

  val SUBEXPRESSION_ELIMINATION_CACHE_MAX_ENTRIES =
    buildConf("spark.sql.subexpressionElimination.cache.maxEntries")
      .internal()
      .doc("The maximum entries of the cache used for interpreted subexpression elimination.")
      .version("3.1.0")
      .intConf
      .checkValue(_ >= 0, "The maximum must not be negative")
      .createWithDefault(100)

  val SUBEXPRESSION_ELIMINATION_SKIP_FOR_SHORTCUT_EXPR =
    buildConf("spark.sql.subexpressionElimination.skipForShortcutExpr")
      .internal()
      .doc("When true, shortcut eliminate subexpression with `AND`, `OR`. " +
        "The subexpression may not need to eval even if it appears more than once. " +
        "e.g., `if(or(a, and(b, b)))`, the expression `b` would be skipped if `a` is true.")
      .version("3.5.0")
      .booleanConf
      .createWithDefault(false)

  val CASE_SENSITIVE = buildConf(SqlApiConfHelper.CASE_SENSITIVE_KEY)
    .internal()
    .doc("Whether the query analyzer should be case sensitive or not. " +
      "Default to case insensitive. It is highly discouraged to turn on case sensitive mode.")
    .version("1.4.0")
    .booleanConf
    .createWithDefault(false)

  val CONSTRAINT_PROPAGATION_ENABLED = buildConf("spark.sql.constraintPropagation.enabled")
    .internal()
    .doc("When true, the query optimizer will infer and propagate data constraints in the query " +
      "plan to optimize them. Constraint propagation can sometimes be computationally expensive " +
      "for certain kinds of query plans (such as those with a large number of predicates and " +
      "aliases) which might negatively impact overall runtime.")
    .version("2.2.0")
    .booleanConf
    .createWithDefault(true)

  val PROPAGATE_DISTINCT_KEYS_ENABLED =
    buildConf("spark.sql.optimizer.propagateDistinctKeys.enabled")
      .internal()
      .doc("When true, the query optimizer will propagate a set of distinct attributes from the " +
        "current node and use it to optimize query.")
      .version("3.3.0")
      .booleanConf
      .createWithDefault(true)

  val EAGER_EVAL_OF_UNRESOLVED_INLINE_TABLE_ENABLED =
    buildConf("spark.sql.parser.eagerEvalOfUnresolvedInlineTable")
      .internal()
      .doc("Controls whether we optimize the ASTree that gets generated when parsing " +
        "VALUES lists (UnresolvedInlineTable) by eagerly evaluating it in the AST Builder.")
      .booleanConf
      .createWithDefault(true)

  val ESCAPED_STRING_LITERALS = buildConf("spark.sql.parser.escapedStringLiterals")
    .internal()
    .doc("When true, string literals (including regex patterns) remain escaped in our SQL " +
      "parser. The default is false since Spark 2.0. Setting it to true can restore the behavior " +
      "prior to Spark 2.0.")
    .version("2.2.1")
    .booleanConf
    .createWithDefault(false)

  val FILE_COMPRESSION_FACTOR = buildConf("spark.sql.sources.fileCompressionFactor")
    .internal()
    .doc("When estimating the output data size of a table scan, multiply the file size with this " +
      "factor as the estimated data size, in case the data is compressed in the file and lead to" +
      " a heavily underestimated result.")
    .version("2.3.1")
    .doubleConf
    .checkValue(_ > 0, "the value of fileCompressionFactor must be greater than 0")
    .createWithDefault(1.0)

  val PARQUET_SCHEMA_MERGING_ENABLED = buildConf("spark.sql.parquet.mergeSchema")
    .doc("When true, the Parquet data source merges schemas collected from all data files, " +
         "otherwise the schema is picked from the summary file or a random data file " +
         "if no summary file is available.")
    .version("1.5.0")
    .booleanConf
    .createWithDefault(false)

  val PARQUET_SCHEMA_RESPECT_SUMMARIES = buildConf("spark.sql.parquet.respectSummaryFiles")
    .doc("When true, we make assumption that all part-files of Parquet are consistent with " +
         "summary files and we will ignore them when merging schema. Otherwise, if this is " +
         "false, which is the default, we will merge all part-files. This should be considered " +
         "as expert-only option, and shouldn't be enabled before knowing what it means exactly.")
    .version("1.5.0")
    .booleanConf
    .createWithDefault(false)

  val PARQUET_BINARY_AS_STRING = buildConf("spark.sql.parquet.binaryAsString")
    .doc("Some other Parquet-producing systems, in particular Impala and older versions of " +
      "Spark SQL, do not differentiate between binary data and strings when writing out the " +
      "Parquet schema. This flag tells Spark SQL to interpret binary data as a string to provide " +
      "compatibility with these systems.")
    .version("1.1.1")
    .booleanConf
    .createWithDefault(false)

  val PARQUET_INT96_AS_TIMESTAMP = buildConf("spark.sql.parquet.int96AsTimestamp")
    .doc("Some Parquet-producing systems, in particular Impala, store Timestamp into INT96. " +
      "Spark would also store Timestamp as INT96 because we need to avoid precision lost of the " +
      "nanoseconds field. This flag tells Spark SQL to interpret INT96 data as a timestamp to " +
      "provide compatibility with these systems.")
    .version("1.3.0")
    .booleanConf
    .createWithDefault(true)

  val PARQUET_INT96_TIMESTAMP_CONVERSION = buildConf("spark.sql.parquet.int96TimestampConversion")
    .doc("This controls whether timestamp adjustments should be applied to INT96 data when " +
      "converting to timestamps, for data written by Impala.  This is necessary because Impala " +
      "stores INT96 data with a different timezone offset than Hive & Spark.")
    .version("2.3.0")
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
    .version("2.3.0")
    .stringConf
    .transform(_.toUpperCase(Locale.ROOT))
    .checkValues(ParquetOutputTimestampType.values.map(_.toString))
    .createWithDefault(ParquetOutputTimestampType.INT96.toString)

  val PARQUET_COMPRESSION = buildConf("spark.sql.parquet.compression.codec")
    .doc("Sets the compression codec used when writing Parquet files. If either `compression` or " +
      "`parquet.compression` is specified in the table-specific options/properties, the " +
      "precedence would be `compression`, `parquet.compression`, " +
      "`spark.sql.parquet.compression.codec`. Acceptable values include: none, uncompressed, " +
      "snappy, gzip, lzo, brotli, lz4, lz4_raw, zstd.")
    .version("1.1.1")
    .stringConf
    .transform(_.toLowerCase(Locale.ROOT))
    .checkValues(
      Set("none", "uncompressed", "snappy", "gzip", "lzo", "brotli", "lz4", "lz4_raw", "zstd"))
    .createWithDefault("snappy")

  val PARQUET_FILTER_PUSHDOWN_ENABLED = buildConf("spark.sql.parquet.filterPushdown")
    .doc("Enables Parquet filter push-down optimization when set to true.")
    .version("1.2.0")
    .booleanConf
    .createWithDefault(true)

  val PARQUET_FILTER_PUSHDOWN_DATE_ENABLED = buildConf("spark.sql.parquet.filterPushdown.date")
    .doc("If true, enables Parquet filter push-down optimization for Date. " +
      s"This configuration only has an effect when '${PARQUET_FILTER_PUSHDOWN_ENABLED.key}' is " +
      "enabled.")
    .version("2.4.0")
    .internal()
    .booleanConf
    .createWithDefault(true)

  val PARQUET_FILTER_PUSHDOWN_TIMESTAMP_ENABLED =
    buildConf("spark.sql.parquet.filterPushdown.timestamp")
      .doc("If true, enables Parquet filter push-down optimization for Timestamp. " +
        s"This configuration only has an effect when '${PARQUET_FILTER_PUSHDOWN_ENABLED.key}' is " +
        "enabled and Timestamp stored as TIMESTAMP_MICROS or TIMESTAMP_MILLIS type.")
      .version("2.4.0")
      .internal()
      .booleanConf
      .createWithDefault(true)

  val PARQUET_FILTER_PUSHDOWN_DECIMAL_ENABLED =
    buildConf("spark.sql.parquet.filterPushdown.decimal")
      .doc("If true, enables Parquet filter push-down optimization for Decimal. " +
        s"This configuration only has an effect when '${PARQUET_FILTER_PUSHDOWN_ENABLED.key}' is " +
        "enabled.")
      .version("2.4.0")
      .internal()
      .booleanConf
      .createWithDefault(true)

  val PARQUET_FILTER_PUSHDOWN_STRING_STARTSWITH_ENABLED =
    buildConf("spark.sql.parquet.filterPushdown.string.startsWith")
    .doc("If true, enables Parquet filter push-down optimization for string startsWith function. " +
      s"This configuration only has an effect when '${PARQUET_FILTER_PUSHDOWN_ENABLED.key}' is " +
      "enabled.")
    .version("2.4.0")
    .internal()
    .booleanConf
    .createWithDefault(true)

  val PARQUET_FILTER_PUSHDOWN_STRING_PREDICATE_ENABLED =
    buildConf("spark.sql.parquet.filterPushdown.stringPredicate")
      .doc("If true, enables Parquet filter push-down optimization for string predicate such " +
        "as startsWith/endsWith/contains function. This configuration only has an effect when " +
        s"'${PARQUET_FILTER_PUSHDOWN_ENABLED.key}' is enabled.")
      .version("3.4.0")
      .internal()
      .fallbackConf(PARQUET_FILTER_PUSHDOWN_STRING_STARTSWITH_ENABLED)

  val PARQUET_FILTER_PUSHDOWN_INFILTERTHRESHOLD =
    buildConf("spark.sql.parquet.pushdown.inFilterThreshold")
      .doc("For IN predicate, Parquet filter will push-down a set of OR clauses if its " +
        "number of values not exceeds this threshold. Otherwise, Parquet filter will push-down " +
        "a value greater than or equal to its minimum value and less than or equal to " +
        "its maximum value. By setting this value to 0 this feature can be disabled. " +
        s"This configuration only has an effect when '${PARQUET_FILTER_PUSHDOWN_ENABLED.key}' is " +
        "enabled.")
      .version("2.4.0")
      .internal()
      .intConf
      .checkValue(threshold => threshold >= 0, "The threshold must not be negative.")
      .createWithDefault(10)

  val PARQUET_AGGREGATE_PUSHDOWN_ENABLED = buildConf("spark.sql.parquet.aggregatePushdown")
    .doc("If true, aggregates will be pushed down to Parquet for optimization. Support MIN, MAX " +
      "and COUNT as aggregate expression. For MIN/MAX, support boolean, integer, float and date " +
      "type. For COUNT, support all data types. If statistics is missing from any Parquet file " +
      "footer, exception would be thrown.")
    .version("3.3.0")
    .booleanConf
    .createWithDefault(false)

  val PARQUET_WRITE_LEGACY_FORMAT = buildConf("spark.sql.parquet.writeLegacyFormat")
    .doc("If true, data will be written in a way of Spark 1.4 and earlier. For example, decimal " +
      "values will be written in Apache Parquet's fixed-length byte array format, which other " +
      "systems such as Apache Hive and Apache Impala use. If false, the newer format in Parquet " +
      "will be used. For example, decimals will be written in int-based format. If Parquet " +
      "output is intended for use with systems that do not support this newer format, set to true.")
    .version("1.6.0")
    .booleanConf
    .createWithDefault(false)

  val PARQUET_OUTPUT_COMMITTER_CLASS = buildConf("spark.sql.parquet.output.committer.class")
    .doc("The output committer class used by Parquet. The specified class needs to be a " +
      "subclass of org.apache.hadoop.mapreduce.OutputCommitter. Typically, it's also a subclass " +
      "of org.apache.parquet.hadoop.ParquetOutputCommitter. If it is not, then metadata " +
      "summaries will never be created, irrespective of the value of " +
      "parquet.summary.metadata.level")
    .version("1.5.0")
    .internal()
    .stringConf
    .checkValue(Utils.classIsLoadableAndAssignableFrom(_, classOf[OutputCommitter]),
      s"Class must be loadable and subclass of ${classOf[OutputCommitter].getName}")
    .createWithDefault("org.apache.parquet.hadoop.ParquetOutputCommitter")

  val PARQUET_VECTORIZED_READER_ENABLED =
    buildConf("spark.sql.parquet.enableVectorizedReader")
      .doc("Enables vectorized parquet decoding.")
      .version("2.0.0")
      .booleanConf
      .createWithDefault(true)

  val PARQUET_VECTORIZED_READER_NESTED_COLUMN_ENABLED =
    buildConf("spark.sql.parquet.enableNestedColumnVectorizedReader")
      .doc("Enables vectorized Parquet decoding for nested columns (e.g., struct, list, map). " +
          s"Requires ${PARQUET_VECTORIZED_READER_ENABLED.key} to be enabled.")
      .version("3.3.0")
      .booleanConf
      .createWithDefault(true)

  val PARQUET_RECORD_FILTER_ENABLED = buildConf("spark.sql.parquet.recordLevelFilter.enabled")
    .doc("If true, enables Parquet's native record-level filtering using the pushed down " +
      "filters. " +
      s"This configuration only has an effect when '${PARQUET_FILTER_PUSHDOWN_ENABLED.key}' " +
      "is enabled and the vectorized reader is not used. You can ensure the vectorized reader " +
      s"is not used by setting '${PARQUET_VECTORIZED_READER_ENABLED.key}' to false.")
    .version("2.3.0")
    .booleanConf
    .createWithDefault(false)

  val PARQUET_VECTORIZED_READER_BATCH_SIZE = buildConf("spark.sql.parquet.columnarReaderBatchSize")
    .doc("The number of rows to include in a parquet vectorized reader batch. The number should " +
      "be carefully chosen to minimize overhead and avoid OOMs in reading data.")
    .version("2.4.0")
    .intConf
    .createWithDefault(4096)

  val PARQUET_FIELD_ID_WRITE_ENABLED =
    buildConf("spark.sql.parquet.fieldId.write.enabled")
      .doc("Field ID is a native field of the Parquet schema spec. When enabled, " +
        "Parquet writers will populate the field Id " +
        "metadata (if present) in the Spark schema to the Parquet schema.")
      .version("3.3.0")
      .booleanConf
      .createWithDefault(true)

  val PARQUET_FIELD_ID_READ_ENABLED =
    buildConf("spark.sql.parquet.fieldId.read.enabled")
      .doc("Field ID is a native field of the Parquet schema spec. When enabled, Parquet readers " +
        "will use field IDs (if present) in the requested Spark schema to look up Parquet " +
        "fields instead of using column names")
      .version("3.3.0")
      .booleanConf
      .createWithDefault(false)

  val IGNORE_MISSING_PARQUET_FIELD_ID =
    buildConf("spark.sql.parquet.fieldId.read.ignoreMissing")
      .doc("When the Parquet file doesn't have any field IDs but the " +
        "Spark read schema is using field IDs to read, we will silently return nulls " +
        "when this flag is enabled, or error otherwise.")
      .version("3.3.0")
      .booleanConf
      .createWithDefault(false)

  val PARQUET_INFER_TIMESTAMP_NTZ_ENABLED =
    buildConf("spark.sql.parquet.inferTimestampNTZ.enabled")
      .doc("When enabled, Parquet timestamp columns with annotation isAdjustedToUTC = false " +
        "are inferred as TIMESTAMP_NTZ type during schema inference. Otherwise, all the Parquet " +
        "timestamp columns are inferred as TIMESTAMP_LTZ types. Note that Spark writes the " +
        "output schema into Parquet's footer metadata on file writing and leverages it on file " +
        "reading. Thus this configuration only affects the schema inference on Parquet files " +
        "which are not written by Spark.")
      .version("3.4.0")
      .booleanConf
      .createWithDefault(true)

  val ORC_COMPRESSION = buildConf("spark.sql.orc.compression.codec")
    .doc("Sets the compression codec used when writing ORC files. If either `compression` or " +
      "`orc.compress` is specified in the table-specific options/properties, the precedence " +
      "would be `compression`, `orc.compress`, `spark.sql.orc.compression.codec`." +
      "Acceptable values include: none, uncompressed, snappy, zlib, lzo, zstd, lz4, brotli.")
    .version("2.3.0")
    .stringConf
    .transform(_.toLowerCase(Locale.ROOT))
    .checkValues(Set("none", "uncompressed", "snappy", "zlib", "lzo", "zstd", "lz4", "brotli"))
    .createWithDefault("zstd")

  val ORC_IMPLEMENTATION = buildConf("spark.sql.orc.impl")
    .doc("When native, use the native version of ORC support instead of the ORC library in Hive. " +
      "It is 'hive' by default prior to Spark 2.4.")
    .version("2.3.0")
    .internal()
    .stringConf
    .checkValues(Set("hive", "native"))
    .createWithDefault("native")

  val ORC_VECTORIZED_READER_ENABLED = buildConf("spark.sql.orc.enableVectorizedReader")
    .doc("Enables vectorized orc decoding.")
    .version("2.3.0")
    .booleanConf
    .createWithDefault(true)

  val ORC_VECTORIZED_READER_BATCH_SIZE = buildConf("spark.sql.orc.columnarReaderBatchSize")
    .doc("The number of rows to include in a orc vectorized reader batch. The number should " +
      "be carefully chosen to minimize overhead and avoid OOMs in reading data.")
    .version("2.4.0")
    .intConf
    .createWithDefault(4096)

  val ORC_VECTORIZED_WRITER_BATCH_SIZE = buildConf("spark.sql.orc.columnarWriterBatchSize")
    .doc("The number of rows to include in a orc vectorized writer batch. The number should " +
      "be carefully chosen to minimize overhead and avoid OOMs in writing data.")
    .version("3.4.0")
    .intConf
    .createWithDefault(1024)

  val ORC_VECTORIZED_READER_NESTED_COLUMN_ENABLED =
    buildConf("spark.sql.orc.enableNestedColumnVectorizedReader")
      .doc("Enables vectorized orc decoding for nested column.")
      .version("3.2.0")
      .booleanConf
      .createWithDefault(true)

  val ORC_FILTER_PUSHDOWN_ENABLED = buildConf("spark.sql.orc.filterPushdown")
    .doc("When true, enable filter pushdown for ORC files.")
    .version("1.4.0")
    .booleanConf
    .createWithDefault(true)

  val ORC_AGGREGATE_PUSHDOWN_ENABLED = buildConf("spark.sql.orc.aggregatePushdown")
    .doc("If true, aggregates will be pushed down to ORC for optimization. Support MIN, MAX and " +
      "COUNT as aggregate expression. For MIN/MAX, support boolean, integer, float and date " +
      "type. For COUNT, support all data types. If statistics is missing from any ORC file " +
      "footer, exception would be thrown.")
    .version("3.3.0")
    .booleanConf
    .createWithDefault(false)

  val ORC_SCHEMA_MERGING_ENABLED = buildConf("spark.sql.orc.mergeSchema")
    .doc("When true, the Orc data source merges schemas collected from all data files, " +
      "otherwise the schema is picked from a random data file.")
    .version("3.0.0")
    .booleanConf
    .createWithDefault(false)

  val HIVE_METASTORE_DROP_PARTITION_BY_NAME =
    buildConf("spark.sql.hive.dropPartitionByName.enabled")
      .doc("When true, Spark will get partition name rather than partition object " +
           "to drop partition, which can improve the performance of drop partition.")
      .version("3.4.0")
      .booleanConf
      .createWithDefault(false)

  val HIVE_METASTORE_PARTITION_PRUNING =
    buildConf("spark.sql.hive.metastorePartitionPruning")
      .doc("When true, some predicates will be pushed down into the Hive metastore so that " +
           "unmatching partitions can be eliminated earlier.")
      .version("1.5.0")
      .booleanConf
      .createWithDefault(true)

  val HIVE_METASTORE_PARTITION_PRUNING_INSET_THRESHOLD =
    buildConf("spark.sql.hive.metastorePartitionPruningInSetThreshold")
      .doc("The threshold of set size for InSet predicate when pruning partitions through Hive " +
        "Metastore. When the set size exceeds the threshold, we rewrite the InSet predicate " +
        "to be greater than or equal to the minimum value in set and less than or equal to the " +
        "maximum value in set. Larger values may cause Hive Metastore stack overflow. But for " +
        "InSet inside Not with values exceeding the threshold, we won't push it to Hive Metastore."
      )
      .version("3.1.0")
      .internal()
      .intConf
      .checkValue(_ > 0, "The value of metastorePartitionPruningInSetThreshold must be positive")
      .createWithDefault(1000)

  val HIVE_METASTORE_PARTITION_PRUNING_FALLBACK_ON_EXCEPTION =
    buildConf("spark.sql.hive.metastorePartitionPruningFallbackOnException")
      .doc("Whether to fallback to get all partitions from Hive metastore and perform partition " +
        "pruning on Spark client side, when encountering MetaException from the metastore. Note " +
        "that Spark query performance may degrade if this is enabled and there are many " +
        "partitions to be listed. If this is disabled, Spark will fail the query instead.")
      .version("3.3.0")
      .booleanConf
      .createWithDefault(false)

  val HIVE_METASTORE_PARTITION_PRUNING_FAST_FALLBACK =
    buildConf("spark.sql.hive.metastorePartitionPruningFastFallback")
      .doc("When this config is enabled, if the predicates are not supported by Hive or Spark " +
        "does fallback due to encountering MetaException from the metastore, " +
        "Spark will instead prune partitions by getting the partition names first " +
        "and then evaluating the filter expressions on the client side. " +
        "Note that the predicates with TimeZoneAwareExpression is not supported.")
      .version("3.3.0")
      .booleanConf
      .createWithDefault(false)

  val HIVE_MANAGE_FILESOURCE_PARTITIONS =
    buildConf("spark.sql.hive.manageFilesourcePartitions")
      .doc("When true, enable metastore partition management for file source tables as well. " +
           "This includes both datasource and converted Hive tables. When partition management " +
           "is enabled, datasource tables store partition in the Hive metastore, and use the " +
           s"metastore to prune partitions during query planning when " +
           s"${HIVE_METASTORE_PARTITION_PRUNING.key} is set to true.")
      .version("2.1.1")
      .booleanConf
      .createWithDefault(true)

  val HIVE_FILESOURCE_PARTITION_FILE_CACHE_SIZE =
    buildConf("spark.sql.hive.filesourcePartitionFileCacheSize")
      .doc("When nonzero, enable caching of partition file metadata in memory. All tables share " +
           "a cache that can use up to specified num bytes for file metadata. This conf only " +
           "has an effect when hive filesource partition management is enabled.")
      .version("2.1.1")
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
    .version("2.1.1")
    .stringConf
    .transform(_.toUpperCase(Locale.ROOT))
    .checkValues(HiveCaseSensitiveInferenceMode.values.map(_.toString))
    .createWithDefault(HiveCaseSensitiveInferenceMode.NEVER_INFER.toString)

  val HIVE_TABLE_PROPERTY_LENGTH_THRESHOLD =
    buildConf("spark.sql.hive.tablePropertyLengthThreshold")
      .internal()
      .doc("The maximum length allowed in a single cell when storing Spark-specific information " +
        "in Hive's metastore as table properties. Currently it covers 2 things: the schema's " +
        "JSON string, the histogram of column statistics.")
      .version("3.2.0")
      .intConf
      .createOptional

  val OPTIMIZER_METADATA_ONLY = buildConf("spark.sql.optimizer.metadataOnly")
    .internal()
    .doc("When true, enable the metadata-only query optimization that use the table's metadata " +
      "to produce the partition columns instead of table scans. It applies when all the columns " +
      "scanned are partition columns and the query has an aggregate operator that satisfies " +
      "distinct semantics. By default the optimization is disabled, and deprecated as of Spark " +
      "3.0 since it may return incorrect results when the files are empty, see also SPARK-26709." +
      "It will be removed in the future releases. If you must use, use 'SparkSessionExtensions' " +
      "instead to inject it as a custom rule.")
    .version("2.1.1")
    .booleanConf
    .createWithDefault(false)

  val COLUMN_NAME_OF_CORRUPT_RECORD = buildConf("spark.sql.columnNameOfCorruptRecord")
    .doc("The name of internal column for storing raw/un-parsed JSON and CSV records that fail " +
      "to parse.")
    .version("1.2.0")
    .stringConf
    .createWithDefault("_corrupt_record")

  val BROADCAST_TIMEOUT = buildConf("spark.sql.broadcastTimeout")
    .doc("Timeout in seconds for the broadcast wait time in broadcast joins.")
    .version("1.3.0")
    .timeConf(TimeUnit.SECONDS)
    .createWithDefaultString(s"${5 * 60}")

  val INTERRUPT_ON_CANCEL = buildConf("spark.sql.execution.interruptOnCancel")
    .doc("When true, all running tasks will be interrupted if one cancels a query.")
    .version("4.0.0")
    .booleanConf
    .createWithDefault(true)

  // This is only used for the thriftserver
  val THRIFTSERVER_POOL = buildConf("spark.sql.thriftserver.scheduler.pool")
    .doc("Set a Fair Scheduler pool for a JDBC client session.")
    .version("1.1.1")
    .stringConf
    .createOptional

  val THRIFTSERVER_INCREMENTAL_COLLECT =
    buildConf("spark.sql.thriftServer.incrementalCollect")
      .internal()
      .doc("When true, enable incremental collection for execution in Thrift Server.")
      .version("2.0.3")
      .booleanConf
      .createWithDefault(false)

  val THRIFTSERVER_FORCE_CANCEL =
    buildConf("spark.sql.thriftServer.interruptOnCancel")
      .doc("When true, all running tasks will be interrupted if one cancels a query. " +
        "When false, all running tasks will remain until finished.")
      .version("3.2.0")
      .fallbackConf(INTERRUPT_ON_CANCEL)

  val THRIFTSERVER_QUERY_TIMEOUT =
    buildConf("spark.sql.thriftServer.queryTimeout")
      .doc("Set a query duration timeout in seconds in Thrift Server. If the timeout is set to " +
        "a positive value, a running query will be cancelled automatically when the timeout is " +
        "exceeded, otherwise the query continues to run till completion. If timeout values are " +
        "set for each statement via `java.sql.Statement.setQueryTimeout` and they are smaller " +
        "than this configuration value, they take precedence. If you set this timeout and prefer " +
        "to cancel the queries right away without waiting task to finish, consider enabling " +
        s"${THRIFTSERVER_FORCE_CANCEL.key} together.")
      .version("3.1.0")
      .timeConf(TimeUnit.SECONDS)
      .createWithDefault(0L)

  val THRIFTSERVER_UI_STATEMENT_LIMIT =
    buildConf("spark.sql.thriftserver.ui.retainedStatements")
      .doc("The number of SQL statements kept in the JDBC/ODBC web UI history.")
      .version("1.4.0")
      .intConf
      .createWithDefault(200)

  val THRIFTSERVER_UI_SESSION_LIMIT = buildConf("spark.sql.thriftserver.ui.retainedSessions")
    .doc("The number of SQL client sessions kept in the JDBC/ODBC web UI history.")
    .version("1.4.0")
    .intConf
    .createWithDefault(200)

  val DATA_SOURCE_DONT_ASSERT_ON_PREDICATE =
    buildConf("spark.sql.dataSource.skipAssertOnPredicatePushdown")
      .internal()
      .doc("Enable skipping assert when expression in not translated to predicate.")
      .booleanConf
      .createWithDefault(!Utils.isTesting)

  // This is used to set the default data source
  val DEFAULT_DATA_SOURCE_NAME = buildConf("spark.sql.sources.default")
    .doc("The default data source to use in input/output.")
    .version("1.3.0")
    .stringConf
    .createWithDefault("parquet")

  val CONVERT_CTAS = buildConf("spark.sql.hive.convertCTAS")
    .internal()
    .doc("When true, a table created by a Hive CTAS statement (no USING clause) " +
      "without specifying any storage property will be converted to a data source table, " +
      s"using the data source set by ${DEFAULT_DATA_SOURCE_NAME.key}.")
    .version("2.0.0")
    .booleanConf
    .createWithDefault(false)

  val GATHER_FASTSTAT = buildConf("spark.sql.hive.gatherFastStats")
      .internal()
      .doc("When true, fast stats (number of files and total size of all files) will be gathered" +
        " in parallel while repairing table partitions to avoid the sequential listing in Hive" +
        " metastore.")
      .version("2.0.1")
      .booleanConf
      .createWithDefault(true)

  /**
   * Output style for binary data.
   */
  object BinaryOutputStyle extends Enumeration {
    type BinaryOutputStyle = Value
    val
    /**
     * Output as UTF-8 string.
     * [83, 112, 97, 114, 107] -> "Spark"
     */
    UTF8: Value = Value("UTF-8")
    /**
     * Output as comma separated byte array string.
     * [83, 112, 97, 114, 107] -> [83, 112, 97, 114, 107]
     */
    val BASIC,
    /**
     * Output as base64 encoded string.
     * [83, 112, 97, 114, 107] -> U3Bhcmsg
     */
    BASE64,
    /**
     * Output as hex string.
     * [83, 112, 97, 114, 107] -> 537061726b
     */
    HEX,
    /**
     * Output as discrete hex string.
     * [83, 112, 97, 114, 107] -> [53 70 61 72 6b]
     */
    HEX_DISCRETE = Value
  }

  val BINARY_OUTPUT_STYLE = buildConf("spark.sql.binaryOutputStyle")
    .doc("The output style used display binary data. Valid values are 'UTF-8', " +
      "'BASIC', 'BASE64', 'HEX', and 'HEX_DISCRETE'.")
    .version("4.0.0")
    .stringConf
    .transform(_.toUpperCase(Locale.ROOT))
    .checkValues(BinaryOutputStyle.values.map(_.toString))
    .createOptional

  val PARTITION_COLUMN_TYPE_INFERENCE =
    buildConf("spark.sql.sources.partitionColumnTypeInference.enabled")
      .doc("When true, automatically infer the data types for partitioned columns.")
      .version("1.5.0")
      .booleanConf
      .createWithDefault(true)

  val BUCKETING_ENABLED = buildConf("spark.sql.sources.bucketing.enabled")
    .doc("When false, we will treat bucketed table as normal table")
    .version("2.0.0")
    .booleanConf
    .createWithDefault(true)

  val V2_BUCKETING_ENABLED = buildConf("spark.sql.sources.v2.bucketing.enabled")
      .doc(s"Similar to ${BUCKETING_ENABLED.key}, this config is used to enable bucketing for V2 " +
        "data sources. When turned on, Spark will recognize the specific distribution " +
        "reported by a V2 data source through SupportsReportPartitioning, and will try to " +
        "avoid shuffle if necessary.")
      .version("3.3.0")
      .booleanConf
      .createWithDefault(false)

  val V2_BUCKETING_PUSH_PART_VALUES_ENABLED =
    buildConf("spark.sql.sources.v2.bucketing.pushPartValues.enabled")
      .doc(s"Whether to pushdown common partition values when ${V2_BUCKETING_ENABLED.key} is " +
        "enabled. When turned on, if both sides of a join are of KeyGroupedPartitioning and if " +
        "they share compatible partition keys, even if they don't have the exact same partition " +
        "values, Spark will calculate a superset of partition values and pushdown that info to " +
        "scan nodes, which will use empty partitions for the missing partition values on either " +
        "side. This could help to eliminate unnecessary shuffles")
      .version("3.4.0")
      .booleanConf
      .createWithDefault(true)

  val V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED =
    buildConf("spark.sql.sources.v2.bucketing.partiallyClusteredDistribution.enabled")
      .doc("During a storage-partitioned join, whether to allow input partitions to be " +
        "partially clustered, when both sides of the join are of KeyGroupedPartitioning. At " +
        "planning time, Spark will pick the side with less data size based on table " +
        "statistics, group and replicate them to match the other side. This is an optimization " +
        "on skew join and can help to reduce data skewness when certain partitions are assigned " +
        s"large amount of data. This config requires both ${V2_BUCKETING_ENABLED.key} and " +
        s"${V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key} to be enabled")
      .version("3.4.0")
      .booleanConf
      .createWithDefault(false)

  val V2_BUCKETING_SHUFFLE_ENABLED =
    buildConf("spark.sql.sources.v2.bucketing.shuffle.enabled")
      .doc("During a storage-partitioned join, whether to allow to shuffle only one side." +
        "When only one side is KeyGroupedPartitioning, if the conditions are met, spark will " +
        "only shuffle the other side. This optimization will reduce the amount of data that " +
        s"needs to be shuffle. This config requires ${V2_BUCKETING_ENABLED.key} to be enabled")
      .version("4.0.0")
      .booleanConf
      .createWithDefault(false)

   val V2_BUCKETING_ALLOW_JOIN_KEYS_SUBSET_OF_PARTITION_KEYS =
    buildConf("spark.sql.sources.v2.bucketing.allowJoinKeysSubsetOfPartitionKeys.enabled")
      .doc("Whether to allow storage-partition join in the case where join keys are" +
        "a subset of the partition keys of the source tables. At planning time, " +
        "Spark will group the partitions by only those keys that are in the join keys." +
        s"This is currently enabled only if ${REQUIRE_ALL_CLUSTER_KEYS_FOR_DISTRIBUTION.key} " +
        "is false."
      )
      .version("4.0.0")
      .booleanConf
      .createWithDefault(false)

  val V2_BUCKETING_ALLOW_COMPATIBLE_TRANSFORMS =
    buildConf("spark.sql.sources.v2.bucketing.allowCompatibleTransforms.enabled")
      .doc("Whether to allow storage-partition join in the case where the partition transforms " +
        "are compatible but not identical.  This config requires both " +
        s"${V2_BUCKETING_ENABLED.key} and ${V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key} to be " +
        s"enabled and ${V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key} " +
        "to be disabled."
      )
      .version("4.0.0")
      .booleanConf
      .createWithDefault(false)

  val V2_BUCKETING_PARTITION_FILTER_ENABLED =
    buildConf("spark.sql.sources.v2.bucketing.partition.filter.enabled")
      .doc(s"Whether to filter partitions when running storage-partition join. " +
        s"When enabled, partitions without matches on the other side can be omitted for " +
        s"scanning, if allowed by the join type. This config requires both " +
        s"${V2_BUCKETING_ENABLED.key} and ${V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key} to be " +
        s"enabled.")
      .version("4.0.0")
      .booleanConf
      .createWithDefault(false)

  val BUCKETING_MAX_BUCKETS = buildConf("spark.sql.sources.bucketing.maxBuckets")
    .doc("The maximum number of buckets allowed.")
    .version("2.4.0")
    .intConf
    .checkValue(_ > 0, "the value of spark.sql.sources.bucketing.maxBuckets must be greater than 0")
    .createWithDefault(100000)

  val AUTO_BUCKETED_SCAN_ENABLED =
    buildConf("spark.sql.sources.bucketing.autoBucketedScan.enabled")
      .doc("When true, decide whether to do bucketed scan on input tables based on query plan " +
        "automatically. Do not use bucketed scan if 1. query does not have operators to utilize " +
        "bucketing (e.g. join, group-by, etc), or 2. there's an exchange operator between these " +
        s"operators and table scan. Note when '${BUCKETING_ENABLED.key}' is set to " +
        "false, this configuration does not take any effect.")
      .version("3.1.0")
      .booleanConf
      .createWithDefault(true)

  val CAN_CHANGE_CACHED_PLAN_OUTPUT_PARTITIONING =
    buildConf("spark.sql.optimizer.canChangeCachedPlanOutputPartitioning")
      .internal()
      .doc("Whether to forcibly enable some optimization rules that can change the output " +
        "partitioning of a cached query when executing it for caching. If it is set to true, " +
        "queries may need an extra shuffle to read the cached data. This configuration is " +
        "disabled by default. The optimization rule enabled by this configuration " +
        s"is ${ADAPTIVE_EXECUTION_APPLY_FINAL_STAGE_SHUFFLE_OPTIMIZATIONS.key}.")
      .version("3.2.0")
      .booleanConf
      .createWithDefault(false)

  val DEFAULT_CACHE_STORAGE_LEVEL = buildConf("spark.sql.defaultCacheStorageLevel")
    .doc("The default storage level of `dataset.cache()`, `catalog.cacheTable()` and " +
      "sql query `CACHE TABLE t`.")
    .version("4.0.0")
    .stringConf
    .transform(_.toUpperCase(Locale.ROOT))
    .checkValues(StorageLevelMapper.values.map(_.name()).toSet)
    .createWithDefault(StorageLevelMapper.MEMORY_AND_DISK.name())

  val DATAFRAME_CACHE_LOG_LEVEL = buildConf("spark.sql.dataframeCache.logLevel")
    .internal()
    .doc("Configures the log level of Dataframe cache operations, including adding and removing " +
      "entries from Dataframe cache, hit and miss on cache application. The default log " +
      "level is 'trace'. This log should only be used for debugging purposes and not in the " +
      "production environment, since it generates a large amount of logs.")
    .version("4.0.0")
    .stringConf
    .transform(_.toUpperCase(Locale.ROOT))
    .checkValue(logLevel => Set("TRACE", "DEBUG", "INFO", "WARN", "ERROR").contains(logLevel),
      "Invalid value for 'spark.sql.dataframeCache.logLevel'. Valid values are " +
        "'trace', 'debug', 'info', 'warn' and 'error'.")
    .createWithDefault("trace")

  val CROSS_JOINS_ENABLED = buildConf("spark.sql.crossJoin.enabled")
    .internal()
    .doc("When false, we will throw an error if a query contains a cartesian product without " +
        "explicit CROSS JOIN syntax.")
    .version("2.0.0")
    .booleanConf
    .createWithDefault(true)

  val ORDER_BY_ORDINAL = buildConf("spark.sql.orderByOrdinal")
    .doc("When true, the ordinal numbers are treated as the position in the select list. " +
         "When false, the ordinal numbers in order/sort by clause are ignored.")
    .version("2.0.0")
    .booleanConf
    .createWithDefault(true)

  val GROUP_BY_ORDINAL = buildConf("spark.sql.groupByOrdinal")
    .doc("When true, the ordinal numbers in group by clauses are treated as the position " +
      "in the select list. When false, the ordinal numbers are ignored.")
    .version("2.0.0")
    .booleanConf
    .createWithDefault(true)

  val GROUP_BY_ALIASES = buildConf("spark.sql.groupByAliases")
    .doc("When true, aliases in a select list can be used in group by clauses. When false, " +
      "an analysis exception is thrown in the case.")
    .version("2.2.0")
    .booleanConf
    .createWithDefault(true)

  val VIEW_SCHEMA_BINDING_ENABLED = buildConf("spark.sql.legacy.viewSchemaBindingMode")
    .internal()
    .doc("Set to false to disable the WITH SCHEMA clause for view DDL and suppress the line in " +
      "DESCRIBE EXTENDED and SHOW CREATE TABLE.")
    .version("4.0.0")
    .booleanConf
    .createWithDefault(true)

  val VIEW_SCHEMA_COMPENSATION = buildConf("spark.sql.legacy.viewSchemaCompensation")
    .internal()
    .doc("Set to false to revert default view schema binding mode from WITH SCHEMA COMPENSATION " +
      "to WITH SCHEMA BINDING.")
    .version("4.0.0")
    .booleanConf
    .createWithDefault(true)

  val OUTPUT_COMMITTER_CLASS = buildConf("spark.sql.sources.outputCommitterClass")
    .version("1.4.0")
    .internal()
    .stringConf
    .checkValue(Utils.classIsLoadableAndAssignableFrom(_, classOf[OutputCommitter]),
      s"Class must be loadable and subclass of ${classOf[OutputCommitter].getName}")
    .createOptional

  val FILE_COMMIT_PROTOCOL_CLASS =
    buildConf("spark.sql.sources.commitProtocolClass")
      .version("2.1.1")
      .internal()
      .stringConf
      .checkValue(Utils.classIsLoadableAndAssignableFrom(_, classOf[FileCommitProtocol]),
        s"Class must be loadable and subclass of ${classOf[FileCommitProtocol].getName}")
      .createWithDefault(
        "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")

  val PARALLEL_PARTITION_DISCOVERY_THRESHOLD =
    buildConf("spark.sql.sources.parallelPartitionDiscovery.threshold")
      .doc("The maximum number of paths allowed for listing files at driver side. If the number " +
        "of detected paths exceeds this value during partition discovery, it tries to list the " +
        "files with another Spark distributed job. This configuration is effective only when " +
        "using file-based sources such as Parquet, JSON and ORC.")
      .version("1.5.0")
      .intConf
      .checkValue(parallel => parallel >= 0, "The maximum number of paths allowed for listing " +
        "files at driver side must not be negative")
      .createWithDefault(32)

  val PARALLEL_PARTITION_DISCOVERY_PARALLELISM =
    buildConf("spark.sql.sources.parallelPartitionDiscovery.parallelism")
      .doc("The number of parallelism to list a collection of path recursively, Set the " +
        "number to prevent file listing from generating too many tasks.")
      .version("2.1.1")
      .internal()
      .intConf
      .createWithDefault(10000)

  val IGNORE_DATA_LOCALITY =
    buildConf("spark.sql.sources.ignoreDataLocality")
      .doc("If true, Spark will not fetch the block locations for each file on " +
        "listing files. This speeds up file listing, but the scheduler cannot " +
        "schedule tasks to take advantage of data locality. It can be particularly " +
        "useful if data is read from a remote cluster so the scheduler could never " +
        "take advantage of locality anyway.")
      .version("3.0.0")
      .internal()
      .booleanConf
      .createWithDefault(false)

  val USE_LISTFILES_FILESYSTEM_LIST =
    buildConf("spark.sql.sources.useListFilesFileSystemList")
      .doc("A comma-separated list of file system schemes to use FileSystem.listFiles API " +
        "for a single root path listing")
      .version("4.0.0")
      .internal()
      .stringConf
      .transform(_.toLowerCase(Locale.ROOT))
      .createWithDefault("s3a")

  // Whether to automatically resolve ambiguity in join conditions for self-joins.
  // See SPARK-6231.
  val DATAFRAME_SELF_JOIN_AUTO_RESOLVE_AMBIGUITY =
    buildConf("spark.sql.selfJoinAutoResolveAmbiguity")
      .version("1.4.0")
      .internal()
      .booleanConf
      .createWithDefault(true)

  val FAIL_AMBIGUOUS_SELF_JOIN_ENABLED =
    buildConf("spark.sql.analyzer.failAmbiguousSelfJoin")
      .doc("When true, fail the Dataset query if it contains ambiguous self-join.")
      .version("3.0.0")
      .internal()
      .booleanConf
      .createWithDefault(true)

  // Whether to retain group by columns or not in GroupedData.agg.
  val DATAFRAME_RETAIN_GROUP_COLUMNS = buildConf("spark.sql.retainGroupColumns")
    .version("1.4.0")
    .internal()
    .booleanConf
    .createWithDefault(true)

  val DATAFRAME_PIVOT_MAX_VALUES = buildConf("spark.sql.pivotMaxValues")
    .doc("When doing a pivot without specifying values for the pivot column this is the maximum " +
      "number of (distinct) values that will be collected without error.")
    .version("1.6.0")
    .intConf
    .createWithDefault(10000)

  val DATAFRAME_TRANSPOSE_MAX_VALUES = buildConf("spark.sql.transposeMaxValues")
    .doc("When doing a transpose without specifying values for the index column this is" +
      " the maximum number of values that will be transposed without error.")
    .version("4.0.0")
    .intConf
    .createWithDefault(500)

  val RUN_SQL_ON_FILES = buildConf("spark.sql.runSQLOnFiles")
    .internal()
    .doc("When true, we could use `datasource`.`path` as table in SQL query.")
    .version("1.6.0")
    .booleanConf
    .createWithDefault(true)

  val WHOLESTAGE_CODEGEN_ENABLED = buildConf("spark.sql.codegen.wholeStage")
    .internal()
    .doc("When true, the whole stage (of multiple operators) will be compiled into single java" +
      " method.")
    .version("2.0.0")
    .booleanConf
    .createWithDefault(true)

  val WHOLESTAGE_CODEGEN_USE_ID_IN_CLASS_NAME =
    buildConf("spark.sql.codegen.useIdInClassName")
    .internal()
    .doc("When true, embed the (whole-stage) codegen stage ID into " +
      "the class name of the generated class as a suffix")
    .version("2.3.1")
    .booleanConf
    .createWithDefault(true)

  val WHOLESTAGE_MAX_NUM_FIELDS = buildConf("spark.sql.codegen.maxFields")
    .internal()
    .doc("The maximum number of fields (including nested fields) that will be supported before" +
      " deactivating whole-stage codegen.")
    .version("2.0.0")
    .intConf
    .createWithDefault(100)

  val CODEGEN_FACTORY_MODE = buildConf("spark.sql.codegen.factoryMode")
    .doc("This config determines the fallback behavior of several codegen generators " +
      "during tests. `FALLBACK` means trying codegen first and then falling back to " +
      "interpreted if any compile error happens. Disabling fallback if `CODEGEN_ONLY`. " +
      "`NO_CODEGEN` skips codegen and goes interpreted path always. Note that " +
      "this configuration is only for the internal usage, and NOT supposed to be set by " +
      "end users.")
    .version("2.4.0")
    .internal()
    .stringConf
    .checkValues(CodegenObjectFactoryMode.values.map(_.toString))
    .createWithDefault(CodegenObjectFactoryMode.FALLBACK.toString)

  val CODEGEN_FALLBACK = buildConf("spark.sql.codegen.fallback")
    .internal()
    .doc("When true, (whole stage) codegen could be temporary disabled for the part of query that" +
      " fail to compile generated code")
    .version("2.0.0")
    .booleanConf
    .createWithDefault(true)

  val CODEGEN_LOGGING_MAX_LINES = buildConf("spark.sql.codegen.logging.maxLines")
    .internal()
    .doc("The maximum number of codegen lines to log when errors occur. Use -1 for unlimited.")
    .version("2.3.0")
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
    .version("2.3.0")
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
    .version("3.0.0")
    .intConf
    .checkValue(threshold => threshold > 0, "The threshold must be a positive integer.")
    .createWithDefault(1024)

  val WHOLESTAGE_SPLIT_CONSUME_FUNC_BY_OPERATOR =
    buildConf("spark.sql.codegen.splitConsumeFuncByOperator")
      .internal()
      .doc("When true, whole stage codegen would put the logic of consuming rows of each " +
        "physical operator into individual methods, instead of a single big method. This can be " +
        "used to avoid oversized function that can miss the opportunity of JIT optimization.")
      .version("2.3.1")
      .booleanConf
      .createWithDefault(true)

  val WHOLESTAGE_BROADCAST_CLEANED_SOURCE_THRESHOLD =
    buildConf("spark.sql.codegen.broadcastCleanedSourceThreshold")
      .internal()
      .doc("A threshold (in string length) to determine if we should make the generated code a" +
        "broadcast variable in whole stage codegen. To disable this, set the threshold to < 0; " +
        "otherwise if the size is above the threshold, it'll use broadcast variable. Note that " +
        "maximum string length allowed in Java is Integer.MAX_VALUE, so anything above it would " +
        "be meaningless. The default value is set to -1 (disabled by default).")
      .version("4.0.0")
      .intConf
      .createWithDefault(-1)

  val FILES_MAX_PARTITION_BYTES = buildConf("spark.sql.files.maxPartitionBytes")
    .doc("The maximum number of bytes to pack into a single partition when reading files. " +
      "This configuration is effective only when using file-based sources such as Parquet, JSON " +
      "and ORC.")
    .version("2.0.0")
    .bytesConf(ByteUnit.BYTE)
    .createWithDefaultString("128MB") // parquet.block.size

  val FILES_OPEN_COST_IN_BYTES = buildConf("spark.sql.files.openCostInBytes")
    .internal()
    .doc("The estimated cost to open a file, measured by the number of bytes could be scanned in" +
      " the same time. This is used when putting multiple files into a partition. It's better to" +
      " over estimated, then the partitions with small files will be faster than partitions with" +
      " bigger files (which is scheduled first). This configuration is effective only when using" +
      " file-based sources such as Parquet, JSON and ORC.")
    .version("2.0.0")
    .bytesConf(ByteUnit.BYTE)
    .createWithDefaultString("4MB")

  val FILES_MIN_PARTITION_NUM = buildConf("spark.sql.files.minPartitionNum")
    .doc("The suggested (not guaranteed) minimum number of split file partitions. " +
      s"If not set, the default value is `${LEAF_NODE_DEFAULT_PARALLELISM.key}`. " +
      "This configuration is effective only when using file-based sources " +
      "such as Parquet, JSON and ORC.")
    .version("3.1.0")
    .intConf
    .checkValue(v => v > 0, "The min partition number must be a positive integer.")
    .createOptional

  val FILES_MAX_PARTITION_NUM = buildConf("spark.sql.files.maxPartitionNum")
    .doc("The suggested (not guaranteed) maximum number of split file partitions. If it is set, " +
      "Spark will rescale each partition to make the number of partitions is close to this " +
      "value if the initial number of partitions exceeds this value. This configuration is " +
      "effective only when using file-based sources such as Parquet, JSON and ORC.")
    .version("3.5.0")
    .intConf
    .checkValue(v => v > 0, "The maximum number of partitions must be a positive integer.")
    .createOptional

  val IGNORE_CORRUPT_FILES = buildConf("spark.sql.files.ignoreCorruptFiles")
    .doc("Whether to ignore corrupt files. If true, the Spark jobs will continue to run when " +
      "encountering corrupted files and the contents that have been read will still be returned. " +
      "This configuration is effective only when using file-based sources such as Parquet, JSON " +
      "and ORC.")
    .version("2.1.1")
    .booleanConf
    .createWithDefault(false)

  val IGNORE_MISSING_FILES = buildConf("spark.sql.files.ignoreMissingFiles")
    .doc("Whether to ignore missing files. If true, the Spark jobs will continue to run when " +
      "encountering missing files and the contents that have been read will still be returned. " +
      "This configuration is effective only when using file-based sources such as Parquet, JSON " +
      "and ORC.")
    .version("2.3.0")
    .booleanConf
    .createWithDefault(false)

  val IGNORE_INVALID_PARTITION_PATHS = buildConf("spark.sql.files.ignoreInvalidPartitionPaths")
    .doc("Whether to ignore invalid partition paths that do not match <column>=<value>. When " +
      "the option is enabled, table with two partition directories 'table/invalid' and " +
      "'table/col=1' will only load the latter directory and ignore the invalid partition")
    .version("4.0.0")
    .booleanConf
    .createWithDefault(false)

  val MAX_RECORDS_PER_FILE = buildConf("spark.sql.files.maxRecordsPerFile")
    .doc("Maximum number of records to write out to a single file. " +
      "If this value is zero or negative, there is no limit.")
    .version("2.2.0")
    .longConf
    .createWithDefault(0)

  val EXCHANGE_REUSE_ENABLED = buildConf("spark.sql.exchange.reuse")
    .internal()
    .doc("When true, the planner will try to find out duplicated exchanges and re-use them.")
    .version("2.0.0")
    .booleanConf
    .createWithDefault(true)

  val SUBQUERY_REUSE_ENABLED = buildConf("spark.sql.execution.reuseSubquery")
    .internal()
    .doc("When true, the planner will try to find out duplicated subqueries and re-use them.")
    .version("3.0.0")
    .booleanConf
    .createWithDefault(true)

  val REMOVE_REDUNDANT_PROJECTS_ENABLED = buildConf("spark.sql.execution.removeRedundantProjects")
    .internal()
    .doc("Whether to remove redundant project exec node based on children's output and " +
      "ordering requirement.")
    .version("3.1.0")
    .booleanConf
    .createWithDefault(true)

  val REMOVE_REDUNDANT_SORTS_ENABLED = buildConf("spark.sql.execution.removeRedundantSorts")
    .internal()
    .doc("Whether to remove redundant physical sort node")
    .version("2.4.8")
    .booleanConf
    .createWithDefault(true)

  val REPLACE_HASH_WITH_SORT_AGG_ENABLED = buildConf("spark.sql.execution.replaceHashWithSortAgg")
    .internal()
    .doc("Whether to replace hash aggregate node with sort aggregate based on children's ordering")
    .version("3.3.0")
    .booleanConf
    .createWithDefault(false)

  val USE_PARTITION_EVALUATOR = buildConf("spark.sql.execution.usePartitionEvaluator")
    .internal()
    .doc("When true, use PartitionEvaluator to execute SQL operators.")
    .version("3.5.0")
    .booleanConf
    .createWithDefault(false)

  val STATE_STORE_PROVIDER_CLASS =
    buildConf("spark.sql.streaming.stateStore.providerClass")
      .internal()
      .doc(
        "The class used to manage state data in stateful streaming queries. This class must " +
          "be a subclass of StateStoreProvider, and must have a zero-arg constructor. " +
          "Note: For structured streaming, this configuration cannot be changed between query " +
          "restarts from the same checkpoint location.")
      .version("2.3.0")
      .stringConf
      .createWithDefault(
        "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider")

  val NUM_STATE_STORE_MAINTENANCE_THREADS =
    buildConf("spark.sql.streaming.stateStore.numStateStoreMaintenanceThreads")
      .internal()
      .doc("Number of threads in the thread pool that perform clean up and snapshotting tasks " +
        "for stateful streaming queries. The default value is the number of cores * 0.25 " +
        "so that this thread pool doesn't take too many resources " +
        "away from the query and affect performance.")
      .intConf
      .checkValue(_ > 0, "Must be greater than 0")
      .createWithDefault(Math.max(Runtime.getRuntime.availableProcessors() / 4, 1))

  val STATE_SCHEMA_CHECK_ENABLED =
    buildConf("spark.sql.streaming.stateStore.stateSchemaCheck")
      .doc("When true, Spark will validate the state schema against schema on existing state and " +
        "fail query if it's incompatible.")
      .version("3.1.0")
      .booleanConf
      .createWithDefault(true)

  val STATE_STORE_MIN_DELTAS_FOR_SNAPSHOT =
    buildConf("spark.sql.streaming.stateStore.minDeltasForSnapshot")
      .internal()
      .doc("Minimum number of state store delta files that needs to be generated before they " +
        "consolidated into snapshots.")
      .version("2.0.0")
      .intConf
      .createWithDefault(10)

  val STATE_STORE_FORMAT_VALIDATION_ENABLED =
    buildConf("spark.sql.streaming.stateStore.formatValidation.enabled")
      .internal()
      .doc("When true, check if the data from state store is valid or not when running streaming " +
        "queries. This can happen if the state store format has been changed. Note, the feature " +
        "is only effective in the build-in HDFS state store provider now.")
      .version("3.1.0")
      .booleanConf
      .createWithDefault(true)

  val FLATMAPGROUPSWITHSTATE_STATE_FORMAT_VERSION =
    buildConf("spark.sql.streaming.flatMapGroupsWithState.stateFormatVersion")
      .internal()
      .doc("State format version used by flatMapGroupsWithState operation in a streaming query")
      .version("2.4.0")
      .intConf
      .checkValue(v => Set(1, 2).contains(v), "Valid versions are 1 and 2")
      .createWithDefault(2)

  val CHECKPOINT_LOCATION = buildConf("spark.sql.streaming.checkpointLocation")
    .doc("The default location for storing checkpoint data for streaming queries.")
    .version("2.0.0")
    .stringConf
    .createOptional

  val FORCE_DELETE_TEMP_CHECKPOINT_LOCATION =
    buildConf("spark.sql.streaming.forceDeleteTempCheckpointLocation")
      .doc("When true, enable temporary checkpoint locations force delete.")
      .version("3.0.0")
      .booleanConf
      .createWithDefault(false)

  val MIN_BATCHES_TO_RETAIN = buildConf("spark.sql.streaming.minBatchesToRetain")
    .internal()
    .doc("The minimum number of batches that must be retained and made recoverable.")
    .version("2.1.1")
    .intConf
    .createWithDefault(100)

  val RATIO_EXTRA_SPACE_ALLOWED_IN_CHECKPOINT =
    buildConf("spark.sql.streaming.ratioExtraSpaceAllowedInCheckpoint")
    .internal()
    .doc("The ratio of extra space allowed for batch deletion of files when maintenance is" +
      "invoked. When value > 0, it optimizes the cost of discovering and deleting old checkpoint " +
      "versions. The minimum number of stale versions we retain in checkpoint location for batch " +
      "deletion is calculated by minBatchesToRetain * ratioExtraSpaceAllowedInCheckpoint.")
    .version("4.0.0")
    .doubleConf
    .createWithDefault(0.3)

  val MAX_BATCHES_TO_RETAIN_IN_MEMORY = buildConf("spark.sql.streaming.maxBatchesToRetainInMemory")
    .internal()
    .doc("The maximum number of batches which will be retained in memory to avoid " +
      "loading from files. The value adjusts a trade-off between memory usage vs cache miss: " +
      "'2' covers both success and direct failure cases, '1' covers only success case, " +
      "and '0' covers extreme case - disable cache to maximize memory size of executors.")
    .version("2.4.0")
    .intConf
    .createWithDefault(2)

  val STREAMING_MAINTENANCE_INTERVAL =
    buildConf("spark.sql.streaming.stateStore.maintenanceInterval")
      .internal()
      .doc("The interval in milliseconds between triggering maintenance tasks in StateStore. " +
        "The maintenance task executes background maintenance task in all the loaded store " +
        "providers if they are still the active instances according to the coordinator. If not, " +
        "inactive instances of store providers will be closed.")
      .version("2.0.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefault(TimeUnit.MINUTES.toMillis(1)) // 1 minute

  val STREAMING_TRANSFORM_WITH_STATE_OP_STATE_SCHEMA_VERSION =
    buildConf("spark.sql.streaming.transformWithState.stateSchemaVersion")
      .doc("The version of the state schema used by the transformWithState operator")
      .version("4.0.0")
      .intConf
      .createWithDefault(3)

  val STATE_STORE_COMPRESSION_CODEC =
    buildConf("spark.sql.streaming.stateStore.compression.codec")
      .internal()
      .doc("The codec used to compress delta and snapshot files generated by StateStore. " +
        "By default, Spark provides four codecs: lz4, lzf, snappy, and zstd. You can also " +
        "use fully qualified class names to specify the codec. Default codec is lz4.")
      .version("3.1.0")
      .stringConf
      .createWithDefault(CompressionCodec.LZ4)

  val CHECKPOINT_RENAMEDFILE_CHECK_ENABLED =
    buildConf("spark.sql.streaming.checkpoint.renamedFileCheck.enabled")
      .doc("When true, Spark will validate if renamed checkpoint file exists.")
      .internal()
      .version("3.4.0")
      .booleanConf
      .createWithDefault(false)

  /**
   * Note: this is defined in `RocksDBConf.FORMAT_VERSION`. These two places should be updated
   * together.
   */
  val STATE_STORE_ROCKSDB_FORMAT_VERSION =
    buildConf("spark.sql.streaming.stateStore.rocksdb.formatVersion")
      .internal()
      .doc("Set the RocksDB format version. This will be stored in the checkpoint when starting " +
        "a streaming query. The checkpoint will use this RocksDB format version in the entire " +
        "lifetime of the query.")
      .version("3.2.0")
      .intConf
      .checkValue(_ >= 0, "Must not be negative")
      // 5 is the default table format version for RocksDB 6.20.3.
      .createWithDefault(5)

  val STREAMING_AGGREGATION_STATE_FORMAT_VERSION =
    buildConf("spark.sql.streaming.aggregation.stateFormatVersion")
      .internal()
      .doc("State format version used by streaming aggregation operations in a streaming query. " +
        "State between versions are tend to be incompatible, so state format version shouldn't " +
        "be modified after running.")
      .version("2.4.0")
      .intConf
      .checkValue(v => Set(1, 2).contains(v), "Valid versions are 1 and 2")
      .createWithDefault(2)

  val STREAMING_STOP_ACTIVE_RUN_ON_RESTART =
    buildConf("spark.sql.streaming.stopActiveRunOnRestart")
    .doc("Running multiple runs of the same streaming query concurrently is not supported. " +
      "If we find a concurrent active run for a streaming query (in the same or different " +
      "SparkSessions on the same cluster) and this flag is true, we will stop the old streaming " +
      "query run to start the new one.")
    .version("3.0.0")
    .booleanConf
    .createWithDefault(true)

  val STREAMING_JOIN_STATE_FORMAT_VERSION =
    buildConf("spark.sql.streaming.join.stateFormatVersion")
      .internal()
      .doc("State format version used by streaming join operations in a streaming query. " +
        "State between versions are tend to be incompatible, so state format version shouldn't " +
        "be modified after running.")
      .version("3.0.0")
      .intConf
      .checkValue(v => Set(1, 2).contains(v), "Valid versions are 1 and 2")
      .createWithDefault(2)

  val STREAMING_SESSION_WINDOW_MERGE_SESSIONS_IN_LOCAL_PARTITION =
    buildConf("spark.sql.streaming.sessionWindow.merge.sessions.in.local.partition")
      .doc("When true, streaming session window sorts and merge sessions in local partition " +
        "prior to shuffle. This is to reduce the rows to shuffle, but only beneficial when " +
        "there're lots of rows in a batch being assigned to same sessions.")
      .version("3.2.0")
      .booleanConf
      .createWithDefault(false)

  val STREAMING_SESSION_WINDOW_STATE_FORMAT_VERSION =
    buildConf("spark.sql.streaming.sessionWindow.stateFormatVersion")
      .internal()
      .doc("State format version used by streaming session window in a streaming query. " +
        "State between versions are tend to be incompatible, so state format version shouldn't " +
        "be modified after running.")
      .version("3.2.0")
      .intConf
      .checkValue(v => Set(1).contains(v), "Valid version is 1")
      .createWithDefault(1)

  val UNSUPPORTED_OPERATION_CHECK_ENABLED =
    buildConf("spark.sql.streaming.unsupportedOperationCheck")
      .internal()
      .doc("When true, the logical plan for streaming query will be checked for unsupported" +
        " operations.")
      .version("2.0.0")
      .booleanConf
      .createWithDefault(true)

  val USE_DEPRECATED_KAFKA_OFFSET_FETCHING =
    buildConf("spark.sql.streaming.kafka.useDeprecatedOffsetFetching")
      .internal()
      .doc("When true, the deprecated Consumer based offset fetching used which could cause " +
        "infinite wait in Spark queries. Such cases query restart is the only workaround. " +
        "For further details please see Offset Fetching chapter of Structured Streaming Kafka " +
        "Integration Guide.")
      .version("3.1.0")
      .booleanConf
      .createWithDefault(false)

  val STATEFUL_OPERATOR_CHECK_CORRECTNESS_ENABLED =
    buildConf("spark.sql.streaming.statefulOperator.checkCorrectness.enabled")
      .internal()
      .doc("When true, the stateful operators for streaming query will be checked for possible " +
        "correctness issue due to global watermark. The correctness issue comes from queries " +
        "containing stateful operation which can emit rows older than the current watermark " +
        "plus allowed late record delay, which are \"late rows\" in downstream stateful " +
        "operations and these rows can be discarded. Please refer the programming guide doc for " +
        "more details. Once the issue is detected, Spark will throw analysis exception. " +
        "When this config is disabled, Spark will just print warning message for users. " +
        "Prior to Spark 3.1.0, the behavior is disabling this config.")
      .version("3.1.0")
      .booleanConf
      .createWithDefault(true)

  val STATEFUL_OPERATOR_ALLOW_MULTIPLE =
    buildConf("spark.sql.streaming.statefulOperator.allowMultiple")
      .internal()
      .doc("When true, multiple stateful operators are allowed to be present in a streaming " +
        "pipeline. The support for multiple stateful operators introduces a minor (semantically " +
        "correct) change in respect to late record filtering - late records are detected and " +
        "filtered in respect to the watermark from the previous microbatch instead of the " +
        "current one. This is a behavior change for Spark streaming pipelines and we allow " +
        "users to revert to the previous behavior of late record filtering (late records are " +
        "detected and filtered by comparing with the current microbatch watermark) by setting " +
        "the flag value to false. In this mode, only a single stateful operator will be allowed " +
        "in a streaming pipeline.")
      .version("3.4.0")
      .booleanConf
      .createWithDefault(true)

  val STATEFUL_OPERATOR_USE_STRICT_DISTRIBUTION =
    buildConf("spark.sql.streaming.statefulOperator.useStrictDistribution")
      .internal()
      .doc("The purpose of this config is only compatibility; DO NOT MANUALLY CHANGE THIS!!! " +
        "When true, the stateful operator for streaming query will use " +
        "StatefulOpClusteredDistribution which guarantees stable state partitioning as long as " +
        "the operator provides consistent grouping keys across the lifetime of query. " +
        "When false, the stateful operator for streaming query will use ClusteredDistribution " +
        "which is not sufficient to guarantee stable state partitioning despite the operator " +
        "provides consistent grouping keys across the lifetime of query. " +
        "This config will be set to true for new streaming queries to guarantee stable state " +
        "partitioning, and set to false for existing streaming queries to not break queries " +
        "which are restored from existing checkpoints. Please refer SPARK-38204 for details.")
      .version("3.3.0")
      .booleanConf
      .createWithDefault(true)

  val FILESTREAM_SINK_METADATA_IGNORED =
    buildConf("spark.sql.streaming.fileStreamSink.ignoreMetadata")
      .internal()
      .doc("If this is enabled, when Spark reads from the results of a streaming query written " +
        "by `FileStreamSink`, Spark will ignore the metadata log and treat it as normal path to " +
        "read, e.g. listing files using HDFS APIs.")
      .version("3.2.0")
      .booleanConf
      .createWithDefault(false)

  /**
   * SPARK-38809 - Config option to allow skipping null values for hash based stream-stream joins.
   * Its possible for us to see nulls if state was written with an older version of Spark,
   * the state was corrupted on disk or if we had an issue with the state iterators.
   */
  val STATE_STORE_SKIP_NULLS_FOR_STREAM_STREAM_JOINS =
  buildConf("spark.sql.streaming.stateStore.skipNullsForStreamStreamJoins.enabled")
    .internal()
    .doc("When true, this config will skip null values in hash based stream-stream joins. " +
      "The number of skipped null values will be shown as custom metric of stream join operator. " +
      "If the streaming query was started with Spark 3.5 or above, please exercise caution " +
      "before enabling this config since it may hide potential data loss/corruption issues.")
    .version("3.3.0")
    .booleanConf
    .createWithDefault(false)

  val ASYNC_LOG_PURGE =
    buildConf("spark.sql.streaming.asyncLogPurge.enabled")
      .internal()
      .doc("When true, purging the offset log and " +
        "commit log of old entries will be done asynchronously.")
      .version("3.4.0")
      .booleanConf
      .createWithDefault(true)

  val STREAMING_METADATA_CACHE_ENABLED =
    buildConf("spark.sql.streaming.metadataCache.enabled")
      .internal()
      .doc("Whether the streaming HDFSMetadataLog caches the metadata of the latest two batches.")
      .booleanConf
      .createWithDefault(true)

  val STREAMING_TRIGGER_AVAILABLE_NOW_WRAPPER_ENABLED =
    buildConf("spark.sql.streaming.triggerAvailableNowWrapper.enabled")
      .internal()
      .doc("Whether to use the wrapper implementation of Trigger.AvailableNow if the source " +
        "does not support Trigger.AvailableNow. Enabling this allows the benefits of " +
        "Trigger.AvailableNow with sources which don't support it, but some sources " +
        "may show unexpected behavior including duplication, data loss, etc. So use with " +
        "extreme care! The ideal direction is to persuade developers of source(s) to " +
        "support Trigger.AvailableNow.")
      .booleanConf
      .createWithDefault(false)

  val STREAMING_OPTIMIZE_ONE_ROW_PLAN_ENABLED =
    buildConf("spark.sql.streaming.optimizeOneRowPlan.enabled")
      .internal()
      .doc("When true, enable OptimizeOneRowPlan rule for the case where the child is a " +
        "streaming Dataset. This is a fallback flag to revert the 'incorrect' behavior, hence " +
        "this configuration must not be used without understanding in depth. Use this only to " +
        "quickly recover failure in existing query!")
      .version("4.0.0")
      .booleanConf
      .createWithDefault(false)

  val VARIABLE_SUBSTITUTE_ENABLED =
    buildConf("spark.sql.variable.substitute")
      .doc("This enables substitution using syntax like `${var}`, `${system:var}`, " +
        "and `${env:var}`.")
      .version("2.0.0")
      .booleanConf
      .createWithDefault(true)

  val ENABLE_TWOLEVEL_AGG_MAP =
    buildConf("spark.sql.codegen.aggregate.map.twolevel.enabled")
      .internal()
      .doc("Enable two-level aggregate hash map. When enabled, records will first be " +
        "inserted/looked-up at a 1st-level, small, fast map, and then fallback to a " +
        "2nd-level, larger, slower map when 1st level is full or keys cannot be found. " +
        "When disabled, records go directly to the 2nd level.")
      .version("2.3.0")
      .booleanConf
      .createWithDefault(true)

  val ENABLE_TWOLEVEL_AGG_MAP_PARTIAL_ONLY =
    buildConf("spark.sql.codegen.aggregate.map.twolevel.partialOnly")
      .internal()
      .doc("Enable two-level aggregate hash map for partial aggregate only, " +
        "because final aggregate might get more distinct keys compared to partial aggregate. " +
        "Overhead of looking up 1st-level map might dominate when having a lot of distinct keys.")
      .version("3.2.1")
      .booleanConf
      .createWithDefault(true)

  val ENABLE_VECTORIZED_HASH_MAP =
    buildConf("spark.sql.codegen.aggregate.map.vectorized.enable")
      .internal()
      .doc("Enable vectorized aggregate hash map. This is for testing/benchmarking only.")
      .version("3.0.0")
      .booleanConf
      .createWithDefault(false)

  val CODEGEN_SPLIT_AGGREGATE_FUNC =
    buildConf("spark.sql.codegen.aggregate.splitAggregateFunc.enabled")
      .internal()
      .doc("When true, the code generator would split aggregate code into individual methods " +
        "instead of a single big method. This can be used to avoid oversized function that " +
        "can miss the opportunity of JIT optimization.")
      .version("3.0.0")
      .booleanConf
      .createWithDefault(true)

  val ENABLE_SORT_AGGREGATE_CODEGEN =
    buildConf("spark.sql.codegen.aggregate.sortAggregate.enabled")
      .internal()
      .doc("When true, enable code-gen for sort aggregate.")
      .version("3.3.0")
      .booleanConf
      .createWithDefault(true)

  val ENABLE_FULL_OUTER_SHUFFLED_HASH_JOIN_CODEGEN =
    buildConf("spark.sql.codegen.join.fullOuterShuffledHashJoin.enabled")
      .internal()
      .doc("When true, enable code-gen for FULL OUTER shuffled hash join.")
      .version("3.3.0")
      .booleanConf
      .createWithDefault(true)

  val ENABLE_BUILD_SIDE_OUTER_SHUFFLED_HASH_JOIN_CODEGEN =
    buildConf("spark.sql.codegen.join.buildSideOuterShuffledHashJoin.enabled")
      .internal()
      .doc("When true, enable code-gen for an OUTER shuffled hash join where outer side" +
        " is the build side.")
      .version("3.5.0")
      .booleanConf
      .createWithDefault(true)

  val ENABLE_FULL_OUTER_SORT_MERGE_JOIN_CODEGEN =
    buildConf("spark.sql.codegen.join.fullOuterSortMergeJoin.enabled")
      .internal()
      .doc("When true, enable code-gen for FULL OUTER sort merge join.")
      .version("3.3.0")
      .booleanConf
      .createWithDefault(true)

  val ENABLE_EXISTENCE_SORT_MERGE_JOIN_CODEGEN =
    buildConf("spark.sql.codegen.join.existenceSortMergeJoin.enabled")
      .internal()
      .doc("When true, enable code-gen for Existence sort merge join.")
      .version("3.3.0")
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
      .version("2.2.0")
      .intConf
      .checkValue(depth => depth > 0, "The maximum depth of a view reference in a nested view " +
        "must be positive.")
      .createWithDefault(100)

  val ALLOW_PARAMETERLESS_COUNT =
    buildConf("spark.sql.legacy.allowParameterlessCount")
      .internal()
      .doc("When true, the SQL function 'count' is allowed to take no parameters.")
      .version("3.1.1")
      .booleanConf
      .createWithDefault(false)

  val ALLOW_NON_EMPTY_LOCATION_IN_CTAS =
    buildConf("spark.sql.legacy.allowNonEmptyLocationInCTAS")
      .internal()
      .doc("When false, CTAS with LOCATION throws an analysis exception if the " +
        "location is not empty.")
      .version("3.2.0")
      .booleanConf
      .createWithDefault(false)

  val ALLOW_STAR_WITH_SINGLE_TABLE_IDENTIFIER_IN_COUNT =
    buildConf("spark.sql.legacy.allowStarWithSingleTableIdentifierInCount")
      .internal()
      .doc("When true, the SQL function 'count' is allowed to take single 'tblName.*' as parameter")
      .version("3.2")
      .booleanConf
      .createWithDefault(false)

  val ALLOW_ZERO_INDEX_IN_FORMAT_STRING =
    buildConf("spark.sql.legacy.allowZeroIndexInFormatString")
      .internal()
      .doc("When false, the `strfmt` in `format_string(strfmt, obj, ...)` and " +
        "`printf(strfmt, obj, ...)` will no longer support to use \"0$\" to specify the first " +
        "argument, the first argument should always reference by \"1$\" when use argument index " +
        "to indicating the position of the argument in the argument list. " +
        "This config will be removed in the future releases.")
      .version("3.3")
      .booleanConf
      .createWithDefault(false)

  val USE_CURRENT_SQL_CONFIGS_FOR_VIEW =
    buildConf("spark.sql.legacy.useCurrentConfigsForView")
      .internal()
      .doc("When true, SQL Configs of the current active SparkSession instead of the captured " +
        "ones will be applied during the parsing and analysis phases of the view resolution.")
      .version("3.1.0")
      .booleanConf
      .createWithDefault(false)

  val STORE_ANALYZED_PLAN_FOR_VIEW =
    buildConf("spark.sql.legacy.storeAnalyzedPlanForView")
      .internal()
      .doc("When true, analyzed plan instead of SQL text will be stored when creating " +
        "temporary view")
      .version("3.1.0")
      .booleanConf
      .createWithDefault(false)

  val ALLOW_TEMP_VIEW_CREATION_WITH_MULTIPLE_NAME_PARTS =
    buildConf("spark.sql.legacy.allowTempViewCreationWithMultipleNameparts")
      .internal()
      .doc("When true, temp view creation Dataset APIs will allow the view creation even if " +
        "the view name is multiple name parts. The extra name parts will be dropped " +
        "during the view creation")
      .version("3.4.0")
      .booleanConf
      .createWithDefault(false)

  val ALLOW_AUTO_GENERATED_ALIAS_FOR_VEW =
    buildConf("spark.sql.legacy.allowAutoGeneratedAliasForView")
      .internal()
      .doc("When true, it's allowed to use a input query without explicit alias when creating " +
        "a permanent view.")
      .version("3.2.0")
      .booleanConf
      .createWithDefault(false)

  val STREAMING_FILE_COMMIT_PROTOCOL_CLASS =
    buildConf("spark.sql.streaming.commitProtocolClass")
      .version("2.1.0")
      .internal()
      .stringConf
      .createWithDefault("org.apache.spark.sql.execution.streaming.ManifestFileCommitProtocol")

  val STREAMING_MULTIPLE_WATERMARK_POLICY =
    buildConf("spark.sql.streaming.multipleWatermarkPolicy")
      .doc("Policy to calculate the global watermark value when there are multiple watermark " +
        "operators in a streaming query. The default value is 'min' which chooses " +
        "the minimum watermark reported across multiple operators. Other alternative value is " +
        "'max' which chooses the maximum across multiple operators. " +
        "Note: This configuration cannot be changed between query restarts from the same " +
        "checkpoint location.")
      .version("2.4.0")
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
      .version("2.2.0")
      .intConf
      // We are trying to be conservative and use a relatively small default count threshold here
      // since the state object of some TypedImperativeAggregate function can be quite large (e.g.
      // percentile_approx).
      .createWithDefault(128)

  val USE_OBJECT_HASH_AGG = buildConf("spark.sql.execution.useObjectHashAggregateExec")
    .internal()
    .doc("Decides if we use ObjectHashAggregateExec")
    .version("2.2.0")
    .booleanConf
    .createWithDefault(true)

  val JSON_GENERATOR_IGNORE_NULL_FIELDS =
    buildConf("spark.sql.jsonGenerator.ignoreNullFields")
      .doc("Whether to ignore null fields when generating JSON objects in JSON data source and " +
        "JSON functions such as to_json. " +
        "If false, it generates null for null fields in JSON objects.")
      .version("3.0.0")
      .booleanConf
      .createWithDefault(true)

  val JSON_EXPRESSION_OPTIMIZATION =
    buildConf("spark.sql.optimizer.enableJsonExpressionOptimization")
      .doc("Whether to optimize JSON expressions in SQL optimizer. It includes pruning " +
        "unnecessary columns from from_json, simplifying from_json + to_json, to_json + " +
        "named_struct(from_json.col1, from_json.col2, ....).")
      .version("3.1.0")
      .booleanConf
      .createWithDefault(true)

  val CSV_EXPRESSION_OPTIMIZATION =
    buildConf("spark.sql.optimizer.enableCsvExpressionOptimization")
      .doc("Whether to optimize CSV expressions in SQL optimizer. It includes pruning " +
        "unnecessary columns from from_csv.")
      .version("3.2.0")
      .booleanConf
      .createWithDefault(true)

  val COLLAPSE_PROJECT_ALWAYS_INLINE = buildConf("spark.sql.optimizer.collapseProjectAlwaysInline")
    .doc("Whether to always collapse two adjacent projections and inline expressions even if " +
      "it causes extra duplication.")
    .version("3.3.0")
    .booleanConf
    .createWithDefault(false)

  val AVOID_COLLAPSE_UDF_WITH_EXPENSIVE_EXPR =
    buildConf("spark.sql.optimizer.avoidCollapseUDFWithExpensiveExpr")
      .doc("Whether to avoid collapsing projections that would duplicate expensive expressions " +
        "in UDFs.")
      .version("4.0.0")
      .booleanConf
      .createWithDefault(true)

  val FILE_SINK_LOG_DELETION = buildConf("spark.sql.streaming.fileSink.log.deletion")
    .internal()
    .doc("Whether to delete the expired log files in file stream sink.")
    .version("2.0.0")
    .booleanConf
    .createWithDefault(true)

  val FILE_SINK_LOG_COMPACT_INTERVAL =
    buildConf("spark.sql.streaming.fileSink.log.compactInterval")
      .internal()
      .doc("Number of log files after which all the previous files " +
        "are compacted into the next log file.")
      .version("2.0.0")
      .intConf
      .createWithDefault(10)

  val FILE_SINK_LOG_CLEANUP_DELAY =
    buildConf("spark.sql.streaming.fileSink.log.cleanupDelay")
      .internal()
      .doc("How long that a file is guaranteed to be visible for all readers.")
      .version("2.0.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefault(TimeUnit.MINUTES.toMillis(10)) // 10 minutes

  val FILE_SOURCE_LOG_DELETION = buildConf("spark.sql.streaming.fileSource.log.deletion")
    .internal()
    .doc("Whether to delete the expired log files in file stream source.")
    .version("2.0.1")
    .booleanConf
    .createWithDefault(true)

  val FILE_SOURCE_LOG_COMPACT_INTERVAL =
    buildConf("spark.sql.streaming.fileSource.log.compactInterval")
      .internal()
      .doc("Number of log files after which all the previous files " +
        "are compacted into the next log file.")
      .version("2.0.1")
      .intConf
      .createWithDefault(10)

  val FILE_SOURCE_LOG_CLEANUP_DELAY =
    buildConf("spark.sql.streaming.fileSource.log.cleanupDelay")
      .internal()
      .doc("How long in milliseconds a file is guaranteed to be visible for all readers.")
      .version("2.0.1")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefault(TimeUnit.MINUTES.toMillis(10)) // 10 minutes

  val FILE_SOURCE_SCHEMA_FORCE_NULLABLE =
    buildConf("spark.sql.streaming.fileSource.schema.forceNullable")
      .internal()
      .doc("When true, force the schema of streaming file source to be nullable (including all " +
        "the fields). Otherwise, the schema might not be compatible with actual data, which " +
        "leads to corruptions.")
      .version("3.0.0")
      .booleanConf
      .createWithDefault(true)

  val FILE_SOURCE_CLEANER_NUM_THREADS =
    buildConf("spark.sql.streaming.fileSource.cleaner.numThreads")
      .doc("Number of threads used in the file source completed file cleaner.")
      .version("3.0.0")
      .intConf
      .createWithDefault(1)

  val STREAMING_SCHEMA_INFERENCE =
    buildConf("spark.sql.streaming.schemaInference")
      .internal()
      .doc("Whether file-based streaming sources will infer its own schema")
      .version("2.0.0")
      .booleanConf
      .createWithDefault(false)

  val STREAMING_POLLING_DELAY =
    buildConf("spark.sql.streaming.pollingDelay")
      .internal()
      .doc("How long to delay polling new data when no data is available")
      .version("2.0.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefault(10L)

  val STREAMING_STOP_TIMEOUT =
    buildConf("spark.sql.streaming.stopTimeout")
      .doc("How long to wait in milliseconds for the streaming execution thread to stop when " +
        "calling the streaming query's stop() method. 0 or negative values wait indefinitely.")
      .version("3.0.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("0")

  val STREAMING_NO_DATA_PROGRESS_EVENT_INTERVAL =
    buildConf("spark.sql.streaming.noDataProgressEventInterval")
      .internal()
      .doc("How long to wait before providing query idle event when there is no data")
      .version("2.1.1")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefault(10000L)

  val STREAMING_NO_DATA_MICRO_BATCHES_ENABLED =
    buildConf("spark.sql.streaming.noDataMicroBatches.enabled")
      .doc(
        "Whether streaming micro-batch engine will execute batches without data " +
          "for eager state management for stateful streaming queries.")
      .version("2.4.1")
      .booleanConf
      .createWithDefault(true)

  val STREAMING_METRICS_ENABLED =
    buildConf("spark.sql.streaming.metricsEnabled")
      .doc("Whether Dropwizard/Codahale metrics will be reported for active streaming queries.")
      .version("2.0.2")
      .booleanConf
      .createWithDefault(false)

  val STREAMING_PROGRESS_RETENTION =
    buildConf("spark.sql.streaming.numRecentProgressUpdates")
      .doc("The number of progress updates to retain for a streaming query")
      .version("2.1.1")
      .intConf
      .createWithDefault(100)

  val STREAMING_CHECKPOINT_FILE_MANAGER_CLASS =
    buildConf("spark.sql.streaming.checkpointFileManagerClass")
      .internal()
      .doc("The class used to write checkpoint files atomically. This class must be a subclass " +
        "of the interface CheckpointFileManager.")
      .version("2.4.0")
      .stringConf

  val STREAMING_CHECKPOINT_ESCAPED_PATH_CHECK_ENABLED =
    buildConf("spark.sql.streaming.checkpoint.escapedPathCheck.enabled")
      .internal()
      .doc("Whether to detect a streaming query may pick up an incorrect checkpoint path due " +
        "to SPARK-26824.")
      .version("3.0.0")
      .booleanConf
      .createWithDefault(true)

  val PARALLEL_FILE_LISTING_IN_STATS_COMPUTATION =
    buildConf("spark.sql.statistics.parallelFileListingInStatsComputation.enabled")
      .internal()
      .doc("When true, SQL commands use parallel file listing, " +
        "as opposed to single thread listing. " +
        "This usually speeds up commands that need to list many directories.")
      .version("2.4.1")
      .booleanConf
      .createWithDefault(true)

  val DEFAULT_SIZE_IN_BYTES = buildConf("spark.sql.defaultSizeInBytes")
    .internal()
    .doc("The default table size used in query planning. By default, it is set to Long.MaxValue " +
      s"which is larger than `${AUTO_BROADCASTJOIN_THRESHOLD.key}` to be more conservative. " +
      "That is to say by default the optimizer will not choose to broadcast a table unless it " +
      "knows for sure its size is small enough.")
    .version("1.1.0")
    .bytesConf(ByteUnit.BYTE)
    .createWithDefault(Long.MaxValue)

  val ENABLE_FALL_BACK_TO_HDFS_FOR_STATS = buildConf("spark.sql.statistics.fallBackToHdfs")
    .doc("When true, it will fall back to HDFS if the table statistics are not available from " +
      "table metadata. This is useful in determining if a table is small enough to use " +
      "broadcast joins. This flag is effective only for non-partitioned Hive tables. " +
      "For non-partitioned data source tables, it will be automatically recalculated if table " +
      "statistics are not available. For partitioned data source and partitioned Hive tables, " +
      s"It is '${DEFAULT_SIZE_IN_BYTES.key}' if table statistics are not available.")
    .version("2.0.0")
    .booleanConf
    .createWithDefault(false)

  val NDV_MAX_ERROR =
    buildConf("spark.sql.statistics.ndv.maxError")
      .internal()
      .doc("The maximum relative standard deviation allowed in HyperLogLog++ algorithm " +
        "when generating column level statistics.")
      .version("2.1.1")
      .doubleConf
      .createWithDefault(0.05)

  val HISTOGRAM_ENABLED =
    buildConf("spark.sql.statistics.histogram.enabled")
      .doc("Generates histograms when computing column statistics if enabled. Histograms can " +
        "provide better estimation accuracy. Currently, Spark only supports equi-height " +
        "histogram. Note that collecting histograms takes extra cost. For example, collecting " +
        "column statistics usually takes only one table scan, but generating equi-height " +
        "histogram will cause an extra table scan.")
      .version("2.3.0")
      .booleanConf
      .createWithDefault(false)

  val HISTOGRAM_NUM_BINS =
    buildConf("spark.sql.statistics.histogram.numBins")
      .internal()
      .doc("The number of bins when generating histograms.")
      .version("2.3.0")
      .intConf
      .checkValue(num => num > 1, "The number of bins must be greater than 1.")
      .createWithDefault(254)

  val PERCENTILE_ACCURACY =
    buildConf("spark.sql.statistics.percentile.accuracy")
      .internal()
      .doc("Accuracy of percentile approximation when generating equi-height histograms. " +
        "Larger value means better accuracy. The relative error can be deduced by " +
        "1.0 / PERCENTILE_ACCURACY.")
      .version("2.3.0")
      .intConf
      .createWithDefault(10000)

  val AUTO_SIZE_UPDATE_ENABLED =
    buildConf("spark.sql.statistics.size.autoUpdate.enabled")
      .doc("Enables automatic update for table size once table's data is changed. Note that if " +
        "the total number of files of the table is very large, this can be expensive and slow " +
        "down data change commands.")
      .version("2.3.0")
      .booleanConf
      .createWithDefault(false)

  val UPDATE_PART_STATS_IN_ANALYZE_TABLE_ENABLED =
    buildConf("spark.sql.statistics.updatePartitionStatsInAnalyzeTable.enabled")
      .doc("When this config is enabled, Spark will also update partition statistics in analyze " +
        "table command (i.e., ANALYZE TABLE .. COMPUTE STATISTICS [NOSCAN]). Note the command " +
        "will also become more expensive. When this config is disabled, Spark will only " +
        "update table level statistics.")
      .version("4.0.0")
      .booleanConf
      .createWithDefault(false)

  val CBO_ENABLED =
    buildConf("spark.sql.cbo.enabled")
      .doc("Enables CBO for estimation of plan statistics when set true.")
      .version("2.2.0")
      .booleanConf
      .createWithDefault(false)

  val PLAN_STATS_ENABLED =
    buildConf("spark.sql.cbo.planStats.enabled")
      .doc("When true, the logical plan will fetch row counts and column statistics from catalog.")
      .version("3.0.0")
      .booleanConf
      .createWithDefault(false)

  val JOIN_REORDER_ENABLED =
    buildConf("spark.sql.cbo.joinReorder.enabled")
      .doc("Enables join reorder in CBO.")
      .version("2.2.0")
      .booleanConf
      .createWithDefault(false)

  val JOIN_REORDER_DP_THRESHOLD =
    buildConf("spark.sql.cbo.joinReorder.dp.threshold")
      .doc("The maximum number of joined nodes allowed in the dynamic programming algorithm.")
      .version("2.2.0")
      .intConf
      .checkValue(number => number > 0, "The maximum number must be a positive integer.")
      .createWithDefault(12)

  val JOIN_REORDER_CARD_WEIGHT =
    buildConf("spark.sql.cbo.joinReorder.card.weight")
      .internal()
      .doc("The weight of the ratio of cardinalities (number of rows) " +
        "in the cost comparison function. The ratio of sizes in bytes has weight " +
        "1 - this value. The weighted geometric mean of these ratios is used to decide " +
        "which of the candidate plans will be chosen by the CBO.")
      .version("2.2.0")
      .doubleConf
      .checkValue(weight => weight >= 0 && weight <= 1, "The weight value must be in [0, 1].")
      .createWithDefault(0.7)

  val JOIN_REORDER_DP_STAR_FILTER =
    buildConf("spark.sql.cbo.joinReorder.dp.star.filter")
      .doc("Applies star-join filter heuristics to cost based join enumeration.")
      .version("2.2.0")
      .booleanConf
      .createWithDefault(false)

  val STARSCHEMA_DETECTION = buildConf("spark.sql.cbo.starSchemaDetection")
    .doc("When true, it enables join reordering based on star schema detection. ")
    .version("2.2.0")
    .booleanConf
    .createWithDefault(false)

  val STARSCHEMA_FACT_TABLE_RATIO = buildConf("spark.sql.cbo.starJoinFTRatio")
    .internal()
    .doc("Specifies the upper limit of the ratio between the largest fact tables" +
      " for a star join to be considered. ")
    .version("2.2.0")
    .doubleConf
    .createWithDefault(0.9)

  private def isValidTimezone(zone: String): Boolean = {
    Try { DateTimeUtils.getZoneId(zone) }.isSuccess
  }

  val SESSION_LOCAL_TIMEZONE = buildConf(SqlApiConfHelper.SESSION_LOCAL_TIMEZONE_KEY)
    .doc("The ID of session local timezone in the format of either region-based zone IDs or " +
      "zone offsets. Region IDs must have the form 'area/city', such as 'America/Los_Angeles'. " +
      "Zone offsets must be in the format '(+|-)HH', '(+|-)HH:mm' or '(+|-)HH:mm:ss', e.g '-08', " +
      "'+01:00' or '-13:33:33'. Also 'UTC' and 'Z' are supported as aliases of '+00:00'. Other " +
      "short names are not recommended to use because they can be ambiguous.")
    .version("2.2.0")
    .stringConf
    .checkValue(isValidTimezone, errorClass = "TIME_ZONE", parameters = tz => Map.empty)
    .createWithDefaultFunction(() => TimeZone.getDefault.getID)

  val WINDOW_EXEC_BUFFER_IN_MEMORY_THRESHOLD =
    buildConf("spark.sql.windowExec.buffer.in.memory.threshold")
      .internal()
      .doc("Threshold for number of rows guaranteed to be held in memory by the window operator")
      .version("2.2.1")
      .intConf
      .createWithDefault(4096)

  val WINDOW_EXEC_BUFFER_SPILL_THRESHOLD =
    buildConf("spark.sql.windowExec.buffer.spill.threshold")
      .internal()
      .doc("Threshold for number of rows to be spilled by window operator")
      .version("2.2.0")
      .intConf
      .createWithDefault(SHUFFLE_SPILL_NUM_ELEMENTS_FORCE_SPILL_THRESHOLD.defaultValue.get)

  val WINDOW_GROUP_LIMIT_THRESHOLD =
    buildConf("spark.sql.optimizer.windowGroupLimitThreshold")
      .internal()
      .doc("Threshold for triggering `InsertWindowGroupLimit`. " +
        "0 means the output results is empty. -1 means disabling the optimization.")
      .version("3.5.0")
      .intConf
      .checkValue(_ >= -1,
        "The threshold of window group limit must be -1, 0 or positive integer.")
      .createWithDefault(1000)

  val SESSION_WINDOW_BUFFER_IN_MEMORY_THRESHOLD =
    buildConf("spark.sql.sessionWindow.buffer.in.memory.threshold")
      .internal()
      .doc("Threshold for number of windows guaranteed to be held in memory by the " +
        "session window operator. Note that the buffer is used only for the query Spark " +
        "cannot apply aggregations on determining session window.")
      .version("3.2.0")
      .intConf
      .createWithDefault(4096)

  val SESSION_WINDOW_BUFFER_SPILL_THRESHOLD =
    buildConf("spark.sql.sessionWindow.buffer.spill.threshold")
      .internal()
      .doc("Threshold for number of rows to be spilled by window operator. Note that " +
        "the buffer is used only for the query Spark cannot apply aggregations on determining " +
        "session window.")
      .version("3.2.0")
      .intConf
      .createWithDefault(SHUFFLE_SPILL_NUM_ELEMENTS_FORCE_SPILL_THRESHOLD.defaultValue.get)

  val SHUFFLE_DEPENDENCY_SKIP_MIGRATION_ENABLED =
    buildConf("spark.sql.shuffleDependency.skipMigration.enabled")
      .doc("When enabled, shuffle dependencies for a Spark Connect SQL execution are marked at " +
        "the end of the execution, and they will not be migrated during decommissions.")
      .version("4.0.0")
      .booleanConf
      .createWithDefault(Utils.isTesting)

  val SHUFFLE_DEPENDENCY_FILE_CLEANUP_ENABLED =
    buildConf("spark.sql.shuffleDependency.fileCleanup.enabled")
      .doc("When enabled, shuffle files will be cleaned up at the end of Spark Connect " +
        "SQL executions.")
      .version("4.0.0")
      .booleanConf
      .createWithDefault(Utils.isTesting)

  val SORT_MERGE_JOIN_EXEC_BUFFER_IN_MEMORY_THRESHOLD =
    buildConf("spark.sql.sortMergeJoinExec.buffer.in.memory.threshold")
      .internal()
      .doc("Threshold for number of rows guaranteed to be held in memory by the sort merge " +
        "join operator")
      .version("2.2.1")
      .intConf
      .createWithDefault(ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH)

  val SORT_MERGE_JOIN_EXEC_BUFFER_SPILL_THRESHOLD =
    buildConf("spark.sql.sortMergeJoinExec.buffer.spill.threshold")
      .internal()
      .doc("Threshold for number of rows to be spilled by sort merge join operator")
      .version("2.2.0")
      .intConf
      .createWithDefault(SHUFFLE_SPILL_NUM_ELEMENTS_FORCE_SPILL_THRESHOLD.defaultValue.get)

  val CARTESIAN_PRODUCT_EXEC_BUFFER_IN_MEMORY_THRESHOLD =
    buildConf("spark.sql.cartesianProductExec.buffer.in.memory.threshold")
      .internal()
      .doc("Threshold for number of rows guaranteed to be held in memory by the cartesian " +
        "product operator")
      .version("2.2.1")
      .intConf
      .createWithDefault(4096)

  val CARTESIAN_PRODUCT_EXEC_BUFFER_SPILL_THRESHOLD =
    buildConf("spark.sql.cartesianProductExec.buffer.spill.threshold")
      .internal()
      .doc("Threshold for number of rows to be spilled by cartesian product operator")
      .version("2.2.0")
      .intConf
      .createWithDefault(SHUFFLE_SPILL_NUM_ELEMENTS_FORCE_SPILL_THRESHOLD.defaultValue.get)

  val SUPPORT_QUOTED_REGEX_COLUMN_NAME = buildConf("spark.sql.parser.quotedRegexColumnNames")
    .doc("When true, quoted Identifiers (using backticks) in SELECT statement are interpreted" +
      " as regular expressions.")
    .version("2.3.0")
    .booleanConf
    .createWithDefault(false)

  val TVF_ALLOW_MULTIPLE_TABLE_ARGUMENTS_ENABLED =
    buildConf("spark.sql.tvf.allowMultipleTableArguments.enabled")
      .doc("When true, allows multiple table arguments for table-valued functions, " +
        "receiving the cartesian product of all the rows of these tables.")
      .version("3.5.0")
      .booleanConf
      .createWithDefault(false)

  val RANGE_EXCHANGE_SAMPLE_SIZE_PER_PARTITION =
    buildConf("spark.sql.execution.rangeExchange.sampleSizePerPartition")
      .internal()
      .doc("Number of points to sample per partition in order to determine the range boundaries" +
          " for range partitioning, typically used in global sorting (without limit).")
      .version("2.3.0")
      .intConf
      .createWithDefault(100)

  val ARROW_EXECUTION_ENABLED =
    buildConf("spark.sql.execution.arrow.enabled")
      .doc("(Deprecated since Spark 3.0, please set 'spark.sql.execution.arrow.pyspark.enabled'.)")
      .version("2.3.0")
      .booleanConf
      .createWithDefault(false)

  val ARROW_PYSPARK_EXECUTION_ENABLED =
    buildConf("spark.sql.execution.arrow.pyspark.enabled")
      .doc("When true, make use of Apache Arrow for columnar data transfers in PySpark. " +
        "This optimization applies to: " +
        "1. pyspark.sql.DataFrame.toPandas. " +
        "2. pyspark.sql.SparkSession.createDataFrame when its input is a Pandas DataFrame " +
        "or a NumPy ndarray. " +
        "The following data type is unsupported: " +
        "ArrayType of TimestampType.")
      .version("3.0.0")
      .fallbackConf(ARROW_EXECUTION_ENABLED)

  val ARROW_PYSPARK_SELF_DESTRUCT_ENABLED =
    buildConf("spark.sql.execution.arrow.pyspark.selfDestruct.enabled")
      .doc("(Experimental) When true, make use of Apache Arrow's self-destruct and split-blocks " +
        "options for columnar data transfers in PySpark, when converting from Arrow to Pandas. " +
        "This reduces memory usage at the cost of some CPU time. " +
        "This optimization applies to: pyspark.sql.DataFrame.toPandas " +
        "when 'spark.sql.execution.arrow.pyspark.enabled' is set.")
      .version("3.2.0")
      .booleanConf
      .createWithDefault(false)

  val ARROW_LOCAL_RELATION_THRESHOLD =
    buildConf("spark.sql.execution.arrow.localRelationThreshold")
      .doc(
        "When converting Arrow batches to Spark DataFrame, local collections are used in the " +
          "driver side if the byte size of Arrow batches is smaller than this threshold. " +
          "Otherwise, the Arrow batches are sent and deserialized to Spark internal rows " +
          "in the executors.")
      .version("3.4.0")
      .bytesConf(ByteUnit.BYTE)
      .checkValue(_ >= 0, "This value must be equal to or greater than 0.")
      .createWithDefaultString("48MB")

  val PYSPARK_JVM_STACKTRACE_ENABLED =
    buildConf("spark.sql.pyspark.jvmStacktrace.enabled")
      .doc("When true, it shows the JVM stacktrace in the user-facing PySpark exception " +
        "together with Python stacktrace. By default, it is disabled to hide JVM stacktrace " +
        "and shows a Python-friendly exception only. Note that this is independent from log " +
        "level settings.")
      .version("3.0.0")
      .booleanConf
      // show full stacktrace in tests but hide in production by default.
      .createWithDefault(Utils.isTesting)

  val PYTHON_UDF_PROFILER =
    buildConf("spark.sql.pyspark.udf.profiler")
      .doc("Configure the Python/Pandas UDF profiler by enabling or disabling it " +
        "with the option to choose between \"perf\" and \"memory\" types, " +
        "or unsetting the config disables the profiler. This is disabled by default.")
      .version("4.0.0")
      .stringConf
      .transform(_.toLowerCase(Locale.ROOT))
      .checkValues(Set("perf", "memory"))
      .createOptional

  val PYTHON_UDF_WORKER_FAULTHANLDER_ENABLED =
    buildConf("spark.sql.execution.pyspark.udf.faulthandler.enabled")
      .doc(
        s"Same as ${Python.PYTHON_WORKER_FAULTHANLDER_ENABLED.key} for Python execution with " +
        "DataFrame and SQL. It can change during runtime.")
      .version("4.0.0")
      .fallbackConf(Python.PYTHON_WORKER_FAULTHANLDER_ENABLED)

  val ARROW_SPARKR_EXECUTION_ENABLED =
    buildConf("spark.sql.execution.arrow.sparkr.enabled")
      .doc("When true, make use of Apache Arrow for columnar data transfers in SparkR. " +
        "This optimization applies to: " +
        "1. createDataFrame when its input is an R DataFrame " +
        "2. collect " +
        "3. dapply " +
        "4. gapply " +
        "The following data types are unsupported: " +
        "FloatType, BinaryType, ArrayType, StructType and MapType.")
      .version("3.0.0")
      .booleanConf
      .createWithDefault(false)

  val ARROW_FALLBACK_ENABLED =
    buildConf("spark.sql.execution.arrow.fallback.enabled")
      .doc("(Deprecated since Spark 3.0, please set " +
        "'spark.sql.execution.arrow.pyspark.fallback.enabled'.)")
      .version("2.4.0")
      .booleanConf
      .createWithDefault(true)

  val ARROW_PYSPARK_FALLBACK_ENABLED =
    buildConf("spark.sql.execution.arrow.pyspark.fallback.enabled")
      .doc(s"When true, optimizations enabled by '${ARROW_PYSPARK_EXECUTION_ENABLED.key}' will " +
        "fallback automatically to non-optimized implementations if an error occurs.")
      .version("3.0.0")
      .fallbackConf(ARROW_FALLBACK_ENABLED)

  val ARROW_EXECUTION_MAX_RECORDS_PER_BATCH =
    buildConf("spark.sql.execution.arrow.maxRecordsPerBatch")
      .doc("When using Apache Arrow, limit the maximum number of records that can be written " +
        "to a single ArrowRecordBatch in memory. This configuration is not effective for the " +
        "grouping API such as DataFrame(.cogroup).groupby.applyInPandas because each group " +
        "becomes each ArrowRecordBatch. If set to zero or negative there is no limit.")
      .version("2.3.0")
      .intConf
      .createWithDefault(10000)

  val ARROW_EXECUTION_USE_LARGE_VAR_TYPES =
    buildConf("spark.sql.execution.arrow.useLargeVarTypes")
      .doc("When using Apache Arrow, use large variable width vectors for string and binary " +
        "types. Regular string and binary types have a 2GiB limit for a column in a single " +
        "record batch. Large variable types remove this limitation at the cost of higher memory " +
        "usage per value. Note that this only works for DataFrame.mapInArrow.")
      .version("3.5.0")
      .internal()
      .booleanConf
      .createWithDefault(false)

  val PANDAS_UDF_BUFFER_SIZE =
    buildConf("spark.sql.execution.pandas.udf.buffer.size")
      .doc(
        s"Same as `${BUFFER_SIZE.key}` but only applies to Pandas UDF executions. If it is not " +
        s"set, the fallback is `${BUFFER_SIZE.key}`. Note that Pandas execution requires more " +
        "than 4 bytes. Lowering this value could make small Pandas UDF batch iterated and " +
        "pipelined; however, it might degrade performance. See SPARK-27870.")
      .version("3.0.0")
      .fallbackConf(BUFFER_SIZE)

  val PANDAS_STRUCT_HANDLING_MODE =
    buildConf("spark.sql.execution.pandas.structHandlingMode")
      .doc(
        "The conversion mode of struct type when creating pandas DataFrame. " +
        "When \"legacy\"," +
        "1. when Arrow optimization is disabled, convert to Row object, " +
        "2. when Arrow optimization is enabled, convert to dict or raise an Exception " +
        "if there are duplicated nested field names. " +
        "When \"row\", convert to Row object regardless of Arrow optimization. " +
        "When \"dict\", convert to dict and use suffixed key names, e.g., a_0, a_1, " +
        "if there are duplicated nested field names, regardless of Arrow optimization."
      )
      .version("3.5.0")
      .stringConf
      .checkValues(Set("legacy", "row", "dict"))
      .createWithDefaultString("legacy")

  val PYSPARK_SIMPLIFIED_TRACEBACK =
    buildConf("spark.sql.execution.pyspark.udf.simplifiedTraceback.enabled")
      .doc(
        "When true, the traceback from Python UDFs is simplified. It hides " +
        "the Python worker, (de)serialization, etc from PySpark in tracebacks, and only " +
        "shows the exception messages from UDFs. Note that this works only with CPython 3.7+.")
      .version("3.1.0")
      .booleanConf
      // show full stacktrace in tests but hide in production by default.
      .createWithDefault(!Utils.isTesting)

  val PYTHON_UDF_ARROW_ENABLED =
    buildConf("spark.sql.execution.pythonUDF.arrow.enabled")
      .doc("Enable Arrow optimization in regular Python UDFs. This optimization " +
        "can only be enabled when the given function takes at least one argument.")
      .version("3.4.0")
      .booleanConf
      .createWithDefault(false)

  val PYTHON_TABLE_UDF_ARROW_ENABLED =
    buildConf("spark.sql.execution.pythonUDTF.arrow.enabled")
      .doc("Enable Arrow optimization for Python UDTFs.")
      .version("3.5.0")
      .booleanConf
      .createWithDefault(false)

  val PYTHON_PLANNER_EXEC_MEMORY =
    buildConf("spark.sql.planner.pythonExecution.memory")
      .doc("Specifies the memory allocation for executing Python code in Spark driver, in MiB. " +
        "When set, it caps the memory for Python execution to the specified amount. " +
        "If not set, Spark will not limit Python's memory usage and it is up to the application " +
        "to avoid exceeding the overhead memory space shared with other non-JVM processes.\n" +
        "Note: Windows does not support resource limiting and actual resource is not limited " +
        "on MacOS.")
      .version("4.0.0")
      .bytesConf(ByteUnit.MiB)
      .createOptional

  val PANDAS_GROUPED_MAP_ASSIGN_COLUMNS_BY_NAME =
    buildConf("spark.sql.legacy.execution.pandas.groupedMap.assignColumnsByName")
      .internal()
      .doc("When true, columns will be looked up by name if labeled with a string and fallback " +
        "to use position if not. When false, a grouped map Pandas UDF will assign columns from " +
        "the returned Pandas DataFrame based on position, regardless of column label type. " +
        "This configuration will be deprecated in future releases.")
      .version("2.4.1")
      .booleanConf
      .createWithDefault(true)

  val PANDAS_ARROW_SAFE_TYPE_CONVERSION =
    buildConf("spark.sql.execution.pandas.convertToArrowArraySafely")
      .internal()
      .doc("When true, Arrow will perform safe type conversion when converting " +
        "Pandas.Series to Arrow array during serialization. Arrow will raise errors " +
        "when detecting unsafe type conversion like overflow. When false, disabling Arrow's type " +
        "check and do type conversions anyway. This config only works for Arrow 0.11.0+.")
      .version("3.0.0")
      .booleanConf
      .createWithDefault(false)

  val PYSPARK_WORKER_PYTHON_EXECUTABLE =
    buildConf("spark.sql.execution.pyspark.python")
      .internal()
      .doc("Python binary executable to use for PySpark in executors when running Python " +
        "UDF, pandas UDF and pandas function APIs." +
        "If not set, it falls back to 'spark.pyspark.python' by default.")
      .version("3.5.0")
      .stringConf
      .createOptional

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
    .version("2.3.0")
    .booleanConf
    .createWithDefault(true)

  val DECIMAL_OPERATIONS_ALLOW_PREC_LOSS =
    buildConf("spark.sql.decimalOperations.allowPrecisionLoss")
      .internal()
      .doc("When true (default), establishing the result type of an arithmetic operation " +
        "happens according to Hive behavior and SQL ANSI 2011 specification, i.e. rounding the " +
        "decimal part of the result if an exact representation is not possible. Otherwise, NULL " +
        "is returned in those cases, as previously.")
      .version("2.3.1")
      .booleanConf
      .createWithDefault(true)

  val LITERAL_PICK_MINIMUM_PRECISION =
    buildConf("spark.sql.legacy.literal.pickMinimumPrecision")
      .internal()
      .doc("When integral literal is used in decimal operations, pick a minimum precision " +
        "required by the literal if this config is true, to make the resulting precision and/or " +
        "scale smaller. This can reduce the possibility of precision lose and/or overflow.")
      .version("2.3.3")
      .booleanConf
      .createWithDefault(true)

  val SQL_OPTIONS_REDACTION_PATTERN = buildConf("spark.sql.redaction.options.regex")
    .doc("Regex to decide which keys in a Spark SQL command's options map contain sensitive " +
      "information. The values of options whose names that match this regex will be redacted " +
      "in the explain output. This redaction is applied on top of the global redaction " +
      s"configuration defined by ${SECRET_REDACTION_PATTERN.key}.")
    .version("2.2.2")
    .regexConf
    .createWithDefault("(?i)url".r)

  val SQL_STRING_REDACTION_PATTERN =
    buildConf("spark.sql.redaction.string.regex")
      .doc("Regex to decide which parts of strings produced by Spark contain sensitive " +
        "information. When this regex matches a string part, that string part is replaced by a " +
        "dummy value. This is currently used to redact the output of SQL explain commands. " +
        "When this conf is not set, the value from `spark.redaction.string.regex` is used.")
      .version("2.3.0")
      .fallbackConf(org.apache.spark.internal.config.STRING_REDACTION_PATTERN)

  val CONCAT_BINARY_AS_STRING = buildConf("spark.sql.function.concatBinaryAsString")
    .doc("When this option is set to false and all inputs are binary, `functions.concat` returns " +
      "an output as binary. Otherwise, it returns as a string.")
    .version("2.3.0")
    .booleanConf
    .createWithDefault(false)

  val ELT_OUTPUT_AS_STRING = buildConf("spark.sql.function.eltOutputAsString")
    .doc("When this option is set to false and all inputs are binary, `elt` returns " +
      "an output as binary. Otherwise, it returns as a string.")
    .version("2.3.0")
    .booleanConf
    .createWithDefault(false)

  val VALIDATE_PARTITION_COLUMNS =
    buildConf("spark.sql.sources.validatePartitionColumns")
      .internal()
      .doc("When this option is set to true, partition column values will be validated with " +
        "user-specified schema. If the validation fails, a runtime exception is thrown. " +
        "When this option is set to false, the partition column value will be converted to null " +
        "if it can not be casted to corresponding user-specified schema.")
      .version("3.0.0")
      .booleanConf
      .createWithDefault(true)

  val CONTINUOUS_STREAMING_EPOCH_BACKLOG_QUEUE_SIZE =
    buildConf("spark.sql.streaming.continuous.epochBacklogQueueSize")
      .doc("The max number of entries to be stored in queue to wait for late epochs. " +
        "If this parameter is exceeded by the size of the queue, stream will stop with an error.")
      .version("3.0.0")
      .intConf
      .createWithDefault(10000)

  val CONTINUOUS_STREAMING_EXECUTOR_QUEUE_SIZE =
    buildConf("spark.sql.streaming.continuous.executorQueueSize")
      .internal()
      .doc("The size (measured in number of rows) of the queue used in continuous execution to" +
        " buffer the results of a ContinuousDataReader.")
      .version("2.3.0")
      .intConf
      .createWithDefault(1024)

  val CONTINUOUS_STREAMING_EXECUTOR_POLL_INTERVAL_MS =
    buildConf("spark.sql.streaming.continuous.executorPollIntervalMs")
      .internal()
      .doc("The interval at which continuous execution readers will poll to check whether" +
        " the epoch has advanced on the driver.")
      .version("2.3.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefault(100)

  val USE_V1_SOURCE_LIST = buildConf("spark.sql.sources.useV1SourceList")
    .internal()
    .doc("A comma-separated list of data source short names or fully qualified data source " +
      "implementation class names for which Data Source V2 code path is disabled. These data " +
      "sources will fallback to Data Source V1 code path.")
    .version("3.0.0")
    .stringConf
    .createWithDefault("avro,csv,json,kafka,orc,parquet,text")

  val ALLOW_EMPTY_SCHEMAS_FOR_WRITES = buildConf("spark.sql.legacy.allowEmptySchemaWrite")
    .internal()
    .doc("When this option is set to true, validation of empty or empty nested schemas that " +
      "occurs when writing into a FileFormat based data source does not happen.")
    .version("3.4.0")
    .booleanConf
    .createWithDefault(false)

  val DISABLED_V2_STREAMING_WRITERS = buildConf("spark.sql.streaming.disabledV2Writers")
    .doc("A comma-separated list of fully qualified data source register class names for which" +
      " StreamWriteSupport is disabled. Writes to these sources will fall back to the V1 Sinks.")
    .version("2.3.1")
    .stringConf
    .createWithDefault("")

  val DISABLED_V2_STREAMING_MICROBATCH_READERS =
    buildConf("spark.sql.streaming.disabledV2MicroBatchReaders")
      .internal()
      .doc(
        "A comma-separated list of fully qualified data source register class names for which " +
          "MicroBatchReadSupport is disabled. Reads from these sources will fall back to the " +
          "V1 Sources.")
      .version("2.4.0")
      .stringConf
      .createWithDefault("")

  val DISABLE_MAP_KEY_NORMALIZATION =
    buildConf("spark.sql.legacy.disableMapKeyNormalization")
      .internal()
      .doc("Disables key normalization when creating a map with `ArrayBasedMapBuilder`. When " +
        "set to `true` it will prevent key normalization when building a map, which will " +
        "allow for values such as `-0.0` and `0.0` to be present as distinct keys.")
      .version("4.0.0")
      .booleanConf
      .createWithDefault(false)

  val FASTFAIL_ON_FILEFORMAT_OUTPUT =
    buildConf("spark.sql.execution.fastFailOnFileFormatOutput")
      .internal()
      .doc("Whether to fast fail task execution when writing output to FileFormat datasource. " +
        "If this is enabled, in `FileFormatWriter` we will catch `FileAlreadyExistsException` " +
        "and fast fail output task without further task retry. Only enabling this if you know " +
        "the `FileAlreadyExistsException` of the output task is unrecoverable, i.e., further " +
        "task attempts won't be able to success. If the `FileAlreadyExistsException` might be " +
        "recoverable, you should keep this as disabled and let Spark to retry output tasks. " +
        "This is disabled by default.")
      .version("3.0.2")
      .booleanConf
      .createWithDefault(false)

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
      .version("2.3.0")
      .stringConf
      .transform(_.toUpperCase(Locale.ROOT))
      .checkValues(PartitionOverwriteMode.values.map(_.toString))
      .createWithDefault(PartitionOverwriteMode.STATIC.toString)

  object StoreAssignmentPolicy extends Enumeration {
    val ANSI, LEGACY, STRICT = Value
  }

  val STORE_ASSIGNMENT_POLICY =
    buildConf("spark.sql.storeAssignmentPolicy")
      .doc("When inserting a value into a column with different data type, Spark will perform " +
        "type coercion. Currently, we support 3 policies for the type coercion rules: ANSI, " +
        "legacy and strict. With ANSI policy, Spark performs the type coercion as per ANSI SQL. " +
        "In practice, the behavior is mostly the same as PostgreSQL. " +
        "It disallows certain unreasonable type conversions such as converting " +
        "`string` to `int` or `double` to `boolean`. " +
        "With legacy policy, Spark allows the type coercion as long as it is a valid `Cast`, " +
        "which is very loose. e.g. converting `string` to `int` or `double` to `boolean` is " +
        "allowed. It is also the only behavior in Spark 2.x and it is compatible with Hive. " +
        "With strict policy, Spark doesn't allow any possible precision loss or data truncation " +
        "in type coercion, e.g. converting `double` to `int` or `decimal` to `double` is " +
        "not allowed."
      )
      .version("3.0.0")
      .stringConf
      .transform(_.toUpperCase(Locale.ROOT))
      .checkValues(StoreAssignmentPolicy.values.map(_.toString))
      .createWithDefault(StoreAssignmentPolicy.ANSI.toString)

  val ANSI_ENABLED = buildConf(SqlApiConfHelper.ANSI_ENABLED_KEY)
    .doc("When true, Spark SQL uses an ANSI compliant dialect instead of being Hive compliant. " +
      "For example, Spark will throw an exception at runtime instead of returning null results " +
      "when the inputs to a SQL operator/function are invalid." +
      "For full details of this dialect, you can find them in the section \"ANSI Compliance\" of " +
      "Spark's documentation. Some ANSI dialect features may be not from the ANSI SQL " +
      "standard directly, but their behaviors align with ANSI SQL's style")
    .version("3.0.0")
    .booleanConf
    .createWithDefault(!sys.env.get("SPARK_ANSI_SQL_MODE").contains("false"))

  val ENFORCE_RESERVED_KEYWORDS = buildConf("spark.sql.ansi.enforceReservedKeywords")
    .doc(s"When true and '${ANSI_ENABLED.key}' is true, the Spark SQL parser enforces the ANSI " +
      "reserved keywords and forbids SQL queries that use reserved keywords as alias names " +
      "and/or identifiers for table, view, function, etc.")
    .version("3.3.0")
    .booleanConf
    .createWithDefault(false)

  val DOUBLE_QUOTED_IDENTIFIERS = buildConf("spark.sql.ansi.doubleQuotedIdentifiers")
    .doc(s"When true and '${ANSI_ENABLED.key}' is true, Spark SQL reads literals enclosed in " +
      "double quoted (\") as identifiers. When false they are read as string literals.")
    .version("3.4.0")
    .booleanConf
    .createWithDefault(false)

  val ANSI_RELATION_PRECEDENCE = buildConf("spark.sql.ansi.relationPrecedence")
    .doc(s"When true and '${ANSI_ENABLED.key}' is true, JOIN takes precedence over comma when " +
      "combining relation. For example, `t1, t2 JOIN t3` should result to `t1 X (t2 X t3)`. If " +
      "the config is false, the result is `(t1 X t2) X t3`.")
    .version("3.4.0")
    .booleanConf
    .createWithDefault(false)

  val CHUNK_BASE64_STRING_ENABLED = buildConf("spark.sql.chunkBase64String.enabled")
    .doc("Whether to truncate string generated by the `Base64` function. When true, base64" +
      " strings generated by the base64 function are chunked into lines of at most 76" +
      " characters. When false, the base64 strings are not chunked.")
    .version("3.5.2")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_DEFAULT_COLUMNS =
    buildConf("spark.sql.defaultColumn.enabled")
      .internal()
      .doc("When true, allow CREATE TABLE, REPLACE TABLE, and ALTER COLUMN statements to set or " +
        "update default values for specific columns. Following INSERT, MERGE, and UPDATE " +
        "statements may then omit these values and their values will be injected automatically " +
        "instead.")
      .version("3.4.0")
      .booleanConf
      .createWithDefault(true)

  val DEFAULT_COLUMN_ALLOWED_PROVIDERS =
    buildConf("spark.sql.defaultColumn.allowedProviders")
      .internal()
      .doc("List of table providers wherein SQL commands are permitted to assign DEFAULT column " +
        "values. Comma-separated list, whitespace ignored, case-insensitive. If an asterisk " +
        "appears after any table provider in this list, any command may assign DEFAULT column " +
        "except `ALTER TABLE ... ADD COLUMN`. Otherwise, if no asterisk appears, all commands " +
        "are permitted. This is useful because in order for such `ALTER TABLE ... ADD COLUMN` " +
        "commands to work, the target data source must include support for substituting in the " +
        "provided values when the corresponding fields are not present in storage.")
      .version("3.4.0")
      .stringConf
      .createWithDefault("csv,json,orc,parquet")

  val JSON_GENERATOR_WRITE_NULL_IF_WITH_DEFAULT_VALUE =
    buildConf("spark.sql.jsonGenerator.writeNullIfWithDefaultValue")
      .internal()
      .doc("When true, when writing NULL values to columns of JSON tables with explicit DEFAULT " +
        "values using INSERT, UPDATE, or MERGE commands, never skip writing the NULL values to " +
        "storage, overriding spark.sql.jsonGenerator.ignoreNullFields or the ignoreNullFields " +
        "option. This can be useful to enforce that inserted NULL values are present in " +
        "storage to differentiate from missing data.")
      .version("3.4.0")
      .booleanConf
      .createWithDefault(true)

  val ALWAYS_INLINE_COMMON_EXPR =
    buildConf("spark.sql.alwaysInlineCommonExpr")
      .internal()
      .doc("When true, always inline common expressions instead of using the WITH expression. " +
        "This may lead to duplicated expressions and the config should only be enabled if you " +
        "hit bugs caused by the WITH expression.")
      .version("4.0.0")
      .booleanConf
      .createWithDefault(false)

  val USE_COMMON_EXPR_ID_FOR_ALIAS =
    buildConf("spark.sql.useCommonExprIdForAlias")
      .internal()
      .doc("When true, use the common expression ID for the alias when rewriting With " +
        "expressions. Otherwise, use the index of the common expression definition. When true " +
        "this avoids duplicate alias names, but is helpful to set to false for testing to ensure" +
        "that alias names are consistent.")
      .version("4.0.0")
      .booleanConf
      .createWithDefault(true)

  val USE_NULLS_FOR_MISSING_DEFAULT_COLUMN_VALUES =
    buildConf("spark.sql.defaultColumn.useNullsForMissingDefaultValues")
      .internal()
      .doc("When true, and DEFAULT columns are enabled, allow INSERT INTO commands with user-" +
        "specified lists of fewer columns than the target table to behave as if they had " +
        "specified DEFAULT for all remaining columns instead, in order.")
      .version("3.4.0")
      .booleanConf
      .createWithDefault(true)

  val SKIP_TYPE_VALIDATION_ON_ALTER_PARTITION =
    buildConf("spark.sql.legacy.skipTypeValidationOnAlterPartition")
      .internal()
      .doc("When true, skip validation for partition spec in ALTER PARTITION. E.g., " +
        "`ALTER TABLE .. ADD PARTITION(p='a')` would work even the partition type is int. " +
        s"When false, the behavior follows ${STORE_ASSIGNMENT_POLICY.key}")
      .version("3.4.0")
      .booleanConf
      .createWithDefault(false)

  val SORT_BEFORE_REPARTITION =
    buildConf("spark.sql.execution.sortBeforeRepartition")
      .internal()
      .doc("When perform a repartition following a shuffle, the output row ordering would be " +
        "nondeterministic. If some downstream stages fail and some tasks of the repartition " +
        "stage retry, these tasks may generate different data, and that can lead to correctness " +
        "issues. Turn on this config to insert a local sort before actually doing repartition " +
        "to generate consistent repartition results. The performance of repartition() may go " +
        "down since we insert extra local sort before it.")
      .version("2.1.4")
      .booleanConf
      .createWithDefault(true)

  val NESTED_SCHEMA_PRUNING_ENABLED =
    buildConf("spark.sql.optimizer.nestedSchemaPruning.enabled")
      .internal()
      .doc("Prune nested fields from a logical relation's output which are unnecessary in " +
        "satisfying a query. This optimization allows columnar file format readers to avoid " +
        "reading unnecessary nested column data. Currently Parquet and ORC are the " +
        "data sources that implement this optimization.")
      .version("2.4.1")
      .booleanConf
      .createWithDefault(true)

  val DISABLE_HINTS =
    buildConf("spark.sql.optimizer.disableHints")
      .internal()
      .doc("When true, the optimizer will disable user-specified hints that are additional " +
        "directives for better planning of a query.")
      .version("3.1.0")
      .booleanConf
      .createWithDefault(false)

  val NESTED_PREDICATE_PUSHDOWN_FILE_SOURCE_LIST =
    buildConf("spark.sql.optimizer.nestedPredicatePushdown.supportedFileSources")
      .internal()
      .doc("A comma-separated list of data source short names or fully qualified data source " +
        "implementation class names for which Spark tries to push down predicates for nested " +
        "columns and/or names containing `dots` to data sources. This configuration is only " +
        "effective with file-based data sources in DSv1. Currently, Parquet and ORC implement " +
        "both optimizations. The other data sources don't support this feature yet. So the " +
        "default value is 'parquet,orc'.")
      .version("3.0.0")
      .stringConf
      .createWithDefault("parquet,orc")

  val SERIALIZER_NESTED_SCHEMA_PRUNING_ENABLED =
    buildConf("spark.sql.optimizer.serializer.nestedSchemaPruning.enabled")
      .internal()
      .doc("Prune nested fields from object serialization operator which are unnecessary in " +
        "satisfying a query. This optimization allows object serializers to avoid " +
        "executing unnecessary nested expressions.")
      .version("3.0.0")
      .booleanConf
      .createWithDefault(true)

  val NESTED_PRUNING_ON_EXPRESSIONS =
    buildConf("spark.sql.optimizer.expression.nestedPruning.enabled")
      .internal()
      .doc("Prune nested fields from expressions in an operator which are unnecessary in " +
        "satisfying a query. Note that this optimization doesn't prune nested fields from " +
        "physical data source scanning. For pruning nested fields from scanning, please use " +
        "`spark.sql.optimizer.nestedSchemaPruning.enabled` config.")
      .version("3.0.0")
      .booleanConf
      .createWithDefault(true)

  val DECORRELATE_INNER_QUERY_ENABLED =
    buildConf("spark.sql.optimizer.decorrelateInnerQuery.enabled")
      .internal()
      .doc("Decorrelate inner query by eliminating correlated references and build domain joins.")
      .version("3.2.0")
      .booleanConf
      .createWithDefault(true)

  val DECORRELATE_SET_OPS_ENABLED =
    buildConf("spark.sql.optimizer.decorrelateSetOps.enabled")
      .internal()
      .doc("Decorrelate subqueries with correlation under set operators.")
      .version("3.4.0")
      .booleanConf
      .createWithDefault(true)

  val DECORRELATE_LIMIT_ENABLED =
    buildConf("spark.sql.optimizer.decorrelateLimit.enabled")
      .internal()
      .doc("Decorrelate subqueries with correlation under LIMIT.")
      .version("4.0.0")
      .booleanConf
      .createWithDefault(true)

  val DECORRELATE_OFFSET_ENABLED =
    buildConf("spark.sql.optimizer.decorrelateOffset.enabled")
      .internal()
      .doc("Decorrelate subqueries with correlation under LIMIT with OFFSET.")
      .version("4.0.0")
      .booleanConf
      .createWithDefault(true)

  val DECORRELATE_EXISTS_IN_SUBQUERY_LEGACY_INCORRECT_COUNT_HANDLING_ENABLED =
    buildConf("spark.sql.optimizer.decorrelateExistsSubqueryLegacyIncorrectCountHandling.enabled")
      .internal()
      .doc("If enabled, revert to legacy incorrect behavior for certain EXISTS/IN subqueries " +
           "with COUNT or similar aggregates.")
      .version("4.0.0")
      .booleanConf
      .createWithDefault(false)

  val DECORRELATE_SUBQUERY_LEGACY_INCORRECT_COUNT_HANDLING_ENABLED =
    buildConf("spark.sql.optimizer.decorrelateSubqueryLegacyIncorrectCountHandling.enabled")
      .internal()
      .doc("If enabled, revert to legacy incorrect behavior for certain subqueries with COUNT or " +
        "similar aggregates: see SPARK-43098.")
      .version("3.5.0")
      .booleanConf
      .createWithDefault(false)

  val DECORRELATE_SUBQUERY_PREVENT_CONSTANT_FOLDING_FOR_COUNT_BUG =
    buildConf("spark.sql.optimizer.decorrelateSubqueryPreventConstantHoldingForCountBug.enabled")
      .internal()
      .doc("If enabled, prevents constant folding in subqueries that contain" +
        " a COUNT-bug-susceptible Aggregate.")
      .version("4.0.0")
      .booleanConf
      .createWithDefault(true)

  val PULL_OUT_NESTED_DATA_OUTER_REF_EXPRESSIONS_ENABLED =
    buildConf("spark.sql.optimizer.pullOutNestedDataOuterRefExpressions.enabled")
      .internal()
      .doc("Handle correlation over nested data extract expressions by pulling out the " +
        "expression into the outer plan. This enables correlation on map attributes for example.")
      .version("4.0.0")
      .booleanConf
      .createWithDefault(true)

  val OPTIMIZE_ONE_ROW_RELATION_SUBQUERY =
    buildConf("spark.sql.optimizer.optimizeOneRowRelationSubquery")
      .internal()
      .doc("When true, the optimizer will inline subqueries with OneRowRelation as leaf nodes.")
      .version("3.2.0")
      .booleanConf
      .createWithDefault(true)

  val WRAP_EXISTS_IN_AGGREGATE_FUNCTION =
    buildConf("spark.sql.optimizer.wrapExistsInAggregateFunction")
      .internal()
      .doc("When true, the optimizer will wrap newly introduced `exists` attributes in an " +
      "aggregate function to ensure that Aggregate nodes preserve semantic invariant that each " +
      "variable among agg expressions appears either in grouping expressions or belongs to " +
      "and aggregate function.")
      .version("4.0.0")
      .booleanConf
      .createWithDefault(true)

  val ALWAYS_INLINE_ONE_ROW_RELATION_SUBQUERY =
    buildConf("spark.sql.optimizer.optimizeOneRowRelationSubquery.alwaysInline")
      .internal()
      .doc(s"When true, the optimizer will always inline single row subqueries even if it " +
        "causes extra duplication. It only takes effect when " +
        s"${OPTIMIZE_ONE_ROW_RELATION_SUBQUERY.key} is set to true.")
      .version("3.4.0")
      .booleanConf
      .createWithDefault(true)

  val PULL_HINTS_INTO_SUBQUERIES =
    buildConf("spark.sql.optimizer.pullHintsIntoSubqueries")
      .internal()
      .doc("Pull hints into subqueries in EliminateResolvedHint if enabled.")
      .booleanConf
      .createWithDefault(true)

  val TOP_K_SORT_FALLBACK_THRESHOLD =
    buildConf("spark.sql.execution.topKSortFallbackThreshold")
      .doc("In SQL queries with a SORT followed by a LIMIT like " +
          "'SELECT x FROM t ORDER BY y LIMIT m', if m is under this threshold, do a top-K sort" +
          " in memory, otherwise do a global sort which spills to disk if necessary.")
      .version("2.4.0")
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
    .version("2.4.0")
    .booleanConf
    .createWithDefault(true)

  val CSV_INPUT_BUFFER_SIZE = buildConf("spark.sql.csv.parser.inputBufferSize")
    .internal()
    .doc("If it is set, it configures the buffer size of CSV input during parsing. " +
      "It is the same as inputBufferSize option in CSV which has a higher priority. " +
      "Note that this is a workaround for the parsing library's regression, and this " +
      "configuration is internal and supposed to be removed in the near future.")
    .version("3.0.3")
    .intConf
    .createOptional

  val LEGACY_RESPECT_NULLABILITY_IN_TEXT_DATASET_CONVERSION =
    buildConf("spark.sql.legacy.respectNullabilityInTextDatasetConversion")
      .internal()
      .doc("When true, the nullability in the user-specified schema for " +
        "`DataFrameReader.schema(schema).json(jsonDataset)` and " +
        "`DataFrameReader.schema(schema).csv(csvDataset)` is respected. Otherwise, they are " +
        "turned to a nullable schema forcibly.")
      .version("3.3.0")
      .booleanConf
      .createWithDefault(false)

  val REPL_EAGER_EVAL_ENABLED = buildConf("spark.sql.repl.eagerEval.enabled")
    .doc("Enables eager evaluation or not. When true, the top K rows of Dataset will be " +
      "displayed if and only if the REPL supports the eager evaluation. Currently, the " +
      "eager evaluation is supported in PySpark and SparkR. In PySpark, for the notebooks like " +
      "Jupyter, the HTML table (generated by _repr_html_) will be returned. For plain Python " +
      "REPL, the returned outputs are formatted like dataframe.show(). In SparkR, the returned " +
      "outputs are showed similar to R data.frame would.")
    .version("2.4.0")
    .booleanConf
    .createWithDefault(false)

  val REPL_EAGER_EVAL_MAX_NUM_ROWS = buildConf("spark.sql.repl.eagerEval.maxNumRows")
    .doc("The max number of rows that are returned by eager evaluation. This only takes " +
      s"effect when ${REPL_EAGER_EVAL_ENABLED.key} is set to true. The valid range of this " +
      "config is from 0 to (Int.MaxValue - 1), so the invalid config like negative and " +
      "greater than (Int.MaxValue - 1) will be normalized to 0 and (Int.MaxValue - 1).")
    .version("2.4.0")
    .intConf
    .createWithDefault(20)

  val REPL_EAGER_EVAL_TRUNCATE = buildConf("spark.sql.repl.eagerEval.truncate")
    .doc("The max number of characters for each cell that is returned by eager evaluation. " +
      s"This only takes effect when ${REPL_EAGER_EVAL_ENABLED.key} is set to true.")
    .version("2.4.0")
    .intConf
    .createWithDefault(20)

  val FAST_HASH_AGGREGATE_MAX_ROWS_CAPACITY_BIT =
    buildConf("spark.sql.codegen.aggregate.fastHashMap.capacityBit")
      .internal()
      .doc("Capacity for the max number of rows to be held in memory " +
        "by the fast hash aggregate product operator. The bit is not for actual value, " +
        "but the actual numBuckets is determined by loadFactor " +
        "(e.g: default bit value 16 , the actual numBuckets is ((1 << 16) / 0.5).")
      .version("2.4.0")
      .intConf
      .checkValue(bit => bit >= 10 && bit <= 30, "The bit value must be in [10, 30].")
      .createWithDefault(16)

  val AVRO_COMPRESSION_CODEC = buildConf("spark.sql.avro.compression.codec")
    .doc("Compression codec used in writing of AVRO files. Supported codecs: " +
      "uncompressed, deflate, snappy, bzip2, xz and zstandard. Default codec is snappy.")
    .version("2.4.0")
    .stringConf
    .checkValues(Set("uncompressed", "deflate", "snappy", "bzip2", "xz", "zstandard"))
    .createWithDefault("snappy")

  val AVRO_DEFLATE_LEVEL = buildConf("spark.sql.avro.deflate.level")
    .doc("Compression level for the deflate codec used in writing of AVRO files. " +
      "Valid value must be in the range of from 1 to 9 inclusive or -1. " +
      "The default value is -1 which corresponds to 6 level in the current implementation.")
    .version("2.4.0")
    .intConf
    .checkValues((1 to 9).toSet + Deflater.DEFAULT_COMPRESSION)
    .createOptional

  val AVRO_XZ_LEVEL = buildConf("spark.sql.avro.xz.level")
    .doc("Compression level for the xz codec used in writing of AVRO files. " +
      "Valid value must be in the range of from 1 to 9 inclusive " +
      "The default value is 6.")
    .version("4.0.0")
    .intConf
    .checkValue(v => v > 0 && v <= 9, "The value must be in the range of from 1 to 9 inclusive.")
    .createOptional

  val AVRO_ZSTANDARD_LEVEL = buildConf("spark.sql.avro.zstandard.level")
    .doc("Compression level for the zstandard codec used in writing of AVRO files. " +
      "The default value is 3.")
    .version("4.0.0")
    .intConf
    .createOptional

  val AVRO_ZSTANDARD_BUFFER_POOL_ENABLED = buildConf("spark.sql.avro.zstandard.bufferPool.enabled")
    .doc("If true, enable buffer pool of ZSTD JNI library when writing of AVRO files")
    .version("4.0.0")
    .booleanConf
    .createWithDefault(false)

  val LEGACY_SIZE_OF_NULL = buildConf("spark.sql.legacy.sizeOfNull")
    .internal()
    .doc(s"If it is set to false, or ${ANSI_ENABLED.key} is true, then size of null returns " +
      "null. Otherwise, it returns -1, which was inherited from Hive.")
    .version("2.4.0")
    .booleanConf
    .createWithDefault(true)

  val LEGACY_PARSE_NULL_PARTITION_SPEC_AS_STRING_LITERAL =
    buildConf("spark.sql.legacy.parseNullPartitionSpecAsStringLiteral")
      .internal()
      .doc("If it is set to true, `PARTITION(col=null)` is parsed as a string literal of its " +
        "text representation, e.g., string 'null', when the partition column is string type. " +
        "Otherwise, it is always parsed as a null literal in the partition spec.")
      .version("3.0.2")
      .booleanConf
      .createWithDefault(false)

  val LEGACY_KEEP_PARTITION_SPEC_AS_STRING_LITERAL =
    buildConf("spark.sql.legacy.keepPartitionSpecAsStringLiteral")
      .internal()
      .doc("If it is set to true, `PARTITION(col=05)` is parsed as a string literal of its " +
        "text representation, e.g., string '05', when the partition column is string type. " +
        "Otherwise, it is always parsed as a numeric literal in the partition spec.")
      .version("3.4.0")
      .booleanConf
      .createWithDefault(false)

  val LEGACY_REPLACE_DATABRICKS_SPARK_AVRO_ENABLED =
    buildConf("spark.sql.legacy.replaceDatabricksSparkAvro.enabled")
      .internal()
      .doc("If it is set to true, the data source provider com.databricks.spark.avro is mapped " +
        "to the built-in but external Avro data source module for backward compatibility.")
      .version("2.4.0")
      .booleanConf
      .createWithDefault(true)

  val LEGACY_SETOPS_PRECEDENCE_ENABLED =
    buildConf("spark.sql.legacy.setopsPrecedence.enabled")
      .internal()
      .doc("When set to true and the order of evaluation is not specified by parentheses, the " +
        "set operations are performed from left to right as they appear in the query. When set " +
        "to false and order of evaluation is not specified by parentheses, INTERSECT operations " +
        "are performed before any UNION, EXCEPT and MINUS operations.")
      .version("2.4.0")
      .booleanConf
      .createWithDefault(false)

  val LEGACY_EXPONENT_LITERAL_AS_DECIMAL_ENABLED =
    buildConf("spark.sql.legacy.exponentLiteralAsDecimal.enabled")
      .internal()
      .doc("When set to true, a literal with an exponent (e.g. 1E-30) would be parsed " +
        "as Decimal rather than Double.")
      .version("3.0.0")
      .booleanConf
      .createWithDefault(false)

  val LEGACY_ALLOW_NEGATIVE_SCALE_OF_DECIMAL_ENABLED =
    buildConf("spark.sql.legacy.allowNegativeScaleOfDecimal")
      .internal()
      .doc("When set to true, negative scale of Decimal type is allowed. For example, " +
        "the type of number 1E10BD under legacy mode is DecimalType(2, -9), but is " +
        "Decimal(11, 0) in non legacy mode.")
      .version("3.0.0")
      .booleanConf
      .createWithDefault(false)

  val LEGACY_BUCKETED_TABLE_SCAN_OUTPUT_ORDERING =
    buildConf("spark.sql.legacy.bucketedTableScan.outputOrdering")
      .internal()
      .doc("When true, the bucketed table scan will list files during planning to figure out the " +
        "output ordering, which is expensive and may make the planning quite slow.")
      .version("3.0.0")
      .booleanConf
      .createWithDefault(false)

  val LEGACY_HAVING_WITHOUT_GROUP_BY_AS_WHERE =
    buildConf("spark.sql.legacy.parser.havingWithoutGroupByAsWhere")
      .internal()
      .doc("If it is set to true, the parser will treat HAVING without GROUP BY as a normal " +
        "WHERE, which does not follow SQL standard.")
      .version("2.4.1")
      .booleanConf
      .createWithDefault(false)

  val LEGACY_ALLOW_EMPTY_STRING_IN_JSON =
    buildConf("spark.sql.legacy.json.allowEmptyString.enabled")
      .internal()
      .doc("When set to true, the parser of JSON data source treats empty strings as null for " +
        "some data types such as `IntegerType`.")
      .version("3.0.0")
      .booleanConf
      .createWithDefault(false)

  val LEGACY_CREATE_EMPTY_COLLECTION_USING_STRING_TYPE =
    buildConf("spark.sql.legacy.createEmptyCollectionUsingStringType")
      .internal()
      .doc("When set to true, Spark returns an empty collection with `StringType` as element " +
        "type if the `array`/`map` function is called without any parameters. Otherwise, Spark " +
        "returns an empty collection with `NullType` as element type.")
      .version("3.0.0")
      .booleanConf
      .createWithDefault(false)

  val LEGACY_ALLOW_UNTYPED_SCALA_UDF =
    buildConf("spark.sql.legacy.allowUntypedScalaUDF")
      .internal()
      .doc("When set to true, user is allowed to use org.apache.spark.sql.functions." +
        "udf(f: AnyRef, dataType: DataType). Otherwise, an exception will be thrown at runtime.")
      .version("3.0.0")
      .booleanConf
      .createWithDefault(false)

  val LEGACY_STATISTICAL_AGGREGATE =
    buildConf("spark.sql.legacy.statisticalAggregate")
      .internal()
      .doc("When set to true, statistical aggregate function returns Double.NaN " +
        "if divide by zero occurred during expression evaluation, otherwise, it returns null. " +
        "Before version 3.1.0, it returns NaN in divideByZero case by default.")
      .version("3.1.0")
      .booleanConf
      .createWithDefault(false)

  val TRUNCATE_TABLE_IGNORE_PERMISSION_ACL =
    buildConf("spark.sql.truncateTable.ignorePermissionAcl.enabled")
      .internal()
      .doc("When set to true, TRUNCATE TABLE command will not try to set back original " +
        "permission and ACLs when re-creating the table/partition paths.")
      .version("2.4.6")
      .booleanConf
      .createWithDefault(false)

  val NAME_NON_STRUCT_GROUPING_KEY_AS_VALUE =
    buildConf("spark.sql.legacy.dataset.nameNonStructGroupingKeyAsValue")
      .internal()
      .doc("When set to true, the key attribute resulted from running `Dataset.groupByKey` " +
        "for non-struct key type, will be named as `value`, following the behavior of Spark " +
        "version 2.4 and earlier.")
      .version("3.0.0")
      .booleanConf
      .createWithDefault(false)

  val MAX_TO_STRING_FIELDS = buildConf("spark.sql.debug.maxToStringFields")
    .doc("Maximum number of fields of sequence-like entries can be converted to strings " +
      "in debug output. Any elements beyond the limit will be dropped and replaced by a" +
      """ "... N more fields" placeholder.""")
    .version("3.0.0")
    .intConf
    .createWithDefault(25)

  val MAX_PLAN_STRING_LENGTH = buildConf("spark.sql.maxPlanStringLength")
    .doc("Maximum number of characters to output for a plan string.  If the plan is " +
      "longer, further output will be truncated.  The default setting always generates a full " +
      "plan.  Set this to a lower value such as 8k if plan strings are taking up too much " +
      "memory or are causing OutOfMemory errors in the driver or UI processes.")
    .version("3.0.0")
    .bytesConf(ByteUnit.BYTE)
    .checkValue(i => i >= 0 && i <= ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH, "Invalid " +
      "value for 'spark.sql.maxPlanStringLength'.  Length must be a valid string length " +
      "(nonnegative and shorter than the maximum size).")
    .createWithDefaultString(s"${ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH}")

  val MAX_METADATA_STRING_LENGTH = buildConf("spark.sql.maxMetadataStringLength")
    .doc("Maximum number of characters to output for a metadata string. e.g. " +
      "file location in `DataSourceScanExec`, every value will be abbreviated if exceed length.")
    .version("3.1.0")
    .intConf
    .checkValue(_ > 3, "This value must be bigger than 3.")
    .createWithDefault(100)

  val SET_COMMAND_REJECTS_SPARK_CORE_CONFS =
    buildConf("spark.sql.legacy.setCommandRejectsSparkCoreConfs")
      .internal()
      .doc("If it is set to true, SET command will fail when the key is registered as " +
        "a SparkConf entry.")
      .version("3.0.0")
      .booleanConf
      .createWithDefault(true)

  object TimestampTypes extends Enumeration {
    val TIMESTAMP_NTZ, TIMESTAMP_LTZ = Value
  }

  val TIMESTAMP_TYPE =
    buildConf("spark.sql.timestampType")
      .doc("Configures the default timestamp type of Spark SQL, including SQL DDL, Cast clause, " +
        "type literal and the schema inference of data sources. " +
        s"Setting the configuration as ${TimestampTypes.TIMESTAMP_NTZ} will " +
        "use TIMESTAMP WITHOUT TIME ZONE as the default type while putting it as " +
        s"${TimestampTypes.TIMESTAMP_LTZ} will use TIMESTAMP WITH LOCAL TIME ZONE. " +
        "Before the 3.4.0 release, Spark only supports the TIMESTAMP WITH " +
        "LOCAL TIME ZONE type.")
      .version("3.4.0")
      .stringConf
      .transform(_.toUpperCase(Locale.ROOT))
      .checkValues(TimestampTypes.values.map(_.toString))
      .createWithDefault(TimestampTypes.TIMESTAMP_LTZ.toString)

  val DATETIME_JAVA8API_ENABLED = buildConf("spark.sql.datetime.java8API.enabled")
    .doc("If the configuration property is set to true, java.time.Instant and " +
      "java.time.LocalDate classes of Java 8 API are used as external types for " +
      "Catalyst's TimestampType and DateType. If it is set to false, java.sql.Timestamp " +
      "and java.sql.Date are used for the same purpose.")
    .version("3.0.0")
    .booleanConf
    .createWithDefault(false)

  val UI_EXPLAIN_MODE = buildConf("spark.sql.ui.explainMode")
    .doc("Configures the query explain mode used in the Spark SQL UI. The value can be 'simple', " +
      "'extended', 'codegen', 'cost', or 'formatted'. The default value is 'formatted'.")
    .version("3.1.0")
    .stringConf
    .transform(_.toUpperCase(Locale.ROOT))
    .checkValue(mode => Set("SIMPLE", "EXTENDED", "CODEGEN", "COST", "FORMATTED").contains(mode),
      "Invalid value for 'spark.sql.ui.explainMode'. Valid values are 'simple', 'extended', " +
      "'codegen', 'cost' and 'formatted'.")
    .createWithDefault("formatted")

  val SOURCES_BINARY_FILE_MAX_LENGTH = buildConf("spark.sql.sources.binaryFile.maxLength")
    .doc("The max length of a file that can be read by the binary file data source. " +
      "Spark will fail fast and not attempt to read the file if its length exceeds this value. " +
      "The theoretical max is Int.MaxValue, though VMs might implement a smaller max.")
    .version("3.0.0")
    .internal()
    .intConf
    .createWithDefault(Int.MaxValue)

  val LEGACY_CAST_DATETIME_TO_STRING =
    buildConf("spark.sql.legacy.typeCoercion.datetimeToString.enabled")
      .internal()
      .doc("If it is set to true, date/timestamp will cast to string in binary comparisons " +
        s"with String when ${ANSI_ENABLED.key} is false.")
      .version("3.0.0")
      .booleanConf
      .createWithDefault(false)

  val DEFAULT_CATALOG = buildConf("spark.sql.defaultCatalog")
    .doc("Name of the default catalog. This will be the current catalog if users have not " +
      "explicitly set the current catalog yet.")
    .version("3.0.0")
    .stringConf
    .createWithDefault(SESSION_CATALOG_NAME)

  val V2_SESSION_CATALOG_IMPLEMENTATION =
    buildConf(s"spark.sql.catalog.$SESSION_CATALOG_NAME")
      .doc("A catalog implementation that will be used as the v2 interface to Spark's built-in " +
        s"v1 catalog: $SESSION_CATALOG_NAME. This catalog shares its identifier namespace with " +
        s"the $SESSION_CATALOG_NAME and must be consistent with it; for example, if a table can " +
        s"be loaded by the $SESSION_CATALOG_NAME, this catalog must also return the table " +
        s"metadata. To delegate operations to the $SESSION_CATALOG_NAME, implementations can " +
        "extend 'CatalogExtension'.")
      .version("3.0.0")
      .stringConf
      .createOptional

  object MapKeyDedupPolicy extends Enumeration {
    val EXCEPTION, LAST_WIN = Value
  }

  val MAP_KEY_DEDUP_POLICY = buildConf("spark.sql.mapKeyDedupPolicy")
    .doc("The policy to deduplicate map keys in builtin function: CreateMap, MapFromArrays, " +
      "MapFromEntries, StringToMap, MapConcat and TransformKeys. When EXCEPTION, the query " +
      "fails if duplicated map keys are detected. When LAST_WIN, the map key that is inserted " +
      "at last takes precedence.")
    .version("3.0.0")
    .stringConf
    .transform(_.toUpperCase(Locale.ROOT))
    .checkValues(MapKeyDedupPolicy.values.map(_.toString))
    .createWithDefault(MapKeyDedupPolicy.EXCEPTION.toString)

  val LEGACY_LOOSE_UPCAST = buildConf("spark.sql.legacy.doLooseUpcast")
    .internal()
    .doc("When true, the upcast will be loose and allows string to atomic types.")
    .version("3.0.0")
    .booleanConf
    .createWithDefault(false)

  val LEGACY_CTE_PRECEDENCE_POLICY = buildConf("spark.sql.legacy.ctePrecedencePolicy")
    .internal()
    .doc("When LEGACY, outer CTE definitions takes precedence over inner definitions. If set to " +
      "EXCEPTION, AnalysisException is thrown while name conflict is detected in nested CTE." +
      "The default is CORRECTED, inner CTE definitions take precedence. This config " +
      "will be removed in future versions and CORRECTED will be the only behavior.")
    .version("3.0.0")
    .stringConf
    .transform(_.toUpperCase(Locale.ROOT))
    .checkValues(LegacyBehaviorPolicy.values.map(_.toString))
    .createWithDefault(LegacyBehaviorPolicy.CORRECTED.toString)

  val LEGACY_INLINE_CTE_IN_COMMANDS = buildConf("spark.sql.legacy.inlineCTEInCommands")
    .internal()
    .doc("If true, always inline the CTE relations for the queries in commands. This is the " +
      "legacy behavior which may produce incorrect results because Spark may evaluate a CTE " +
      "relation more than once, even if it's nondeterministic.")
    .version("4.0.0")
    .booleanConf
    .createWithDefault(false)

  val LEGACY_TIME_PARSER_POLICY = buildConf(SqlApiConfHelper.LEGACY_TIME_PARSER_POLICY_KEY)
    .internal()
    .doc("When LEGACY, java.text.SimpleDateFormat is used for formatting and parsing " +
      "dates/timestamps in a locale-sensitive manner, which is the approach before Spark 3.0. " +
      "When set to CORRECTED, classes from java.time.* packages are used for the same purpose. " +
      "When set to EXCEPTION, RuntimeException is thrown when we will get different " +
      "results. The default is CORRECTED.")
    .version("3.0.0")
    .stringConf
    .transform(_.toUpperCase(Locale.ROOT))
    .checkValues(LegacyBehaviorPolicy.values.map(_.toString))
    .createWithDefault(LegacyBehaviorPolicy.CORRECTED.toString)

  val LEGACY_ARRAY_EXISTS_FOLLOWS_THREE_VALUED_LOGIC =
    buildConf("spark.sql.legacy.followThreeValuedLogicInArrayExists")
      .internal()
      .doc("When true, the ArrayExists will follow the three-valued boolean logic.")
      .version("3.0.0")
      .booleanConf
      .createWithDefault(true)

  val ADDITIONAL_REMOTE_REPOSITORIES =
    buildConf("spark.sql.maven.additionalRemoteRepositories")
      .doc("A comma-delimited string config of the optional additional remote Maven mirror " +
        "repositories. This is only used for downloading Hive jars in IsolatedClientLoader " +
        "if the default Maven Central repo is unreachable.")
      .version("3.0.0")
      .stringConf
      .createWithDefault(
        sys.env.getOrElse("DEFAULT_ARTIFACT_REPOSITORY",
          "https://maven-central.storage-download.googleapis.com/maven2/"))

  val LEGACY_FROM_DAYTIME_STRING =
    buildConf("spark.sql.legacy.fromDayTimeString.enabled")
      .internal()
      .doc("When true, the `from` bound is not taken into account in conversion of " +
        "a day-time string to an interval, and the `to` bound is used to skip " +
        "all interval units out of the specified range. If it is set to `false`, " +
        "`ParseException` is thrown if the input does not match to the pattern " +
        "defined by `from` and `to`.")
      .version("3.0.0")
      .booleanConf
      .createWithDefault(false)

  val LEGACY_PROPERTY_NON_RESERVED =
    buildConf("spark.sql.legacy.notReserveProperties")
      .internal()
      .doc("When true, all database and table properties are not reserved and available for " +
        "create/alter syntaxes. But please be aware that the reserved properties will be " +
        "silently removed.")
      .version("3.0.0")
      .booleanConf
      .createWithDefault(false)

  val LEGACY_ADD_SINGLE_FILE_IN_ADD_FILE =
    buildConf("spark.sql.legacy.addSingleFileInAddFile")
      .internal()
      .doc("When true, only a single file can be added using ADD FILE. If false, then users " +
        "can add directory by passing directory path to ADD FILE.")
      .version("3.0.0")
      .booleanConf
      .createWithDefault(false)

  val LEGACY_MSSQLSERVER_NUMERIC_MAPPING_ENABLED =
    buildConf("spark.sql.legacy.mssqlserver.numericMapping.enabled")
      .internal()
      .doc("When true, use legacy MsSqlServer TINYINT, SMALLINT and REAL type mapping.")
      .version("2.4.5")
      .booleanConf
      .createWithDefault(false)

  val LEGACY_MSSQLSERVER_DATETIMEOFFSET_MAPPING_ENABLED =
    buildConf("spark.sql.legacy.mssqlserver.datetimeoffsetMapping.enabled")
      .internal()
      .doc("When true, DATETIMEOFFSET is mapped to StringType; otherwise, it is mapped to " +
        "TimestampType.")
      .version("4.0.0")
      .booleanConf
      .createWithDefault(false)

  val LEGACY_MYSQL_BIT_ARRAY_MAPPING_ENABLED =
    buildConf("spark.sql.legacy.mysql.bitArrayMapping.enabled")
      .internal()
      .doc("When true, use LongType to represent MySQL BIT(n>1); otherwise, use BinaryType.")
      .version("4.0.0")
      .booleanConf
      .createWithDefault(false)

  val LEGACY_MYSQL_TIMESTAMPNTZ_MAPPING_ENABLED =
    buildConf("spark.sql.legacy.mysql.timestampNTZMapping.enabled")
      .internal()
      .doc("When true, TimestampNTZType and MySQL TIMESTAMP can be converted bidirectionally. " +
        "For reading, MySQL TIMESTAMP is converted to TimestampNTZType when JDBC read option " +
        "preferTimestampNTZ is true. For writing, TimestampNTZType is converted to MySQL " +
        "TIMESTAMP; otherwise, DATETIME")
      .version("4.0.0")
      .booleanConf
      .createWithDefault(false)

  val LEGACY_ORACLE_TIMESTAMP_MAPPING_ENABLED =
    buildConf("spark.sql.legacy.oracle.timestampMapping.enabled")
      .internal()
      .doc("When true, TimestampType maps to TIMESTAMP in Oracle; otherwise, " +
        "TIMESTAMP WITH LOCAL TIME ZONE.")
      .version("4.0.0")
      .booleanConf
      .createWithDefault(false)

  val LEGACY_DB2_TIMESTAMP_MAPPING_ENABLED =
    buildConf("spark.sql.legacy.db2.numericMapping.enabled")
      .internal()
      .doc("When true, SMALLINT maps to IntegerType in DB2; otherwise, ShortType" )
      .version("4.0.0")
      .booleanConf
      .createWithDefault(false)

  val LEGACY_DB2_BOOLEAN_MAPPING_ENABLED =
    buildConf("spark.sql.legacy.db2.booleanMapping.enabled")
      .internal()
      .doc("When true, BooleanType maps to CHAR(1) in DB2; otherwise, BOOLEAN" )
      .version("4.0.0")
      .booleanConf
      .createWithDefault(false)

  val LEGACY_POSTGRES_DATETIME_MAPPING_ENABLED =
    buildConf("spark.sql.legacy.postgres.datetimeMapping.enabled")
      .internal()
      .doc("When true, TimestampType maps to TIMESTAMP WITHOUT TIME ZONE in PostgreSQL for " +
        "writing; otherwise, TIMESTAMP WITH TIME ZONE. When true, TIMESTAMP WITH TIME ZONE " +
        "can be converted to TimestampNTZType when JDBC read option preferTimestampNTZ is " +
        "true; otherwise, converted to TimestampType regardless of preferTimestampNTZ.")
      .version("4.0.0")
      .booleanConf
      .createWithDefault(false)

  val CSV_FILTER_PUSHDOWN_ENABLED = buildConf("spark.sql.csv.filterPushdown.enabled")
    .doc("When true, enable filter pushdown to CSV datasource.")
    .version("3.0.0")
    .booleanConf
    .createWithDefault(true)

  val JSON_FILTER_PUSHDOWN_ENABLED = buildConf("spark.sql.json.filterPushdown.enabled")
    .doc("When true, enable filter pushdown to JSON datasource.")
    .version("3.1.0")
    .booleanConf
    .createWithDefault(true)

  val AVRO_FILTER_PUSHDOWN_ENABLED = buildConf("spark.sql.avro.filterPushdown.enabled")
    .doc("When true, enable filter pushdown to Avro datasource.")
    .version("3.1.0")
    .booleanConf
    .createWithDefault(true)

  val JSON_ENABLE_PARTIAL_RESULTS =
    buildConf("spark.sql.json.enablePartialResults")
      .internal()
      .doc("When set to true, enables partial results for structs, maps, and arrays in JSON " +
        "when one or more fields do not match the schema")
      .version("3.4.0")
      .booleanConf
      .createWithDefault(true)

  val JSON_EXACT_STRING_PARSING =
    buildConf("spark.sql.json.enableExactStringParsing")
      .internal()
      .doc("When set to true, string columns extracted from JSON objects will be extracted " +
        "exactly as they appear in the input string, with no changes")
      .version("4.0.0")
      .booleanConf
      .createWithDefault(true)

  val JSON_USE_UNSAFE_ROW =
    buildConf("spark.sql.json.useUnsafeRow")
      .doc("When set to true, use UnsafeRow to represent struct result in the JSON parser. It " +
        "can be overwritten by the JSON option `useUnsafeRow`.")
      .version("4.0.0")
      .booleanConf
      .createWithDefault(false)

  val VARIANT_ALLOW_DUPLICATE_KEYS =
    buildConf("spark.sql.variant.allowDuplicateKeys")
      .internal()
      .doc("When set to false, parsing variant from JSON will throw an error if there are " +
        "duplicate keys in the input JSON object. When set to true, the parser will keep the " +
        "last occurrence of all fields with the same key.")
      .version("4.0.0")
      .booleanConf
      .createWithDefault(false)

  val LEGACY_CSV_ENABLE_DATE_TIME_PARSING_FALLBACK =
    buildConf("spark.sql.legacy.csv.enableDateTimeParsingFallback")
      .internal()
      .doc("When true, enable legacy date/time parsing fallback in CSV")
      .version("3.4.0")
      .booleanConf
      .createOptional

  val LEGACY_JSON_ENABLE_DATE_TIME_PARSING_FALLBACK =
    buildConf("spark.sql.legacy.json.enableDateTimeParsingFallback")
      .internal()
      .doc("When true, enable legacy date/time parsing fallback in JSON")
      .version("3.4.0")
      .booleanConf
      .createOptional

  val ADD_PARTITION_BATCH_SIZE =
    buildConf("spark.sql.addPartitionInBatch.size")
      .internal()
      .doc("The number of partitions to be handled in one turn when use " +
        "`AlterTableAddPartitionCommand` or `RepairTableCommand` to add partitions into table. " +
        "The smaller batch size is, the less memory is required for the real handler, e.g. " +
        "Hive Metastore.")
      .version("3.0.0")
      .intConf
      .checkValue(_ > 0, "The value of spark.sql.addPartitionInBatch.size must be positive")
      .createWithDefault(100)

  val LEGACY_ALLOW_HASH_ON_MAPTYPE = buildConf("spark.sql.legacy.allowHashOnMapType")
    .internal()
    .doc("When set to true, hash expressions can be applied on elements of MapType. Otherwise, " +
      "an analysis exception will be thrown.")
    .version("3.0.0")
    .booleanConf
    .createWithDefault(false)

  val LEGACY_INTEGER_GROUPING_ID =
    buildConf("spark.sql.legacy.integerGroupingId")
      .internal()
      .doc("When true, grouping_id() returns int values instead of long values.")
      .version("3.1.0")
      .booleanConf
      .createWithDefault(false)

  val LEGACY_GROUPING_ID_WITH_APPENDED_USER_GROUPBY =
    buildConf("spark.sql.legacy.groupingIdWithAppendedUserGroupBy")
      .internal()
      .doc("When true, grouping_id() returns values based on grouping set columns plus " +
        "user-given group-by expressions order like Spark 3.2.0, 3.2.1, 3.2.2, and 3.3.0.")
      .version("3.2.3")
      .booleanConf
      .createWithDefault(false)

  val LEGACY_PARQUET_NANOS_AS_LONG = buildConf("spark.sql.legacy.parquet.nanosAsLong")
    .internal()
    .doc("When true, the Parquet's nanos precision timestamps are converted to SQL long values.")
    .version("3.2.4")
    .booleanConf
    .createWithDefault(false)

  val PARQUET_INT96_REBASE_MODE_IN_WRITE =
    buildConf("spark.sql.parquet.int96RebaseModeInWrite")
      .internal()
      .doc("When LEGACY, Spark will rebase INT96 timestamps from Proleptic Gregorian calendar to " +
        "the legacy hybrid (Julian + Gregorian) calendar when writing Parquet files. " +
        "When CORRECTED, Spark will not do rebase and write the timestamps as it is. " +
        "When EXCEPTION, which is the default, Spark will fail the writing if it sees ancient " +
        "timestamps that are ambiguous between the two calendars.")
      .version("3.1.0")
      .stringConf
      .transform(_.toUpperCase(Locale.ROOT))
      .checkValues(LegacyBehaviorPolicy.values.map(_.toString))
      .createWithDefault(LegacyBehaviorPolicy.CORRECTED.toString)

  val PARQUET_REBASE_MODE_IN_WRITE =
    buildConf("spark.sql.parquet.datetimeRebaseModeInWrite")
      .internal()
      .doc("When LEGACY, Spark will rebase dates/timestamps from Proleptic Gregorian calendar " +
        "to the legacy hybrid (Julian + Gregorian) calendar when writing Parquet files. " +
        "When CORRECTED, Spark will not do rebase and write the dates/timestamps as it is. " +
        "When EXCEPTION, which is the default, Spark will fail the writing if it sees " +
        "ancient dates/timestamps that are ambiguous between the two calendars. " +
        "This config influences on writes of the following parquet logical types: DATE, " +
        "TIMESTAMP_MILLIS, TIMESTAMP_MICROS. The INT96 type has the separate config: " +
        s"${PARQUET_INT96_REBASE_MODE_IN_WRITE.key}.")
      .version("3.0.0")
      .stringConf
      .transform(_.toUpperCase(Locale.ROOT))
      .checkValues(LegacyBehaviorPolicy.values.map(_.toString))
      .createWithDefault(LegacyBehaviorPolicy.CORRECTED.toString)

  val PARQUET_INT96_REBASE_MODE_IN_READ =
    buildConf("spark.sql.parquet.int96RebaseModeInRead")
      .internal()
      .doc("When LEGACY, Spark will rebase INT96 timestamps from the legacy hybrid (Julian + " +
        "Gregorian) calendar to Proleptic Gregorian calendar when reading Parquet files. " +
        "When CORRECTED, Spark will not do rebase and read the timestamps as it is. " +
        "When EXCEPTION, which is the default, Spark will fail the reading if it sees ancient " +
        "timestamps that are ambiguous between the two calendars. This config is only effective " +
        "if the writer info (like Spark, Hive) of the Parquet files is unknown.")
      .version("3.1.0")
      .stringConf
      .transform(_.toUpperCase(Locale.ROOT))
      .checkValues(LegacyBehaviorPolicy.values.map(_.toString))
      .createWithDefault(LegacyBehaviorPolicy.CORRECTED.toString)

  val PARQUET_REBASE_MODE_IN_READ =
    buildConf("spark.sql.parquet.datetimeRebaseModeInRead")
      .internal()
      .doc("When LEGACY, Spark will rebase dates/timestamps from the legacy hybrid (Julian + " +
        "Gregorian) calendar to Proleptic Gregorian calendar when reading Parquet files. " +
        "When CORRECTED, Spark will not do rebase and read the dates/timestamps as it is. " +
        "When EXCEPTION, which is the default, Spark will fail the reading if it sees " +
        "ancient dates/timestamps that are ambiguous between the two calendars. This config is " +
        "only effective if the writer info (like Spark, Hive) of the Parquet files is unknown. " +
        "This config influences on reads of the following parquet logical types: DATE, " +
        "TIMESTAMP_MILLIS, TIMESTAMP_MICROS. The INT96 type has the separate config: " +
        s"${PARQUET_INT96_REBASE_MODE_IN_READ.key}.")
      .version("3.0.0")
      .withAlternative("spark.sql.legacy.parquet.datetimeRebaseModeInRead")
      .stringConf
      .transform(_.toUpperCase(Locale.ROOT))
      .checkValues(LegacyBehaviorPolicy.values.map(_.toString))
      .createWithDefault(LegacyBehaviorPolicy.CORRECTED.toString)

  val AVRO_REBASE_MODE_IN_WRITE =
    buildConf("spark.sql.avro.datetimeRebaseModeInWrite")
      .internal()
      .doc("When LEGACY, Spark will rebase dates/timestamps from Proleptic Gregorian calendar " +
        "to the legacy hybrid (Julian + Gregorian) calendar when writing Avro files. " +
        "When CORRECTED, Spark will not do rebase and write the dates/timestamps as it is. " +
        "When EXCEPTION, which is the default, Spark will fail the writing if it sees " +
        "ancient dates/timestamps that are ambiguous between the two calendars.")
      .version("3.0.0")
      .stringConf
      .transform(_.toUpperCase(Locale.ROOT))
      .checkValues(LegacyBehaviorPolicy.values.map(_.toString))
      .createWithDefault(LegacyBehaviorPolicy.CORRECTED.toString)

  val AVRO_REBASE_MODE_IN_READ =
    buildConf("spark.sql.avro.datetimeRebaseModeInRead")
      .internal()
      .doc("When LEGACY, Spark will rebase dates/timestamps from the legacy hybrid (Julian + " +
        "Gregorian) calendar to Proleptic Gregorian calendar when reading Avro files. " +
        "When CORRECTED, Spark will not do rebase and read the dates/timestamps as it is. " +
        "When EXCEPTION, which is the default, Spark will fail the reading if it sees " +
        "ancient dates/timestamps that are ambiguous between the two calendars. This config is " +
        "only effective if the writer info (like Spark, Hive) of the Avro files is unknown.")
      .version("3.0.0")
      .stringConf
      .transform(_.toUpperCase(Locale.ROOT))
      .checkValues(LegacyBehaviorPolicy.values.map(_.toString))
      .createWithDefault(LegacyBehaviorPolicy.CORRECTED.toString)

  val SCRIPT_TRANSFORMATION_EXIT_TIMEOUT =
    buildConf("spark.sql.scriptTransformation.exitTimeoutInSeconds")
      .internal()
      .doc("Timeout for executor to wait for the termination of transformation script when EOF.")
      .version("3.0.0")
      .timeConf(TimeUnit.SECONDS)
      .checkValue(_ > 0, "The timeout value must be positive")
      .createWithDefault(10L)

  val COALESCE_BUCKETS_IN_JOIN_ENABLED =
    buildConf("spark.sql.bucketing.coalesceBucketsInJoin.enabled")
      .doc("When true, if two bucketed tables with the different number of buckets are joined, " +
        "the side with a bigger number of buckets will be coalesced to have the same number " +
        "of buckets as the other side. Bigger number of buckets is divisible by the smaller " +
        "number of buckets. Bucket coalescing is applied to sort-merge joins and " +
        "shuffled hash join. Note: Coalescing bucketed table can avoid unnecessary shuffling " +
        "in join, but it also reduces parallelism and could possibly cause OOM for " +
        "shuffled hash join.")
      .version("3.1.0")
      .booleanConf
      .createWithDefault(false)

  val COALESCE_BUCKETS_IN_JOIN_MAX_BUCKET_RATIO =
    buildConf("spark.sql.bucketing.coalesceBucketsInJoin.maxBucketRatio")
      .doc("The ratio of the number of two buckets being coalesced should be less than or " +
        "equal to this value for bucket coalescing to be applied. This configuration only " +
        s"has an effect when '${COALESCE_BUCKETS_IN_JOIN_ENABLED.key}' is set to true.")
      .version("3.1.0")
      .intConf
      .checkValue(_ > 0, "The difference must be positive.")
      .createWithDefault(4)

  val BROADCAST_HASH_JOIN_OUTPUT_PARTITIONING_EXPAND_LIMIT =
    buildConf("spark.sql.execution.broadcastHashJoin.outputPartitioningExpandLimit")
      .internal()
      .doc("The maximum number of partitionings that a HashPartitioning can be expanded to. " +
        "This configuration is applicable only for BroadcastHashJoin inner joins and can be " +
        "set to '0' to disable this feature.")
      .version("3.1.0")
      .intConf
      .checkValue(_ >= 0, "The value must be non-negative.")
      .createWithDefault(8)

  val OPTIMIZE_NULL_AWARE_ANTI_JOIN =
    buildConf("spark.sql.optimizeNullAwareAntiJoin")
      .internal()
      .doc("When true, NULL-aware anti join execution will be planed into " +
        "BroadcastHashJoinExec with flag isNullAwareAntiJoin enabled, " +
        "optimized from O(M*N) calculation into O(M) calculation " +
        "using Hash lookup instead of Looping lookup." +
        "Only support for singleColumn NAAJ for now.")
      .version("3.1.0")
      .booleanConf
      .createWithDefault(true)

  val LEGACY_DUPLICATE_BETWEEN_INPUT =
    buildConf("spark.sql.legacy.duplicateBetweenInput")
      .internal()
      .doc("When true, we use legacy between implementation. This is a flag that fixes a " +
        "problem introduced by a between optimization, see ticket SPARK-49063.")
      .version("4.0.0")
      .booleanConf
      .createWithDefault(false)

  val LEGACY_COMPLEX_TYPES_TO_STRING =
    buildConf("spark.sql.legacy.castComplexTypesToString.enabled")
      .internal()
      .doc("When true, maps and structs are wrapped by [] in casting to strings, and " +
        "NULL elements of structs/maps/arrays will be omitted while converting to strings. " +
        "Otherwise, if this is false, which is the default, maps and structs are wrapped by {}, " +
        "and NULL elements will be converted to \"null\".")
      .version("3.1.0")
      .booleanConf
      .createWithDefault(false)

  val LEGACY_PATH_OPTION_BEHAVIOR =
    buildConf("spark.sql.legacy.pathOptionBehavior.enabled")
      .internal()
      .doc("When true, \"path\" option is overwritten if one path parameter is passed to " +
        "DataFrameReader.load(), DataFrameWriter.save(), DataStreamReader.load(), or " +
        "DataStreamWriter.start(). Also, \"path\" option is added to the overall paths if " +
        "multiple path parameters are passed to DataFrameReader.load()")
      .version("3.1.0")
      .booleanConf
      .createWithDefault(false)

  val LEGACY_EXTRA_OPTIONS_BEHAVIOR =
    buildConf("spark.sql.legacy.extraOptionsBehavior.enabled")
      .internal()
      .doc("When true, the extra options will be ignored for DataFrameReader.table(). If set it " +
        "to false, which is the default, Spark will check if the extra options have the same " +
        "key, but the value is different with the table serde properties. If the check passes, " +
        "the extra options will be merged with the serde properties as the scan options. " +
        "Otherwise, an exception will be thrown.")
      .version("3.1.0")
      .booleanConf
      .createWithDefault(false)

  val LEGACY_CREATE_HIVE_TABLE_BY_DEFAULT =
    buildConf("spark.sql.legacy.createHiveTableByDefault")
      .internal()
      .doc("When set to true, CREATE TABLE syntax without USING or STORED AS will use Hive " +
        s"instead of the value of ${DEFAULT_DATA_SOURCE_NAME.key} as the table provider.")
      .version("3.1.0")
      .booleanConf
      .createWithDefault(sys.env.get("SPARK_SQL_LEGACY_CREATE_HIVE_TABLE").contains("true"))

  val LEGACY_CHAR_VARCHAR_AS_STRING =
    buildConf("spark.sql.legacy.charVarcharAsString")
      .internal()
      .doc("When true, Spark treats CHAR/VARCHAR type the same as STRING type, which is the " +
        "behavior of Spark 3.0 and earlier. This means no length check for CHAR/VARCHAR type and " +
        "no padding for CHAR type when writing data to the table.")
      .version("3.1.0")
      .booleanConf
      .createWithDefault(false)

  val CHAR_AS_VARCHAR = buildConf("spark.sql.charAsVarchar")
    .doc("When true, Spark replaces CHAR type with VARCHAR type in CREATE/REPLACE/ALTER TABLE " +
      "commands, so that newly created/updated tables will not have CHAR type columns/fields. " +
      "Existing tables with CHAR type columns/fields are not affected by this config.")
    .version("3.3.0")
    .booleanConf
    .createWithDefault(false)

  val READ_SIDE_CHAR_PADDING = buildConf("spark.sql.readSideCharPadding")
    .doc("When true, Spark applies string padding when reading CHAR type columns/fields, " +
      "in addition to the write-side padding. This config is true by default to better enforce " +
      "CHAR type semantic in cases such as external tables.")
    .version("3.4.0")
    .booleanConf
    .createWithDefault(true)

  val LEGACY_NO_CHAR_PADDING_IN_PREDICATE = buildConf("spark.sql.legacy.noCharPaddingInPredicate")
    .internal()
    .doc("When true, Spark will not apply char type padding for CHAR type columns in string " +
      s"comparison predicates, when '${READ_SIDE_CHAR_PADDING.key}' is false.")
    .version("4.0.0")
    .booleanConf
    .createWithDefault(false)

  val CLI_PRINT_HEADER =
    buildConf("spark.sql.cli.print.header")
     .doc("When set to true, spark-sql CLI prints the names of the columns in query output.")
     .version("3.2.0")
    .booleanConf
    .createWithDefault(false)

  val LEGACY_EMPTY_CURRENT_DB_IN_CLI =
    buildConf("spark.sql.legacy.emptyCurrentDBInCli")
      .internal()
      .doc("When false, spark-sql CLI prints the current database in prompt.")
      .version("3.4.0")
      .booleanConf
      .createWithDefault(false)

  val LEGACY_KEEP_COMMAND_OUTPUT_SCHEMA =
    buildConf("spark.sql.legacy.keepCommandOutputSchema")
      .internal()
      .doc("When true, Spark will keep the output schema of commands such as SHOW DATABASES " +
        "unchanged.")
      .version("3.0.2")
      .booleanConf
      .createWithDefault(false)

  val LEGACY_INTERVAL_ENABLED = buildConf("spark.sql.legacy.interval.enabled")
    .internal()
    .doc("When set to true, Spark SQL uses the mixed legacy interval type `CalendarIntervalType` " +
      "instead of the ANSI compliant interval types `YearMonthIntervalType` and " +
      "`DayTimeIntervalType`. For instance, the date subtraction expression returns " +
      "`CalendarIntervalType` when the SQL config is set to `true` otherwise an ANSI interval.")
    .version("3.2.0")
    .booleanConf
    .createWithDefault(false)

  val MAX_CONCURRENT_OUTPUT_FILE_WRITERS = buildConf("spark.sql.maxConcurrentOutputFileWriters")
    .internal()
    .doc("Maximum number of output file writers to use concurrently. If number of writers " +
      "needed reaches this limit, task will sort rest of output then writing them.")
    .version("3.2.0")
    .intConf
    .createWithDefault(0)

  val INFER_NESTED_DICT_AS_STRUCT = buildConf("spark.sql.pyspark.inferNestedDictAsStruct.enabled")
    .doc("PySpark's SparkSession.createDataFrame infers the nested dict as a map by default. " +
      "When it set to true, it infers the nested dict as a struct.")
    .version("3.3.0")
    .booleanConf
    .createWithDefault(false)

  val INFER_PANDAS_DICT_AS_MAP = buildConf("spark.sql.execution.pandas.inferPandasDictAsMap")
    .doc("When true, spark.createDataFrame will infer dict from Pandas DataFrame " +
      "as a MapType. When false, spark.createDataFrame infers dict from Pandas DataFrame " +
      "as a StructType which is default inferring from PyArrow.")
    .version("4.0.0")
    .booleanConf
    .createWithDefault(false)

  val LEGACY_INFER_ARRAY_TYPE_FROM_FIRST_ELEMENT =
    buildConf("spark.sql.pyspark.legacy.inferArrayTypeFromFirstElement.enabled")
      .internal()
      .doc("PySpark's SparkSession.createDataFrame infers the element type of an array from all " +
        "values in the array by default. If this config is set to true, it restores the legacy " +
        "behavior of only inferring the type from the first array element.")
      .version("3.4.0")
      .booleanConf
      .createWithDefault(false)

  val LEGACY_INFER_MAP_STRUCT_TYPE_FROM_FIRST_ITEM =
    buildConf("spark.sql.pyspark.legacy.inferMapTypeFromFirstPair.enabled")
      .internal()
      .doc("PySpark's SparkSession.createDataFrame infers the key/value types of a map from all " +
        "pairs in the map by default. If this config is set to true, it restores the legacy " +
        "behavior of only inferring the type from the first non-null pair.")
      .version("4.0.0")
      .booleanConf
      .createWithDefault(false)

  val LEGACY_USE_V1_COMMAND =
    buildConf("spark.sql.legacy.useV1Command")
      .internal()
      .doc("When true, Spark will use legacy V1 SQL commands.")
      .version("3.3.0")
      .booleanConf
      .createWithDefault(false)

  val HISTOGRAM_NUMERIC_PROPAGATE_INPUT_TYPE =
    buildConf("spark.sql.legacy.histogramNumericPropagateInputType")
      .internal()
      .doc("The histogram_numeric function computes a histogram on numeric 'expr' using nb bins. " +
        "The return value is an array of (x,y) pairs representing the centers of the histogram's " +
        "bins. If this config is set to true, the output type of the 'x' field in the return " +
        "value is propagated from the input value consumed in the aggregate function. Otherwise, " +
        "'x' always has double type.")
      .version("3.3.0")
      .booleanConf
      .createWithDefault(true)

  val LEGACY_LPAD_RPAD_BINARY_TYPE_AS_STRING =
    buildConf("spark.sql.legacy.lpadRpadAlwaysReturnString")
      .internal()
      .doc("When set to false, when the first argument and the optional padding pattern is a " +
        "byte sequence, the result is a BINARY value. The default padding pattern in this case " +
        "is the zero byte. " +
        "When set to true, it restores the legacy behavior of always returning string types " +
        "even for binary inputs.")
      .version("3.3.0")
      .booleanConf
      .createWithDefault(false)

  val LEGACY_NULL_VALUE_WRITTEN_AS_QUOTED_EMPTY_STRING_CSV =
    buildConf("spark.sql.legacy.nullValueWrittenAsQuotedEmptyStringCsv")
      .internal()
      .doc("When set to false, nulls are written as unquoted empty strings in CSV data source. " +
        "If set to true, it restores the legacy behavior that nulls were written as quoted " +
        "empty strings, `\"\"`.")
      .version("3.3.0")
      .booleanConf
      .createWithDefault(false)

  val LEGACY_ALLOW_NULL_COMPARISON_RESULT_IN_ARRAY_SORT =
    buildConf("spark.sql.legacy.allowNullComparisonResultInArraySort")
      .internal()
      .doc("When set to false, `array_sort` function throws an error " +
        "if the comparator function returns null. " +
        "If set to true, it restores the legacy behavior that handles null as zero (equal).")
      .version("3.2.2")
      .booleanConf
      .createWithDefault(false)

  val LEGACY_AVRO_ALLOW_INCOMPATIBLE_SCHEMA =
    buildConf("spark.sql.legacy.avro.allowIncompatibleSchema")
      .internal()
      .doc("When set to false, if types in Avro are encoded in the same format, but " +
        "the type in the Avro schema explicitly says that the data types are different, " +
        "reject reading the data type in the format to avoid returning incorrect results. " +
        "When set to true, it restores the legacy behavior of allow reading the data in the" +
        " format, which may return incorrect results.")
      .version("3.5.1")
      .booleanConf
      .createWithDefault(false)

  val LEGACY_NON_IDENTIFIER_OUTPUT_CATALOG_NAME =
    buildConf("spark.sql.legacy.v1IdentifierNoCatalog")
      .internal()
      .doc(s"When set to false, the v1 identifier will include '$SESSION_CATALOG_NAME' as " +
        "the catalog name if database is defined. When set to true, it restores the legacy " +
        "behavior that does not include catalog name.")
      .version("3.4.0")
      .booleanConf
      .createWithDefault(false)

  val LEGACY_IN_SUBQUERY_NULLABILITY =
    buildConf("spark.sql.legacy.inSubqueryNullability")
      .internal()
      .doc(s"When set to false, IN subquery nullability is correctly calculated based on " +
        s"both the left and right sides of the IN. When set to true, restores the legacy " +
        "behavior that does not check the right side's nullability.")
      .version("3.5.0")
      .booleanConf
      .createWithDefault(false)

  // Default is false (new, correct behavior) when ANSI is on, true (legacy, incorrect behavior)
  // when ANSI is off. See legacyNullInEmptyBehavior.
  val LEGACY_NULL_IN_EMPTY_LIST_BEHAVIOR =
    buildConf("spark.sql.legacy.nullInEmptyListBehavior")
      .internal()
      .doc("When set to true, restores the legacy incorrect behavior of IN expressions for " +
        "NULL values IN an empty list (including IN subqueries and literal IN lists): " +
        "`null IN (empty list)` should evaluate to false, but sometimes (not always) " +
        "incorrectly evaluates to null in the legacy behavior.")
      .version("3.5.0")
      .booleanConf
      .createOptional

  val ERROR_MESSAGE_FORMAT = buildConf("spark.sql.error.messageFormat")
    .doc("When PRETTY, the error message consists of textual representation of error class, " +
      "message and query context. The MINIMAL and STANDARD formats are pretty JSON formats where " +
      "STANDARD includes an additional JSON field `message`. This configuration property " +
      "influences on error messages of Thrift Server and SQL CLI while running queries.")
    .version("3.4.0")
    .stringConf.transform(_.toUpperCase(Locale.ROOT))
    .checkValues(ErrorMessageFormat.values.map(_.toString))
    .createWithDefault(ErrorMessageFormat.PRETTY.toString)

  val LATERAL_COLUMN_ALIAS_IMPLICIT_ENABLED =
    buildConf("spark.sql.lateralColumnAlias.enableImplicitResolution")
      .internal()
      .doc("Enable resolving implicit lateral column alias defined in the same SELECT list. For " +
        "example, with this conf turned on, for query `SELECT 1 AS a, a + 1` the `a` in `a + 1` " +
        "can be resolved as the previously defined `1 AS a`. But note that table column has " +
        "higher resolution priority than the lateral column alias.")
      .version("3.4.0")
      .booleanConf
      .createWithDefault(true)

  val STABLE_DERIVED_COLUMN_ALIAS_ENABLED =
    buildConf("spark.sql.stableDerivedColumnAlias.enabled")
      .internal()
      .doc("Enable deriving of stable column aliases from the lexer tree instead of parse tree " +
        "and form them via pretty SQL print.")
      .version("3.5.0")
      .booleanConf
      .createWithDefault(false)

  val LOCAL_RELATION_CACHE_THRESHOLD =
    buildConf(SqlApiConfHelper.LOCAL_RELATION_CACHE_THRESHOLD_KEY)
      .doc("The threshold for the size in bytes of local relations to be cached at " +
        "the driver side after serialization.")
      .version("3.5.0")
      .intConf
      .checkValue(_ >= 0, "The threshold of cached local relations must not be negative")
      .createWithDefault(64 * 1024 * 1024)

  val DECORRELATE_JOIN_PREDICATE_ENABLED =
    buildConf("spark.sql.optimizer.decorrelateJoinPredicate.enabled")
      .internal()
      .doc("Decorrelate scalar and lateral subqueries with correlated references in join " +
        "predicates. This configuration is only effective when " +
        s"'${DECORRELATE_INNER_QUERY_ENABLED.key}' is true.")
      .version("4.0.0")
      .booleanConf
      .createWithDefault(true)

  val DECORRELATE_PREDICATE_SUBQUERIES_IN_JOIN_CONDITION =
    buildConf("spark.sql.optimizer.decorrelatePredicateSubqueriesInJoinPredicate.enabled")
      .internal()
      .doc("Decorrelate predicate (in and exists) subqueries with correlated references in join " +
        "predicates.")
      .version("4.0.0")
      .booleanConf
      .createWithDefault(true)

  val OPTIMIZE_UNCORRELATED_IN_SUBQUERIES_IN_JOIN_CONDITION =
    buildConf("spark.sql.optimizer.optimizeUncorrelatedInSubqueriesInJoinCondition.enabled")
      .internal()
      .doc("When true, optimize uncorrelated IN subqueries in join predicates by rewriting them " +
        s"to joins. This interacts with ${LEGACY_NULL_IN_EMPTY_LIST_BEHAVIOR.key} because it " +
        "can rewrite IN predicates.")
      .version("4.0.0")
      .booleanConf
      .createWithDefault(true)

  val EXCLUDE_SUBQUERY_EXP_REFS_FROM_REMOVE_REDUNDANT_ALIASES =
    buildConf("spark.sql.optimizer.excludeSubqueryRefsFromRemoveRedundantAliases.enabled")
      .internal()
      .doc("When true, exclude the references from the subquery expressions (in, exists, etc.) " +
        s"while removing redundant aliases.")
      .version("4.0.0")
      .booleanConf
      .createWithDefault(true)

  val TIME_TRAVEL_TIMESTAMP_KEY =
    buildConf("spark.sql.timeTravelTimestampKey")
      .doc("The option name to specify the time travel timestamp when reading a table.")
      .version("4.0.0")
      .stringConf
      .createWithDefault("timestampAsOf")

  val TIME_TRAVEL_VERSION_KEY =
    buildConf("spark.sql.timeTravelVersionKey")
      .doc("The option name to specify the time travel table version when reading a table.")
      .version("4.0.0")
      .stringConf
      .createWithDefault("versionAsOf")

  val OPERATOR_PIPE_SYNTAX_ENABLED =
    buildConf("spark.sql.operatorPipeSyntaxEnabled")
      .doc("If true, enable operator pipe syntax for Apache Spark SQL. This uses the operator " +
        "pipe marker |> to indicate separation between clauses of SQL in a manner that describes " +
        "the sequence of steps that the query performs in a composable fashion.")
      .version("4.0.0")
      .booleanConf
      .createWithDefault(Utils.isTesting)

  val LEGACY_PERCENTILE_DISC_CALCULATION = buildConf("spark.sql.legacy.percentileDiscCalculation")
    .internal()
    .doc("If true, the old bogus percentile_disc calculation is used. The old calculation " +
      "incorrectly mapped the requested percentile to the sorted range of values in some cases " +
      "and so returned incorrect results. Also, the new implementation is faster as it doesn't " +
      "contain the interpolation logic that the old percentile_cont based one did.")
    .version("3.3.4")
    .booleanConf
    .createWithDefault(false)

  val LEGACY_NEGATIVE_INDEX_IN_ARRAY_INSERT =
    buildConf("spark.sql.legacy.negativeIndexInArrayInsert")
      .internal()
      .doc("When set to true, restores the legacy behavior of `array_insert` for " +
        "negative indexes - 0-based: the function inserts new element before the last one " +
        "for the index -1. For example, `array_insert(['a', 'b'], -1, 'x')` returns " +
        "`['a', 'x', 'b']`. When set to false, the -1 index points out to the last element, " +
        "and the given example produces `['a', 'b', 'x']`.")
      .version("3.4.2")
      .booleanConf
      .createWithDefault(false)

  val LEGACY_RAISE_ERROR_WITHOUT_ERROR_CLASS =
    buildConf("spark.sql.legacy.raiseErrorWithoutErrorClass")
      .internal()
      .doc("When set to true, restores the legacy behavior of `raise_error` and `assert_true` to " +
        "not return the `[USER_RAISED_EXCEPTION]` prefix." +
        "For example, `raise_error('error!')` returns `error!` instead of " +
        "`[USER_RAISED_EXCEPTION] Error!`.")
      .version("4.0.0")
      .booleanConf
      .createWithDefault(false)

  val LEGACY_SCALAR_SUBQUERY_COUNT_BUG_HANDLING =
    buildConf("spark.sql.legacy.scalarSubqueryCountBugBehavior")
      .internal()
      .doc("When set to true, restores legacy behavior of potential incorrect count bug " +
        "handling for scalar subqueries.")
      .version("4.0.0")
      .booleanConf
      .createWithDefault(false)

  val LEGACY_SCALAR_SUBQUERY_ALLOW_GROUP_BY_NON_EQUALITY_CORRELATED_PREDICATE =
    buildConf("spark.sql.legacy.scalarSubqueryAllowGroupByNonEqualityCorrelatedPredicate")
      .internal()
      .doc("When set to true, use incorrect legacy behavior for checking whether a scalar " +
        "subquery with a group-by on correlated columns is allowed. See SPARK-48503")
      .version("4.0.0")
      .booleanConf
      .createWithDefault(false)

  val SCALAR_SUBQUERY_ALLOW_GROUP_BY_COLUMN_EQUAL_TO_CONSTANT =
    buildConf("spark.sql.analyzer.scalarSubqueryAllowGroupByColumnEqualToConstant")
      .internal()
      .doc("When set to true, allow scalar subqueries with group-by on a column that also " +
        " has an equality filter with a constant (SPARK-48557).")
      .version("4.0.0")
      .booleanConf
      .createWithDefault(true)

  val SCALAR_SUBQUERY_USE_SINGLE_JOIN =
    buildConf("spark.sql.optimizer.scalarSubqueryUseSingleJoin")
      .internal()
      .doc("When set to true, allow scalar subqueries with group-by on a column that also " +
        " has an equality filter with a constant (SPARK-48557).")
      .version("4.0.0")
      .booleanConf
      .createWithDefault(true)

  val ALLOW_SUBQUERY_EXPRESSIONS_IN_LAMBDAS_AND_HIGHER_ORDER_FUNCTIONS =
    buildConf("spark.sql.analyzer.allowSubqueryExpressionsInLambdasOrHigherOrderFunctions")
      .internal()
      .doc("When set to false, the analyzer will throw an error if a subquery expression appears " +
        "in a lambda function or higher-order function. When set to true, it restores the legacy " +
        "behavior of allowing subquery eexpressions in lambda functions or higher-order functions.")
      .version("4.0.0")
      .booleanConf
      .createWithDefault(false)

  val SUPPORT_SECOND_OFFSET_FORMAT =
    buildConf("spark.sql.files.supportSecondOffsetFormat")
      .internal()
      .doc("When set to true, datetime formatter used for csv, json and xml " +
        "will support zone offsets that have seconds in it. e.g. LA timezone offset prior to 1883" +
        "was -07:52:58. When this flag is not set we lose seconds information." )
      .version("4.0.0")
      .booleanConf
      .createWithDefault(true)

  // Deprecate "spark.connect.copyFromLocalToFs.allowDestLocal" in favor of this config. This is
  // currently optional because we don't want to break existing users who are using the old config.
  // If this config is set, then we override the deprecated config.
  val ARTIFACT_COPY_FROM_LOCAL_TO_FS_ALLOW_DEST_LOCAL =
    buildConf("spark.sql.artifact.copyFromLocalToFs.allowDestLocal")
      .internal()
      .doc("""
             |Allow `spark.copyFromLocalToFs` destination to be local file system
             | path on spark driver node when
             |`spark.sql.artifact.copyFromLocalToFs.allowDestLocal` is true.
             |This will allow user to overwrite arbitrary file on spark
             |driver node we should only enable it for testing purpose.
             |""".stripMargin)
      .version("4.0.0")
      .booleanConf
      .createOptional

  val LEGACY_RETAIN_FRACTION_DIGITS_FIRST =
    buildConf("spark.sql.legacy.decimal.retainFractionDigitsOnTruncate")
      .internal()
      .doc("When set to true, we will try to retain the fraction digits first rather than " +
        "integral digits as prior Spark 4.0, when getting a least common type between decimal " +
        "types, and the result decimal precision exceeds the max precision.")
      .version("4.0.0")
      .booleanConf
      .createWithDefault(false)

  val STACK_TRACES_IN_DATAFRAME_CONTEXT = buildConf("spark.sql.stackTracesInDataFrameContext")
    .doc("The number of non-Spark stack traces in the captured DataFrame query context.")
    .version("4.0.0")
    .intConf
    .checkValue(_ > 0, "The number of stack traces in the DataFrame context must be positive.")
    .createWithDefault(1)

  val LEGACY_JAVA_CHARSETS = buildConf("spark.sql.legacy.javaCharsets")
    .internal()
    .doc("When set to true, the functions like `encode()` can use charsets from JDK while " +
      "encoding or decoding string values. If it is false, such functions support only one of " +
      "the charsets: 'US-ASCII', 'ISO-8859-1', 'UTF-8', 'UTF-16BE', 'UTF-16LE', 'UTF-16', " +
      "'UTF-32'.")
    .version("4.0.0")
    .booleanConf
    .createWithDefault(false)

  val LEGACY_CODING_ERROR_ACTION = buildConf("spark.sql.legacy.codingErrorAction")
    .internal()
    .doc("When set to true, encode/decode functions replace unmappable characters with mojibake " +
      "instead of reporting coding errors.")
    .version("4.0.0")
    .booleanConf
    .createWithDefault(false)

  val LEGACY_EVAL_CURRENT_TIME = buildConf("spark.sql.legacy.earlyEvalCurrentTime")
    .internal()
    .doc("When set to true, evaluation and constant folding will happen for now() and " +
      "current_timestamp() expressions before finish analysis phase. " +
      "This flag will allow a bit more liberal syntax but it will sacrifice correctness - " +
      "Results of now() and current_timestamp() can be different for different operations " +
      "in a single query."
    )
    .version("4.0.0")
    .booleanConf
    .createWithDefault(false)

  val LEGACY_BANG_EQUALS_NOT = buildConf("spark.sql.legacy.bangEqualsNot")
    .internal()
    .doc("When set to true, '!' is a lexical equivalent for 'NOT'. That is '!' can be used " +
      "outside of the documented prefix usage in a logical expression." +
      "Examples are: `expr ! IN (1, 2)` and `expr ! BETWEEN 1 AND 2`, but also `IF ! EXISTS`."
    )
    .version("4.0.0")
    .booleanConf
    .createWithDefault(false)

  /**
   * Holds information about keys that have been deprecated.
   *
   * @param key The deprecated key.
   * @param version Version of Spark where key was deprecated.
   * @param comment Additional info regarding to the removed config. For example,
   *                reasons of config deprecation, what users should use instead of it.
   */
  case class DeprecatedConfig(key: String, version: String, comment: String) {
    def toDeprecationString: String = {
      s"The SQL config '$key' has been deprecated in Spark v$version " +
        s"and may be removed in the future. $comment"
    }
  }

  /**
   * Maps deprecated SQL config keys to information about the deprecation.
   *
   * The extra information is logged as a warning when the SQL config is present
   * in the user's configuration.
   */
  val deprecatedSQLConfigs: Map[String, DeprecatedConfig] = {
    val configs = Seq(
      DeprecatedConfig(
        PANDAS_GROUPED_MAP_ASSIGN_COLUMNS_BY_NAME.key, "2.4",
        "The config allows to switch to the behaviour before Spark 2.4 " +
          "and will be removed in the future releases."),
      DeprecatedConfig(ARROW_EXECUTION_ENABLED.key, "3.0",
        s"Use '${ARROW_PYSPARK_EXECUTION_ENABLED.key}' instead of it."),
      DeprecatedConfig(ARROW_FALLBACK_ENABLED.key, "3.0",
        s"Use '${ARROW_PYSPARK_FALLBACK_ENABLED.key}' instead of it."),
      DeprecatedConfig(SHUFFLE_TARGET_POSTSHUFFLE_INPUT_SIZE.key, "3.0",
        s"Use '${ADVISORY_PARTITION_SIZE_IN_BYTES.key}' instead of it."),
      DeprecatedConfig(OPTIMIZER_METADATA_ONLY.key, "3.0",
        "Avoid to depend on this optimization to prevent a potential correctness issue. " +
          "If you must use, use 'SparkSessionExtensions' instead to inject it as a custom rule."),
      DeprecatedConfig(CONVERT_CTAS.key, "3.1",
        s"Set '${LEGACY_CREATE_HIVE_TABLE_BY_DEFAULT.key}' to false instead."),
      DeprecatedConfig("spark.sql.sources.schemaStringLengthThreshold", "3.2",
        s"Use '${HIVE_TABLE_PROPERTY_LENGTH_THRESHOLD.key}' instead."),
      DeprecatedConfig(LEGACY_REPLACE_DATABRICKS_SPARK_AVRO_ENABLED.key, "3.2",
        """Use `.format("avro")` in `DataFrameWriter` or `DataFrameReader` instead."""),
      DeprecatedConfig(COALESCE_PARTITIONS_MIN_PARTITION_NUM.key, "3.2",
        s"Use '${COALESCE_PARTITIONS_MIN_PARTITION_SIZE.key}' instead."),
      DeprecatedConfig(PARQUET_REBASE_MODE_IN_READ.alternatives.head, "3.2",
        s"Use '${PARQUET_REBASE_MODE_IN_READ.key}' instead."),
      DeprecatedConfig(ESCAPED_STRING_LITERALS.key, "4.0",
        "Use raw string literals with the `r` prefix instead. "),
      DeprecatedConfig("spark.connect.copyFromLocalToFs.allowDestLocal", "4.0",
        s"Use '${ARTIFACT_COPY_FROM_LOCAL_TO_FS_ALLOW_DEST_LOCAL.key}' instead."),
      DeprecatedConfig(ALLOW_ZERO_INDEX_IN_FORMAT_STRING.key, "4.0", "Increase indexes by 1 " +
        "in `strfmt` of the `format_string` function. Refer to the first argument by \"1$\".")
    )

    Map(configs.map { cfg => cfg.key -> cfg } : _*)
  }

  /**
   * Holds information about keys that have been removed.
   *
   * @param key The removed config key.
   * @param version Version of Spark where key was removed.
   * @param defaultValue The default config value. It can be used to notice
   *                     users that they set non-default value to an already removed config.
   * @param comment Additional info regarding to the removed config.
   */
  case class RemovedConfig(key: String, version: String, defaultValue: String, comment: String) {
    if (VersionUtils.majorMinorPatchVersion(version).isEmpty) {
      throw SparkException.internalError(
        s"The removed SQL config $key has the wrong Spark version: $version")
    }
  }

  /**
   * The map contains info about removed SQL configs. Keys are SQL config names,
   * map values contain extra information like the version in which the config was removed,
   * config's default value and a comment.
   *
   * Please, add a removed SQL configuration property here only when it affects behaviours.
   * For example, `spark.sql.variable.substitute.depth` was not added as it virtually
   * became no-op later. By this, it makes migrations to new Spark versions painless.
   */
  val removedSQLConfigs: Map[String, RemovedConfig] = {
    val configs = Seq(
      RemovedConfig("spark.sql.fromJsonForceNullableSchema", "3.0.0", "true",
        "It was removed to prevent errors like SPARK-23173 for non-default value."),
      RemovedConfig(
        "spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "3.0.0", "false",
        "It was removed to prevent loss of user data for non-default value."),
      RemovedConfig("spark.sql.legacy.compareDateTimestampInTimestamp", "3.0.0", "true",
        "It was removed to prevent errors like SPARK-23549 for non-default value."),
      RemovedConfig("spark.sql.parquet.int64AsTimestampMillis", "3.0.0", "false",
        "The config was deprecated since Spark 2.3." +
        s"Use '${PARQUET_OUTPUT_TIMESTAMP_TYPE.key}' instead of it."),
      RemovedConfig("spark.sql.execution.pandas.respectSessionTimeZone", "3.0.0", "true",
        "The non-default behavior is considered as a bug, see SPARK-22395. " +
        "The config was deprecated since Spark 2.3."),
      RemovedConfig("spark.sql.optimizer.planChangeLog.level", "3.1.0", "trace",
        s"Please use `${PLAN_CHANGE_LOG_LEVEL.key}` instead."),
      RemovedConfig("spark.sql.optimizer.planChangeLog.rules", "3.1.0", "",
        s"Please use `${PLAN_CHANGE_LOG_RULES.key}` instead."),
      RemovedConfig("spark.sql.optimizer.planChangeLog.batches", "3.1.0", "",
        s"Please use `${PLAN_CHANGE_LOG_BATCHES.key}` instead."),
      RemovedConfig("spark.sql.ansi.strictIndexOperator", "3.4.0", "true",
        "This was an internal configuration. It is not needed anymore since Spark SQL always " +
          "returns null when getting a map value with a non-existing key. See SPARK-40066 " +
          "for more details."),
      RemovedConfig("spark.sql.hive.verifyPartitionPath", "4.0.0", "false",
        s"This config was replaced by '${IGNORE_MISSING_FILES.key}'."),
      RemovedConfig("spark.sql.optimizer.runtimeFilter.semiJoinReduction.enabled", "4.0.0", "false",
        "This optimizer config is useless as runtime filter cannot be an IN subquery now."),
      RemovedConfig("spark.sql.legacy.parquet.int96RebaseModeInWrite", "4.0.0",
        LegacyBehaviorPolicy.CORRECTED.toString,
        s"Use '${PARQUET_INT96_REBASE_MODE_IN_WRITE.key}' instead."),
      RemovedConfig("spark.sql.legacy.parquet.int96RebaseModeInRead", "4.0.0",
        LegacyBehaviorPolicy.CORRECTED.toString,
        s"Use '${PARQUET_INT96_REBASE_MODE_IN_READ.key}' instead."),
      RemovedConfig("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "4.0.0",
        LegacyBehaviorPolicy.CORRECTED.toString,
        s"Use '${PARQUET_REBASE_MODE_IN_WRITE.key}' instead."),
      RemovedConfig("spark.sql.legacy.avro.datetimeRebaseModeInWrite", "4.0.0",
        LegacyBehaviorPolicy.CORRECTED.toString,
        s"Use '${AVRO_REBASE_MODE_IN_WRITE.key}' instead."),
      RemovedConfig("spark.sql.legacy.avro.datetimeRebaseModeInRead", "4.0.0",
        LegacyBehaviorPolicy.CORRECTED.toString,
        s"Use '${AVRO_REBASE_MODE_IN_READ.key}' instead.")
    )

    Map(configs.map { cfg => cfg.key -> cfg } : _*)
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
class SQLConf extends Serializable with Logging with SqlApiConf {
  import SQLConf._

  /** Only low degree of contention is expected for conf, thus NOT using ConcurrentHashMap. */
  @transient protected[spark] val settings = java.util.Collections.synchronizedMap(
    new java.util.HashMap[String, String]())

  @transient protected val reader = new ConfigReader(settings)

  /** ************************ Spark SQL Params/Hints ******************* */

  def analyzerMaxIterations: Int = getConf(ANALYZER_MAX_ITERATIONS)

  def optimizerExcludedRules: Option[String] = getConf(OPTIMIZER_EXCLUDED_RULES)

  def optimizerMaxIterations: Int = getConf(OPTIMIZER_MAX_ITERATIONS)

  def optimizerInSetConversionThreshold: Int = getConf(OPTIMIZER_INSET_CONVERSION_THRESHOLD)

  def optimizerInSetSwitchThreshold: Int = getConf(OPTIMIZER_INSET_SWITCH_THRESHOLD)

  def planChangeLogLevel: String = getConf(PLAN_CHANGE_LOG_LEVEL)

  def planChangeRules: Option[String] = getConf(PLAN_CHANGE_LOG_RULES)

  def planChangeBatches: Option[String] = getConf(PLAN_CHANGE_LOG_BATCHES)

  def dynamicPartitionPruningEnabled: Boolean = getConf(DYNAMIC_PARTITION_PRUNING_ENABLED)

  def dynamicPartitionPruningUseStats: Boolean = getConf(DYNAMIC_PARTITION_PRUNING_USE_STATS)

  def dynamicPartitionPruningFallbackFilterRatio: Double =
    getConf(DYNAMIC_PARTITION_PRUNING_FALLBACK_FILTER_RATIO)

  def dynamicPartitionPruningReuseBroadcastOnly: Boolean =
    getConf(DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY)

  def runtimeFilterBloomFilterEnabled: Boolean =
    getConf(RUNTIME_BLOOM_FILTER_ENABLED)

  def runtimeFilterCreationSideThreshold: Long =
    getConf(RUNTIME_BLOOM_FILTER_CREATION_SIDE_THRESHOLD)

  def runtimeRowLevelOperationGroupFilterEnabled: Boolean =
    getConf(RUNTIME_ROW_LEVEL_OPERATION_GROUP_FILTER_ENABLED)

  def stateStoreProviderClass: String = getConf(STATE_STORE_PROVIDER_CLASS)

  def isStateSchemaCheckEnabled: Boolean = getConf(STATE_SCHEMA_CHECK_ENABLED)

  def numStateStoreMaintenanceThreads: Int = getConf(NUM_STATE_STORE_MAINTENANCE_THREADS)

  def stateStoreMinDeltasForSnapshot: Int = getConf(STATE_STORE_MIN_DELTAS_FOR_SNAPSHOT)

  def stateStoreFormatValidationEnabled: Boolean = getConf(STATE_STORE_FORMAT_VALIDATION_ENABLED)

  def stateStoreSkipNullsForStreamStreamJoins: Boolean =
    getConf(STATE_STORE_SKIP_NULLS_FOR_STREAM_STREAM_JOINS)

  def checkpointLocation: Option[String] = getConf(CHECKPOINT_LOCATION)

  def isUnsupportedOperationCheckEnabled: Boolean = getConf(UNSUPPORTED_OPERATION_CHECK_ENABLED)

  def useDeprecatedKafkaOffsetFetching: Boolean = getConf(USE_DEPRECATED_KAFKA_OFFSET_FETCHING)

  def statefulOperatorCorrectnessCheckEnabled: Boolean =
    getConf(STATEFUL_OPERATOR_CHECK_CORRECTNESS_ENABLED)

  def fileStreamSinkMetadataIgnored: Boolean = getConf(FILESTREAM_SINK_METADATA_IGNORED)

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

  def filesMinPartitionNum: Option[Int] = getConf(FILES_MIN_PARTITION_NUM)

  def filesMaxPartitionNum: Option[Int] = getConf(FILES_MAX_PARTITION_NUM)

  def ignoreCorruptFiles: Boolean = getConf(IGNORE_CORRUPT_FILES)

  def ignoreMissingFiles: Boolean = getConf(IGNORE_MISSING_FILES)

  def ignoreInvalidPartitionPaths: Boolean = getConf(IGNORE_INVALID_PARTITION_PATHS)

  def maxRecordsPerFile: Long = getConf(MAX_RECORDS_PER_FILE)

  def useCompression: Boolean = getConf(COMPRESS_CACHED)

  def orcCompressionCodec: String = getConf(ORC_COMPRESSION)

  def orcVectorizedReaderEnabled: Boolean = getConf(ORC_VECTORIZED_READER_ENABLED)

  def orcVectorizedReaderBatchSize: Int = getConf(ORC_VECTORIZED_READER_BATCH_SIZE)

  def orcVectorizedWriterBatchSize: Int = getConf(ORC_VECTORIZED_WRITER_BATCH_SIZE)

  def orcVectorizedReaderNestedColumnEnabled: Boolean =
    getConf(ORC_VECTORIZED_READER_NESTED_COLUMN_ENABLED)

  def parquetCompressionCodec: String = getConf(PARQUET_COMPRESSION)

  def parquetVectorizedReaderEnabled: Boolean = getConf(PARQUET_VECTORIZED_READER_ENABLED)

  def parquetVectorizedReaderNestedColumnEnabled: Boolean =
    getConf(PARQUET_VECTORIZED_READER_NESTED_COLUMN_ENABLED)

  def parquetVectorizedReaderBatchSize: Int = getConf(PARQUET_VECTORIZED_READER_BATCH_SIZE)

  def columnBatchSize: Int = getConf(COLUMN_BATCH_SIZE)

  def vectorizedHugeVectorThreshold: Int = getConf(VECTORIZED_HUGE_VECTOR_THRESHOLD).toInt

  def vectorizedHugeVectorReserveRatio: Double = getConf(VECTORIZED_HUGE_VECTOR_RESERVE_RATIO)

  def cacheVectorizedReaderEnabled: Boolean = getConf(CACHE_VECTORIZED_READER_ENABLED)

  def defaultNumShufflePartitions: Int = getConf(SHUFFLE_PARTITIONS)

  def numShufflePartitions: Int = {
    if (adaptiveExecutionEnabled && coalesceShufflePartitionsEnabled) {
      getConf(COALESCE_PARTITIONS_INITIAL_PARTITION_NUM).getOrElse(defaultNumShufflePartitions)
    } else {
      defaultNumShufflePartitions
    }
  }

  override def defaultStringType: StringType = {
    if (getConf(DEFAULT_COLLATION).toUpperCase(Locale.ROOT) == "UTF8_BINARY") {
      StringType
    } else {
      StringType(CollationFactory.collationNameToId(getConf(DEFAULT_COLLATION)))
    }
  }

  def adaptiveExecutionEnabled: Boolean = getConf(ADAPTIVE_EXECUTION_ENABLED)

  def adaptiveExecutionLogLevel: String = getConf(ADAPTIVE_EXECUTION_LOG_LEVEL)

  def fetchShuffleBlocksInBatch: Boolean = getConf(FETCH_SHUFFLE_BLOCKS_IN_BATCH)

  def nonEmptyPartitionRatioForBroadcastJoin: Double =
    getConf(NON_EMPTY_PARTITION_RATIO_FOR_BROADCAST_JOIN)

  def coalesceShufflePartitionsEnabled: Boolean = getConf(COALESCE_PARTITIONS_ENABLED)

  def minBatchesToRetain: Int = getConf(MIN_BATCHES_TO_RETAIN)

  def ratioExtraSpaceAllowedInCheckpoint: Double = getConf(RATIO_EXTRA_SPACE_ALLOWED_IN_CHECKPOINT)

  def maxBatchesToRetainInMemory: Int = getConf(MAX_BATCHES_TO_RETAIN_IN_MEMORY)

  def streamingMaintenanceInterval: Long = getConf(STREAMING_MAINTENANCE_INTERVAL)

  def stateStoreCompressionCodec: String = getConf(STATE_STORE_COMPRESSION_CODEC)

  def checkpointRenamedFileCheck: Boolean = getConf(CHECKPOINT_RENAMEDFILE_CHECK_ENABLED)

  def parquetFilterPushDown: Boolean = getConf(PARQUET_FILTER_PUSHDOWN_ENABLED)

  def parquetFilterPushDownDate: Boolean = getConf(PARQUET_FILTER_PUSHDOWN_DATE_ENABLED)

  def parquetFilterPushDownTimestamp: Boolean = getConf(PARQUET_FILTER_PUSHDOWN_TIMESTAMP_ENABLED)

  def parquetFilterPushDownDecimal: Boolean = getConf(PARQUET_FILTER_PUSHDOWN_DECIMAL_ENABLED)

  def parquetFilterPushDownStringPredicate: Boolean =
    getConf(PARQUET_FILTER_PUSHDOWN_STRING_PREDICATE_ENABLED)

  def parquetFilterPushDownInFilterThreshold: Int =
    getConf(PARQUET_FILTER_PUSHDOWN_INFILTERTHRESHOLD)

  def parquetAggregatePushDown: Boolean = getConf(PARQUET_AGGREGATE_PUSHDOWN_ENABLED)

  def orcFilterPushDown: Boolean = getConf(ORC_FILTER_PUSHDOWN_ENABLED)

  def orcAggregatePushDown: Boolean = getConf(ORC_AGGREGATE_PUSHDOWN_ENABLED)

  def isOrcSchemaMergingEnabled: Boolean = getConf(ORC_SCHEMA_MERGING_ENABLED)

  def metastoreDropPartitionsByName: Boolean = getConf(HIVE_METASTORE_DROP_PARTITION_BY_NAME)

  def metastorePartitionPruning: Boolean = getConf(HIVE_METASTORE_PARTITION_PRUNING)

  def metastorePartitionPruningInSetThreshold: Int =
    getConf(HIVE_METASTORE_PARTITION_PRUNING_INSET_THRESHOLD)

  def metastorePartitionPruningFallbackOnException: Boolean =
    getConf(HIVE_METASTORE_PARTITION_PRUNING_FALLBACK_ON_EXCEPTION)

  def metastorePartitionPruningFastFallback: Boolean =
    getConf(HIVE_METASTORE_PARTITION_PRUNING_FAST_FALLBACK)

  def manageFilesourcePartitions: Boolean = getConf(HIVE_MANAGE_FILESOURCE_PARTITIONS)

  def filesourcePartitionFileCacheSize: Long = getConf(HIVE_FILESOURCE_PARTITION_FILE_CACHE_SIZE)

  def caseSensitiveInferenceMode: HiveCaseSensitiveInferenceMode.Value =
    HiveCaseSensitiveInferenceMode.withName(getConf(HIVE_CASE_SENSITIVE_INFERENCE))

  def gatherFastStats: Boolean = getConf(GATHER_FASTSTAT)

  def optimizerMetadataOnly: Boolean = getConf(OPTIMIZER_METADATA_ONLY)

  def wholeStageEnabled: Boolean = getConf(WHOLESTAGE_CODEGEN_ENABLED)

  def wholeStageUseIdInClassName: Boolean = getConf(WHOLESTAGE_CODEGEN_USE_ID_IN_CLASS_NAME)

  def wholeStageMaxNumFields: Int = getConf(WHOLESTAGE_MAX_NUM_FIELDS)

  def codegenFallback: Boolean = getConf(CODEGEN_FALLBACK)

  def codegenFactoryMode: String = getConf(CODEGEN_FACTORY_MODE)

  def codegenComments: Boolean = getConf(StaticSQLConf.CODEGEN_COMMENTS)

  def loggingMaxLinesForCodegen: Int = getConf(CODEGEN_LOGGING_MAX_LINES)

  def hugeMethodLimit: Int = getConf(WHOLESTAGE_HUGE_METHOD_LIMIT)

  def methodSplitThreshold: Int = getConf(CODEGEN_METHOD_SPLIT_THRESHOLD)

  def wholeStageSplitConsumeFuncByOperator: Boolean =
    getConf(WHOLESTAGE_SPLIT_CONSUME_FUNC_BY_OPERATOR)

  def broadcastCleanedSourceThreshold: Int =
    getConf(SQLConf.WHOLESTAGE_BROADCAST_CLEANED_SOURCE_THRESHOLD)

  def tableRelationCacheSize: Int =
    getConf(StaticSQLConf.FILESOURCE_TABLE_RELATION_CACHE_SIZE)

  def codegenCacheMaxEntries: Int = getConf(StaticSQLConf.CODEGEN_CACHE_MAX_ENTRIES)

  def exchangeReuseEnabled: Boolean = getConf(EXCHANGE_REUSE_ENABLED)

  def subqueryReuseEnabled: Boolean = getConf(SUBQUERY_REUSE_ENABLED)

  override def caseSensitiveAnalysis: Boolean = getConf(SQLConf.CASE_SENSITIVE)

  def constraintPropagationEnabled: Boolean = getConf(CONSTRAINT_PROPAGATION_ENABLED)

  def escapedStringLiterals: Boolean = getConf(ESCAPED_STRING_LITERALS)

  def fileCompressionFactor: Double = getConf(FILE_COMPRESSION_FACTOR)

  def stringRedactionPattern: Option[Regex] = getConf(SQL_STRING_REDACTION_PATTERN)

  def sortBeforeRepartition: Boolean = getConf(SORT_BEFORE_REPARTITION)

  def topKSortFallbackThreshold: Int = getConf(TOP_K_SORT_FALLBACK_THRESHOLD)

  def fastHashAggregateRowMaxCapacityBit: Int = getConf(FAST_HASH_AGGREGATE_MAX_ROWS_CAPACITY_BIT)

  def streamingSessionWindowMergeSessionInLocalPartition: Boolean =
    getConf(STREAMING_SESSION_WINDOW_MERGE_SESSIONS_IN_LOCAL_PARTITION)

  override def datetimeJava8ApiEnabled: Boolean = getConf(DATETIME_JAVA8API_ENABLED)

  def uiExplainMode: String = getConf(UI_EXPLAIN_MODE)

  def addSingleFileInAddFile: Boolean = getConf(LEGACY_ADD_SINGLE_FILE_IN_ADD_FILE)

  def legacyMsSqlServerNumericMappingEnabled: Boolean =
    getConf(LEGACY_MSSQLSERVER_NUMERIC_MAPPING_ENABLED)

  def legacyMsSqlServerDatetimeOffsetMappingEnabled: Boolean =
    getConf(LEGACY_MSSQLSERVER_DATETIMEOFFSET_MAPPING_ENABLED)

  def legacyMySqlBitArrayMappingEnabled: Boolean =
    getConf(LEGACY_MYSQL_BIT_ARRAY_MAPPING_ENABLED)

  def legacyMySqlTimestampNTZMappingEnabled: Boolean =
    getConf(LEGACY_MYSQL_TIMESTAMPNTZ_MAPPING_ENABLED)

  def legacyOracleTimestampMappingEnabled: Boolean =
    getConf(LEGACY_ORACLE_TIMESTAMP_MAPPING_ENABLED)

  def legacyDB2numericMappingEnabled: Boolean =
    getConf(LEGACY_DB2_TIMESTAMP_MAPPING_ENABLED)

  def legacyDB2BooleanMappingEnabled: Boolean =
    getConf(LEGACY_DB2_BOOLEAN_MAPPING_ENABLED)

  def legacyPostgresDatetimeMappingEnabled: Boolean =
    getConf(LEGACY_POSTGRES_DATETIME_MAPPING_ENABLED)

  override def legacyTimeParserPolicy: LegacyBehaviorPolicy.Value = {
    LegacyBehaviorPolicy.withName(getConf(SQLConf.LEGACY_TIME_PARSER_POLICY))
  }

  def broadcastHashJoinOutputPartitioningExpandLimit: Int =
    getConf(BROADCAST_HASH_JOIN_OUTPUT_PARTITIONING_EXPAND_LIMIT)

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

  /**
   * Returns the error handler for handling hint errors.
   */
  def hintErrorHandler: HintErrorHandler = HintErrorLogger

  def subexpressionEliminationEnabled: Boolean =
    getConf(SUBEXPRESSION_ELIMINATION_ENABLED)

  def subexpressionEliminationCacheMaxEntries: Int =
    getConf(SUBEXPRESSION_ELIMINATION_CACHE_MAX_ENTRIES)

  def subexpressionEliminationSkipForShotcutExpr: Boolean =
    getConf(SUBEXPRESSION_ELIMINATION_SKIP_FOR_SHORTCUT_EXPR)

  def autoBroadcastJoinThreshold: Long = getConf(AUTO_BROADCASTJOIN_THRESHOLD)

  def limitInitialNumPartitions: Int = getConf(LIMIT_INITIAL_NUM_PARTITIONS)

  def limitScaleUpFactor: Int = getConf(LIMIT_SCALE_UP_FACTOR)

  def advancedPartitionPredicatePushdownEnabled: Boolean =
    getConf(ADVANCED_PARTITION_PREDICATE_PUSHDOWN)

  def preferSortMergeJoin: Boolean = getConf(PREFER_SORTMERGEJOIN)

  def enableRadixSort: Boolean = getConf(RADIX_SORT_ENABLED)

  def isParquetSchemaMergingEnabled: Boolean = getConf(PARQUET_SCHEMA_MERGING_ENABLED)

  def isParquetSchemaRespectSummaries: Boolean = getConf(PARQUET_SCHEMA_RESPECT_SUMMARIES)

  def isParquetBinaryAsString: Boolean = getConf(PARQUET_BINARY_AS_STRING)

  def isParquetINT96AsTimestamp: Boolean = getConf(PARQUET_INT96_AS_TIMESTAMP)

  def isParquetINT96TimestampConversion: Boolean = getConf(PARQUET_INT96_TIMESTAMP_CONVERSION)

  def parquetOutputTimestampType: ParquetOutputTimestampType.Value = {
    ParquetOutputTimestampType.withName(getConf(PARQUET_OUTPUT_TIMESTAMP_TYPE))
  }

  def writeLegacyParquetFormat: Boolean = getConf(PARQUET_WRITE_LEGACY_FORMAT)

  def parquetRecordFilterEnabled: Boolean = getConf(PARQUET_RECORD_FILTER_ENABLED)

  def inMemoryPartitionPruning: Boolean = getConf(IN_MEMORY_PARTITION_PRUNING)

  def inMemoryTableScanStatisticsEnabled: Boolean = getConf(IN_MEMORY_TABLE_SCAN_STATISTICS_ENABLED)

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

  def autoBucketedScanEnabled: Boolean = getConf(SQLConf.AUTO_BUCKETED_SCAN_ENABLED)

  def v2BucketingEnabled: Boolean = getConf(SQLConf.V2_BUCKETING_ENABLED)

  def v2BucketingPushPartValuesEnabled: Boolean =
    getConf(SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED)

  def v2BucketingPartiallyClusteredDistributionEnabled: Boolean =
    getConf(SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED)

  def v2BucketingShuffleEnabled: Boolean =
    getConf(SQLConf.V2_BUCKETING_SHUFFLE_ENABLED)

  def v2BucketingAllowJoinKeysSubsetOfPartitionKeys: Boolean =
    getConf(SQLConf.V2_BUCKETING_ALLOW_JOIN_KEYS_SUBSET_OF_PARTITION_KEYS)

  def v2BucketingAllowCompatibleTransforms: Boolean =
    getConf(SQLConf.V2_BUCKETING_ALLOW_COMPATIBLE_TRANSFORMS)

  def dataFrameSelfJoinAutoResolveAmbiguity: Boolean =
    getConf(DATAFRAME_SELF_JOIN_AUTO_RESOLVE_AMBIGUITY)

  def dataFrameRetainGroupColumns: Boolean = getConf(DATAFRAME_RETAIN_GROUP_COLUMNS)

  def dataFramePivotMaxValues: Int = getConf(DATAFRAME_PIVOT_MAX_VALUES)

  def dataFrameTransposeMaxValues: Int = getConf(DATAFRAME_TRANSPOSE_MAX_VALUES)

  def runSQLonFile: Boolean = getConf(RUN_SQL_ON_FILES)

  def enableTwoLevelAggMap: Boolean = getConf(ENABLE_TWOLEVEL_AGG_MAP)

  def enableVectorizedHashMap: Boolean = getConf(ENABLE_VECTORIZED_HASH_MAP)

  def useObjectHashAggregation: Boolean = getConf(USE_OBJECT_HASH_AGG)

  def objectAggSortBasedFallbackThreshold: Int = getConf(OBJECT_AGG_SORT_BASED_FALLBACK_THRESHOLD)

  def variableSubstituteEnabled: Boolean = getConf(VARIABLE_SUBSTITUTE_ENABLED)

  def warehousePath: String = new Path(getConf(StaticSQLConf.WAREHOUSE_PATH)).toString

  def hiveThriftServerSingleSession: Boolean =
    getConf(StaticSQLConf.HIVE_THRIFT_SERVER_SINGLESESSION)

  def orderByOrdinal: Boolean = getConf(ORDER_BY_ORDINAL)

  def groupByOrdinal: Boolean = getConf(GROUP_BY_ORDINAL)

  def groupByAliases: Boolean = getConf(GROUP_BY_ALIASES)

  def viewSchemaBindingEnabled: Boolean = getConf(VIEW_SCHEMA_BINDING_ENABLED)

  def viewSchemaCompensation: Boolean = getConf(VIEW_SCHEMA_COMPENSATION)

  def defaultCacheStorageLevel: StorageLevel =
    StorageLevel.fromString(getConf(DEFAULT_CACHE_STORAGE_LEVEL))

  def dataframeCacheLogLevel: String = getConf(DATAFRAME_CACHE_LOG_LEVEL)

  def crossJoinEnabled: Boolean = getConf(SQLConf.CROSS_JOINS_ENABLED)

  override def sessionLocalTimeZone: String = getConf(SQLConf.SESSION_LOCAL_TIMEZONE)

  def jsonGeneratorIgnoreNullFields: Boolean = getConf(SQLConf.JSON_GENERATOR_IGNORE_NULL_FIELDS)

  def jsonExpressionOptimization: Boolean = getConf(SQLConf.JSON_EXPRESSION_OPTIMIZATION)

  def csvExpressionOptimization: Boolean = getConf(SQLConf.CSV_EXPRESSION_OPTIMIZATION)

  def parallelFileListingInStatsComputation: Boolean =
    getConf(SQLConf.PARALLEL_FILE_LISTING_IN_STATS_COMPUTATION)

  def fallBackToHdfsForStatsEnabled: Boolean = getConf(ENABLE_FALL_BACK_TO_HDFS_FOR_STATS)

  def defaultSizeInBytes: Long = getConf(DEFAULT_SIZE_IN_BYTES)

  def ndvMaxError: Double = getConf(NDV_MAX_ERROR)

  def histogramEnabled: Boolean = getConf(HISTOGRAM_ENABLED)

  def histogramNumBins: Int = getConf(HISTOGRAM_NUM_BINS)

  def percentileAccuracy: Int = getConf(PERCENTILE_ACCURACY)

  def cboEnabled: Boolean = getConf(SQLConf.CBO_ENABLED)

  def planStatsEnabled: Boolean = getConf(SQLConf.PLAN_STATS_ENABLED)

  def autoSizeUpdateEnabled: Boolean = getConf(SQLConf.AUTO_SIZE_UPDATE_ENABLED)

  def updatePartStatsInAnalyzeTableEnabled: Boolean =
    getConf(SQLConf.UPDATE_PART_STATS_IN_ANALYZE_TABLE_ENABLED)

  def joinReorderEnabled: Boolean = getConf(SQLConf.JOIN_REORDER_ENABLED)

  def joinReorderDPThreshold: Int = getConf(SQLConf.JOIN_REORDER_DP_THRESHOLD)

  def joinReorderCardWeight: Double = getConf(SQLConf.JOIN_REORDER_CARD_WEIGHT)

  def joinReorderDPStarFilter: Boolean = getConf(SQLConf.JOIN_REORDER_DP_STAR_FILTER)

  def windowExecBufferInMemoryThreshold: Int = getConf(WINDOW_EXEC_BUFFER_IN_MEMORY_THRESHOLD)

  def windowExecBufferSpillThreshold: Int = getConf(WINDOW_EXEC_BUFFER_SPILL_THRESHOLD)

  def windowGroupLimitThreshold: Int = getConf(WINDOW_GROUP_LIMIT_THRESHOLD)

  def sessionWindowBufferInMemoryThreshold: Int = getConf(SESSION_WINDOW_BUFFER_IN_MEMORY_THRESHOLD)

  def sessionWindowBufferSpillThreshold: Int = getConf(SESSION_WINDOW_BUFFER_SPILL_THRESHOLD)

  def sortMergeJoinExecBufferInMemoryThreshold: Int =
    getConf(SORT_MERGE_JOIN_EXEC_BUFFER_IN_MEMORY_THRESHOLD)

  def sortMergeJoinExecBufferSpillThreshold: Int =
    getConf(SORT_MERGE_JOIN_EXEC_BUFFER_SPILL_THRESHOLD)

  def cartesianProductExecBufferInMemoryThreshold: Int =
    getConf(CARTESIAN_PRODUCT_EXEC_BUFFER_IN_MEMORY_THRESHOLD)

  def cartesianProductExecBufferSpillThreshold: Int =
    getConf(CARTESIAN_PRODUCT_EXEC_BUFFER_SPILL_THRESHOLD)

  def codegenSplitAggregateFunc: Boolean = getConf(SQLConf.CODEGEN_SPLIT_AGGREGATE_FUNC)

  def maxNestedViewDepth: Int = getConf(SQLConf.MAX_NESTED_VIEW_DEPTH)

  def useCurrentSQLConfigsForView: Boolean = getConf(SQLConf.USE_CURRENT_SQL_CONFIGS_FOR_VIEW)

  def storeAnalyzedPlanForView: Boolean = getConf(SQLConf.STORE_ANALYZED_PLAN_FOR_VIEW)

  def allowAutoGeneratedAliasForView: Boolean = getConf(SQLConf.ALLOW_AUTO_GENERATED_ALIAS_FOR_VEW)

  def allowStarWithSingleTableIdentifierInCount: Boolean =
    getConf(SQLConf.ALLOW_STAR_WITH_SINGLE_TABLE_IDENTIFIER_IN_COUNT)

  def allowNonEmptyLocationInCTAS: Boolean =
    getConf(SQLConf.ALLOW_NON_EMPTY_LOCATION_IN_CTAS)

  def starSchemaDetection: Boolean = getConf(STARSCHEMA_DETECTION)

  def starSchemaFTRatio: Double = getConf(STARSCHEMA_FACT_TABLE_RATIO)

  def supportQuotedRegexColumnName: Boolean = getConf(SUPPORT_QUOTED_REGEX_COLUMN_NAME)

  def tvfAllowMultipleTableArguments: Boolean = getConf(TVF_ALLOW_MULTIPLE_TABLE_ARGUMENTS_ENABLED)

  def rangeExchangeSampleSizePerPartition: Int = getConf(RANGE_EXCHANGE_SAMPLE_SIZE_PER_PARTITION)

  def arrowPySparkEnabled: Boolean = getConf(ARROW_PYSPARK_EXECUTION_ENABLED)

  def arrowLocalRelationThreshold: Long = getConf(ARROW_LOCAL_RELATION_THRESHOLD)

  def arrowPySparkSelfDestructEnabled: Boolean = getConf(ARROW_PYSPARK_SELF_DESTRUCT_ENABLED)

  def pysparkJVMStacktraceEnabled: Boolean = getConf(PYSPARK_JVM_STACKTRACE_ENABLED)

  def pythonUDFProfiler: Option[String] = getConf(PYTHON_UDF_PROFILER)

  def pythonUDFWorkerFaulthandlerEnabled: Boolean = getConf(PYTHON_UDF_WORKER_FAULTHANLDER_ENABLED)

  def arrowSparkREnabled: Boolean = getConf(ARROW_SPARKR_EXECUTION_ENABLED)

  def arrowPySparkFallbackEnabled: Boolean = getConf(ARROW_PYSPARK_FALLBACK_ENABLED)

  def arrowMaxRecordsPerBatch: Int = getConf(ARROW_EXECUTION_MAX_RECORDS_PER_BATCH)

  def arrowUseLargeVarTypes: Boolean = getConf(ARROW_EXECUTION_USE_LARGE_VAR_TYPES)

  def pandasUDFBufferSize: Int = getConf(PANDAS_UDF_BUFFER_SIZE)

  def pandasStructHandlingMode: String = getConf(PANDAS_STRUCT_HANDLING_MODE)

  def pysparkSimplifiedTraceback: Boolean = getConf(PYSPARK_SIMPLIFIED_TRACEBACK)

  def pandasGroupedMapAssignColumnsByName: Boolean =
    getConf(SQLConf.PANDAS_GROUPED_MAP_ASSIGN_COLUMNS_BY_NAME)

  def arrowSafeTypeConversion: Boolean = getConf(SQLConf.PANDAS_ARROW_SAFE_TYPE_CONVERSION)

  def pysparkWorkerPythonExecutable: Option[String] =
    getConf(SQLConf.PYSPARK_WORKER_PYTHON_EXECUTABLE)

  def pythonPlannerExecMemory: Option[Long] = getConf(PYTHON_PLANNER_EXEC_MEMORY)

  def replaceExceptWithFilter: Boolean = getConf(REPLACE_EXCEPT_WITH_FILTER)

  def decimalOperationsAllowPrecisionLoss: Boolean = getConf(DECIMAL_OPERATIONS_ALLOW_PREC_LOSS)

  def literalPickMinimumPrecision: Boolean = getConf(LITERAL_PICK_MINIMUM_PRECISION)

  def continuousStreamingEpochBacklogQueueSize: Int =
    getConf(CONTINUOUS_STREAMING_EPOCH_BACKLOG_QUEUE_SIZE)

  def continuousStreamingExecutorQueueSize: Int = getConf(CONTINUOUS_STREAMING_EXECUTOR_QUEUE_SIZE)

  def continuousStreamingExecutorPollIntervalMs: Long =
    getConf(CONTINUOUS_STREAMING_EXECUTOR_POLL_INTERVAL_MS)

  def disabledV2StreamingWriters: String = getConf(DISABLED_V2_STREAMING_WRITERS)

  def disabledV2StreamingMicroBatchReaders: String =
    getConf(DISABLED_V2_STREAMING_MICROBATCH_READERS)

  def fastFailFileFormatOutput: Boolean = getConf(FASTFAIL_ON_FILEFORMAT_OUTPUT)

  def concatBinaryAsString: Boolean = getConf(CONCAT_BINARY_AS_STRING)

  def eltOutputAsString: Boolean = getConf(ELT_OUTPUT_AS_STRING)

  def validatePartitionColumns: Boolean = getConf(VALIDATE_PARTITION_COLUMNS)

  def partitionOverwriteMode: PartitionOverwriteMode.Value =
    PartitionOverwriteMode.withName(getConf(PARTITION_OVERWRITE_MODE))

  def storeAssignmentPolicy: StoreAssignmentPolicy.Value =
    StoreAssignmentPolicy.withName(getConf(STORE_ASSIGNMENT_POLICY))

  override def ansiEnabled: Boolean = getConf(ANSI_ENABLED)

  def enableDefaultColumns: Boolean = getConf(SQLConf.ENABLE_DEFAULT_COLUMNS)

  def defaultColumnAllowedProviders: String = getConf(SQLConf.DEFAULT_COLUMN_ALLOWED_PROVIDERS)

  def jsonWriteNullIfWithDefaultValue: Boolean =
    getConf(JSON_GENERATOR_WRITE_NULL_IF_WITH_DEFAULT_VALUE)

  def useNullsForMissingDefaultColumnValues: Boolean =
    getConf(SQLConf.USE_NULLS_FOR_MISSING_DEFAULT_COLUMN_VALUES)

  override def enforceReservedKeywords: Boolean = ansiEnabled && getConf(ENFORCE_RESERVED_KEYWORDS)

  override def doubleQuotedIdentifiers: Boolean = ansiEnabled && getConf(DOUBLE_QUOTED_IDENTIFIERS)

  def ansiRelationPrecedence: Boolean = ansiEnabled && getConf(ANSI_RELATION_PRECEDENCE)

  def chunkBase64StringEnabled: Boolean = getConf(CHUNK_BASE64_STRING_ENABLED)

  def timestampType: AtomicType = getConf(TIMESTAMP_TYPE) match {
    case "TIMESTAMP_LTZ" =>
      // For historical reason, the TimestampType maps to TIMESTAMP WITH LOCAL TIME ZONE
      TimestampType

    case "TIMESTAMP_NTZ" =>
      TimestampNTZType
  }

  def nestedSchemaPruningEnabled: Boolean = getConf(NESTED_SCHEMA_PRUNING_ENABLED)

  def serializerNestedSchemaPruningEnabled: Boolean =
    getConf(SERIALIZER_NESTED_SCHEMA_PRUNING_ENABLED)

  def nestedPruningOnExpressions: Boolean = getConf(NESTED_PRUNING_ON_EXPRESSIONS)

  def csvColumnPruning: Boolean = getConf(SQLConf.CSV_PARSER_COLUMN_PRUNING)

  def legacySizeOfNull: Boolean = {
    // size(null) should return null under ansi mode.
    getConf(SQLConf.LEGACY_SIZE_OF_NULL) && !getConf(ANSI_ENABLED)
  }

  def legacyNullInEmptyBehavior: Boolean = {
    getConf(SQLConf.LEGACY_NULL_IN_EMPTY_LIST_BEHAVIOR).getOrElse(!ansiEnabled)
  }

  def isReplEagerEvalEnabled: Boolean = getConf(SQLConf.REPL_EAGER_EVAL_ENABLED)

  def replEagerEvalMaxNumRows: Int = getConf(SQLConf.REPL_EAGER_EVAL_MAX_NUM_ROWS)

  def replEagerEvalTruncate: Int = getConf(SQLConf.REPL_EAGER_EVAL_TRUNCATE)

  def avroCompressionCodec: String = getConf(SQLConf.AVRO_COMPRESSION_CODEC)

  def replaceDatabricksSparkAvroEnabled: Boolean =
    getConf(SQLConf.LEGACY_REPLACE_DATABRICKS_SPARK_AVRO_ENABLED)

  override def setOpsPrecedenceEnforced: Boolean =
    getConf(SQLConf.LEGACY_SETOPS_PRECEDENCE_ENABLED)

  override def exponentLiteralAsDecimalEnabled: Boolean =
    getConf(SQLConf.LEGACY_EXPONENT_LITERAL_AS_DECIMAL_ENABLED)

  def allowNegativeScaleOfDecimalEnabled: Boolean =
    getConf(SQLConf.LEGACY_ALLOW_NEGATIVE_SCALE_OF_DECIMAL_ENABLED)

  def legacyStatisticalAggregate: Boolean = getConf(SQLConf.LEGACY_STATISTICAL_AGGREGATE)

  def truncateTableIgnorePermissionAcl: Boolean =
    getConf(SQLConf.TRUNCATE_TABLE_IGNORE_PERMISSION_ACL)

  def nameNonStructGroupingKeyAsValue: Boolean =
    getConf(SQLConf.NAME_NON_STRUCT_GROUPING_KEY_AS_VALUE)

  override def maxToStringFields: Int = getConf(SQLConf.MAX_TO_STRING_FIELDS)

  def maxPlanStringLength: Int = getConf(SQLConf.MAX_PLAN_STRING_LENGTH).toInt

  def maxMetadataStringLength: Int = getConf(SQLConf.MAX_METADATA_STRING_LENGTH)

  def setCommandRejectsSparkCoreConfs: Boolean =
    getConf(SQLConf.SET_COMMAND_REJECTS_SPARK_CORE_CONFS)

  def castDatetimeToString: Boolean = getConf(SQLConf.LEGACY_CAST_DATETIME_TO_STRING)

  def ignoreDataLocality: Boolean = getConf(SQLConf.IGNORE_DATA_LOCALITY)

  def useListFilesFileSystemList: String = getConf(SQLConf.USE_LISTFILES_FILESYSTEM_LIST)

  def csvFilterPushDown: Boolean = getConf(CSV_FILTER_PUSHDOWN_ENABLED)

  def jsonFilterPushDown: Boolean = getConf(JSON_FILTER_PUSHDOWN_ENABLED)

  def avroFilterPushDown: Boolean = getConf(AVRO_FILTER_PUSHDOWN_ENABLED)

  def jsonEnablePartialResults: Boolean = getConf(JSON_ENABLE_PARTIAL_RESULTS)

  def jsonEnableDateTimeParsingFallback: Option[Boolean] =
    getConf(LEGACY_JSON_ENABLE_DATE_TIME_PARSING_FALLBACK)

  def csvEnableDateTimeParsingFallback: Option[Boolean] =
    getConf(LEGACY_CSV_ENABLE_DATE_TIME_PARSING_FALLBACK)

  def integerGroupingIdEnabled: Boolean = getConf(SQLConf.LEGACY_INTEGER_GROUPING_ID)

  def groupingIdWithAppendedUserGroupByEnabled: Boolean =
    getConf(SQLConf.LEGACY_GROUPING_ID_WITH_APPENDED_USER_GROUPBY)

  def metadataCacheTTL: Long = getConf(StaticSQLConf.METADATA_CACHE_TTL_SECONDS)

  def coalesceBucketsInJoinEnabled: Boolean = getConf(SQLConf.COALESCE_BUCKETS_IN_JOIN_ENABLED)

  def coalesceBucketsInJoinMaxBucketRatio: Int =
    getConf(SQLConf.COALESCE_BUCKETS_IN_JOIN_MAX_BUCKET_RATIO)

  def optimizeNullAwareAntiJoin: Boolean =
    getConf(SQLConf.OPTIMIZE_NULL_AWARE_ANTI_JOIN)

  def legacyDuplicateBetweenInput: Boolean =
    getConf(SQLConf.LEGACY_DUPLICATE_BETWEEN_INPUT)

  def legacyPathOptionBehavior: Boolean = getConf(SQLConf.LEGACY_PATH_OPTION_BEHAVIOR)

  def supportSecondOffsetFormat: Boolean = getConf(SQLConf.SUPPORT_SECOND_OFFSET_FORMAT)

  def disabledJdbcConnectionProviders: String = getConf(
    StaticSQLConf.DISABLED_JDBC_CONN_PROVIDER_LIST)

  def charVarcharAsString: Boolean = getConf(SQLConf.LEGACY_CHAR_VARCHAR_AS_STRING)

  def readSideCharPadding: Boolean = getConf(SQLConf.READ_SIDE_CHAR_PADDING)

  def cliPrintHeader: Boolean = getConf(SQLConf.CLI_PRINT_HEADER)

  def legacyIntervalEnabled: Boolean = getConf(LEGACY_INTERVAL_ENABLED)

  def decorrelateInnerQueryEnabled: Boolean = getConf(SQLConf.DECORRELATE_INNER_QUERY_ENABLED)

  def decorrelateInnerQueryEnabledForExistsIn: Boolean =
    !getConf(SQLConf.DECORRELATE_EXISTS_IN_SUBQUERY_LEGACY_INCORRECT_COUNT_HANDLING_ENABLED)

  def maxConcurrentOutputFileWriters: Int = getConf(SQLConf.MAX_CONCURRENT_OUTPUT_FILE_WRITERS)

  def plannedWriteEnabled: Boolean = getConf(SQLConf.PLANNED_WRITE_ENABLED)

  def inferDictAsStruct: Boolean = getConf(SQLConf.INFER_NESTED_DICT_AS_STRUCT)

  def inferPandasDictAsMap: Boolean = getConf(SQLConf.INFER_PANDAS_DICT_AS_MAP)

  def legacyInferArrayTypeFromFirstElement: Boolean = getConf(
    SQLConf.LEGACY_INFER_ARRAY_TYPE_FROM_FIRST_ELEMENT)

  def legacyInferMapStructTypeFromFirstItem: Boolean = getConf(
    SQLConf.LEGACY_INFER_MAP_STRUCT_TYPE_FROM_FIRST_ITEM)

  def parquetFieldIdReadEnabled: Boolean = getConf(SQLConf.PARQUET_FIELD_ID_READ_ENABLED)

  def parquetFieldIdWriteEnabled: Boolean = getConf(SQLConf.PARQUET_FIELD_ID_WRITE_ENABLED)

  def ignoreMissingParquetFieldId: Boolean = getConf(SQLConf.IGNORE_MISSING_PARQUET_FIELD_ID)

  def legacyParquetNanosAsLong: Boolean = getConf(SQLConf.LEGACY_PARQUET_NANOS_AS_LONG)

  def parquetInferTimestampNTZEnabled: Boolean = getConf(PARQUET_INFER_TIMESTAMP_NTZ_ENABLED)

  def useV1Command: Boolean = getConf(SQLConf.LEGACY_USE_V1_COMMAND)

  def histogramNumericPropagateInputType: Boolean =
    getConf(SQLConf.HISTOGRAM_NUMERIC_PROPAGATE_INPUT_TYPE)

  def errorMessageFormat: ErrorMessageFormat.Value =
    ErrorMessageFormat.withName(getConf(SQLConf.ERROR_MESSAGE_FORMAT))

  def defaultDatabase: String = getConf(StaticSQLConf.CATALOG_DEFAULT_DATABASE)

  def globalTempDatabase: String = getConf(StaticSQLConf.GLOBAL_TEMP_DATABASE)

  def allowsTempViewCreationWithMultipleNameparts: Boolean =
    getConf(SQLConf.ALLOW_TEMP_VIEW_CREATION_WITH_MULTIPLE_NAME_PARTS)

  def usePartitionEvaluator: Boolean = getConf(SQLConf.USE_PARTITION_EVALUATOR)

  def legacyNegativeIndexInArrayInsert: Boolean = {
    getConf(SQLConf.LEGACY_NEGATIVE_INDEX_IN_ARRAY_INSERT)
  }

  def legacyRaiseErrorWithoutErrorClass: Boolean =
    getConf(SQLConf.LEGACY_RAISE_ERROR_WITHOUT_ERROR_CLASS)

  override def stackTracesInDataFrameContext: Int =
    getConf(SQLConf.STACK_TRACES_IN_DATAFRAME_CONTEXT)

  override def legacyAllowUntypedScalaUDFs: Boolean =
    getConf(SQLConf.LEGACY_ALLOW_UNTYPED_SCALA_UDF)

  def legacyJavaCharsets: Boolean = getConf(SQLConf.LEGACY_JAVA_CHARSETS)

  def legacyCodingErrorAction: Boolean = getConf(SQLConf.LEGACY_CODING_ERROR_ACTION)

  def legacyEvalCurrentTime: Boolean = getConf(SQLConf.LEGACY_EVAL_CURRENT_TIME)

  /** ********************** SQLConf functionality methods ************ */

  /** Set Spark SQL configuration properties. */
  def setConf(props: Properties): Unit = settings.synchronized {
    props.asScala.foreach { case (k, v) => setConfString(k, v) }
  }

  /** Set the given Spark SQL configuration property using a `string` value. */
  def setConfString(key: String, value: String): Unit = {
    require(key != null, "key cannot be null")
    require(value != null, s"value cannot be null for key: $key")
    val entry = getConfigEntry(key)
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
    require(containsConfigEntry(entry), s"$entry is not registered")
    setConfWithCheck(entry.key, entry.stringConverter(value))
  }

  /** Return the value of Spark SQL configuration property for the given key. */
  @throws[NoSuchElementException]("if key is not set")
  def getConfString(key: String): String = {
    Option(settings.get(key)).
      orElse {
        // Try to use the default value
        Option(getConfigEntry(key)).map { e => e.stringConverter(e.readFrom(reader)) }
      }.
      getOrElse(throw QueryExecutionErrors.sqlConfigNotFoundError(key))
  }

  /**
   * Return the value of Spark SQL configuration property for the given key. If the key is not set
   * yet, return `defaultValue`. This is useful when `defaultValue` in ConfigEntry is not the
   * desired one.
   */
  def getConf[T](entry: ConfigEntry[T], defaultValue: T): T = {
    require(containsConfigEntry(entry), s"$entry is not registered")
    Option(settings.get(entry.key)).map(entry.valueConverter).getOrElse(defaultValue)
  }

  /**
   * Return the value of Spark SQL configuration property for the given key. If the key is not set
   * yet, return `defaultValue` in [[ConfigEntry]].
   */
  def getConf[T](entry: ConfigEntry[T]): T = {
    require(containsConfigEntry(entry), s"$entry is not registered")
    entry.readFrom(reader)
  }

  /**
   * Return the value of an optional Spark SQL configuration property for the given key. If the key
   * is not set yet, returns None.
   */
  def getConf[T](entry: OptionalConfigEntry[T]): Option[T] = {
    require(containsConfigEntry(entry), s"$entry is not registered")
    entry.readFrom(reader)
  }

  /**
   * Return the `string` value of Spark SQL configuration property for the given key. If the key is
   * not set yet, return `defaultValue`.
   */
  def getConfString(key: String, defaultValue: String): String = {
    Option(settings.get(key)).getOrElse {
      // If the key is not set, need to check whether the config entry is registered and is
      // a fallback conf, so that we can check its parent.
      getConfigEntry(key) match {
        case e: FallbackConfigEntry[_] =>
          getConfString(e.fallback.key, defaultValue)
        case e: ConfigEntry[_] if defaultValue != null && defaultValue != ConfigEntry.UNDEFINED =>
          // Only verify configs in the SQLConf object
          e.valueConverter(defaultValue)
          defaultValue
        case _ =>
          defaultValue
      }
    }
  }

  private var definedConfsLoaded = false
  /**
   * Init [[StaticSQLConf]] and [[org.apache.spark.sql.hive.HiveUtils]] so that all the defined
   * SQL Configurations will be registered to SQLConf
   */
  private def loadDefinedConfs(): Unit = {
    if (!definedConfsLoaded) {
      definedConfsLoaded = true
      // Force to register static SQL configurations
      StaticSQLConf
      try {
        // Force to register SQL configurations from Hive module
        val symbol = ScalaReflection.mirror.staticModule("org.apache.spark.sql.hive.HiveUtils")
        ScalaReflection.mirror.reflectModule(symbol).instance
      } catch {
        case NonFatal(e) =>
          logWarning("SQL configurations from Hive module is not loaded", e)
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
  def getAllDefinedConfs: Seq[(String, String, String, String)] = {
    loadDefinedConfs()
    getConfigEntries().asScala.filter(_.isPublic).map { entry =>
      val displayValue =
        // We get the display value in this way rather than call getConfString(entry.key)
        // because we want the default _definition_ and not the computed value.
        //   e.g. `<undefined>` instead of `null`
        //   e.g. `<value of spark.buffer.size>` instead of `65536`
        Option(getConfString(entry.key, null)).getOrElse(entry.defaultValueString)
      (entry.key, displayValue, entry.doc, entry.version)
    }.toSeq
  }

  /**
   * Redacts the given option map according to the description of SQL_OPTIONS_REDACTION_PATTERN.
   */
  def redactOptions[K, V](options: Map[K, V]): Map[K, V] = {
    redactOptions(options.toSeq).toMap
  }

  /**
   * Redacts the given option map according to the description of SQL_OPTIONS_REDACTION_PATTERN.
   */
  def redactOptions[K, V](options: collection.Seq[(K, V)]): collection.Seq[(K, V)] = {
    val regexes = Seq(
      getConf(SQL_OPTIONS_REDACTION_PATTERN),
      SECRET_REDACTION_PATTERN.readFrom(reader))

    regexes.foldLeft(options) { case (opts, r) => Utils.redact(Some(r), opts) }
  }

  /**
   * Return whether a given key is set in this [[SQLConf]].
   */
  def contains(key: String): Boolean = {
    settings.containsKey(key)
  }

  /**
   * Logs a warning message if the given config key is deprecated.
   */
  private def logDeprecationWarning(key: String): Unit = {
    SQLConf.deprecatedSQLConfigs.get(key).foreach { config =>
      logWarning(config.toDeprecationString)
    }
  }

  private def requireDefaultValueOfRemovedConf(key: String, value: String): Unit = {
    SQLConf.removedSQLConfigs.get(key).foreach {
      case RemovedConfig(configName, version, defaultValue, comment) =>
        if (value != defaultValue) {
          throw QueryCompilationErrors.configRemovedInVersionError(configName, version, comment)
        }
    }
  }

  protected def setConfWithCheck(key: String, value: String): Unit = {
    logDeprecationWarning(key)
    requireDefaultValueOfRemovedConf(key, value)
    settings.put(key, value)
  }

  def unsetConf(key: String): Unit = {
    logDeprecationWarning(key)
    settings.remove(key)
  }

  def unsetConf(entry: ConfigEntry[_]): Unit = {
    unsetConf(entry.key)
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
    containsConfigKey(key) && !isStaticConfigKey(key)
  }
}
