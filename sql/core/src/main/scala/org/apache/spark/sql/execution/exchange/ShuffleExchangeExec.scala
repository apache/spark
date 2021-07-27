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

package org.apache.spark.sql.execution.exchange

import java.util.Random
import java.util.function.Supplier

import scala.concurrent.Future

import org.apache.spark._
import org.apache.spark.internal.config
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.{ShuffleWriteMetricsReporter, ShuffleWriteProcessor}
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, BoundReference, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.LazilyGeneratedOrdering
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics, SQLShuffleReadMetricsReporter, SQLShuffleWriteMetricsReporter}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.MutablePair
import org.apache.spark.util.collection.unsafe.sort.{PrefixComparators, RecordComparator}

/**
 * Common trait for all shuffle exchange implementations to facilitate pattern matching.
 */
trait ShuffleExchangeLike extends Exchange {

  /**
   * Returns the number of mappers of this shuffle.
   */
  def numMappers: Int

  /**
   * Returns the shuffle partition number.
   */
  def numPartitions: Int

  /**
   * The origin of this shuffle operator.
   */
  def shuffleOrigin: ShuffleOrigin

  /**
   * The asynchronous job that materializes the shuffle. It also does the preparations work,
   * such as waiting for the subqueries.
   */
  final def submitShuffleJob: Future[MapOutputStatistics] = executeQuery {
    mapOutputStatisticsFuture
  }

  protected def mapOutputStatisticsFuture: Future[MapOutputStatistics]

  /**
   * Returns the shuffle RDD with specified partition specs.
   */
  def getShuffleRDD(partitionSpecs: Array[ShufflePartitionSpec]): RDD[_]

  /**
   * Returns the runtime statistics after shuffle materialization.
   */
  def runtimeStatistics: Statistics
}

// Describes where the shuffle operator comes from.
sealed trait ShuffleOrigin

// Indicates that the shuffle operator was added by the internal `EnsureRequirements` rule. It
// means that the shuffle operator is used to ensure internal data partitioning requirements and
// Spark is free to optimize it as long as the requirements are still ensured.
case object ENSURE_REQUIREMENTS extends ShuffleOrigin

// Indicates that the shuffle operator was added by the user-specified repartition operator. Spark
// can still optimize it via changing shuffle partition number, as data partitioning won't change.
case object REPARTITION_BY_COL extends ShuffleOrigin

// Indicates that the shuffle operator was added by the user-specified repartition operator with
// a certain partition number. Spark can't optimize it.
case object REPARTITION_BY_NUM extends ShuffleOrigin

// Indicates that the shuffle operator was added by the user-specified rebalance operator.
// Spark will try to rebalance partitions that make per-partition size not too small and not
// too big. Local shuffle read will be used if possible to reduce network traffic.
case object REBALANCE_PARTITIONS_BY_NONE extends ShuffleOrigin

// Indicates that the shuffle operator was added by the user-specified rebalance operator with
// columns. Spark will try to rebalance partitions that make per-partition size not too small and
// not too big.
// Different from `REBALANCE_PARTITIONS_BY_NONE`, local shuffle read cannot be used for it as
// the output needs to be partitioned by the given columns.
case object REBALANCE_PARTITIONS_BY_COL extends ShuffleOrigin

/**
 * Performs a shuffle that will result in the desired partitioning.
 */
case class ShuffleExchangeExec(
    override val outputPartitioning: Partitioning,
    child: SparkPlan,
    shuffleOrigin: ShuffleOrigin = ENSURE_REQUIREMENTS)
  extends ShuffleExchangeLike {

  private lazy val writeMetrics =
    SQLShuffleWriteMetricsReporter.createShuffleWriteMetrics(sparkContext)
  private[sql] lazy val readMetrics =
    SQLShuffleReadMetricsReporter.createShuffleReadMetrics(sparkContext)
  override lazy val metrics = Map(
    "dataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size")
  ) ++ readMetrics ++ writeMetrics

  override def nodeName: String = "Exchange"

  private lazy val serializer: Serializer =
    new UnsafeRowSerializer(child.output.size, longMetric("dataSize"))

  @transient lazy val inputRDD: RDD[InternalRow] = child.execute()

  // 'mapOutputStatisticsFuture' is only needed when enable AQE.
  @transient
  override lazy val mapOutputStatisticsFuture: Future[MapOutputStatistics] = {
    if (inputRDD.getNumPartitions == 0) {
      Future.successful(null)
    } else {
      sparkContext.submitMapStage(shuffleDependency)
    }
  }

  override def numMappers: Int = shuffleDependency.rdd.getNumPartitions

  override def numPartitions: Int = shuffleDependency.partitioner.numPartitions

  override def getShuffleRDD(partitionSpecs: Array[ShufflePartitionSpec]): RDD[InternalRow] = {
    new ShuffledRowRDD(shuffleDependency, readMetrics, partitionSpecs)
  }

  override def runtimeStatistics: Statistics = {
    val dataSize = metrics("dataSize").value
    val rowCount = metrics(SQLShuffleWriteMetricsReporter.SHUFFLE_RECORDS_WRITTEN).value
    Statistics(dataSize, Some(rowCount))
  }

  /**
   * A [[ShuffleDependency]] that will partition rows of its child based on
   * the partitioning scheme defined in `newPartitioning`. Those partitions of
   * the returned ShuffleDependency will be the input of shuffle.
   */
  @transient
  lazy val shuffleDependency : ShuffleDependency[Int, InternalRow, InternalRow] = {
    ShuffleExchangeExec.prepareShuffleDependency(
      inputRDD,
      child.output,
      outputPartitioning,
      serializer,
      writeMetrics)
  }

  /**
   * Caches the created ShuffleRowRDD so we can reuse that.
   */
  private var cachedShuffleRDD: ShuffledRowRDD = null

  protected override def doExecute(): RDD[InternalRow] = {
    // Returns the same ShuffleRowRDD if this plan is used by multiple plans.
    if (cachedShuffleRDD == null) {
      cachedShuffleRDD = new ShuffledRowRDD(shuffleDependency, readMetrics)
    }
    cachedShuffleRDD
  }

  override protected def withNewChildInternal(newChild: SparkPlan): ShuffleExchangeExec =
    copy(child = newChild)
}

object ShuffleExchangeExec {

  /**
   * Determines whether records must be defensively copied before being sent to the shuffle.
   * Several of Spark's shuffle components will buffer deserialized Java objects in memory. The
   * shuffle code assumes that objects are immutable and hence does not perform its own defensive
   * copying. In Spark SQL, however, operators' iterators return the same mutable `Row` object. In
   * order to properly shuffle the output of these operators, we need to perform our own copying
   * prior to sending records to the shuffle. This copying is expensive, so we try to avoid it
   * whenever possible. This method encapsulates the logic for choosing when to copy.
   *
   * In the long run, we might want to push this logic into core's shuffle APIs so that we don't
   * have to rely on knowledge of core internals here in SQL.
   *
   * See SPARK-2967, SPARK-4479, and SPARK-7375 for more discussion of this issue.
   *
   * @param partitioner the partitioner for the shuffle
   * @return true if rows should be copied before being shuffled, false otherwise
   */
  private def needToCopyObjectsBeforeShuffle(partitioner: Partitioner): Boolean = {
    // Note: even though we only use the partitioner's `numPartitions` field, we require it to be
    // passed instead of directly passing the number of partitions in order to guard against
    // corner-cases where a partitioner constructed with `numPartitions` partitions may output
    // fewer partitions (like RangePartitioner, for example).
    val conf = SparkEnv.get.conf
    val shuffleManager = SparkEnv.get.shuffleManager
    val sortBasedShuffleOn = shuffleManager.isInstanceOf[SortShuffleManager]
    val bypassMergeThreshold = conf.get(config.SHUFFLE_SORT_BYPASS_MERGE_THRESHOLD)
    val numParts = partitioner.numPartitions
    if (sortBasedShuffleOn) {
      if (numParts <= bypassMergeThreshold) {
        // If we're using the original SortShuffleManager and the number of output partitions is
        // sufficiently small, then Spark will fall back to the hash-based shuffle write path, which
        // doesn't buffer deserialized records.
        // Note that we'll have to remove this case if we fix SPARK-6026 and remove this bypass.
        false
      } else if (numParts <= SortShuffleManager.MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE) {
        // SPARK-4550 and  SPARK-7081 extended sort-based shuffle to serialize individual records
        // prior to sorting them. This optimization is only applied in cases where shuffle
        // dependency does not specify an aggregator or ordering and the record serializer has
        // certain properties and the number of partitions doesn't exceed the limitation. If this
        // optimization is enabled, we can safely avoid the copy.
        //
        // Exchange never configures its ShuffledRDDs with aggregators or key orderings, and the
        // serializer in Spark SQL always satisfy the properties, so we only need to check whether
        // the number of partitions exceeds the limitation.
        false
      } else {
        // Spark's SortShuffleManager uses `ExternalSorter` to buffer records in memory, so we must
        // copy.
        true
      }
    } else {
      // Catch-all case to safely handle any future ShuffleManager implementations.
      true
    }
  }

  /**
   * Returns a [[ShuffleDependency]] that will partition rows of its child based on
   * the partitioning scheme defined in `newPartitioning`. Those partitions of
   * the returned ShuffleDependency will be the input of shuffle.
   */
  def prepareShuffleDependency(
      rdd: RDD[InternalRow],
      outputAttributes: Seq[Attribute],
      newPartitioning: Partitioning,
      serializer: Serializer,
      writeMetrics: Map[String, SQLMetric])
    : ShuffleDependency[Int, InternalRow, InternalRow] = {
    val part: Partitioner = newPartitioning match {
      case RoundRobinPartitioning(numPartitions) => new HashPartitioner(numPartitions)
      case HashPartitioning(_, n) =>
        new Partitioner {
          override def numPartitions: Int = n
          // For HashPartitioning, the partitioning key is already a valid partition ID, as we use
          // `HashPartitioning.partitionIdExpression` to produce partitioning key.
          override def getPartition(key: Any): Int = key.asInstanceOf[Int]
        }
      case RangePartitioning(sortingExpressions, numPartitions) =>
        // Extract only fields used for sorting to avoid collecting large fields that does not
        // affect sorting result when deciding partition bounds in RangePartitioner
        val rddForSampling = rdd.mapPartitionsInternal { iter =>
          val projection =
            UnsafeProjection.create(sortingExpressions.map(_.child), outputAttributes)
          val mutablePair = new MutablePair[InternalRow, Null]()
          // Internally, RangePartitioner runs a job on the RDD that samples keys to compute
          // partition bounds. To get accurate samples, we need to copy the mutable keys.
          iter.map(row => mutablePair.update(projection(row).copy(), null))
        }
        // Construct ordering on extracted sort key.
        val orderingAttributes = sortingExpressions.zipWithIndex.map { case (ord, i) =>
          ord.copy(child = BoundReference(i, ord.dataType, ord.nullable))
        }
        implicit val ordering = new LazilyGeneratedOrdering(orderingAttributes)
        new RangePartitioner(
          numPartitions,
          rddForSampling,
          ascending = true,
          samplePointsPerPartitionHint = SQLConf.get.rangeExchangeSampleSizePerPartition)
      case SinglePartition =>
        new Partitioner {
          override def numPartitions: Int = 1
          override def getPartition(key: Any): Int = 0
        }
      case _ => sys.error(s"Exchange not implemented for $newPartitioning")
      // TODO: Handle BroadcastPartitioning.
    }
    def getPartitionKeyExtractor(): InternalRow => Any = newPartitioning match {
      case RoundRobinPartitioning(numPartitions) =>
        // Distributes elements evenly across output partitions, starting from a random partition.
        var position = new Random(TaskContext.get().partitionId()).nextInt(numPartitions)
        (row: InternalRow) => {
          // The HashPartitioner will handle the `mod` by the number of partitions
          position += 1
          position
        }
      case h: HashPartitioning =>
        val projection = UnsafeProjection.create(h.partitionIdExpression :: Nil, outputAttributes)
        row => projection(row).getInt(0)
      case RangePartitioning(sortingExpressions, _) =>
        val projection = UnsafeProjection.create(sortingExpressions.map(_.child), outputAttributes)
        row => projection(row)
      case SinglePartition => identity
      case _ => sys.error(s"Exchange not implemented for $newPartitioning")
    }

    val isRoundRobin = newPartitioning.isInstanceOf[RoundRobinPartitioning] &&
      newPartitioning.numPartitions > 1

    val rddWithPartitionIds: RDD[Product2[Int, InternalRow]] = {
      // [SPARK-23207] Have to make sure the generated RoundRobinPartitioning is deterministic,
      // otherwise a retry task may output different rows and thus lead to data loss.
      //
      // Currently we following the most straight-forward way that perform a local sort before
      // partitioning.
      //
      // Note that we don't perform local sort if the new partitioning has only 1 partition, under
      // that case all output rows go to the same partition.
      val newRdd = if (isRoundRobin && SQLConf.get.sortBeforeRepartition) {
        rdd.mapPartitionsInternal { iter =>
          val recordComparatorSupplier = new Supplier[RecordComparator] {
            override def get: RecordComparator = new RecordBinaryComparator()
          }
          // The comparator for comparing row hashcode, which should always be Integer.
          val prefixComparator = PrefixComparators.LONG

          // The prefix computer generates row hashcode as the prefix, so we may decrease the
          // probability that the prefixes are equal when input rows choose column values from a
          // limited range.
          val prefixComputer = new UnsafeExternalRowSorter.PrefixComputer {
            private val result = new UnsafeExternalRowSorter.PrefixComputer.Prefix
            override def computePrefix(row: InternalRow):
            UnsafeExternalRowSorter.PrefixComputer.Prefix = {
              // The hashcode generated from the binary form of a [[UnsafeRow]] should not be null.
              result.isNull = false
              result.value = row.hashCode()
              result
            }
          }
          val pageSize = SparkEnv.get.memoryManager.pageSizeBytes

          val sorter = UnsafeExternalRowSorter.createWithRecordComparator(
            StructType.fromAttributes(outputAttributes),
            recordComparatorSupplier,
            prefixComparator,
            prefixComputer,
            pageSize,
            // We are comparing binary here, which does not support radix sort.
            // See more details in SPARK-28699.
            false)
          sorter.sort(iter.asInstanceOf[Iterator[UnsafeRow]])
        }
      } else {
        rdd
      }

      // round-robin function is order sensitive if we don't sort the input.
      val isOrderSensitive = isRoundRobin && !SQLConf.get.sortBeforeRepartition
      if (needToCopyObjectsBeforeShuffle(part)) {
        newRdd.mapPartitionsWithIndexInternal((_, iter) => {
          val getPartitionKey = getPartitionKeyExtractor()
          iter.map { row => (part.getPartition(getPartitionKey(row)), row.copy()) }
        }, isOrderSensitive = isOrderSensitive)
      } else {
        newRdd.mapPartitionsWithIndexInternal((_, iter) => {
          val getPartitionKey = getPartitionKeyExtractor()
          val mutablePair = new MutablePair[Int, InternalRow]()
          iter.map { row => mutablePair.update(part.getPartition(getPartitionKey(row)), row) }
        }, isOrderSensitive = isOrderSensitive)
      }
    }

    // Now, we manually create a ShuffleDependency. Because pairs in rddWithPartitionIds
    // are in the form of (partitionId, row) and every partitionId is in the expected range
    // [0, part.numPartitions - 1]. The partitioner of this is a PartitionIdPassthrough.
    val dependency =
      new ShuffleDependency[Int, InternalRow, InternalRow](
        rddWithPartitionIds,
        new PartitionIdPassthrough(part.numPartitions),
        serializer,
        shuffleWriterProcessor = createShuffleWriteProcessor(writeMetrics))

    dependency
  }

  /**
   * Create a customized [[ShuffleWriteProcessor]] for SQL which wrap the default metrics reporter
   * with [[SQLShuffleWriteMetricsReporter]] as new reporter for [[ShuffleWriteProcessor]].
   */
  def createShuffleWriteProcessor(metrics: Map[String, SQLMetric]): ShuffleWriteProcessor = {
    new ShuffleWriteProcessor {
      override protected def createMetricsReporter(
          context: TaskContext): ShuffleWriteMetricsReporter = {
        new SQLShuffleWriteMetricsReporter(context.taskMetrics().shuffleWriteMetrics, metrics)
      }
    }
  }
}
