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

package org.apache.spark.sql.execution

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.hash.HashShuffleManager
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.shuffle.unsafe.UnsafeShuffleManager
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.errors.attachTree
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.util.MutablePair
import org.apache.spark.{HashPartitioner, Partitioner, RangePartitioner, SparkEnv}

/**
 * :: DeveloperApi ::
 * Performs a shuffle that will result in the desired `newPartitioning`.
 */
@DeveloperApi
case class Exchange(newPartitioning: Partitioning, child: SparkPlan) extends UnaryNode {

  override def outputPartitioning: Partitioning = newPartitioning

  override def output: Seq[Attribute] = child.output

  override def outputsUnsafeRows: Boolean = child.outputsUnsafeRows

  override def canProcessSafeRows: Boolean = true

  override def canProcessUnsafeRows: Boolean = {
    // Do not use the Unsafe path if we are using a RangePartitioning, since this may lead to
    // an interpreted RowOrdering being applied to an UnsafeRow, which will lead to
    // ClassCastExceptions at runtime. This check can be removed after SPARK-9054 is fixed.
    !newPartitioning.isInstanceOf[RangePartitioning]
  }

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
   * @param serializer the serializer that will be used to write rows
   * @return true if rows should be copied before being shuffled, false otherwise
   */
  private def needToCopyObjectsBeforeShuffle(
      partitioner: Partitioner,
      serializer: Serializer): Boolean = {
    // Note: even though we only use the partitioner's `numPartitions` field, we require it to be
    // passed instead of directly passing the number of partitions in order to guard against
    // corner-cases where a partitioner constructed with `numPartitions` partitions may output
    // fewer partitions (like RangePartitioner, for example).
    val conf = child.sqlContext.sparkContext.conf
    val shuffleManager = SparkEnv.get.shuffleManager
    val sortBasedShuffleOn = shuffleManager.isInstanceOf[SortShuffleManager] ||
      shuffleManager.isInstanceOf[UnsafeShuffleManager]
    val bypassMergeThreshold = conf.getInt("spark.shuffle.sort.bypassMergeThreshold", 200)
    val serializeMapOutputs = conf.getBoolean("spark.shuffle.sort.serializeMapOutputs", true)
    if (sortBasedShuffleOn) {
      val bypassIsSupported = SparkEnv.get.shuffleManager.isInstanceOf[SortShuffleManager]
      if (bypassIsSupported && partitioner.numPartitions <= bypassMergeThreshold) {
        // If we're using the original SortShuffleManager and the number of output partitions is
        // sufficiently small, then Spark will fall back to the hash-based shuffle write path, which
        // doesn't buffer deserialized records.
        // Note that we'll have to remove this case if we fix SPARK-6026 and remove this bypass.
        false
      } else if (serializeMapOutputs && serializer.supportsRelocationOfSerializedObjects) {
        // SPARK-4550 extended sort-based shuffle to serialize individual records prior to sorting
        // them. This optimization is guarded by a feature-flag and is only applied in cases where
        // shuffle dependency does not specify an aggregator or ordering and the record serializer
        // has certain properties. If this optimization is enabled, we can safely avoid the copy.
        //
        // Exchange never configures its ShuffledRDDs with aggregators or key orderings, so we only
        // need to check whether the optimization is enabled and supported by our serializer.
        //
        // This optimization also applies to UnsafeShuffleManager (added in SPARK-7081).
        false
      } else {
        // Spark's SortShuffleManager uses `ExternalSorter` to buffer records in memory. This code
        // path is used both when SortShuffleManager is used and when UnsafeShuffleManager falls
        // back to SortShuffleManager to perform a shuffle that the new fast path can't handle. In
        // both cases, we must copy.
        true
      }
    } else if (shuffleManager.isInstanceOf[HashShuffleManager]) {
      // We're using hash-based shuffle, so we don't need to copy.
      false
    } else {
      // Catch-all case to safely handle any future ShuffleManager implementations.
      true
    }
  }

  @transient private lazy val sparkConf = child.sqlContext.sparkContext.getConf

  private val serializer: Serializer = {
    val rowDataTypes = child.output.map(_.dataType).toArray
    // It is true when there is no field that needs to be write out.
    // For now, we will not use SparkSqlSerializer2 when noField is true.
    val noField = rowDataTypes == null || rowDataTypes.length == 0

    val useSqlSerializer2 =
        child.sqlContext.conf.useSqlSerializer2 &&   // SparkSqlSerializer2 is enabled.
        SparkSqlSerializer2.support(rowDataTypes) &&  // The schema of row is supported.
        !noField

    if (child.outputsUnsafeRows) {
      logInfo("Using UnsafeRowSerializer.")
      new UnsafeRowSerializer(child.output.size)
    } else if (useSqlSerializer2) {
      logInfo("Using SparkSqlSerializer2.")
      new SparkSqlSerializer2(rowDataTypes)
    } else {
      logInfo("Using SparkSqlSerializer.")
      new SparkSqlSerializer(sparkConf)
    }
  }

  protected override def doExecute(): RDD[InternalRow] = attachTree(this , "execute") {
    val rdd = child.execute()
    val part: Partitioner = newPartitioning match {
      case HashPartitioning(expressions, numPartitions) => new HashPartitioner(numPartitions)
      case RangePartitioning(sortingExpressions, numPartitions) =>
        // Internally, RangePartitioner runs a job on the RDD that samples keys to compute
        // partition bounds. To get accurate samples, we need to copy the mutable keys.
        val rddForSampling = rdd.mapPartitions { iter =>
          val mutablePair = new MutablePair[InternalRow, Null]()
          iter.map(row => mutablePair.update(row.copy(), null))
        }
        implicit val ordering = new RowOrdering(sortingExpressions, child.output)
        new RangePartitioner(numPartitions, rddForSampling, ascending = true)
      case SinglePartition =>
        new Partitioner {
          override def numPartitions: Int = 1
          override def getPartition(key: Any): Int = 0
        }
      case _ => sys.error(s"Exchange not implemented for $newPartitioning")
      // TODO: Handle BroadcastPartitioning.
    }
    def getPartitionKeyExtractor(): InternalRow => InternalRow = newPartitioning match {
      case HashPartitioning(expressions, _) => newMutableProjection(expressions, child.output)()
      case RangePartitioning(_, _) | SinglePartition => identity
      case _ => sys.error(s"Exchange not implemented for $newPartitioning")
    }
    val rddWithPartitionIds: RDD[Product2[Int, InternalRow]] = {
      if (needToCopyObjectsBeforeShuffle(part, serializer)) {
        rdd.mapPartitions { iter =>
          val getPartitionKey = getPartitionKeyExtractor()
          iter.map { row => (part.getPartition(getPartitionKey(row)), row.copy()) }
        }
      } else {
        rdd.mapPartitions { iter =>
          val getPartitionKey = getPartitionKeyExtractor()
          val mutablePair = new MutablePair[Int, InternalRow]()
          iter.map { row => mutablePair.update(part.getPartition(getPartitionKey(row)), row) }
        }
      }
    }
    new ShuffledRowRDD(rddWithPartitionIds, serializer, part.numPartitions)
  }
}

/**
 * Ensures that the [[org.apache.spark.sql.catalyst.plans.physical.Partitioning Partitioning]]
 * of input data meets the
 * [[org.apache.spark.sql.catalyst.plans.physical.Distribution Distribution]] requirements for
 * each operator by inserting [[Exchange]] Operators where required.  Also ensure that the
 * required input partition ordering requirements are met.
 */
private[sql] case class EnsureRequirements(sqlContext: SQLContext) extends Rule[SparkPlan] {
  // TODO: Determine the number of partitions.
  def numPartitions: Int = sqlContext.conf.numShufflePartitions

  def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
    case operator: SparkPlan =>
      // True iff every child's outputPartitioning satisfies the corresponding
      // required data distribution.
      def meetsRequirements: Boolean =
        operator.requiredChildDistribution.zip(operator.children).forall {
          case (required, child) =>
            val valid = child.outputPartitioning.satisfies(required)
            logDebug(
              s"${if (valid) "Valid" else "Invalid"} distribution," +
                s"required: $required current: ${child.outputPartitioning}")
            valid
        }

      // True iff any of the children are incorrectly sorted.
      def needsAnySort: Boolean =
        operator.requiredChildOrdering.zip(operator.children).exists {
          case (required, child) => required.nonEmpty && required != child.outputOrdering
        }

      // True iff outputPartitionings of children are compatible with each other.
      // It is possible that every child satisfies its required data distribution
      // but two children have incompatible outputPartitionings. For example,
      // A dataset is range partitioned by "a.asc" (RangePartitioning) and another
      // dataset is hash partitioned by "a" (HashPartitioning). Tuples in these two
      // datasets are both clustered by "a", but these two outputPartitionings are not
      // compatible.
      // TODO: ASSUMES TRANSITIVITY?
      def compatible: Boolean =
        operator.children
          .map(_.outputPartitioning)
          .sliding(2)
          .forall {
            case Seq(a) => true
            case Seq(a, b) => a.compatibleWith(b)
          }

      // Adds Exchange or Sort operators as required
      def addOperatorsIfNecessary(
          partitioning: Partitioning,
          rowOrdering: Seq[SortOrder],
          child: SparkPlan): SparkPlan = {

        def addShuffleIfNecessary(child: SparkPlan): SparkPlan = {
          if (child.outputPartitioning != partitioning) {
            Exchange(partitioning, child)
          } else {
            child
          }
        }

        def addSortIfNecessary(child: SparkPlan): SparkPlan = {

          if (rowOrdering.nonEmpty) {
            // If child.outputOrdering is [a, b] and rowOrdering is [a], we do not need to sort.
            val minSize = Seq(rowOrdering.size, child.outputOrdering.size).min
            if (minSize == 0 || rowOrdering.take(minSize) != child.outputOrdering.take(minSize)) {
              sqlContext.planner.BasicOperators.getSortOperator(rowOrdering, global = false, child)
            } else {
              child
            }
          } else {
            child
          }
        }

        addSortIfNecessary(addShuffleIfNecessary(child))
      }

      if (meetsRequirements && compatible && !needsAnySort) {
        operator
      } else {
        // At least one child does not satisfies its required data distribution or
        // at least one child's outputPartitioning is not compatible with another child's
        // outputPartitioning. In this case, we need to add Exchange operators.
        val requirements =
          (operator.requiredChildDistribution, operator.requiredChildOrdering, operator.children)

        val fixedChildren = requirements.zipped.map {
          case (AllTuples, rowOrdering, child) =>
            addOperatorsIfNecessary(SinglePartition, rowOrdering, child)
          case (ClusteredDistribution(clustering), rowOrdering, child) =>
            addOperatorsIfNecessary(HashPartitioning(clustering, numPartitions), rowOrdering, child)
          case (OrderedDistribution(ordering), rowOrdering, child) =>
            addOperatorsIfNecessary(RangePartitioning(ordering, numPartitions), rowOrdering, child)

          case (UnspecifiedDistribution, Seq(), child) =>
            child
          case (UnspecifiedDistribution, rowOrdering, child) =>
            sqlContext.planner.BasicOperators.getSortOperator(rowOrdering, global = false, child)

          case (dist, ordering, _) =>
            sys.error(s"Don't know how to ensure $dist with ordering $ordering")
        }

        operator.withNewChildren(fixedChildren)
      }
  }
}
