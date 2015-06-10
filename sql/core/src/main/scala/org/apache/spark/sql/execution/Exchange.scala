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

import org.apache.spark.{HashPartitioner, Partitioner, RangePartitioner, SparkEnv}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.shuffle.unsafe.UnsafeShuffleManager
import org.apache.spark.sql.catalyst.errors.attachTree
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{SQLContext, Row}
import org.apache.spark.util.MutablePair

object Exchange {
  /**
   * Returns true when the ordering expressions are a subset of the key.
   * if true, ShuffledRDD can use `setKeyOrdering(orderingKey)` to sort within [[Exchange]].
   */
  def canSortWithShuffle(partitioning: Partitioning, desiredOrdering: Seq[SortOrder]): Boolean = {
    desiredOrdering.map(_.child).toSet.subsetOf(partitioning.keyExpressions.toSet)
  }
}

/**
 * :: DeveloperApi ::
 * Performs a shuffle that will result in the desired `newPartitioning`.  Optionally sorts each
 * resulting partition based on expressions from the partition key.  It is invalid to construct an
 * exchange operator with a `newOrdering` that cannot be calculated using the partitioning key.
 */
@DeveloperApi
case class Exchange(
    newPartitioning: Partitioning,
    newOrdering: Seq[SortOrder],
    child: SparkPlan)
  extends UnaryNode {

  override def outputPartitioning: Partitioning = newPartitioning

  override def outputOrdering: Seq[SortOrder] = newOrdering

  override def output: Seq[Attribute] = child.output

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
    if (newOrdering.nonEmpty) {
      // If a new ordering is required, then records will be sorted with Spark's `ExternalSorter`,
      // which requires a defensive copy.
      true
    } else if (sortBasedShuffleOn) {
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
        // shuffle dependency does not specify an ordering and the record serializer has certain
        // properties. If this optimization is enabled, we can safely avoid the copy.
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
    } else {
      // We're using hash-based shuffle, so we don't need to copy.
      false
    }
  }

  private val keyOrdering = {
    if (newOrdering.nonEmpty) {
      val key = newPartitioning.keyExpressions
      val boundOrdering = newOrdering.map { o =>
        val ordinal = key.indexOf(o.child)
        if (ordinal == -1) sys.error(s"Invalid ordering on $o requested for $newPartitioning")
        o.copy(child = BoundReference(ordinal, o.child.dataType, o.child.nullable))
      }
      new RowOrdering(boundOrdering)
    } else {
      null // Ordering will not be used
    }
  }

  @transient private lazy val sparkConf = child.sqlContext.sparkContext.getConf

  private def getSerializer(
      keySchema: Array[DataType],
      valueSchema: Array[DataType],
      hasKeyOrdering: Boolean,
      numPartitions: Int): Serializer = {
    // It is true when there is no field that needs to be write out.
    // For now, we will not use SparkSqlSerializer2 when noField is true.
    val noField =
      (keySchema == null || keySchema.length == 0) &&
      (valueSchema == null || valueSchema.length == 0)

    val useSqlSerializer2 =
        child.sqlContext.conf.useSqlSerializer2 &&   // SparkSqlSerializer2 is enabled.
        SparkSqlSerializer2.support(keySchema) &&    // The schema of key is supported.
        SparkSqlSerializer2.support(valueSchema) &&  // The schema of value is supported.
        !noField

    val serializer = if (useSqlSerializer2) {
      logInfo("Using SparkSqlSerializer2.")
      new SparkSqlSerializer2(keySchema, valueSchema, hasKeyOrdering)
    } else {
      logInfo("Using SparkSqlSerializer.")
      new SparkSqlSerializer(sparkConf)
    }

    serializer
  }

  protected override def doExecute(): RDD[Row] = attachTree(this , "execute") {
    newPartitioning match {
      case HashPartitioning(expressions, numPartitions) =>
        val keySchema = expressions.map(_.dataType).toArray
        val valueSchema = child.output.map(_.dataType).toArray
        val serializer = getSerializer(keySchema, valueSchema, newOrdering.nonEmpty, numPartitions)
        val part = new HashPartitioner(numPartitions)

        val rdd = if (needToCopyObjectsBeforeShuffle(part, serializer)) {
          child.execute().mapPartitions { iter =>
            val hashExpressions = newMutableProjection(expressions, child.output)()
            iter.map(r => (hashExpressions(r).copy(), r.copy()))
          }
        } else {
          child.execute().mapPartitions { iter =>
            val hashExpressions = newMutableProjection(expressions, child.output)()
            val mutablePair = new MutablePair[Row, Row]()
            iter.map(r => mutablePair.update(hashExpressions(r), r))
          }
        }
        val shuffled = new ShuffledRDD[Row, Row, Row](rdd, part)
        if (newOrdering.nonEmpty) {
          shuffled.setKeyOrdering(keyOrdering)
        }
        shuffled.setSerializer(serializer)
        shuffled.map(_._2)

      case RangePartitioning(sortingExpressions, numPartitions) =>
        val keySchema = child.output.map(_.dataType).toArray
        val serializer = getSerializer(keySchema, null, newOrdering.nonEmpty, numPartitions)

        val childRdd = child.execute()
        val part: Partitioner = {
          // Internally, RangePartitioner runs a job on the RDD that samples keys to compute
          // partition bounds. To get accurate samples, we need to copy the mutable keys.
          val rddForSampling = childRdd.mapPartitions { iter =>
            val mutablePair = new MutablePair[Row, Null]()
            iter.map(row => mutablePair.update(row.copy(), null))
          }
          // TODO: RangePartitioner should take an Ordering.
          implicit val ordering = new RowOrdering(sortingExpressions, child.output)
          new RangePartitioner(numPartitions, rddForSampling, ascending = true)
        }

        val rdd = if (needToCopyObjectsBeforeShuffle(part, serializer)) {
          childRdd.mapPartitions { iter => iter.map(row => (row.copy(), null))}
        } else {
          childRdd.mapPartitions { iter =>
            val mutablePair = new MutablePair[Row, Null]()
            iter.map(row => mutablePair.update(row, null))
          }
        }

        val shuffled = new ShuffledRDD[Row, Null, Null](rdd, part)
        if (newOrdering.nonEmpty) {
          shuffled.setKeyOrdering(keyOrdering)
        }
        shuffled.setSerializer(serializer)
        shuffled.map(_._1)

      case SinglePartition =>
        val valueSchema = child.output.map(_.dataType).toArray
        val serializer = getSerializer(null, valueSchema, hasKeyOrdering = false, 1)
        val partitioner = new HashPartitioner(1)

        val rdd = if (needToCopyObjectsBeforeShuffle(partitioner, serializer)) {
          child.execute().mapPartitions { iter => iter.map(r => (null, r.copy())) }
        } else {
          child.execute().mapPartitions { iter =>
            val mutablePair = new MutablePair[Null, Row]()
            iter.map(r => mutablePair.update(null, r))
          }
        }
        val shuffled = new ShuffledRDD[Null, Row, Row](rdd, partitioner)
        shuffled.setSerializer(serializer)
        shuffled.map(_._2)

      case _ => sys.error(s"Exchange not implemented for $newPartitioning")
      // TODO: Handle BroadcastPartitioning.
    }
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
        !operator.children
          .map(_.outputPartitioning)
          .sliding(2)
          .map {
            case Seq(a) => true
            case Seq(a, b) => a.compatibleWith(b)
          }.exists(!_)

      // Adds Exchange or Sort operators as required
      def addOperatorsIfNecessary(
          partitioning: Partitioning,
          rowOrdering: Seq[SortOrder],
          child: SparkPlan): SparkPlan = {
        val needSort = rowOrdering.nonEmpty && child.outputOrdering != rowOrdering
        val needsShuffle = child.outputPartitioning != partitioning
        val canSortWithShuffle = Exchange.canSortWithShuffle(partitioning, rowOrdering)

        if (needSort && needsShuffle && canSortWithShuffle) {
          Exchange(partitioning, rowOrdering, child)
        } else {
          val withShuffle = if (needsShuffle) {
            Exchange(partitioning, Nil, child)
          } else {
            child
          }

          val withSort = if (needSort) {
            if (sqlContext.conf.externalSortEnabled) {
              ExternalSort(rowOrdering, global = false, withShuffle)
            } else {
              Sort(rowOrdering, global = false, withShuffle)
            }
          } else {
            withShuffle
          }

          withSort
        }
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
            if (sqlContext.conf.externalSortEnabled) {
              ExternalSort(rowOrdering, global = false, child)
            } else {
              Sort(rowOrdering, global = false, child)
            }

          case (dist, ordering, _) =>
            sys.error(s"Don't know how to ensure $dist with ordering $ordering")
        }

        operator.withNewChildren(fixedChildren)
      }
  }
}
