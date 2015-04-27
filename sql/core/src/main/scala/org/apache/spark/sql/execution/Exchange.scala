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
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.{SparkEnv, HashPartitioner, RangePartitioner}
import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.{SQLContext, Row}
import org.apache.spark.sql.catalyst.errors.attachTree
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.DataType
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

  /** We must copy rows when sort based shuffle is on */
  protected def sortBasedShuffleOn = SparkEnv.get.shuffleManager.isInstanceOf[SortShuffleManager]

  private val bypassMergeThreshold =
    child.sqlContext.sparkContext.conf.getInt("spark.shuffle.sort.bypassMergeThreshold", 200)

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

  def serializer(
      keySchema: Array[DataType],
      valueSchema: Array[DataType],
      numPartitions: Int): Serializer = {
    // In ExternalSorter's spillToMergeableFile function, key-value pairs are written out
    // through write(key) and then write(value) instead of write((key, value)). Because
    // SparkSqlSerializer2 assumes that objects passed in are Product2, we cannot safely use
    // it when spillToMergeableFile in ExternalSorter will be used.
    // So, we will not use SparkSqlSerializer2 when
    //  - Sort-based shuffle is enabled and the number of reducers (numPartitions) is greater
    //     then the bypassMergeThreshold; or
    //  - newOrdering is defined.
    val cannotUseSqlSerializer2 =
      (sortBasedShuffleOn && numPartitions > bypassMergeThreshold) || newOrdering.nonEmpty

    // It is true when there is no field that needs to be write out.
    // For now, we will not use SparkSqlSerializer2 when noField is true.
    val noField =
      (keySchema == null || keySchema.length == 0) &&
      (valueSchema == null || valueSchema.length == 0)

    val useSqlSerializer2 =
        child.sqlContext.conf.useSqlSerializer2 &&   // SparkSqlSerializer2 is enabled.
        !cannotUseSqlSerializer2 &&                  // Safe to use Serializer2.
        SparkSqlSerializer2.support(keySchema) &&    // The schema of key is supported.
        SparkSqlSerializer2.support(valueSchema) &&  // The schema of value is supported.
        !noField

    val serializer = if (useSqlSerializer2) {
      logInfo("Using SparkSqlSerializer2.")
      new SparkSqlSerializer2(keySchema, valueSchema)
    } else {
      logInfo("Using SparkSqlSerializer.")
      new SparkSqlSerializer(sparkConf)
    }

    serializer
  }

  override def execute(): RDD[Row] = attachTree(this , "execute") {
    newPartitioning match {
      case HashPartitioning(expressions, numPartitions) =>
        // TODO: Eliminate redundant expressions in grouping key and value.
        // This is a workaround for SPARK-4479. When:
        //  1. sort based shuffle is on, and
        //  2. the partition number is under the merge threshold, and
        //  3. no ordering is required
        // we can avoid the defensive copies to improve performance. In the long run, we probably
        // want to include information in shuffle dependencies to indicate whether elements in the
        // source RDD should be copied.
        val willMergeSort = sortBasedShuffleOn && numPartitions > bypassMergeThreshold

        val rdd = if (willMergeSort || newOrdering.nonEmpty) {
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
        val part = new HashPartitioner(numPartitions)
        val shuffled =
          if (newOrdering.nonEmpty) {
            new ShuffledRDD[Row, Row, Row](rdd, part).setKeyOrdering(keyOrdering)
          } else {
            new ShuffledRDD[Row, Row, Row](rdd, part)
          }
        val keySchema = expressions.map(_.dataType).toArray
        val valueSchema = child.output.map(_.dataType).toArray
        shuffled.setSerializer(serializer(keySchema, valueSchema, numPartitions))

        shuffled.map(_._2)

      case RangePartitioning(sortingExpressions, numPartitions) =>
        val rdd = if (sortBasedShuffleOn || newOrdering.nonEmpty) {
          child.execute().mapPartitions { iter => iter.map(row => (row.copy(), null))}
        } else {
          child.execute().mapPartitions { iter =>
            val mutablePair = new MutablePair[Row, Null](null, null)
            iter.map(row => mutablePair.update(row, null))
          }
        }

        // TODO: RangePartitioner should take an Ordering.
        implicit val ordering = new RowOrdering(sortingExpressions, child.output)

        val part = new RangePartitioner(numPartitions, rdd, ascending = true)
        val shuffled =
          if (newOrdering.nonEmpty) {
            new ShuffledRDD[Row, Null, Null](rdd, part).setKeyOrdering(keyOrdering)
          } else {
            new ShuffledRDD[Row, Null, Null](rdd, part)
          }
        val keySchema = child.output.map(_.dataType).toArray
        shuffled.setSerializer(serializer(keySchema, null, numPartitions))

        shuffled.map(_._1)

      case SinglePartition =>
        // SPARK-4479: Can't turn off defensive copy as what we do for `HashPartitioning`, since
        // operators like `TakeOrdered` may require an ordering within the partition, and currently
        // `SinglePartition` doesn't include ordering information.
        // TODO Add `SingleOrderedPartition` for operators like `TakeOrdered`
        val rdd = if (sortBasedShuffleOn) {
          child.execute().mapPartitions { iter => iter.map(r => (null, r.copy())) }
        } else {
          child.execute().mapPartitions { iter =>
            val mutablePair = new MutablePair[Null, Row]()
            iter.map(r => mutablePair.update(null, r))
          }
        }
        val partitioner = new HashPartitioner(1)
        val shuffled = new ShuffledRDD[Null, Row, Row](rdd, partitioner)
        val valueSchema = child.output.map(_.dataType).toArray
        shuffled.setSerializer(serializer(null, valueSchema, 1))
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
            case Seq(a,b) => a compatibleWith b
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
