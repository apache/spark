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

import scala.collection.mutable.{ArrayBuffer, BitSet}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration._

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical._

@DeveloperApi
sealed abstract class BuildSide

@DeveloperApi
case object BuildLeft extends BuildSide

@DeveloperApi
case object BuildRight extends BuildSide

/**
 * Constant Value for Binary Join Node
 */
object BinaryJoinNode {
  val SINGLE_NULL_LIST = Seq[Row](null)
  val EMPTY_NULL_LIST  = Seq[Row]()
}

// TODO If join key was null should be considered as equal? In Hive this is configurable.

/**
 * Output the tuples for the matched (with the same join key) join group, base on the join types,
 * Both input iterators should be repeatable.
 */
trait BinaryRepeatableIteratorNode extends BinaryNode {
  self: Product =>

  val leftNullRow = new GenericRow(left.output.length)
  val rightNullRow = new GenericRow(right.output.length)

  val joinedRow = new JoinedRow()

  val boundCondition = InterpretedPredicate(
    condition
      .map(c => BindReferences.bindReference(c, left.output ++ right.output))
      .getOrElse(Literal(true)))

  def condition: Option[Expression]
  def joinType: JoinType

  // TODO we need to rewrite all of the iterators with our own implementation instead of the scala
  // iterator for performance / memory usage reason. 

  def leftOuterIterator(key: Row, leftIter: Iterable[Row], rightIter: Iterable[Row])
  : Iterator[Row] = {
    leftIter.iterator.flatMap { l => 
      joinedRow.withLeft(l)
      var matched = false
      (if (!key.anyNull) rightIter else BinaryJoinNode.EMPTY_NULL_LIST).collect {
        case r if (boundCondition(joinedRow.withRight(r))) => {
          matched = true
          joinedRow.copy
        }
      } ++ BinaryJoinNode.SINGLE_NULL_LIST.collect {
        case dummy if (!matched) => {
          joinedRow.withRight(rightNullRow).copy
        }
      }
    }
  }

  // TODO need to unit test this, currently it's the dead code, but should be used in SortMergeJoin
  def leftSemiIterator(key: Row, leftIter: Iterable[Row], rightIter: Iterable[Row])
  : Iterator[Row] = {
    leftIter.iterator.filter { l => 
      joinedRow.withLeft(l)
      (if (!key.anyNull) rightIter else BinaryJoinNode.EMPTY_NULL_LIST).exists {
        case r => (boundCondition(joinedRow.withRight(r)))
      }
    }
  }

  def rightOuterIterator(key: Row, leftIter: Iterable[Row], rightIter: Iterable[Row])
  : Iterator[Row] = {
    rightIter.iterator.flatMap{r => 
      joinedRow.withRight(r)
      var matched = false
      (if (!key.anyNull) leftIter else BinaryJoinNode.EMPTY_NULL_LIST).collect { 
        case l if (boundCondition(joinedRow.withLeft(l))) => {
          matched = true
          joinedRow.copy
        }
      } ++ BinaryJoinNode.SINGLE_NULL_LIST.collect {
        case dummy if (!matched) => {
          joinedRow.withLeft(leftNullRow).copy
        }
      }
    }
  }

  def fullOuterIterator(key: Row, leftIter: Iterable[Row], rightIter: Iterable[Row])
  : Iterator[Row] = {
    if (!key.anyNull) {
      val rightMatchedSet =  scala.collection.mutable.Set[Int]()
      leftIter.iterator.flatMap[Row] { l =>
        joinedRow.withLeft(l)
        var matched = false
        rightIter.zipWithIndex.collect {
          case (r, idx) if (boundCondition(joinedRow.withRight(r)))=> {
              matched = true
              rightMatchedSet.add(idx)
              joinedRow.copy
            }
        } ++ BinaryJoinNode.SINGLE_NULL_LIST.collect {
          case dummy if (!matched) => {
            joinedRow.withRight(rightNullRow).copy
          } 
        }
      } ++ rightIter.zipWithIndex.collect {
        case (r, idx) if (!rightMatchedSet.contains(idx)) => {
          joinedRow(leftNullRow, r).copy
        }
      }
    } else {
      leftIter.iterator.map[Row] { l =>
        joinedRow(l, rightNullRow).copy
      } ++ rightIter.iterator.map[Row] { r =>
        joinedRow(leftNullRow, r).copy
      }
    }
  }

  // TODO need to unit test this, currently it's the dead code, but should be used in SortMergeJoin
  def innerIterator(key: Row, leftIter: Iterable[Row], rightIter: Iterable[Row])
  : Iterator[Row] = {
    // ignore the join filter for inner join, we assume it will done in the select filter
    if (!key.anyNull) {
      leftIter.iterator.flatMap { l => 
        joinedRow.withLeft(l)
        rightIter.iterator.collect { 
          case r if boundCondition(joinedRow.withRight(r)) => joinedRow
        }
      }
    } else {
      BinaryJoinNode.EMPTY_NULL_LIST.iterator
    }
  }
}

trait HashJoin {
  self: SparkPlan =>

  val leftKeys: Seq[Expression]
  val rightKeys: Seq[Expression]
  val buildSide: BuildSide
  val left: SparkPlan
  val right: SparkPlan

  lazy val (buildPlan, streamedPlan) = buildSide match {
    case BuildLeft => (left, right)
    case BuildRight => (right, left)
  }

  lazy val (buildKeys, streamedKeys) = buildSide match {
    case BuildLeft => (leftKeys, rightKeys)
    case BuildRight => (rightKeys, leftKeys)
  }

  def output = left.output ++ right.output

  @transient lazy val buildSideKeyGenerator = newProjection(buildKeys, buildPlan.output)
  @transient lazy val streamSideKeyGenerator =
    newMutableProjection(streamedKeys, streamedPlan.output)

  def joinIterators(buildIter: Iterator[Row], streamIter: Iterator[Row]): Iterator[Row] = {
    // TODO: Use Spark's HashMap implementation.

    val hashTable = new java.util.HashMap[Row, ArrayBuffer[Row]]()
    var currentRow: Row = null

    // Create a mapping of buildKeys -> rows
    while (buildIter.hasNext) {
      currentRow = buildIter.next()
      val rowKey = buildSideKeyGenerator(currentRow)
      if (!rowKey.anyNull) {
        val existingMatchList = hashTable.get(rowKey)
        val matchList = if (existingMatchList == null) {
          val newMatchList = new ArrayBuffer[Row]()
          hashTable.put(rowKey, newMatchList)
          newMatchList
        } else {
          existingMatchList
        }
        matchList += currentRow.copy()
      }
    }

    new Iterator[Row] {
      private[this] var currentStreamedRow: Row = _
      private[this] var currentHashMatches: ArrayBuffer[Row] = _
      private[this] var currentMatchPosition: Int = -1

      // Mutable per row objects.
      private[this] val joinRow = new JoinedRow

      private[this] val joinKeys = streamSideKeyGenerator()

      override final def hasNext: Boolean =
        (currentMatchPosition != -1 && currentMatchPosition < currentHashMatches.size) ||
          (streamIter.hasNext && fetchNext())

      override final def next() = {
        val ret = buildSide match {
          case BuildRight => joinRow(currentStreamedRow, currentHashMatches(currentMatchPosition))
          case BuildLeft => joinRow(currentHashMatches(currentMatchPosition), currentStreamedRow)
        }
        currentMatchPosition += 1
        ret
      }

      /**
       * Searches the streamed iterator for the next row that has at least one match in hashtable.
       *
       * @return true if the search is successful, and false if the streamed iterator runs out of
       *         tuples.
       */
      private final def fetchNext(): Boolean = {
        currentHashMatches = null
        currentMatchPosition = -1

        while (currentHashMatches == null && streamIter.hasNext) {
          currentStreamedRow = streamIter.next()
          if (!joinKeys(currentStreamedRow).anyNull) {
            currentHashMatches = hashTable.get(joinKeys.currentValue)
          }
        }

        if (currentHashMatches == null) {
          false
        } else {
          currentMatchPosition = 0
          true
        }
      }
    }
  }
}

/**
 * :: DeveloperApi ::
 * Performs a hash join of two child relations by shuffling the data using the join keys.
 * This operator requires loading both tables into memory.
 */
@DeveloperApi
case class HashOuterJoin(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan) extends BinaryRepeatableIteratorNode {

  override def outputPartitioning: Partitioning = left.outputPartitioning

  override def requiredChildDistribution =
    ClusteredDistribution(leftKeys) :: ClusteredDistribution(rightKeys) :: Nil

  def output = left.output ++ right.output

  private[this] def buildHashTable(iter: Iterator[Row], keyGenerator: Projection)
  : Map[Row, ArrayBuffer[Row]] = {
    // TODO: Use Spark's HashMap implementation.
    val hashTable = scala.collection.mutable.Map[Row, ArrayBuffer[Row]]()
    while (iter.hasNext) {
      val currentRow = iter.next()
      val rowKey = keyGenerator(currentRow)

      val existingMatchList = hashTable.getOrElseUpdate(rowKey, {new ArrayBuffer[Row]()})
      existingMatchList += currentRow.copy()
    }
    
    hashTable.toMap[Row, ArrayBuffer[Row]]
  }
  
  def execute() = {
    left.execute().zipPartitions(right.execute()) { (leftIter, rightIter) =>
      // TODO this probably can be replaced by external sort (sort merged join?)
      val leftHashTable = buildHashTable(leftIter, newProjection(leftKeys, left.output))
      val rightHashTable= buildHashTable(rightIter, newProjection(rightKeys, right.output))

      joinType match {
        case LeftOuter => leftHashTable.keysIterator.flatMap { key =>
          leftOuterIterator(key, leftHashTable.getOrElse(key, BinaryJoinNode.EMPTY_NULL_LIST), 
            rightHashTable.getOrElse(key, BinaryJoinNode.EMPTY_NULL_LIST))
        }
        case RightOuter => rightHashTable.keysIterator.flatMap { key =>
          rightOuterIterator(key, leftHashTable.getOrElse(key, BinaryJoinNode.EMPTY_NULL_LIST), 
            rightHashTable.getOrElse(key, BinaryJoinNode.EMPTY_NULL_LIST))
        }
        case FullOuter => (leftHashTable.keySet ++ rightHashTable.keySet).iterator.flatMap { key =>
          fullOuterIterator(key, leftHashTable.getOrElse(key, BinaryJoinNode.EMPTY_NULL_LIST), 
            rightHashTable.getOrElse(key, BinaryJoinNode.EMPTY_NULL_LIST))
        }
        case x => throw new Exception(s"Need to add implementation for $x")
      }
    }
  }
}

/**
 * :: DeveloperApi ::
 * Performs an inner hash join of two child relations by first shuffling the data using the join
 * keys.
 */
@DeveloperApi
case class ShuffledHashJoin(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    buildSide: BuildSide,
    left: SparkPlan,
    right: SparkPlan) extends BinaryNode with HashJoin {

  override def outputPartitioning: Partitioning = left.outputPartitioning

  override def requiredChildDistribution =
    ClusteredDistribution(leftKeys) :: ClusteredDistribution(rightKeys) :: Nil

  def execute() = {
    buildPlan.execute().zipPartitions(streamedPlan.execute()) {
      (buildIter, streamIter) => joinIterators(buildIter, streamIter)
    }
  }
}

/**
 * :: DeveloperApi ::
 * Build the right table's join keys into a HashSet, and iteratively go through the left
 * table, to find the if join keys are in the Hash set.
 */
@DeveloperApi
case class LeftSemiJoinHash(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    left: SparkPlan,
    right: SparkPlan) extends BinaryNode with HashJoin {

  val buildSide = BuildRight

  override def requiredChildDistribution =
    ClusteredDistribution(leftKeys) :: ClusteredDistribution(rightKeys) :: Nil

  override def output = left.output

  def execute() = {
    buildPlan.execute().zipPartitions(streamedPlan.execute()) { (buildIter, streamIter) =>
      val hashSet = new java.util.HashSet[Row]()
      var currentRow: Row = null

      // Create a Hash set of buildKeys
      while (buildIter.hasNext) {
        currentRow = buildIter.next()
        val rowKey = buildSideKeyGenerator(currentRow)
        if (!rowKey.anyNull) {
          val keyExists = hashSet.contains(rowKey)
          if (!keyExists) {
            hashSet.add(rowKey)
          }
        }
      }

      val joinKeys = streamSideKeyGenerator()
      streamIter.filter(current => {
        !joinKeys(current).anyNull && hashSet.contains(joinKeys.currentValue)
      })
    }
  }
}


/**
 * :: DeveloperApi ::
 * Performs an inner hash join of two child relations.  When the output RDD of this operator is
 * being constructed, a Spark job is asynchronously started to calculate the values for the
 * broadcasted relation.  This data is then placed in a Spark broadcast variable.  The streamed
 * relation is not shuffled.
 */
@DeveloperApi
case class BroadcastHashJoin(
     leftKeys: Seq[Expression],
     rightKeys: Seq[Expression],
     buildSide: BuildSide,
     left: SparkPlan,
     right: SparkPlan) extends BinaryNode with HashJoin {


  override def outputPartitioning: Partitioning = left.outputPartitioning

  override def requiredChildDistribution =
    UnspecifiedDistribution :: UnspecifiedDistribution :: Nil

  @transient
  lazy val broadcastFuture = future {
    sparkContext.broadcast(buildPlan.executeCollect())
  }

  def execute() = {
    val broadcastRelation = Await.result(broadcastFuture, 5.minute)

    streamedPlan.execute().mapPartitions { streamedIter =>
      joinIterators(broadcastRelation.value.iterator, streamedIter)
    }
  }
}

/**
 * :: DeveloperApi ::
 * Using BroadcastNestedLoopJoin to calculate left semi join result when there's no join keys
 * for hash join.
 */
@DeveloperApi
case class LeftSemiJoinBNL(
    streamed: SparkPlan, broadcast: SparkPlan, condition: Option[Expression])
  extends BinaryNode {
  // TODO: Override requiredChildDistribution.

  override def outputPartitioning: Partitioning = streamed.outputPartitioning

  def output = left.output

  /** The Streamed Relation */
  def left = streamed
  /** The Broadcast relation */
  def right = broadcast

  @transient lazy val boundCondition =
    InterpretedPredicate(
      condition
        .map(c => BindReferences.bindReference(c, left.output ++ right.output))
        .getOrElse(Literal(true)))

  def execute() = {
    val broadcastedRelation =
      sparkContext.broadcast(broadcast.execute().map(_.copy()).collect().toIndexedSeq)

    streamed.execute().mapPartitions { streamedIter =>
      val joinedRow = new JoinedRow

      streamedIter.filter(streamedRow => {
        var i = 0
        var matched = false

        while (i < broadcastedRelation.value.size && !matched) {
          val broadcastedRow = broadcastedRelation.value(i)
          if (boundCondition(joinedRow(streamedRow, broadcastedRow))) {
            matched = true
          }
          i += 1
        }
        matched
      })
    }
  }
}

/**
 * :: DeveloperApi ::
 */
@DeveloperApi
case class CartesianProduct(left: SparkPlan, right: SparkPlan) extends BinaryNode {
  def output = left.output ++ right.output

  def execute() = {
    val leftResults = left.execute().map(_.copy())
    val rightResults = right.execute().map(_.copy())

    leftResults.cartesian(rightResults).mapPartitions { iter =>
      val joinedRow = new JoinedRow
      iter.map(r => joinedRow(r._1, r._2))
    }
  }
}

/**
 * :: DeveloperApi ::
 */
@DeveloperApi
case class BroadcastNestedLoopJoin(
    streamed: SparkPlan, broadcast: SparkPlan, joinType: JoinType, condition: Option[Expression])
  extends BinaryNode {
  // TODO: Override requiredChildDistribution.

  override def outputPartitioning: Partitioning = streamed.outputPartitioning

  override def output = {
    joinType match {
      case LeftOuter =>
        left.output ++ right.output.map(_.withNullability(true))
      case RightOuter =>
        left.output.map(_.withNullability(true)) ++ right.output
      case FullOuter =>
        left.output.map(_.withNullability(true)) ++ right.output.map(_.withNullability(true))
      case _ =>
        left.output ++ right.output
    }
  }

  /** The Streamed Relation */
  def left = streamed
  /** The Broadcast relation */
  def right = broadcast

  @transient lazy val boundCondition =
    InterpretedPredicate(
      condition
        .map(c => BindReferences.bindReference(c, left.output ++ right.output))
        .getOrElse(Literal(true)))

  def execute() = {
    val broadcastedRelation =
      sparkContext.broadcast(broadcast.execute().map(_.copy()).collect().toIndexedSeq)

    val streamedPlusMatches = streamed.execute().mapPartitions { streamedIter =>
      val matchedRows = new ArrayBuffer[Row]
      // TODO: Use Spark's BitSet.
      val includedBroadcastTuples = new BitSet(broadcastedRelation.value.size)
      val joinedRow = new JoinedRow
      val rightNulls = new GenericMutableRow(right.output.size)

      streamedIter.foreach { streamedRow =>
        var i = 0
        var matched = false

        while (i < broadcastedRelation.value.size) {
          // TODO: One bitset per partition instead of per row.
          val broadcastedRow = broadcastedRelation.value(i)
          if (boundCondition(joinedRow(streamedRow, broadcastedRow))) {
            matchedRows += joinedRow(streamedRow, broadcastedRow).copy()
            matched = true
            includedBroadcastTuples += i
          }
          i += 1
        }

        if (!matched && (joinType == LeftOuter || joinType == FullOuter)) {
          matchedRows += joinedRow(streamedRow, rightNulls).copy()
        }
      }
      Iterator((matchedRows, includedBroadcastTuples))
    }

    val includedBroadcastTuples = streamedPlusMatches.map(_._2)
    val allIncludedBroadcastTuples =
      if (includedBroadcastTuples.count == 0) {
        new scala.collection.mutable.BitSet(broadcastedRelation.value.size)
      } else {
        streamedPlusMatches.map(_._2).reduce(_ ++ _)
      }

    val leftNulls = new GenericMutableRow(left.output.size)
    val rightOuterMatches: Seq[Row] =
      if (joinType == RightOuter || joinType == FullOuter) {
        broadcastedRelation.value.zipWithIndex.filter {
          case (row, i) => !allIncludedBroadcastTuples.contains(i)
        }.map {
          case (row, _) => new JoinedRow(leftNulls, row)
        }
      } else {
        Vector()
      }

    // TODO: Breaks lineage.
    sparkContext.union(
      streamedPlusMatches.flatMap(_._1), sparkContext.makeRDD(rightOuterMatches))
  }
}
