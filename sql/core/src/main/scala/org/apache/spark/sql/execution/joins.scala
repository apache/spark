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

import java.util.{HashMap => JavaHashMap}

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
 * Performs a hash based outer join for two child relations by shuffling the data using 
 * the join keys. This operator requires loading the associated partition in both side into memory.
 */
@DeveloperApi
case class HashOuterJoin(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan) extends BinaryNode {

  override def outputPartitioning: Partitioning = joinType match {
    case LeftOuter => left.outputPartitioning
    case RightOuter => right.outputPartitioning
    case FullOuter => UnknownPartitioning(left.outputPartitioning.numPartitions)
    case x => throw new Exception(s"HashOuterJoin should not take $x as the JoinType")
  }

  override def requiredChildDistribution =
    ClusteredDistribution(leftKeys) :: ClusteredDistribution(rightKeys) :: Nil

  override def output = {
    joinType match {
      case LeftOuter =>
        left.output ++ right.output.map(_.withNullability(true))
      case RightOuter =>
        left.output.map(_.withNullability(true)) ++ right.output
      case FullOuter =>
        left.output.map(_.withNullability(true)) ++ right.output.map(_.withNullability(true))
      case x =>
        throw new Exception(s"HashOuterJoin should not take $x as the JoinType")
    }
  }

  @transient private[this] lazy val DUMMY_LIST = Seq[Row](null)
  @transient private[this] lazy val EMPTY_LIST = Seq.empty[Row]

  // TODO we need to rewrite all of the iterators with our own implementation instead of the Scala
  // iterator for performance purpose. 

  private[this] def leftOuterIterator(
      key: Row, leftIter: Iterable[Row], rightIter: Iterable[Row]): Iterator[Row] = {
    val joinedRow = new JoinedRow()
    val rightNullRow = new GenericRow(right.output.length)
    val boundCondition = 
      condition.map(newPredicate(_, left.output ++ right.output)).getOrElse((row: Row) => true)

    leftIter.iterator.flatMap { l => 
      joinedRow.withLeft(l)
      var matched = false
      (if (!key.anyNull) rightIter.collect { case r if (boundCondition(joinedRow.withRight(r))) => 
        matched = true
        joinedRow.copy
      } else {
        Nil
      }) ++ DUMMY_LIST.filter(_ => !matched).map( _ => {
        // DUMMY_LIST.filter(_ => !matched) is a tricky way to add additional row,
        // as we don't know whether we need to append it until finish iterating all of the 
        // records in right side.
        // If we didn't get any proper row, then append a single row with empty right
        joinedRow.withRight(rightNullRow).copy
      })
    }
  }

  private[this] def rightOuterIterator(
      key: Row, leftIter: Iterable[Row], rightIter: Iterable[Row]): Iterator[Row] = {
    val joinedRow = new JoinedRow()
    val leftNullRow = new GenericRow(left.output.length)
    val boundCondition = 
      condition.map(newPredicate(_, left.output ++ right.output)).getOrElse((row: Row) => true)

    rightIter.iterator.flatMap { r => 
      joinedRow.withRight(r)
      var matched = false
      (if (!key.anyNull) leftIter.collect { case l if (boundCondition(joinedRow.withLeft(l))) => 
        matched = true
        joinedRow.copy
      } else {
        Nil
      }) ++ DUMMY_LIST.filter(_ => !matched).map( _ => {
        // DUMMY_LIST.filter(_ => !matched) is a tricky way to add additional row,
        // as we don't know whether we need to append it until finish iterating all of the 
        // records in left side.
        // If we didn't get any proper row, then append a single row with empty left.
        joinedRow.withLeft(leftNullRow).copy
      })
    }
  }

  private[this] def fullOuterIterator(
      key: Row, leftIter: Iterable[Row], rightIter: Iterable[Row]): Iterator[Row] = {
    val joinedRow = new JoinedRow()
    val leftNullRow = new GenericRow(left.output.length)
    val rightNullRow = new GenericRow(right.output.length)
    val boundCondition = 
      condition.map(newPredicate(_, left.output ++ right.output)).getOrElse((row: Row) => true)

    if (!key.anyNull) {
      // Store the positions of records in right, if one of its associated row satisfy
      // the join condition.
      val rightMatchedSet = scala.collection.mutable.Set[Int]()
      leftIter.iterator.flatMap[Row] { l =>
        joinedRow.withLeft(l)
        var matched = false
        rightIter.zipWithIndex.collect { 
          // 1. For those matched (satisfy the join condition) records with both sides filled, 
          //    append them directly

          case (r, idx) if (boundCondition(joinedRow.withRight(r)))=> {
            matched = true
            // if the row satisfy the join condition, add its index into the matched set
            rightMatchedSet.add(idx)
            joinedRow.copy
          }
        } ++ DUMMY_LIST.filter(_ => !matched).map( _ => {
          // 2. For those unmatched records in left, append additional records with empty right.

          // DUMMY_LIST.filter(_ => !matched) is a tricky way to add additional row,
          // as we don't know whether we need to append it until finish iterating all 
          // of the records in right side.
          // If we didn't get any proper row, then append a single row with empty right.
          joinedRow.withRight(rightNullRow).copy
        })
      } ++ rightIter.zipWithIndex.collect {
        // 3. For those unmatched records in right, append additional records with empty left.

        // Re-visiting the records in right, and append additional row with empty left, if its not 
        // in the matched set. 
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

  private[this] def buildHashTable(
      iter: Iterator[Row], keyGenerator: Projection): JavaHashMap[Row, ArrayBuffer[Row]] = {
    val hashTable = new JavaHashMap[Row, ArrayBuffer[Row]]()
    while (iter.hasNext) {
      val currentRow = iter.next()
      val rowKey = keyGenerator(currentRow)

      var existingMatchList = hashTable.get(rowKey)
      if (existingMatchList == null) {
        existingMatchList = new ArrayBuffer[Row]()
        hashTable.put(rowKey, existingMatchList)
      }

      existingMatchList += currentRow.copy()
    }

    hashTable
  }

  def execute() = {
    left.execute().zipPartitions(right.execute()) { (leftIter, rightIter) =>
      // TODO this probably can be replaced by external sort (sort merged join?)
      // Build HashMap for current partition in left relation
      val leftHashTable = buildHashTable(leftIter, newProjection(leftKeys, left.output))
      // Build HashMap for current partition in right relation
      val rightHashTable = buildHashTable(rightIter, newProjection(rightKeys, right.output))

      import scala.collection.JavaConversions._
      val boundCondition = 
        condition.map(newPredicate(_, left.output ++ right.output)).getOrElse((row: Row) => true)
      joinType match {
        case LeftOuter => leftHashTable.keysIterator.flatMap { key =>
          leftOuterIterator(key, leftHashTable.getOrElse(key, EMPTY_LIST), 
            rightHashTable.getOrElse(key, EMPTY_LIST))
        }
        case RightOuter => rightHashTable.keysIterator.flatMap { key =>
          rightOuterIterator(key, leftHashTable.getOrElse(key, EMPTY_LIST), 
            rightHashTable.getOrElse(key, EMPTY_LIST))
        }
        case FullOuter => (leftHashTable.keySet ++ rightHashTable.keySet).iterator.flatMap { key =>
          fullOuterIterator(key, 
            leftHashTable.getOrElse(key, EMPTY_LIST), 
            rightHashTable.getOrElse(key, EMPTY_LIST))
        }
        case x => throw new Exception(s"HashOuterJoin should not take $x as the JoinType")
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

  override def outputPartitioning: Partitioning = streamedPlan.outputPartitioning

  override def requiredChildDistribution =
    UnspecifiedDistribution :: UnspecifiedDistribution :: Nil

  @transient
  val broadcastFuture = future {
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
    left: SparkPlan,
    right: SparkPlan,
    buildSide: BuildSide,
    joinType: JoinType,
    condition: Option[Expression]) extends BinaryNode {
  // TODO: Override requiredChildDistribution.

  /** BuildRight means the right relation <=> the broadcast relation. */
  val (streamed, broadcast) = buildSide match {
    case BuildRight => (left, right)
    case BuildLeft => (right, left)
  }

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

  @transient lazy val boundCondition =
    InterpretedPredicate(
      condition
        .map(c => BindReferences.bindReference(c, left.output ++ right.output))
        .getOrElse(Literal(true)))

  def execute() = {
    val broadcastedRelation =
      sparkContext.broadcast(broadcast.execute().map(_.copy()).collect().toIndexedSeq)

    /** All rows that either match both-way, or rows from streamed joined with nulls. */
    val matchesOrStreamedRowsWithNulls = streamed.execute().mapPartitions { streamedIter =>
      val matchedRows = new ArrayBuffer[Row]
      // TODO: Use Spark's BitSet.
      val includedBroadcastTuples =
        new scala.collection.mutable.BitSet(broadcastedRelation.value.size)
      val joinedRow = new JoinedRow
      val leftNulls = new GenericMutableRow(left.output.size)
      val rightNulls = new GenericMutableRow(right.output.size)

      streamedIter.foreach { streamedRow =>
        var i = 0
        var streamRowMatched = false

        while (i < broadcastedRelation.value.size) {
          // TODO: One bitset per partition instead of per row.
          val broadcastedRow = broadcastedRelation.value(i)
          buildSide match {
            case BuildRight if boundCondition(joinedRow(streamedRow, broadcastedRow)) =>
              matchedRows += joinedRow(streamedRow, broadcastedRow).copy()
              streamRowMatched = true
              includedBroadcastTuples += i
            case BuildLeft if boundCondition(joinedRow(broadcastedRow, streamedRow)) =>
              matchedRows += joinedRow(broadcastedRow, streamedRow).copy()
              streamRowMatched = true
              includedBroadcastTuples += i
            case _ =>
          }
          i += 1
        }

        (streamRowMatched, joinType, buildSide) match {
          case (false, LeftOuter | FullOuter, BuildRight) =>
            matchedRows += joinedRow(streamedRow, rightNulls).copy()
          case (false, RightOuter | FullOuter, BuildLeft) =>
            matchedRows += joinedRow(leftNulls, streamedRow).copy()
          case _ =>
        }
      }
      Iterator((matchedRows, includedBroadcastTuples))
    }

    val includedBroadcastTuples = matchesOrStreamedRowsWithNulls.map(_._2)
    val allIncludedBroadcastTuples =
      if (includedBroadcastTuples.count == 0) {
        new scala.collection.mutable.BitSet(broadcastedRelation.value.size)
      } else {
        includedBroadcastTuples.reduce(_ ++ _)
      }

    val leftNulls = new GenericMutableRow(left.output.size)
    val rightNulls = new GenericMutableRow(right.output.size)
    /** Rows from broadcasted joined with nulls. */
    val broadcastRowsWithNulls: Seq[Row] = {
      val arrBuf: collection.mutable.ArrayBuffer[Row] = collection.mutable.ArrayBuffer()
      var i = 0
      val rel = broadcastedRelation.value
      while (i < rel.length) {
        if (!allIncludedBroadcastTuples.contains(i)) {
          (joinType, buildSide) match {
            case (RightOuter | FullOuter, BuildRight) => arrBuf += new JoinedRow(leftNulls, rel(i))
            case (LeftOuter | FullOuter, BuildLeft) => arrBuf += new JoinedRow(rel(i), rightNulls)
            case _ =>
          }
        }
        i += 1
      }
      arrBuf.toSeq
    }

    // TODO: Breaks lineage.
    sparkContext.union(
      matchesOrStreamedRowsWithNulls.flatMap(_._1), sparkContext.makeRDD(broadcastRowsWithNulls))
  }
}
