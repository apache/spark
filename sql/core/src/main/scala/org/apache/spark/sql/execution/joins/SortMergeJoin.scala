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

package org.apache.spark.sql.execution.joins

import java.util.NoSuchElementException

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.{BinaryNode, SparkPlan}
import org.apache.spark.util.collection.CompactBuffer

/**
 * :: DeveloperApi ::
 * Performs an sort merge join of two child relations.
 */
@DeveloperApi
case class SortMergeJoin(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    left: SparkPlan,
    right: SparkPlan,
    condition: Option[Expression] = None) extends BinaryNode {

  val (streamedPlan, bufferedPlan, streamedKeys, bufferedKeys) = joinType match {
    case RightOuter => (right, left, rightKeys, leftKeys)
    case _ => (left, right, leftKeys, rightKeys)
  }

  override def output: Seq[Attribute] = joinType match {
    case Inner =>
      left.output ++ right.output
    case LeftOuter =>
      left.output ++ right.output.map(_.withNullability(true))
    case RightOuter =>
      left.output.map(_.withNullability(true)) ++ right.output
    case FullOuter =>
      left.output.map(_.withNullability(true)) ++ right.output.map(_.withNullability(true))
    case x =>
      throw new IllegalStateException(s"SortMergeJoin should not take $x as the JoinType")
  }

  override def outputPartitioning: Partitioning = joinType match {
    case FullOuter =>
      // when doing Full Outer join, NULL rows from both sides are not so partitioned.
      UnknownPartitioning(streamedPlan.outputPartitioning.numPartitions)
    case _ => streamedPlan.outputPartitioning
  }

  override def requiredChildDistribution: Seq[Distribution] =
    ClusteredDistribution(leftKeys) :: ClusteredDistribution(rightKeys) :: Nil

  // this is to manually construct an ordering that can be used to compare keys from both sides
  private val keyOrdering: RowOrdering = RowOrdering.forSchema(streamedKeys.map(_.dataType))

  override def outputOrdering: Seq[SortOrder] = joinType match {
    case FullOuter => Nil // when doing Full Outer join, NULL rows from both sides are not ordered.
    case _ => requiredOrders(streamedKeys)
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    requiredOrders(leftKeys) :: requiredOrders(rightKeys) :: Nil

  @transient protected lazy val streamedKeyGenerator =
    newProjection(streamedKeys, streamedPlan.output)
  @transient protected lazy val bufferedKeyGenerator =
    newProjection(bufferedKeys, bufferedPlan.output)

  // checks if the joinedRow can meet condition requirements
  @transient private[this] lazy val boundCondition =
    condition.map(newPredicate(_, streamedPlan.output ++ bufferedPlan.output)).getOrElse(
      (row: InternalRow) => true)

  private def requiredOrders(keys: Seq[Expression]): Seq[SortOrder] =
    keys.map(SortOrder(_, Ascending))

  protected override def doExecute(): RDD[InternalRow] = {
    val streamResults = streamedPlan.execute().map(_.copy())
    val bufferResults = bufferedPlan.execute().map(_.copy())

    streamResults.zipPartitions(bufferResults) ( (streamedIter, bufferedIter) => {
      // standard null rows
      val streamedNullRow = InternalRow.fromSeq(Seq.fill(bufferedPlan.output.length)(null))
      val bufferedNullRow = InternalRow.fromSeq(Seq.fill(bufferedPlan.output.length)(null))
      new Iterator[InternalRow] {
        // Mutable per row objects.
        private[this] val joinRow = new JoinedRow
        private[this] var streamedElement: InternalRow = _
        private[this] var bufferedElement: InternalRow = _
        private[this] var streamedKey: InternalRow = _
        private[this] var bufferedKey: InternalRow = _
        private[this] var bufferedMatches: CompactBuffer[InternalRow] = _
        private[this] var bufferedPosition: Int = -1
        private[this] var stop: Boolean = false
        private[this] var matchKey: InternalRow = _
        // when we do merge algorithm and find some not matched join key, there must be a side
        // that do not have a corresponding match. So we need to mark which side it is. True means
        // streamed side not have match, and False means the buffered side. Only set when needed.
        private[this] var continueStreamed: Boolean = _
        // when we do full outer join and find all matched keys, we put a null stream row into
        // this to tell next() that we need to combine null stream row with all rows that not match
        // conditions.
        private[this] var secondStreamedElement: InternalRow = _
        // Stores rows that match the join key but not match conditions.
        // These rows will be useful when we are doing Full Outer Join.
        private[this] var secondBufferedMatches: CompactBuffer[InternalRow] = _

        // initialize iterator
        initialize()

        override final def hasNext: Boolean = nextMatchingPair()

        override final def next(): InternalRow = {
          if (hasNext) {
            if (bufferedMatches == null || bufferedMatches.size == 0) {
              // we just found a row with no join match and we are here to produce a row
              // with this row and a standard null row from the other side.
              if (continueStreamed) {
                val joinedRow = smartJoinRow(streamedElement, bufferedNullRow)
                fetchStreamed()
                joinedRow
              } else {
                val joinedRow = smartJoinRow(streamedNullRow, bufferedElement)
                fetchBuffered()
                joinedRow
              }
            } else {
              // we are using the buffered right rows and run down left iterator
              val joinedRow = smartJoinRow(streamedElement, bufferedMatches(bufferedPosition))
              bufferedPosition += 1
              if (bufferedPosition >= bufferedMatches.size) {
                bufferedPosition = 0
                if (joinType != FullOuter || secondStreamedElement == null) {
                  fetchStreamed()
                  if (streamedElement == null || keyOrdering.compare(streamedKey, matchKey) != 0) {
                    stop = false
                    bufferedMatches = null
                  }
                } else {
                  // in FullOuter join and the first time we finish the match buffer,
                  // we still want to generate all rows with streamed null row and buffered
                  // rows that match the join key but not the conditions.
                  streamedElement = secondStreamedElement
                  bufferedMatches = secondBufferedMatches
                  secondStreamedElement = null
                  secondBufferedMatches = null
                }
              }
              joinedRow
            }
          } else {
            // no more result
            throw new NoSuchElementException
          }
        }

        private def smartJoinRow(streamedRow: InternalRow, bufferedRow: InternalRow): InternalRow =
          joinType match {
            case RightOuter => joinRow(bufferedRow, streamedRow)
            case _ => joinRow(streamedRow, bufferedRow)
          }

        private def fetchStreamed(): Unit = {
          if (streamedIter.hasNext) {
            streamedElement = streamedIter.next()
            streamedKey = streamedKeyGenerator(streamedElement)
          } else {
            streamedElement = null
          }
        }

        private def fetchBuffered(): Unit = {
          if (bufferedIter.hasNext) {
            bufferedElement = bufferedIter.next()
            bufferedKey = bufferedKeyGenerator(bufferedElement)
          } else {
            bufferedElement = null
          }
        }

        private def initialize() = {
          fetchStreamed()
          fetchBuffered()
        }

        /**
         * Searches the right iterator for the next rows that have matches in left side, and store
         * them in a buffer.
         * When this is not a Inner join, we will also return true when we get a row with no match
         * on the other side. This search will jump out every time from the same position until
         * `next()` is called.
         * Unless we call `next()`, this function can be called multiple times, with the same
         * return value and result as running it once, since we have set guardians in it.
         *
         * @return true if the search is successful, and false if the right iterator runs out of
         *         tuples.
         */
        private def nextMatchingPair(): Boolean = {
          if (!stop && streamedElement != null) {
            // step 1: run both side to get the first match pair
            while (!stop && streamedElement != null && bufferedElement != null) {
              val comparing = keyOrdering.compare(streamedKey, bufferedKey)
              // for inner join, we need to filter those null keys
              stop = comparing == 0 && !streamedKey.anyNull
              if (comparing > 0 || bufferedKey.anyNull) {
                if (joinType == FullOuter) {
                  // the join type is full outer and the buffered side has a row with no
                  // join match, so we have a result row with streamed null with buffered
                  // side as this row. Then we fetch next buffered element and go back.
                  continueStreamed = false
                  return true
                } else {
                  fetchBuffered()
                }
              } else if (comparing < 0 || streamedKey.anyNull) {
                if (joinType == Inner) {
                  fetchStreamed()
                } else {
                  // the join type is not inner and the streamed side has a row with no
                  // join match, so we have a result row with this streamed row with buffered
                  // null row. Then we fetch next streamed element and go back.
                  continueStreamed = true
                  return true
                }
              }
            }
            // step 2: run down the buffered side to put all matched rows in a buffer
            bufferedMatches = new CompactBuffer[InternalRow]()
            secondBufferedMatches = new CompactBuffer[InternalRow]()
            if (stop) {
              stop = false
              // iterate the right side to buffer all rows that matches
              // as the records should be ordered, exit when we meet the first that not match
              while (!stop) {
                if (boundCondition(joinRow(streamedElement, bufferedElement))) {
                  bufferedMatches += bufferedElement
                } else if (joinType == FullOuter) {
                  bufferedMatches += bufferedNullRow
                  secondBufferedMatches += bufferedElement
                }
                fetchBuffered()
                stop =
                  keyOrdering.compare(streamedKey, bufferedKey) != 0 || bufferedElement == null
              }
              if (bufferedMatches.size == 0 && joinType != Inner) {
                bufferedMatches += bufferedNullRow
              }
              if (bufferedMatches.size > 0) {
                bufferedPosition = 0
                matchKey = streamedKey
                // secondBufferedMatches.size cannot be larger than bufferedMatches
                if (secondBufferedMatches.size > 0) {
                  secondStreamedElement = streamedNullRow
                }
              }
            }
          }
          // `stop` is false iff left or right has finished iteration in step 1.
          // if we get into step 2, `stop` cannot be false.
          if (!stop && (bufferedMatches == null || bufferedMatches.size == 0)) {
            if (streamedElement == null && bufferedElement != null) {
              // streamedElement == null but bufferedElement != null
              if (joinType == FullOuter) {
                continueStreamed = false
                return true
              }
            } else if (streamedElement != null && bufferedElement == null) {
              // bufferedElement == null but streamedElement != null
              if (joinType != Inner) {
                continueStreamed = true
                return true
              }
            }
          }
          bufferedMatches != null && bufferedMatches.size > 0
        }
      }
    })
  }
}
