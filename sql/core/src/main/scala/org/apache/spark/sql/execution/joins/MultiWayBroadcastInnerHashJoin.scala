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

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.util.collection.CompactBuffer

/**
* :: DeveloperApi ::
* Equi-inner-join a single large table with multiple small broadcast tables.
*
* @param streamPlan the large table to stream through
* @param streamSideJoinKeys streaming keys to join
* @param broadcastPlans all the small (dimension) tables to broadcast and do inner join on.
*                       a sequence of (join keys, operator to generate the table).
*/
@DeveloperApi
case class MultiWayBroadcastInnerHashJoin(
    streamPlan: SparkPlan,
    streamSideJoinKeys: Seq[Seq[Expression]],
    broadcastPlans: Seq[(Seq[Expression], SparkPlan)])
  extends SparkPlan {

  override def outputPartitioning: Partitioning = streamPlan.outputPartitioning

  // TODO(rxin): Maybe memoize output and children?
  override def output: Seq[Attribute] = {
    streamPlan.output ++ broadcastPlans.flatMap(_._2.output)
  }

  override def children: Seq[SparkPlan] = Seq(streamPlan) ++ broadcastPlans.map(_._2)

  override def execute(): RDD[Row] = {
    // TODO(rxin0: Should we use mutable projection here?
    val streamSideKeyProjections: Array[Projection] = streamSideJoinKeys.map { keys =>
      newProjection(keys, streamPlan.output)
    }.toArray

    // TODO(rxin): Parallelize the broadcast.
    val broadcasts: Array[Broadcast[HashedRelation]] = {
      broadcastPlans.map { case (exprs, plan) =>
        val input = plan.executeCollect()
        val keyProjection = newProjection(exprs, plan.output)
        val hashed = HashedRelation(input.iterator, keyProjection, input.length)
        sparkContext.broadcast(hashed)
      }.toArray
    }

    streamPlan.execute().mapPartitions { streamedIter =>
      val hashedRelations: Array[HashedRelation] = broadcasts.map(_.value)
      multiwayHashJoin(streamedIter, streamSideKeyProjections, hashedRelations)
    }
  }

  private def multiwayHashJoin(
      streamIter: Iterator[Row],
      joinKeyGenerators: Array[Projection],
      hashedRelations: Array[HashedRelation]) = new Iterator[Row] {

    private[this] val numHashTables = broadcastPlans.size

    private[this] val streamSideJoinKeys =
      MultiWayBroadcastInnerHashJoin.this.streamSideJoinKeys.map { key =>
        newMutableProjection(key, streamPlan.output)()
      }.toArray

    private[this] var currentStreamRow: Row = null

    /** Whether we have a match for the current stream row. */
    private[this] var foundMatch = false

    /**
     * Matches of the current stream row from all hash tables. The output for this stream row
     * would be the cartesian product of this array.
     */
    private[this] val currentHashMatches = new Array[CompactBuffer[Row]](numHashTables)

    /**
     * Positions in each of the [[currentHashMatches]] that the current output row points to.
     */
    private[this] val currentMatchPositions = new Array[Int](numHashTables)

    /**
     * Number of output rows for the current stream row.
     * Equals the product of all sizes in [[currentHashMatches]].
     */
    private[this] var numTotalOutputForCurrentStreamRow = 0

    /**
     * Index of the current output row for the current stream row.
     * Equals the product of all values in currentMatchPositions.
     * When this is less than [[numTotalOutputForCurrentStreamRow]], we know we haven't exhausted
     * all the matches for the current stream row yet.
     */
    private[this] var numOutputForCurrentStreamRow = 0

    /** Output row returned by the iterator. */
    private[this] val outputRow: SpecificMutableRow = {
      val broadcastTypes = broadcastPlans.flatMap(_._2.output.map(_.dataType))
      val types = streamPlan.output.map(_.dataType) ++ broadcastTypes
      new SpecificMutableRow(types)
    }

    /**
     * Searches the streamed iterator for the next row that has at least one match in hash tables.
     *
     * @return true if the search is successful; false if the streamed iterator runs out of tuples.
     */
    private[this] final def fetchNext(): Boolean = {
      while (!foundMatch && streamIter.hasNext) {
        currentStreamRow = streamIter.next()

        foundMatch = true
        var i = 0
        while (foundMatch && i < numHashTables) {
          val joinKey = streamSideJoinKeys(i)(currentStreamRow)
          currentHashMatches(i) = hashedRelations(i).get(joinKey)
          if (currentHashMatches(i) eq null) {
            // quit
            foundMatch = false
          }
          i += 1
        }
      }

      if (foundMatch) {
        numTotalOutputForCurrentStreamRow = currentHashMatches.foldLeft(0)(_ * _.size)
        numOutputForCurrentStreamRow = 0

        java.util.Arrays.fill(currentMatchPositions, 0, numHashTables, 0)

        // Copy the stream side row to the output. This only needs to be done once.
        var pos = 0
        while (pos < streamPlan.output.length) {
          outputRow.update(pos, currentStreamRow(pos))
          pos += 1
        }
        true
      } else {
        false
      }
    }

    override def hasNext: Boolean = {
      numOutputForCurrentStreamRow < numTotalOutputForCurrentStreamRow ||
        (streamIter.hasNext && fetchNext())
    }

    override def next(): Row = {
      assert(numOutputForCurrentStreamRow < numTotalOutputForCurrentStreamRow)

      // Find the proper currentMatchPositions
      var i = numHashTables - 1
      assert(currentMatchPositions(i) <= currentHashMatches(i).size)
      while (currentMatchPositions(i) == currentHashMatches(i).size) {
        // i should never reach 0 given
        // numOutputForCurrentStreamRow < numTotalOutputForCurrentStreamRow
        assert(i > 0)
        currentMatchPositions(i) = 0
        currentMatchPositions(i - 1) += 1
        i -= 1
      }

      // Copy hash table data into outputRow from currentMatchPositions.
      // Note that streamRow should have already been copied in fetchNext().
      var pos = streamPlan.output.length
      i = 0
      // TODO(rxin): This whole copying can probably be optimized.
      while (i < numHashTables) {
        var j = 0
        val matched: Row = currentHashMatches(i)(currentMatchPositions(i))
        while (j < matched.length) {
          outputRow.update(pos, matched(j))
          j += 1
        }
        i += 1
        pos += 1
      }

      numOutputForCurrentStreamRow += 1
      currentMatchPositions(numHashTables - 1) += 1

      outputRow
    }
  }
}
