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
import org.apache.spark.sql.catalyst.joins.HashedRelation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.util.collection.CompactBuffer

case class SingleHashJoin(
    plan: LogicalPlan,
    streamKeys: Seq[Expression],
    buildKeys: Seq[Expression],
    output: Seq[Attribute],
    hashTable: Broadcast[HashedRelation])

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
    output2: Seq[Attribute],
    joins: Seq[SingleHashJoin],
    streamPlan: SparkPlan)
  extends SparkPlan {

  override val output = streamPlan.output ++ joins.flatMap(_.output)

  override def outputPartitioning: Partitioning = streamPlan.outputPartitioning

  override def children: Seq[SparkPlan] = streamPlan :: Nil

  override def execute(): RDD[Row] = {

    streamPlan.execute().mapPartitions { streamedIter =>
      // TODO(rxin0: Should we use mutable projection here?
      val streamSideKeyProjections: Array[Projection] = joins.map(_.streamKeys).map { keys =>
        newProjection(keys, streamPlan.output)
      }.toArray

      val hashedRelations: Array[HashedRelation] = joins.map(_.hashTable.value).toArray
      multiwayHashJoin(streamedIter, streamSideKeyProjections, hashedRelations)
    }
  }

  private def multiwayHashJoin(
      streamIter: Iterator[Row],
      joinKeyGenerators: Array[Projection],
      hashedRelations: Array[HashedRelation]) = new Iterator[Row] {
    private[this] val numHashTables = joins.size

    private[this] val streamSideJoinKeys =
      joins.map(_.streamKeys).map { key =>
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
    private[this] var totalForCurrentStreamRow = 0

    /**
     * Number of rows we have already been produced for the current stream row.
     * When this is less than [[totalForCurrentStreamRow]], we know we haven't exhausted
     * all the matches for the current stream row yet.
     */
    private[this] var numOutputForCurrentStreamRow = 0

    /** Output row returned by the iterator. */
    private[this] val outputRow: SpecificMutableRow = {
      val broadcastTypes = joins.flatMap(_.output.map(_.dataType))
      val types = streamPlan.output.map(_.dataType) ++ broadcastTypes
      // logWarning(s"types: $types")
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
        foundMatch = false
        // logWarning(s"FoundMatches ${currentHashMatches.toSeq}")
        totalForCurrentStreamRow = currentHashMatches.foldLeft(1)(_ * _.size)
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
        // logWarning("No Match")
        false
      }
    }

    override def hasNext: Boolean = {
      val result =
        numOutputForCurrentStreamRow < totalForCurrentStreamRow ||
       (streamIter.hasNext && fetchNext())
      logWarning(s"$numOutputForCurrentStreamRow ?< $totalForCurrentStreamRow")
      logWarning(s"hasNext: $result")
      result
    }

    override def next(): Row = {
      assert(numOutputForCurrentStreamRow < totalForCurrentStreamRow, s"$numOutputForCurrentStreamRow !< $totalForCurrentStreamRow")

      // Find the proper currentMatchPositions
      var i = numHashTables - 1
      assert(currentMatchPositions(i) <= currentHashMatches(i).size)
      while (currentMatchPositions(i) == currentHashMatches(i).size) {
        // i should never reach 0 given
        // numOutputForCurrentStreamRow < totalForCurrentStreamRow
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
          // logWarning(s"position: $pos, $j")
          outputRow.update(pos, matched(j))
          j += 1
          pos += 1
        }
        i += 1
      }

      numOutputForCurrentStreamRow += 1
      logWarning(s"num output: $numOutputForCurrentStreamRow")
      currentMatchPositions(numHashTables - 1) += 1

      outputRow
    }
  }

  /** Appends the string represent of this node and its children to the given StringBuilder. */
  override def generateTreeString(depth: Int, builder: StringBuilder): StringBuilder = {
    builder.append(" " * depth)
    builder.append("MultiWayBroadcastInnerHashJoin")
    builder.append(output.mkString(" [", ",", "]"))
    builder.append("\n")

    joins.foreach { j =>
      builder.append(" " * (depth + 1))
      builder.append(j.streamKeys.mkString("[", ",", "]"))
      builder.append(" = ")
      builder.append(j.buildKeys.mkString("[", ",", "] "))
      builder.append(j.plan.simpleString.take(50))
      builder.append(if (j.plan.simpleString.length > 50) "..." else "")
      builder.append("\n")
    }
    children.foreach(_.generateTreeString(depth + 1, builder))
    builder
  }
}
