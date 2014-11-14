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
import org.apache.spark.sql.catalyst.joins._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
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
case class OptimizedMultiWayBroadcastInnerHashJoin(
    output: Seq[Attribute],
    joins: Seq[SingleHashJoin],
    streamPlan: SparkPlan)
  extends SparkPlan {

  override def outputPartitioning: Partitioning = streamPlan.outputPartitioning

  override def children: Seq[SparkPlan] = streamPlan :: Nil

  override def execute(): RDD[Row] = {
    streamPlan.execute().mapPartitions { streamedIter =>
      val joinSeq =
        JoinSequence(streamPlan.output,
          output,
          joins.map(j =>
            IntJoin(
              j.streamKeys.head,
              j.output,
              j.hashTable.value.asInstanceOf[UniqueIntKeyHashedRelation])))

      GenerateInnerJoin(joinSeq)(streamedIter)
    }
  }

  /** Appends the string represent of this node and its children to the given StringBuilder. */
  override def generateTreeString(depth: Int, builder: StringBuilder): StringBuilder = {
    builder.append(" " * depth)
    builder.append("OptimizedMultiWayBroadcastInnerHashJoin")
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
