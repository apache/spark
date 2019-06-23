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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReferences
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.util.collection.BitSet

case class MergeAsOfJoinExec(
                              left: SparkPlan,
                              right: SparkPlan,
                              on: String,
                              leftKeys: Seq[Expression],
                              rightKeys: Seq[Expression]
                            ) extends BinaryExecNode {


  // override def output: Seq[Attribute] = left.output ++ right.output.map(_.withNullability((true)))
  // TODO filter

  override def output: Seq[Attribute] = left.output ++ right.output
  // TODO filter

  //override def output: Seq[Attribute] = left.output ++ right.output.drop(2)
  override def outputPartitioning: Partitioning = left.outputPartitioning

  override def requiredChildDistribution: Seq[Distribution] =
    HashClusteredDistribution(leftKeys) :: HashClusteredDistribution(rightKeys) :: Nil

  override def outputOrdering: Seq[SortOrder] = getKeyOrdering(leftKeys, left.outputOrdering)

  override def requiredChildOrdering: Seq[Seq[SortOrder]] = {
    leftKeys.map(SortOrder(_, Ascending)) :: rightKeys.map(SortOrder(_, Ascending)) :: Nil
  }

  // key generators spill and inmemory threshold

  private def getKeyOrdering(keys: Seq[Expression], childOutputOrdering: Seq[SortOrder])
    : Seq[SortOrder] = {
    val requiredOrdering = keys.map(SortOrder(_, Ascending))
    if (SortOrder.orderingSatisfies(childOutputOrdering, requiredOrdering)) {
      keys.zip(childOutputOrdering).map { case (key, childOrder) =>
        SortOrder(key, Ascending, childOrder.sameOrderExpressions + childOrder.child - key)
      }
    } else {
      requiredOrdering
    }
  }

  protected override def doExecute(): RDD[InternalRow] = {
    println("exec doexecute()")
    println("left output " + left.output + " right output " + right.output)

    // basic error checking
    // both data frames must be sorted by the key

      var numOutputRows: Int = 0
//    val numOutputRows = longMetric("numOutputRows")

    val inputSchema = left.output ++ right.output


    println(inputSchema)
    println(output)

    left.execute().zipPartitions(right.execute()) { (leftIter, rightIter) =>
      // println("left: " + leftIter.hasNext + " right: " + rightIter.hasNext)
      val resultProj: InternalRow => InternalRow = UnsafeProjection.create(output, inputSchema)
      if (!leftIter.hasNext || !rightIter.hasNext) {
        println("if")
        Iterator.empty
      }
      else {
        println("else")

        val joinedRow = new JoinedRow()
        val rfirstrow = rightIter.next()
//        val proj = UnsafeProjection.create(leftKeys, left.output)
        leftIter.map(leftrow => {
          //        resultProj(new joinedRow(leftrow, rfirstrow))
          //          resultProj(joinedRow(leftrow, rfirstrow))
          val ret = resultProj(joinedRow(leftrow, rfirstrow))
          println(ret)
          ret
          //        new JoinedRow(leftrow, rfirstrow)))
          //        UnsafeProjection.create(leftKeys)(joinedRow(leftrow, rfirstrow))
          //          joinedRow(leftrow, UnsafeProjection.create(leftKeys)(rfirstrow))
          }
        )
      }

      // figure out zip! TODO
      // how many parititons on left how many on right**

    }


  }


}
