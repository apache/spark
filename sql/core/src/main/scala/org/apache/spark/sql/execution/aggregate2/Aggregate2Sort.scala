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

package org.apache.spark.sql.execution.aggregate2

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate2._
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution, Distribution, UnspecifiedDistribution}
import org.apache.spark.sql.execution.{SparkPlan, UnaryNode}
import org.apache.spark.sql.types.NullType

import scala.collection.mutable.ArrayBuffer

case class Aggregate2Sort(
    preShuffle: Boolean,
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression2],
    aggregateAttributes: Seq[Attribute],
    resultExpressions: Seq[NamedExpression],
    child: SparkPlan)
  extends UnaryNode {


  override def requiredChildDistribution: List[Distribution] = {
    if (preShuffle) {
      UnspecifiedDistribution :: Nil
    } else {
      if (groupingExpressions == Nil) {
        AllTuples :: Nil
      } else {
        ClusteredDistribution(groupingExpressions) :: Nil
      }
    }
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    groupingExpressions.map(SortOrder(_, Ascending)) :: Nil

  override def output: Seq[Attribute] = resultExpressions.map(_.toAttribute)

  protected override def doExecute(): RDD[InternalRow] = attachTree(this, "execute") {
    child.execute().mapPartitions { iter =>

      new Iterator[InternalRow] {
        private val aggregateExprsWithBufferOffset = {
          var bufferOffset =
            if (preShuffle) {
              0
            } else {
              groupingExpressions.length
            }
          val bufferOffsets = new ArrayBuffer[Int]()
          var i = 0
          while (i < aggregateExpressions.length) {
            val func = aggregateExpressions(i).aggregateFunction
            bufferOffsets += bufferOffset
            bufferOffset = aggregateExpressions(i).mode match {
              case Partial | PartialMerge => bufferOffset + func.bufferSchema.length
              case Final | Complete => bufferOffset + 1
            }
            i += 1
          }
          aggregateExpressions.zip(bufferOffsets)
        }

        private val algebraicAggregateFunctions: Array[AlgebraicAggregate] = {
          aggregateExprsWithBufferOffset.collect {
            case (AggregateExpression2(agg: AlgebraicAggregate, mode, isDistinct), offset) =>
              agg.inputSchema = child.output
              agg.withBufferOffset(offset)
          }.toArray
        }

        private val nonAlgebraicAggregateFunctions: Array[AggregateFunction2] = {
          aggregateExprsWithBufferOffset.collect {
            case (AggregateExpression2(agg: AggregateFunction2, mode, isDistinct), offset)
              if !agg.isInstanceOf[AlgebraicAggregate] =>
              val func = agg.withBufferOffset(offset)
              mode match {
                case Partial | Complete =>
                  // Only need to bind reference when the function is not an AlgebraicAggregate
                  // and the mode is Partial or Complete.
                  BindReferences.bindReference(func, child.output)
                case _ => func
              }
          }.toArray
        }

        private val nonAlgebraicAggregateFunctionOrdinals: Array[Int] = {
          val ordinals = new ArrayBuffer[Int]()
          var i = 0
          while (i < aggregateExpressions.length) {
            aggregateExpressions(i).aggregateFunction match {
              case agg: AlgebraicAggregate =>
              case _ => ordinals += i
            }
            i += 1
          }
          ordinals.toArray
        }

        private val bufferSize: Int = {
          var size = 0
          var i = 0
          while (i < algebraicAggregateFunctions.length) {
            size += algebraicAggregateFunctions(i).bufferSchema.length
            i += 1
          }
          i = 0
          while (i < nonAlgebraicAggregateFunctions.length) {
            size += nonAlgebraicAggregateFunctions(i).bufferSchema.length
            i += 1
          }
          if (preShuffle) {
            size
          } else {
            groupingExpressions.length + size
          }
        }

        // This is used to project expressions for the grouping expressions.
        protected val groupGenerator =
          newMutableProjection(groupingExpressions, child.output)()
        // A ordering used to compare if a new row belongs to the current group
        // or a new group.
        private val groupOrdering: Ordering[InternalRow] =
          RowOrdering.forSchema(groupingExpressions.map(_.dataType))
        // The partition key of the current partition.
        private var currentGroupingKey: InternalRow = _
        // The partition key of next partition.
        private var nextGroupingKey: InternalRow = _
        // The first row of next partition.
        private var firstRowInNextGroup: InternalRow = _
        private var hasNewGroup: Boolean = true
        private val buffer: MutableRow = new GenericMutableRow(bufferSize)
        private val aggregateResult: MutableRow = new GenericMutableRow(aggregateAttributes.length)
        private val joinedRow = new JoinedRow4
        private lazy val resultProjection =
          newMutableProjection(
            resultExpressions, groupingExpressions.map(_.toAttribute) ++ aggregateAttributes)()

        val offsetAttributes = if (preShuffle) Nil else Seq.fill(groupingExpressions.length)(AttributeReference("offset", NullType)())
        val offsetExpressions = if (preShuffle) Nil else Seq.fill(groupingExpressions.length)(NoOp)

        val algebraicInitialProjection = {
          val initExpressions = offsetExpressions ++ algebraicAggregateFunctions.flatMap {
            case ae: AlgebraicAggregate => ae.initialValues
          }
          // println(initExpressions.mkString(","))

          newMutableProjection(initExpressions, Nil)().target(buffer)
        }

        lazy val algebraicUpdateProjection = {
          val bufferSchema = algebraicAggregateFunctions.flatMap {
            case ae: AlgebraicAggregate => ae.bufferAttributes
          }
          val updateExpressions = algebraicAggregateFunctions.flatMap {
            case ae: AlgebraicAggregate => ae.updateExpressions
          }

          // println(updateExpressions.mkString(","))
          newMutableProjection(updateExpressions, bufferSchema ++ child.output)().target(buffer)
        }

        lazy val algebraicMergeProjection = {
          val bufferSchemata =
            offsetAttributes ++ algebraicAggregateFunctions.flatMap {
              case ae: AlgebraicAggregate => ae.bufferAttributes
            } ++ offsetAttributes ++ algebraicAggregateFunctions.flatMap {
              case ae: AlgebraicAggregate => ae.rightBufferSchema
            }
          val mergeExpressions = offsetExpressions ++ algebraicAggregateFunctions.flatMap {
            case ae: AlgebraicAggregate => ae.mergeExpressions
          }

          newMutableProjection(mergeExpressions, bufferSchemata)()
        }

        lazy val algebraicEvalProjection = {
          val bufferSchemata =
            offsetAttributes ++ algebraicAggregateFunctions.flatMap {
              case ae: AlgebraicAggregate => ae.bufferAttributes
            } ++ offsetAttributes ++ algebraicAggregateFunctions.flatMap {
              case ae: AlgebraicAggregate => ae.rightBufferSchema
            }
          val evalExpressions = algebraicAggregateFunctions.map {
            case ae: AlgebraicAggregate => ae.evaluateExpression
          }

          newMutableProjection(evalExpressions, bufferSchemata)()
        }

        // Initialize this iterator.
        initialize()

        private def initialize(): Unit = {
          if (iter.hasNext) {
            initializeBuffer()
            val currentRow = iter.next().copy()
            // partitionGenerator is a mutable projection. Since we need to track nextGroupingKey,
            // we are making a copy at here.
            nextGroupingKey = groupGenerator(currentRow).copy()
            firstRowInNextGroup = currentRow
          } else {
            // This iter is an empty one.
            hasNewGroup = false
          }
        }

        private def initializeBuffer(): Unit = {
          algebraicInitialProjection(EmptyRow)
          var i = 0
          while (i < nonAlgebraicAggregateFunctions.length) {
            nonAlgebraicAggregateFunctions(i).initialize(buffer)
            i += 1
          }
          // println("initilized: " + buffer)
        }

        private def processRow(row: InternalRow): Unit = {
          // The new row is still in the current group.
          if (preShuffle) {
            algebraicUpdateProjection(joinedRow(buffer, row))
            var i = 0
            while (i < nonAlgebraicAggregateFunctions.length) {
              nonAlgebraicAggregateFunctions(i).update(buffer, row)
              i += 1
            }
          } else {
            algebraicMergeProjection.target(buffer)(joinedRow(buffer, row))
            var i = 0
            while (i < nonAlgebraicAggregateFunctions.length) {
              nonAlgebraicAggregateFunctions(i).merge(buffer, row)
              i += 1
            }
          }
        }

        private def iterateNextGroup(): Unit = {
          currentGroupingKey = nextGroupingKey
          // Now, we will start to find all rows belonging to this group.
          // Create a variable to track if we see the next group.
          var findNextPartition = false
          // The search will stop when we see the next group or there is no
          // input row left in the iter.
          processRow(firstRowInNextGroup)
          while (iter.hasNext && !findNextPartition) {
            val currentRow = iter.next()
            // Get the grouping key based on the grouping expressions.
            // For the below compare method, we do not need to make a copy of groupingKey.
            val groupingKey = groupGenerator(currentRow)
            // Check if the current row belongs the current input row.
            val comparing = groupOrdering.compare(currentGroupingKey, groupingKey)
            if (comparing == 0) {
              processRow(currentRow)
            } else {
              // We see a new group.
              findNextPartition = true
              nextGroupingKey = groupingKey.copy()
              firstRowInNextGroup = currentRow.copy()
            }
          }
          // We have not seen a new group. It means that there is no new row in the
          // iter. The current group is the last group of the iter.
          if (!findNextPartition) {
            hasNewGroup = false
          }
        }

        override final def hasNext: Boolean = hasNewGroup

        override final def next(): InternalRow = {
          if (hasNext) {
            iterateNextGroup()
            val outputRow =
              if (preShuffle) {
                // If it is preShuffle, we just output the grouping columns and the buffer.
                joinedRow(currentGroupingKey, buffer).copy()
              } else {
                algebraicEvalProjection.target(aggregateResult)(buffer)
                var i = 0
                while (i < nonAlgebraicAggregateFunctions.length) {
                  aggregateResult.update(
                    nonAlgebraicAggregateFunctionOrdinals(i),
                    nonAlgebraicAggregateFunctions(i).eval(buffer))
                  i += 1
                }
                resultProjection(joinedRow(currentGroupingKey, aggregateResult))

              }
            initializeBuffer()

            // println(s"outputRow $preShuffle " + outputRow)
            outputRow
          } else {
            // no more result
            throw new NoSuchElementException
          }
        }
      }
    }
  }
}
