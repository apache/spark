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
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression2],
    aggregateAttributes: Seq[Attribute],
    resultExpressions: Seq[NamedExpression],
    child: SparkPlan)
  extends UnaryNode {

  /** Indicates if this operator is for partial aggregations. */
  val partialAggregation: Boolean = {
    aggregateExpressions.map(_.mode).distinct.toList match {
      case Partial :: Nil => true
      case Final :: Nil => false
      case other =>
        sys.error(
          s"Could not evaluate ${aggregateExpressions} because we do not support evaluate " +
          s"modes $other in this operator.")
    }
  }

  override def requiredChildDistribution: List[Distribution] = {
    if (partialAggregation) {
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
        // aggregateFunctions contains all of aggregate functions used by this operator.
        // When populating aggregateFunctions, we also set bufferOffsets for those
        // functions and bind references for non-algebraic aggregate functions when
        // the mode is Partial or Complete.
        private val aggregateFunctions: Array[AggregateFunction2] = {
          var bufferOffset =
            if (partialAggregation) {
              0
            } else {
              groupingExpressions.length
            }
          val functions = new Array[AggregateFunction2](aggregateExpressions.length)
          var i = 0
          while (i < aggregateExpressions.length) {
            val func = aggregateExpressions(i).aggregateFunction
            val funcWithBoundReferences = aggregateExpressions(i).mode match {
              case Partial | Complete if !func.isInstanceOf[AlgebraicAggregate] =>
                // We need to create BoundReferences if the function is not an
                // AlgebraicAggregate (it does not support code-gen) and the mode of
                // this function is Partial or Complete because we will call eval of this
                // function's children in the update method of this aggregate function.
                // Those eval calls require BoundReferences to work.
                BindReferences.bindReference(func, child.output)
              case _ => func
            }
            // Set bufferOffset for this function. It is important that setting bufferOffset
            // happens after all potential bindReference operations because bindReference
            // will create a new instance of the function.
            funcWithBoundReferences.bufferOffset = bufferOffset
            bufferOffset += funcWithBoundReferences.bufferSchema.length
            functions(i) = funcWithBoundReferences
            i += 1
          }
          functions
        }

        // All non-algebraic aggregate functions.
        private val nonAlgebraicAggregateFunctions: Array[AggregateFunction2] = {
          aggregateFunctions.collect {
            case func: AggregateFunction2 if !func.isInstanceOf[AlgebraicAggregate] => func
          }.toArray
        }

        // Positions of those non-algebraic aggregate functions in aggregateFunctions.
        // For example, we have func1, func2, func3, func4 in aggregateFunctions, and
        // func2 and func3 are non-algebraic aggregate functions.
        // nonAlgebraicAggregateFunctionPositions will be [1, 2].
        private val nonAlgebraicAggregateFunctionPositions: Array[Int] = {
          val positions = new ArrayBuffer[Int]()
          var i = 0
          while (i < aggregateFunctions.length) {
            aggregateFunctions(i) match {
              case agg: AlgebraicAggregate =>
              case _ => positions += i
            }
            i += 1
          }
          positions.toArray
        }

        // The number of elements of the underlying buffer of this operator.
        // All aggregate functions are sharing this underlying buffer and they find their
        // buffer values through bufferOffset.
        private val bufferSize: Int = {
          var size = 0
          var i = 0
          while (i < aggregateFunctions.length) {
            size += aggregateFunctions(i).bufferSchema.length
            i += 1
          }
          if (partialAggregation) {
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
        private val groupOrdering: Ordering[InternalRow] = {
          val groupingAttributes = groupingExpressions.map(_.toAttribute)
          newOrdering(
            groupingAttributes.map(expr => SortOrder(expr, Ascending)),
            groupingAttributes)
        }
        // The partition key of the current partition.
        private var currentGroupingKey: InternalRow = _
        // The partition key of next partition.
        private var nextGroupingKey: InternalRow = _
        // The first row of next partition.
        private var firstRowInNextGroup: InternalRow = _
        // Indicates if we has new group of rows to process.
        private var hasNewGroup: Boolean = true
        // The underlying buffer shared by all aggregate functions.
        private val buffer: MutableRow = new GenericMutableRow(bufferSize)
        // The result of aggregate functions. It is only used when aggregate functions' modes
        // are Final.
        private val aggregateResult: MutableRow = new GenericMutableRow(aggregateAttributes.length)
        private val joinedRow = new JoinedRow4
        // The projection used to generate the output rows of this operator.
        // This is only used when we are generating final results of aggregate functions.
        private lazy val resultProjection =
          newMutableProjection(
            resultExpressions, groupingExpressions.map(_.toAttribute) ++ aggregateAttributes)()

        // When we merge buffers (for mode PartialMerge or Final), the input rows start with
        // values for grouping expressions. So, when we construct our buffer for this
        // aggregate function, the size of the buffer matches the number of values in the
        // input rows. To simplify the code for code-gen, we need create some dummy
        // attributes and expressions for these grouping expressions.
        val offsetAttributes = {
          if (partialAggregation) {
            Nil
          } else {
            Seq.fill(groupingExpressions.length)(AttributeReference("offset", NullType)())
          }
        }
        val offsetExpressions =
          if (partialAggregation) Nil else Seq.fill(groupingExpressions.length)(NoOp)

        // This projection is used to initialize buffer values for all AlgebraicAggregates.
        val algebraicInitialProjection = {
          val initExpressions = offsetExpressions ++ aggregateFunctions.flatMap {
            case ae: AlgebraicAggregate => ae.initialValues
            case agg: AggregateFunction2 => NoOp :: Nil
          }
          newMutableProjection(initExpressions, Nil)().target(buffer)
        }

        // This projection is used to update buffer values for all AlgebraicAggregates.
        lazy val algebraicUpdateProjection = {
          val bufferSchema = aggregateFunctions.flatMap {
            case ae: AlgebraicAggregate => ae.bufferAttributes
            case agg: AggregateFunction2 => agg.bufferAttributes
          }
          val updateExpressions = aggregateFunctions.flatMap {
            case ae: AlgebraicAggregate => ae.updateExpressions
            case agg: AggregateFunction2 => NoOp :: Nil
          }
          newMutableProjection(updateExpressions, bufferSchema ++ child.output)().target(buffer)
        }

        // This projection is used to merge buffer values for all AlgebraicAggregates.
        lazy val algebraicMergeProjection = {
          val bufferSchemata =
            offsetAttributes ++ aggregateFunctions.flatMap {
              case ae: AlgebraicAggregate => ae.bufferAttributes
              case agg: AggregateFunction2 => agg.bufferAttributes
            } ++ offsetAttributes ++ aggregateFunctions.flatMap {
              case ae: AlgebraicAggregate => ae.cloneBufferAttributes
              case agg: AggregateFunction2 => agg.cloneBufferAttributes
            }
          val mergeExpressions = offsetExpressions ++ aggregateFunctions.flatMap {
            case ae: AlgebraicAggregate => ae.mergeExpressions
            case agg: AggregateFunction2 => NoOp :: Nil
          }

          newMutableProjection(mergeExpressions, bufferSchemata)()
        }

        // This projection is used to evaluate all AlgebraicAggregates.
        lazy val algebraicEvalProjection = {
          val bufferSchemata =
            offsetAttributes ++ aggregateFunctions.flatMap {
              case ae: AlgebraicAggregate => ae.bufferAttributes
              case agg: AggregateFunction2 => agg.bufferAttributes
            } ++ offsetAttributes ++ aggregateFunctions.flatMap {
              case ae: AlgebraicAggregate => ae.cloneBufferAttributes
              case agg: AggregateFunction2 => agg.cloneBufferAttributes
            }
          val evalExpressions = aggregateFunctions.map {
            case ae: AlgebraicAggregate => ae.evaluateExpression
            case agg: AggregateFunction2 => NoOp
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

        /** Initializes buffer values for all aggregate functions. */
        private def initializeBuffer(): Unit = {
          algebraicInitialProjection(EmptyRow)
          var i = 0
          while (i < nonAlgebraicAggregateFunctions.length) {
            nonAlgebraicAggregateFunctions(i).initialize(buffer)
            i += 1
          }
        }

        /** Processes the current input row. */
        private def processRow(row: InternalRow): Unit = {
          // The new row is still in the current group.
          if (partialAggregation) {
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

        /** Processes rows in the current group. It will stop when it find a new group. */
        private def processCurrentGroup(): Unit = {
          currentGroupingKey = nextGroupingKey
          // Now, we will start to find all rows belonging to this group.
          // We create a variable to track if we see the next group.
          var findNextPartition = false
          // firstRowInNextGroup is the first row of this group. We first process it.
          processRow(firstRowInNextGroup)
          // The search will stop when we see the next group or there is no
          // input row left in the iter.
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
              // We find a new group.
              findNextPartition = true
              nextGroupingKey = groupingKey.copy()
              firstRowInNextGroup = currentRow.copy()
            }
          }
          // We have not seen a new group. It means that there is no new row in the input
          // iter. The current group is the last group of the iter.
          if (!findNextPartition) {
            hasNewGroup = false
          }
        }

        private def generateOutput: () => InternalRow = {
          if (partialAggregation) {
            // If it is partialAggregation, we just output the grouping columns and the buffer.
            () => joinedRow(currentGroupingKey, buffer).copy()
          } else {
            () => {
              algebraicEvalProjection.target(aggregateResult)(buffer)
              var i = 0
              while (i < nonAlgebraicAggregateFunctions.length) {
                aggregateResult.update(
                  nonAlgebraicAggregateFunctionPositions(i),
                  nonAlgebraicAggregateFunctions(i).eval(buffer))
                i += 1
              }
              resultProjection(joinedRow(currentGroupingKey, aggregateResult))
            }
          }
        }

        override final def hasNext: Boolean = hasNewGroup

        override final def next(): InternalRow = {
          if (hasNext) {
            // Process the current group.
            processCurrentGroup()
            // Generate output row for the current group.
            val outputRow = generateOutput()
            // Initilize buffer values for the next group.
            initializeBuffer()

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
