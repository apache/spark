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
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.trees._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.PlatformDependent
import org.apache.spark.unsafe.array.ByteArrayMethods
import org.apache.spark.unsafe.map.BytesToBytesMap
import org.apache.spark.unsafe.memory.MemoryAllocator

// TODO: finish cleaning up documentation instead of just copying it

/**
 * TODO: copy of GeneratedAggregate that uses unsafe / offheap row implementations + hashtables.
 */
@DeveloperApi
case class UnsafeGeneratedAggregate(
  partial: Boolean,
  groupingExpressions: Seq[Expression],
  aggregateExpressions: Seq[NamedExpression],
  child: SparkPlan)
  extends UnaryNode {

  override def requiredChildDistribution: Seq[Distribution] =
    if (partial) {
      UnspecifiedDistribution :: Nil
    } else {
      if (groupingExpressions == Nil) {
        AllTuples :: Nil
      } else {
        ClusteredDistribution(groupingExpressions) :: Nil
      }
    }

  override def output: Seq[Attribute] = aggregateExpressions.map(_.toAttribute)

  override def execute(): RDD[Row] = {
    val aggregatesToCompute = aggregateExpressions.flatMap { a =>
      a.collect { case agg: AggregateExpression => agg}
    }

    // If you add any new function support, please add tests in org.apache.spark.sql.SQLQuerySuite
    // (in test "aggregation with codegen").
    val computeFunctions = aggregatesToCompute.map {
      case c @ Count(expr) =>
        // If we're evaluating UnscaledValue(x), we can do Count on x directly, since its
        // UnscaledValue will be null if and only if x is null; helps with Average on decimals
        val toCount = expr match {
          case UnscaledValue(e) => e
          case _ => expr
        }
        val currentCount = AttributeReference("currentCount", LongType, nullable = false)()
        val initialValue = Literal(0L)
        val updateFunction = If(IsNotNull(toCount), Add(currentCount, Literal(1L)), currentCount)
        val result = currentCount

        AggregateEvaluation(currentCount :: Nil, initialValue :: Nil, updateFunction :: Nil, result)

      case s @ Sum(expr) =>
        val calcType =
          expr.dataType match {
            case DecimalType.Fixed(_, _) =>
              DecimalType.Unlimited
            case _ =>
              expr.dataType
          }

        val currentSum = AttributeReference("currentSum", calcType, nullable = true)()
        val initialValue = Literal.create(null, calcType)

        // Coalesce avoids double calculation...
        // but really, common sub expression elimination would be better....
        val zero = Cast(Literal(0), calcType)
        val updateFunction = Coalesce(
          Add(
            Coalesce(currentSum :: zero :: Nil),
            Cast(expr, calcType)
          ) :: currentSum :: zero :: Nil)
        val result =
          expr.dataType match {
            case DecimalType.Fixed(_, _) =>
              Cast(currentSum, s.dataType)
            case _ => currentSum
          }

        AggregateEvaluation(currentSum :: Nil, initialValue :: Nil, updateFunction :: Nil, result)

      case cs @ CombineSum(expr) =>
        val calcType = expr.dataType
        expr.dataType match {
          case DecimalType.Fixed(_, _) =>
            DecimalType.Unlimited
          case _ =>
            expr.dataType
        }

        val currentSum = AttributeReference("currentSum", calcType, nullable = true)()
        val initialValue = Literal.create(null, calcType)

        // Coalasce avoids double calculation...
        // but really, common sub expression elimination would be better....
        val zero = Cast(Literal(0), calcType)
        // If we're evaluating UnscaledValue(x), we can do Count on x directly, since its
        // UnscaledValue will be null if and only if x is null; helps with Average on decimals
        val actualExpr = expr match {
          case UnscaledValue(e) => e
          case _ => expr
        }
        // partial sum result can be null only when no input rows present 
        val updateFunction = If(
          IsNotNull(actualExpr),
          Coalesce(
            Add(
              Coalesce(currentSum :: zero :: Nil),
              Cast(expr, calcType)) :: currentSum :: zero :: Nil),
          currentSum)

        val result =
          expr.dataType match {
            case DecimalType.Fixed(_, _) =>
              Cast(currentSum, cs.dataType)
            case _ => currentSum
          }

        AggregateEvaluation(currentSum :: Nil, initialValue :: Nil, updateFunction :: Nil, result)

      case m @ Max(expr) =>
        val currentMax = AttributeReference("currentMax", expr.dataType, nullable = true)()
        val initialValue = Literal.create(null, expr.dataType)
        val updateMax = MaxOf(currentMax, expr)

        AggregateEvaluation(
          currentMax :: Nil,
          initialValue :: Nil,
          updateMax :: Nil,
          currentMax)

      case m @ Min(expr) =>
        val currentMin = AttributeReference("currentMin", expr.dataType, nullable = true)()
        val initialValue = Literal.create(null, expr.dataType)
        val updateMin = MinOf(currentMin, expr)

        AggregateEvaluation(
          currentMin :: Nil,
          initialValue :: Nil,
          updateMin :: Nil,
          currentMin)

      case CollectHashSet(Seq(expr)) =>
        val set =
          AttributeReference("hashSet", new OpenHashSetUDT(expr.dataType), nullable = false)()
        val initialValue = NewSet(expr.dataType)
        val addToSet = AddItemToSet(expr, set)

        AggregateEvaluation(
          set :: Nil,
          initialValue :: Nil,
          addToSet :: Nil,
          set)

      case CombineSetsAndCount(inputSet) =>
        val elementType = inputSet.dataType.asInstanceOf[OpenHashSetUDT].elementType
        val set =
          AttributeReference("hashSet", new OpenHashSetUDT(elementType), nullable = false)()
        val initialValue = NewSet(elementType)
        val collectSets = CombineSets(set, inputSet)

        AggregateEvaluation(
          set :: Nil,
          initialValue :: Nil,
          collectSets :: Nil,
          CountSet(set))

      case o => sys.error(s"$o can't be codegened.")
    }

    val computationSchema = computeFunctions.flatMap(_.schema)

    val resultMap: Map[TreeNodeRef, Expression] =
      aggregatesToCompute.zip(computeFunctions).map {
        case (agg, func) => new TreeNodeRef(agg) -> func.result
      }.toMap

    val namedGroups = groupingExpressions.zipWithIndex.map {
      case (ne: NamedExpression, _) => (ne, ne)
      case (e, i) => (e, Alias(e, s"GroupingExpr$i")())
    }

    val groupMap: Map[Expression, Attribute] =
      namedGroups.map { case (k, v) => k -> v.toAttribute}.toMap

    // The set of expressions that produce the final output given the aggregation buffer and the
    // grouping expressions.
    val resultExpressions = aggregateExpressions.map(_.transform {
      case e: Expression if resultMap.contains(new TreeNodeRef(e)) => resultMap(new TreeNodeRef(e))
      case e: Expression if groupMap.contains(e) => groupMap(e)
    })

    child.execute().mapPartitions { iter =>
      // Builds a new custom class for holding the results of aggregation for a group.
      val initialValues = computeFunctions.flatMap(_.initialValues)
      val newAggregationBuffer = newProjection(initialValues, child.output)
      log.info(s"Initial values: ${initialValues.mkString(",")}")

      // A projection that computes the group given an input tuple.
      val groupProjection = newProjection(groupingExpressions, child.output)
      log.info(s"Grouping Projection: ${groupingExpressions.mkString(",")}")

      // A projection that is used to update the aggregate values for a group given a new tuple.
      // This projection should be targeted at the current values for the group and then applied
      // to a joined row of the current values with the new input row.
      val updateExpressions = computeFunctions.flatMap(_.update)
      val updateSchema = computeFunctions.flatMap(_.schema) ++ child.output
      val updateProjection = newMutableProjection(updateExpressions, updateSchema)()
      log.info(s"Update Expressions: ${updateExpressions.mkString(",")}")

      // A projection that produces the final result, given a computation.
      val resultProjectionBuilder =
        newMutableProjection(
          resultExpressions,
          (namedGroups.map(_._2.toAttribute) ++ computationSchema).toSeq)
      log.info(s"Result Projection: ${resultExpressions.mkString(",")}")

      val joinedRow = new JoinedRow3

      if (groupingExpressions.isEmpty) {
        // TODO: Codegening anything other than the updateProjection is probably over kill.
        val buffer = newAggregationBuffer(EmptyRow).asInstanceOf[MutableRow]
        var currentRow: Row = null
        updateProjection.target(buffer)

        while (iter.hasNext) {
          currentRow = iter.next()
          updateProjection(joinedRow(buffer, currentRow))
        }

        val resultProjection = resultProjectionBuilder()
        Iterator(resultProjection(buffer))
      } else {
        // TODO: if we knew how many groups to expect, we could size this hashmap appropriately
        val buffers = new BytesToBytesMap(MemoryAllocator.HEAP, 128)

        // Set up the mutable "pointers" that we'll re-use when pointing to key and value rows
        val keyPointer: UnsafeRow = new UnsafeRow()
        val currentBuffer: UnsafeRow = new UnsafeRow()

        // We're going to need to allocate a lot of empty aggregation buffers, so let's do it
        // once and keep a copy of the serialized buffer and copy it into the hash map when we see
        // new keys:
        val javaAggregationBuffer: MutableRow =
          newAggregationBuffer(EmptyRow).asInstanceOf[MutableRow]
        val numberOfFieldsInAggregationBuffer: Int = javaAggregationBuffer.schema.fields.length
        val aggregationBufferSchema: StructType = javaAggregationBuffer.schema
        // TODO perform that conversion to an UnsafeRow
        // Allocate some scratch space for holding the keys that we use to index into the hash map.
        val unsafeRowBuffer: Array[Long] = new Array[Long](1024)

        // TODO: there's got got to be an actual way of obtaining this up front.
        var groupProjectionSchema: StructType = null

        while (iter.hasNext) {
          // Zero out the buffer that's used to hold the current row. This is necessary in order
          // to ensure that rows hash properly, since garbage data from the previous row could
          // otherwise end up as padding in this row.
          ByteArrayMethods.zeroBytes(
            unsafeRowBuffer, PlatformDependent.LONG_ARRAY_OFFSET, unsafeRowBuffer.length)
          // Grab the next row from our input iterator and compute its group projection.
          // In the long run, it might be nice to use Unsafe rows for this as well, but for now
          // we'll just rely on the existing code paths to compute the projection.
          val currentJavaRow = iter.next()
          val currentGroup: Row = groupProjection(currentJavaRow)
          // Convert the current group into an UnsafeRow so that we can use it as a key for our
          // aggregation hash map
          // --- TODO ---
          val keyLengthInBytes: Int = 0
          val loc: BytesToBytesMap#Location =
            buffers.lookup(unsafeRowBuffer, PlatformDependent.LONG_ARRAY_OFFSET, keyLengthInBytes)
          if (!loc.isDefined) {
            // This is the first time that we've seen this key, so we'll copy the empty aggregation
            // buffer row that we created earlier.  TODO: this doesn't work very well for aggregates
            // where the size of the aggregate buffer is different for different rows (even if the
            // size of buffers don't grow once created, as is the case for things like grabbing the
            // first row's value for a string-valued column (or the shortest string)).

            // TODO

            loc.storeKeyAndValue(
              unsafeRowBuffer,
              PlatformDependent.LONG_ARRAY_OFFSET,
              keyLengthInBytes,
              null, // empty agg buffer
              PlatformDependent.LONG_ARRAY_OFFSET,
              0 // length of the aggregation buffer
            )
          }
          // Reset our pointer to point to the buffer stored in the hash map
          val address = loc.getValueAddress
          currentBuffer.set(
            address.getBaseObject,
            address.getBaseOffset,
            numberOfFieldsInAggregationBuffer,
            javaAggregationBuffer.schema
          )
          // Target the projection at the current aggregation buffer and then project the updated
          // values.
          updateProjection.target(currentBuffer)(joinedRow(currentBuffer, currentJavaRow))
        }

        new Iterator[Row] {
          private[this] val resultIterator = buffers.iterator()
          private[this] val resultProjection = resultProjectionBuilder()
          private[this] val key: UnsafeRow = new UnsafeRow()
          private[this] val value: UnsafeRow = new UnsafeRow()

          def hasNext: Boolean = resultIterator.hasNext

          def next(): Row = {
            val currentGroup: BytesToBytesMap#Location = resultIterator.next()
            val keyAddress = currentGroup.getKeyAddress
            key.set(
              keyAddress.getBaseObject,
              keyAddress.getBaseOffset,
              groupProjectionSchema.fields.length,
              groupProjectionSchema)
            val valueAddress = currentGroup.getValueAddress
            value.set(
              valueAddress.getBaseObject,
              valueAddress.getBaseOffset,
              aggregationBufferSchema.fields.length,
              aggregationBufferSchema
            )
            resultProjection(joinedRow(key, value))
          }
        }
      }
    }
  }
}
