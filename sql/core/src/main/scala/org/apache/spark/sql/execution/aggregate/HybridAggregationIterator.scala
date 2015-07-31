package org.apache.spark.sql.execution.aggregate

import org.apache.spark.{Logging, SparkEnv, TaskContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering
import org.apache.spark.sql.execution.UnsafeExternalAggregation
import org.apache.spark.sql.types.{StructField, StructType, NullType}

/**
 * An iterator used to do partial aggregations for hybrid aggregate
 */
class HybridPartialAggregationIterator(
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression2],
    newMutableProjection: (Seq[Expression], Seq[Attribute]) => (() => MutableProjection),
    inputAttributes: Seq[Attribute],
    inputIter: Iterator[InternalRow])
  extends AggregationIterator(
    groupingExpressions,
    aggregateExpressions,
    newMutableProjection,
    inputAttributes,
    inputIter)
  with Logging {

  override protected def initialBufferOffset: Int = 0

  // This projection is used to update buffer values for all AlgebraicAggregates.
  private val algebraicUpdateProjection = {
    val bufferSchema = aggregateFunctions.flatMap(_.bufferAttributes)
    val updateExpressions = aggregateFunctions.flatMap {
      case ae: AlgebraicAggregate => ae.updateExpressions
      case agg: AggregateFunction2 => Seq.fill(agg.bufferAttributes.length)(NoOp)
    }
    newMutableProjection(updateExpressions, bufferSchema ++ inputAttributes)()
  }

  // This projection is used to merge buffer values for all AlgebraicAggregates.
  private val algebraicMergeProjection = {
    val bufferSchemata = aggregateFunctions.flatMap(_.bufferAttributes) ++
      aggregateFunctions.flatMap(_.cloneBufferAttributes)
    val mergeExpressions = aggregateFunctions.flatMap {
      case ae: AlgebraicAggregate => ae.mergeExpressions
      case agg: AggregateFunction2 => Seq.fill(agg.bufferAttributes.length)(NoOp)
    }

    newMutableProjection(mergeExpressions, bufferSchemata)()
  }

  def iterator: Iterator[InternalRow] = {

    val groupingKeyOrdering: Ordering[InternalRow] = GenerateOrdering.generate(
      groupingExpressions.map(_.dataType).zipWithIndex.map { case(dt, index) =>
        new SortOrder(BoundReference(index, dt, nullable = true), Ascending)
        })

    val aggregationBufferSchema: StructType = StructType.fromAttributes(
      aggregateFunctions.flatMap(_.bufferAttributes)
    )

    val groupKeySchema: StructType = {
      val fields = groupingExpressions.zipWithIndex.map { case (expr, idx) =>
        StructField(idx.toString, expr.dataType, expr.nullable)
      }
      StructType(fields)
    }

    this.initializeBuffer()

    log.info(s"GroupKey Schema: ${groupKeySchema.mkString(",")}")
    log.info(s"AggregationBuffer Schema: ${aggregationBufferSchema.mkString(",")}")

    val sparkEnv: SparkEnv = SparkEnv.get
    val taskContext: TaskContext = TaskContext.get
    val aggregationMap = new UnsafeExternalAggregation(
      taskContext.taskMemoryManager,
      sparkEnv.shuffleMemoryManager,
      sparkEnv.blockManager,
      taskContext,
      algebraicUpdateProjection,
      algebraicMergeProjection,
      nonAlgebraicAggregateFunctions,
      buffer,
      aggregationBufferSchema,
      groupKeySchema,
      groupingKeyOrdering,
      1024 * 16,
      sparkEnv.conf,
      false
    )

    while (inputIter.hasNext) {
      val currentRow: InternalRow = inputIter.next()
      val groupKey: InternalRow = groupGenerator(currentRow).copy()
      aggregationMap.insertRow(groupKey, currentRow)
    }

    new Iterator[InternalRow] {
      private[this] val mapIterator = aggregationMap.iterator()

      def hasNext: Boolean = mapIterator.hasNext

      def next(): InternalRow = {
        val entry = mapIterator.next()
        if (hasNext) {
          joinedRow(entry.key, entry.value)
        } else {
          // This is the last element in the iterator, so let's free the buffer. Before we do,
          // though, we need to make a defensive copy of the result so that we don't return an
          // object that might contain dangling pointers to the freed memory
          val resultCopy = joinedRow(entry.key.copy(), entry.value.copy())
          aggregationMap.freeMemory()
          resultCopy
        }
      }
    }
  }
}

/**
 * An iterator used to do final aggregations for hybrid aggregate
 */
class HybridFinalAggregationIterator(
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression2],
    aggregateAttributes: Seq[Attribute],
    resultExpressions: Seq[NamedExpression],
    newMutableProjection: (Seq[Expression], Seq[Attribute]) => (() => MutableProjection),
    inputAttributes: Seq[Attribute],
    inputIter: Iterator[InternalRow])
  extends AggregationIterator(
    groupingExpressions,
    aggregateExpressions,
    newMutableProjection,
    inputAttributes,
    inputIter)
  with Logging {

  // The result of aggregate functions.
  private val aggregateResult: MutableRow = new GenericMutableRow(aggregateAttributes.length)

  // The projection used to generate the output rows of this operator.
  // This is only used when we are generating final results of aggregate functions.
  private val resultProjection =
    newMutableProjection(
      resultExpressions, groupingExpressions.map(_.toAttribute) ++ aggregateAttributes)()

  protected def initialBufferOffset: Int = 0

  private val offsetAttributes =
    Seq.fill(groupingExpressions.length)(AttributeReference("placeholder", NullType)())

  // This projection is used to partial merge buffer values for all AlgebraicAggregates.
  private val algebraicPartialMergeProjection = {
    val bufferSchema = aggregateFunctions.flatMap(_.bufferAttributes) ++
        offsetAttributes ++ aggregateFunctions.flatMap(_.cloneBufferAttributes)
    val mergeExpressions = aggregateFunctions.flatMap {
      case ae: AlgebraicAggregate => ae.mergeExpressions
      case agg: AggregateFunction2 => Seq.fill(agg.bufferAttributes.length)(NoOp)
    }

    newMutableProjection(mergeExpressions, bufferSchema)()
  }

  // This projection is used to merge buffer values for all AlgebraicAggregates.
  private val algebraicMergeProjection = {
    val bufferSchemata = aggregateFunctions.flatMap(_.bufferAttributes) ++
      aggregateFunctions.flatMap(_.cloneBufferAttributes)
    val mergeExpressions = aggregateFunctions.flatMap {
      case ae: AlgebraicAggregate => ae.mergeExpressions
      case agg: AggregateFunction2 => Seq.fill(agg.bufferAttributes.length)(NoOp)
    }

    newMutableProjection(mergeExpressions, bufferSchemata)()
  }

  // This projection is used to evaluate all AlgebraicAggregates.
  private val algebraicEvalProjection = {
    val bufferSchemata = aggregateFunctions.flatMap(_.bufferAttributes) ++
      aggregateFunctions.flatMap(_.cloneBufferAttributes)
    val evalExpressions = aggregateFunctions.map {
      case ae: AlgebraicAggregate => ae.evaluateExpression
      case agg: AggregateFunction2 => NoOp
    }

    newMutableProjection(evalExpressions, bufferSchemata)()
  }

  def iterator: Iterator[InternalRow] = {

    val groupingKeyOrdering: Ordering[InternalRow] = GenerateOrdering.generate(
      groupingExpressions.map(_.dataType).zipWithIndex.map { case(dt, index) =>
        new SortOrder(BoundReference(index, dt, nullable = true), Ascending)
      })

    val aggregationBufferSchema: StructType = StructType.fromAttributes(
      aggregateFunctions.flatMap(_.bufferAttributes)
    )

    val groupKeySchema: StructType = {
      val fields = groupingExpressions.zipWithIndex.map { case (expr, idx) =>
        // This is a dummy field name
        StructField(idx.toString, expr.dataType, expr.nullable)
      }
      StructType(fields)
    }

    this.initializeBuffer()

    log.info(s"GroupKey Schema: ${groupKeySchema.mkString(",")}")
    log.info(s"AggregationBuffer Schema: ${aggregationBufferSchema.mkString(",")}")

    val sparkEnv: SparkEnv = SparkEnv.get
    val taskContext: TaskContext = TaskContext.get
    val aggregationMap = new UnsafeExternalAggregation(
      taskContext.taskMemoryManager,
      sparkEnv.shuffleMemoryManager,
      sparkEnv.blockManager,
      taskContext,
      algebraicPartialMergeProjection,
      algebraicMergeProjection,
      nonAlgebraicAggregateFunctions,
      buffer,
      aggregationBufferSchema,
      groupKeySchema,
      groupingKeyOrdering,
      1024 * 16,
      sparkEnv.conf,
      false
    )

    while (inputIter.hasNext) {
      val currentRow: InternalRow = inputIter.next()
      val groupKey: InternalRow = groupGenerator(currentRow).copy()
      aggregationMap.insertRow(groupKey, currentRow)
    }

    new Iterator[InternalRow] {
      private[this] val mapIterator = aggregationMap.iterator()
      private[this] var nextKey: InternalRow = _
      private[this] var nextValue: InternalRow = _

      def hasNext: Boolean = mapIterator.hasNext

      def next(): InternalRow = {
        val entry = mapIterator.next()
        if (hasNext) {
          nextKey = entry.key
          nextValue = entry.value
        } else {
          nextKey = entry.key.copy()
          nextValue = entry.value.copy()
          aggregationMap.freeMemory()
        }
        // Generate results for all algebraic aggregate functions.
        algebraicEvalProjection.target(aggregateResult)(nextValue)
        // Generate results for all non-algebraic aggregate functions.
        var i = 0
        while (i < nonAlgebraicAggregateFunctions.length) {
          aggregateResult.update(
            nonAlgebraicAggregateFunctionPositions(i),
            nonAlgebraicAggregateFunctions(i).eval(nextValue))
          i += 1
        }
        resultProjection(joinedRow(nextKey, aggregateResult))
      }
    }
  }

}

