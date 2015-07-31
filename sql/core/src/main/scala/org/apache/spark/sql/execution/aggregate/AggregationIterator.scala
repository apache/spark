package org.apache.spark.sql.execution.aggregate

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._

import scala.collection.mutable.ArrayBuffer

private[sql] abstract class AggregationIterator(
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression2],
    newMutableProjection: (Seq[Expression], Seq[Attribute]) => (() => MutableProjection),
    inputAttributes: Seq[Attribute],
    inputIter: Iterator[InternalRow]){

  ///////////////////////////////////////////////////////////////////////////
  // Static fields for this iterator
  ///////////////////////////////////////////////////////////////////////////

  protected val aggregateFunctions: Array[AggregateFunction2] = {
    var bufferOffset = initialBufferOffset
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
          BindReferences.bindReference(func, inputAttributes)
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

  // Positions of those non-algebraic aggregate functions in aggregateFunctions.
  // For example, we have func1, func2, func3, func4 in aggregateFunctions, and
  // func2 and func3 are non-algebraic aggregate functions.
  // nonAlgebraicAggregateFunctionPositions will be [1, 2].
  protected val nonAlgebraicAggregateFunctionPositions: Array[Int] = {
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

  // All non-algebraic aggregate functions.
  protected val nonAlgebraicAggregateFunctions: Array[AggregateFunction2] =
    nonAlgebraicAggregateFunctionPositions.map(aggregateFunctions)

  // The underlying buffer shared by all aggregate functions.
  protected val buffer: MutableRow = {
    // The number of elements of the underlying buffer of this operator.
    // All aggregate functions are sharing this underlying buffer and they find their
    // buffer values through bufferOffset.
    var size = initialBufferOffset
    var i = 0
    while (i < aggregateFunctions.length) {
      size += aggregateFunctions(i).bufferSchema.length
      i += 1
    }
    new GenericMutableRow(size)
  }

  /** Initializes buffer values for all aggregate functions. */
  protected def initializeBuffer(): Unit = {
    algebraicInitialProjection(EmptyRow)
    var i = 0
    while (i < nonAlgebraicAggregateFunctions.length) {
      nonAlgebraicAggregateFunctions(i).initialize(buffer)
      i += 1
    }
  }

  protected val joinedRow = new JoinedRow

  // This is used to project expressions for the grouping expressions.
  protected val groupGenerator =
    newMutableProjection(groupingExpressions, inputAttributes)()

  protected def initialBufferOffset: Int

  protected val placeholderExpressions = Seq.fill(initialBufferOffset)(NoOp)

  // This projection is used to initialize buffer values for all AlgebraicAggregates.
  protected val algebraicInitialProjection = {
    val initExpressions = placeholderExpressions ++ aggregateFunctions.flatMap {
      case ae: AlgebraicAggregate => ae.initialValues
      case agg: AggregateFunction2 => Seq.fill(agg.bufferAttributes.length)(NoOp)
    }
    newMutableProjection(initExpressions, Nil)().target(buffer)
  }


}
