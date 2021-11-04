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

import scala.collection.mutable

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, AttributeSeq, BindReferences, Expression, InterpretedMutableProjection, InterpretedUnsafeProjection, JoinedRow, MutableProjection, NamedExpression, Projection, SpecificInternalRow}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, DeclarativeAggregate, ImperativeAggregate, NoOp, TypedImperativeAggregate}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.util.AccumulatorV2

/**
 * Accumulator that computes a global aggregate.
 */
class AggregatingAccumulator private(
    bufferSchema: Seq[DataType],
    initialValues: Seq[Expression],
    updateExpressions: Seq[Expression],
    mergeExpressions: Seq[Expression],
    @transient private val resultExpressions: Seq[Expression],
    imperatives: Array[ImperativeAggregate],
    typedImperatives: Array[TypedImperativeAggregate[_]],
    @transient private val conf: SQLConf)
  extends AccumulatorV2[InternalRow, InternalRow] {
  assert(bufferSchema.size == initialValues.size)
  assert(bufferSchema.size == updateExpressions.size)
  assert(mergeExpressions == null || bufferSchema.size == mergeExpressions.size)

  @transient
  private var joinedRow: JoinedRow = _

  private var buffer: SpecificInternalRow = _

  private def createBuffer(): SpecificInternalRow = {
    val buffer = new SpecificInternalRow(bufferSchema)

    // Initialize the buffer. Note that we do not use a code generated projection here because
    // generating and compiling a projection is probably more expensive than using an interpreted
    // projection.
    InterpretedMutableProjection.createProjection(initialValues)
      .target(buffer)
      .apply(InternalRow.empty)
    imperatives.foreach(_.initialize(buffer))
    typedImperatives.foreach(_.initialize(buffer))
    buffer
  }

  private def getOrCreateBuffer(): SpecificInternalRow = {
    if (buffer == null) {
      buffer = createBuffer()

      // Create the joined row and set the buffer as its 'left' row.
      joinedRow = new JoinedRow()
      joinedRow.withLeft(buffer)
    }
    buffer
  }

  private def initializeProjection[T <: Projection](projection: T): T = {
    projection.initialize(TaskContext.getPartitionId())
    projection
  }

  @transient
  private[this] lazy val updateProjection = initializeProjection {
    MutableProjection.create(updateExpressions)
  }

  @transient
  private[this] lazy val mergeProjection = initializeProjection {
    InterpretedMutableProjection.createProjection(mergeExpressions)
  }

  @transient
  private[this] lazy val resultProjection = initializeProjection {
    InterpretedUnsafeProjection.createProjection(resultExpressions)
  }

  /**
   * Driver side operations like `merge` and `value` are executed in the DAGScheduler thread. This
   * thread does not have a SQL configuration so we attach our own here.
   */
  private[this] def withSQLConf[T](canRunOnExecutor: Boolean, default: => T)(body: => T): T = {
    if (conf != null) {
      // When we can reach here, we are on the driver side.
      SQLConf.withExistingConf(conf)(body)
    } else if (canRunOnExecutor) {
      body
    } else {
      default
    }
  }

  override def reset(): Unit = {
    buffer = null
    joinedRow = null
  }

  override def isZero: Boolean = buffer == null

  override def copyAndReset(): AggregatingAccumulator = {
    new AggregatingAccumulator(
      bufferSchema,
      initialValues,
      updateExpressions,
      mergeExpressions,
      resultExpressions,
      imperatives,
      typedImperatives,
      conf)
  }

  override def copy(): AggregatingAccumulator = {
    val copy = copyAndReset()
    copy.merge(this)
    copy
  }

  override def add(v: InternalRow): Unit = {
    val buffer = getOrCreateBuffer()
    updateProjection.target(buffer)(joinedRow.withRight(v))
    var i = 0
    while (i < imperatives.length) {
      imperatives(i).update(buffer, v)
      i += 1
    }
    i = 0
    while (i < typedImperatives.length) {
      typedImperatives(i).update(buffer, v)
      i += 1
    }
  }

  override def merge(
      other: AccumulatorV2[InternalRow, InternalRow]): Unit = withSQLConf(true, ()) {
    if (!other.isZero) {
      other match {
        case agg: AggregatingAccumulator =>
          val buffer = getOrCreateBuffer()
          val otherBuffer = agg.buffer
          mergeProjection.target(buffer)(joinedRow.withRight(otherBuffer))
          var i = 0
          while (i < imperatives.length) {
            imperatives(i).merge(buffer, otherBuffer)
            i += 1
          }
          i = 0
          if (isAtDriverSide) {
            while (i < typedImperatives.length) {
              // The input buffer stores serialized data
              typedImperatives(i).merge(buffer, otherBuffer)
              i += 1
            }
          } else {
            while (i < typedImperatives.length) {
              // The input buffer stores deserialized object
              typedImperatives(i).mergeBuffersObjects(buffer, otherBuffer)
              i += 1
            }
          }
        case _ =>
          throw QueryExecutionErrors.cannotMergeClassWithOtherClassError(
            this.getClass.getName, other.getClass.getName)
      }
    }
  }

  override def value: InternalRow = withSQLConf(false, InternalRow.empty) {
    // Either use the existing buffer or create a temporary one.
    val input = if (!isZero) {
      buffer
    } else {
      // Create a temporary buffer because we want to avoid changing the state of the accumulator
      // here, which would happen if we called getOrCreateBuffer(). This is relatively expensive to
      // do but it should be no problem since this method is supposed to be called rarely (once per
      // query execution).
      createBuffer()
    }
    resultProjection(input)
  }

  override def withBufferSerialized(): AggregatingAccumulator = {
    assert(!isAtDriverSide)
    var i = 0
    // AggregatingAccumulator runs on executor, we should serialize all TypedImperativeAggregate.
    while (i < typedImperatives.length) {
      typedImperatives(i).serializeAggregateBufferInPlace(buffer)
      i += 1
    }
    this
  }

  /**
   * Get the output schema of the aggregating accumulator.
   */
  lazy val schema: StructType = {
    StructType(resultExpressions.zipWithIndex.map {
      case (e: NamedExpression, _) => StructField(e.name, e.dataType, e.nullable, e.metadata)
      case (e, i) => StructField(s"c_$i", e.dataType, e.nullable)
    })
  }

  /**
   * Set the state of the accumulator to the state of another accumulator. This is used in cases
   * where we only want to publish the state of the accumulator when the task completes, see
   * [[CollectMetricsExec]] for an example.
   */
  private[execution] def setState(other: AggregatingAccumulator): Unit = {
    assert(buffer == null || (buffer eq other.buffer))
    buffer = other.buffer
    joinedRow = other.joinedRow
  }
}

object AggregatingAccumulator {
  /**
   * Create an aggregating accumulator for the given functions and input schema.
   */
  def apply(functions: Seq[Expression], inputAttributes: Seq[Attribute]): AggregatingAccumulator = {
    // There are a couple of things happening here:
    // - Collect the schema's of the aggregate and input aggregate buffers. These are needed to bind
    //   the expressions which will be done when we create the accumulator.
    // - Collect the initialValues, update and merge expressions for declarative aggregate
    //   functions.
    // - Bind and Collect the imperative aggregate functions. Note that we insert NoOps into the
    //   (declarative) initialValues, update and merge expression buffers to keep these aligned with
    //   the aggregate buffer.
    // - Build the result expressions.
    val aggBufferAttributes = mutable.Buffer.empty[AttributeReference]
    val inputAggBufferAttributes = mutable.Buffer.empty[AttributeReference]
    val initialValues = mutable.Buffer.empty[Expression]
    val updateExpressions = mutable.Buffer.empty[Expression]
    val mergeExpressions = mutable.Buffer.empty[Expression]
    val imperatives = mutable.Buffer.empty[ImperativeAggregate]
    val typedImperatives = mutable.Buffer.empty[TypedImperativeAggregate[_]]
    val inputAttributeSeq: AttributeSeq = inputAttributes
    val resultExpressions = functions.map(_.transform {
      case AggregateExpression(agg: DeclarativeAggregate, _, _, _, _) =>
        aggBufferAttributes ++= agg.aggBufferAttributes
        inputAggBufferAttributes ++= agg.inputAggBufferAttributes
        initialValues ++= agg.initialValues
        updateExpressions ++= agg.updateExpressions
        mergeExpressions ++= agg.mergeExpressions
        agg.evaluateExpression
      case AggregateExpression(agg: ImperativeAggregate, _, _, _, _) =>
        val imperative = BindReferences.bindReference(agg
          .withNewMutableAggBufferOffset(aggBufferAttributes.size)
          .withNewInputAggBufferOffset(inputAggBufferAttributes.size),
          inputAttributeSeq)
        imperative match {
          case typedImperative: TypedImperativeAggregate[_] =>
            typedImperatives += typedImperative
          case _ =>
            imperatives += imperative
        }
        aggBufferAttributes ++= imperative.aggBufferAttributes
        inputAggBufferAttributes ++= agg.inputAggBufferAttributes
        val noOps = Seq.fill(imperative.aggBufferAttributes.size)(NoOp)
        initialValues ++= noOps
        updateExpressions ++= noOps
        mergeExpressions ++= noOps
        imperative
    })

    val updateAttrSeq: AttributeSeq = (aggBufferAttributes ++ inputAttributes).toSeq
    val mergeAttrSeq: AttributeSeq = (aggBufferAttributes ++ inputAggBufferAttributes).toSeq
    val aggBufferAttributesSeq: AttributeSeq = aggBufferAttributes.toSeq

    // Create the accumulator.
    new AggregatingAccumulator(
      aggBufferAttributes.map(_.dataType).toSeq,
      initialValues.toSeq,
      updateExpressions.map(BindReferences.bindReference(_, updateAttrSeq)).toSeq,
      mergeExpressions.map(BindReferences.bindReference(_, mergeAttrSeq)).toSeq,
      resultExpressions.map(BindReferences.bindReference(_, aggBufferAttributesSeq)),
      imperatives.toArray,
      typedImperatives.toArray,
      SQLConf.get)
  }
}
