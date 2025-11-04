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

package org.apache.spark.sql.catalyst.expressions.aggregate

import org.apache.datasketches.memory.Memory
import org.apache.datasketches.theta.{CompactSketch, Intersection, SetOperation, Sketch, Union, UpdateSketch, UpdateSketchBuilder}

import org.apache.spark.SparkUnsupportedOperationException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, ExpressionDescription, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.TypedImperativeAggregate
import org.apache.spark.sql.catalyst.trees.{BinaryLike, UnaryLike}
import org.apache.spark.sql.catalyst.util.{ArrayData, CollationFactory, ThetaSketchUtils}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.internal.types.StringTypeWithCollation
import org.apache.spark.sql.types.{AbstractDataType, ArrayType, BinaryType, DataType, DoubleType, FloatType, IntegerType, LongType, StringType, TypeCollection}
import org.apache.spark.unsafe.types.UTF8String

sealed trait ThetaSketchState {
  def serialize(): Array[Byte]
  def eval(): Array[Byte]
}
case class UpdatableSketchBuffer(sketch: UpdateSketch) extends ThetaSketchState {
  override def serialize(): Array[Byte] = sketch.rebuild.compact.toByteArrayCompressed
  override def eval(): Array[Byte] = sketch.rebuild.compact.toByteArrayCompressed
}
case class UnionAggregationBuffer(union: Union) extends ThetaSketchState {
  override def serialize(): Array[Byte] = union.getResult.toByteArrayCompressed
  override def eval(): Array[Byte] = union.getResult.toByteArrayCompressed
}
case class IntersectionAggregationBuffer(intersection: Intersection) extends ThetaSketchState {
  override def serialize(): Array[Byte] = intersection.getResult.toByteArrayCompressed
  override def eval(): Array[Byte] = intersection.getResult.toByteArrayCompressed
}
case class FinalizedSketch(sketch: CompactSketch) extends ThetaSketchState {
  override def serialize(): Array[Byte] = sketch.toByteArrayCompressed
  override def eval(): Array[Byte] = sketch.toByteArrayCompressed
}

/**
 * The ThetaSketchAgg function utilizes a Datasketches ThetaSketch instance to count a
 * probabilistic approximation of the number of unique values in a given column, and outputs the
 * binary representation of the ThetaSketch.
 *
 * See [[https://datasketches.apache.org/docs/Theta/ThetaSketches.html]] for more information.
 *
 * @param left
 *   child expression against which unique counting will occur
 * @param right
 *   the log-base-2 of nomEntries decides the number of buckets for the sketch
 * @param mutableAggBufferOffset
 *   offset for mutable aggregation buffer
 * @param inputAggBufferOffset
 *   offset for input aggregation buffer
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(expr, lgNomEntries) - Returns the ThetaSketch compact binary representation.
      `lgNomEntries` (optional) is the log-base-2 of nominal entries, with nominal entries deciding
      the number buckets or slots for the ThetaSketch. """,
  examples = """
    Examples:
      > SELECT theta_sketch_estimate(_FUNC_(col, 12)) FROM VALUES (1), (1), (2), (2), (3) tab(col);
       3
  """,
  group = "agg_funcs",
  since = "4.1.0")
// scalastyle:on line.size.limit
case class ThetaSketchAgg(
    left: Expression,
    right: Expression,
    override val mutableAggBufferOffset: Int,
    override val inputAggBufferOffset: Int)
    extends TypedImperativeAggregate[ThetaSketchState]
    with BinaryLike[Expression]
    with ExpectsInputTypes {

  // ThetaSketch config - mark as lazy so that they're not evaluated during tree transformation.

  lazy val lgNomEntries: Int = {
    if (!right.foldable) {
      throw QueryExecutionErrors.thetaLgNomEntriesMustBeConstantError(prettyName)
    }
    val lgNomEntriesInput = right.eval().asInstanceOf[Int]
    ThetaSketchUtils.checkLgNomLongs(lgNomEntriesInput, prettyName)
    lgNomEntriesInput
  }

  // Constructors

  def this(child: Expression) = {
    this(child, Literal(ThetaSketchUtils.DEFAULT_LG_NOM_LONGS), 0, 0)
  }

  def this(child: Expression, lgNomEntries: Expression) = {
    this(child, lgNomEntries, 0, 0)
  }

  def this(child: Expression, lgNomEntries: Int) = {
    this(child, Literal(lgNomEntries), 0, 0)
  }

  // Copy constructors required by ImperativeAggregate

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ThetaSketchAgg =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ThetaSketchAgg =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): ThetaSketchAgg =
    copy(left = newLeft, right = newRight)

  // Overrides for TypedImperativeAggregate

  override def prettyName: String = "theta_sketch_agg"

  override def inputTypes: Seq[AbstractDataType] =
    Seq(
      TypeCollection(
        ArrayType(IntegerType),
        ArrayType(LongType),
        BinaryType,
        DoubleType,
        FloatType,
        IntegerType,
        LongType,
        StringTypeWithCollation(supportsTrimCollation = true)),
      IntegerType)

  override def dataType: DataType = BinaryType

  override def nullable: Boolean = false

  /**
   * Instantiate an UpdateSketch instance using the lgNomEntries param.
   *
   * @return
   *   an UpdateSketch instance wrapped with UpdatableSketchBuffer
   */
  override def createAggregationBuffer(): ThetaSketchState = {
    val builder = new UpdateSketchBuilder
    builder.setLogNominalEntries(lgNomEntries)
    UpdatableSketchBuffer(builder.build)
  }

  /**
   * Evaluate the input row and update the UpdateSketch instance with the row's value. The update
   * function only supports a subset of Spark SQL types, and an exception will be thrown for
   * unsupported types.
   * Notes:
   *   - Null values are ignored.
   *   - Empty byte arrays are ignored
   *   - Empty arrays of supported element types are ignored
   *   - Strings that are collation-equal to the empty string are ignored.
   *
   * @param updateBuffer
   *   A previously initialized UpdateSketch instance
   * @param input
   *   An input row
   */
  override def update(updateBuffer: ThetaSketchState, input: InternalRow): ThetaSketchState = {
    // Return early for null values.
    val v = left.eval(input)
    if (v == null) return updateBuffer

    // Initialized buffer should be UpdatableSketchBuffer, else error out.
    val sketch = updateBuffer match {
      case UpdatableSketchBuffer(s) => s
      case _ => throw QueryExecutionErrors.thetaInvalidInputSketchBuffer(prettyName)
    }

    // Handle the different data types for sketch updates.
    left.dataType match {
      case ArrayType(IntegerType, _) =>
        val arr = v.asInstanceOf[ArrayData].toIntArray()
        sketch.update(arr)
      case ArrayType(LongType, _) =>
        val arr = v.asInstanceOf[ArrayData].toLongArray()
        sketch.update(arr)
      case BinaryType =>
        val bytes = v.asInstanceOf[Array[Byte]]
        sketch.update(bytes)
      case DoubleType =>
        sketch.update(v.asInstanceOf[Double])
      case FloatType =>
        sketch.update(v.asInstanceOf[Float].toDouble) // Float is promoted to double.
      case IntegerType =>
        sketch.update(v.asInstanceOf[Int].toLong) // Int is promoted to Long.
      case LongType =>
        sketch.update(v.asInstanceOf[Long])
      case st: StringType =>
        val collation = CollationFactory.fetchCollation(st.collationId)
        val str = v.asInstanceOf[UTF8String]
        if (!collation.equalsFunction(str, UTF8String.EMPTY_UTF8)) {
          sketch.update(collation.sortKeyFunction.apply(str))
        }
      case _ =>
        throw new SparkUnsupportedOperationException(
          errorClass = "_LEGACY_ERROR_TEMP_3121",
          messageParameters = Map("dataType" -> left.dataType.toString))
    }

    UpdatableSketchBuffer(sketch)
  }

  /**
   * Merges an input Compact sketch into the UpdateSketch which is acting as the aggregation
   * buffer.
   *
   * @param updateBuffer
   *   The UpdateSketch or Union instance used to store the aggregation result
   * @param input
   *   An input UpdateSketch, Union, or Compact sketch instance
   */
  override def merge(
      updateBuffer: ThetaSketchState,
      input: ThetaSketchState): ThetaSketchState = {
    // This is a helper function to create union only when needed.
    def createUnionWith(sketch1: Sketch, sketch2: Sketch): UnionAggregationBuffer = {
      val union = SetOperation.builder.setLogNominalEntries(lgNomEntries).buildUnion
      union.union(sketch1)
      union.union(sketch2)
      UnionAggregationBuffer(union)
    }

    (updateBuffer, input) match {
      // Reuse the existing union in the next iteration. This is the most efficient path.
      case (UnionAggregationBuffer(existingUnion), UpdatableSketchBuffer(sketch)) =>
        existingUnion.union(sketch.compact)
        UnionAggregationBuffer(existingUnion)
      case (UnionAggregationBuffer(existingUnion), FinalizedSketch(sketch)) =>
        existingUnion.union(sketch)
        UnionAggregationBuffer(existingUnion)
      case (UnionAggregationBuffer(union1), UnionAggregationBuffer(union2)) =>
        union1.union(union2.getResult)
        UnionAggregationBuffer(union1)
      // Create a new union only when necessary.
      case (UpdatableSketchBuffer(sketch1), UpdatableSketchBuffer(sketch2)) =>
        createUnionWith(sketch1.compact, sketch2.compact)
      case (UpdatableSketchBuffer(sketch1), FinalizedSketch(sketch2)) =>
        createUnionWith(sketch1.compact, sketch2)
      // The program should never make it here, the cases are for defensive programming.
      case (FinalizedSketch(sketch1), UpdatableSketchBuffer(sketch2)) =>
        createUnionWith(sketch1, sketch2.compact)
      case (FinalizedSketch(sketch1), FinalizedSketch(sketch2)) =>
        createUnionWith(sketch1, sketch2)
      case _ => throw QueryExecutionErrors.thetaInvalidInputSketchBuffer(prettyName)
    }
  }

  /**
   * Returns a Compact sketch derived from the input column or expression
   *
   * @param sketchState
   *   Union instance used as an aggregation buffer
   * @return
   *   A Compact binary sketch
   */
  override def eval(sketchState: ThetaSketchState): Any = {
    sketchState.eval()
  }

  /** Convert the underlying UpdateSketch/Union into an Compact byte array. */
  override def serialize(sketchState: ThetaSketchState): Array[Byte] = {
    sketchState.serialize()
  }

  /** Wrap the byte array into a Compact sketch instance. */
  override def deserialize(buffer: Array[Byte]): ThetaSketchState = {
    if (buffer.nonEmpty) {
      FinalizedSketch(CompactSketch.heapify(Memory.wrap(buffer)))
    } else {
      this.createAggregationBuffer()
    }
  }
}

/**
 * The ThetaUnionAgg function ingests and merges Datasketches ThetaSketch instances previously
 * produced by the ThetaSketchAgg function and outputs the merged ThetaSketch.
 *
 * See [[https://datasketches.apache.org/docs/Theta/ThetaSketches.html]] for more information.
 *
 * @param left
 *   Child expression against which unique counting will occur
 * @param right
 *   the log-base-2 of nomEntries decides the number of buckets for the sketch
 * @param mutableAggBufferOffset
 *   offset for mutable aggregation buffer
 * @param inputAggBufferOffset
 *   offset for input aggregation buffer
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(expr, lgNomEntries) - Returns the ThetaSketch's Compact binary representation.
      `lgNomEntries` (optional) the log-base-2 of Nominal Entries, with Nominal Entries deciding
      the number buckets or slots for the ThetaSketch.""",
  examples = """
    Examples:
      > SELECT theta_sketch_estimate(_FUNC_(sketch)) FROM (SELECT theta_sketch_agg(col) as sketch FROM VALUES (1) tab(col) UNION ALL SELECT theta_sketch_agg(col, 20) as sketch FROM VALUES (1) tab(col));
       1
  """,
  group = "agg_funcs",
  since = "4.1.0")
// scalastyle:on line.size.limit
case class ThetaUnionAgg(
    left: Expression,
    right: Expression,
    override val mutableAggBufferOffset: Int,
    override val inputAggBufferOffset: Int)
    extends TypedImperativeAggregate[ThetaSketchState]
    with BinaryLike[Expression]
    with ExpectsInputTypes {

  // ThetaSketch config - mark as lazy so that they're not evaluated during tree transformation.

  lazy val lgNomEntries: Int = {
    if (!right.foldable) {
      throw QueryExecutionErrors.thetaLgNomEntriesMustBeConstantError(prettyName)
    }
    val lgNomEntriesInput = right.eval().asInstanceOf[Int]
    ThetaSketchUtils.checkLgNomLongs(lgNomEntriesInput, prettyName)
    lgNomEntriesInput
  }

  // Constructors

  def this(child: Expression) = {
    this(child, Literal(ThetaSketchUtils.DEFAULT_LG_NOM_LONGS), 0, 0)
  }

  def this(child: Expression, lgNomEntries: Expression) = {
    this(child, lgNomEntries, 0, 0)
  }

  def this(child: Expression, lgNomEntries: Int) = {
    this(child, Literal(lgNomEntries), 0, 0)
  }

  // Copy constructors required by ImperativeAggregate

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ThetaUnionAgg =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ThetaUnionAgg =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): ThetaUnionAgg =
    copy(left = newLeft, right = newRight)

  // Overrides for TypedImperativeAggregate

  override def prettyName: String = "theta_union_agg"

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType, IntegerType)

  override def dataType: DataType = BinaryType

  override def nullable: Boolean = false

  /**
   * Instantiate a Union instance using the lgNomEntries param.
   *
   * @return
   *   a Union instance wrapped with UnionAggregationBuffer
   */
  override def createAggregationBuffer(): ThetaSketchState = {
    UnionAggregationBuffer(
      SetOperation.builder
        .setLogNominalEntries(lgNomEntries)
        .buildUnion)
  }

  /**
   * Update the Union instance with the Compact sketch byte array obtained from the row.
   *
   * @param unionBuffer
   *   A previously initialized Union instance
   * @param input
   *   An input row
   */
  override def update(unionBuffer: ThetaSketchState, input: InternalRow): ThetaSketchState = {
    // Return early for null input values.
    val v = left.eval(input)
    if (v == null) return unionBuffer

    // Sketches must be in binary form to be aggregated, else error out.
    left.dataType match {
      case BinaryType => // Continue processing with a BinaryType.
      case _ => throw QueryExecutionErrors.thetaInvalidInputSketchBuffer(prettyName)
    }

    val sketchBytes = v.asInstanceOf[Array[Byte]]
    val inputSketch = ThetaSketchUtils.wrapCompactSketch(sketchBytes, prettyName)

    val union = unionBuffer match {
      case UnionAggregationBuffer(existingUnionBuffer) => existingUnionBuffer
      case _ => throw QueryExecutionErrors.thetaInvalidInputSketchBuffer(prettyName)
    }
    union.union(inputSketch)
    UnionAggregationBuffer(union)
  }

  /**
   * Merges an input Compact sketch into the Union which is acting as the aggregation buffer.
   *
   * @param unionBuffer
   *   The Union instance used to store the aggregation result
   * @param input
   *   An input Union or Compact sketch instance
   */
  override def merge(unionBuffer: ThetaSketchState, input: ThetaSketchState): ThetaSketchState = {
    (unionBuffer, input) match {
      // If both arguments are union objects, merge them directly.
      case (UnionAggregationBuffer(unionLeft), UnionAggregationBuffer(unionRight)) =>
        unionLeft.union(unionRight.getResult)
        UnionAggregationBuffer(unionLeft)
      // The input was serialized then deserialized.
      case (UnionAggregationBuffer(union), FinalizedSketch(sketch)) =>
        union.union(sketch)
        UnionAggregationBuffer(union)
      // The program should never make it here, the cases are for defensive programming.
      case (FinalizedSketch(sketch1), FinalizedSketch(sketch2)) =>
        val union = SetOperation.builder.setLogNominalEntries(lgNomEntries).buildUnion
        union.union(sketch1)
        union.union(sketch2)
        UnionAggregationBuffer(union)
      case (FinalizedSketch(sketch), UnionAggregationBuffer(union)) =>
        union.union(sketch)
        UnionAggregationBuffer(union)
      case _ => throw QueryExecutionErrors.thetaInvalidInputSketchBuffer(prettyName)
    }
  }

  /**
   * Returns a Compact sketch derived from the merged sketches
   *
   * @param sketchState
   *   Union instance used as an aggregation buffer
   * @return
   *   A Compact binary sketch
   */
  override def eval(sketchState: ThetaSketchState): Any = {
    sketchState.eval()
  }

  /** Converts the underlying Union into an Compact byte array. */
  override def serialize(sketchState: ThetaSketchState): Array[Byte] = {
    sketchState.serialize()
  }

  /** Wrap the byte array into a Compact sketch instance. */
  override def deserialize(buffer: Array[Byte]): ThetaSketchState = {
    if (buffer.nonEmpty) {
      FinalizedSketch(CompactSketch.heapify(Memory.wrap(buffer)))
    } else {
      this.createAggregationBuffer()
    }
  }
}

/**
 * The ThetaIntersectionAgg function ingests and intersects Datasketches ThetaSketch instances
 * previously produced by the ThetaSketchAgg function, and outputs the intersected ThetaSketch.
 *
 * See [[https://datasketches.apache.org/docs/Theta/ThetaSketches.html]] for more information.
 *
 * @param child
 *   Child expression against which unique counting will occur
 * @param mutableAggBufferOffset
 *   offset for mutable aggregation buffer
 * @param inputAggBufferOffset
 *   offset for input aggregation buffer
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(expr, lgNomEntries) - Returns the ThetaSketch's Compact binary representation
      by intersecting all the Theta sketches in the input column.""",
  examples = """
    Examples:
      > SELECT theta_sketch_estimate(_FUNC_(sketch)) FROM (SELECT theta_sketch_agg(col) as sketch FROM VALUES (1) tab(col) UNION ALL SELECT theta_sketch_agg(col, 20) as sketch FROM VALUES (1) tab(col));
       1
  """,
  group = "agg_funcs",
  since = "4.1.0")
// scalastyle:on line.size.limit
case class ThetaIntersectionAgg(
    child: Expression,
    override val mutableAggBufferOffset: Int,
    override val inputAggBufferOffset: Int)
    extends TypedImperativeAggregate[ThetaSketchState]
    with UnaryLike[Expression]
    with ExpectsInputTypes {

  // Constructor

  def this(child: Expression) = {
    this(child, 0, 0)
  }

  // Copy constructors required by ImperativeAggregate

  override def withNewMutableAggBufferOffset(
      newMutableAggBufferOffset: Int): ThetaIntersectionAgg =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ThetaIntersectionAgg =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override protected def withNewChildInternal(newChild: Expression): ThetaIntersectionAgg =
    copy(child = newChild)

  // Overrides for TypedImperativeAggregate

  override def prettyName: String = "theta_intersection_agg"

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType)

  override def dataType: DataType = BinaryType

  override def nullable: Boolean = false

  /**
   * Instantiate an Intersection instance using the lgNomEntries param.
   *
   * @return
   *   an Intersection instance wrapped with IntersectionAggregationBuffer
   */
  override def createAggregationBuffer(): ThetaSketchState = {
    IntersectionAggregationBuffer(SetOperation.builder.buildIntersection)
  }

  /**
   * Update the Intersection instance with the Compact sketch byte array obtained from the row.
   *
   * @param intersectionBuffer
   *   A previously initialized Intersection instance
   * @param input
   *   An input row
   */
  override def update(
      intersectionBuffer: ThetaSketchState,
      input: InternalRow): ThetaSketchState = {
    // Return early for null input values.
    val v = child.eval(input)
    if (v == null) return intersectionBuffer

    // Sketches must be in binary form to be aggregated, else error out.
    child.dataType match {
      case BinaryType => // Continue processing with a BinaryType.
      case _ => throw QueryExecutionErrors.thetaInvalidInputSketchBuffer(prettyName)
    }

    val sketchBytes = v.asInstanceOf[Array[Byte]]
    val inputSketch = ThetaSketchUtils.wrapCompactSketch(sketchBytes, prettyName)

    val intersection = intersectionBuffer match {
      case IntersectionAggregationBuffer(existingIntersection) => existingIntersection
      case _ => throw QueryExecutionErrors.thetaInvalidInputSketchBuffer(prettyName)
    }
    intersection.intersect(inputSketch)
    IntersectionAggregationBuffer(intersection)
  }

  /**
   * Merges an input Compact sketch into the Intersection which is acting as the aggregation
   * buffer.
   *
   * @param intersectionBuffer
   *   The Intersection instance used to store the aggregation result
   * @param input
   *   An input Intersection or Compact sketch instance
   */
  override def merge(
      intersectionBuffer: ThetaSketchState,
      input: ThetaSketchState): ThetaSketchState = {
    (intersectionBuffer, input) match {
      // If both arguments are intersection objects, merge them directly.
      case (
            IntersectionAggregationBuffer(intersectLeft),
            IntersectionAggregationBuffer(intersectRight)) =>
        intersectLeft.intersect(intersectRight.getResult)
        IntersectionAggregationBuffer(intersectLeft)
      // The input was serialized then deserialized.
      case (IntersectionAggregationBuffer(intersection), FinalizedSketch(sketch)) =>
        intersection.intersect(sketch)
        IntersectionAggregationBuffer(intersection)
      // The program should never make it here, the cases are for defensive programming.
      case (FinalizedSketch(sketch1), FinalizedSketch(sketch2)) =>
        val intersection =
          SetOperation.builder.buildIntersection
        intersection.intersect(sketch1)
        intersection.intersect(sketch2)
        IntersectionAggregationBuffer(intersection)
      case (FinalizedSketch(sketch), IntersectionAggregationBuffer(intersection)) =>
        intersection.intersect(sketch)
        IntersectionAggregationBuffer(intersection)
      case _ => throw QueryExecutionErrors.thetaInvalidInputSketchBuffer(prettyName)
    }
  }

  /**
   * Returns a Compact sketch derived from the intersected sketches
   *
   * @param sketchState
   *   Intersection instance used as an aggregation buffer
   * @return
   *   A Compact binary sketch
   */
  override def eval(sketchState: ThetaSketchState): Any = {
    sketchState.eval()
  }

  /** Convert the underlying Intersection into an Compact byte array. */
  override def serialize(sketchState: ThetaSketchState): Array[Byte] = {
    sketchState.serialize()
  }

  /** Wrap the byte array into a Compact sketch instance. */
  override def deserialize(buffer: Array[Byte]): ThetaSketchState = {
    if (buffer.nonEmpty) {
      FinalizedSketch(CompactSketch.heapify(Memory.wrap(buffer)))
    } else {
      this.createAggregationBuffer()
    }
  }
}
