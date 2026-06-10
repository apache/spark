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

package org.apache.spark.sql.catalyst.expressions

import org.apache.datasketches.theta.SetOperation

import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, ExpressionDescription, Literal}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.{SketchEnvelope, SketchProfile, ThetaSketchUtils}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{AbstractDataType, BinaryType, DataType, IntegerType, LongType}

/**
 * Shared helpers for applying the sketch provenance envelope policy in the scalar Theta set
 * operations (theta_union / theta_difference / theta_intersection).
 */
private[expressions] object ThetaSetOpEnvelope {
  /** Unwrap an input buffer, returning its optional profile and the native payload. */
  def unwrap(value: Any): (Option[SketchProfile], Array[Byte]) =
    SketchEnvelope.unwrap(value.asInstanceOf[Array[Byte]])

  /** Validate two observed input profiles against each other per the compatibility policy. */
  def check(
      p1: Option[SketchProfile], p2: Option[SketchProfile], prettyName: String): Unit = {
    (p1, p2) match {
      case (Some(a), Some(b)) =>
        SketchEnvelope.assertCompatible(b, a, prettyName, SQLConf.get.sketchAllowVersionMismatch)
      case _ =>
    }
  }

  /** Wrap an output payload, propagating the first available input profile. */
  def wrap(
      payload: Array[Byte],
      p1: Option[SketchProfile],
      p2: Option[SketchProfile]): Array[Byte] = {
    if (SQLConf.get.sketchEnvelopeWriteEnabled) {
      p1.orElse(p2) match {
        case Some(p) => SketchEnvelope.wrap(payload, p)
        case None => payload
      }
    } else {
      payload
    }
  }
}

@ExpressionDescription(
  usage = """
    _FUNC_(expr) - Returns the estimated number of unique values
    given the binary representation of a Datasketches ThetaSketch. """,
  examples = """
    Examples:
      > SELECT _FUNC_(theta_sketch_agg(col)) FROM VALUES (1), (1), (2), (2), (3) tab(col);
       3
  """,
  group = "sketch_funcs",
  since = "4.1.0")
case class ThetaSketchEstimate(child: Expression)
    extends UnaryExpression
    with CodegenFallback
    with ExpectsInputTypes {
  override def nullIntolerant: Boolean = true

  override protected def withNewChildInternal(newChild: Expression): ThetaSketchEstimate =
    copy(child = newChild)

  override def prettyName: String = "theta_sketch_estimate"

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType)

  override def dataType: DataType = LongType

  override def nullSafeEval(input: Any): Any = {
    val (_, buffer) = SketchEnvelope.unwrap(input.asInstanceOf[Array[Byte]])

    val sketch = ThetaSketchUtils.wrapCompactSketch(buffer, prettyName)

    Math.round(sketch.getEstimate)
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(first, second, lgNomEntries) - Merges two binary representations of
    Datasketches ThetaSketch objects using a ThetaSketch Union object. Users can set
    lgNomEntries to a value between 4 and 26 to find the union of sketches with different
    union buffer size values (defaults to 12). """,
  examples = """
    Examples:
      > SELECT theta_sketch_estimate(_FUNC_(theta_sketch_agg(col1), theta_sketch_agg(col2))) FROM VALUES (1, 4), (1, 4), (2, 5), (2, 5), (3, 6) tab(col1, col2);
       6
  """,
  group = "sketch_funcs",
  since = "4.1.0")
// scalastyle:on line.size.limit
case class ThetaUnion(first: Expression, second: Expression, third: Expression)
    extends TernaryExpression
    with CodegenFallback
    with ExpectsInputTypes {
  override def nullIntolerant: Boolean = true

  def this(first: Expression, second: Expression) = {
    this(first, second, Literal(ThetaSketchUtils.DEFAULT_LG_NOM_LONGS))
  }

  def this(first: Expression, second: Expression, third: Int) = {
    this(first, second, Literal(third))
  }

  override protected def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression): ThetaUnion =
    copy(first = newFirst, second = newSecond, third = newThird)

  override def prettyName: String = "theta_union"

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType, BinaryType, IntegerType)

  override def dataType: DataType = BinaryType

  override def nullSafeEval(value1: Any, value2: Any, value3: Any): Any = {
    val logNominalEntries = value3.asInstanceOf[Int]
    ThetaSketchUtils.checkLgNomLongs(logNominalEntries, prettyName)

    val (profile1, sketch1Bytes) = ThetaSetOpEnvelope.unwrap(value1)
    val sketch1 = ThetaSketchUtils.wrapCompactSketch(sketch1Bytes, prettyName)

    val (profile2, sketch2Bytes) = ThetaSetOpEnvelope.unwrap(value2)
    val sketch2 = ThetaSketchUtils.wrapCompactSketch(sketch2Bytes, prettyName)

    ThetaSetOpEnvelope.check(profile1, profile2, prettyName)

    val union = SetOperation.builder
      .setLogNominalEntries(logNominalEntries)
      .buildUnion
      .union(sketch1, sketch2)

    ThetaSetOpEnvelope.wrap(union.toByteArrayCompressed, profile1, profile2)
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(first, second) - Subtracts two binary representations of
    Datasketches ThetaSketch objects from two input columns using a
    ThetaSketch AnotB object. """,
  examples = """
    Examples:
      > SELECT theta_sketch_estimate(_FUNC_(theta_sketch_agg(col1), theta_sketch_agg(col2))) FROM VALUES (5, 4), (1, 4), (2, 5), (2, 5), (3, 1) tab(col1, col2);
       2
  """,
  group = "sketch_funcs",
  since = "4.1.0")
// scalastyle:on line.size.limit
case class ThetaDifference(first: Expression, second: Expression)
    extends BinaryExpression
    with CodegenFallback
    with ExpectsInputTypes {
  override def nullIntolerant: Boolean = true

  override def left: Expression = first
  override def right: Expression = second

  override protected def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression): ThetaDifference =
    copy(first = newFirst, second = newSecond)

  override def prettyName: String = "theta_difference"

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType, BinaryType)

  override def dataType: DataType = BinaryType

  override def nullSafeEval(value1: Any, value2: Any): Any = {
    val (profile1, sketch1Bytes) = ThetaSetOpEnvelope.unwrap(value1)
    val sketch1 = ThetaSketchUtils.wrapCompactSketch(sketch1Bytes, prettyName)

    val (profile2, sketch2Bytes) = ThetaSetOpEnvelope.unwrap(value2)
    val sketch2 = ThetaSketchUtils.wrapCompactSketch(sketch2Bytes, prettyName)

    ThetaSetOpEnvelope.check(profile1, profile2, prettyName)

    val difference = SetOperation.builder.buildANotB
      .aNotB(sketch1, sketch2)

    ThetaSetOpEnvelope.wrap(difference.toByteArrayCompressed, profile1, profile2)
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(first, second) - Intersects two binary representations of
    Datasketches ThetaSketch objects from two input columns using a
    ThetaSketch Intersect object. """,
  examples = """
    Examples:
      > SELECT theta_sketch_estimate(_FUNC_(theta_sketch_agg(col1), theta_sketch_agg(col2))) FROM VALUES (5, 4), (1, 4), (2, 5), (2, 5), (3, 1) tab(col1, col2);
       2
  """,
  group = "sketch_funcs",
  since = "4.1.0")
// scalastyle:on line.size.limit
case class ThetaIntersection(first: Expression, second: Expression)
    extends BinaryExpression
    with CodegenFallback
    with ExpectsInputTypes {
  override def nullIntolerant: Boolean = true

  override def left: Expression = first
  override def right: Expression = second

  override protected def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression): ThetaIntersection =
    copy(first = newFirst, second = newSecond)

  override def prettyName: String = "theta_intersection"

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType, BinaryType)

  override def dataType: DataType = BinaryType

  override def nullSafeEval(value1: Any, value2: Any): Any = {
    val (profile1, sketch1Bytes) = ThetaSetOpEnvelope.unwrap(value1)
    val sketch1 = ThetaSketchUtils.wrapCompactSketch(sketch1Bytes, prettyName)

    val (profile2, sketch2Bytes) = ThetaSetOpEnvelope.unwrap(value2)
    val sketch2 = ThetaSketchUtils.wrapCompactSketch(sketch2Bytes, prettyName)

    ThetaSetOpEnvelope.check(profile1, profile2, prettyName)

    val intersection = SetOperation.builder.buildIntersection
      .intersect(sketch1, sketch2)

    ThetaSetOpEnvelope.wrap(intersection.toByteArrayCompressed, profile1, profile2)
  }
}
