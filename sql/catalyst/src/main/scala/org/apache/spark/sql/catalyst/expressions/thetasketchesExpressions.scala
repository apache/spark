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

import org.apache.datasketches.common.SketchesArgumentException
import org.apache.datasketches.memory.Memory
import org.apache.datasketches.theta.{CompactSketch, SetOperation}
import org.apache.datasketches.thetacommon.ThetaUtil

import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, ExpressionDescription, Literal}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types.{AbstractDataType, BinaryType, DataType, IntegerType, LongType}

@ExpressionDescription(
  usage = """
    _FUNC_(expr) - Returns the estimated number of unique values given the binary representation
    of a Datasketches ThetaSketch. """,
  examples = """
    Examples:
      > SELECT _FUNC_(theta_sketch_agg(col)) FROM VALUES (1), (1), (2), (2), (3) tab(col);
       3
  """,
  group = "misc_funcs",
  since = "4.0.0")
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
    val buffer = input.asInstanceOf[Array[Byte]]
    try {
      Math.round(CompactSketch.wrap(Memory.wrap(buffer)).getEstimate)
    } catch {
      case _: SketchesArgumentException | _: java.lang.Error =>
        throw QueryExecutionErrors.thetaInvalidInputSketchBuffer(prettyName)
    }
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(first, second, lgNomEntries) - Merges two binary representations of
    Datasketches ThetaSketch objects, using a Datasketches Union object. Set
    lgNomEntries to a value between 4 and 26 to find the unions of sketches with different
    union buffer sizes values (defaults to 12). """,
  examples = """
    Examples:
      > SELECT theta_sketch_estimate(_FUNC_(theta_sketch_agg(col1), theta_sketch_agg(col2))) FROM VALUES (1, 4), (1, 4), (2, 5), (2, 5), (3, 6) tab(col1, col2);
       6
  """,
  group = "misc_funcs",
  since = "4.0.0")
// scalastyle:on line.size.limit
case class ThetaUnion(first: Expression, second: Expression, third: Expression)
    extends TernaryExpression
    with CodegenFallback
    with ExpectsInputTypes {
  override def nullIntolerant: Boolean = true

  def this(first: Expression, second: Expression) = {
    this(first, second, Literal(ThetaUtil.checkNomLongs(ThetaUtil.DEFAULT_NOMINAL_ENTRIES)))
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
    val sketch1 =
      try {
        CompactSketch.wrap(Memory.wrap(value1.asInstanceOf[Array[Byte]]))
      } catch {
        case _: SketchesArgumentException | _: java.lang.Error =>
          throw QueryExecutionErrors.thetaInvalidInputSketchBuffer(prettyName)
      }
    val sketch2 =
      try {
        CompactSketch.wrap(Memory.wrap(value2.asInstanceOf[Array[Byte]]))
      } catch {
        case _: SketchesArgumentException | _: java.lang.Error =>
          throw QueryExecutionErrors.thetaInvalidInputSketchBuffer(prettyName)
      }
    val logNominalEntries = value3.asInstanceOf[Int]
    val union = SetOperation.builder
      .setLogNominalEntries(logNominalEntries)
      .buildUnion
      .union(sketch1, sketch2)

    union.toByteArray
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(first, second, lgNomEntries) - Subtracts two binary representations of
    Datasketches ThetaSketch objects, using a Datasketches AnotB object. Set
    lgNomEntries to a value between 4 and 26 to find the difference of sketches with different
    AnotB buffer sizes values (defaults to 12). """,
  examples = """
    Examples:
      > SELECT theta_sketch_estimate(_FUNC_(theta_sketch_agg(col1), theta_sketch_agg(col2))) FROM VALUES (5, 4), (1, 4), (2, 5), (2, 5), (3, 1) tab(col1, col2);
       2
  """,
  group = "misc_funcs",
  since = "4.0.0")
// scalastyle:on line.size.limit
case class ThetaDifference(first: Expression, second: Expression, third: Expression)
    extends TernaryExpression
    with CodegenFallback
    with ExpectsInputTypes {
  override def nullIntolerant: Boolean = true

  def this(first: Expression, second: Expression) = {
    this(first, second, Literal(ThetaUtil.checkNomLongs(ThetaUtil.DEFAULT_NOMINAL_ENTRIES)))
  }

  def this(first: Expression, second: Expression, third: Int) = {
    this(first, second, Literal(third))
  }

  override protected def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression): ThetaDifference =
    copy(first = newFirst, second = newSecond, third = newThird)

  override def prettyName: String = "theta_difference"

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType, BinaryType, IntegerType)

  override def dataType: DataType = BinaryType

  override def nullSafeEval(value1: Any, value2: Any, value3: Any): Any = {
    val sketch1 =
      try {
        CompactSketch.wrap(Memory.wrap(value1.asInstanceOf[Array[Byte]]))
      } catch {
        case _: SketchesArgumentException | _: java.lang.Error =>
          throw QueryExecutionErrors.thetaInvalidInputSketchBuffer(prettyName)
      }
    val sketch2 =
      try {
        CompactSketch.wrap(Memory.wrap(value2.asInstanceOf[Array[Byte]]))
      } catch {
        case _: SketchesArgumentException | _: java.lang.Error =>
          throw QueryExecutionErrors.thetaInvalidInputSketchBuffer(prettyName)
      }
    val logNominalEntries = value3.asInstanceOf[Int]
    val difference = SetOperation.builder
      .setLogNominalEntries(logNominalEntries)
      .buildANotB
      .aNotB(sketch1, sketch2)

    difference.toByteArray
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(first, second, lgNomEntries) - Intersects two binary representations of
    Datasketches ThetaSketch objects, using a Datasketches Intersect object. Set
    lgNomEntries to a value between 4 and 26 to find the intersection of sketches with different
    intersection buffer sizes values (defaults to 12). """,
  examples = """
    Examples:
      > SELECT theta_sketch_estimate(_FUNC_(theta_sketch_agg(col1), theta_sketch_agg(col2))) FROM VALUES (5, 4), (1, 4), (2, 5), (2, 5), (3, 1) tab(col1, col2);
       2
  """,
  group = "misc_funcs",
  since = "4.0.0")
// scalastyle:on line.size.limit
case class ThetaIntersection(first: Expression, second: Expression, third: Expression)
    extends TernaryExpression
    with CodegenFallback
    with ExpectsInputTypes {
  override def nullIntolerant: Boolean = true

  def this(first: Expression, second: Expression) = {
    this(first, second, Literal(ThetaUtil.checkNomLongs(ThetaUtil.DEFAULT_NOMINAL_ENTRIES)))
  }

  def this(first: Expression, second: Expression, third: Int) = {
    this(first, second, Literal(third))
  }

  override protected def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression): ThetaIntersection =
    copy(first = newFirst, second = newSecond, third = newThird)

  override def prettyName: String = "theta_intersection"

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType, BinaryType, IntegerType)

  override def dataType: DataType = BinaryType

  override def nullSafeEval(value1: Any, value2: Any, value3: Any): Any = {
    val sketch1 =
      try {
        CompactSketch.wrap(Memory.wrap(value1.asInstanceOf[Array[Byte]]))
      } catch {
        case _: SketchesArgumentException | _: java.lang.Error =>
          throw QueryExecutionErrors.thetaInvalidInputSketchBuffer(prettyName)
      }
    val sketch2 =
      try {
        CompactSketch.wrap(Memory.wrap(value2.asInstanceOf[Array[Byte]]))
      } catch {
        case _: SketchesArgumentException | _: java.lang.Error =>
          throw QueryExecutionErrors.thetaInvalidInputSketchBuffer(prettyName)
      }
    val logNominalEntries = value3.asInstanceOf[Int]
    val intersection = SetOperation.builder
      .setLogNominalEntries(logNominalEntries)
      .buildIntersection
      .intersect(sketch1, sketch2)

    intersection.toByteArray
  }
}
