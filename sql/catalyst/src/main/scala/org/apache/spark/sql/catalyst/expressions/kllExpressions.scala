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

import org.apache.datasketches.kll._
import org.apache.datasketches.memory.Memory

import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(expr) - Returns human readable summary information about this sketch.
  """,
  examples = """
    Examples:
      > SELECT LENGTH(_FUNC_(kll_sketch_agg_bigint(col))) > 0 FROM VALUES (1), (2), (3), (4), (5) tab(col);
       true
  """,
  group = "misc_funcs",
  since = "4.1.0")
case class KllSketchToStringBigint(child: Expression) extends KllSketchToStringBase {
  override protected def withNewChildInternal(newChild: Expression): KllSketchToStringBigint =
    copy(child = newChild)
  override def prettyName: String = "kll_sketch_to_string_bigint"
  override def nullSafeEval(input: Any): Any = {
    try {
      val buffer = input.asInstanceOf[Array[Byte]]
      val sketch = KllLongsSketch.wrap(Memory.wrap(buffer))
      UTF8String.fromString(sketch.toString())
    } catch {
      case e: Exception =>
        throw QueryExecutionErrors.kllSketchInvalidInputError(prettyName, e.getMessage)
    }
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(expr) - Returns human readable summary information about this sketch.
  """,
  examples = """
    Examples:
      > SELECT LENGTH(_FUNC_(kll_sketch_agg_float(col))) > 0 FROM VALUES (1), (2), (3), (4), (5) tab(col);
       true
  """,
  group = "misc_funcs",
  since = "4.1.0")
case class KllSketchToStringFloat(child: Expression) extends KllSketchToStringBase {
  override protected def withNewChildInternal(newChild: Expression): KllSketchToStringFloat =
    copy(child = newChild)
  override def prettyName: String = "kll_sketch_to_string_float"
  override def nullSafeEval(input: Any): Any = {
    try {
      val buffer = input.asInstanceOf[Array[Byte]]
      val sketch = KllFloatsSketch.wrap(Memory.wrap(buffer))
      UTF8String.fromString(sketch.toString())
    } catch {
      case e: Exception =>
        throw QueryExecutionErrors.kllSketchInvalidInputError(prettyName, e.getMessage)
    }
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(expr) - Returns human readable summary information about this sketch.
  """,
  examples = """
    Examples:
      > SELECT LENGTH(_FUNC_(kll_sketch_agg_double(col))) > 0 FROM VALUES (1), (2), (3), (4), (5) tab(col);
       true
  """,
  group = "misc_funcs",
  since = "4.1.0")
case class KllSketchToStringDouble(child: Expression) extends KllSketchToStringBase {
  override protected def withNewChildInternal(newChild: Expression): KllSketchToStringDouble =
    copy(child = newChild)
  override def prettyName: String = "kll_sketch_to_string_double"
  override def nullSafeEval(input: Any): Any = {
    try {
      val buffer = input.asInstanceOf[Array[Byte]]
      val sketch = KllDoublesSketch.wrap(Memory.wrap(buffer))
      UTF8String.fromString(sketch.toString())
    } catch {
      case e: Exception =>
        throw QueryExecutionErrors.kllSketchInvalidInputError(prettyName, e.getMessage)
    }
  }
}

/** This is a base class for the above expressions to reduce boilerplate. */
abstract class KllSketchToStringBase
    extends UnaryExpression
        with CodegenFallback
        with ImplicitCastInputTypes {
  override def dataType: DataType = StringType
  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType)
  override def nullIntolerant: Boolean = true
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(left, right) - Merges two sketch buffers together into one.
  """,
  examples = """
    Examples:
      > SELECT LENGTH(kll_sketch_to_string_bigint(_FUNC_(kll_sketch_agg_bigint(col), kll_sketch_agg_bigint(col)))) > 0 FROM VALUES (1), (2), (3), (4), (5) tab(col);
       true
  """,
  group = "misc_funcs",
  since = "4.1.0")
case class KllSketchMergeBigint(left: Expression, right: Expression) extends KllSketchMergeBase {
  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression =
    copy(left = newLeft, right = newRight)
  override def prettyName: String = "kll_sketch_merge_bigint"
  override def nullSafeEval(left: Any, right: Any): Any = {
    try {
      val leftBuffer = left.asInstanceOf[Array[Byte]]
      val rightBuffer = right.asInstanceOf[Array[Byte]]
      val leftSketch = KllLongsSketch.heapify(Memory.wrap(leftBuffer))
      val rightSketch = KllLongsSketch.wrap(Memory.wrap(rightBuffer))
      leftSketch.merge(rightSketch)
      leftSketch.toByteArray
    } catch {
      case e: Exception =>
        throw QueryExecutionErrors.kllSketchIncompatibleMergeError(prettyName, e.getMessage)
    }
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(left, right) - Merges two sketch buffers together into one.
  """,
  examples = """
    Examples:
      > SELECT LENGTH(kll_sketch_to_string_float(_FUNC_(kll_sketch_agg_float(col), kll_sketch_agg_float(col)))) > 0 FROM VALUES (1), (2), (3), (4), (5) tab(col);
       true
  """,
  group = "misc_funcs",
  since = "4.1.0")
case class KllSketchMergeFloat(left: Expression, right: Expression) extends KllSketchMergeBase {
  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression =
    copy(left = newLeft, right = newRight)
  override def prettyName: String = "kll_sketch_merge_float"
  override def nullSafeEval(left: Any, right: Any): Any = {
    try {
      val leftBuffer = left.asInstanceOf[Array[Byte]]
      val rightBuffer = right.asInstanceOf[Array[Byte]]
      val leftSketch = KllFloatsSketch.heapify(Memory.wrap(leftBuffer))
      val rightSketch = KllFloatsSketch.wrap(Memory.wrap(rightBuffer))
      leftSketch.merge(rightSketch)
      leftSketch.toByteArray
    } catch {
      case e: Exception =>
        throw QueryExecutionErrors.kllSketchIncompatibleMergeError(prettyName, e.getMessage)
    }
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(left, right) - Merges two sketch buffers together into one.
  """,
  examples = """
    Examples:
      > SELECT LENGTH(kll_sketch_to_string_double(_FUNC_(kll_sketch_agg_double(col), kll_sketch_agg_double(col)))) > 0 FROM VALUES (1), (2), (3), (4), (5) tab(col);
       true
  """,
  group = "misc_funcs",
  since = "4.1.0")
case class KllSketchMergeDouble(left: Expression, right: Expression) extends KllSketchMergeBase {
  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression =
    copy(left = newLeft, right = newRight)
  override def prettyName: String = "kll_sketch_merge_double"
  override def nullSafeEval(left: Any, right: Any): Any = {
    try {
      val leftBuffer = left.asInstanceOf[Array[Byte]]
      val rightBuffer = right.asInstanceOf[Array[Byte]]
      val leftSketch = KllDoublesSketch.heapify(Memory.wrap(leftBuffer))
      val rightSketch = KllDoublesSketch.wrap(Memory.wrap(rightBuffer))
      leftSketch.merge(rightSketch)
      leftSketch.toByteArray
    } catch {
      case e: Exception =>
        throw QueryExecutionErrors.kllSketchIncompatibleMergeError(prettyName, e.getMessage)
    }
  }
}

/** This is a base class for the above expressions to reduce boilerplate. */
abstract class KllSketchMergeBase
    extends BinaryExpression
        with CodegenFallback
        with ImplicitCastInputTypes {
  override def dataType: DataType = BinaryType
  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType, BinaryType)
  override def nullIntolerant: Boolean = true
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(left, right) - Extracts a single value from the quantiles sketch representing the
    desired quantile given the input rank. The desired quantile can either be a single value
    or an array. In the latter case, the function will return an array of results of equal
    length to the input array.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(kll_sketch_agg_bigint(col), 0.5) > 1 FROM VALUES (1), (2), (3), (4), (5) tab(col);
       true
  """,
  group = "misc_funcs",
  since = "4.1.0")
case class KllSketchGetQuantileBigint(left: Expression, right: Expression)
    extends KllSketchGetQuantileBase {
  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression =
    copy(left = newLeft, right = newRight)
  override def prettyName: String = "kll_sketch_get_quantile_bigint"
  override def outputDataType: DataType = LongType
  override def kllSketchGetQuantile(memory: Memory, rank: Double): Any = {
    try {
      KllLongsSketch.wrap(memory).getQuantile(rank)
    } catch {
      case e: org.apache.datasketches.common.SketchesArgumentException =>
        if (e.getMessage.contains("normalized rank")) {
          throw QueryExecutionErrors.kllSketchInvalidQuantileRangeError(prettyName, rank)
        } else {
          throw QueryExecutionErrors.kllSketchInvalidInputError(prettyName, e.getMessage)
        }
      case e: Exception =>
        throw QueryExecutionErrors.kllSketchInvalidInputError(prettyName, e.getMessage)
    }
  }
}

@ExpressionDescription(
  usage = """
    _FUNC_(left, right) - Extracts a single value from the quantiles sketch representing the
    desired quantile given the input rank. The desired quantile can either be a single value
    or an array. In the latter case, the function will return an array of results of equal
    length to the input array.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(kll_sketch_agg_bigint(col), 0.5) > 1 FROM VALUES (1), (2), (3), (4), (5) tab(col);
       true
  """,
  group = "misc_funcs",
  since = "4.1.0")
case class KllSketchGetQuantileFloat(left: Expression, right: Expression)
    extends KllSketchGetQuantileBase {
  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression =
    copy(left = newLeft, right = newRight)
  override def prettyName: String = "kll_sketch_get_quantile_float"
  override def outputDataType: DataType = FloatType
  override def kllSketchGetQuantile(memory: Memory, rank: Double): Any = {
    try {
      KllFloatsSketch.wrap(memory).getQuantile(rank)
    } catch {
      case e: org.apache.datasketches.common.SketchesArgumentException =>
        if (e.getMessage.contains("normalized rank")) {
          throw QueryExecutionErrors.kllSketchInvalidQuantileRangeError(prettyName, rank)
        } else {
          throw QueryExecutionErrors.kllSketchInvalidInputError(prettyName, e.getMessage)
        }
      case e: Exception =>
        throw QueryExecutionErrors.kllSketchInvalidInputError(prettyName, e.getMessage)
    }
  }
}

@ExpressionDescription(
  usage = """
    _FUNC_(left, right) - Extracts a single value from the quantiles sketch representing the
    desired quantile given the input rank. The desired quantile can either be a single value
    or an array. In the latter case, the function will return an array of results of equal
    length to the input array.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(kll_sketch_agg_bigint(col), 0.5) > 1 FROM VALUES (1), (2), (3), (4), (5) tab(col);
       true
  """,
  group = "misc_funcs",
  since = "4.1.0")
case class KllSketchGetQuantileDouble(left: Expression, right: Expression)
    extends KllSketchGetQuantileBase {
  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression =
    copy(left = newLeft, right = newRight)
  override def prettyName: String = "kll_sketch_get_quantile_double"
  override def outputDataType: DataType = DoubleType
  override def kllSketchGetQuantile(memory: Memory, rank: Double): Any = {
    try {
      KllDoublesSketch.wrap(memory).getQuantile(rank)
    } catch {
      case e: org.apache.datasketches.common.SketchesArgumentException =>
        if (e.getMessage.contains("normalized rank")) {
          throw QueryExecutionErrors.kllSketchInvalidQuantileRangeError(prettyName, rank)
        } else {
          throw QueryExecutionErrors.kllSketchInvalidInputError(prettyName, e.getMessage)
        }
      case e: Exception =>
        throw QueryExecutionErrors.kllSketchInvalidInputError(prettyName, e.getMessage)
    }
  }
}

/**
 * This is a base class for the above expressions to reduce boilerplate.
 * Each implementor is expected to define two methods, one to specify the output data type,
 * and another to compute the quantile of an input sketch buffer given the input rank.
 */
abstract class KllSketchGetQuantileBase
    extends BinaryExpression
        with CodegenFallback
        with ImplicitCastInputTypes {
  /**
   * This method accepts a KLL quantiles Memory segment, wraps it with the corresponding
   * Kll*Sketch.wrap method, and then calls getQuantile on the result.
   * @param memory The input KLL quantiles sketch buffer to extract the quantile from
   * @param rank The input rank to use to compute the quantile
   * @return The result quantile
   */
  protected def kllSketchGetQuantile(memory: Memory, rank: Double): Any

  /** The output data type for a single value (not array) */
  protected def outputDataType: DataType

  override def nullIntolerant: Boolean = true
  override def inputTypes: Seq[AbstractDataType] =
    Seq(
      BinaryType,
      TypeCollection(
        DoubleType,
        ArrayType(DoubleType, containsNull = false)))

  override def dataType: DataType = {
    right.dataType match {
      case ArrayType(_, _) => ArrayType(outputDataType, false)
      case _ => outputDataType
    }
  }

  override def nullSafeEval(leftInput: Any, rightInput: Any): Any = {
    val buffer = leftInput.asInstanceOf[Array[Byte]]
    val memory = Memory.wrap(buffer)

    right.eval() match {
      case null => null
      case num: Double =>
        // Single value case
        kllSketchGetQuantile(memory, num)
      case arrayData: ArrayData =>
        // Array case
        val ranks = arrayData.toDoubleArray()
        val results = ranks.map(rank => kllSketchGetQuantile(memory, rank))
        new GenericArrayData(results)
    }
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(left, right) - Extracts a single value from the quantiles sketch representing the
    desired rank given the input quantile. The desired rank can either be a single value
    or an array. In the latter case, the function will return an array of results of equal
    length to the input array.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(kll_sketch_agg_bigint(col), 3) > 1 FROM VALUES (1), (2), (3), (4), (5) tab(col);
       true
  """,
  group = "misc_funcs",
  since = "4.1.0")
case class KllSketchGetRankBigint(left: Expression, right: Expression)
    extends KllSketchGetRankBase {
  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression =
    copy(left = newLeft, right = newRight)
  override def prettyName: String = "kll_sketch_get_rank_bigint"
  override def inputDataType: DataType = LongType
  override def kllSketchGetRank(memory: Memory, quantile: Any): Double = {
    try {
      KllLongsSketch.wrap(memory).getRank(quantile.asInstanceOf[Long])
    } catch {
      case e: Exception =>
        throw QueryExecutionErrors.kllSketchInvalidInputError(prettyName, e.getMessage)
    }
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(left, right) - Extracts a single value from the quantiles sketch representing the
    desired rank given the input quantile. The desired rank can either be a single value
    or an array. In the latter case, the function will return an array of results of equal
    length to the input array.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(kll_sketch_agg_float(col), 3) > 1 FROM VALUES (1), (2), (3), (4), (5) tab(col);
       true
  """,
  group = "misc_funcs",
  since = "4.1.0")
case class KllSketchGetRankFloat(left: Expression, right: Expression)
    extends KllSketchGetRankBase {
  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression =
    copy(left = newLeft, right = newRight)
  override def prettyName: String = "kll_sketch_get_rank_float"
  override def inputDataType: DataType = FloatType
  override def kllSketchGetRank(memory: Memory, quantile: Any): Double = {
    try {
      KllFloatsSketch.wrap(memory).getRank(quantile.asInstanceOf[Float])
    } catch {
      case e: Exception =>
        throw QueryExecutionErrors.kllSketchInvalidInputError(prettyName, e.getMessage)
    }
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(left, right) - Extracts a single value from the quantiles sketch representing the
    desired rank given the input quantile. The desired rank can either be a single value
    or an array. In the latter case, the function will return an array of results of equal
    length to the input array.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(kll_sketch_agg_double(col), 3) > 1 FROM VALUES (1), (2), (3), (4), (5) tab(col);
       true
  """,
  group = "misc_funcs",
  since = "4.1.0")
case class KllSketchGetRankDouble(left: Expression, right: Expression)
    extends KllSketchGetRankBase {
  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression =
    copy(left = newLeft, right = newRight)
  override def prettyName: String = "kll_sketch_get_rank_double"
  override def inputDataType: DataType = DoubleType
  override def kllSketchGetRank(memory: Memory, quantile: Any): Double = {
    try {
      KllDoublesSketch.wrap(memory).getRank(quantile.asInstanceOf[Double])
    } catch {
      case e: Exception =>
        throw QueryExecutionErrors.kllSketchInvalidInputError(prettyName, e.getMessage)
    }
  }
}

/**
 * This is a base class for the above expressions to reduce boilerplate.
 * Each implementor is expected to define two methods, one to specify the input argument data type,
 * and another to compute the rank of an input sketch buffer given the input quantile.
 */
abstract class KllSketchGetRankBase
    extends BinaryExpression
        with CodegenFallback
        with ImplicitCastInputTypes {
  protected def inputDataType: DataType

  /**
   * This method accepts a KLL quantiles Memory segment, wraps it with the corresponding
   * Kll*Sketch.wrap method, and then calls getRank on the result.
   * @param memory The input KLL quantiles sketch buffer to extract the rank from
   * @param quantile The input quantile to use to compute the rank
   * @return The result rank
   */
  protected def kllSketchGetRank(memory: Memory, quantile: Any): Double

  override def nullIntolerant: Boolean = true
  override def inputTypes: Seq[AbstractDataType] = {
    Seq(
      BinaryType,
      TypeCollection(
        inputDataType,
        ArrayType(inputDataType, containsNull = false)))
  }
  override def dataType: DataType = {
    right.dataType match {
      case ArrayType(_, _) => ArrayType(DoubleType, false)
      case _ => DoubleType
    }
  }

  override def nullSafeEval(leftInput: Any, rightInput: Any): Any = {
    val buffer: Array[Byte] = leftInput.asInstanceOf[Array[Byte]]
    val memory: Memory = Memory.wrap(buffer)

    right.eval() match {
      case null => null
      case value if !value.isInstanceOf[ArrayData] =>
        // Single value case
        kllSketchGetRank(memory, value)
      case arrayData: ArrayData =>
        // Array case - need to convert based on inputDataType
        val quantiles = inputDataType match {
          case LongType => arrayData.toLongArray().map(_.asInstanceOf[Any])
          case FloatType => arrayData.toFloatArray().map(_.asInstanceOf[Any])
          case DoubleType => arrayData.toDoubleArray().map(_.asInstanceOf[Any])
        }
        val results = quantiles.map(q => kllSketchGetRank(memory, q))
        new GenericArrayData(results)
    }
  }
}


