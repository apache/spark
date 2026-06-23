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
import org.apache.datasketches.hll.{HllSketch, TgtHllType, Union}
import org.apache.datasketches.memory.Memory

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.{KeyEncoding, SketchEnvelope}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{AbstractDataType, BinaryType, BooleanType, DataType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

@ExpressionDescription(
  usage = """
    _FUNC_(expr) - Returns the estimated number of unique values given the binary representation
    of a Datasketches HllSketch. """,
  examples = """
    Examples:
      > SELECT _FUNC_(hll_sketch_agg(col)) FROM VALUES (1), (1), (2), (2), (3) tab(col);
       3
  """,
  group = "sketch_funcs",
  since = "3.5.0")
case class HllSketchEstimate(child: Expression)
  extends UnaryExpression
    with CodegenFallback
    with ExpectsInputTypes {
  override def nullIntolerant: Boolean = true

  override protected def withNewChildInternal(newChild: Expression): HllSketchEstimate =
    copy(child = newChild)

  override def prettyName: String = "hll_sketch_estimate"

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType)

  override def dataType: DataType = LongType

  override def nullSafeEval(input: Any): Any = {
    val (_, buffer) = SketchEnvelope.unwrap(input.asInstanceOf[Array[Byte]])
    try {
      Math.round(HllSketch.heapify(Memory.wrap(buffer)).getEstimate)
    } catch {
      case _: SketchesArgumentException | _: java.lang.Error
           | _: ArrayIndexOutOfBoundsException =>
        throw QueryExecutionErrors.hllInvalidInputSketchBuffer(prettyName)
    }
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(first, second, allowDifferentLgConfigK) - Merges two binary representations of
    Datasketches HllSketch objects, using a Datasketches Union object. Set
    allowDifferentLgConfigK to true to allow unions of sketches with different
    lgConfigK values (defaults to false). """,
  examples = """
    Examples:
      > SELECT hll_sketch_estimate(_FUNC_(hll_sketch_agg(col1), hll_sketch_agg(col2))) FROM VALUES (1, 4), (1, 4), (2, 5), (2, 5), (3, 6) tab(col1, col2);
       6
  """,
  group = "sketch_funcs",
  since = "3.5.0")
// scalastyle:on line.size.limit
case class HllUnion(first: Expression, second: Expression, third: Expression)
  extends TernaryExpression
    with CodegenFallback
    with ExpectsInputTypes {
  override def nullIntolerant: Boolean = true

  // The default target type (register size) to use.
  private val targetType = TgtHllType.HLL_8

  def this(first: Expression, second: Expression) = {
    this(first, second, Literal(false))
  }

  def this(first: Expression, second: Expression, third: Boolean) = {
    this(first, second, Literal(third))
  }

  override protected def withNewChildrenInternal(
    newFirst: Expression, newSecond: Expression, newThird: Expression):
  HllUnion = copy(first = newFirst, second = newSecond, third = newThird)

  override def prettyName: String = "hll_union"

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType, BinaryType, BooleanType)

  override def dataType: DataType = BinaryType

  override def nullSafeEval(value1: Any, value2: Any, value3: Any): Any = {
    val (profile1, payload1) = SketchEnvelope.unwrap(value1.asInstanceOf[Array[Byte]])
    val (profile2, payload2) = SketchEnvelope.unwrap(value2.asInstanceOf[Array[Byte]])
    (profile1, profile2) match {
      case (Some(p1), Some(p2)) =>
        SketchEnvelope.assertCompatible(p2, p1, prettyName, SQLConf.get.sketchAllowVersionMismatch)
      case _ =>
    }
    val sketch1 = try {
      HllSketch.heapify(Memory.wrap(payload1))
    } catch {
      case _: SketchesArgumentException | _: java.lang.Error
           | _: ArrayIndexOutOfBoundsException =>
        throw QueryExecutionErrors.hllInvalidInputSketchBuffer(prettyName)
    }
    val sketch2 = try {
      HllSketch.heapify(Memory.wrap(payload2))
    } catch {
      case _: SketchesArgumentException | _: java.lang.Error
           | _: ArrayIndexOutOfBoundsException =>
        throw QueryExecutionErrors.hllInvalidInputSketchBuffer(prettyName)
    }
    val allowDifferentLgConfigK = value3.asInstanceOf[Boolean]
    if (!allowDifferentLgConfigK && sketch1.getLgConfigK != sketch2.getLgConfigK) {
      throw QueryExecutionErrors.hllUnionDifferentLgK(
        sketch1.getLgConfigK, sketch2.getLgConfigK, function = prettyName)
    }
    val union = new Union(Math.min(sketch1.getLgConfigK, sketch2.getLgConfigK))
    union.update(sketch1)
    union.update(sketch2)
    val result = union.getResult(targetType).toUpdatableByteArray
    if (SQLConf.get.sketchEnvelopeWriteEnabled) {
      profile1.orElse(profile2) match {
        case Some(p) => SketchEnvelope.wrap(result, p)
        case None => result
      }
    } else {
      result
    }
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(sketch) - Returns the provenance metadata recorded in a DataSketches sketch envelope.
    The result is a struct with the derived key encoding, collation id, ICU version, DataSketches
    library version and a flag indicating whether the input carried an envelope. For legacy
    sketches without an envelope, every field except `has_envelope` is NULL. This reads only the
    envelope header and does not touch the underlying DataSketches library. """,
  examples = """
    Examples:
      > SELECT _FUNC_(hll_sketch_agg(col)).has_envelope FROM VALUES (1), (2), (3) tab(col);
       false
  """,
  group = "sketch_funcs",
  since = "4.2.0")
// scalastyle:on line.size.limit
case class SketchMetadata(child: Expression)
  extends UnaryExpression
    with CodegenFallback
    with ExpectsInputTypes {
  override def nullIntolerant: Boolean = true

  override protected def withNewChildInternal(newChild: Expression): SketchMetadata =
    copy(child = newChild)

  override def prettyName: String = "sketch_metadata"

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType)

  override def dataType: DataType = SketchMetadata.outputType

  override def nullSafeEval(input: Any): Any = {
    val (profileOpt, _) = SketchEnvelope.unwrap(input.asInstanceOf[Array[Byte]])
    profileOpt match {
      case Some(p) =>
        val collationId: Any =
          if (p.collationId == SketchEnvelope.NO_COLLATION_ID) null else p.collationId
        val keyEncoding = SketchEnvelope.keyEncodingForCollation(p.collationId)
        InternalRow(
          UTF8String.fromString(KeyEncoding.name(keyEncoding)),
          collationId,
          if (p.icuVersionString == null) null else UTF8String.fromString(p.icuVersionString),
          UTF8String.fromString(p.datasketchesVersionString),
          true)
      case None =>
        InternalRow(null, null, null, null, false)
    }
  }
}

object SketchMetadata {
  val outputType: StructType = StructType(Seq(
    StructField("key_encoding", StringType, nullable = true),
    StructField("collation_id", IntegerType, nullable = true),
    StructField("icu_version", StringType, nullable = true),
    StructField("datasketches_version", StringType, nullable = true),
    StructField("has_envelope", BooleanType, nullable = false)))
}
