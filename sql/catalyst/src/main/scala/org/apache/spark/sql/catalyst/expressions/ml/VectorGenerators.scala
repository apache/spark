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

package org.apache.spark.sql.catalyst.expressions.ml

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{DataTypeMismatch, TypeCheckFailure}
import org.apache.spark.sql.catalyst.expressions.{Expression, Generator, Literal}
import org.apache.spark.sql.catalyst.expressions.Cast._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Explodes the SQL struct representation of an MLlib vector into index-value pairs. This
 * expression is dedicated only for Spark ML and should be used together with `unwrap_udt`.
 * The mode controls whether it emits all entries or nonzero entries. It always emits a marker
 * row before each vector for ML computations that need a per-vector row. The marker index is
 * `-1 - vector.size`.
 *
 * Sparse vector examples:
 * {{{
 *   // v = {type: 0, size: 4, indices: [1, 3], values: [2.0, 4.0]}
 *   vector_posexplode(v)
 *   index  value
 *   -5     NaN
 *   1      2.0
 *   3      4.0
 *
 *   vector_posexplode(v, mode = "dense")
 *   index  value
 *   -5     NaN
 *   0      0.0
 *   1      2.0
 *   2      0.0
 *   3      4.0
 * }}}
 *
 * Dense vector examples:
 * {{{
 *   // v = {type: 1, size: null, indices: null, values: [1.0, 0.0, 3.0]}
 *   vector_posexplode(v)
 *   index  value
 *   -4     NaN
 *   0      1.0
 *   2      3.0
 *
 *   vector_posexplode(v, mode = "dense")
 *   index  value
 *   -4     NaN
 *   0      1.0
 *   1      0.0
 *   2      3.0
 * }}}
 */
case class VectorPosExplode(child: Expression, mode: Expression)
  extends Generator with CodegenFallback {

  def this(child: Expression) = this(child, Literal("sparse"))

  override def children: Seq[Expression] = Seq(child, mode)

  @transient private lazy val vectorMode: VectorPosExplode.VectorMode.Value =
    VectorPosExplode.toMode(mode.eval().asInstanceOf[UTF8String].toString)

  override def checkInputDataTypes(): TypeCheckResult = {
    if (!VectorPosExplode.isVectorType(child.dataType)) {
      return DataTypeMismatch(
        errorSubClass = "UNEXPECTED_INPUT_TYPE",
        messageParameters = Map(
          "paramIndex" -> ordinalNumber(0),
          "requiredType" ->
            toSQLType(s"STRUCT with SQL type ${VectorPosExplode.vectorSqlType.sql}"),
          "inputSql" -> toSQLExpr(child),
          "inputType" -> toSQLType(child.dataType)))
    }
    if (!mode.foldable || !mode.dataType.isInstanceOf[StringType]) {
      return DataTypeMismatch(
        errorSubClass = "UNEXPECTED_INPUT_TYPE",
        messageParameters = Map(
          "paramIndex" -> ordinalNumber(1),
          "requiredType" -> toSQLType("foldable STRING"),
          "inputSql" -> toSQLExpr(mode),
          "inputType" -> toSQLType(mode.dataType)))
    }
    val modeValue = mode.eval()
    if (modeValue == null) {
      return TypeCheckFailure("The second argument of vector_posexplode cannot be null.")
    }
    VectorPosExplode.toModeOption(modeValue.asInstanceOf[UTF8String].toString) match {
      case Some(_) =>
      case None =>
        return TypeCheckFailure(
          "The second argument of vector_posexplode must be one of: dense, sparse.")
    }
    TypeCheckResult.TypeCheckSuccess
  }

  override def elementSchema: StructType = VectorPosExplode.elementSchema

  override def eval(input: InternalRow): IterableOnce[InternalRow] = {
    val vector = child.eval(input).asInstanceOf[InternalRow]
    if (vector == null) {
      Iterator.empty
    } else {
      val values = vector.getArray(3)
      val (size, rows) = vector.getByte(0) match {
        case VectorPosExplode.SparseVectorType =>
          val indices = vector.getArray(2)
          if (indices == null || values == null || vector.isNullAt(1)) {
            return Iterator.empty
          }
          val size = vector.getInt(1)
          (size, VectorPosExplode.explodeSparse(vectorMode, size, indices, values))
        case VectorPosExplode.DenseVectorType =>
          if (values == null) {
            return Iterator.empty
          }
          (values.numElements(), VectorPosExplode.explodeDense(vectorMode, values))
        case vectorType =>
          throw new IllegalArgumentException(s"Unknown vector type $vectorType.")
      }
      Iterator.single(VectorPosExplode.markerRow(size)) ++ rows
    }
  }

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): VectorPosExplode = {
    copy(child = newChildren(0), mode = newChildren(1))
  }
}

object VectorPosExplode {
  object VectorMode extends Enumeration {
    val Dense, Sparse = Value
  }

  private val SparseVectorType: Byte = 0
  private val DenseVectorType: Byte = 1

  private val vectorSqlType = StructType(Array(
    StructField("type", ByteType, nullable = false),
    StructField("size", IntegerType, nullable = true),
    StructField("indices", ArrayType(IntegerType, containsNull = false), nullable = true),
    StructField("values", ArrayType(DoubleType, containsNull = false), nullable = true)))

  private val elementSchema = new StructType()
    .add("index", IntegerType, nullable = false)
    .add("value", DoubleType, nullable = false)

  private def isVectorType(dataType: DataType): Boolean = dataType match {
    case struct: StructType => struct == vectorSqlType
    case _ => false
  }

  private def toModeOption(mode: String): Option[VectorMode.Value] = mode match {
    case "dense" => Some(VectorMode.Dense)
    case "sparse" => Some(VectorMode.Sparse)
    case _ => None
  }

  private def toMode(mode: String): VectorMode.Value = toModeOption(mode).get

  private def markerRow(size: Int): InternalRow = InternalRow(-1 - size, Double.NaN)

  private def explodeSparse(
      mode: VectorMode.Value,
      size: Int,
      indices: ArrayData,
      values: ArrayData): Iterator[InternalRow] = mode match {
    case VectorMode.Dense =>
      explodeSparseAsDense(size, indices, values)
    case VectorMode.Sparse =>
      explodeSparseNonzero(indices, values)
  }

  private def explodeSparseAsDense(
      vectorSize: Int,
      indices: ArrayData,
      values: ArrayData): Iterator[InternalRow] = {
    val numActives = values.numElements()
    // Mirrors SparseVector.iterator without depending on MLlib from Catalyst.
    new Iterator[InternalRow] {
      private var index = 0
      private var activeIndex = 0
      private var nextActiveIndex = if (numActives > 0) indices.getInt(0) else -1

      override def hasNext: Boolean = index < vectorSize

      override def next(): InternalRow = {
        if (!hasNext) {
          throw new NoSuchElementException("next on empty iterator")
        }
        val value = if (index == nextActiveIndex) {
          val activeValue = values.getDouble(activeIndex)
          activeIndex += 1
          nextActiveIndex = if (activeIndex < numActives) indices.getInt(activeIndex) else -1
          activeValue
        } else {
          0.0
        }
        val row = InternalRow(index, value)
        index += 1
        row
      }
    }
  }

  private def explodeSparseNonzero(
      indices: ArrayData,
      values: ArrayData): Iterator[InternalRow] = {
    val numElements = values.numElements()
    new Iterator[InternalRow] {
      private var index = 0
      private var nextRow: InternalRow = _

      override def hasNext: Boolean = {
        while (nextRow == null && index < numElements) {
          val value = values.getDouble(index)
          if (value != 0.0) {
            nextRow = InternalRow(indices.getInt(index), value)
          }
          index += 1
        }
        nextRow != null
      }

      override def next(): InternalRow = {
        if (!hasNext) {
          throw new NoSuchElementException("next on empty iterator")
        }
        val row = nextRow
        nextRow = null
        row
      }
    }
  }

  private def explodeDense(mode: VectorMode.Value, values: ArrayData): Iterator[InternalRow] = {
    mode match {
      case VectorMode.Dense =>
        explodeDense(values, skipZero = false)
      case VectorMode.Sparse =>
        explodeDense(values, skipZero = true)
    }
  }

  private def explodeDense(values: ArrayData, skipZero: Boolean): Iterator[InternalRow] = {
    val numElements = values.numElements()
    new Iterator[InternalRow] {
      private var index = 0
      private var nextRow: InternalRow = _

      override def hasNext: Boolean = {
        while (nextRow == null && index < numElements) {
          val value = values.getDouble(index)
          if (!skipZero || value != 0.0) {
            nextRow = InternalRow(index, value)
          }
          index += 1
        }
        nextRow != null
      }

      override def next(): InternalRow = {
        if (!hasNext) {
          throw new NoSuchElementException("next on empty iterator")
        }
        val row = nextRow
        nextRow = null
        row
      }
    }
  }
}
