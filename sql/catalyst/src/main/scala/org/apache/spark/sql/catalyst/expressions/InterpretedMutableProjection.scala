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

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.aggregate.NoOp
import org.apache.spark.sql.types._


/**
 * A [[MutableProjection]] that is calculated by calling `eval` on each of the specified
 * expressions.
 *
 * @param expressions a sequence of expressions that determine the value of each column of the
 *                    output row.
 */
class InterpretedMutableProjection(expressions: Seq[Expression]) extends MutableProjection {
  def this(expressions: Seq[Expression], inputSchema: Seq[Attribute]) =
    this(toBoundExprs(expressions, inputSchema))

  private[this] val buffer = new Array[Any](expressions.size)

  override def initialize(partitionIndex: Int): Unit = {
    expressions.foreach(_.foreach {
      case n: Nondeterministic => n.initialize(partitionIndex)
      case _ =>
    })
  }

  private[this] val validExprs = expressions.zipWithIndex.filter {
    case (NoOp, _) => false
    case _ => true
  }
  private[this] var mutableRow: InternalRow = new GenericInternalRow(expressions.size)
  def currentValue: InternalRow = mutableRow

  override def target(row: InternalRow): MutableProjection = {
    // If `mutableRow` is `UnsafeRow`, `MutableProjection` accepts fixed-length types only
    assert(!row.isInstanceOf[UnsafeRow] ||
      validExprs.forall { case (e, _) => UnsafeRow.isFixedLength(e.dataType) })
    mutableRow = row
    this
  }

  private[this] val fieldWriters = validExprs.map { case (e, i) =>
    val writer = generateRowWriter(i, e.dataType)
    if (!e.nullable) {
      (v: Any) => writer(v)
    } else {
      (v: Any) => {
        if (v == null) {
          mutableRow.setNullAt(i)
        } else {
          writer(v)
        }
      }
    }
  }

  private def generateRowWriter(ordinal: Int, dt: DataType): Any => Unit = dt match {
    case BooleanType =>
      v => mutableRow.setBoolean(ordinal, v.asInstanceOf[Boolean])
    case ByteType =>
      v => mutableRow.setByte(ordinal, v.asInstanceOf[Byte])
    case ShortType =>
      v => mutableRow.setShort(ordinal, v.asInstanceOf[Short])
    case IntegerType | DateType =>
      v => mutableRow.setInt(ordinal, v.asInstanceOf[Int])
    case LongType | TimestampType =>
      v => mutableRow.setLong(ordinal, v.asInstanceOf[Long])
    case FloatType =>
      v => mutableRow.setFloat(ordinal, v.asInstanceOf[Float])
    case DoubleType =>
      v => mutableRow.setDouble(ordinal, v.asInstanceOf[Double])
    case DecimalType.Fixed(precision, _) =>
      v => mutableRow.setDecimal(ordinal, v.asInstanceOf[Decimal], precision)
    case CalendarIntervalType | BinaryType | _: ArrayType | StringType | _: StructType |
         _: MapType | _: UserDefinedType[_] =>
      v => mutableRow.update(ordinal, v)
    case NullType =>
      v => {}
    case _ =>
      throw new SparkException(s"Unsupported data type $dt")
  }

  override def apply(input: InternalRow): InternalRow = {
    var i = 0
    while (i < validExprs.length) {
      val (expr, ordinal) = validExprs(i)
      // Store the result into buffer first, to make the projection atomic (needed by aggregation)
      buffer(ordinal) = expr.eval(input)
      i += 1
    }
    i = 0
    while (i < validExprs.length) {
      val (_, ordinal) = validExprs(i)
      fieldWriters(i)(buffer(ordinal))
      i += 1
    }
    mutableRow
  }
}

/**
 * Helper functions for creating an [[InterpretedMutableProjection]].
 */
object InterpretedMutableProjection {

  /**
   * Returns a [[MutableProjection]] for given sequence of bound Expressions.
   */
  def createProjection(exprs: Seq[Expression]): MutableProjection = {
    // We need to make sure that we do not reuse stateful expressions.
    val cleanedExpressions = exprs.map(_.transform {
      case s: Stateful => s.freshCopy()
    })
    new InterpretedMutableProjection(cleanedExpressions)
  }
}
