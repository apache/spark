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

package org.apache.spark.sql.execution.datasources.json

import org.apache.spark.sql.catalyst.InternalRow

import scala.collection.Map

import com.fasterxml.jackson.core._

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

private[sql] object JacksonGenerator {
  /** Transforms a single Row to JSON using Jackson
    *
    * @param rowSchema the schema object used for conversion
    * @param gen a JsonGenerator object
    * @param row The row to convert
    */
  def apply(rowSchema: StructType, gen: JsonGenerator)(row: Row): Unit = {
    def valWriter: (DataType, Any) => Unit = {
      case (_, null) | (NullType, _) => gen.writeNull()
      case (StringType, v: String) => gen.writeString(v)
      case (TimestampType, v: java.sql.Timestamp) => gen.writeString(v.toString)
      case (IntegerType, v: Int) => gen.writeNumber(v)
      case (ShortType, v: Short) => gen.writeNumber(v)
      case (FloatType, v: Float) => gen.writeNumber(v)
      case (DoubleType, v: Double) => gen.writeNumber(v)
      case (LongType, v: Long) => gen.writeNumber(v)
      case (DecimalType(), v: java.math.BigDecimal) => gen.writeNumber(v)
      case (ByteType, v: Byte) => gen.writeNumber(v.toInt)
      case (BinaryType, v: Array[Byte]) => gen.writeBinary(v)
      case (BooleanType, v: Boolean) => gen.writeBoolean(v)
      case (DateType, v) => gen.writeString(v.toString)
      case (udt: UserDefinedType[_], v) => valWriter(udt.sqlType, udt.serialize(v))

      case (ArrayType(ty, _), v: Seq[_]) =>
        gen.writeStartArray()
        v.foreach(valWriter(ty, _))
        gen.writeEndArray()

      case (MapType(kv, vv, _), v: Map[_, _]) =>
        gen.writeStartObject()
        v.foreach { p =>
          gen.writeFieldName(p._1.toString)
          valWriter(vv, p._2)
        }
        gen.writeEndObject()

      case (StructType(ty), v: Row) =>
        gen.writeStartObject()
        ty.zip(v.toSeq).foreach {
          case (_, null) =>
          case (field, v) =>
            gen.writeFieldName(field.name)
            valWriter(field.dataType, v)
        }
        gen.writeEndObject()
    }

    valWriter(rowSchema, row)
  }

  /** Transforms a single InternalRow to JSON using Jackson
   *
   * TODO: make the code shared with the other apply method.
   *
   * @param rowSchema the schema object used for conversion
   * @param gen a JsonGenerator object
   * @param row The row to convert
   */
  def apply(rowSchema: StructType, gen: JsonGenerator, row: InternalRow): Unit = {
    def valWriter: (DataType, Any) => Unit = {
      case (_, null) | (NullType, _) => gen.writeNull()
      case (StringType, v) => gen.writeString(v.toString)
      case (TimestampType, v: java.sql.Timestamp) => gen.writeString(v.toString)
      case (IntegerType, v: Int) => gen.writeNumber(v)
      case (ShortType, v: Short) => gen.writeNumber(v)
      case (FloatType, v: Float) => gen.writeNumber(v)
      case (DoubleType, v: Double) => gen.writeNumber(v)
      case (LongType, v: Long) => gen.writeNumber(v)
      case (DecimalType(), v: Decimal) => gen.writeNumber(v.toJavaBigDecimal)
      case (ByteType, v: Byte) => gen.writeNumber(v.toInt)
      case (BinaryType, v: Array[Byte]) => gen.writeBinary(v)
      case (BooleanType, v: Boolean) => gen.writeBoolean(v)
      case (DateType, v) => gen.writeString(v.toString)
      case (udt: UserDefinedType[_], v) => valWriter(udt.sqlType, udt.serialize(v))

      case (ArrayType(ty, _), v: ArrayData) =>
        gen.writeStartArray()
        v.foreach(ty, (_, value) => valWriter(ty, value))
        gen.writeEndArray()

      case (MapType(kt, vt, _), v: MapData) =>
        gen.writeStartObject()
        v.foreach(kt, vt, { (k, v) =>
          gen.writeFieldName(k.toString)
          valWriter(vt, v)
        })
        gen.writeEndObject()

      case (StructType(ty), v: InternalRow) =>
        gen.writeStartObject()
        var i = 0
        while (i < ty.length) {
          val field = ty(i)
          val value = v.get(i, field.dataType)
          if (value != null) {
            gen.writeFieldName(field.name)
            valWriter(field.dataType, value)
          }
          i += 1
        }
        gen.writeEndObject()
    }

    valWriter(rowSchema, row)
  }
}
