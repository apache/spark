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

package org.apache.spark.sql.connect.planner

import scala.collection.mutable

import com.google.protobuf.ByteString

import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.Expression.Literal
import org.apache.spark.sql.catalyst.{expressions, InternalRow}
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData, GenericArrayData, MapData}
import org.apache.spark.sql.connect.common.FromProtoConvertor
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

object LiteralExpressionProtoConverter {

  /**
   * Transforms the protocol buffers literals into the appropriate Catalyst literal expression.
   *
   * @return
   *   Expression
   */
  def toCatalystExpression(lit: proto.Expression.Literal): expressions.Literal = {
    val (dataType, value) = FromProtoToCatalystConverter.convert(lit)
    expressions.Literal(value, dataType)
  }

  private object FromProtoToCatalystConverter extends FromProtoConvertor {
    override protected def convertString(string: ByteString): UTF8String =
      UTF8String.fromBytes(string.toByteArray)

    override protected def convertTime(value: Literal.Time): Long = value.getNano

    override protected def convertDate(value: Int): Int = value

    override protected def convertTimestamp(value: Long): Long = value

    override protected def convertTimestampNTZ(value: Long): Long = value

    override protected def convertDayTimeInterval(value: Long): Long = value

    override protected def convertYearMonthInterval(value: Int): Int = value

    override protected def arrayBuilder(size: Int): mutable.Builder[Any, ArrayData] = {
      new mutable.Builder[Any, ArrayData] {
        private var index = 0
        private var data: Array[Any] = _
        clear()

        override def clear(): Unit = {
          index = 0
          data = new Array[Any](size)
        }

        override def addOne(elem: Any): this.type = {
          data(index) = elem
          index += 1
          this
        }

        override def result(): ArrayData = {
          assert(index == data.length)
          new GenericArrayData(data)
        }
      }
    }

    override protected def mapBuilder(size: Int): mutable.Builder[(Any, Any), Any] = {
      new mutable.Builder[(Any, Any), MapData] {
        private val keys: mutable.Builder[Any, ArrayData] = arrayBuilder(size)
        private val values: mutable.Builder[Any, ArrayData] = arrayBuilder(size)
        clear()

        override def clear(): Unit = {
          keys.clear()
          values.clear()
        }

        override def addOne(elem: (Any, Any)): this.type = {
          keys += elem._1
          values += elem._2
          this
        }

        override def result(): MapData = {
          new ArrayBasedMapData(keys.result(), values.result())
        }
      }
    }

    override protected def structBuilder(schema: StructType): mutable.Builder[Any, Any] = {
      new mutable.Builder[Any, InternalRow] {
        private var index = 0
        private var row: GenericInternalRow = _
        clear()

        override def clear(): Unit = {
          index = 0
          row = new GenericInternalRow(schema.length)
        }

        override def addOne(elem: Any): this.type = {
          row.update(index, elem)
          index += 1
          this
        }

        override def result(): InternalRow = {
          assert(index == row.values.length)
          row
        }
      }
    }
  }
}
