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
package org.apache.spark.sql.hbase.util

import org.apache.hadoop.hbase.filter.BinaryComparator
import org.apache.spark.sql.catalyst.expressions.{Literal, MutableRow, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.hbase._

/**
 * Data Type conversion utilities
 */
object DataTypeUtils {
  //  TODO: more data types support?
  def bytesToData(src: HBaseRawType, offset: Int, length: Int, dt: DataType): Any = {
    dt match {
      case StringType => BytesUtils.toString(src, offset, length)
      case IntegerType => BytesUtils.toInt(src, offset)
      case BooleanType => BytesUtils.toBoolean(src, offset)
      case ByteType => src(offset)
      case DoubleType => BytesUtils.toDouble(src, offset)
      case FloatType => BytesUtils.toFloat(src, offset)
      case LongType => BytesUtils.toLong(src, offset)
      case ShortType => BytesUtils.toShort(src, offset)
      case _ => throw new Exception("Unsupported HBase SQL Data Type")
    }
  }

  def dataToBytes(src: Any,
                  dt: DataType): HBaseRawType = {
    // TODO: avoid new instance per invocation
    val bu = BytesUtils.create(dt)
    dt match {
      case StringType => bu.toBytes(src.asInstanceOf[String])
      case IntegerType => bu.toBytes(src.asInstanceOf[Int])
      case BooleanType => bu.toBytes(src.asInstanceOf[Boolean])
      case ByteType => bu.toBytes(src.asInstanceOf[Byte])
      case DoubleType => bu.toBytes(src.asInstanceOf[Double])
      case FloatType => bu.toBytes(src.asInstanceOf[Float])
      case LongType => bu.toBytes(src.asInstanceOf[Long])
      case ShortType => bu.toBytes(src.asInstanceOf[Short])
      case _ => throw new Exception("Unsupported HBase SQL Data Type")
    }
  }

  def setRowColumnFromHBaseRawType(row: MutableRow,
                                   index: Int,
                                   src: HBaseRawType,
                                   offset: Int,
                                   length: => Int,
                                   dt: DataType): Unit = {
    if (src == null || src.isEmpty) {
      row.setNullAt(index)
      return
    }
    dt match {
      case StringType => row.setString(index, BytesUtils.toString(src, offset, length))
      case IntegerType => row.setInt(index, BytesUtils.toInt(src, offset)) 
      case BooleanType => row.setBoolean(index, BytesUtils.toBoolean(src, offset))
      case ByteType => row.setByte(index, BytesUtils.toByte(src, offset))
      case DoubleType => row.setDouble(index, BytesUtils.toDouble(src, offset))
      case FloatType => row.setFloat(index, BytesUtils.toFloat(src, offset))
      case LongType => row.setLong(index, BytesUtils.toLong(src, offset))
      case ShortType => row.setShort(index, BytesUtils.toShort(src, offset))
      case _ => throw new Exception("Unsupported HBase SQL Data Type")
    }
  }

  def getRowColumnInHBaseRawType(row: Row,
                                   index: Int,
                                   dt: DataType): HBaseRawType = {
    val bu = BytesUtils.create(dt)
    dt match {
      case StringType => bu.toBytes(row.getString(index))
      case IntegerType => bu.toBytes(row.getInt(index))
      case BooleanType => bu.toBytes(row.getBoolean(index))
      case ByteType => bu.toBytes(row.getByte(index))
      case DoubleType => bu.toBytes(row.getDouble(index))
      case FloatType => bu.toBytes(row.getFloat(index))
      case LongType => bu.toBytes(row.getLong(index))
      case ShortType => bu.toBytes(row.getShort(index))
      case _ => throw new Exception("Unsupported HBase SQL Data Type")
    }
  }

  def getComparator(bu: BytesUtils, expression: Literal): BinaryComparator = {
    expression.dataType match {
      case DoubleType => new BinaryComparator(bu.toBytes(expression.value.asInstanceOf[Double]))
      case FloatType => new BinaryComparator(bu.toBytes(expression.value.asInstanceOf[Float]))
      case IntegerType => new BinaryComparator(bu.toBytes(expression.value.asInstanceOf[Int]))
      case LongType => new BinaryComparator(bu.toBytes(expression.value.asInstanceOf[Long]))
      case ShortType => new BinaryComparator(bu.toBytes(expression.value.asInstanceOf[Short]))
      case StringType => new BinaryComparator(bu.toBytes(expression.value.asInstanceOf[String]))
      case BooleanType => new BinaryComparator(bu.toBytes(expression.value.asInstanceOf[Boolean]))
      case _ => throw new Exception("Cannot convert the data type using BinaryComparator")
    }
  }
}
