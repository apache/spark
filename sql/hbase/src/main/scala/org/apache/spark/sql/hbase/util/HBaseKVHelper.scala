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

import org.apache.spark.sql.catalyst.expressions.{Attribute, Row}
import org.apache.spark.sql.hbase._
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer

object HBaseKVHelper {
  private val delimiter: Byte = 0

  /**
   * create row key based on key columns information
   * for strings, it will add '0x00' as its delimiter
   * @param rawKeyColumns sequence of byte array and data type representing the key columns
   * @return array of bytes
   */
  def encodingRawKeyColumns(rawKeyColumns: Seq[(HBaseRawType, DataType)]): HBaseRawType = {
    val length = rawKeyColumns.foldLeft(0)((b, a) => {
      val len = b + a._1.length
      if (a._2 == StringType) len + 1 else len
    })
    val result = new HBaseRawType(length)
    var index = 0
    for (rawKeyColumn <- rawKeyColumns) {
      Array.copy(rawKeyColumn._1, 0, result, index, rawKeyColumn._1.length)
      index += rawKeyColumn._1.length
      if (rawKeyColumn._2 == StringType) {
        result(index) = delimiter
        index += 1
      }
    }
    result
  }

  /**
   * generate the sequence information of key columns from the byte array
   * @param rowKey array of bytes
   * @param keyColumns the sequence of key columns
   * @return sequence of information in (offset, length) tuple
   */
  def decodingRawKeyColumns(rowKey: HBaseRawType, keyColumns: Seq[KeyColumn]): Seq[(Int, Int)] = {
    var index = 0
    keyColumns.map {
      case c =>
        if (index >= rowKey.length) (-1, -1)
        else {
          val offset = index
          if (c.dataType == StringType) {
            val pos = rowKey.indexOf(delimiter, index)
            index = pos + 1
            (offset, pos - offset)
          } else {
            val length = c.dataType.asInstanceOf[NativeType].defaultSize
            index += length
            (offset, length)
          }
        }
    }
  }

  /**
   * Takes a record, translate it into HBase row key column and value by matching with metadata
   * @param values record that as a sequence of string
   * @param relation HBaseRelation
   * @param keyBytes  output parameter, array of (key column and its type);
   * @param valueBytes array of (column family, column qualifier, value)
   */
  def string2KV(values: Seq[String],
                relation: HBaseRelation,
                lineBuffer: Array[BytesUtils],
                keyBytes: Array[(Array[Byte], DataType)],
                valueBytes: Array[HBaseRawType]) = {
    assert(values.length == relation.output.length,
      s"values length ${values.length} not equals columns length ${relation.output.length}")

    relation.keyColumns.foreach(kc => {
      val ordinal = kc.ordinal
      keyBytes(kc.order) = (string2Bytes(values(ordinal), lineBuffer(ordinal)),
        relation.output(ordinal).dataType)
    })
    for (i <- 0 until relation.nonKeyColumns.size) {
      val nkc = relation.nonKeyColumns(i)
      val bytes = if (values(nkc.ordinal) != null) {
        // we should not use the same buffer in bulk-loading otherwise it will lead to corrupted
        lineBuffer(nkc.ordinal) = BytesUtils.create(lineBuffer(nkc.ordinal).dataType)
        string2Bytes(values(nkc.ordinal), lineBuffer(nkc.ordinal))
      } else null
      valueBytes(i) = bytes
    }
  }

  private def string2Bytes(v: String, bu: BytesUtils): Array[Byte] = {
    bu.dataType match {
      // todo: handle some complex types
      case BooleanType => bu.toBytes(v.toBoolean)
      case ByteType => bu.toBytes(v)
      case DoubleType => bu.toBytes(v.toDouble)
      case FloatType => bu.toBytes(v.toFloat)
      case IntegerType => bu.toBytes(v.toInt)
      case LongType => bu.toBytes(v.toLong)
      case ShortType => bu.toBytes(v.toShort)
      case StringType => bu.toBytes(v)
    }
  }

  /**
   * create a array of buffer that to be used for creating HBase Put object
   * @param schema the schema of the line buffer
   * @return
   */
  private[hbase] def createLineBuffer(schema: Seq[Attribute]): Array[BytesUtils] = {
    val buffer = ArrayBuffer[BytesUtils]()
    schema.foreach { x =>
      buffer.append(BytesUtils.create(x.dataType))
    }
    buffer.toArray
  }

  /**
   * create a row key
   * @param row the generic row
   * @param dataTypeOfKeys sequence of data type
   * @return the row key
   */
  def makeRowKey(row: Row, dataTypeOfKeys: Seq[DataType]): HBaseRawType = {
    val rawKeyCol = dataTypeOfKeys.zipWithIndex.map {
      case (dataType, index) =>
        (DataTypeUtils.getRowColumnInHBaseRawType(row, index, dataType), dataType)
    }

    encodingRawKeyColumns(rawKeyCol)
  }
}
