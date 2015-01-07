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

package org.apache.spark.sql.hbase

import org.apache.log4j.Logger
import org.apache.spark.sql.catalyst.types._
import org.scalatest.{FunSuite, ShouldMatchers}


import scala.collection.mutable.ArrayBuffer

/**
 * CompositeRowKeyParserTest
 */
case class TestCall(callId: Int, userId: String, duration: Double)

class RowKeyParserSuite extends FunSuite with ShouldMatchers {
  @transient val logger = Logger.getLogger(getClass.getName)

  val allColumns: Seq[AbstractColumn] = Seq(
    KeyColumn("callId", IntegerType, 1),
    KeyColumn("userId", StringType, 2),
    NonKeyColumn("cellTowers", StringType, "cf2", "cellTowersq"),
    NonKeyColumn("callType", ByteType, "cf1", "callTypeq"),
    KeyColumn("deviceId", LongType, 0),
    NonKeyColumn("duration", DoubleType, "cf2", "durationq")
  )

  val keyColumns = allColumns.filter(_.isInstanceOf[KeyColumn])
    .asInstanceOf[Seq[KeyColumn]].sortBy(_.order)
  val nonKeyColumns = allColumns.filter(_.isInstanceOf[NonKeyColumn])
    .asInstanceOf[Seq[NonKeyColumn]]

  /**
   * create row key based on key columns information
   * @param rawKeyColumns sequence of byte array representing the key columns
   * @return array of bytes
   */
  def encodingRawKeyColumns(rawKeyColumns: Seq[HBaseRawType]): HBaseRawType = {
    var buffer = ArrayBuffer[Byte]()
    val delimiter: Byte = 0
    var index = 0
    for (rawKeyColumn <- rawKeyColumns) {
      val keyColumn = keyColumns(index)
      buffer = buffer ++ rawKeyColumn
      if (keyColumn.dataType == StringType) {
        buffer += delimiter
      }
      index = index + 1
    }
    buffer.toArray
  }

  /**
   * get the sequence of key columns from the byte array
   * @param rowKey array of bytes
   * @return sequence of byte array
   */
  def decodingRawKeyColumns(rowKey: HBaseRawType): Seq[HBaseRawType] = {
    var rowKeyList = List[HBaseRawType]()
    val delimiter: Byte = 0
    var index = 0
    for (keyColumn <- keyColumns) {
      var buffer = ArrayBuffer[Byte]()
      val dataType = keyColumn.dataType
      if (dataType == StringType) {
        while (index < rowKey.length && rowKey(index) != delimiter) {
          buffer += rowKey(index)
          index = index + 1
        }
        index = index + 1
      }
      else {
        val length = NativeType.defaultSizeOf(dataType.asInstanceOf[NativeType])
        for (i <- 0 to (length - 1)) {
          buffer += rowKey(index)
          index = index + 1
        }
      }
      rowKeyList = rowKeyList :+ buffer.toArray
    }
    rowKeyList
  }
//
//  test("CreateKeyFromCatalystRow") {
//    val row = Row(12345678, "myUserId1", "tower1,tower9,tower3", 22.toByte, 111223445L, 12345678.90123)
//    val allColumnsWithIndex = allColumns.zipWithIndex
//    val rawKeyColsWithKeyIndex: Seq[(HBaseRawType, Int)] = {
//      for {
//        (column, index) <- allColumnsWithIndex
//        if column.isInstanceOf[KeyColumn]
//        key = column.asInstanceOf[KeyColumn]
//      } yield (
//        DataTypeUtils.getRowColumnFromHBaseRawType(row, index, column.dataType),
//        key.order)
//    }
//
//    val rawKeyCols = rawKeyColsWithKeyIndex.sortBy(_._2).map(_._1)
//    val rowkeyA = encodingRawKeyColumns(rawKeyCols)
//    val parsedKey = decodingRawKeyColumns(rowkeyA)
//
//    val mr = new GenericMutableRow(allColumns.length)
//    parsedKey.zipWithIndex.foreach{
//      case (rawkey, keyIndex) =>
//        val key = keyColumns(keyIndex)
//        val index = allColumns.indexOf(key)
//        setRowColumnFromHBaseRawType(
//          mr, index, rawkey, key.dataType)
//    }
//
//    println(mr.getLong(4))
//  }
}
