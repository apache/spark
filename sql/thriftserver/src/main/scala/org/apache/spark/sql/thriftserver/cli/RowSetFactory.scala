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

package org.apache.spark.sql.thriftserver.cli

import java.io.ByteArrayInputStream
import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.thrift.TException
import org.apache.thrift.protocol.TCompactProtocol
import org.apache.thrift.transport.TIOStreamTransport

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.thriftserver.cli.thrift.{TColumn, TProtocolVersion, TRowSet}
import org.apache.spark.sql.thriftserver.cli.thrift.TProtocolVersion._
import org.apache.spark.sql.types.StructType

private[thriftserver] object RowSetFactory extends Logging {
  def create(types: StructType, rows: Seq[Row], version: TProtocolVersion): RowSet = {
    if (version.getValue >= HIVE_CLI_SERVICE_PROTOCOL_V6.getValue) {
      ColumnBasedSet(types, ArrayBuffer(rows: _*), 0)
    } else {
      RowBasedSet(types, ArrayBuffer(rows: _*), 0)
    }
  }

  def create(types: StructType, version: TProtocolVersion): RowSet = {
    if (version.getValue >= HIVE_CLI_SERVICE_PROTOCOL_V6.getValue) {
      ColumnBasedSet(types, new ArrayBuffer[Row](), 0)
    } else {
      RowBasedSet(types, new ArrayBuffer[Row](), 0)
    }
  }

  def create(tRowSet: TRowSet, version: TProtocolVersion): RowSet = {
    val rows = new ArrayBuffer[Row]()
    if (version.getValue >= HIVE_CLI_SERVICE_PROTOCOL_V6.getValue) {
      if (tRowSet.isSetBinaryColumns) {
        val protocol =
          new TCompactProtocol(
            new TIOStreamTransport(
              new ByteArrayInputStream(tRowSet.getBinaryColumns)))
        // Read from the stream using the protocol for each column in final schema
        val bufferMap = new util.HashMap[Int, ColumnBuffer]
        var i = 0
        while (i < tRowSet.getColumnCount) {
          val tvalue = new TColumn
          try {
            tvalue.read(protocol)
          } catch {
            case e: TException =>
              logError(e.getMessage, e)
              throw new TException("Error reading column value from the row set blob", e)
          }
          bufferMap.put(i, new ColumnBuffer(tvalue))
          i = i + 1
        }
        (0 until tRowSet.getRowsSize).foreach(index => {
          rows += Row((0 until tRowSet.getColumnCount).map(colPos =>
            bufferMap.get(colPos).get(index)))
        })
      } else {
        tRowSet.getRows.asScala.foreach(row => {
          rows += Row(row.getColVals.asScala.map(_.getFieldValue))
        })
      }
      ColumnBasedSet(null, rows, tRowSet.getStartRowOffset)
    } else {
      tRowSet.getRows.asScala.foreach(row => {
        rows += Row(row.getColVals.asScala.map(_.getFieldValue))
      })
      RowBasedSet(null, rows, tRowSet.getStartRowOffset)
    }
  }
}
