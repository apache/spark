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

package org.apache.spark.sql.execution.datasources.v2.jdbc

import java.io.IOException

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.PartitionReader
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

class DBPartitionReader(schema : StructType) extends PartitionReader[InternalRow] with Logging {

  var dummyRows = 0

  @throws[IOException]
  def next(): Boolean = {

    logInfo("***dsv2-flows*** next() called")

    if(dummyRows <2) {
      dummyRows = dummyRows + 1
      true
    } else {
      false
    }
  }

  def get: InternalRow = {

    logInfo("***dsv2-flows*** get() called for row " + dummyRows)

    // Value for row1
    var v_name = "shiv"
    var v_rollnum = "38"
    var v_occupation = "worker"

    if(dummyRows == 2) {
      // Values for row2
      v_name = "someone"
      v_rollnum = "39"
      v_occupation = "dontknow"
    }

    val values = schema.map(_.name).map {
      case "name" => UTF8String.fromString(v_name)
      case "rollnum" => UTF8String.fromString(v_rollnum)
      case "occupation" => UTF8String.fromString(v_occupation)
      case _ => UTF8String.fromString("anything")
    }

    InternalRow.fromSeq(values)
  }

  @throws[IOException]
  override def close(): Unit = {

  }

}
