/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive.thriftserver.cli

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.service.cli.thrift.TProtocolVersion
import org.apache.spark.service.cli.thrift.TProtocolVersion._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

object RowSetFactory {
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
}
