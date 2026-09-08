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

package org.apache.spark.sql.hive.thriftserver

import scala.jdk.CollectionConverters._

import org.apache.hive.service.rpc.thrift.TProtocolVersion

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{CharType, DataType, StringType, VarcharType}

class RowSetUtilsSuite extends SparkFunSuite {

  private def stringValues(value: String, dataType: DataType): Seq[String] = {
    val rowSet = RowSetUtils.toTRowSet(
      0,
      Seq(Row(value)),
      Array(dataType),
      TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V8)
    rowSet.getColumns.asScala.head.getStringVal.getValues.asScala.toSeq
  }

  // Only the default-collation StringType singleton takes the fast path in toTColumn. CHAR,
  // VARCHAR and collated strings fall through to the generic branch, which renders values with
  // toHiveString and used to quote them there.
  test("SPARK-58794: CHAR, VARCHAR and collated string values are not quoted") {
    Seq(
      CharType(4),
      VarcharType(4),
      StringType("UTF8_LCASE"),
      StringType).foreach { dataType =>
      assert(stringValues("ab", dataType) === Seq("ab"), s"$dataType was rendered with quotes")
    }
  }
}
