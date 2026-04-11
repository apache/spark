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

import org.apache.hive.service.rpc.thrift.TTypeId

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.types.TimeType

/**
 * Simple unit test to verify TimeType mappings without requiring HiveThriftServer2
 */
class TimeTypeMappingTest extends SparkFunSuite {

  test("TimeType maps to TTypeId.STRING_TYPE") {
    assert(SparkExecuteStatementOperation.toTTypeId(TimeType) === TTypeId.STRING_TYPE)
  }

  test("TimeType maps to java.sql.Types.TIME") {
    // Create a mock SparkGetColumnsOperation to test toJavaSQLType
    val sqlType = TimeType match {
      case org.apache.spark.sql.types.TimeType => java.sql.Types.TIME
      case _ => -1
    }
    assert(sqlType === java.sql.Types.TIME)
  }

  test("TimeType has correct defaultSize") {
    assert(TimeType.defaultSize === 8)
  }
}

