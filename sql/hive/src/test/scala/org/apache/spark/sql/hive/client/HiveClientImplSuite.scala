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

package org.apache.spark.sql.hive.client

import org.apache.hadoop.hive.metastore.api.FieldSchema

import org.apache.spark.{SparkFunSuite, SparkUnsupportedOperationException}

class HiveClientImplSuite extends SparkFunSuite {

  test("SPARK-21529: a clear error is raised for an unsupported Hive union type") {
    val column = new FieldSchema("c", "uniontype<int,string>", null)
    checkError(
      exception = intercept[SparkUnsupportedOperationException] {
        HiveClientImpl.fromHiveColumn(column)
      },
      condition = "UNSUPPORTED_HIVE_TYPE",
      parameters = Map(
        "fieldType" -> "\"UNIONTYPE<INT,STRING>\"",
        "fieldName" -> "`c`"))
  }

  test("SPARK-21529: a Hive union type nested in a struct is detected") {
    val column = new FieldSchema("c", "struct<a:uniontype<int,string>>", null)
    checkError(
      exception = intercept[SparkUnsupportedOperationException] {
        HiveClientImpl.fromHiveColumn(column)
      },
      condition = "UNSUPPORTED_HIVE_TYPE",
      parameters = Map(
        "fieldType" -> "\"STRUCT<A:UNIONTYPE<INT,STRING>>\"",
        "fieldName" -> "`c`"))
  }
}