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

package org.apache.spark.sql

import org.apache.spark.{SparkFunSuite, SparkRuntimeException}
import org.apache.spark.sql.test.SharedSparkSession

class CollectionFunctionsSuite extends SparkFunSuite with SharedSparkSession {

  test("Unexpected `start` for slice()") {
    checkError(
      exception = intercept[SparkRuntimeException] {
        sql("select slice(array(1,2,3), 0, 1)").collect()
      },
      errorClass = "UNEXPECTED_VALUE_FOR_START_IN_SLICE_FUNCTION",
      parameters = Map("prettyName" -> "slice")
    )
  }

  test("Unexpected `length` for slice()") {
    checkError(
      exception = intercept[SparkRuntimeException] {
        sql("select slice(array(1,2,3), 1, -1)").collect()
      },
      errorClass = "UNEXPECTED_VALUE_FOR_LENGTH_IN_SLICE_FUNCTION",
      parameters = Map("prettyName" -> "slice")
    )
  }

}
