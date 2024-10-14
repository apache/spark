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

package org.apache.spark.sql.catalyst.json

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.json.JsonExpressionUtils
import org.apache.spark.unsafe.types.UTF8String

class JsonObjectKeysSuite extends SparkFunSuite {

  test("Test jsonObjectKeys with null and non-null keys") {
    // JSON string that has a sequence where the parser will encounter null, then non-null keys
    val jsonString = """{"key1":null, "key2":{"nestedKey1":"value1"}, "key3":"value2"}"""

    val json = UTF8String.fromString(jsonString)

    val result = JsonExpressionUtils.jsonObjectKeys(json)

    val expectedKeys = Array(
      UTF8String.fromString("key1"),
      UTF8String.fromString("key2"),
      UTF8String.fromString("key3")
    )

    assert(result != null, "Result should not be null")
    assert(result.numElements() == expectedKeys.length, "Number of keys should match")
  }
}

