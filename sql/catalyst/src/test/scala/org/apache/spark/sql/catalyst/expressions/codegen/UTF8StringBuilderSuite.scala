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

package org.apache.spark.sql.catalyst.expressions.codegen

import org.apache.spark.SparkFunSuite
import org.apache.spark.unsafe.types.UTF8String

class UTF8StringBuilderSuite extends SparkFunSuite {

  test("basic test") {
    val sb = new UTF8StringBuilder()
    assert(sb.build() === UTF8String.EMPTY_UTF8)

    sb.append("")
    assert(sb.build() === UTF8String.EMPTY_UTF8)

    sb.append("abcd")
    assert(sb.build() === UTF8String.fromString("abcd"))

    sb.append(UTF8String.fromString("1234"))
    assert(sb.build() === UTF8String.fromString("abcd1234"))

    // expect to grow an internal buffer
    sb.append(UTF8String.fromString("efgijk567890"))
    assert(sb.build() === UTF8String.fromString("abcd1234efgijk567890"))
  }
}
