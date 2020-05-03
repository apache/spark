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

package org.apache.spark.sql.util

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.util.truncatedString

class UtilSuite extends SparkFunSuite {
  test("truncatedString") {
    assert(truncatedString(Nil, "[", ", ", "]", 2) == "[]")
    assert(truncatedString(Seq(1, 2), "[", ", ", "]", 2) == "[1, 2]")
    assert(truncatedString(Seq(1, 2, 3), "[", ", ", "]", 2) == "[1, ... 2 more fields]")
    assert(truncatedString(Seq(1, 2, 3), "[", ", ", "]", -5) == "[, ... 3 more fields]")
    assert(truncatedString(Seq(1, 2, 3), ", ", 10) == "1, 2, 3")
  }
}
