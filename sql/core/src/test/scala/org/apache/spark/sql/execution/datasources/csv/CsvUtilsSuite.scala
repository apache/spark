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

package org.apache.spark.sql.execution.datasources.csv

import org.apache.spark.SparkFunSuite

class CsvUtilsSuite extends SparkFunSuite {

  test("count malformed lines") {
    val malformedLinesInfo = new MalformedLinesInfo(3)
    malformedLinesInfo.add("aaa, bbb, ccc")
    malformedLinesInfo.add("ddd, eee")
    malformedLinesInfo.add("fff, ggg, hhh, iii")
    malformedLinesInfo.add("jjj")
    assert(s"${malformedLinesInfo}" ===
      s"""
        |# of total malformed lines: 4
      """.stripMargin.trim)
  }
}
