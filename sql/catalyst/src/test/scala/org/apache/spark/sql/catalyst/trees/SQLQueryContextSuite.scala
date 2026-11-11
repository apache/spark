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

package org.apache.spark.sql.catalyst.trees

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.plans.SQLHelper

class SQLQueryContextSuite extends SparkFunSuite with SQLHelper {

  test("SPARK-40136: the length of the fragment in query context") {
    def getFragment(sqlText: String, start: Int, stop: Int): String = {
      val context = SQLQueryContext(
        line = None,
        startPosition = None,
        originStartIndex = Some(start),
        originStopIndex = Some(stop),
        sqlText = Some(sqlText),
        originObjectType = None,
        originObjectName = None)
      context.fragment
    }
    Seq(
      ("select 1 / 0", 7, 11) -> "1 / 0",
      ("select 1 / 0", 7, 7) -> "1",
      ("", 7, 7) -> "",
      // Empty fragment for invalid indexes
      ("select 1 / 0", -1, 11) -> "",
      ("select 1 / 0", 7, 12) -> "",
      ("select 1 / 0", 11, 7) -> ""
    ).foreach { case ((sqlText, start, stop), expectedFragment) =>
      assert(getFragment(sqlText, start, stop) === expectedFragment)
    }

    val emoji = new String(Character.toChars(0x1F600))
    val sql = s"SELECT '$emoji' AS a, DATE 'bad'"
    val start = sql.codePointCount(0, sql.indexOf("DATE"))
    val stop = sql.codePointCount(0, sql.length) - 1
    assert(getFragment(sql, start, stop) === "DATE 'bad'")
    // A UTF-16 slice of the same indexes would include a leading space and drop the last quote.
    assert(sql.substring(start, stop + 1) !== "DATE 'bad'")

    val overshoot = SQLQueryContext(
      line = None,
      startPosition = None,
      originStartIndex = Some(0),
      originStopIndex = Some(sql.codePointCount(0, sql.length)),
      sqlText = Some(sql),
      originObjectType = None,
      originObjectName = None)
    assert(overshoot.fragment === "")
    assert(overshoot.summary === "")
  }

  test("summary caret follows code-point indexes after a supplementary character") {
    def summaryLines(sql: String, line: Int, startPos: Int): Array[String] = {
      val start = sql.codePointCount(0, sql.indexOf("DATE"))
      val stop = sql.codePointCount(0, sql.length) - 1
      SQLQueryContext(
        line = Some(line),
        startPosition = Some(startPos),
        originStartIndex = Some(start),
        originStopIndex = Some(stop),
        sqlText = Some(sql),
        originObjectType = None,
        originObjectName = None).summary.split('\n')
    }

    val emoji = new String(Character.toChars(0x1F600))
    val sameLine = s"SELECT '$emoji' AS a, DATE 'bad'"
    val sameStart = sameLine.codePointCount(0, sameLine.indexOf("DATE"))
    val sameLines = summaryLines(sameLine, 1, sameStart)
    assert(sameLines(1) === sameLine)
    assert(sameLines(2) === " " * sameStart + "^" * "DATE 'bad'".length)

    // A later line is where UTF-16 caret placement no longer cancels with the emoji width.
    val nextLine = s"SELECT '$emoji' AS a,\nDATE 'bad'"
    val nextLines = summaryLines(nextLine, 2, 0)
    assert(nextLines(1) === "DATE 'bad'")
    assert(nextLines(2) === "^" * "DATE 'bad'".length)

    val indented = s"SELECT '$emoji' AS a,\n  b, DATE 'bad'"
    val indentedLines = summaryLines(indented, 2, indented.split('\n')(1).indexOf("DATE"))
    assert(indentedLines(1) === "  b, DATE 'bad'")
    val col = indentedLines(1).indexOf("DATE")
    assert(indentedLines(2) === " " * col + "^" * "DATE 'bad'".length)
  }

  test("sqlTextSlice uses code-point indexes after a supplementary character") {
    val emoji = new String(Character.toChars(0x1F600))
    val sql = s"BEGIN SELECT '$emoji'; SELECT 2; END"
    val start = sql.codePointCount(0, sql.indexOf("SELECT 2"))
    val stop = start + "SELECT 2".length - 1
    val origin = Origin(
      startIndex = Some(start),
      stopIndex = Some(stop),
      sqlText = Some(sql))
    assert(origin.sqlTextSlice.contains("SELECT 2"))
  }
}
