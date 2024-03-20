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

package org.apache.spark.sql.execution.command

import org.apache.spark.sql.catalyst.analysis.AnalysisTest
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.test.SharedSparkSession

class ParserSuite extends AnalysisTest with SharedSparkSession {
  private lazy val parser = new SparkSqlParser()

  // scalastyle:off
  test("verify whitespace handling") {
    parser.parsePlan("SELECT 1") // ASCII space
    parser.parsePlan("SELECT\r1") // ASCII carriage return
    parser.parsePlan("SELECT\n1") // ASCII line feed
    parser.parsePlan("SELECT\t1") // ASCII tab
    parser.parsePlan("SELECT\u000C1") // ASCII form feed
    parser.parsePlan("SELECT\u00A01") // Unicode no-break space
    parser.parsePlan("SELECT\u16801") // Unicode ogham space mark
    parser.parsePlan("SELECT\u20001") // Unicode en quad
    parser.parsePlan("SELECT\u20011") // Unicode em quad
    parser.parsePlan("SELECT\u20021") // Unicode en space
    parser.parsePlan("SELECT\u20031") // Unicode em space
    parser.parsePlan("SELECT\u20041") // Unicode three-per-em space
    parser.parsePlan("SELECT\u20051") // Unicode four-per-em space
    parser.parsePlan("SELECT\u20061") // Unicode six-per-em space
    parser.parsePlan("SELECT\u20071") // Unicode figure space
    parser.parsePlan("SELECT\u20081") // Unicode punctuation space
    parser.parsePlan("SELECT\u20091") // Unicode thin space
    parser.parsePlan("SELECT\u200A1") // Unicode hair space
    parser.parsePlan("SELECT\u20281") // Unicode line separator
    parser.parsePlan("SELECT\u202F1") // Unicode narrow no-break space
    parser.parsePlan("SELECT\u205F1") // Unicode medium mathematical space
    parser.parsePlan("SELECT\u30001") // Unicode ideographic space
  }
  // scalastyle:on
}
