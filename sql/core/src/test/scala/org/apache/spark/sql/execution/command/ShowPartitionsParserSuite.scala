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
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser.parsePlan
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.ShowPartitionsStatement
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.test.SharedSparkSession

class ShowPartitionsParserSuite extends AnalysisTest with SharedSparkSession {
  test("SHOW PARTITIONS") {
    Seq(
      "SHOW PARTITIONS t1" -> ShowPartitionsStatement(Seq("t1"), None),
      "SHOW PARTITIONS db1.t1" -> ShowPartitionsStatement(Seq("db1", "t1"), None),
      "SHOW PARTITIONS t1 PARTITION(partcol1='partvalue', partcol2='partvalue')" ->
        ShowPartitionsStatement(
          Seq("t1"),
          Some(Map("partcol1" -> "partvalue", "partcol2" -> "partvalue"))),
      "SHOW PARTITIONS a.b.c" -> ShowPartitionsStatement(Seq("a", "b", "c"), None),
      "SHOW PARTITIONS a.b.c PARTITION(ds='2017-06-10')" ->
        ShowPartitionsStatement(Seq("a", "b", "c"), Some(Map("ds" -> "2017-06-10")))
    ).foreach { case (sql, expected) =>
      val parsed = parsePlan(sql)
      comparePlans(parsed, expected)
    }
  }

  test("empty values in non-optional partition specs") {
    val e = intercept[ParseException] {
      new SparkSqlParser().parsePlan(
        "SHOW PARTITIONS dbx.tab1 PARTITION (a='1', b)")
    }.getMessage
    assert(e.contains("Found an empty partition key 'b'"))
  }
}
