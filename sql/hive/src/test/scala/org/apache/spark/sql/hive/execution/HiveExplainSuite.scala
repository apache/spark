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

package org.apache.spark.sql.hive.execution

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.hive.test.TestHive._

/**
 * A set of tests that validates support for Hive Explain command.
 */
class HiveExplainSuite extends QueryTest {
  test("explain extended command") {
    checkExistence(sql(" explain   select * from src where key=123 "), true,
                   "== Physical Plan ==")
    checkExistence(sql(" explain   select * from src where key=123 "), false,
                   "== Parsed Logical Plan ==",
                   "== Analyzed Logical Plan ==",
                   "== Optimized Logical Plan ==")
    checkExistence(sql(" explain   extended select * from src where key=123 "), true,
                   "== Parsed Logical Plan ==",
                   "== Analyzed Logical Plan ==",
                   "== Optimized Logical Plan ==",
                   "== Physical Plan ==",
                   "Code Generation")
  }

  test("explain create table command") {
    checkExistence(sql("explain create table temp__b as select * from src limit 2"), true,
                   "== Physical Plan ==",
                   "ExecutedCommand [cmd:CreateTableAsSelect [",
                   "Limit",
                   "src")

    checkExistence(
      sql("explain create table temp__b as select *, 1+4 from src limit 2"), false,
      "== Parsed Logical Plan ==",
      "== Analyzed Logical Plan ==",
      "== Optimized Logical Plan ==",
      "unresolvedalias(*)", // parsed plan
      "unresolvedalias((1 + 4))", // parsed plan
      "(1 + 4) AS") // analyzed plan


    checkExistence(sql(
      """
        | EXPLAIN EXTENDED CREATE TABLE temp__b
        | ROW FORMAT SERDE "org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe"
        | WITH SERDEPROPERTIES("serde_p1"="p1","serde_p2"="p2")
        | STORED AS RCFile
        | TBLPROPERTIES("tbl_p1"="p11", "tbl_p2"="p22")
        | AS SELECT *, 4+1 FROM src LIMIT 2
      """.stripMargin), true,
      "== Parsed Logical Plan ==",
      "== Analyzed Logical Plan ==",
      "== Optimized Logical Plan ==",
      "== Physical Plan ==",
      "CreateTableAsSelect",
      "HiveTable",
      "Limit",
      "Map(tbl_p1 -> p11, tbl_p2 -> p22)",
      "Map(serde_p1 -> p1, serde_p2 -> p2)",
      "unresolvedalias(*)", // parsed logical plan
      "unresolvedalias((4 + 1))", // parsed logical plan
      "(4 + 1) AS", // analyzed logical plan
      "key", // analyzed logical plan
      "value", // analyzed logical plan
      "5 AS", // optimized logical plan
      "ExecutedCommand [cmd:", // pyhsical plan
      "src")
  }
}
