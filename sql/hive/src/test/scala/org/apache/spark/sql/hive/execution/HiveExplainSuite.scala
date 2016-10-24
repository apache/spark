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
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.SQLTestUtils

/**
 * A set of tests that validates support for Hive Explain command.
 */
class HiveExplainSuite extends QueryTest with SQLTestUtils with TestHiveSingleton {

  test("explain extended command") {
    checkKeywordsExist(sql(" explain   select * from src where key=123 "),
                   "== Physical Plan ==")
    checkKeywordsNotExist(sql(" explain   select * from src where key=123 "),
                   "== Parsed Logical Plan ==",
                   "== Analyzed Logical Plan ==",
                   "== Optimized Logical Plan ==")
    checkKeywordsExist(sql(" explain   extended select * from src where key=123 "),
                   "== Parsed Logical Plan ==",
                   "== Analyzed Logical Plan ==",
                   "== Optimized Logical Plan ==",
                   "== Physical Plan ==")
  }

  test("explain create table command") {
    checkKeywordsExist(sql("explain create table temp__b as select * from src limit 2"),
                   "== Physical Plan ==",
                   "InsertIntoHiveTable",
                   "Limit",
                   "src")

    checkKeywordsExist(sql("explain extended create table temp__b as select * from src limit 2"),
      "== Parsed Logical Plan ==",
      "== Analyzed Logical Plan ==",
      "== Optimized Logical Plan ==",
      "== Physical Plan ==",
      "CreateHiveTableAsSelect",
      "InsertIntoHiveTable",
      "Limit",
      "src")

    checkKeywordsExist(sql(
      """
        | EXPLAIN EXTENDED CREATE TABLE temp__b
        | ROW FORMAT SERDE "org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe"
        | WITH SERDEPROPERTIES("serde_p1"="p1","serde_p2"="p2")
        | STORED AS RCFile
        | TBLPROPERTIES("tbl_p1"="p11", "tbl_p2"="p22")
        | AS SELECT * FROM src LIMIT 2
      """.stripMargin),
      "== Parsed Logical Plan ==",
      "== Analyzed Logical Plan ==",
      "== Optimized Logical Plan ==",
      "== Physical Plan ==",
      "CreateHiveTableAsSelect",
      "InsertIntoHiveTable",
      "Limit",
      "src")
  }

  test("SPARK-17409: The EXPLAIN output of CTAS only shows the analyzed plan") {
    withTempView("jt") {
      val rdd = sparkContext.parallelize((1 to 10).map(i => s"""{"a":$i, "b":"str$i"}"""))
      spark.read.json(rdd).createOrReplaceTempView("jt")
      val outputs = sql(
        s"""
           |EXPLAIN EXTENDED
           |CREATE TABLE t1
           |AS
           |SELECT * FROM jt
         """.stripMargin).collect().map(_.mkString).mkString

      val shouldContain =
        "== Parsed Logical Plan ==" :: "== Analyzed Logical Plan ==" :: "Subquery" ::
        "== Optimized Logical Plan ==" :: "== Physical Plan ==" ::
        "CreateHiveTableAsSelect" :: "InsertIntoHiveTable" :: "jt" :: Nil
      for (key <- shouldContain) {
        assert(outputs.contains(key), s"$key doesn't exist in result")
      }

      val physicalIndex = outputs.indexOf("== Physical Plan ==")
      assert(outputs.substring(physicalIndex).contains("Subquery"),
        "Physical Plan should contain SubqueryAlias since the query should not be optimized")
    }
  }

  test("EXPLAIN CODEGEN command") {
    checkKeywordsExist(sql("EXPLAIN CODEGEN SELECT 1"),
      "WholeStageCodegen",
      "Generated code:",
      "/* 001 */ public Object generate(Object[] references) {",
      "/* 002 */   return new GeneratedIterator(references);",
      "/* 003 */ }"
    )

    checkKeywordsNotExist(sql("EXPLAIN CODEGEN SELECT 1"),
      "== Physical Plan =="
    )

    intercept[ParseException] {
      sql("EXPLAIN EXTENDED CODEGEN SELECT 1")
    }
  }
}
