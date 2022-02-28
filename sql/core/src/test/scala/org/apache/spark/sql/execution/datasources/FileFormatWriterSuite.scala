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

package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.plans.CodegenInterpretedPlanTest
import org.apache.spark.sql.test.SharedSparkSession

class FileFormatWriterSuite
  extends QueryTest
  with SharedSparkSession
  with CodegenInterpretedPlanTest{

  import testImplicits._

  test("empty file should be skipped while write to file") {
    withTempPath { path =>
      spark.range(100).repartition(10).where("id = 50").write.parquet(path.toString)
      val partFiles = path.listFiles()
        .filter(f => f.isFile && !f.getName.startsWith(".") && !f.getName.startsWith("_"))
      assert(partFiles.length === 2)
    }
  }

  test("SPARK-22252: FileFormatWriter should respect the input query schema") {
    withTable("t1", "t2", "t3", "t4") {
      spark.range(1).select(Symbol("id") as Symbol("col1"), Symbol("id") as Symbol("col2"))
        .write.saveAsTable("t1")
      spark.sql("select COL1, COL2 from t1").write.saveAsTable("t2")
      checkAnswer(spark.table("t2"), Row(0, 0))

      // Test picking part of the columns when writing.
      spark.range(1)
        .select(Symbol("id"), Symbol("id") as Symbol("col1"), Symbol("id") as Symbol("col2"))
        .write.saveAsTable("t3")
      spark.sql("select COL1, COL2 from t3").write.saveAsTable("t4")
      checkAnswer(spark.table("t4"), Row(0, 0))
    }
  }

  test("Null and '' values should not cause dynamic partition failure of string types") {
    withTable("t1", "t2") {
      Seq((0, None), (1, Some("")), (2, None)).toDF("id", "p")
        .write.partitionBy("p").saveAsTable("t1")
      checkAnswer(spark.table("t1").sort("id"), Seq(Row(0, null), Row(1, null), Row(2, null)))

      sql("create table t2(id long, p string) using parquet partitioned by (p)")
      sql("insert overwrite table t2 partition(p) select id, p from t1")
      checkAnswer(spark.table("t2").sort("id"), Seq(Row(0, null), Row(1, null), Row(2, null)))
    }
  }

  test("SPARK-33904: save and insert into a table in a namespace of spark_catalog") {
    val ns = "spark_catalog.ns"
    withNamespace(ns) {
      spark.sql(s"CREATE NAMESPACE $ns")
      val t = s"$ns.tbl"
      withTable(t) {
        spark.range(1).write.saveAsTable(t)
        Seq(100).toDF().write.insertInto(t)
        checkAnswer(spark.table(t), Seq(Row(0), Row(100)))
      }
    }
  }
}
