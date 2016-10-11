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

package org.apache.spark.sql.hive

import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.QueryTest

class HiveDataFrameSuite extends QueryTest with TestHiveSingleton with SQLTestUtils {
  test("table name with schema") {
    // regression test for SPARK-11778
    spark.sql("create schema usrdb")
    spark.sql("create table usrdb.test(c int)")
    spark.read.table("usrdb.test")
    spark.sql("drop table usrdb.test")
    spark.sql("drop schema usrdb")
  }

  test("SPARK-15887: hive-site.xml should be loaded") {
    val hiveClient = spark.sharedState.externalCatalog.asInstanceOf[HiveExternalCatalog].client
    assert(hiveClient.getConf("hive.in.test", "") == "true")
  }

  test("partitioned pruned table reports only selected files") {
    withTable("test") {
      withTempDir { dir =>
        spark.range(5).selectExpr("id", "id as f1", "id as f2").write
          .partitionBy("f1", "f2")
          .mode("overwrite")
          .parquet(dir.getAbsolutePath)

        spark.sql(s"""
          |create external table test (id long)
          |partitioned by (f1 int, f2 int)
          |stored as parquet
          |location "${dir.getAbsolutePath}"""".stripMargin)
        spark.sql("msck repair table test")

        val df = spark.sql("select * from test")
        assert(df.count() == 5)
        assert(df.inputFiles.length == 5)  // unpruned

        val df2 = spark.sql("select * from test where f2 = 3 or f2 = 4")
        assert(df2.count() == 2)
        assert(df2.inputFiles.length == 2)  // pruned, so we have less files
      }
    }
  }
}
