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

package org.apache.spark.sql.sources.v2

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.internal.SQLConf.{PARTITION_OVERWRITE_MODE, PartitionOverwriteMode}
import org.apache.spark.sql.test.SharedSQLContext

class DataSourceV2DataFrameSuite extends QueryTest with SharedSQLContext with BeforeAndAfter {
  import testImplicits._

  before {
    spark.conf.set("spark.sql.catalog.testcat", classOf[TestInMemoryTableCatalog].getName)
    spark.conf.set("spark.sql.catalog.testcat2", classOf[TestInMemoryTableCatalog].getName)

    val df = spark.createDataFrame(Seq((1L, "a"), (2L, "b"), (3L, "c"))).toDF("id", "data")
    df.createOrReplaceTempView("source")
    val df2 = spark.createDataFrame(Seq((4L, "d"), (5L, "e"), (6L, "f"))).toDF("id", "data")
    df2.createOrReplaceTempView("source2")
  }

  after {
    spark.catalog("testcat").asInstanceOf[TestInMemoryTableCatalog].clearTables()
    spark.sql("DROP VIEW source")
    spark.sql("DROP VIEW source2")
  }

  test("insertInto: append") {
    val t1 = "testcat.ns1.ns2.tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING foo")
      spark.table("source").select("id", "data").write.insertInto(t1)
      checkAnswer(spark.table(t1), spark.table("source"))
    }
  }

  test("insertInto: append - across catalog") {
    val t1 = "testcat.ns1.ns2.tbl"
    val t2 = "testcat2.db.tbl"
    withTable(t1, t2) {
      sql(s"CREATE TABLE $t1 USING foo AS TABLE source")
      sql(s"CREATE TABLE $t2 (id bigint, data string) USING foo")
      spark.table(t1).write.insertInto(t2)
      checkAnswer(spark.table(t2), spark.table("source"))
    }
  }

  test("insertInto: append partitioned table") {
    val t1 = "testcat.ns1.ns2.tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING foo PARTITIONED BY (id)")
      spark.table("source").write.insertInto(t1)
      checkAnswer(spark.table(t1), spark.table("source"))
    }
  }

  test("insertInto: overwrite non-partitioned table") {
    val t1 = "testcat.ns1.ns2.tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 USING foo AS TABLE source")
      spark.table("source2").write.mode("overwrite").insertInto(t1)
      checkAnswer(spark.table(t1), spark.table("source2"))
    }
  }

  test("insertInto: overwrite - static mode") {
    withSQLConf(PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.STATIC.toString) {
      val t1 = "testcat.ns1.ns2.tbl"
      withTable(t1) {
        sql(s"CREATE TABLE $t1 (id bigint, data string) USING foo PARTITIONED BY (id)")
        Seq((2L, "dummy"), (4L, "keep")).toDF("id", "data").write.insertInto(t1)
        spark.table("source").write.mode("overwrite").insertInto(t1)
        checkAnswer(spark.table(t1), spark.table("source"))
      }
    }
  }

  test("insertInto: overwrite - dynamic mode") {
    withSQLConf(PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.DYNAMIC.toString) {
      val t1 = "testcat.ns1.ns2.tbl"
      withTable(t1) {
        sql(s"CREATE TABLE $t1 (id bigint, data string) USING foo PARTITIONED BY (id)")
        Seq((2L, "dummy"), (4L, "keep")).toDF("id", "data").write.insertInto(t1)
        spark.table("source").write.mode("overwrite").insertInto(t1)
        checkAnswer(spark.table(t1),
          spark.table("source").union(sql("SELECT 4L, 'keep'")))
      }
    }
  }
}
