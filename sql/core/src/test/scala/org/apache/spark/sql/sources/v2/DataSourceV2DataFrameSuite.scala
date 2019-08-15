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

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.internal.SQLConf.{PARTITION_OVERWRITE_MODE, PartitionOverwriteMode}
import org.apache.spark.sql.test.SharedSQLContext

class DataSourceV2DataFrameSuite extends QueryTest with SharedSQLContext with BeforeAndAfter {
  import testImplicits._

  before {
    spark.conf.set("spark.sql.catalog.testcat", classOf[TestInMemoryTableCatalog].getName)
    spark.conf.set("spark.sql.catalog.testcat2", classOf[TestInMemoryTableCatalog].getName)
  }

  test("insertInto: append") {
    val t1 = "testcat.ns1.ns2.tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING foo")
      val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
      df.write.insertInto(t1)
      checkAnswer(spark.table(t1), df)
    }
  }

  test("insertInto: append by position") {
    val t1 = "testcat.ns1.ns2.tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING foo")
      val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
      val dfr = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("data", "id")
      dfr.write.insertInto(t1)
      checkAnswer(spark.table(t1), df)
    }
  }

  test("insertInto: append across catalog") {
    val t1 = "testcat.ns1.ns2.tbl"
    val t2 = "testcat2.db.tbl"
    withTable(t1, t2) {
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING foo")
      sql(s"CREATE TABLE $t2 (id bigint, data string) USING foo")
      val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
      df.write.insertInto(t1)
      spark.table(t1).write.insertInto(t2)
      checkAnswer(spark.table(t2), df)
    }
  }

  test("insertInto: append partitioned table") {
    val t1 = "testcat.ns1.ns2.tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING foo PARTITIONED BY (id)")
      val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
      df.write.insertInto(t1)
      checkAnswer(spark.table(t1), df)
    }
  }

  test("insertInto: overwrite non-partitioned table") {
    val t1 = "testcat.ns1.ns2.tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING foo")
      val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
      val df2 = Seq((4L, "d"), (5L, "e"), (6L, "f")).toDF("id", "data")
      df.write.insertInto(t1)
      df2.write.mode("overwrite").insertInto(t1)
      checkAnswer(spark.table(t1), df2)
    }
  }

  test("insertInto: overwrite partitioned table in static mode") {
    withSQLConf(PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.STATIC.toString) {
      val t1 = "testcat.ns1.ns2.tbl"
      withTable(t1) {
        sql(s"CREATE TABLE $t1 (id bigint, data string) USING foo PARTITIONED BY (id)")
        Seq((2L, "dummy"), (4L, "keep")).toDF("id", "data").write.insertInto(t1)
        val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
        df.write.mode("overwrite").insertInto(t1)
        checkAnswer(spark.table(t1), df)
      }
    }
  }


  test("insertInto: overwrite partitioned table in static mode by position") {
    withSQLConf(PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.STATIC.toString) {
      val t1 = "testcat.ns1.ns2.tbl"
      withTable(t1) {
        sql(s"CREATE TABLE $t1 (id bigint, data string) USING foo PARTITIONED BY (id)")
        Seq((2L, "dummy"), (4L, "keep")).toDF("id", "data").write.insertInto(t1)
        val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
        val dfr = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("data", "id")
        dfr.write.mode("overwrite").insertInto(t1)
        checkAnswer(spark.table(t1), df)
      }
    }
  }

  test("insertInto: overwrite partitioned table in dynamic mode") {
    withSQLConf(PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.DYNAMIC.toString) {
      val t1 = "testcat.ns1.ns2.tbl"
      withTable(t1) {
        sql(s"CREATE TABLE $t1 (id bigint, data string) USING foo PARTITIONED BY (id)")
        Seq((2L, "dummy"), (4L, "keep")).toDF("id", "data").write.insertInto(t1)
        val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
        df.write.mode("overwrite").insertInto(t1)
        checkAnswer(spark.table(t1), df.union(sql("SELECT 4L, 'keep'")))
      }
    }
  }

  test("insertInto: overwrite partitioned table in dynamic mode by position") {
    withSQLConf(PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.DYNAMIC.toString) {
      val t1 = "testcat.ns1.ns2.tbl"
      withTable(t1) {
        sql(s"CREATE TABLE $t1 (id bigint, data string) USING foo PARTITIONED BY (id)")
        Seq((2L, "dummy"), (4L, "keep")).toDF("id", "data").write.insertInto(t1)
        val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
        val dfr = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("data", "id")
        dfr.write.mode("overwrite").insertInto(t1)
        checkAnswer(spark.table(t1), df.union(sql("SELECT 4L, 'keep'")))
      }
    }
  }

  testQuietly("saveAsTable: table doesn't exist => create table") {
    val t1 = "testcat.ns1.ns2.tbl"
    withTable(t1) {
      val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
      df.write.saveAsTable(t1)
      checkAnswer(spark.table(t1), df)
    }
  }

  testQuietly("saveAsTable: table exists => append by name") {
    val t1 = "testcat.ns1.ns2.tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING foo")
      val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
      // Default saveMode is append, therefore this doesn't throw a table already exists exception
      df.write.saveAsTable(t1)
      checkAnswer(spark.table(t1), df)

      // also appends are by name not by position
      df.select('data, 'id).write.saveAsTable(t1)
      checkAnswer(spark.table(t1), df.union(df))
    }
  }

  testQuietly("saveAsTable: table overwrite and table doesn't exist => create table") {
    val t1 = "testcat.ns1.ns2.tbl"
    withTable(t1) {
      val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
      df.write.mode("overwrite").saveAsTable(t1)
      checkAnswer(spark.table(t1), df)
    }
  }

  testQuietly("saveAsTable: table overwrite and table exists => replace table") {
    val t1 = "testcat.ns1.ns2.tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 USING foo AS SELECT 'c', 'd'")
      val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
      df.write.mode("overwrite").saveAsTable(t1)
      checkAnswer(spark.table(t1), df)
    }
  }

  testQuietly("saveAsTable: ignore mode and table doesn't exist => create table") {
    val t1 = "testcat.ns1.ns2.tbl"
    withTable(t1) {
      val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
      df.write.mode("ignore").saveAsTable(t1)
      checkAnswer(spark.table(t1), df)
    }
  }

  testQuietly("saveAsTable: ignore mode and table exists => do nothing") {
    val t1 = "testcat.ns1.ns2.tbl"
    withTable(t1) {
      val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
      sql(s"CREATE TABLE $t1 USING foo AS SELECT 'c', 'd'")
      df.write.mode("ignore").saveAsTable(t1)
      checkAnswer(spark.table(t1), Seq(Row("c", "d")))
    }
  }
}
