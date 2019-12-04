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

import org.apache.spark.sql.{AnalysisException, ShowCreateTableSuite}
import org.apache.spark.sql.hive.test.TestHiveSingleton

class HiveShowCreateTableSuite extends ShowCreateTableSuite with TestHiveSingleton {

  test("simple hive table") {
    withTable("t1") {
      sql(
        s"""CREATE TABLE t1 (
           |  c1 INT COMMENT 'bla',
           |  c2 STRING
           |)
           |TBLPROPERTIES (
           |  'prop1' = 'value1',
           |  'prop2' = 'value2'
           |)
         """.stripMargin
      )

      checkCreateTable("t1")
    }
  }

  test("simple external hive table") {
    withTempDir { dir =>
      withTable("t1") {
        sql(
          s"""CREATE TABLE t1 (
             |  c1 INT COMMENT 'bla',
             |  c2 STRING
             |)
             |LOCATION '${dir.toURI}'
             |TBLPROPERTIES (
             |  'prop1' = 'value1',
             |  'prop2' = 'value2'
             |)
           """.stripMargin
        )

        checkCreateTable("t1")
      }
    }
  }

  test("partitioned hive table") {
    withTable("t1") {
      sql(
        s"""CREATE TABLE t1 (
           |  c1 INT COMMENT 'bla',
           |  c2 STRING
           |)
           |COMMENT 'bla'
           |PARTITIONED BY (
           |  p1 BIGINT COMMENT 'bla',
           |  p2 STRING
           |)
         """.stripMargin
      )

      checkCreateTable("t1")
    }
  }

  test("hive table with explicit storage info") {
    withTable("t1") {
      sql(
        s"""CREATE TABLE t1 (
           |  c1 INT COMMENT 'bla',
           |  c2 STRING
           |)
           |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
           |COLLECTION ITEMS TERMINATED BY '@'
           |MAP KEYS TERMINATED BY '#'
           |NULL DEFINED AS 'NaN'
         """.stripMargin
      )

      checkCreateTable("t1")
    }
  }

  test("hive table with STORED AS clause") {
    withTable("t1") {
      sql(
        s"""CREATE TABLE t1 (
           |  c1 INT COMMENT 'bla',
           |  c2 STRING
           |)
           |STORED AS PARQUET
         """.stripMargin
      )

      checkCreateTable("t1")
    }
  }

  test("hive table with serde info") {
    withTable("t1") {
      sql(
        s"""CREATE TABLE t1 (
           |  c1 INT COMMENT 'bla',
           |  c2 STRING
           |)
           |ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
           |WITH SERDEPROPERTIES (
           |  'mapkey.delim' = ',',
           |  'field.delim' = ','
           |)
           |STORED AS
           |  INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
           |  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
         """.stripMargin
      )

      checkCreateTable("t1")
    }
  }

  test("hive bucketing is supported") {
    withTable("t1") {
      sql(
        s"""CREATE TABLE t1 (a INT, b STRING)
           |CLUSTERED BY (a)
           |SORTED BY (b)
           |INTO 2 BUCKETS
         """.stripMargin
      )
      checkCreateTable("t1")
    }
  }

  test("hive partitioned view is not supported") {
    withTable("t1") {
      withView("v1") {
        sql(
          s"""
             |CREATE TABLE t1 (c1 INT, c2 STRING)
             |PARTITIONED BY (
             |  p1 BIGINT COMMENT 'bla',
             |  p2 STRING )
           """.stripMargin)

        createRawHiveTable(
          s"""
             |CREATE VIEW v1
             |PARTITIONED ON (p1, p2)
             |AS SELECT * from t1
           """.stripMargin
        )

        val cause = intercept[AnalysisException] {
          sql("SHOW CREATE TABLE v1")
        }

        assert(cause.getMessage.contains(" - partitioned view"))
      }
    }
  }

  test("SPARK-24911: keep quotes for nested fields in hive") {
    withTable("t1") {
      val createTable = "CREATE TABLE `t1`(`a` STRUCT<`b`: STRING>)"
      sql(createTable)
      val shownDDL = sql(s"SHOW CREATE TABLE t1")
        .head()
        .getString(0)
        .split("\n")
        .head
      assert(shownDDL == createTable)

      checkCreateTable("t1")
    }
  }

  private def createRawHiveTable(ddl: String): Unit = {
    hiveContext.sharedState.externalCatalog.unwrapped.asInstanceOf[HiveExternalCatalog]
      .client.runSqlHive(ddl)
  }
}
