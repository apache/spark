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

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.hive.test.TestHive
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils

class HiveSaveDynamicInsertPartitionTest extends HivePlanTest with SQLTestUtils with BeforeAndAfter{

  private val tableNamePrefix =
    TestHive.getConf(SQLConf.DYNAMIC_PARTITION_SAVE_PARTITIONS_TABLE_NAME_PREFIX.key)

  override def beforeAll(): Unit = {
    super.beforeAll()
    TestHive.setConf(SQLConf.ENABLE_DYNAMIC_PARTITION_SAVE_PARTITIONS, true)
  }

  override def afterAll(): Unit = {
    try {
      TestHive.setConf(SQLConf.ENABLE_DYNAMIC_PARTITION_SAVE_PARTITIONS, false)
    } finally {
      super.afterAll()
    }
  }

  test("test_save_dynamic_partition") {
    withDatabase("test_db") {
      withTable("test_table") {
        withSQLConf(
          "hive.exec.dynamic.partition" -> "true",
          "hive.exec.dynamic.partition.mode" -> "nonstrict"
        ) {
          sql("CREATE DATABASE test_db")

          sql(
            s"""
               |CREATE TABLE test_db.test_table(value int)
               |PARTITIONED BY (p1 int, p2 int)
               |STORED AS textfile""".stripMargin)

          sql(
            s"""
               |INSERT OVERWRITE TABLE test_db.test_table PARTITION (p1=1, p2)
               |select * from (
               |  select 1 as value, 1 as p1, 2 as p2
               |  union all
               |  select 2 as value, 1 as p1, 3 as p2
               |)""".stripMargin)

          val df = sql(
            s"""
               |SELECT * FROM ${tableNamePrefix}_test_db_test_table
               |""".stripMargin
          )

          val rows = df.collect()
          assert(rows.length == 2)
          assert(rows(0).getAs[String]("p1") == "1")
          assert(rows(1).getAs[String]("p1") == "1")
          assert(
            (rows(0).getAs[String]("p2") == "2" &&
              rows(1).getAs[String]("p2") == "3")
              ||
              (rows(0).getAs[String]("p2") == "3" &&
                rows(1).getAs[String]("p2") == "2")
          )
        }
      }
    }
  }

}
