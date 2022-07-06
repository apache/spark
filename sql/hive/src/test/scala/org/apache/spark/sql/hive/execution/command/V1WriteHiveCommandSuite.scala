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

package org.apache.spark.sql.hive.execution.command

import org.apache.spark.sql.execution.datasources.V1WriteCommandSuiteBase
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf

class V1WriteHiveCommandSuite extends V1WriteCommandSuiteBase with TestHiveSingleton {

  test("create hive table as select") {
    Seq(true, false).foreach { enabled =>
      withSQLConf(
        SQLConf.PLANNED_WRITE_ENABLED.key -> enabled.toString,
        "hive.exec.dynamic.partition.mode" -> "nonstrict") {
        withTable("t") {
          val logAppender = new LogAppender("create hive table")
          withLogAppender(logAppender) {
            sql(
              """
                |CREATE TABLE t
                |STORED AS PARQUET
                |PARTITIONED BY (k)
                |AS SELECT * FROM t0
                |""".stripMargin)
            checkOrdering(logAppender, matched = enabled)
          }
        }
      }
    }
  }

  test("insert into hive table") {
    Seq(true, false).foreach { enabled =>
      withSQLConf(
        SQLConf.PLANNED_WRITE_ENABLED.key -> enabled.toString,
        "hive.exec.dynamic.partition.mode" -> "nonstrict") {
        withTable("t") {
          sql(
            """
              |CREATE TABLE t (i INT, j INT)
              |STORED AS PARQUET
              |PARTITIONED BY (k STRING)
              |CLUSTERED BY (i, j) SORTED BY (j) INTO 2 BUCKETS
              |""".stripMargin)

          val logAppender = new LogAppender("insert into hive table")
          withLogAppender(logAppender) {
            sql("INSERT OVERWRITE t SELECT * FROM t0")
            checkOrdering(logAppender, matched = enabled)
          }
        }
      }
    }
  }
}
