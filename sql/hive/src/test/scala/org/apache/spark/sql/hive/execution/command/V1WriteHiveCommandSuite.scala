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

class V1WriteHiveCommandSuite extends V1WriteCommandSuiteBase with TestHiveSingleton {

  test("create hive table as select - no partition column") {
    withPlannedWrite { enabled =>
      withTable("t") {
        executeAndCheckOrdering(hasLogicalSort = false, orderingMatched = true) {
          sql("CREATE TABLE t AS SELECT * FROM t0")
        }
      }
    }
  }

  test("create hive table as select") {
    withPlannedWrite { enabled =>
      withTable("t") {
        withSQLConf("hive.exec.dynamic.partition.mode" -> "nonstrict") {
          executeAndCheckOrdering(
            hasLogicalSort = enabled, orderingMatched = enabled, hasEmpty2Null = enabled) {
            sql(
              """
                |CREATE TABLE t
                |PARTITIONED BY (k)
                |AS SELECT * FROM t0
                |""".stripMargin)
          }
        }
      }
    }
  }

  test("insert into hive table") {
    withPlannedWrite { enabled =>
      withTable("t") {
        sql(
          """
            |CREATE TABLE t (i INT, j INT)
            |PARTITIONED BY (k STRING)
            |CLUSTERED BY (i, j) SORTED BY (j) INTO 2 BUCKETS
            |""".stripMargin)
        withSQLConf("hive.exec.dynamic.partition.mode" -> "nonstrict") {
          executeAndCheckOrdering(
            hasLogicalSort = enabled, orderingMatched = enabled, hasEmpty2Null = enabled) {
            sql("INSERT INTO t SELECT * FROM t0")
          }
        }
      }
    }
  }

  test("insert overwrite hive table") {
    withPlannedWrite { enabled =>
      withTable("t") {
        withSQLConf("hive.exec.dynamic.partition.mode" -> "nonstrict") {
        sql(
          """
            |CREATE TABLE t
            |PARTITIONED BY (k)
            |AS SELECT * FROM t0
            |""".stripMargin)
          executeAndCheckOrdering(
            hasLogicalSort = enabled, orderingMatched = enabled, hasEmpty2Null = enabled) {
            sql("INSERT OVERWRITE t SELECT j AS i, i AS j, k FROM t0")
          }
        }
      }
    }
  }

  test("insert into hive table with static partitions only") {
    withPlannedWrite { enabled =>
      withTable("t") {
        sql(
          """
            |CREATE TABLE t (i INT, j INT)
            |PARTITIONED BY (k STRING)
            |""".stripMargin)
        // No dynamic partition so no sort is needed.
        executeAndCheckOrdering(hasLogicalSort = false, orderingMatched = true) {
          sql("INSERT INTO t PARTITION (k='0') SELECT i, j FROM t0 WHERE k = '0'")
        }
      }
    }
  }
}
