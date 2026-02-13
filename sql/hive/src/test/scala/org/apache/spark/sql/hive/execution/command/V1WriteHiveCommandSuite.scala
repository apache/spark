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

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, SortOrder}
import org.apache.spark.sql.execution.datasources.V1WriteCommandSuiteBase
import org.apache.spark.sql.hive.HiveUtils._
import org.apache.spark.sql.hive.test.TestHiveSingleton

class V1WriteHiveCommandSuite
    extends QueryTest with TestHiveSingleton with V1WriteCommandSuiteBase  {

  def withConvertMetastore(testFunc: Boolean => Any): Unit = {
    Seq(true, false).foreach { enabled =>
      withSQLConf(
        CONVERT_METASTORE_PARQUET.key -> enabled.toString,
        CONVERT_METASTORE_ORC.key -> enabled.toString) {
        testFunc(enabled)
      }
    }
  }

  test("create hive table as select - no partition column") {
    withConvertMetastore { _ =>
      withPlannedWrite { enabled =>
        withTable("t") {
          executeAndCheckOrdering(hasLogicalSort = false, orderingMatched = true) {
            sql("CREATE TABLE t STORED AS PARQUET AS SELECT * FROM t0")
          }
        }
      }
    }
  }

  test("create hive table as select") {
    withConvertMetastore { _ =>
      withPlannedWrite { enabled =>
        withTable("t") {
          withSQLConf("hive.exec.dynamic.partition.mode" -> "nonstrict") {
            executeAndCheckOrdering(
              hasLogicalSort = enabled, orderingMatched = enabled, hasEmpty2Null = enabled) {
              sql(
                """
                  |CREATE TABLE t STORED AS PARQUET
                  |PARTITIONED BY (k)
                  |AS SELECT * FROM t0
                  |""".stripMargin)
            }
          }
        }
      }
    }
  }

  test("insert into hive table") {
    withConvertMetastore { _ =>
      withPlannedWrite { enabled =>
        withTable("t") {
          sql(
            """
              |CREATE TABLE t (i INT, j INT) STORED AS PARQUET
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
  }

  test("insert overwrite hive table") {
    withConvertMetastore { _ =>
      withPlannedWrite { enabled =>
        withTable("t") {
          withSQLConf("hive.exec.dynamic.partition.mode" -> "nonstrict") {
            sql(
              """
                |CREATE TABLE t STORED AS PARQUET
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
  }

  test("insert into hive table with static partitions only") {
    withConvertMetastore { _ =>
      withPlannedWrite { enabled =>
        withTable("t") {
          sql(
            """
              |CREATE TABLE t (i INT, j INT) STORED AS PARQUET
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

  test("v1 write to hive table with sort by literal column preserve custom order") {
    withConvertMetastore { _ =>
      withPlannedWrite { enabled =>
        withSQLConf("hive.exec.dynamic.partition.mode" -> "nonstrict") {
          withTable("t") {
            sql(
              """
                |CREATE TABLE t(i INT, j INT, k STRING) STORED AS PARQUET
                |PARTITIONED BY (k)
                |""".stripMargin)
            // Skip checking orderingMatched temporarily to avoid touching `FileFormatWriter`,
            // see details at https://github.com/apache/spark/pull/52584#issuecomment-3407716019
            executeAndCheckOrderingAndCustomValidate(
              hasLogicalSort = true, orderingMatched = None) {
              sql(
                """
                  |INSERT OVERWRITE t
                  |SELECT i, j, '0' as k FROM t0 SORT BY k, i
                  |""".stripMargin)
            } { optimizedPlan =>
              assert {
                optimizedPlan.outputOrdering.exists {
                  case SortOrder(attr: AttributeReference, _, _, _) => attr.name == "i"
                  case _ => false
                }
              }
            }
          }
        }
      }
    }
  }
}
