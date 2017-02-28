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

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils

class ColumnResolutionSuite extends QueryTest with SQLTestUtils with TestHiveSingleton {

  def columnResolutionTests(db1: String, db2: String): Unit = {
    spark.catalog.setCurrentDatabase(db1)

    checkAnswer(spark.sql("SELECT i1 FROM t1"), Row(1))
    checkAnswer(spark.sql(s"SELECT i1 FROM $db1.t1"), Row(1))

    checkAnswer(spark.sql("SELECT t1.i1 FROM t1"), Row(1))
    checkAnswer(spark.sql(s"SELECT t1.i1 FROM $db1.t1"), Row(1))

    // TODO: Support this scenario
    intercept[AnalysisException] {
      spark.sql(s"SELECT $db1.t1.i1 FROM t1")
    }

    // TODO: Support this scenario
    intercept[AnalysisException] {
      spark.sql(s"SELECT $db1.t1.i1 FROM $db1.t1")
    }

    // Change current database to db2
    spark.catalog.setCurrentDatabase(db2)
    checkAnswer(spark.sql("SELECT i1 FROM t1"), Row(20))
    checkAnswer(spark.sql(s"SELECT i1 FROM $db1.t1"), Row(1))

    checkAnswer(spark.sql("SELECT t1.i1 FROM t1"), Row(20))
    checkAnswer(spark.sql(s"SELECT t1.i1 FROM $db1.t1"), Row(1))

    // TODO: Support this scenario
    intercept[AnalysisException] {
      spark.sql(s"SELECT $db1.t1.i1 FROM $db1.t1")
    }
  }

  test("column resolution scenarios with non datasource table") {
    val currentDb = spark.catalog.currentDatabase
    withTempDatabase { db1 =>
      withTempDatabase { db2 =>
        try {
          spark.catalog.setCurrentDatabase(db1)
          spark.sql("CREATE TABLE t1 as SELECT 1 as i1")
          spark.catalog.setCurrentDatabase(db2)
          spark.sql("CREATE TABLE t1 as SELECT 20 as i1")

          columnResolutionTests(db1, db2)
        } finally {
          spark.catalog.setCurrentDatabase(currentDb)
        }
      }
    }
  }

  test("column resolution scenarios with ambiguous cases in join queries - negative cases") {
    val currentDb = spark.catalog.currentDatabase
    withSQLConf(SQLConf.CROSS_JOINS_ENABLED.key -> "true") {
      withTempDatabase { db1 =>
        withTempDatabase { db2 =>
          withTempPath { f =>
            try {
              spark.catalog.setCurrentDatabase(db1)
              spark.sql("CREATE TABLE t1 AS SELECT 1 AS i1")
              spark.catalog.setCurrentDatabase(db2)
              spark.sql("CREATE TABLE t1 AS SELECT 20 AS i1")
              spark.catalog.setCurrentDatabase(db1)

              intercept[AnalysisException] {
                spark.sql(s"SELECT i1 FROM t1, $db1.t1")
              }

              intercept[AnalysisException] {
                spark.sql(s"SELECT t1.i1 FROM t1, $db1.t1")
              }

              intercept[AnalysisException] {
                spark.sql(s"SELECT $db1.t1.i1 FROM t1, $db1.t1")
              }

              intercept[AnalysisException] {
                spark.sql(s"SELECT i1 FROM t1, $db2.t1")
              }

              intercept[AnalysisException] {
                spark.sql(s"SELECT t1.i1 FROM t1, $db2.t1")
              }

              spark.catalog.setCurrentDatabase(db2)

              intercept[AnalysisException] {
                spark.sql(s"SELECT i1 FROM t1, $db1.t1")
              }

              intercept[AnalysisException] {
                spark.sql(s"SELECT t1.i1 FROM t1, $db1.t1")
              }

              intercept[AnalysisException] {
                spark.sql(s"SELECT i1 FROM t1, $db2.t1")
              }

              intercept[AnalysisException] {
                spark.sql(s"SELECT t1.i1 FROM t1, $db2.t1")
              }

              intercept[AnalysisException] {
                spark.sql(s"SELECT $db1.t1.i1 FROM t1, $db2.t1")
              }
            } finally {
              spark.catalog.setCurrentDatabase(currentDb)
            }
          }
        }
      }
    }
  }

  test("col resolution - error case") {
    val currentDb = spark.catalog.currentDatabase
    withTempDatabase { db2 =>
      withTempDatabase { db1 =>
        withTempPath { f =>
          try {
            spark.catalog.setCurrentDatabase(db1)
            spark.sql("CREATE TABLE t1 as SELECT 1 as i1")
            intercept[AnalysisException] {
              spark.sql(s"SELECT $db1.t1 FROM t1")
            }
            intercept[AnalysisException] {
              spark.sql(s"SELECT t1.x.y.* FROM t1")
            }
            intercept[AnalysisException] {
              spark.sql(s"SELECT t1 FROM $db1.t1")
            }

            spark.catalog.setCurrentDatabase(db2)
            spark.sql("CREATE TABLE t1 as SELECT 20 as i1")
            intercept[AnalysisException] {
              spark.sql(s"SELECT $db1.t1.i1 FROM t1")
            }
          } finally {
            spark.catalog.setCurrentDatabase(currentDb)
          }
        }
      }
    }
  }
}
