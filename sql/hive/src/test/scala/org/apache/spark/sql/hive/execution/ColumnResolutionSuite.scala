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

import java.io.File

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils

class ColumnResolutionSuite extends QueryTest with SQLTestUtils with TestHiveSingleton {

  import spark.implicits._

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

    intercept[AnalysisException] {
      spark.sql(s"SELECT $db1.t1.i1 FROM t1")
    }

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
          spark.sql("CREATE TABLE t1(i1 INT)")
          spark.sql("INSERT INTO t1 VALUES(1)")
          spark.catalog.setCurrentDatabase(db2)
          spark.sql("CREATE TABLE t1(i1 INT)")
          spark.sql("INSERT INTO t1 VALUES(20)")

          columnResolutionTests(db1, db2)
        } finally {
          spark.catalog.setCurrentDatabase(currentDb)
        }
      }
    }
  }

  test("column resolution scenarios with datasource table") {
    val currentDb = spark.catalog.currentDatabase
    withTempDatabase { db1 =>
      withTempDatabase { db2 =>
        withTempDir { f =>
          try {
            val df = Seq(1).toDF()
            val path = s"${f.getCanonicalPath}${File.separator}test1"
            df.write.csv(path)
            spark.catalog.setCurrentDatabase(db1)

            sql(
              s"""
                |CREATE TABLE t1(i1 INT) USING csv OPTIONS
                |(path "$path", header "false")
              """.stripMargin)

            spark.catalog.setCurrentDatabase(db2)
            val df2 = Seq(20).toDF()
            val path2 = s"${f.getCanonicalPath}${File.separator}test2"
            df2.write.csv(path2)

            sql(
              s"""
                |CREATE TABLE t1(i1 INT) USING csv OPTIONS
                |(path "$path2", header "false")
              """.stripMargin)

            columnResolutionTests(db1, db2)
          } finally {
            spark.catalog.setCurrentDatabase (currentDb)
          }
        }
      }
    }
  }

  test("column resolution scenarios with ambiguous cases") {
    val currentDb = spark.catalog.currentDatabase
    withSQLConf(SQLConf.CROSS_JOINS_ENABLED.key -> "true") {
      withTempDatabase { db1 =>
        withTempDatabase { db2 =>
          withTempPath { f =>
            try {
              spark.catalog.setCurrentDatabase(db1)
              spark.sql("CREATE TABLE t1(i1 INT)")
              spark.sql("INSERT INTO t1 VALUES(1)")
              spark.catalog.setCurrentDatabase(db2)
              spark.sql("CREATE TABLE t1(i1 INT)")
              spark.sql("INSERT INTO t1 VALUES(20)")

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

              // TODO: Support this scenario
              intercept[AnalysisException] {
                spark.sql(s"SELECT $db1.t1.i1 FROM t1, $db2.t1")
              }
              // TODO: Support this scenario
              intercept[AnalysisException] {
                spark.sql(s"SELECT $db1.t1.i1 FROM $db1.t1, $db2.t1")
              }

              spark.catalog.setCurrentDatabase(db2)

              intercept[AnalysisException] {
                spark.sql(s"SELECT i1 FROM t1, $db1.t1")
              }

              intercept[AnalysisException] {
                spark.sql(s"SELECT t1.i1 FROM t1, $db1.t1")
              }

              // TODO: Support this scenario
              intercept[AnalysisException] {
                spark.sql(s"SELECT $db1.t1.i1 FROM t1, $db1.t1")
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

  test("resolve fully qualified table name in star expansion ") {
    val currentDb = spark.catalog.currentDatabase
    withTempDatabase { db1 =>
      withTempDatabase { db2 =>
        withTempPath { f =>
          try {
            val df = spark.range(1).toDF()
            df.write.csv(f.getCanonicalPath)
            spark.catalog.setCurrentDatabase(db1)

            sql(
              s"""
                |CREATE TABLE t1(i1 INT) USING csv OPTIONS
                |(path "${f.getCanonicalPath}", header "false")
              """.stripMargin)

            spark.catalog.setCurrentDatabase(db2)
            spark.sql("CREATE TABLE t1(i1 INT)")
            spark.sql("INSERT INTO t1 VALUES(20)")

            spark.catalog.setCurrentDatabase(db1)
            checkAnswer(spark.sql("SELECT t1.* FROM t1"), Row(0))

            // TODO: Support this scenario
            intercept[AnalysisException] {
              spark.sql(s"SELECT $db1.t1.* FROM $db1.t1")
            }

            checkAnswer(spark.sql(s"SELECT t1.* FROM $db1.t1"), Row(0))

            spark.catalog.setCurrentDatabase(db2)
            checkAnswer(spark.sql("SELECT t1.* FROM t1"), Row(20))

            // TODO: Support this scenario
            intercept[AnalysisException] {
              spark.sql(s"SELECT $db1.t1.* FROM $db1.t1")
            }

            checkAnswer(spark.sql(s"SELECT t1.* FROM $db1.t1"), Row(0))
            checkAnswer(spark.sql(s"SELECT a.* FROM $db1.t1 AS a"), Row(0))

          } finally {
            spark.catalog.setCurrentDatabase(currentDb)
          }
        }
      }
    }
  }

  test("resolve in case of subquery") {
    val currentDb = spark.catalog.currentDatabase
    withTempDatabase { db1 =>
      withTempDir { f =>
        try {
          val df = Seq((4, 1), (3, 1)).toDF()
          val path = s"${f.getCanonicalPath}${File.separator}test1"
          df.write.csv(path)
          spark.catalog.setCurrentDatabase(db1)

          sql(
            s"""
              |CREATE TABLE t3(c1 INT, c2 INT) USING csv OPTIONS
              |(path "$path", header "false")
            """.stripMargin)

          val df2 = Seq((4, 1), (2, 1)).toDF()
          val path2 = s"${f.getCanonicalPath}${File.separator}test2"
          df2.write.csv(path2)

          sql(
            s"""
              |CREATE TABLE t4(c2 INT, c3 INT) USING csv OPTIONS
              |(path "$path2", header "false")
            """.stripMargin)

          checkAnswer(spark.sql("SELECT * FROM t3 WHERE c1 IN " +
            "(SELECT c2 FROM t4 WHERE t4.c3 = t3.c2)"), Row(4, 1))

          // TODO: Support this scenario
          intercept[AnalysisException] {
            spark.sql(s"SELECT * FROM $db1.t3 WHERE c1 IN " +
              s"(SELECT $db1.t4.c2 FROM $db1.t4 WHERE $db1.t4.c3 = $db1.t3.c2)")
          }

        } finally {
          spark.catalog.setCurrentDatabase(currentDb)
        }
      }
    }
  }

  test("col resolution - error case") {
    val currentDb = spark.catalog.currentDatabase
    withTempDatabase { db1 =>
      withTempPath { f =>
        try {
          spark.catalog.setCurrentDatabase(db1)
          spark.sql("CREATE TABLE t1(i1 INT)")
          spark.sql("INSERT INTO t1 VALUES(1)")
          intercept[AnalysisException] {
            spark.sql(s"SELECT $db1.t1 FROM t1")
          }
          intercept[AnalysisException] {
            spark.sql(s"SELECT t1.x.y.* FROM t1")
          }
          intercept[AnalysisException] {
            spark.sql(s"SELECT t1 FROM $db1.t1")
          }
        } finally {
          spark.catalog.setCurrentDatabase(currentDb)
        }
      }
    }
  }

  test("Table with struct column") {
    val currentDb = spark.catalog.currentDatabase
    withTempDatabase { db1 =>
      try {
        spark.catalog.setCurrentDatabase(db1)
        spark.sql("CREATE TABLE t1(i1 INT, t1 STRUCT<i1:INT, i2:INT>)")
        spark.sql("INSERT INTO t1 VALUES(1, (2, 3))")
        checkAnswer(spark.sql(s"SELECT t1.i1 FROM t1"), Row(1))
        checkAnswer(spark.sql(s"SELECT t1.t1.i1 FROM t1"), Row(2))
        checkAnswer(spark.sql(s"SELECT t1.t1.i1 FROM $db1.t1"), Row(2))
        checkAnswer(spark.sql(s"SELECT t1.i1 FROM $db1.t1"), Row(1))

        // TODO: Support this scenario
        intercept[AnalysisException] {
          spark.sql(s"SELECT $db1.t1.t1.i1 FROM $db1.t1")
        }

        // TODO: Support this scenario
        intercept[AnalysisException] {
          spark.sql(s"SELECT $db1.t1.t1.i2 FROM $db1.t1")
        }
      } finally {
        spark.catalog.setCurrentDatabase(currentDb)
      }
    }
  }
}
