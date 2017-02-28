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

package org.apache.spark.sql

import java.io.File

import org.apache.spark.sql.test.SharedSQLContext


class ColumnResolutionSuite extends QueryTest with SharedSQLContext {

  import testImplicits._

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
}
