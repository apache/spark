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


package org.apache.spark.sql.execution

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class RecursiveCTESuite extends QueryTest with SharedSparkSession {
  test("LocalRelation with optimization") {
    withSQLConf(SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> "") {
      val query =
        s"""
           |WITH RECURSIVE t1(n) AS (
           |  VALUES 1
           |  UNION ALL
           |  SELECT n+1 FROM t1 WHERE n < 100)
           |SELECT * FROM t1""".stripMargin
      var correctAnswer: Seq[Row] = List()
      for (i <- 1 to 100) {
        correctAnswer ++= Seq(Row(i))
      }
      checkAnswer(spark.sql(query), correctAnswer)
    }
  }

  test("OneRowRelation with optimization") {
    withSQLConf(SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> "") {
      val query = s"""
                     |WITH RECURSIVE t1(n) AS (
                     |  SELECT 1
                     |  UNION ALL
                     |  SELECT n+1 FROM t1 WHERE n < 100)
                     |SELECT * FROM t1""".stripMargin
      var correctAnswer: Seq[Row] = List()
      for (i <- 1 to 100) {
        correctAnswer ++= Seq(Row(i))
      }
      checkAnswer(spark.sql(query), correctAnswer)
    }
  }
}
