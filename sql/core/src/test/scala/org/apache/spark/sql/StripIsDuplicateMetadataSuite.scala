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

import java.util.ArrayList

import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class StripIsDuplicateMetadataSuite extends QueryTest with SharedSparkSession {

  test("Strip __is_duplicate from project list") {
    withTable("t1") {
      sql("CREATE TABLE t1(col1 INT, col2 INT)")
      val query =
        """SELECT * FROM (
          | SELECT col1, col1 FROM t1
          | UNION
          | SELECT col1, col2 FROM t1
          |)""".stripMargin

      checkMetadata(query)
    }
  }

  test("Strip __is_duplicate from Union output") {
    withTable("t1") {
      sql("CREATE TABLE t1(col1 INT, col2 INT)")
      val query =
        """SELECT col1, col1 FROM t1
          |UNION
          |SELECT col1, col2 FROM t1""".stripMargin

      checkMetadata(query)
    }
  }

  test("Strip __is_duplicate from CTEs") {
    withTable("t1") {
      sql("CREATE TABLE t1(col1 INT, col2 INT)")
      val query =
        """WITH cte1 AS (
          | SELECT col1, col1 FROM t1
          |),
          |cte2 AS (
          | SELECT col1, col2 FROM t1
          |)
          |SELECT * FROM cte1
          |UNION
          |SELECT * FROM cte2""".stripMargin

      checkMetadata(query)
    }
  }

  test("Strip __is_duplicate from subquery") {
    withTable("t1") {
      sql("CREATE TABLE t1(col1 INT, col2 INT)")
      val query =
        """SELECT sub.col1
          |FROM (
          |  SELECT col1, col1 FROM t1
          |  UNION
          |  SELECT col1, col2 FROM t1
          |) sub
      """.stripMargin

      checkMetadata(query)
    }
  }

  private def checkMetadata(query: String): Unit = {
    for (stripMetadata <- Seq(true, false)) {
      withSQLConf(SQLConf.STRIP_IS_DUPLICATE_METADATA.key -> stripMetadata.toString) {
        val analyzedPlan = sql(query).queryExecution.analyzed
        val duplicateAttributes = new ArrayList[NamedExpression]
        analyzedPlan.foreachWithSubqueries {
          case plan: LogicalPlan => plan.expressions.foreach {
              case namedExpression: NamedExpression
                  if namedExpression.metadata.contains("__is_duplicate") =>
                duplicateAttributes.add(namedExpression)
              case _ =>
          }
        }
        assert(duplicateAttributes.isEmpty == stripMetadata)
      }
    }
  }
}
