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

import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.logical.Sort
import org.apache.spark.sql.classic.Dataset
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Tests of optional references (`x?`) that need more than one analysis of a plan. The golden file
 * `optional-references.sql` covers the queries.
 */
class OptionalReferencesSuite extends QueryTest with SharedSparkSession {

  test("SPARK-61494: analyzing the same unresolved plan again resolves a field that appeared") {
    withSQLConf(SQLConf.OPTIONAL_REFERENCES_ENABLED.key -> "true") {
      withTempView("t") {
        def defineT(fields: String): Unit = {
          sql(s"CREATE OR REPLACE TEMPORARY VIEW t AS SELECT id, array(named_struct($fields)) " +
            "AS arr FROM VALUES (1), (2) AS v(id)")
        }
        // ORDER BY resolves `arr[0].b?` against the SELECT list first, whose `arr` has no field
        // `b`, and then against `t.arr`.
        val sqlText = "SELECT id, array(named_struct('a', 1)) AS arr FROM t ORDER BY arr[0].b?, id"
        val plan = spark.sessionState.sqlParser.parsePlan(sqlText)

        // Neither `arr` has `b`: the rows are sorted by NULL, then by `id`.
        defineT("'a', id")
        checkAnswer(Dataset.ofRows(spark, plan).select("id"), Seq(Row(1), Row(2)))

        // `t.arr` gains `b`. The same unresolved plan sorts by it, like a newly parsed plan would:
        // the first analysis did not record its decision on the plan.
        defineT("'a', id, 'b', 3 - id")
        val reanalyzed = Dataset.ofRows(spark, plan)
        checkAnswer(reanalyzed.select("id"), Seq(Row(2), Row(1)))
        val sortKey = reanalyzed.queryExecution.analyzed.collectFirst {
          case sort: Sort => sort.order.head.child
        }
        assert(sortKey.exists(!_.isInstanceOf[Literal]), s"sorted by $sortKey")
        val fresh = Dataset.ofRows(spark, spark.sessionState.sqlParser.parsePlan(sqlText))
        checkAnswer(reanalyzed, fresh.collect())
      }
    }
  }
}
