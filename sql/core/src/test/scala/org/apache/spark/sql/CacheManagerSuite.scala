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

import org.apache.hadoop.fs.Path

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.plans.logical.{CTERelationDef, LogicalPlan, WithCTE}
import org.apache.spark.sql.test.SharedSparkSession

class CacheManagerSuite extends SparkFunSuite with SharedSparkSession {

  test("SPARK-44199: isSubDirectory tests") {
    val cacheManager = spark.sharedState.cacheManager
    val testCases = Map[(String, String), Boolean](
      ("s3://bucket/a/b", "s3://bucket/a/b/c") -> true,
      ("s3://bucket/a/b/c", "s3://bucket/a/b/c") -> true,
      ("s3://bucket/a/b/c", "s3://bucket/a/b") -> false,
      ("s3://bucket/a/z/c", "s3://bucket/a/b/c") -> false,
      ("s3://bucket/a/b/c", "abfs://bucket/a/b/c") -> false)
    testCases.foreach { test =>
      val result = cacheManager.isSubDir(new Path(test._1._1), new Path(test._1._2))
      assert(result == test._2)
    }
  }

  test("SPARK-56738: NormalizeCTEIds stabilizes orphan CTERelationRef across SQL parses") {
    // End-to-end repro: parse the same WITH ... SQL twice, dig out a CTE body that
    // still contains a CTERelationRef to another CTE (no enclosing WithCTE within the
    // sub-tree), and verify QueryExecution.normalize (which CacheManager runs before
    // every cacheQuery / lookupCachedData) produces the same canonical form across
    // parses. Before SPARK-56738 the per-parse CTERelationRef.cteId leaked into
    // canonicalize so sameResult comparisons over such sub-trees were not
    // parse-stable, breaking the NormalizeCTEIds contract that CacheManager relies on.
    val sqlText =
      """WITH inner_cte AS (SELECT max(id) AS m FROM range(50)),
        |     outer_cte AS (SELECT id FROM range(100) WHERE id > (SELECT m FROM inner_cte))
        |SELECT * FROM outer_cte""".stripMargin

    def outerCteBody(): LogicalPlan = {
      val analyzed = spark.sql(sqlText).queryExecution.analyzed
      val withCTE = analyzed.collectFirst { case w: WithCTE => w }.get
      val outerDef = withCTE.cteDefs.collectFirst {
        case d: CTERelationDef if {
            var hasRef = false
            d.child.foreachWithSubqueries {
              case _: org.apache.spark.sql.catalyst.plans.logical.CTERelationRef => hasRef = true
              case _ =>
            }
            hasRef
          } => d
      }.get
      outerDef.child
    }

    val body1 = outerCteBody()
    val body2 = outerCteBody()

    val n1 = org.apache.spark.sql.execution.QueryExecution.normalize(spark, body1)
    val n2 = org.apache.spark.sql.execution.QueryExecution.normalize(spark, body2)

    assert(n1.canonicalized == n2.canonicalized,
      s"Normalized CTE-body sub-trees must canonicalize identically across parses; " +
        s"otherwise CacheManager.lookupCachedData cannot reuse cached entries.\n" +
        s"n1.canonicalized=\n${n1.canonicalized}\nn2.canonicalized=\n${n2.canonicalized}")
    assert(n1.sameResult(n2),
      "Normalized CTE-body sub-trees must satisfy sameResult across parses.")
  }
}
