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

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class KllDeterminismRegressionSuite extends QueryTest with SharedSparkSession {
  private val scalar = """(SELECT kll_sketch_get_n_bigint(kll_sketch_agg_bigint(id))
      |FROM VALUES (0), (1), (2) AS t(id))""".stripMargin

  test("random IN operands retain their input-row evaluation boundary") {
    val kllBooleans = s"SELECT $scalar = 3 AS b UNION ALL SELECT $scalar <> 3 AS b"
    for (codegen <- Seq("CODEGEN_ONLY", "NO_CODEGEN"); not <- Seq("", "NOT ");
        values <- Seq("SELECT b FROM VALUES (false), (true) AS t(b)", kllBooleans)) {
      withSQLConf(SQLConf.CODEGEN_FACTORY_MODE.key -> codegen) {
        checkAnswer(sql(s"""SELECT count(*) FROM range(1000)
            |GROUP BY (rand(0) >= 0.5) ${not}IN ($values)""".stripMargin), Row(1000L))
      }
    }
  }

  test("kll scalars in lateral ON preserve matched and unmatched rows") {
    for (tentative <- Seq(false, true); join <- Seq("JOIN", "LEFT JOIN");
        dataType <- Seq("bigint", "float", "double"); merge <- Seq(false, true)) {
      val sketch = s"kll_sketch_agg_$dataType(CAST(id AS $dataType), 8)"
      val values = "VALUES (0), (1), (2) AS t(id)"
      val aggregate = if (merge) s"kll_merge_agg_$dataType(s, 8)" else sketch
      val source = if (merge) s"(SELECT $sketch AS s FROM $values GROUP BY id)" else values
      val n = s"(SELECT kll_sketch_get_n_$dataType($aggregate) FROM $source)"
      withSQLConf(
          SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "false",
          SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED_TENTATIVELY.key -> tentative.toString) {
        val expected = if (join == "JOIN") Seq(Row(0, 1)) else Seq(Row(0, 1), Row(3, null))
        checkAnswer(sql(s"""SELECT o.id, l.x FROM VALUES (0), (3) AS o(id)
            |$join LATERAL (SELECT 1 AS x) l ON o.id < $n""".stripMargin), expected)
      }
    }
  }

  test("lateral ON retains direct and non-KLL randomness restrictions") {
    Seq(s"o.id < $scalar + rand(0)", "o.id < (SELECT rand(0))").foreach { condition =>
      val error = intercept[AnalysisException] {
        sql(s"""SELECT * FROM VALUES (0), (1) AS o(id)
            |JOIN LATERAL (SELECT 1 AS x) l ON $condition""".stripMargin)
      }
      assert(error.getCondition ==
        "UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.LATERAL_JOIN_CONDITION_NON_DETERMINISTIC")
    }
  }

  test("kll mixed random aggregate diagnostics agree across analyzers") {
    val errors = Seq(false, true).map { singlePass =>
      withSQLConf(
          SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> singlePass.toString,
          SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED_TENTATIVELY.key -> "false",
          SQLConf.ANALYZER_DUAL_RUN_LEGACY_AND_SINGLE_PASS_RESOLVER.key -> "false") {
        intercept[AnalysisException] {
          sql(s"SELECT sum(rand(0) + $scalar) FROM VALUES (0), (1) AS o(id)")
        }
      }
    }
    assert(errors.map(_.getCondition).distinct ==
      Seq("AGGREGATE_FUNCTION_WITH_NONDETERMINISTIC_EXPRESSION"))
    assert(errors.map(_.getMessageParameters.asScala.toMap).distinct.size == 1)
  }

  test("kll predicate analysis rejects direct randomness inside IN operands") {
    for (singlePass <- Seq(false, true); join <- Seq(false, true); not <- Seq("", "NOT ")) {
      val predicate = s"rand(0) ${not}IN (SELECT CAST($scalar AS DOUBLE))"
      val query = if (join) {
        s"SELECT * FROM VALUES (0) AS l(id) JOIN VALUES (0) AS r(id) ON $predicate"
      } else {
        s"SELECT count(*) FILTER (WHERE $predicate) FROM VALUES (0), (1) AS o(id)"
      }
      withSQLConf(
          SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> singlePass.toString,
          SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED_TENTATIVELY.key -> "false",
          SQLConf.ANALYZER_DUAL_RUN_LEGACY_AND_SINGLE_PASS_RESOLVER.key -> "false") {
        val error = intercept[AnalysisException] { sql(query) }
        assert(error.getCondition == (if (join) "INVALID_NON_DETERMINISTIC_EXPRESSIONS"
          else "INVALID_AGGREGATE_FILTER.NON_DETERMINISTIC"))
      }
    }
  }

  test("kll join scalar staging preserves independent partition pruning") {
    withTempPath { path =>
      spark.range(8).selectExpr("id", "CAST(id % 2 AS INT) AS p")
        .write.partitionBy("p").parquet(path.getAbsolutePath)
      withTempView("kll_partitioned") {
        spark.read.parquet(path.getAbsolutePath).createOrReplaceTempView("kll_partitioned")
        for (predicateInOn <- Seq(false, true)) {
          val predicate = "r.p = 1"
          val query = sql(s"""SELECT r.id FROM VALUES (1L), (3L) AS l(id)
              |JOIN kll_partitioned r ON l.id = r.id AND r.id < $scalar
              |${if (predicateInOn) s"AND $predicate" else s"WHERE $predicate"}
              |""".stripMargin)
          val scans = query.queryExecution.sparkPlan.collect {
            case scan: FileSourceScanExec => scan
          }
          assert(scans.size == 1)
          assert(scans.head.partitionFilters.nonEmpty, query.queryExecution.toString)
          checkAnswer(query, Row(1L))
        }
      }
    }
  }
}
