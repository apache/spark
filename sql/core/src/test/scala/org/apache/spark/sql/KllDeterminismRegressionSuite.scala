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
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, FileScan}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.functions.{expr, udf}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class KllDeterminismRegressionSuite extends QueryTest with SharedSparkSession {
  import testImplicits._
  private val scalar = """(SELECT kll_sketch_get_n_bigint(kll_sketch_agg_bigint(id))
      |FROM VALUES (0), (1), (2) AS t(id))""".stripMargin

  test("KLL join decoders remain inside dynamic conditional guards") {
    withTempView("guarded_kll_left", "guarded_kll_right") {
      spark.sparkContext.parallelize(Seq(0L), 1).toDF("id")
        .createOrReplaceTempView("guarded_kll_left")
      spark.sparkContext.parallelize(Seq(0L), 1).toDF("id")
        .createOrReplaceTempView("guarded_kll_right")
      val invalid = """kll_sketch_get_n_bigint((SELECT
          |substring(kll_sketch_agg_bigint(id, 8), 1, 1) FROM range(10)))""".stripMargin
      withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
          SQLConf.CODEGEN_FACTORY_MODE.key -> "NO_CODEGEN",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        val query = sql(s"""SELECT l.id, r.id FROM guarded_kll_left l
            |JOIN guarded_kll_right r ON l.id = r.id AND
            |CASE WHEN l.id + r.id < 0 THEN $invalid > 0 ELSE true END""".stripMargin)
        checkAnswer(query, Row(0L, 0L))
      }
    }
  }

  test("renamed sibling columns retain KLL-independent partition pruning") {
    withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "parquet") {
      withTempPath { path =>
        spark.range(8).selectExpr("id", "CAST(id % 2 AS INT) AS p")
          .write.partitionBy("p").parquet(path.getAbsolutePath)
        withTempView("renamed_kll_pruning") {
          spark.read.parquet(path.getAbsolutePath).createOrReplaceTempView("renamed_kll_pruning")
          val query = sql(s"""SELECT p2, n FROM
              |(SELECT p AS p2, $scalar AS n FROM renamed_kll_pruning) q WHERE p2 = 1
              |""".stripMargin)
          val scans = query.queryExecution.sparkPlan.collect {
            case scan: FileSourceScanExec => scan
          }
          assert(scans.size == 1)
          assert(scans.head.selectedPartitions.filePartitionIterator
            .map(_.values.getInt(0)).toSet == Set(1), query.queryExecution.toString)
          checkAnswer(query, Seq.fill(4)(Row(1, 3L)))
        }
      }
    }
  }

  test("primitive Scala UDF null guards retain conditional random arguments") {
    val first = udf((n: Int, xs: Seq[Double]) => n + xs.head)
    withUserDefinedFunction("null_guarded_first" -> true) {
      spark.udf.register("null_guarded_first", first)
      for (codegen <- Seq("NO_CODEGEN", "CODEGEN_ONLY")) {
        withSQLConf(SQLConf.CODEGEN_FACTORY_MODE.key -> codegen,
            SQLConf.ANSI_ENABLED.key -> "true") {
          checkAnswer(sql("""SELECT count(*) FROM range(2) GROUP BY
              |null_guarded_first(IF(id = 0, CAST(NULL AS INT), 1),
              |shuffle(array(1D / id, 1D / id)))""".stripMargin), Seq(Row(1L), Row(1L)))
        }
      }
    }
  }

  test("KLL ASOF predicate subqueries retain analyzer parity") {
    for (singlePass <- Seq(false, true); valueType <- Seq("bigint", "float", "double");
        merge <- Seq(false, true); exists <- Seq(false, true)) {
      val values = "VALUES (0), (1), (2) AS input(id)"
      val sketch = s"kll_sketch_agg_$valueType(CAST(id AS $valueType))"
      val aggregate = if (merge) s"kll_merge_agg_$valueType(s)" else sketch
      val from = if (merge) s"(SELECT $sketch AS s FROM $values GROUP BY id)" else values
      val count = s"SELECT kll_sketch_get_n_$valueType($aggregate) FROM $from"
      val condition = if (exists) s"EXISTS (SELECT * FROM ($count) q)" else s"l.k IN ($count)"
      withSQLConf(SQLConf.SQL_ASOF_JOIN_ENABLED.key -> "true",
          SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> singlePass.toString,
          SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED_TENTATIVELY.key -> "false",
          SQLConf.ANALYZER_DUAL_RUN_LEGACY_AND_SINGLE_PASS_RESOLVER.key -> "false") {
        QueryTest.checkAnswer(sql(s"""SELECT l.k, r.k FROM VALUES (3), (4) AS l(k)
            |ASOF JOIN VALUES (0), (1) AS r(k) MATCH_CONDITION (l.k >= r.k)
            |ON $condition""".stripMargin),
          if (exists) Seq(Row(3, 1), Row(4, 1)) else Seq(Row(3, 1)), checkToRDD = false)
      }
    }
  }

  test("strict ASOF ON rejects unrelated scalar and direct randomness") {
    withSQLConf(SQLConf.SQL_ASOF_JOIN_ENABLED.key -> "true",
        SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "true",
        SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED_TENTATIVELY.key -> "false",
        SQLConf.ANALYZER_DUAL_RUN_LEGACY_AND_SINGLE_PASS_RESOLVER.key -> "false") {
      for (condition <- Seq("l.k = r.k + (SELECT rand())", s"l.k + rand() = r.k + $scalar",
          "l.k IN (SELECT rand())", "EXISTS (SELECT rand())",
          s"l.k + rand() IN (SELECT $scalar)")) {
        val error = intercept[AnalysisException] {
          sql(s"""SELECT l.k FROM VALUES (3), (4) AS l(k)
              |ASOF JOIN VALUES (0), (1) AS r(k) MATCH_CONDITION (l.k >= r.k)
              |ON $condition""".stripMargin)
        }
        assert(error.getCondition == "INVALID_NON_DETERMINISTIC_EXPRESSIONS")
      }
    }
  }

  test("user-aliased independent KLL scalars retain partition pruning") {
    withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "parquet") {
      withTempPath { path =>
        spark.range(8).selectExpr("id", "CAST(id % 2 AS INT) AS p")
          .write.partitionBy("p").parquet(path.getAbsolutePath)
        withTempView("kll_user_pruning") {
          spark.read.parquet(path.getAbsolutePath).createOrReplaceTempView("kll_user_pruning")
          val query = sql(s"""SELECT p, n FROM
              |(SELECT p, $scalar AS n FROM kll_user_pruning) q WHERE p = 1""".stripMargin)
          val scans = query.queryExecution.sparkPlan.collect {
            case scan: FileSourceScanExec => scan
          }
          assert(scans.size == 1)
          assert(scans.head.selectedPartitions.filePartitionIterator
            .map(_.values.getInt(0)).toSet == Set(1), query.queryExecution.toString)
          checkAnswer(query, Seq.fill(4)(Row(1, 3L)))
        }
      }
    }
  }

  test("mixed KLL and seeded row randomness retains the pre-filter draw sequence") {
    withUserDefinedFunction("mixed_kll_bool" -> true) {
      spark.udf.register("mixed_kll_bool", (b: java.lang.Boolean) => b)
      val argument = s"mixed_kll_bool(CASE WHEN id % 2 = 0 THEN $scalar > 0 " +
        "ELSE rand(17) > 0.5 END)"
      val materialized = spark.range(0, 128, 1, 1)
        .selectExpr("id % 3 AS p", s"$argument AS b").cache()
      try {
        materialized.count()
        val expected = materialized.where("p = 1").groupBy("p", "b").count().collect().toSeq
        checkAnswer(sql(s"""SELECT id % 3 AS p, $argument AS b, count(*)
            |FROM range(0, 128, 1, 1) GROUP BY 1, 2 HAVING p = 1""".stripMargin), expected)
      } finally {
        materialized.unpersist()
      }
    }
  }

  test("KLL ASOF ON scalars retain analyzer parity") {
    for (singlePass <- Seq(false, true); valueType <- Seq("bigint", "float", "double");
        merge <- Seq(false, true)) {
      val sketch = s"kll_sketch_agg_$valueType(CAST(id AS $valueType))"
      val aggregate = if (merge) s"kll_merge_agg_$valueType(s)" else sketch
      val values = "VALUES (0), (1), (2) AS input(id)"
      val from = if (merge) s"(SELECT $sketch AS s FROM $values GROUP BY id)" else values
      val count = s"(SELECT kll_sketch_get_n_$valueType($aggregate) FROM $from)"
      withSQLConf(SQLConf.SQL_ASOF_JOIN_ENABLED.key -> "true",
          SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> singlePass.toString,
          SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED_TENTATIVELY.key -> "false",
          SQLConf.ANALYZER_DUAL_RUN_LEGACY_AND_SINGLE_PASS_RESOLVER.key -> "false") {
        val query = sql(s"""SELECT l.k, r.k FROM VALUES (3), (4) AS l(k)
            |ASOF JOIN VALUES (0), (1) AS r(k) MATCH_CONDITION (l.k >= r.k)
            |ON l.k = r.k + $count""".stripMargin)
        QueryTest.checkAnswer(query, Seq(Row(3, 0), Row(4, 1)), checkToRDD = false)
      }
    }
  }

  test("Boolean short circuiting retains conditional random UDF dependencies") {
    withUserDefinedFunction("bool_identity" -> true) {
      spark.udf.register("bool_identity", (b: Boolean) => b)
      for (codegen <- Seq("CODEGEN_ONLY", "NO_CODEGEN");
          condition <- Seq(
            "id = 0 OR array_contains(shuffle(array(1D / id, 2D)), 2D)",
            "id <> 0 AND array_contains(shuffle(array(1D / id, 2D)), 2D)")) {
        withSQLConf(SQLConf.CODEGEN_FACTORY_MODE.key -> codegen,
            SQLConf.ANSI_ENABLED.key -> "true") {
          val query = sql(s"""SELECT count(*) FROM range(2)
              |GROUP BY bool_identity($condition)""".stripMargin)
          val expected = if (condition.startsWith("id =")) Seq(Row(2L))
            else Seq(Row(1L), Row(1L))
          checkAnswer(query, expected)
        }
      }
    }
  }

  test("KLL-only correlated lateral queries retain per-key results") {
    for (legacy <- Seq(true, false); valueType <- Seq("bigint", "float", "double");
        merge <- Seq(false, true)) {
      withSQLConf("spark.sql.legacy.kllAggregateDeterministic.enabled" -> legacy.toString) {
        val sketch = s"kll_sketch_agg_$valueType(CAST(i.id AS $valueType))"
        val aggregate = if (merge) s"kll_merge_agg_$valueType(s)" else sketch
        val values = "VALUES (0), (1) AS i(id) WHERE i.id = o.id"
        val from = if (merge) s"(SELECT $sketch AS s FROM $values GROUP BY i.id)" else values
        checkAnswer(sql(s"""SELECT o.id, q.n FROM VALUES (0), (0), (1), (9) AS o(id),
            |LATERAL (SELECT kll_sketch_get_n_$valueType($aggregate) AS n
            |FROM $from) q""".stripMargin),
          Seq(Row(0, 1L), Row(0, 1L), Row(1, 1L), Row(9, 0L)))
      }
    }
    val outer = spark.range(2).as("o")
    val inner = spark.range(2).toDF().as("i").where($"i.id" === $"o.id".outer())
      .select(expr("kll_sketch_get_n_bigint(kll_sketch_agg_bigint(id))").as("n"))
    checkAnswer(outer.lateralJoin(inner), Seq(Row(0L, 1L), Row(1L, 1L)))
  }

  test("conditional Boolean UDF grouping preserves seeded draw and NULL behavior") {
    withUserDefinedFunction("nullable_bool_identity" -> true) {
      spark.udf.register("nullable_bool_identity", (b: java.lang.Boolean) => b)
      for (codegen <- Seq("CODEGEN_ONLY", "NO_CODEGEN"); operator <- Seq("AND", "OR")) {
        withSQLConf(SQLConf.CODEGEN_FACTORY_MODE.key -> codegen) {
          val left = "CASE WHEN id % 3 = 0 THEN CAST(NULL AS BOOLEAN) ELSE id % 2 = 0 END"
          val expression = s"nullable_bool_identity(($left) $operator (rand(7) > 0.5))"
          val expected = spark.range(0, 128, 1, 1).selectExpr(s"$expression AS b")
            .groupBy("b").count().collect().toSeq
          checkAnswer(sql(s"""SELECT $expression, count(*) FROM range(0, 128, 1, 1)
              |GROUP BY 1""".stripMargin), expected)
        }
      }
    }
  }

  test("compiler KLL bindings retain throwable conjunct ordering") {
    checkAnswer(sql(s"""SELECT id, $scalar AS n, count(*)
        |FROM VALUES (0), (4) AS t(id) GROUP BY 1, 2
        |HAVING n < id AND IF(id = 0, raise_error('bad row'), id) > 0""".stripMargin),
      Row(4, 3L, 1L))
  }

  test("correlated scalars retain independent KLL CTE scopes") {
    for (legacy <- Seq(true, false);
        (aggregate, having, unmatched, matched) <- Seq(
          ("max(a.n + b.n)", "", null, 12L),
          ("sum(a.n + b.n)", "", null, 12L),
          ("count(*)", "", 0L, 1L),
          ("count(*)", "HAVING count(*) > 0", null, 1L),
          ("count(*)", "HAVING count(*) = 0", 0L, null))) {
      withSQLConf("spark.sql.legacy.kllAggregateDeterministic.enabled" -> legacy.toString) {
        checkAnswer(sql(s"""SELECT o.id, (WITH c AS
            |(SELECT kll_sketch_get_n_bigint(kll_sketch_agg_bigint(id)) AS n FROM range(3)),
            |d AS (SELECT a.n + b.n AS n FROM c a CROSS JOIN c b)
            |SELECT $aggregate FROM d a CROSS JOIN d b WHERE a.n = o.id $having) AS n
            |FROM VALUES (0), (6) AS o(id)""".stripMargin),
          Seq(Row(0, unmatched), Row(6, matched)))
      }
    }
  }

  test("invalid direct KLL grouping retains a structured diagnostic") {
    for (valueType <- Seq("bigint", "float", "double"); merge <- Seq(false, true);
        wrapped <- Seq(false, true)) {
      val sketch = s"kll_sketch_agg_$valueType(CAST(id AS $valueType))"
      val aggregate = if (merge) s"kll_merge_agg_$valueType(s)" else sketch
      val from = if (merge) s"(SELECT $sketch AS s FROM range(3) GROUP BY id)" else "range(3)"
      val grouping = if (wrapped) s"kll_sketch_get_n_$valueType($aggregate)" else aggregate
      val error = intercept[AnalysisException] {
        sql(s"SELECT count(*) FROM $from GROUP BY $grouping").collect()
      }
      assert(error.getCondition == "GROUP_BY_AGGREGATE", error.toString)
    }
  }

  test("KLL scalar placement keeps a selective inner join ahead of a Cartesian product") {
    withTempView("kll_a", "kll_b", "kll_c") {
      spark.sparkContext.parallelize(0L until 100L).toDF("id")
        .createOrReplaceTempView("kll_a")
      spark.sparkContext.parallelize(0L until 100L).toDF("id")
        .createOrReplaceTempView("kll_b")
      spark.sparkContext.parallelize(Seq(3L)).toDF("id")
        .createOrReplaceTempView("kll_c")
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
        val query = sql(s"""SELECT a.id, b.id FROM kll_a a CROSS JOIN kll_b b
            |JOIN kll_c c ON a.id = c.id AND b.id + $scalar = c.id""".stripMargin)
        assert(!query.queryExecution.executedPlan.exists(_.nodeName == "CartesianProduct"),
          query.queryExecution.toString)
        checkAnswer(query, Row(3L, 0L))
      }
    }
  }

  test("KLL scalar placement keeps a conditional join component visible to CBO") {
    withTempView("kll_cbo_a", "kll_cbo_b", "kll_cbo_c") {
      val a = spark.sparkContext.parallelize(0L until 100L).toDF("a_key").cache()
      val b = spark.sparkContext.parallelize((0L until 1000L).map(i => (i, i % 100)))
        .toDF("b_id", "b_key").cache()
      val c = spark.sparkContext.parallelize(Seq(3L)).toDF("c_key").cache()
      try {
        a.count()
        b.count()
        c.count()
        a.createOrReplaceTempView("kll_cbo_a")
        b.createOrReplaceTempView("kll_cbo_b")
        c.createOrReplaceTempView("kll_cbo_c")
        withSQLConf(SQLConf.CBO_ENABLED.key -> "true",
            SQLConf.JOIN_REORDER_ENABLED.key -> "true",
            SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
          for (wrapped <- Seq(false, true)) {
            val component = "kll_cbo_a JOIN kll_cbo_b ON a_key = b_key"
            val input = if (wrapped) s"(SELECT a_key, b_id, b_key FROM $component) ab"
              else component
            val query = sql(s"""SELECT a_key, b_id FROM $input
                |JOIN kll_cbo_c ON b_id + $scalar = c_key""".stripMargin)
            assert(query.queryExecution.optimizedPlan.exists {
              case join: org.apache.spark.sql.catalyst.plans.logical.Join =>
                val names = join.output.map(_.name).toSet
                names.contains("b_id") && names.contains("c_key") && !names.contains("a_key")
              case _ => false
            }, query.queryExecution.toString)
            checkAnswer(query, Row(0L, 0L))
          }
        }
      } finally {
        a.unpersist()
        b.unpersist()
        c.unpersist()
      }
    }
  }

  for (groupedScalar <- Seq(false, true)) {
    test(s"KLL aggregates retain selective pruning: groupedScalar=$groupedScalar") {
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "parquet") {
        withTempPath { path =>
          spark.range(8).selectExpr("id", "CAST(id % 2 AS INT) AS p")
            .write.partitionBy("p").parquet(path.getAbsolutePath)
          withTempView("kll_pruning") {
            spark.read.parquet(path.getAbsolutePath).createOrReplaceTempView("kll_pruning")
            val grouped = """(SELECT kll_sketch_get_n_bigint(kll_sketch_agg_bigint(id))
                |FROM range(6) GROUP BY id % 2 LIMIT 1)""".stripMargin
            val queryText = if (groupedScalar) {
              s"SELECT p, count(*) FROM kll_pruning GROUP BY p, $grouped HAVING p = 1"
            } else {
              "SELECT p, kll_sketch_get_n_bigint(kll_sketch_agg_bigint(id)) " +
                "FROM kll_pruning GROUP BY p HAVING p = 1"
            }
            val query = sql(queryText)
            val scans = query.queryExecution.sparkPlan.collect { case s: FileSourceScanExec => s }
            assert(scans.size == 1)
            assert(scans.head.selectedPartitions.filePartitionIterator
              .map(_.values.getInt(0)).toSet == Set(1), query.queryExecution.toString)
            checkAnswer(query, Row(1, 4L))
          }
        }
      }
    }
  }

  test("extracted UDFs preserve conditional nondeterministic evaluation") {
    val first = udf((a: Seq[Double]) => a.head)
    withUserDefinedFunction("branch_first" -> true) {
      spark.udf.register("branch_first", (a: Seq[Double]) => a.head)
      for (codegen <- Seq("CODEGEN_ONLY", "NO_CODEGEN")) {
        withSQLConf(SQLConf.CODEGEN_FACTORY_MODE.key -> codegen,
            SQLConf.ANSI_ENABLED.key -> "true") {
          val data = spark.range(2)
          val key = first(expr("IF(id = 0, array(0D), shuffle(array(1D / id, 2D)))"))
          assert(data.sort(key).collect().toSeq == Seq(0L, 1L))
          checkAnswer(sql("""SELECT count(*) FROM range(2) GROUP BY
              |branch_first(IF(id = 0, array(0D), shuffle(array(1D / id, 2D))))""".stripMargin),
            Seq(Row(1L), Row(1L)))
        }
      }
    }
  }

  test("boolean wrappers retain KLL scalar equi join keys") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      for (condition <- Seq(s"(l.id = r.id + $scalar) OR false",
          s"NOT NOT (l.id = r.id + $scalar)")) {
        val query = sql(s"""SELECT l.id, r.id FROM range(5) l JOIN range(2) r
            |ON $condition""".stripMargin)
        assert(query.queryExecution.executedPlan.exists {
          case _: BroadcastHashJoinExec | _: SortMergeJoinExec => true
          case _ => false
        }, query.queryExecution.toString)
        checkAnswer(query, Seq(Row(3L, 0L), Row(4L, 1L)))
      }
    }
  }

  test("observed aggregate wrappers accept an independent KLL scalar sibling") {
    val observation = Observation()
    val data = spark.range(2).observe("kll_scalar_metric", expr(s"count(*) + $scalar").as("n"))
      .observe(observation, expr(s"count(*) + $scalar").as("n"))
    data.collect()
    assert(observation.get == Map("n" -> 5L))
    assert(data.queryExecution.observedMetrics("kll_scalar_metric") == Row(5L))
  }

  test("a shared KLL scalar preserves both SQL function equi join operands") {
    withUserDefinedFunction("kll_equal" -> true) {
      sql("""CREATE TEMPORARY FUNCTION kll_equal(a BIGINT, b BIGINT, s BIGINT)
          |RETURNS BOOLEAN RETURN a + s = b + s""".stripMargin)
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        val query = sql(s"""SELECT l.id, r.id FROM range(3) l JOIN range(3) r
            |ON kll_equal(l.id, r.id, $scalar)""".stripMargin)
        assert(query.queryExecution.executedPlan.exists {
          case _: BroadcastHashJoinExec | _: SortMergeJoinExec => true
          case _ => false
        }, query.queryExecution.toString)
        checkAnswer(query, Seq(Row(0L, 0L), Row(1L, 1L), Row(2L, 2L)))
      }
    }
  }

  test("KLL IN bindings preserve early V2 selective partition pruning") {
    withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
      withTempPath { path =>
        spark.range(8).selectExpr("id", "CAST(id % 2 AS INT) AS p")
          .write.partitionBy("p").parquet(path.getAbsolutePath)
        withTempView("kll_v2_partitioned") {
          spark.read.parquet(path.getAbsolutePath).createOrReplaceTempView("kll_v2_partitioned")
          val query = sql(s"""SELECT p, count(*) FROM kll_v2_partitioned
              |GROUP BY p, id IN (SELECT $scalar) HAVING p = 1""".stripMargin)
          val scans = query.queryExecution.sparkPlan.collect {
            case scan: BatchScanExec if scan.scan.isInstanceOf[FileScan] =>
              scan.scan.asInstanceOf[FileScan]
          }
          assert(scans.size == 1)
          val scan = scans.head
          val selected = scan.fileIndex.listFiles(scan.partitionFilters, scan.dataFilters)
          assert(selected.size == 1 && selected.head.values.getInt(0) == 1,
            query.queryExecution.toString)
          checkAnswer(query, Seq(Row(1, 1L), Row(1, 3L)))
        }
      }
    }
  }

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
          val partitions = scans.head.selectedPartitions.filePartitionIterator
            .map(_.values.getInt(0)).toSet
          assert(partitions == Set(1),
            query.queryExecution.toString)
          checkAnswer(query, Row(1L))
        }
      }
    }
  }
}
