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

package org.apache.spark.sql.collation

import java.util.Locale

import org.apache.spark.sql.{AnalysisException, Column, DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, ObjectHashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.CalendarInterval

class CollationAggregationSuite
  extends QueryTest
  with SharedSparkSession
  with AdaptiveSparkPlanHelper {

  import testImplicits._

  private def assertUsesHashAggregate(df: DataFrame): Unit = {
    val plan = df.queryExecution.executedPlan
    assert(exists(plan)(_.isInstanceOf[HashAggregateExec]), plan.toString)
    assert(!exists(plan)(_.isInstanceOf[SortAggregateExec]), plan.toString)
  }

  private def assertUsesObjectHashAggregate(df: DataFrame): Unit = {
    val plan = df.queryExecution.executedPlan
    assert(exists(plan)(_.isInstanceOf[ObjectHashAggregateExec]), plan.toString)
    assert(!exists(plan)(_.isInstanceOf[SortAggregateExec]), plan.toString)
  }

  private def assertUsesSortAggregate(df: DataFrame): Unit = {
    val plan = df.queryExecution.executedPlan
    assert(exists(plan)(_.isInstanceOf[SortAggregateExec]), plan.toString)
    assert(!exists(plan)(_.isInstanceOf[HashAggregateExec]), plan.toString)
    assert(!exists(plan)(_.isInstanceOf[ObjectHashAggregateExec]), plan.toString)
  }

  private def generatedCode(df: DataFrame): String = {
    flatMap(df.queryExecution.executedPlan) {
      case stage: WholeStageCodegenExec => Seq(stage.doCodeGen()._2.body)
      case _ => Nil
    }.mkString("\n")
  }

  test("HashAggregateExec groups collated string keys") {
    val testCases = Seq(
      "UTF8_LCASE" -> Seq("hello", "HELLO", "HeLlO"),
      "UNICODE_CI" -> Seq("hello", "HELLO", "HeLlO"),
      "UTF8_BINARY_RTRIM" -> Seq("hello", "hello ", "hello  "))

    testCases.foreach { case (collation, spellings) =>
      withTempView("collated_keys") {
        val values = spellings.zipWithIndex
          .map { case (key, index) => s"('$key', ${index + 1})" }
          .mkString(", ")
        sql(
          s"""
             |SELECT CAST(key AS STRING COLLATE $collation) AS key, value
             |FROM VALUES $values, (NULL, 4) AS data(key, value)
             |""".stripMargin).createOrReplaceTempView("collated_keys")

        val result = sql(
          """
            |SELECT LOWER(RTRIM(key)) AS normalized_key, SUM(value) AS total
            |FROM collated_keys
            |GROUP BY key
            |ORDER BY normalized_key NULLS LAST
            |""".stripMargin)
        assertUsesHashAggregate(result)
        checkAnswer(result, Seq(Row("hello", 6L), Row(null, 4L)))
      }
    }
  }

  test("HashAggregateExec preserves binary semantics for stable sibling keys") {
    withTempView("interval_keys") {
      Seq(
        ("hello", new CalendarInterval(1, 2, 3), 1),
        ("HELLO", new CalendarInterval(1, 2, 3), 2))
        .toDF("key", "calendar_interval", "value")
        .selectExpr(
          "CAST(key AS STRING COLLATE UTF8_LCASE) AS key",
          "calendar_interval",
          "value")
        .repartition(1)
        .createOrReplaceTempView("interval_keys")

      withSQLConf(
        SQLConf.CODEGEN_FACTORY_MODE.key -> CodegenObjectFactoryMode.NO_CODEGEN.toString) {
        val result = sql(
          """
            |SELECT LOWER(key) AS normalized_key, SUM(value) AS total
            |FROM interval_keys
            |GROUP BY key, calendar_interval
            |""".stripMargin)
        assertUsesHashAggregate(result)
        checkAnswer(result, Row("hello", 3L))

        val objectResult = sql(
          """
            |SELECT LOWER(key) AS normalized_key, ARRAY_SORT(COLLECT_LIST(value)) AS values
            |FROM interval_keys
            |GROUP BY key, calendar_interval
            |""".stripMargin)
        assertUsesObjectHashAggregate(objectResult)
        checkAnswer(objectResult, Row("hello", Seq(1, 2)))
      }
    }

    withTempView("geometry_keys") {
      sql(
        """
          |SELECT
          |  CAST(key AS STRING COLLATE UTF8_LCASE) AS key,
          |  ST_GeomFromWKB(wkb) AS geometry,
          |  value
          |FROM VALUES
          |  ('hello', X'0101000000000000000000F03F0000000000000040', 1),
          |  ('HELLO', X'0101000000000000000000F03F0000000000000040', 2)
          |AS data(key, wkb, value)
          |""".stripMargin)
        .repartition(1)
        .createOrReplaceTempView("geometry_keys")

      val result = sql(
        """
          |SELECT LOWER(key) AS normalized_key, SUM(value) AS total
          |FROM geometry_keys
          |GROUP BY key, geometry
          |""".stripMargin)
      assertUsesHashAggregate(result)
      checkAnswer(result, Row("hello", 3L))
    }
  }

  test("nested and distinct collated grouping works across partial and final aggregation") {
    withTempView("nested_keys") {
      sql(
        """
          |SELECT id, CAST(key AS STRING COLLATE UTF8_LCASE) AS key, bucket, value
          |FROM VALUES
          |  (1, 'alpha', 0, 10),
          |  (2, 'ALPHA', 0, 20),
          |  (3, 'beta', 1, 30),
          |  (4, 'BETA', 1, 40),
          |  (5, NULL, 2, 50),
          |  (6, NULL, 2, 60)
          |AS data(id, key, bucket, value)
          |""".stripMargin)
        .repartition(4)
        .createOrReplaceTempView("nested_keys")

      val distinctKeys = sql(
        """
          |SELECT LOWER(key) AS normalized_key
          |FROM (SELECT DISTINCT key FROM nested_keys)
          |ORDER BY normalized_key NULLS LAST
          |""".stripMargin)
      assertUsesHashAggregate(distinctKeys)
      checkAnswer(distinctKeys, Seq(Row("alpha"), Row("beta"), Row(null)))

      val result = sql(
        """
          |SELECT
          |  LOWER(array_key[0]) AS array_key,
          |  LOWER(struct_key.s) AS struct_key,
          |  bucket,
          |  distinct_ids,
          |  total
          |FROM (
          |  SELECT
          |    ARRAY(key) AS array_key,
          |    NAMED_STRUCT('s', key) AS struct_key,
          |    bucket,
          |    COUNT(DISTINCT id) AS distinct_ids,
          |    SUM(value) AS total
          |  FROM nested_keys
          |  GROUP BY ARRAY(key), NAMED_STRUCT('s', key), bucket
          |)
          |ORDER BY bucket
          |""".stripMargin)
      assertUsesHashAggregate(result)
      checkAnswer(result, Seq(
        Row("alpha", "alpha", 0, 2L, 30L),
        Row("beta", "beta", 1, 2L, 70L),
        Row(null, null, 2, 2L, 110L)))
    }
  }

  test("ObjectHashAggregateExec groups collated keys before and after sort fallback") {
    withTempView("object_keys") {
      sql(
        """
          |SELECT CAST(key AS STRING COLLATE UTF8_LCASE) AS key, value
          |FROM VALUES
          |  ('hello', 1), ('world', 4),
          |  ('HELLO', 2), ('WORLD', 5),
          |  ('HeLlO', 3), ('WoRlD', 6)
          |AS data(key, value)
          |""".stripMargin).createOrReplaceTempView("object_keys")

      Seq(Int.MaxValue.toString, "1").foreach { fallbackThreshold =>
        withSQLConf(
          SQLConf.OBJECT_AGG_SORT_BASED_FALLBACK_THRESHOLD.key -> fallbackThreshold) {
          val result = sql(
            """
              |SELECT
              |  LOWER(key) AS normalized_key,
              |  ARRAY_SORT(COLLECT_LIST(value)) AS collected_list,
              |  ARRAY_SORT(COLLECT_SET(value)) AS collected_set
              |FROM object_keys
              |GROUP BY key
              |ORDER BY normalized_key
              |""".stripMargin)
          assertUsesObjectHashAggregate(result)
          checkAnswer(result, Seq(
            Row("hello", Seq(1, 2, 3), Seq(1, 2, 3)),
            Row("world", Seq(4, 5, 6), Seq(4, 5, 6))))

          if (fallbackThreshold == "1") {
            assert(exists(result.queryExecution.executedPlan) {
              case aggregate: ObjectHashAggregateExec =>
                aggregate.metrics("numTasksFallBacked").value > 0
              case _ => false
            })
          }
        }
      }
    }
  }

  test("HashAggregateExec spill merges collation-equivalent keys") {
    withTempView("spill_keys") {
      sql(
        """
          |SELECT CAST(key AS STRING COLLATE UTF8_LCASE_RTRIM) AS key, value
          |FROM VALUES
          |  ('hello', 1), ('world', 4),
          |  ('HELLO ', 2), ('WORLD ', 5),
          |  ('HeLlO  ', 3), ('WoRlD  ', 6)
          |AS data(key, value)
          |""".stripMargin).createOrReplaceTempView("spill_keys")

      Seq(true, false).foreach { wholeStage =>
        withSQLConf(
          SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> wholeStage.toString,
          "spark.sql.TungstenAggregate.testFallbackStartsAt" -> "0, 1") {
          withClue(s"wholeStage=$wholeStage: ") {
            val result = sql(
              """
                |SELECT LOWER(RTRIM(key)) AS normalized_key, SUM(value) AS total
                |FROM spill_keys
                |GROUP BY key
                |ORDER BY normalized_key
                |""".stripMargin)
            assertUsesHashAggregate(result)
            checkAnswer(result, Seq(Row("hello", 6L), Row("world", 15L)))
            assert(exists(result.queryExecution.executedPlan) {
              case aggregate: HashAggregateExec =>
                aggregate.metrics("numTasksFallBacked").value > 0
              case _ => false
            })
          }
        }
      }
    }
  }

  test("two-level aggregation remains enabled only for binary-stable grouping keys") {
    withSQLConf(SQLConf.ENABLE_TWOLEVEL_AGG_MAP.key -> "true") {
      val binary = sql(
        """
          |SELECT key, SUM(value)
          |FROM VALUES ('a', 1), ('a', 2), ('b', 3) AS data(key, value)
          |GROUP BY key
          |ORDER BY key
          |""".stripMargin)
      assertUsesHashAggregate(binary)
      checkAnswer(binary, Seq(Row("a", 3L), Row("b", 3L)))
      val binaryCode = generatedCode(binary)
      assert(binaryCode.contains("FastHashMap"))

      val collated = sql(
        """
          |SELECT LOWER(key) AS normalized_key, SUM(value)
          |FROM (
          |  SELECT CAST(key AS STRING COLLATE UTF8_LCASE) AS key, value
          |  FROM VALUES ('a', 1), ('A', 2) AS data(key, value)
          |)
          |GROUP BY key
          |""".stripMargin)
      assertUsesHashAggregate(collated)
      checkAnswer(collated, Row("a", 3L))
      val collatedCode = generatedCode(collated)
      assert(!collatedCode.contains("FastHashMap"))
      assert(collatedCode.contains("getAggregationBufferFromUnsafeRow"))
    }
  }

  test("SortAggregateExec groups collated keys when hash aggregation is disabled") {
    withSQLConf(
      SQLConf.USE_HASH_AGG.key -> "false",
      SQLConf.USE_OBJECT_HASH_AGG.key -> "false") {
      val result = sql(
        """
          |SELECT LOWER(key) AS normalized_key, SUM(value) AS total
          |FROM (
          |  SELECT CAST(key AS STRING COLLATE UTF8_LCASE) AS key, value
          |  FROM VALUES
          |    ('hello', 1), ('HELLO', 2),
          |    ('world', 4), ('WORLD', 5)
          |  AS data(key, value)
          |)
          |GROUP BY key
          |ORDER BY normalized_key
          |""".stripMargin)
      assertUsesSortAggregate(result)
      checkAnswer(result, Seq(Row("hello", 3L), Row("world", 9L)))
    }
  }

  // collect_set is non-deterministic in which representative of a collation-equal group it keeps,
  // so tests assert on a collation-collapsed form (lower(...)) rather than a fixed case.

  test("collect_set dedups collation-equal strings (UTF8_LCASE)") {
    val tblName = "collect_set_lcase"
    withTable(tblName) {
      sql(s"CREATE TABLE $tblName (c1 STRING COLLATE UTF8_LCASE) USING PARQUET")
      sql(s"INSERT INTO $tblName VALUES ('foo'), ('FOO'), ('FoO'), ('bar'), ('BAR')")

      // 'foo'/'FOO'/'FoO' collapse to one group and 'bar'/'BAR' to another under UTF8_LCASE.
      checkAnswer(sql(s"SELECT size(collect_set(c1)) FROM $tblName"), Seq(Row(2)))
      checkAnswer(
        sql(s"SELECT array_sort(transform(collect_set(c1), x -> lower(x))) FROM $tblName"),
        Seq(Row(Seq("bar", "foo"))))
    }
  }

  test("collect_set dedups collation-equal strings (UNICODE_CI)") {
    val tblName = "collect_set_unicode_ci"
    withTable(tblName) {
      sql(s"CREATE TABLE $tblName (c1 STRING COLLATE UNICODE_CI) USING PARQUET")
      sql(s"INSERT INTO $tblName VALUES ('cafe'), ('CAFE'), ('Cafe'), ('bar')")

      checkAnswer(sql(s"SELECT size(collect_set(c1)) FROM $tblName"), Seq(Row(2)))
      checkAnswer(
        sql(s"SELECT array_sort(transform(collect_set(c1), x -> lower(x))) FROM $tblName"),
        Seq(Row(Seq("bar", "cafe"))))
    }
  }

  test("collect_set is collation-aware for collated strings nested in struct/array") {
    val tblName = "collect_set_nested"
    withTable(tblName) {
      sql(s"CREATE TABLE $tblName (c1 STRING COLLATE UTF8_LCASE, c2 INT) USING PARQUET")
      sql(s"INSERT INTO $tblName VALUES ('foo', 1), ('FOO', 1), ('bar', 1)")

      // Nested in a struct: named_struct('s', c1) dedups on c1's collation key.
      checkAnswer(
        sql(s"SELECT size(collect_set(named_struct('s', c1))) FROM $tblName"), Seq(Row(2)))
      checkAnswer(
        sql(s"SELECT array_sort(transform(collect_set(named_struct('s', c1)), " +
          s"x -> lower(x.s))) FROM $tblName"),
        Seq(Row(Seq("bar", "foo"))))

      // Nested in an array: array(c1) dedups on the element's collation key.
      checkAnswer(sql(s"SELECT size(collect_set(array(c1))) FROM $tblName"), Seq(Row(2)))
      checkAnswer(
        sql(s"SELECT array_sort(transform(collect_set(array(c1)), x -> lower(x[0]))) " +
          s"FROM $tblName"),
        Seq(Row(Seq("bar", "foo"))))
    }
  }

  test("collect_set is collation-aware for collated strings nested at depth >= 2") {
    val tblName = "collect_set_nested_deep"
    withTable(tblName) {
      sql(s"CREATE TABLE $tblName (c1 STRING COLLATE UTF8_LCASE) USING PARQUET")
      sql(s"INSERT INTO $tblName VALUES ('foo'), ('FOO'), ('bar')")

      // array(named_struct('s', c1)) nests the collated string two levels deep (array of struct),
      // exercising the injectCollationKey recursion below the top-level struct/array cases above.
      checkAnswer(
        sql(s"SELECT size(collect_set(array(named_struct('s', c1)))) FROM $tblName"),
        Seq(Row(2)))
      checkAnswer(
        sql(s"SELECT array_sort(transform(collect_set(array(named_struct('s', c1))), " +
          s"x -> lower(x[0].s))) FROM $tblName"),
        Seq(Row(Seq("bar", "foo"))))
    }
  }

  test("collect_set dedups on a space-trimming collation (UTF8_LCASE_RTRIM)") {
    val tblName = "collect_set_lcase_rtrim"
    withTable(tblName) {
      sql(s"CREATE TABLE $tblName (c1 STRING COLLATE UTF8_LCASE_RTRIM) USING PARQUET")
      sql(s"INSERT INTO $tblName VALUES ('foo '), ('foo'), ('FOO'), ('bar'), ('BAR  ')")

      // UTF8_LCASE_RTRIM ignores both case and trailing spaces, so 'foo '/'foo'/'FOO' collapse to
      // one group and 'bar'/'BAR  ' to another. The retained representative may keep trailing
      // spaces, so collapse with trim(lower(...)) before comparing.
      checkAnswer(sql(s"SELECT size(collect_set(c1)) FROM $tblName"), Seq(Row(2)))
      checkAnswer(
        sql(s"SELECT array_sort(transform(collect_set(c1), x -> trim(lower(x)))) FROM $tblName"),
        Seq(Row(Seq("bar", "foo"))))
    }
  }

  test("collect_set collation and float normalization compose for nested structs") {
    val tblName = "collect_set_nested_float"
    withTable(tblName) {
      sql(s"CREATE TABLE $tblName (c1 STRING COLLATE UTF8_LCASE, d DOUBLE) USING PARQUET")
      // ('foo', 0.0) and ('FOO', -0.0) dedup only if BOTH the collation key and float
      // normalization fire on the same nested key (size 2, not 3).
      sql(s"INSERT INTO $tblName VALUES ('foo', 0.0), ('FOO', -0.0), ('bar', 1.0)")

      checkAnswer(
        sql(s"SELECT size(collect_set(named_struct('s', c1, 'd', d))) FROM $tblName"),
        Seq(Row(2)))
      checkAnswer(
        sql(s"SELECT array_sort(transform(collect_set(named_struct('s', c1, 'd', d)), " +
          s"x -> lower(x.s))) FROM $tblName"),
        Seq(Row(Seq("bar", "foo"))))
    }
  }

  test("collect_set IGNORE NULLS (default) drops nulls while deduping collated values") {
    val tblName = "collect_set_ignore_nulls"
    withTable(tblName) {
      sql(s"CREATE TABLE $tblName (c1 STRING COLLATE UTF8_LCASE) USING PARQUET")
      sql(s"INSERT INTO $tblName VALUES ('foo'), ('FOO'), (NULL), ('bar'), (NULL)")

      // Default IGNORE NULLS: nulls are dropped, and 'foo'/'FOO' still collapse under UTF8_LCASE,
      // so the set is {<foo group>, <bar group>} -> size 2 with no null element.
      checkAnswer(sql(s"SELECT size(collect_set(c1)) FROM $tblName"), Seq(Row(2)))
      checkAnswer(
        sql(s"SELECT array_sort(transform(collect_set(c1), x -> lower(x))) FROM $tblName"),
        Seq(Row(Seq("bar", "foo"))))
    }
  }

  test("collect_set still rejects maps even when they carry collated strings") {
    // The collation relaxation must not open the map gate: a HashSet cannot dedup maps, so a map
    // with a collated-string key (which would otherwise take the collation-key path) must still
    // fail checkInputDataTypes with UNSUPPORTED_INPUT_TYPE.
    val collatedMap = spark.sql(
      "SELECT map(CAST(k AS STRING COLLATE UTF8_LCASE), v) AS m " +
        "FROM VALUES ('a', 1), ('A', 2) AS t(k, v)")
    checkError(
      exception = intercept[AnalysisException](collatedMap.select(collect_set(col("m")))),
      condition = "DATATYPE_MISMATCH.UNSUPPORTED_INPUT_TYPE",
      parameters = Map(
        "functionName" -> "`collect_set`",
        "dataType" -> "\"MAP\"",
        "sqlExpr" -> "\"collect_set(m)\""),
      context = ExpectedContext(
        fragment = "collect_set", callSitePattern = getCurrentClassCallSitePattern))

    // A map nested alongside a collated string in a struct is still rejected: existsRecursively
    // finds the MapType before the collation-key path is ever considered.
    val nestedMap = spark.sql(
      "SELECT named_struct('s', CAST(k AS STRING COLLATE UTF8_LCASE), 'm', map(k, v)) AS a " +
        "FROM VALUES ('a', 1), ('A', 2) AS t(k, v)")
    checkError(
      exception = intercept[AnalysisException](nestedMap.select(collect_set(col("a")))),
      condition = "DATATYPE_MISMATCH.UNSUPPORTED_INPUT_TYPE",
      parameters = Map(
        "functionName" -> "`collect_set`",
        "dataType" -> "\"MAP\"",
        "sqlExpr" -> "\"collect_set(a)\""),
      context = ExpectedContext(
        fragment = "collect_set", callSitePattern = getCurrentClassCallSitePattern))
  }

  test("collect_set RESPECT NULLS keeps one null alongside collation-deduped values") {
    val tblName = "collect_set_respect_nulls"
    withTable(tblName) {
      sql(s"CREATE TABLE $tblName (c1 STRING COLLATE UTF8_LCASE) USING PARQUET")
      sql(s"INSERT INTO $tblName VALUES ('foo'), ('FOO'), (NULL), ('bar'), (NULL)")

      // RESPECT NULLS keeps a single null; 'foo'/'FOO' still collapse under UTF8_LCASE, so the
      // set is {null, <foo group>, <bar group>} -> size 3. sort_array orders null first.
      checkAnswer(
        sql(s"SELECT size(collect_set(c1) RESPECT NULLS) FROM $tblName"), Seq(Row(3)))
      checkAnswer(
        sql(s"SELECT sort_array(transform(collect_set(c1) RESPECT NULLS, x -> lower(x))) " +
          s"FROM $tblName"),
        Seq(Row(Seq(null, "bar", "foo"))))
    }
  }

  test("collect_set collation-aware dedup survives the merge path across partitions") {
    Seq("UTF8_LCASE", "UNICODE_CI").foreach { collation =>
      withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "8") {
        val values = (0 until 200).map(i => if (i % 2 == 0) "foo" else "FOO")
        val df = values.toDF("c")
          .select(col("c").cast(s"string collate $collation").as("c"))
          .repartition(8)

        // All 200 values are collation-equal, so after partial aggregation on 8 partitions and
        // the final merge (serialize/deserialize + union), the set has a single element.
        val result = df.agg(collect_set(col("c")).as("s"))
        checkAnswer(result.select(size(col("s"))), Seq(Row(1)))
        checkAnswer(result.select(transform(col("s"), x => lower(x))), Seq(Row(Seq("foo"))))
      }
    }
  }

  // `pandas_mode` (backing pandas-on-Spark Series.mode / DataFrame.mode) is an internal
  // expression, so it is invoked here via `Column.internalFn` rather than SQL. Its second
  // argument is `ignoreNA` (true = drop NULLs, mirroring pandas `dropna`). It returns an
  // array of all the most frequent values; the array order is unspecified, so the helpers
  // below normalize before asserting.

  private def pandasMode(df: DataFrame, colName: String, ignoreNA: Boolean): Seq[AnyRef] = {
    df.select(Column.internalFn("pandas_mode", col(colName), lit(ignoreNA)))
      .collect().head.getSeq[AnyRef](0)
  }

  // Case-insensitive, order-insensitive view of a string-valued mode result.
  private def normalizedStringModes(modes: Seq[AnyRef]): Set[String] =
    modes.map {
      case null => null
      case s: String => s.toLowerCase(Locale.ROOT)
    }.toSet

  test("SPARK-48701: pandas_mode is collation-aware for non-binary collations") {
    Seq("UTF8_LCASE", "UNICODE_CI").foreach { collation =>
      withTable("t") {
        sql(s"CREATE TABLE t (c STRING COLLATE $collation) USING parquet")
        // Spread across partitions to also exercise the partial-buffer merge path.
        sql("INSERT INTO t VALUES ('a'), ('a'), ('b'), ('B')")
        val modes = pandasMode(spark.table("t").repartition(4), "c", ignoreNA = true)
        // 'b' and 'B' are collation-equal, so they fold into one group of 2 that ties
        // 'a' (also 2). Both are modes. Without folding 'a' (2) would win alone.
        assert(modes.length == 2, s"$collation: expected two modes, got $modes")
        assert(normalizedStringModes(modes) == Set("a", "b"))
      }
    }
  }

  test("collect_set on UTF8_BINARY strings is unchanged (case-sensitive)") {
    val tblName = "collect_set_binary"
    withTable(tblName) {
      sql(s"CREATE TABLE $tblName (c1 STRING) USING PARQUET")
      sql(s"INSERT INTO $tblName VALUES ('foo'), ('FOO'), ('foo'), ('bar')")

      // Default UTF8_BINARY collation is case-sensitive: 'foo' and 'FOO' stay distinct.
      checkAnswer(sql(s"SELECT size(collect_set(c1)) FROM $tblName"), Seq(Row(3)))
      checkAnswer(
        sql(s"SELECT array_sort(collect_set(c1)) FROM $tblName"),
        Seq(Row(Seq("FOO", "bar", "foo"))))
    }
  }

  test("SPARK-48701: pandas_mode keeps binary collation unchanged") {
    withTable("t") {
      // Default UTF8_BINARY: 'b' and 'B' are distinct, so 'a' (2) is the sole mode.
      sql("CREATE TABLE t (c STRING) USING parquet")
      sql("INSERT INTO t VALUES ('a'), ('a'), ('b'), ('B')")
      val modes = pandasMode(spark.table("t").repartition(4), "c", ignoreNA = true)
      assert(modes == Seq("a"))
    }
  }

  test("SPARK-48701: pandas_mode collation-aware with ignoreNA controlling NULLs") {
    withTable("t") {
      sql("CREATE TABLE t (c STRING COLLATE UTF8_LCASE) USING parquet")
      sql("INSERT INTO t VALUES ('a'), ('b'), ('B'), (null), (null), (null)")
      val df = spark.table("t").repartition(4)

      // ignoreNA = true: NULLs dropped. 'b'/'B' fold to 2, outvoting 'a' (1).
      val dropped = pandasMode(df, "c", ignoreNA = true)
      assert(dropped.length == 1)
      assert(normalizedStringModes(dropped) == Set("b"))

      // ignoreNA = false: the null key is preserved as its own group (count 3) and wins.
      // This also exercises the null guard in getCollationAwareBuffer's folding.
      val kept = pandasMode(df, "c", ignoreNA = false)
      assert(kept == Seq(null))
    }
  }

  test("SPARK-48701: pandas_mode collation-aware for collated string nested in struct") {
    withTable("t") {
      sql("CREATE TABLE t (c STRUCT<f: STRING COLLATE UTF8_LCASE>) USING parquet")
      sql(
        """INSERT INTO t VALUES (named_struct('f', 'a')), (named_struct('f', 'a')),
          |  (named_struct('f', 'b')), (named_struct('f', 'B'))""".stripMargin)
      val modes = pandasMode(spark.table("t").repartition(4), "c", ignoreNA = true)
        .map(_.asInstanceOf[Row])
      // Same fold as the top-level case, applied to the collated struct field.
      assert(modes.length == 2)
      assert(modes.map(_.getString(0).toLowerCase(Locale.ROOT)).toSet == Set("a", "b"))
    }
  }

  // `mode` (the public aggregate) is already collation-aware; these tests guard that
  // behavior, which currently has no coverage (the original tests were removed with
  // CollationSQLExpressionsSuite by SPARK-51067). Unlike pandas_mode, `mode` returns a
  // single value and shares the same eval-time folding via ModeCollationAware.

  test("SPARK-47353: mode is collation-aware for non-binary collations") {
    // Buffer counts a=3, b=2, B=2. Under a case-insensitive collation 'b' and 'B' fold
    // into one group of 4 that outvotes 'a' (3); under binary collation 'a' (3) wins.
    Seq(
      ("UTF8_BINARY", "a"),
      ("UTF8_LCASE", "b"),
      ("UNICODE", "a"),
      ("UNICODE_CI", "b")).foreach { case (collation, expected) =>
      withTable("t") {
        sql(s"CREATE TABLE t (c STRING COLLATE $collation) USING parquet")
        sql("INSERT INTO t VALUES ('a'), ('a'), ('a'), ('b'), ('b'), ('B'), ('B')")
        val df = sql("SELECT mode(c) AS m FROM t")
        // The result keeps the input's collated string type.
        assert(df.schema("m").dataType.sameType(StringType(collation)))
        // Representative case within a folded group is unspecified; normalize with lower.
        checkAnswer(df.select(lower(col("m"))), Row(expected))
      }
    }
  }

  test("SPARK-47353: mode is collation-aware for collated string nested in struct") {
    withTable("t") {
      sql("CREATE TABLE t (c STRUCT<f: STRING COLLATE UTF8_LCASE>) USING parquet")
      sql(
        """INSERT INTO t VALUES (named_struct('f', 'a')), (named_struct('f', 'a')),
          |  (named_struct('f', 'a')), (named_struct('f', 'b')), (named_struct('f', 'b')),
          |  (named_struct('f', 'B')), (named_struct('f', 'B'))""".stripMargin)
      // The collated struct field folds: {b, B} (4) outvotes {a} (3).
      checkAnswer(sql("SELECT lower(mode(c).f) FROM t"), Row("b"))
    }
  }
}
