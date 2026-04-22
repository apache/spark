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

package org.apache.spark.sql.connector

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.expressions.{DynamicPruning, DynamicPruningExpression}
import org.apache.spark.sql.connector.catalog.{BufferedRows, InMemoryEnhancedRuntimePartitionFilterTable, InMemoryTableEnhancedRuntimePartitionFilterCatalog}
import org.apache.spark.sql.connector.expressions.PartitionFieldReference
import org.apache.spark.sql.connector.expressions.filter.PartitionPredicate
import org.apache.spark.sql.execution.ExplainUtils.stripAQEPlan
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Tests that [[PartitionPredicate]] instances are pushed via iterative runtime filtering
 * (second [[SupportsRuntimeV2Filtering#filter]] call) for both DPP and scalar subquery
 * runtime filters.
 *
 * Pushdown cases (DPP / Scalar Subquery, Translated / Untranslatable,
 *     Accepted / Rejected, Partition / Data Column, In filterAttributes):
 *
 * PartitionPredicate IS created:
 *  1. DPP, translated, rejected in 1st pass, partition col -> PartitionPredicate
 *  2. DPP, translated, rejected in 1st pass, non-first partition col -> PartitionPredicate
 *  3. Scalar, translatable, rejected in 1st pass, partition col -> PartitionPredicate
 *  4. Scalar, untranslatable, partition col -> PartitionPredicate
 *  5. Scalar, two subqueries on two partition cols -> 2 PartitionPredicates
 *  6. Scalar, non-first of 3 partition cols -> PartitionPredicate
 *  7. Mixed: 1st pass accepted + untranslatable -> only untranslatable gets PartitionPredicate
 *
 * PartitionPredicate is NOT created:
 *  8. DPP, translated, accepted in 1st pass -> no PartitionPredicate
 *  9. Scalar, translatable, accepted in 1st pass -> no PartitionPredicate
 * 10. Scalar on data column -> no PartitionPredicate
 * 11. supportsIterativePushdown is false -> no PartitionPredicate
 * 12. Partition col not in filterAttributes -> no PartitionPredicate
 */
class DataSourceV2EnhancedRuntimePartitionFilterSuite
  extends QueryTest with SharedSparkSession with BeforeAndAfter {

  protected val v2Source = classOf[FakeV2ProviderWithCustomSchema].getName
  protected val catalogName = "testruntimepartfilter"

  before {
    spark.conf.set(s"spark.sql.catalog.$catalogName",
      classOf[InMemoryTableEnhancedRuntimePartitionFilterCatalog].getName)
  }

  after {
    spark.sessionState.catalogManager.reset()
  }

  private def withDPPConf(f: => Unit): Unit = {
    withSQLConf(
      SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
      SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "false",
      SQLConf.DYNAMIC_PARTITION_PRUNING_FALLBACK_FILTER_RATIO.key -> "10")(f)
  }

  // ---------------------------------------------------------------------------
  // PartitionPredicate IS created
  // ---------------------------------------------------------------------------

  test("case 1: DPP translated, rejected in 1st pass -> PartitionPredicate") {
    val fact = s"$catalogName.fact"
    val dim = s"$catalogName.dim"
    withTable(fact, dim) {
      sql(s"CREATE TABLE $fact (id INT, part INT) USING $v2Source PARTITIONED BY (part)")
      for (i <- 0 until 5) {
        sql(s"INSERT INTO $fact VALUES ($i, $i)")
      }
      sql(s"CREATE TABLE $dim (dim_id INT, dim_val STRING) USING $v2Source")
      sql(s"INSERT INTO $dim VALUES (2, 'two')")

      withDPPConf {
        val df = sql(
          s"""SELECT f.id, f.part FROM $fact f JOIN $dim d
             |ON f.part = d.dim_id WHERE d.dim_val = 'two'""".stripMargin)
        checkAnswer(df, Row(2, 2))

        assertDPPRuntimeFilters(df)
        assertPushedPartitionPredicates(df, 1)
        assertScanReturnsPartitionKeys(df, Set("2"))
        assertReferencedPartitionFieldOrdinals(df, Array(0), Array("part"))
      }
    }
  }

  test("case 2: DPP translated, rejected, non-first partition col -> PartitionPredicate") {
    val fact = s"$catalogName.fact2"
    val dim = s"$catalogName.dim2"
    withTable(fact, dim) {
      sql(s"CREATE TABLE $fact (id INT, p1 INT, p2 INT) " +
        s"USING $v2Source PARTITIONED BY (p1, p2)")
      for (i <- 0 until 5; j <- 0 until 2) {
        sql(s"INSERT INTO $fact VALUES (${i * 2 + j}, $i, $j)")
      }
      sql(s"CREATE TABLE $dim (dim_id INT, dim_val STRING) USING $v2Source")
      sql(s"INSERT INTO $dim VALUES (1, 'one')")

      withDPPConf {
        val df = sql(
          s"""SELECT f.id, f.p1, f.p2 FROM $fact f JOIN $dim d
             |ON f.p2 = d.dim_id
             |WHERE d.dim_val = 'one'""".stripMargin)
        checkAnswer(df, Seq(
          Row(1, 0, 1), Row(3, 1, 1), Row(5, 2, 1),
          Row(7, 3, 1), Row(9, 4, 1)))

        assertDPPRuntimeFilters(df)
        assertPushedPartitionPredicates(df, 1)
        assertReferencedPartitionFieldOrdinals(df, Array(1), Array("p1", "p2"))
      }
    }
  }

  test("case 3: scalar subquery translatable, rejected in 1st pass -> PartitionPredicate") {
    val tbl = s"$catalogName.tbl"
    val dim = s"$catalogName.dim"
    withTable(tbl, dim) {
      sql(s"CREATE TABLE $tbl (id INT, part INT) USING $v2Source PARTITIONED BY (part)")
      for (i <- 0 until 5) {
        sql(s"INSERT INTO $tbl VALUES ($i, $i)")
      }
      sql(s"CREATE TABLE $dim (val INT) USING $v2Source")
      sql(s"INSERT INTO $dim VALUES (3)")

      val df = sql(s"SELECT * FROM $tbl WHERE part = (SELECT max(val) FROM $dim)")
      checkAnswer(df, Row(3, 3))

      assertScalarSubqueryRuntimeFilters(df)
      assertPushedPartitionPredicates(df, 1)
      assertScanReturnsPartitionKeys(df, Set("3"))
      assertReferencedPartitionFieldOrdinals(df, Array(0), Array("part"))
    }
  }

  test("case 4a: scalar subquery untranslatable (complex expr) -> PartitionPredicate") {
    val tbl = s"$catalogName.tbl_complex"
    val dim = s"$catalogName.dim_complex"
    withTable(tbl, dim) {
      sql(s"CREATE TABLE $tbl (id INT, part INT) USING $v2Source PARTITIONED BY (part)")
      for (i <- 0 until 5) {
        sql(s"INSERT INTO $tbl VALUES ($i, $i)")
      }
      sql(s"CREATE TABLE $dim (val INT) USING $v2Source")
      sql(s"INSERT INTO $dim VALUES (2)")

      val df = sql(s"SELECT * FROM $tbl WHERE part > (SELECT max(val) FROM $dim) + 1")
      checkAnswer(df, Row(4, 4))

      assertScalarSubqueryRuntimeFilters(df)
      assertPushedPartitionPredicates(df, 1)
      assertScanReturnsPartitionKeys(df, Set("4"))
      assertReferencedPartitionFieldOrdinals(df, Array(0), Array("part"))
    }
  }

  test("case 4b: scalar subquery untranslatable (RLIKE) -> PartitionPredicate") {
    val tbl = s"$catalogName.tbl_rlike"
    val dim = s"$catalogName.dim_rlike"
    withTable(tbl, dim) {
      sql(s"CREATE TABLE $tbl (id INT, part STRING) USING $v2Source PARTITIONED BY (part)")
      sql(s"INSERT INTO $tbl VALUES (1, 'abc')")
      sql(s"INSERT INTO $tbl VALUES (2, 'def')")
      sql(s"INSERT INTO $tbl VALUES (3, 'abx')")
      sql(s"INSERT INTO $tbl VALUES (4, 'xyz')")

      sql(s"CREATE TABLE $dim (pattern STRING) USING $v2Source")
      sql(s"INSERT INTO $dim VALUES ('^ab')")

      val df = sql(s"SELECT * FROM $tbl WHERE part RLIKE (SELECT max(pattern) FROM $dim)")
      checkAnswer(df, Seq(Row(1, "abc"), Row(3, "abx")))

      assertScalarSubqueryRuntimeFilters(df)
      assertPushedPartitionPredicates(df, 1)
      assertScanReturnsPartitionKeys(df, Set("abc", "abx"))
      assertReferencedPartitionFieldOrdinals(df, Array(0), Array("part"))
    }
  }

  test("case 4c: scalar subquery untranslatable (UDF) -> PartitionPredicate") {
    val tbl = s"$catalogName.tbl_udf"
    val dim = s"$catalogName.dim_udf"
    withTable(tbl, dim) {
      sql(s"CREATE TABLE $tbl (id INT, part STRING) USING $v2Source PARTITIONED BY (part)")
      sql(s"INSERT INTO $tbl VALUES (1, 'a')")
      sql(s"INSERT INTO $tbl VALUES (2, 'A')")
      sql(s"INSERT INTO $tbl VALUES (3, 'b')")
      sql(s"INSERT INTO $tbl VALUES (4, 'B')")

      sql(s"CREATE TABLE $dim (val STRING) USING $v2Source")
      sql(s"INSERT INTO $dim VALUES ('A')")

      spark.udf.register("my_upper_runtime",
        (s: String) => if (s == null) null
          else s.toUpperCase(java.util.Locale.ROOT))

      val df = sql(
        s"SELECT * FROM $tbl WHERE my_upper_runtime(part) = (SELECT max(val) FROM $dim)")
      checkAnswer(df, Seq(Row(1, "a"), Row(2, "A")))

      assertScalarSubqueryRuntimeFilters(df)
      assertPushedPartitionPredicates(df, 1)
      assertScanReturnsPartitionKeys(df, Set("a", "A"))
      assertReferencedPartitionFieldOrdinals(df, Array(0), Array("part"))
    }
  }

  test("case 5: scalar subquery two partition cols -> 2 PartitionPredicates") {
    val fact = s"$catalogName.fact_2sub"
    val dim1 = s"$catalogName.dim_2sub1"
    val dim2 = s"$catalogName.dim_2sub2"
    withTable(fact, dim1, dim2) {
      sql(s"CREATE TABLE $fact (id INT, p1 INT, p2 STRING) " +
        s"USING $v2Source PARTITIONED BY (p1, p2)")
      sql(s"INSERT INTO $fact VALUES (1, 1, 'a')")
      sql(s"INSERT INTO $fact VALUES (2, 1, 'b')")
      sql(s"INSERT INTO $fact VALUES (3, 2, 'a')")
      sql(s"INSERT INTO $fact VALUES (4, 2, 'b')")

      sql(s"CREATE TABLE $dim1 (val INT) USING $v2Source")
      sql(s"INSERT INTO $dim1 VALUES (1)")

      sql(s"CREATE TABLE $dim2 (val STRING) USING $v2Source")
      sql(s"INSERT INTO $dim2 VALUES ('a')")

      val df = sql(
        s"""SELECT * FROM $fact
           |WHERE p1 = (SELECT max(val) FROM $dim1)
           |  AND p2 = (SELECT max(val) FROM $dim2)""".stripMargin)
      checkAnswer(df, Row(1, 1, "a"))

      assertScalarSubqueryRuntimeFilters(df, expectedCount = 2)
      assertPushedPartitionPredicates(df, 2)
      assertScanReturnsPartitionKeys(df, Set("1/a"))

      val partFieldNames = Array("p1", "p2")
      assertPredicateForOrdinal(df, 0, partFieldNames)
      assertPredicateForOrdinal(df, 1, partFieldNames)
    }
  }

  test("case 6: scalar subquery non-first of 3 partition cols -> PartitionPredicate") {
    val tbl = s"$catalogName.tbl_3part"
    val dim = s"$catalogName.dim_3part"
    withTable(tbl, dim) {
      sql(s"CREATE TABLE $tbl (id INT, p0 INT, p1 STRING, p2 INT) " +
        s"USING $v2Source PARTITIONED BY (p0, p1, p2)")
      sql(s"INSERT INTO $tbl VALUES (1, 1, 'a', 10)")
      sql(s"INSERT INTO $tbl VALUES (2, 1, 'b', 10)")
      sql(s"INSERT INTO $tbl VALUES (3, 2, 'a', 20)")
      sql(s"INSERT INTO $tbl VALUES (4, 2, 'b', 20)")

      sql(s"CREATE TABLE $dim (val STRING) USING $v2Source")
      sql(s"INSERT INTO $dim VALUES ('a')")

      val df = sql(
        s"SELECT * FROM $tbl WHERE p1 = (SELECT max(val) FROM $dim)")
      checkAnswer(df, Seq(Row(1, 1, "a", 10), Row(3, 2, "a", 20)))

      assertScalarSubqueryRuntimeFilters(df)
      assertPushedPartitionPredicates(df, 1)
      assertScanReturnsPartitionKeys(df, Set("1/a/10", "2/a/20"))
      assertReferencedPartitionFieldOrdinals(
        df, Array(1), Array("p0", "p1", "p2"))
    }
  }

  test("case 7: mixed - accepted in 1st pass + untranslatable -> " +
    "only untranslatable gets PartitionPredicate") {
    val tbl = s"$catalogName.tbl_mixed"
    val dim1 = s"$catalogName.dim_mixed1"
    val dim2 = s"$catalogName.dim_mixed2"
    withTable(tbl, dim1, dim2) {
      sql(s"CREATE TABLE $tbl (id INT, p1 INT, p2 STRING) " +
        s"USING $v2Source PARTITIONED BY (p1, p2) " +
        "TBLPROPERTIES('accept-v2-predicates' = 'true')")
      sql(s"INSERT INTO $tbl VALUES (1, 1, 'a')")
      sql(s"INSERT INTO $tbl VALUES (2, 1, 'b')")
      sql(s"INSERT INTO $tbl VALUES (3, 2, 'a')")
      sql(s"INSERT INTO $tbl VALUES (4, 2, 'b')")

      sql(s"CREATE TABLE $dim1 (val INT) USING $v2Source")
      sql(s"INSERT INTO $dim1 VALUES (1)")
      sql(s"CREATE TABLE $dim2 (val STRING) USING $v2Source")
      sql(s"INSERT INTO $dim2 VALUES ('A')")

      spark.udf.register("my_upper_mixed",
        (s: String) => if (s == null) null
          else s.toUpperCase(java.util.Locale.ROOT))

      // p1 = (subquery) is translatable and accepted in 1st pass.
      // my_upper_mixed(p2) = (subquery) is untranslatable -> PartitionPredicate.
      // Only the untranslatable filter should produce a PartitionPredicate.
      val df = sql(
        s"""SELECT * FROM $tbl
           |WHERE p1 = (SELECT max(val) FROM $dim1)
           |  AND my_upper_mixed(p2) = (SELECT max(val) FROM $dim2)
           |""".stripMargin)
      checkAnswer(df, Row(1, 1, "a"))

      assertScalarSubqueryRuntimeFilters(df, expectedCount = 2)
      assertPushedPartitionPredicates(df, 1)
      // The V2 predicate for p1=1 was accepted but not evaluated by the
      // test table, so both p2='a' partitions remain after the
      // PartitionPredicate. Spark applies p1=1 as a post-scan filter.
      assertScanReturnsPartitionKeys(df, Set("1/a", "2/a"))
      assertReferencedPartitionFieldOrdinals(df, Array(1), Array("p1", "p2"))
    }
  }

  // ---------------------------------------------------------------------------
  // PartitionPredicate is NOT created
  // ---------------------------------------------------------------------------

  test("case 8: DPP translated, accepted in 1st pass -> no PartitionPredicate") {
    val fact = s"$catalogName.fact_acc"
    val dim = s"$catalogName.dim_acc"
    withTable(fact, dim) {
      sql(s"CREATE TABLE $fact (id INT, part INT) USING $v2Source " +
        "PARTITIONED BY (part) " +
        "TBLPROPERTIES('accept-v2-predicates' = 'true')")
      for (i <- 0 until 5) {
        sql(s"INSERT INTO $fact VALUES ($i, $i)")
      }
      sql(s"CREATE TABLE $dim (dim_id INT, dim_val STRING) USING $v2Source")
      sql(s"INSERT INTO $dim VALUES (2, 'two')")

      withDPPConf {
        val df = sql(
          s"""SELECT f.id, f.part FROM $fact f JOIN $dim d
             |ON f.part = d.dim_id WHERE d.dim_val = 'two'""".stripMargin)
        checkAnswer(df, Row(2, 2))

        assertDPPRuntimeFilters(df)
        assertPushedPartitionPredicates(df, 0)
      }
    }
  }

  test("case 9: scalar subquery translatable, accepted in 1st pass -> " +
    "no PartitionPredicate") {
    val tbl = s"$catalogName.tbl_acc"
    val dim = s"$catalogName.dim_acc2"
    withTable(tbl, dim) {
      sql(s"CREATE TABLE $tbl (id INT, part INT) USING $v2Source " +
        "PARTITIONED BY (part) " +
        "TBLPROPERTIES('accept-v2-predicates' = 'true')")
      for (i <- 0 until 5) {
        sql(s"INSERT INTO $tbl VALUES ($i, $i)")
      }
      sql(s"CREATE TABLE $dim (val INT) USING $v2Source")
      sql(s"INSERT INTO $dim VALUES (3)")

      val df = sql(
        s"SELECT * FROM $tbl WHERE part = (SELECT max(val) FROM $dim)")
      checkAnswer(df, Row(3, 3))

      assertScalarSubqueryRuntimeFilters(df)
      assertPushedPartitionPredicates(df, 0)
    }
  }

  test("case 10: scalar subquery on data column -> no PartitionPredicate") {
    val tbl = s"$catalogName.tbl_data"
    val dim = s"$catalogName.dim_data"
    withTable(tbl, dim) {
      sql(s"CREATE TABLE $tbl (id INT, data INT, part INT) " +
        s"USING $v2Source PARTITIONED BY (part)")
      for (i <- 0 until 5) {
        sql(s"INSERT INTO $tbl VALUES ($i, ${i * 10}, $i)")
      }
      sql(s"CREATE TABLE $dim (val INT) USING $v2Source")
      sql(s"INSERT INTO $dim VALUES (30)")

      val df = sql(
        s"SELECT * FROM $tbl WHERE data = (SELECT max(val) FROM $dim)")
      checkAnswer(df, Row(3, 30, 3))

      assertPushedPartitionPredicates(df, 0)
    }
  }

  test("case 11: supportsIterativePushdown is false -> no PartitionPredicate") {
    val noIterCatalog = "testNoIterativeFiltering"
    withSQLConf(s"spark.sql.catalog.$noIterCatalog" ->
      classOf[InMemoryTableEnhancedRuntimePartitionFilterCatalog].getName) {
      val tbl = s"$noIterCatalog.tbl"
      val dim = s"$noIterCatalog.dim"
      withTable(tbl, dim) {
        sql(s"CREATE TABLE $tbl (id INT, part INT) " +
          s"USING $v2Source PARTITIONED BY (part) " +
          s"TBLPROPERTIES('supports-iterative-pushdown' = 'false')")
        for (i <- 0 until 5) {
          sql(s"INSERT INTO $tbl VALUES ($i, $i)")
        }
        sql(s"CREATE TABLE $dim (val INT) USING $v2Source")
        sql(s"INSERT INTO $dim VALUES (3)")

        val df = sql(
          s"SELECT * FROM $tbl WHERE part = (SELECT max(val) FROM $dim)")
        checkAnswer(df, Row(3, 3))

        assertHasRuntimeFilters(df)
        assertPushedPartitionPredicates(df, 0)
      }
    }
  }

  test("case 12: partition col not in filterAttributes -> no PartitionPredicate") {
    val tbl = s"$catalogName.tbl_noattr"
    val dim = s"$catalogName.dim_noattr"
    withTable(tbl, dim) {
      sql(s"CREATE TABLE $tbl (id INT, p1 INT, p2 INT) " +
        s"USING $v2Source PARTITIONED BY (p1, p2) " +
        "TBLPROPERTIES('filter-attributes' = 'p1')")
      sql(s"INSERT INTO $tbl VALUES (1, 1, 10)")
      sql(s"INSERT INTO $tbl VALUES (2, 1, 20)")
      sql(s"INSERT INTO $tbl VALUES (3, 2, 10)")
      sql(s"INSERT INTO $tbl VALUES (4, 2, 20)")

      sql(s"CREATE TABLE $dim (val INT) USING $v2Source")
      sql(s"INSERT INTO $dim VALUES (10)")

      // Scalar subquery on p2, which is NOT in filterAttributes.
      // p2 is a partition column, but since it's not declared as a
      // filterable attribute the PartitionPredicate should not be pushed.
      val df = sql(
        s"SELECT * FROM $tbl WHERE p2 = (SELECT max(val) FROM $dim)")
      checkAnswer(df, Seq(Row(1, 1, 10), Row(3, 2, 10)))

      assertPushedPartitionPredicates(df, 0)
    }
  }

  // Test for a buggy connector that supports iterative filtering but
  // does not correctly report first-pass filters in pushedPredicates().
  // The second round will push duplicate PartitionPredicate.
  test("pushedPredicates() omits first-pass filters -> second round still prunes") {
    val tbl = s"$catalogName.tbl_nopushed"
    val dim = s"$catalogName.dim_nopushed"
    withTable(tbl, dim) {
      sql(s"CREATE TABLE $tbl (id INT, part INT) USING $v2Source PARTITIONED BY (part)")
      for (i <- 0 until 5) {
        sql(s"INSERT INTO $tbl VALUES ($i, $i)")
      }
      sql(s"CREATE TABLE $dim (dim_id INT, dim_val STRING) USING $v2Source")
      sql(s"INSERT INTO $dim VALUES (2, 'two')")

      withDPPConf {
        val df = sql(
          s"""SELECT f.id, f.part FROM $tbl f JOIN $dim d
             |ON f.part = d.dim_id WHERE d.dim_val = 'two'""".stripMargin)
        checkAnswer(df, Row(2, 2))

        assertDPPRuntimeFilters(df)
        assertPushedPartitionPredicates(df, 1)
        assertScanReturnsPartitionKeys(df, Set("2"))

        val batchScan = collectBatchScan(df)
        assert(batchScan.filteredPartitions.flatten.length < 5,
          "Expected PartitionPredicate from second round to actually prune partitions")
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Helper methods
  // ---------------------------------------------------------------------------

  private def assertDPPRuntimeFilters(
      df: DataFrame, expectedCount: Int = 1): Unit = {
    val batchScan = collectBatchScan(df)
    val dppFilters = batchScan.runtimeFilters.collect {
      case d: DynamicPruningExpression => d
    }
    assert(dppFilters.size === expectedCount,
      s"Expected $expectedCount DynamicPruningExpression(s) " +
        s"in runtimeFilters, got ${dppFilters.size}")
  }

  private def assertHasRuntimeFilters(df: DataFrame): Unit = {
    assert(collectBatchScan(df).runtimeFilters.nonEmpty,
      "Expected non-empty runtimeFilters on BatchScanExec")
  }

  private def assertScalarSubqueryRuntimeFilters(
      df: DataFrame, expectedCount: Int = 1): Unit = {
    val batchScan = collectBatchScan(df)
    val scalarFilters = batchScan.runtimeFilters.collect {
      case f if !f.isInstanceOf[DynamicPruning] => f
    }
    val dppFilters = batchScan.runtimeFilters.collect {
      case d: DynamicPruning => d
    }
    assert(scalarFilters.size === expectedCount,
      s"Expected $expectedCount scalar subquery runtime filter(s), " +
        s"got ${scalarFilters.size}")
    assert(dppFilters.isEmpty,
      "Expected non-DPP runtime filters (scalar subquery)")
  }

  private def collectBatchScan(df: DataFrame): BatchScanExec = {
    stripAQEPlan(df.queryExecution.executedPlan).collectFirst {
      case b: BatchScanExec => b
    }.getOrElse(fail("Expected BatchScanExec in plan"))
  }

  private[connector] def getPushedPartitionPredicates(
      df: DataFrame): Seq[PartitionPredicate] = {
    val batchScan = collectBatchScan(df)
    batchScan.scan match {
      case s: InMemoryEnhancedRuntimePartitionFilterTable#
        InMemoryEnhancedRuntimePartitionFilterBatchScan =>
        s.pushedPartitionPredicates
      case _ => Seq.empty
    }
  }

  private def assertPushedPartitionPredicates(
      df: DataFrame,
      expectedCount: Int): Unit = {
    val predicates = getPushedPartitionPredicates(df)
    assert(predicates.size === expectedCount,
      s"Expected $expectedCount pushed partition predicate(s), " +
        s"got ${predicates.size}: $predicates")
  }

  private def assertPartitionPredicateOrdinals(
      predicate: PartitionPredicate,
      expectedOrdinals: Array[Int],
      expectedPartitionFieldNames: Array[String]): Unit = {
    val refs = predicate.references()
    val ordinals =
      refs.map(_.asInstanceOf[PartitionFieldReference].ordinal()).sorted
    assert(ordinals.sameElements(expectedOrdinals.sorted),
      s"Expected references().map(_.ordinal()) " +
        s"${expectedOrdinals.sorted.mkString("[", ", ", "]")}, " +
        s"got ${ordinals.mkString("[", ", ", "]")}")

    val names = expectedPartitionFieldNames
    refs.foreach { ref =>
      assert(ref.isInstanceOf[PartitionFieldReference],
        s"Expected PartitionFieldReference, " +
          s"got ${ref.getClass.getName}")
      val partRef = ref.asInstanceOf[PartitionFieldReference]
      assert(partRef.fieldNames().nonEmpty,
        s"ordinal=${partRef.ordinal()} has empty fieldNames")
      assert(partRef.ordinal() < names.length,
        s"ordinal=${partRef.ordinal()} out of range " +
          s"for names length ${names.length}")
      val expectedName = names(partRef.ordinal())
      val actualName = partRef.fieldNames().mkString(".")
      assert(actualName === expectedName,
        s"ordinal=${partRef.ordinal()}: expected " +
          s"fieldNames '$expectedName', got '$actualName'")
    }
  }

  private def assertPredicateForOrdinal(
      df: DataFrame,
      ordinal: Int,
      expectedPartitionFieldNames: Array[String]): Unit = {
    val predicates = getPushedPartitionPredicates(df)
    val pred = predicates.find(_.references().exists(
      _.asInstanceOf[PartitionFieldReference].ordinal() == ordinal))
    assert(pred.isDefined,
      s"Expected a PartitionPredicate referencing ordinal $ordinal")
    assertPartitionPredicateOrdinals(
      pred.get, Array(ordinal), expectedPartitionFieldNames)
  }

  private def assertReferencedPartitionFieldOrdinals(
      df: DataFrame,
      expectedOrdinals: Array[Int],
      expectedPartitionFieldNames: Array[String]): Unit = {
    getPushedPartitionPredicates(df).foreach { p =>
      assertPartitionPredicateOrdinals(
        p, expectedOrdinals, expectedPartitionFieldNames)
    }
  }

  private def assertScanReturnsPartitionKeys(
      df: DataFrame,
      expectedPartitionKeys: Set[String]): Unit = {
    val batchScan = collectBatchScan(df)
    val partitions = batchScan.batch.planInputPartitions()
    assert(partitions.length === expectedPartitionKeys.size,
      s"Expected ${expectedPartitionKeys.size} partition(s), " +
        s"got ${partitions.length}")
    val partKeys =
      partitions.map(_.asInstanceOf[BufferedRows].keyString()).toSet
    assert(partKeys === expectedPartitionKeys,
      s"Partition keys should be $expectedPartitionKeys, " +
        s"got $partKeys")
  }
}
