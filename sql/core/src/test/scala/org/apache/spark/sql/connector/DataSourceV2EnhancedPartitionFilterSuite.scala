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

import java.util.Locale

import scala.jdk.CollectionConverters._

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Expression, In, InSubquery, Like, ListQuery, Literal, ScalaUDF}
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.connector.catalog.{BufferedRows, Identifier, TableCatalog}
import org.apache.spark.sql.connector.catalog.InMemoryEnhancedPartitionFilterTable
import org.apache.spark.sql.connector.catalog.InMemoryTableEnhancedPartitionFilterCatalog
import org.apache.spark.sql.connector.catalog.TestPartitionPredicateScan
import org.apache.spark.sql.connector.expressions.filter.{PartitionPredicate, Predicate}
import org.apache.spark.sql.connector.read.{Scan, SupportsPushDownV2Filters}
import org.apache.spark.sql.execution.FilterExec
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, PushDownUtils}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String

/**
 * Tests for enhanced partition filter pushdown with tables whose scan builder handles
 * PartitionPredicates in a second pass of partition filter pushdown, for those
 * Catalyst Expression filters that are not translatable to DSV2, or are returned by DSV2
 * in the first pushdown.
 *
 * Pushdown cases (Translated/Untranslatable, Partition Filter/Data Filter, 1st/2nd Pass):
 * 1. Translated, Data Filter, 1st Pass Returned -> Post-Scan Filters
 * 2. Translated, Data Filter, 1st Pass Accepted -> Pushed Down
 * 3. Translated, Partition Filter, 1st Pass Returned, 2nd Pass Returned -> Post-Scan Filters
 * 4. Translated, Partition Filter, 1st Pass Returned, 2nd Pass Accepted -> Pushed Down
 * 5. Translated, Partition Filter, 1st Pass Accepted -> Pushed Down
 * 6. Untranslatable, Data Filter -> Post-Scan Filters
 * 7. Untranslatable, Partition Filter, 2nd Pass Returned -> Post-Scan Filters
 * 8. Untranslatable, Partition Filter, 2nd Pass Accepted -> Pushed Down
 */
class DataSourceV2EnhancedPartitionFilterSuite
  extends QueryTest with SharedSparkSession with BeforeAndAfter {

  protected val v2Source = classOf[FakeV2ProviderWithCustomSchema].getName
  protected val partFilterTableName = "testpartfilter.t"

  protected def registerCatalog(name: String, clazz: Class[_]): Unit = {
    spark.conf.set(s"spark.sql.catalog.$name", clazz.getName)
  }

  before {
    registerCatalog("testpartfilter", classOf[InMemoryTableEnhancedPartitionFilterCatalog])
  }

  after {
    spark.sessionState.catalogManager.reset()
  }

  /**
   * Collects pushed partition predicates from the plan when the scan is our
   * test in-memory scan.
   */
  private def getPushedPartitionPredicates(
      df: org.apache.spark.sql.DataFrame): Seq[PartitionPredicate] = {
    val batchScan = df.queryExecution.executedPlan.collectFirst {
      case b: BatchScanExec => b
    }.getOrElse(fail("Expected BatchScanExec in plan"))
    batchScan.batch match {
      case s: TestPartitionPredicateScan => s.getPushedPartitionPredicates
      case _ => Seq.empty
    }
  }

  /**
   * Asserts that the number of pushed partition predicates (second pass) in the plan
   * matches the expected count. Use for tests that run a query against the in-memory
   * enhanced partition filter table.
   */
  private def assertPushedPartitionPredicates(
      df: org.apache.spark.sql.DataFrame,
      expectedCount: Int): Unit = {
    val predicates = getPushedPartitionPredicates(df)
    assert(predicates.size === expectedCount,
      s"Expected $expectedCount pushed partition predicate(s), got ${predicates.size}: $predicates")
  }

  test("translated data filter returned in first pass is in post-scan (case 1)") {
    // Translated, Data Filter; 1st Pass Returned -> Post-Scan Filters.
    withTable(partFilterTableName) {
      sql(s"CREATE TABLE $partFilterTableName (part_col string, data string) USING $v2Source " +
        "PARTITIONED BY (part_col)")
      val catalog = spark.sessionState.catalogManager.catalog("testpartfilter")
        .asInstanceOf[TableCatalog]
      val table = catalog.loadTable(Identifier.of(Array(), "t"))
        .asInstanceOf[InMemoryEnhancedPartitionFilterTable]
      val options = new CaseInsensitiveStringMap(Map.empty[String, String].asJava)
      val builder = table.newScanBuilder(options)
      val partitionSchema = StructType(StructField("part_col", StringType) :: Nil)
      val dataAttr = AttributeReference("data", StringType)()
      val litKeep = Literal(UTF8String.fromString("keep"), StringType)
      val filterExpr: Expression = EqualTo(dataAttr, litKeep)
      val (_, postScanFilters) =
        PushDownUtils.pushFilters(builder, Seq(filterExpr), Some(partitionSchema))
      assert(postScanFilters.size === 1,
        s"Case 1: data filter in post-scan, got ${postScanFilters.size}: $postScanFilters")
      assert(postScanFilters.head.semanticEquals(filterExpr),
        s"Post-scan filter should equal original: ${postScanFilters.head}")
    }
  }

  test("translated data filter accepted in first pass is pushed down (case 2)") {
    // Translated, Data Filter; 1st Pass Accepted -> Pushed Down (not in post-scan).
    // Use a scan builder that accepts all predicates to simulate a source that pushes data filters.
    val builder: SupportsPushDownV2Filters = new SupportsPushDownV2Filters {
      override def pushPredicates(predicates: Array[Predicate]): Array[Predicate] = Array.empty
      override def pushedPredicates(): Array[Predicate] = Array.empty
      override def supportsEnhancedPartitionFiltering(): Boolean = false
      override def build(): Scan = throw new UnsupportedOperationException("not used in this test")
    }
    val partitionSchema = StructType(StructField("part_col", StringType) :: Nil)
    val dataAttr = AttributeReference("data", StringType)()
    val litKeep = Literal(UTF8String.fromString("keep"), StringType)
    val filterExpr: Expression = EqualTo(dataAttr, litKeep)
    val (_, postScanFilters) =
      PushDownUtils.pushFilters(builder, Seq(filterExpr), Some(partitionSchema))
    assert(postScanFilters.isEmpty,
      s"Case 2: data filter accepted in 1st pass should not be in post-scan, got: $postScanFilters")
  }

  test("filter returned in both first and second pass (case 3)") {
    // Use a table with reject-partition-predicates so the scan returns all PartitionPredicates.
    // A Partition Filter is 1st Pass Returned, then re-sent as PartitionPredicate and
    // 2nd Pass Returned again when property is set; dedup ensures one post-scan filter.
    withTable(partFilterTableName) {
      sql(s"CREATE TABLE $partFilterTableName (part_col string, data string) USING $v2Source " +
        "PARTITIONED BY (part_col) " +
        "TBLPROPERTIES('reject-partition-predicates' = 'true')")

      val catalog = spark.sessionState.catalogManager.catalog("testpartfilter")
        .asInstanceOf[TableCatalog]
      val table = catalog.loadTable(Identifier.of(Array(), "t"))
        .asInstanceOf[InMemoryEnhancedPartitionFilterTable]
      val options = new CaseInsensitiveStringMap(Map.empty[String, String].asJava)
      val builder = table.newScanBuilder(options)

      val partitionSchema = StructType(StructField("part_col", StringType) :: Nil)
      val partColAttr = AttributeReference("part_col", StringType)()
      val litA = Literal(UTF8String.fromString("a"), StringType)
      // IN: Translated, Partition Filter; 1st Pass Returned, then 2nd Pass Returned (property).
      val filterExpr: Expression = In(partColAttr, Seq(litA))

      val (_, postScanFilters) =
        PushDownUtils.pushFilters(builder, Seq(filterExpr), Some(partitionSchema))

      // Dedup by semantic equality yields a single post-scan filter.
      assert(postScanFilters.size === 1,
        s"Post-scan filters should be deduplicated, got ${postScanFilters.size}: $postScanFilters")
      assert(postScanFilters.head.semanticEquals(filterExpr),
        s"Single post-scan filter should equal original: ${postScanFilters.head}")
    }
  }

  test("first pass partition predicate returned by source (e.g. IN) " +
    "applied in second pass (case 4)") {
    withTable(partFilterTableName) {
      sql(s"CREATE TABLE $partFilterTableName (part_col string, data string) USING $v2Source " +
        "PARTITIONED BY (part_col)")
      sql(s"INSERT INTO $partFilterTableName VALUES ('a', 'x'), ('b', 'y'), ('c', 'z')")

      // Translated, Partition Filter; 1st Pass Returned (supportsPredicates returns IN), then
      // pushed as PartitionPredicate and 2nd Pass Accepted -> used to prune.
      val df = sql(s"SELECT * FROM $partFilterTableName WHERE part_col IN ('a', 'b')")
      checkAnswer(df, Seq(Row("a", "x"), Row("b", "y")))
      assertPushedPartitionPredicates(df, 1)

      val batchScan = df.queryExecution.executedPlan.collectFirst {
        case b: BatchScanExec => b
      }.get
      val partitions = batchScan.batch.planInputPartitions()

      assert(partitions.length === 2,
        "Rejected first-pass predicate (IN) should be applied in second pass via " +
          "PartitionPredicate and prune to matching partitions")
      val partKeys = partitions.map(_.asInstanceOf[BufferedRows].keyString()).toSet
      assert(partKeys === Set("a", "b"))
    }
  }

  test("first pass partition filter still works (e.g. part_col = value) (case 5)") {
    withTable(partFilterTableName) {
      sql(s"CREATE TABLE $partFilterTableName (part_col string, data string) USING $v2Source " +
        "PARTITIONED BY (part_col)")
      sql(s"INSERT INTO $partFilterTableName VALUES ('a', 'x'), ('b', 'y'), ('c', 'z')")

      // Translated, Partition Filter; 1st Pass Accepted -> pushed, used to prune partitions.
      val df = sql(s"SELECT * FROM $partFilterTableName WHERE part_col = 'b'")
      checkAnswer(df, Seq(Row("b", "y")))
      assertPushedPartitionPredicates(df, 0)

      val batchScan = df.queryExecution.executedPlan.collectFirst {
        case b: BatchScanExec => b
      }.get
      val partitions = batchScan.batch.planInputPartitions()

      assert(partitions.length === 1,
        "First-pass pushed predicate (part_col = 'b') should prune to one partition")
      assert(partitions.head.asInstanceOf[BufferedRows].keyString() === "b")
    }
  }

  test("untranslatable data filters are applied after scan (case 6)") {
    withTable(partFilterTableName) {
      sql(s"CREATE TABLE $partFilterTableName (part_col string, data string) USING $v2Source " +
        "PARTITIONED BY (part_col)")
      sql(s"INSERT INTO $partFilterTableName VALUES ('a', 'keep'), ('a', 'drop'), ('b', 'other')")

      // Untranslatable, Data Filter -> Post-Scan Filters so Spark applies it after scan.
      spark.udf.register("is_keep", (s: String) => s != null && s == "keep")

      val df = sql(s"SELECT * FROM $partFilterTableName WHERE part_col = 'a' AND is_keep(data)")
      checkAnswer(df, Seq(Row("a", "keep")))
      assertPushedPartitionPredicates(df, 0)

      val batchScan = df.queryExecution.executedPlan.collectFirst {
        case b: BatchScanExec => b
      }.get
      val partitions = batchScan.batch.planInputPartitions()
      assert(partitions.length === 1,
        "Partition predicate part_col = 'a' should prune to one partition")
      assert(partitions.head.asInstanceOf[BufferedRows].keyString() === "a")
    }
  }

  test("case 7: untranslatable partition filter returned in second pass is in post-scan (case 7)") {
    // Untranslatable, Partition Filter; 2nd Pass Returned -> Post-Scan Filters.
    withTable(partFilterTableName) {
      sql(s"CREATE TABLE $partFilterTableName (part_col string, data string) USING $v2Source " +
        "PARTITIONED BY (part_col) " +
        "TBLPROPERTIES('reject-partition-predicates' = 'true')")
      val catalog = spark.sessionState.catalogManager.catalog("testpartfilter")
        .asInstanceOf[TableCatalog]
      val table = catalog.loadTable(Identifier.of(Array(), "t"))
        .asInstanceOf[InMemoryEnhancedPartitionFilterTable]
      val options = new CaseInsensitiveStringMap(Map.empty[String, String].asJava)
      val builder = table.newScanBuilder(options)
      val partitionSchema = StructType(StructField("part_col", StringType) :: Nil)
      val partColAttr = AttributeReference("part_col", StringType)()
      val pattern = Literal(UTF8String.fromString("b%"), StringType)
      val filterExpr: Expression = Like(partColAttr, pattern, '\\')
      val (_, postScanFilters) =
        PushDownUtils.pushFilters(builder, Seq(filterExpr), Some(partitionSchema))
      assert(postScanFilters.size === 1,
        s"Case 7: in post-scan, got ${postScanFilters.size}: $postScanFilters")
      assert(postScanFilters.head.semanticEquals(filterExpr),
        s"Post-scan filter should equal original: ${postScanFilters.head}")
    }
  }

  test("untranslatable partition-only expression handled by second pass (e.g. LIKE) (case 8)") {
    withTable(partFilterTableName) {
      sql(s"CREATE TABLE $partFilterTableName (part_col string, data string) USING $v2Source " +
        "PARTITIONED BY (part_col)")
      sql(s"INSERT INTO $partFilterTableName VALUES ('a', 'x'), ('b', 'y'), ('bc', 'z')")

      // Untranslatable, Partition Filter; pushed as PartitionPredicate in 2nd Pass (Accepted).
      val df = sql(s"SELECT * FROM $partFilterTableName WHERE part_col LIKE 'b%'")
      checkAnswer(df, Seq(Row("b", "y"), Row("bc", "z")))
      assertPushedPartitionPredicates(df, 1)

      val batchScan = df.queryExecution.executedPlan.collectFirst {
        case b: BatchScanExec => b
      }.get
      val partitions = batchScan.batch.planInputPartitions()

      assert(partitions.length === 2,
        "Second-pass PartitionPredicate (part_col LIKE 'b%') should prune to matching partitions")
      val partKeys = partitions.map(_.asInstanceOf[BufferedRows].keyString()).toSet
      assert(partKeys === Set("b", "bc"))
    }
  }

  test("Second-pass PartitionPredicate filter works for UDF filter on partition column (case 8)") {
    withTable(partFilterTableName) {
      sql(s"CREATE TABLE $partFilterTableName (part_col string, data string) USING $v2Source " +
        "PARTITIONED BY (part_col)")
      sql(s"INSERT INTO $partFilterTableName VALUES ('a', 'x'), ('A', 'y'), ('b', 'z')")

      spark.udf.register("my_upper", (s: String) =>
        if (s == null) null else s.toUpperCase(Locale.ROOT))

      val df = sql(s"SELECT * FROM $partFilterTableName WHERE my_upper(part_col) = 'A'")
      checkAnswer(df, Seq(Row("a", "x"), Row("A", "y")))
      assertPushedPartitionPredicates(df, 1)

      val batchScan = df.queryExecution.executedPlan.collectFirst {
        case b: BatchScanExec => b
      }.get
      val scan = batchScan.batch
      val partitions = scan.planInputPartitions()

      assert(partitions.length === 2,
        "Only partitions satisfying all pushed PartitionPredicates should be read")
      val expectedPartitionKeys = Set("a", "A")
      partitions.foreach { p =>
        val keyStr = p.asInstanceOf[BufferedRows].keyString()
        assert(expectedPartitionKeys.contains(keyStr),
          s"Partition $keyStr (InternalRow) must be among partitions accepted by " +
            "all pushed PartitionPredicates")
      }
      val partKeys = partitions.map(_.asInstanceOf[BufferedRows].keyString()).toSet
      assert(partKeys === expectedPartitionKeys)
    }
  }

  test("referencedPartitionColumnOrdinals: one non-first partition column " +
    "in second-pass (case 8)") {
    withTable(partFilterTableName) {
      // Partitioning (p0, p1, p2); filter only on p1 (ordinal 1) via LIKE in second pass.
      sql(s"CREATE TABLE $partFilterTableName (p0 string, p1 string, p2 string, data string) " +
        s"USING $v2Source PARTITIONED BY (p0, p1, p2)")
      sql(s"INSERT INTO $partFilterTableName VALUES " +
        "('a', 'x', '1', 'd1'), ('a', 'y', '1', 'd2'), " +
        "('a', 'x', '2', 'd3'), ('b', 'x', '1', 'd4')")

      val df = sql(s"SELECT * FROM $partFilterTableName WHERE p1 LIKE 'x%'")
      checkAnswer(df, Seq(
        Row("a", "x", "1", "d1"), Row("a", "x", "2", "d3"), Row("b", "x", "1", "d4")))
      assertPushedPartitionPredicates(df, 1)

      val predicates = getPushedPartitionPredicates(df)
      predicates.foreach { p =>
        val ordinals = p.referencedPartitionColumnOrdinals()
        assert(ordinals.sameElements(Array(1)),
          s"Predicate on p1 only (ordinal 1) should have ordinals [1], " +
            s"got ${ordinals.mkString("[", ", ", "]")}")
      }
    }
  }

  test("referencedPartitionColumnOrdinals: two non-first partition columns " +
    "in second-pass (case 8)") {
    withTable(partFilterTableName) {
      // Partitioning (p0, p1, p2); filter on p1 and p2 (ordinals 1, 2) via UDF.
      sql(s"CREATE TABLE $partFilterTableName (p0 string, p1 string, p2 string, data string) " +
        s"USING $v2Source PARTITIONED BY (p0, p1, p2)")
      sql(s"INSERT INTO $partFilterTableName VALUES " +
        "('a', 'x', '1', 'd1'), ('a', 'y', '1', 'd2'), " +
        "('a', 'x', '2', 'd3'), ('b', 'x', '1', 'd4')")

      spark.udf.register("concat2", (a: String, b: String) =>
        if (a == null || b == null) null else a + b)

      val df = sql(s"SELECT * FROM $partFilterTableName WHERE concat2(p1, p2) = 'x1'")
      checkAnswer(df, Seq(Row("a", "x", "1", "d1"), Row("b", "x", "1", "d4")))
    }
  }

  test("all eight pushdown cases: untranslatable filters before translatable in post-scan Filter") {
    // Cases 1, 3, 6, 7 end in post-scan (reject-partition-predicates).
    // PushDownUtils orders post-scan as untranslatable first.
    withTable(partFilterTableName) {
      sql(s"CREATE TABLE $partFilterTableName (part_col string, data string) USING $v2Source " +
        "PARTITIONED BY (part_col) TBLPROPERTIES('reject-partition-predicates' = 'true')")
      sql(s"INSERT INTO $partFilterTableName VALUES " +
        "('a', 'keep'), ('a', 'drop'), ('A', 'keep'), ('b', 'keep'), ('b', 'other')")

      spark.udf.register("is_keep", (s: String) => s != null && s == "keep")
      spark.udf.register("my_upper", (s: String) =>
        if (s == null) null else s.toUpperCase(Locale.ROOT))

      val df = sql(s"SELECT * FROM $partFilterTableName WHERE " +
        "part_col IN ('a', 'A') AND data = 'keep' AND is_keep(data) AND my_upper(part_col) = 'A'")
      checkAnswer(df, Seq(Row("a", "keep"), Row("A", "keep")))
      assertUntranslatableBeforeTranslatableInPostScan(df)

      // Reversed filter order in the query; post-scan order still untranslatable first.
      val dfReversed = sql(s"SELECT * FROM $partFilterTableName WHERE " +
        "my_upper(part_col) = 'A' AND is_keep(data) AND data = 'keep' AND part_col IN ('a', 'A')")
      checkAnswer(dfReversed, Seq(Row("a", "keep"), Row("A", "keep")))
      assertUntranslatableBeforeTranslatableInPostScan(dfReversed)
    }
  }

  def assertUntranslatableBeforeTranslatableInPostScan(df: DataFrame): Unit = {
    val postScanFilterExec = df.queryExecution.executedPlan.collect {
      case f @ FilterExec(_, _) if f.exists(_.isInstanceOf[BatchScanExec]) => f
    }.headOption.getOrElse(fail("Expected a post-scan FilterExec above BatchScanExec"))

    val predicates = splitConjunctivePredicates(postScanFilterExec.condition)
    val udfIndices = predicates.indices.filter(i =>
      predicates(i).collect { case _: ScalaUDF => true }.nonEmpty)
    val translatableIndices = predicates.indices.filter(i =>
      predicates(i).collect { case _: ScalaUDF => true }.isEmpty)

    assert(
      udfIndices.isEmpty || translatableIndices.isEmpty ||
        udfIndices.max < translatableIndices.min,
      s"Untranslatable (UDF) filters must appear before translatable filters in post-scan " +
        s"condition; predicates: ${predicates.mkString(", ")}; " +
        s"UDF indices: $udfIndices, translatable indices: $translatableIndices")
  }

  test("non-deterministic partition filter not pushed as PartitionPredicate (post-scan)") {
    // Same checks as FileSourceStrategy/PruneFileSourcePartitions: non-deterministic
    // partition filters must not be pushed as PartitionPredicate; they are applied after scan.
    withTable(partFilterTableName) {
      sql(s"CREATE TABLE $partFilterTableName (part_col string, data string) USING $v2Source " +
        "PARTITIONED BY (part_col)")
      sql(s"INSERT INTO $partFilterTableName VALUES ('a', 'x'), ('b', 'y'), ('c', 'z')")

      spark.udf.register("nondet_identity", udf((s: String) => s).asNondeterministic())

      val df = sql(s"SELECT * FROM $partFilterTableName WHERE nondet_identity(part_col) = 'a'")
      checkAnswer(df, Seq(Row("a", "x")))
      assertPushedPartitionPredicates(df, 0)
    }
  }

  test("partition filter with subquery is not pushed as PartitionPredicate (stays in post-scan)") {
    // Same checks as FileSourceStrategy/PruneFileSourcePartitions: partition filters
    // containing a subquery must not be pushed as PartitionPredicate.
    withTable(partFilterTableName) {
      sql(s"CREATE TABLE $partFilterTableName (part_col string, data string) USING $v2Source " +
        "PARTITIONED BY (part_col) TBLPROPERTIES('reject-partition-predicates' = 'true')")
      val catalog = spark.sessionState.catalogManager.catalog("testpartfilter")
        .asInstanceOf[TableCatalog]
      val table = catalog.loadTable(Identifier.of(Array(), "t"))
        .asInstanceOf[InMemoryEnhancedPartitionFilterTable]
      val options = new CaseInsensitiveStringMap(Map.empty[String, String].asJava)
      val builder = table.newScanBuilder(options)
      val partitionSchema = StructType(StructField("part_col", StringType) :: Nil)
      val partColAttr = AttributeReference("part_col", StringType)()
      val listQuery = ListQuery(
        LocalRelation(AttributeReference("a", StringType)()),
        numCols = 1)
      val filterExpr: Expression = InSubquery(Seq(partColAttr), listQuery)

      val (_, postScanFilters) =
        PushDownUtils.pushFilters(builder, Seq(filterExpr), Some(partitionSchema))

      assert(postScanFilters.nonEmpty,
        s"Partition filter with subquery should be in post-scan, got " +
          s"${postScanFilters.size}: ${postScanFilters.mkString(", ")}")
      assert(postScanFilters.exists(_.semanticEquals(filterExpr)),
        s"Post-scan filters should contain the subquery filter: $postScanFilters")
    }
  }
}
