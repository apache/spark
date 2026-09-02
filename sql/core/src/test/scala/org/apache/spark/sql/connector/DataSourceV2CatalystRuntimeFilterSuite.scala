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

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.{AnalysisException, DataFrame, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Add, AttributeReference, DynamicPruning, DynamicPruningExpression, EqualTo, Expression, GetStructField, GreaterThan, Literal, RLike}
import org.apache.spark.sql.catalyst.plans.physical.KeyedPartitioning
import org.apache.spark.sql.catalyst.util.InternalRowComparableWrapper
import org.apache.spark.sql.connector.catalog.{
  Column,
  Identifier,
  InMemoryCatalystRuntimeFilterTable,
  InMemoryTable,
  InMemoryTableCatalystRuntimeFilterCatalog,
  TableCatalog}
import org.apache.spark.sql.connector.expressions.{FieldReference, NamedReference, Transform}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{Batch, HasPartitionKey, InputPartition, PartitionReaderFactory, Scan, SupportsRuntimeV2Filtering}
import org.apache.spark.sql.execution.{FilterExec, ScalarSubquery => ExecScalarSubquery}
import org.apache.spark.sql.execution.ExplainUtils.stripAQEPlan
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, DataSourceV2ScanRelation, DataSourceV2Strategy, PushDownUtils}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.connector.SupportsRuntimeCatalystFiltering
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

/**
 * Tests for scans that implement
 * [[org.apache.spark.sql.internal.connector.SupportsRuntimeCatalystFiltering]],
 * where runtime filters are pushed once as Catalyst expressions instead of connector
 * predicates.
 */
class DataSourceV2CatalystRuntimeFilterSuite extends SharedSparkSession {

  protected val v2Source = classOf[FakeV2ProviderWithCustomSchema].getName
  protected val catalogName = "testcatalystruntimefilter"

  override def sparkConf: SparkConf = super.sparkConf
    .set(s"spark.sql.catalog.$catalogName",
      classOf[InMemoryTableCatalystRuntimeFilterCatalog].getName)

  private def withDPPConf(f: => Unit): Unit = {
    withSQLConf(
      SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
      SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "false",
      SQLConf.DYNAMIC_PARTITION_PRUNING_FALLBACK_FILTER_RATIO.key -> "10")(f)
  }

  test("scalar subquery on partition column -> pushed as Catalyst expression") {
    val tbl = s"$catalogName.tbl1"
    val dim = s"$catalogName.dim1"
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
      val part = AttributeReference("part", IntegerType, nullable = false)()
      assertPushedCatalystPredicatesEqual(df, EqualTo(part, Literal(3)))
      // `part` is not declared fully pushed, so Spark still evaluates the filter after the scan.
      assertScalarSubqueryEvaluatedAfterScan(df, expected = true)

      // The answer alone would be right even without pruning, since that post-scan filter drops
      // the extra rows.
      val batchScan = collectBatchScan(df)
      assert(batchScan.inputPartitions.size === 5)
      assert(batchScan.filteredPartitions.flatten.size === 1,
        s"expected 1 partition after pruning, got ${batchScan.filteredPartitions.flatten.size}")
    }
  }

  test("predicate on fully pushed filter attributes -> not evaluated after the scan") {
    val tbl = s"$catalogName.tbl_fully_pushed"
    val dim = s"$catalogName.dim_fully_pushed"
    withTable(tbl, dim) {
      sql(s"CREATE TABLE $tbl (id INT, part INT) USING $v2Source PARTITIONED BY (part) " +
        "TBLPROPERTIES('fully-pushed-filter-attributes' = 'part')")
      // Matching and nonmatching partitions: the scan must prune nonmatching ones itself
      // because Spark drops the post-scan FilterExec for fully pushed attributes.
      for (i <- 0 until 5) {
        sql(s"INSERT INTO $tbl VALUES ($i, $i)")
      }
      sql(s"CREATE TABLE $dim (val INT) USING $v2Source")
      sql(s"INSERT INTO $dim VALUES (3)")

      val df = sql(s"SELECT * FROM $tbl WHERE part = (SELECT max(val) FROM $dim)")
      checkAnswer(df, Row(3, 3))

      assertScalarSubqueryRuntimeFilters(df)
      val part = AttributeReference("part", IntegerType, nullable = false)()
      assertPushedCatalystPredicatesEqual(df, EqualTo(part, Literal(3)))
      assertScalarSubqueryEvaluatedAfterScan(df, expected = false)
    }
  }

  test("nested fully pushed filter attribute -> rejected without a runtime filter") {
    val tbl = s"$catalogName.tbl_nested_fully_pushed"
    withTable(tbl) {
      sql(s"CREATE TABLE $tbl (id INT, s STRUCT<part: INT, other: INT>) USING $v2Source " +
        "PARTITIONED BY (s.part) " +
        "TBLPROPERTIES('fully-pushed-filter-attributes' = 's.part')")

      val df = sql(s"SELECT * FROM $tbl")
      val scanClass = df.queryExecution.optimizedPlan.collectFirst {
        case r: DataSourceV2ScanRelation => r.scan.getClass.getName
      }.getOrElse(fail("Expected a DataSourceV2ScanRelation"))
      val e = intercept[AnalysisException] {
        df.queryExecution.executedPlan
      }
      checkError(
        exception = e,
        condition = "DATA_SOURCE_INVALID_RUNTIME_FILTER_ATTRIBUTE.NOT_TOP_LEVEL",
        parameters = Map(
          "attribute" -> "`s`.`part`",
          "method" -> "fullyPushedFilterAttributes()",
          "scanClass" -> scanClass,
          "relationOutput" -> "\"STRUCT<id: INT, s: STRUCT<part: INT, other: INT>>\""),
        sqlState = "KD000")
    }
  }

  test("nested filter attribute under a non-struct column -> rejected during resolution") {
    val tbl = s"$catalogName.tbl_malformed_nested_filter_attr"
    withTable(tbl) {
      sql(s"CREATE TABLE $tbl (id INT, part INT) USING $v2Source PARTITIONED BY (part)")

      val scanRelation = sql(s"SELECT * FROM $tbl").queryExecution.optimizedPlan.collectFirst {
        case r: DataSourceV2ScanRelation => r
      }.getOrElse(fail("Expected a DataSourceV2ScanRelation"))
      val e = intercept[AnalysisException] {
        scanRelation.copy(scan = new NestedFilterAttributeScan).runtimeFilterAttrs
      }
      val scanClass = classOf[NestedFilterAttributeScan].getName
      checkError(
        exception = e,
        condition = "DATA_SOURCE_INVALID_RUNTIME_FILTER_ATTRIBUTE.CANNOT_RESOLVE",
        parameters = Map(
          "attribute" -> "`part`.`nested`",
          "method" -> "filterAttributes()",
          "scanClass" -> scanClass,
          "relationOutput" -> "\"STRUCT<id: INT, part: INT>\""),
        sqlState = "KD000")
      checkError(
        exception = e.getCause.asInstanceOf[AnalysisException],
        condition = "INVALID_EXTRACT_BASE_FIELD_TYPE",
        parameters = Map("base" -> "\"part\"", "other" -> "\"INT\""))
    }
  }

  test("missing filter attribute -> rejected") {
    val tbl = s"$catalogName.tbl_missing_filter_attr"
    withTable(tbl) {
      sql(s"CREATE TABLE $tbl (id INT, part INT) USING $v2Source PARTITIONED BY (part)")

      val scanRelation = sql(s"SELECT * FROM $tbl").queryExecution.optimizedPlan.collectFirst {
        case r: DataSourceV2ScanRelation => r
      }.getOrElse(fail("Expected a DataSourceV2ScanRelation"))
      val e = intercept[AnalysisException] {
        scanRelation.copy(scan = new MissingFilterAttributeScan).runtimeFilterAttrs
      }
      val scanClass = classOf[MissingFilterAttributeScan].getName
      checkError(
        exception = e,
        condition = "DATA_SOURCE_INVALID_RUNTIME_FILTER_ATTRIBUTE.CANNOT_RESOLVE",
        parameters = Map(
          "attribute" -> "`missing`",
          "method" -> "filterAttributes()",
          "scanClass" -> scanClass,
          "relationOutput" -> "\"STRUCT<id: INT, part: INT>\""),
        sqlState = "KD000")
      checkError(
        exception = e.getCause.asInstanceOf[AnalysisException],
        condition = "_LEGACY_ERROR_TEMP_1137",
        parameters = Map("name" -> "missing", "outputStr" -> "id,part"))
    }
  }

  test("missing fully pushed filter attribute -> rejected through runtime filter attrs") {
    val tbl = s"$catalogName.tbl_missing_fully_pushed_filter_attr"
    withTable(tbl) {
      sql(s"CREATE TABLE $tbl (id INT, part INT) USING $v2Source PARTITIONED BY (part)")

      val scanRelation = sql(s"SELECT * FROM $tbl").queryExecution.optimizedPlan.collectFirst {
        case r: DataSourceV2ScanRelation => r
      }.getOrElse(fail("Expected a DataSourceV2ScanRelation"))
      val e = intercept[AnalysisException] {
        scanRelation.copy(scan = new MissingFullyPushedFilterAttributeScan)
          .runtimeFilterAttrs
      }
      val scanClass = classOf[MissingFullyPushedFilterAttributeScan].getName
      checkError(
        exception = e,
        condition = "DATA_SOURCE_INVALID_RUNTIME_FILTER_ATTRIBUTE.CANNOT_RESOLVE",
        parameters = Map(
          "attribute" -> "`missing`",
          "method" -> "fullyPushedFilterAttributes()",
          "scanClass" -> scanClass,
          "relationOutput" -> "\"STRUCT<id: INT, part: INT>\""),
        sqlState = "KD000")
      checkError(
        exception = e.getCause.asInstanceOf[AnalysisException],
        condition = "_LEGACY_ERROR_TEMP_1137",
        parameters = Map("name" -> "missing", "outputStr" -> "id,part"))
    }
  }

  test("non-deterministic predicate on fully pushed attributes -> evaluated after the scan") {
    val tbl = s"$catalogName.tbl_nondeterministic"
    val dim = s"$catalogName.dim_nondeterministic"
    withTable(tbl, dim) {
      sql(s"CREATE TABLE $tbl (id INT, part INT) USING $v2Source PARTITIONED BY (part) " +
        "TBLPROPERTIES('fully-pushed-filter-attributes' = 'part')")
      for (i <- 0 until 5) {
        sql(s"INSERT INTO $tbl VALUES ($i, $i)")
      }
      sql(s"CREATE TABLE $dim (val INT) USING $v2Source")
      sql(s"INSERT INTO $dim VALUES (3)")

      // A non-deterministic filter is never pushed, so it must keep its post-scan FilterExec even
      // though it only references a fully pushed attribute. Dropping it there would leave nothing
      // to evaluate it and the scan would return the nonmatching partitions too.
      val df = sql(
        s"SELECT * FROM $tbl WHERE part = (SELECT max(val) FROM $dim) OR rand() < 0.5")
      // The row in the matching partition always qualifies, the others qualify at random.
      assert(df.collect().contains(Row(3, 3)))

      assertScalarSubqueryEvaluatedAfterScan(df, expected = true)
      assertScalarSubqueryRuntimeFilters(df, expectedCount = 0)
      assertPushedCatalystPredicates(df, 0)
    }
  }

  test("predicate on partly fully pushed filter attributes -> evaluated after the scan") {
    val tbl = s"$catalogName.tbl_partly_pushed"
    val dim = s"$catalogName.dim_partly_pushed"
    withTable(tbl, dim) {
      sql(s"CREATE TABLE $tbl (id INT, p1 INT, p2 INT) USING $v2Source " +
        "PARTITIONED BY (p1, p2) " +
        "TBLPROPERTIES('fully-pushed-filter-attributes' = 'p1')")
      for (i <- 0 until 5) {
        sql(s"INSERT INTO $tbl VALUES ($i, 1, 2)")
      }
      sql(s"CREATE TABLE $dim (val INT) USING $v2Source")
      sql(s"INSERT INTO $dim VALUES (3)")

      // The predicate also references p2, which is not declared fully pushed, so it is not
      // considered fully pushed and Spark keeps evaluating it after the scan.
      val df = sql(s"SELECT * FROM $tbl WHERE p1 + p2 = (SELECT max(val) FROM $dim)")
      checkAnswer(df, (0 until 5).map(i => Row(i, 1, 2)))

      assertScalarSubqueryRuntimeFilters(df)
      val p1 = AttributeReference("p1", IntegerType, nullable = false)()
      val p2 = AttributeReference("p2", IntegerType, nullable = false)()
      assertPushedCatalystPredicatesEqual(df, EqualTo(Add(p1, p2), Literal(3)))
      assertScalarSubqueryEvaluatedAfterScan(df, expected = true)
    }
  }

  test("arithmetic around a scalar subquery -> subquery literalized, expression pushed intact") {
    val tbl = s"$catalogName.tbl2"
    val dim = s"$catalogName.dim2"
    withTable(tbl, dim) {
      sql(s"CREATE TABLE $tbl (id INT, part INT) USING $v2Source PARTITIONED BY (part)")
      for (i <- 0 until 5) {
        sql(s"INSERT INTO $tbl VALUES ($i, $i)")
      }
      sql(s"CREATE TABLE $dim (val INT) USING $v2Source")
      sql(s"INSERT INTO $dim VALUES (2)")

      // The scalar subquery is literalized on the way to the scan, and the arithmetic around it
      // survives. This one would also translate to a V2 predicate (`part > 3`, after the
      // literalized operands are folded); see the next test for one that would not.
      val df = sql(s"SELECT * FROM $tbl WHERE part > (SELECT max(val) FROM $dim) + 1")
      checkAnswer(df, Row(4, 4))

      assertScalarSubqueryRuntimeFilters(df)
      val part = AttributeReference("part", IntegerType, nullable = false)()
      assertPushedCatalystPredicatesEqual(
        df, GreaterThan(part, Add(Literal(2), Literal(1))))
    }
  }

  test("filter with no V2 translation -> pushed instead of dropped") {
    val tbl = s"$catalogName.tbl_untranslatable"
    val dim = s"$catalogName.dim_untranslatable"
    withTable(tbl, dim) {
      sql(s"CREATE TABLE $tbl (id INT, part STRING) USING $v2Source PARTITIONED BY (part)")
      for (i <- 0 until 5) {
        sql(s"INSERT INTO $tbl VALUES ($i, '$i')")
      }
      sql(s"CREATE TABLE $dim (val STRING) USING $v2Source")
      sql(s"INSERT INTO $dim VALUES ('3')")

      // `V2ExpressionBuilder` has no RLike case, so this filter has no V2 predicate at all and
      // the V2 interfaces would never see it. The pattern is still a subquery when the optimizer
      // runs, so nothing rewrites the RLIKE into a translatable Contains beforehand either.
      val df = sql(s"SELECT * FROM $tbl WHERE part RLIKE (SELECT max(val) FROM $dim)")
      checkAnswer(df, Row(3, "3"))

      assertScalarSubqueryRuntimeFilters(df)
      val runtimeFilter = collectBatchScan(df).runtimeFilters.head
      assert(DataSourceV2Strategy.translateScalarSubqueryFilterV2(runtimeFilter).isEmpty,
        s"Expected no V2 translation for $runtimeFilter")
      val part = AttributeReference("part", StringType, nullable = false)()
      assertPushedCatalystPredicatesEqual(df, RLike(part, Literal("3")))
    }
  }

  test("DPP filter -> pushed as InSubqueryExec expression") {
    val fact = s"$catalogName.fact3"
    val dim = s"$catalogName.dim3"
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
        val dppPredicate = collectBatchScan(df).runtimeFilters.collectFirst {
          case DynamicPruningExpression(e) => e
        }.get
        assertPushedCatalystPredicatesEqual(df, dppPredicate)
      }
    }
  }

  test("DPP filter on a nested partition source -> pushed with the nested access intact") {
    val fact = s"$catalogName.fact_nested_dpp"
    val dim = s"$catalogName.dim_nested_dpp"
    withTable(fact, dim) {
      sql(s"CREATE TABLE $fact " +
        "(id INT, derives STRUCT<toStr: STRING, other: STRING>) USING " +
        s"$v2Source PARTITIONED BY (derives.toStr)")
      sql(s"INSERT INTO $fact VALUES " +
        "(1, named_struct('toStr', 'AA', 'other', 'a')), " +
        "(2, named_struct('toStr', 'BB', 'other', 'b')), " +
        "(3, named_struct('toStr', 'CC', 'other', 'c'))")
      sql(s"CREATE TABLE $dim (value STRING, selected INT) USING $v2Source")
      sql(s"INSERT INTO $dim VALUES ('AA', 0), ('BB', 1)")

      withDPPConf {
        val df = sql(
          s"""SELECT f.id FROM $fact f JOIN $dim d
             |ON f.derives.toStr = d.value WHERE d.selected = 1""".stripMargin)
        checkAnswer(df, Row(2))

        assertDPPRuntimeFilters(df)
        // The scan reports `derives.toStr`, and the predicate it receives keeps that nested access.
        val pushed = getPushedCatalystPredicates(df)
        assert(pushed.size === 1, s"expected a single pushed predicate, got $pushed")
        assert(pushed.head.exists(_.isInstanceOf[GetStructField]),
          s"expected the pushed predicate to keep the nested access, got ${pushed.head}")

        val batchScan = collectBatchScan(df)
        assert(batchScan.inputPartitions.size === 3)
        assert(batchScan.filteredPartitions.flatten.size === 1,
          s"expected 1 partition after pruning, got ${batchScan.filteredPartitions.flatten.size}")
      }
    }
  }

  test("sibling of a nested filter attribute remains evaluated after the scan") {
    val fact = s"$catalogName.fact_nested_sibling"
    val dim = s"$catalogName.dim_nested_sibling"
    withTable(fact, dim) {
      sql(s"CREATE TABLE $fact (id INT, s STRUCT<part: INT, other: INT>) USING $v2Source " +
        "PARTITIONED BY (s.part)")
      sql(s"INSERT INTO $fact VALUES " +
        "(1, named_struct('part', 1, 'other', 10)), " +
        "(2, named_struct('part', 1, 'other', 20)), " +
        "(3, named_struct('part', 2, 'other', 30))")
      sql(s"CREATE TABLE $dim (value INT) USING $v2Source")
      sql(s"INSERT INTO $dim VALUES (10)")

      val df = sql(s"SELECT id FROM $fact WHERE s.other = (SELECT max(value) FROM $dim)")
      checkAnswer(df, Row(1))

      // A nested reference currently contributes its root attribute to eligibility, so `s.other`
      // may be routed to a scan that advertises `s.part`. It must remain above the scan unless
      // eligibility becomes path-aware.
      assertScalarSubqueryEvaluatedAfterScan(df, expected = true)
    }
  }

  test("scan implementing both runtime filtering interfaces -> rejected") {
    val tbl = s"$catalogName.tbl_both_interfaces"
    withTable(tbl) {
      sql(s"CREATE TABLE $tbl (id INT, part INT) USING $v2Source PARTITIONED BY (part)")
      sql(s"INSERT INTO $tbl VALUES (1, 1)")

      val scanRelation = sql(s"SELECT * FROM $tbl").queryExecution.optimizedPlan.collectFirst {
        case r: DataSourceV2ScanRelation => r
      }.getOrElse(fail("Expected a DataSourceV2ScanRelation"))

      // every runtime filtering path starts at runtimeFilterAttrs
      val e = intercept[SparkException] {
        scanRelation.copy(scan = new BothRuntimeFilteringInterfacesScan).runtimeFilterAttrs
      }
      assert(e.getMessage.contains("A scan must not implement both SupportsRuntimeV2Filtering " +
        "and SupportsRuntimeCatalystFiltering"))
    }
  }

  test("filter on column outside filterAttributes -> not pushed, even if declared fully pushed") {
    val tbl = s"$catalogName.tbl4"
    val dim = s"$catalogName.dim4"
    withTable(tbl, dim) {
      // p2 is a partition column but is not declared filterable, so no runtime filter is derived
      // for it. Declaring it fully pushed as well, which the interface forbids for an attribute
      // that is not filterable, must not cost it the post-scan filter: nothing was pushed, so the
      // scan prunes nothing and the nonmatching rows would come back.
      sql(s"CREATE TABLE $tbl (id INT, p1 INT, p2 INT) USING $v2Source " +
        "PARTITIONED BY (p1, p2) " +
        "TBLPROPERTIES('filter-attributes' = 'p1', 'fully-pushed-filter-attributes' = 'p2')")
      for (i <- 0 until 5) {
        sql(s"INSERT INTO $tbl VALUES ($i, $i, $i)")
      }
      sql(s"CREATE TABLE $dim (val INT) USING $v2Source")
      sql(s"INSERT INTO $dim VALUES (3)")

      val df = sql(s"SELECT * FROM $tbl WHERE p2 = (SELECT max(val) FROM $dim)")
      checkAnswer(df, Row(3, 3, 3))

      assert(collectBatchScan(df).runtimeFilters.isEmpty,
        "Expected no runtime filters for a column outside filterAttributes")
      assertPushedCatalystPredicates(df, 0)
      assertScalarSubqueryEvaluatedAfterScan(df, expected = true)
    }
  }

  test("two predicates on filter attributes -> pushed together in a single filter() call") {
    val tbl = s"$catalogName.tbl_two_predicates"
    val dim1 = s"$catalogName.dim_two_predicates1"
    val dim2 = s"$catalogName.dim_two_predicates2"
    withTable(tbl, dim1, dim2) {
      sql(s"CREATE TABLE $tbl (id INT, p1 INT, p2 INT) USING $v2Source PARTITIONED BY (p1, p2)")
      for (i <- 0 until 5) {
        sql(s"INSERT INTO $tbl VALUES ($i, $i, ${i * 10})")
      }
      sql(s"CREATE TABLE $dim1 (val INT) USING $v2Source")
      sql(s"INSERT INTO $dim1 VALUES (3)")
      sql(s"CREATE TABLE $dim2 (val INT) USING $v2Source")
      sql(s"INSERT INTO $dim2 VALUES (30)")

      val df = sql(s"SELECT * FROM $tbl WHERE p1 = (SELECT max(val) FROM $dim1) " +
        s"AND p2 = (SELECT max(val) FROM $dim2)")
      checkAnswer(df, Row(3, 3, 30))

      assertScalarSubqueryRuntimeFilters(df, expectedCount = 2)
      val p1 = AttributeReference("p1", IntegerType, nullable = false)()
      val p2 = AttributeReference("p2", IntegerType, nullable = false)()
      assertPushedCatalystPredicatesEqual(
        df, EqualTo(p1, Literal(3)), EqualTo(p2, Literal(30)))
      assert(getCatalystScan(df).filterCallCount === 1,
        "expected both predicates pushed in a single filter() call")
    }
  }

  test("nested field of a filter attribute -> pushed with the nested access intact") {
    val tbl = s"$catalogName.tbl_nested"
    val dim = s"$catalogName.dim_nested"
    withTable(tbl, dim) {
      sql(s"CREATE TABLE $tbl (id INT, s STRUCT<tz: STRING>) USING $v2Source " +
        "PARTITIONED BY (s.tz)")
      for (i <- 0 until 3) {
        sql(s"INSERT INTO $tbl VALUES ($i, named_struct('tz', 'tz$i'))")
      }
      sql(s"CREATE TABLE $dim (val STRING) USING $v2Source")
      sql(s"INSERT INTO $dim VALUES ('tz1')")

      // The scan declares the nested partition source `s.tz` as its filter attribute. The
      // predicate arrives with the nested access intact, and matching it against the partition
      // layout is left to the scan, which this fixture does.
      val df = sql(s"SELECT * FROM $tbl WHERE s.tz = (SELECT max(val) FROM $dim)")
      checkAnswer(df, Row(1, Row("tz1")))

      assertScalarSubqueryRuntimeFilters(df)
      val pushed = getPushedCatalystPredicates(df)
      assert(pushed.size === 1, s"expected a single pushed predicate, got $pushed")
      val nestedAccesses = pushed.head.collect { case g: GetStructField => g }
      assert(nestedAccesses.size === 1,
        s"expected the pushed predicate to keep the nested access, got ${pushed.head}")
      assert(nestedAccesses.head.childSchema.fieldNames.contains("tz"))
    }
  }

  test("dotted top-level and nested partition columns -> bound to the correct partition slot") {
    val tbl = s"$catalogName.tbl_dotted_collision"
    val dim = s"$catalogName.dim_dotted_collision"
    withTable(tbl, dim) {
      // Two partition columns whose dotted names collide: the quoted top-level column `x.y` and
      // the nested field `x`.`y`. They carry different values in each row, so a predicate bound
      // to the wrong slot would prune the wrong partitions. `x.y` is 3 exactly where `x`.`y` is
      // 30, so binding a filter on the nested field to the top-level slot would find nothing.
      sql(s"CREATE TABLE $tbl (id INT, `x.y` INT, x STRUCT<y: INT>) USING $v2Source " +
        "PARTITIONED BY (`x.y`, x.y)")
      for (i <- 0 until 5) {
        sql(s"INSERT INTO $tbl VALUES ($i, $i, named_struct('y', ${i * 10}))")
      }
      sql(s"CREATE TABLE $dim (val INT) USING $v2Source")
      sql(s"INSERT INTO $dim VALUES (30)")

      // Alias the table so `f.x.y` unambiguously reads the nested field, not the column `x.y`.
      val df = sql(s"SELECT * FROM $tbl f WHERE f.x.y = (SELECT max(val) FROM $dim)")
      checkAnswer(df, Row(3, 3, Row(30)))

      assertScalarSubqueryRuntimeFilters(df)
      // The pushed predicate keeps the nested access, and the scan prunes to the single partition
      // whose nested `x`.`y` is 30 rather than binding to the colliding top-level `x.y` slot.
      val pushed = getPushedCatalystPredicates(df)
      assert(pushed.size === 1, s"expected a single pushed predicate, got $pushed")
      assert(pushed.head.exists(_.isInstanceOf[GetStructField]),
        s"expected the pushed predicate to keep the nested access, got ${pushed.head}")
      val batchScan = collectBatchScan(df)
      assert(batchScan.inputPartitions.size === 5)
      assert(batchScan.filteredPartitions.flatten.size === 1,
        s"expected 1 partition after pruning, got ${batchScan.filteredPartitions.flatten.size}")
    }
  }

  test("no runtime filter -> filter() is never called") {
    val tbl = s"$catalogName.tbl5"
    withTable(tbl) {
      sql(s"CREATE TABLE $tbl (id INT, part INT) USING $v2Source PARTITIONED BY (part)")
      for (i <- 0 until 5) {
        sql(s"INSERT INTO $tbl VALUES ($i, $i)")
      }

      val df = sql(s"SELECT * FROM $tbl WHERE part = 3")
      checkAnswer(df, Row(3, 3))

      assert(collectBatchScan(df).runtimeFilters.isEmpty)
      assertPushedCatalystPredicates(df, 0)
    }
  }

  test("ALTER TABLE keeps the Catalyst runtime-filter table type") {
    val tbl = s"$catalogName.tbl_alter"
    withTable(tbl) {
      sql(s"CREATE TABLE $tbl (id INT, part INT) USING $v2Source PARTITIONED BY (part)")
      sql(s"ALTER TABLE $tbl ADD COLUMNS (extra INT)")

      val table = spark.sessionState.catalogManager.catalog(catalogName)
        .asInstanceOf[TableCatalog]
        .loadTable(Identifier.of(Array.empty, "tbl_alter"))
      assert(table.isInstanceOf[InMemoryCatalystRuntimeFilterTable],
        s"ALTER TABLE reconstructed ${table.getClass.getName}")

      sql(s"INSERT INTO $tbl VALUES (0, 0, 10), (1, 1, 11)")
      checkAnswer(sql(s"SELECT id, part FROM $tbl WHERE extra = 11"), Row(1, 1))
    }
  }

  /**
   * While SPJ is active the scan's partitioning has to survive runtime filtering, so the
   * post-filter partitions still line up with the other side of the join: splits may be pruned,
   * but the source may not drop a partition key, invent one, or grow a key's split count.
   */
  test("data source that breaks the partitioning it reported -> rejected") {
    val partAttr = AttributeReference("part", IntegerType)()
    val table = new InMemoryTable("t", Array(Column.create("part", IntegerType)),
      Array.empty[Transform], java.util.Collections.emptyMap[String, String])
    val partitioning = KeyedPartitioning(
      Seq(partAttr),
      Seq(InternalRowComparableWrapper(InternalRow(1), Seq(partAttr))),
      isGrouped = false, isCollapsed = false)

    def replanAfterFiltering(afterFilter: Seq[InputPartition]): Unit = {
      val scan = new PartitioningBreakingScan(Seq(KeyedInputPartition(1)), afterFilter)
      PushDownUtils.replanWithRuntimeFilters(scan, Seq(EqualTo(partAttr, Literal(1))), table,
        Seq(partAttr), partitioning, originalPartitions = Seq.empty)
    }

    val keyDropped = intercept[SparkException](replanAfterFiltering(Seq(new InputPartition {})))
    assert(keyDropped.getMessage.contains("must have preserved the original partitioning"))

    val keyInvented = intercept[SparkException](replanAfterFiltering(Seq(KeyedInputPartition(99))))
    assert(keyInvented.getMessage.contains("must not report new partition keys"))

    val splitsGrown = intercept[SparkException] {
      replanAfterFiltering(Seq(KeyedInputPartition(1), KeyedInputPartition(1)))
    }
    assert(splitsGrown.getMessage.contains("must not report new partitions for a given key"))
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

  /**
   * Checks whether a scalar subquery runtime filter is still evaluated by a [[FilterExec]] above
   * the scan. Filters that only reference `fullyPushedFilterAttributes` are dropped from it.
   */
  private def assertScalarSubqueryEvaluatedAfterScan(
      df: DataFrame,
      expected: Boolean): Unit = {
    val postScanConditions = stripAQEPlan(df.queryExecution.executedPlan).collect {
      case f: FilterExec => f.condition
    }
    val evaluated = postScanConditions.exists(_.exists(_.isInstanceOf[ExecScalarSubquery]))
    assert(evaluated === expected,
      s"Expected scalar subquery evaluated after scan to be $expected, " +
        s"post-scan filter conditions: $postScanConditions")
  }

  private def collectBatchScan(df: DataFrame): BatchScanExec = {
    stripAQEPlan(df.queryExecution.executedPlan).collectFirst {
      case b: BatchScanExec => b
    }.getOrElse(fail("Expected BatchScanExec in plan"))
  }

  private type CatalystScan =
    InMemoryCatalystRuntimeFilterTable#InMemoryCatalystRuntimeFilterBatchScan

  private def getCatalystScan(df: DataFrame): CatalystScan = {
    collectBatchScan(df).scan match {
      case s: CatalystScan => s
      case other => fail(s"Expected InMemoryCatalystRuntimeFilterBatchScan, got $other")
    }
  }

  private def getPushedCatalystPredicates(df: DataFrame): Seq[Expression] = {
    getCatalystScan(df).pushedCatalystPredicates
  }

  private def assertPushedCatalystPredicates(df: DataFrame, expected: Int): Unit = {
    val preds = getPushedCatalystPredicates(df)
    assert(preds.size === expected,
      s"Expected $expected pushed Catalyst runtime predicate(s), got ${preds.size}: $preds")
  }

  /**
   * Binds [[AttributeReference]]s in `expected` to the scan output (by name) and checks that the
   * pushed Catalyst runtime predicates match exactly via [[Expression.semanticEquals]].
   */
  private def assertPushedCatalystPredicatesEqual(
      df: DataFrame,
      expected: Expression*): Unit = {
    val batchScan = collectBatchScan(df)
    val actual = getPushedCatalystPredicates(df)
    val normalizedExpected = expected.map(bindToScanOutput(_, batchScan.output))
    assert(actual.size === normalizedExpected.size,
      s"Expected ${normalizedExpected.size} pushed Catalyst predicate(s), " +
        s"got ${actual.size}: $actual")
    actual.zip(normalizedExpected).foreach { case (a, e) =>
      assert(a.semanticEquals(e),
        s"Pushed Catalyst predicate mismatch.\nExpected: $e\nActual:   $a")
    }
  }

  private def bindToScanOutput(
      expr: Expression,
      output: Seq[AttributeReference]): Expression = {
    val resolver = SQLConf.get.resolver
    expr.transformUp {
      case a: AttributeReference =>
        output.find(o => resolver(o.name, a.name))
          .map(_.withNullability(a.nullable).withQualifier(a.qualifier))
          .getOrElse(a)
    }
  }
}

/** A scan violating the rule that only one runtime filtering interface may be implemented. */
private class BothRuntimeFilteringInterfacesScan
  extends Scan with SupportsRuntimeV2Filtering with SupportsRuntimeCatalystFiltering {

  override def readSchema(): StructType = new StructType().add("part", IntegerType)

  override def filterAttributes(): Array[NamedReference] = Array(FieldReference("part"))

  override def filter(predicates: Array[Predicate]): Unit = {}

  override def filter(expressions: Array[Expression]): Unit = {}
}

/** A scan declaring a filter attribute that its read schema does not contain. */
private class MissingFilterAttributeScan extends SupportsRuntimeCatalystFiltering {

  override def readSchema(): StructType = new StructType().add("part", IntegerType)

  override def filterAttributes(): Array[NamedReference] = Array(FieldReference("missing"))

  override def filter(expressions: Array[Expression]): Unit = {}
}

/** A scan declaring a fully pushed filter attribute that its relation output does not contain. */
private class MissingFullyPushedFilterAttributeScan extends SupportsRuntimeCatalystFiltering {

  override def readSchema(): StructType = new StructType().add("part", IntegerType)

  override def filterAttributes(): Array[NamedReference] = Array(FieldReference("part"))

  override def fullyPushedFilterAttributes(): Array[NamedReference] =
    Array(FieldReference("missing"))

  override def filter(expressions: Array[Expression]): Unit = {}
}

/** A scan declaring a nested runtime-filter attribute beneath an integer column. */
private class NestedFilterAttributeScan extends SupportsRuntimeCatalystFiltering {

  override def readSchema(): StructType = new StructType().add("part", IntegerType)

  override def filterAttributes(): Array[NamedReference] =
    Array(FieldReference(Seq("part", "nested")))

  override def filter(expressions: Array[Expression]): Unit = {}
}

private case class KeyedInputPartition(key: Int) extends InputPartition with HasPartitionKey {
  override def partitionKey(): InternalRow = InternalRow(key)
}

/**
 * A scan reporting one set of partitions before filtering and another after, so it can break the
 * requirement to preserve the partitioning it originally reported.
 */
private class PartitioningBreakingScan(
    initialPartitions: Seq[InputPartition],
    afterFilter: Seq[InputPartition])
  extends Scan with Batch with SupportsRuntimeCatalystFiltering {

  private var filtered = false

  override def readSchema(): StructType = new StructType().add("part", IntegerType)

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] =
    if (filtered) afterFilter.toArray else initialPartitions.toArray

  override def createReaderFactory(): PartitionReaderFactory =
    throw new UnsupportedOperationException()

  override def filterAttributes(): Array[NamedReference] = Array(FieldReference("part"))

  override def filter(expressions: Array[Expression]): Unit = {
    filtered = true
  }
}
