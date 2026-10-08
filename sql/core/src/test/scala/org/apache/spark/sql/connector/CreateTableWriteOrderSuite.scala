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

import java.util
import java.util.Locale

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.{CreateTable, CreateTableAsSelect, LogicalPlan, ReplaceTable, ReplaceTableAsSelect, V2CreateTablePlan}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, RangePartitioning}
import org.apache.spark.sql.connector.catalog.{Column, DelegatingCatalogExtension, DelegatingTable, Identifier, InMemoryTable, InMemoryTableCatalog, StagedTable, StagingInMemoryTableCatalog, Table, TableCatalogCapability, TableInfo, WriteDistributionMode}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.connector.catalog.WriteDistributionMode.{HASH, NONE, RANGE}
import org.apache.spark.sql.connector.distributions.Distributions
import org.apache.spark.sql.connector.expressions.{ClusterByTransform, Expression, FieldReference, LogicalExpressions, NamedReference, NullOrdering, SortDirection, SortOrder, Transform}
import org.apache.spark.sql.connector.expressions.LogicalExpressions.literal
import org.apache.spark.sql.execution.{SortExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.v2.V2TableWriteExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Tests the create-time write distribution and ordering clauses: CREATE/REPLACE TABLE ...
 * DISTRIBUTED BY PARTITION ... [LOCALLY] ORDERED BY ... | UNORDERED. Covers what the parser
 * produces, what reaches the catalog, and how a catalog without the capability is rejected.
 */
class CreateTableWriteOrderSuite extends QueryTest with SharedSparkSession {

  // The catalog manager caches catalogs for the session; reset it so tests don't share tables.
  override def afterEach(): Unit = {
    spark.sessionState.catalogManager.reset()
    super.afterEach()
  }

  private def parse(sql: String): LogicalPlan = spark.sessionState.sqlParser.parsePlan(sql)

  private def analyze(sql: String): LogicalPlan =
    spark.sessionState.executePlan(parse(sql)).analyzed

  private def orderingOf(plan: LogicalPlan): Seq[String] = plan match {
    case create: V2CreateTablePlan => WriteSpecCall.render(create.writeOrdering)
    case other => fail(s"unexpected plan: $other")
  }

  private def calls(catalogName: String): Seq[WriteSpecCall] =
    spark.sessionState.catalogManager.catalog(catalogName)
      .asInstanceOf[RecordsWriteSpecs].recordedCalls

  private def writeClauseLines(ddl: String): Seq[String] = ddl.split("\n").toSeq.filter { line =>
    Seq("DISTRIBUTED BY", "ORDERED BY", "LOCALLY ORDERED BY", "UNORDERED").exists(line.startsWith)
  }

  private def loadTable(catalogName: String, name: String): Table =
    spark.sessionState.catalogManager.catalog(catalogName).asTableCatalog
      .loadTable(Identifier.of(Array.empty, name))

  private def clusteringOf(table: Table): Seq[Seq[String]] =
    table.partitioning().toSeq.collect {
      case ClusterByTransform(columns) => columns.map(_.describe)
    }

  test("parse DISTRIBUTED BY PARTITION / ORDERED BY on CREATE TABLE") {
    parse("CREATE TABLE t (id INT, c STRING) USING foo PARTITIONED BY (c) " +
      "DISTRIBUTED BY PARTITION ORDERED BY id ASC NULLS LAST") match {
      case c: CreateTable =>
        assert(c.writeDistributionMode === HASH)
        assert(c.writeOrdering.length === 1)
        val order = c.writeOrdering.head
        assert(order.expression() === FieldReference(Seq("id")))
        assert(order.direction() === SortDirection.ASCENDING)
        assert(order.nullOrdering() === NullOrdering.NULLS_LAST)
      case other => fail(s"unexpected plan: $other")
    }
  }

  test("bare ORDERED BY implies range, LOCALLY implies none, UNORDERED implies none") {
    Seq(
      ("ORDERED BY id", RANGE, 1),
      ("LOCALLY ORDERED BY id", NONE, 1),
      ("UNORDERED", NONE, 0)
    ).foreach { case (clause, expectedMode, expectedOrderingSize) =>
      parse(s"CREATE TABLE t (id INT) USING foo $clause") match {
        case c: CreateTable =>
          assert(c.writeDistributionMode === expectedMode, s"for $clause")
          assert(c.writeOrdering.length === expectedOrderingSize, s"for $clause")
        case other => fail(s"unexpected plan for $clause: $other")
      }
    }
  }

  test("an explicit DISTRIBUTED BY PARTITION decides the distribution on its own") {
    Seq(
      ("ORDERED BY id", 1),
      ("LOCALLY ORDERED BY id", 1),
      ("UNORDERED", 0)
    ).foreach { case (clause, expectedOrderingSize) =>
      parse(s"CREATE TABLE t (id INT, c STRING) USING foo PARTITIONED BY (c) " +
        s"DISTRIBUTED BY PARTITION $clause") match {
        case c: CreateTable =>
          assert(c.writeDistributionMode === HASH, s"for $clause")
          assert(c.writeOrdering.length === expectedOrderingSize, s"for $clause")
        case other => fail(s"unexpected plan for $clause: $other")
      }
    }
  }

  test("no clause leaves the mode unset (null), distinct from an explicit none") {
    parse("CREATE TABLE t (id INT) USING foo") match {
      case c: CreateTable =>
        assert(c.writeDistributionMode === null)
        assert(c.writeOrdering.isEmpty)
      case other => fail(s"unexpected plan: $other")
    }
  }

  test("CTAS / REPLACE / RTAS carry the clauses too") {
    parse("CREATE TABLE t USING foo ORDERED BY id AS SELECT 1 AS id") match {
      case c: CreateTableAsSelect =>
        assert(c.writeDistributionMode === RANGE && c.writeOrdering.length == 1)
      case other => fail(s"unexpected plan: $other")
    }
    parse("REPLACE TABLE t (id INT) USING foo ORDERED BY id DESC NULLS LAST") match {
      case r: ReplaceTable =>
        assert(r.writeDistributionMode === RANGE)
        assert(r.writeOrdering.head.direction() === SortDirection.DESCENDING)
        assert(r.writeOrdering.head.nullOrdering() === NullOrdering.NULLS_LAST)
      case other => fail(s"unexpected plan: $other")
    }
    parse("REPLACE TABLE t USING foo UNORDERED AS SELECT 1 AS id") match {
      case r: ReplaceTableAsSelect =>
        assert(r.writeDistributionMode === NONE && r.writeOrdering.isEmpty)
      case other => fail(s"unexpected plan: $other")
    }
  }

  test("transforms are allowed in the ordering") {
    val stmt = "CREATE TABLE t (id INT, ts TIMESTAMP) USING foo " +
      "ORDERED BY days(ts), bucket(16, id)"
    parse(stmt) match {
      case c: CreateTable =>
        assert(c.writeOrdering.length === 2)
        assert(c.writeOrdering.map(_.expression().describe()) ===
          Seq("days(ts)", "bucket(16, id)"))
      case other => fail(s"unexpected plan: $other")
    }
  }

  test("the parenthesised ORDERED BY form parses the same") {
    Seq("ORDERED BY id DESC, c", "ORDERED BY (id DESC, c)").foreach { clause =>
      parse(s"CREATE TABLE t (id INT, c STRING) USING foo $clause") match {
        case c: CreateTable =>
          assert(WriteSpecCall.render(c.writeOrdering) ===
            Seq("id DESC NULLS LAST", "c ASC NULLS FIRST"), s"for $clause")
        case other => fail(s"unexpected plan for $clause: $other")
      }
    }
  }

  test("the write clauses may appear in any position among the other create-table clauses") {
    Seq(
      "ORDERED BY id PARTITIONED BY (c) DISTRIBUTED BY PARTITION",
      "DISTRIBUTED BY PARTITION PARTITIONED BY (c) LOCALLY ORDERED BY id",
      "PARTITIONED BY (c) ORDERED BY id COMMENT 'a table' DISTRIBUTED BY PARTITION"
    ).foreach { clauses =>
      parse(s"CREATE TABLE t (id INT, c STRING) USING foo $clauses") match {
        case c: CreateTable =>
          assert(c.writeDistributionMode === HASH, s"for $clauses")
          assert(c.writeOrdering.length === 1, s"for $clauses")
        case other => fail(s"unexpected plan for $clauses: $other")
      }
    }
  }

  test("a repeated write clause is rejected") {
    Seq(
      ("DISTRIBUTED BY PARTITION DISTRIBUTED BY PARTITION", "DISTRIBUTED BY PARTITION"),
      ("ORDERED BY id UNORDERED", "ORDERED BY/UNORDERED"),
      ("UNORDERED LOCALLY ORDERED BY id", "ORDERED BY/UNORDERED")
    ).foreach { case (clauses, clauseName) =>
      val stmt = s"CREATE TABLE t (id INT, c STRING) USING foo PARTITIONED BY (c) $clauses"
      checkError(
        exception = intercept[ParseException](parse(stmt)),
        condition = "DUPLICATE_CLAUSES",
        sqlState = "42614",
        parameters = Map("clauseName" -> clauseName),
        context = ExpectedContext(stmt, 0, stmt.length - 1))
    }
  }

  test("DISTRIBUTED BY PARTITION requires a partitioned table") {
    Seq(
      "CREATE TABLE t (id INT) USING foo DISTRIBUTED BY PARTITION",
      "CREATE TABLE t USING foo DISTRIBUTED BY PARTITION AS SELECT 1 AS id",
      "REPLACE TABLE t (id INT) USING foo DISTRIBUTED BY PARTITION",
      "REPLACE TABLE t USING foo DISTRIBUTED BY PARTITION AS SELECT 1 AS id"
    ).foreach { stmt =>
      checkError(
        exception = intercept[ParseException](parse(stmt)),
        condition = "SPECIFY_DISTRIBUTED_BY_PARTITION_WITHOUT_PARTITIONING_IS_NOT_ALLOWED",
        sqlState = "42908",
        parameters = Map.empty,
        context = ExpectedContext(stmt, 0, stmt.length - 1))
    }
  }

  test("ordering references are normalized to the schema's spelling") {
    withSQLConf("spark.sql.catalog.testcat" -> classOf[RecordingInMemoryTableCatalog].getName) {
      assert(orderingOf(analyze("CREATE TABLE testcat.t (ID INT, TS TIMESTAMP) USING foo " +
        "ORDERED BY id, days(ts) DESC")) === Seq("ID ASC NULLS FIRST", "days(TS) DESC NULLS LAST"))

      assert(orderingOf(analyze(
        "CREATE TABLE testcat.t USING foo ORDERED BY id AS SELECT 1 AS ID")) ===
        Seq("ID ASC NULLS FIRST"))

      assert(orderingOf(analyze(
        "CREATE TABLE testcat.t (P STRUCT<X: INT>) USING foo ORDERED BY p.x")) ===
        Seq("P.X ASC NULLS FIRST"))

      assert(orderingOf(analyze("CREATE TABLE testcat.t (ID INT) USING foo " +
        "ORDERED BY truncate(4, ID)")) === Seq("truncate(4, ID) ASC NULLS FIRST"))
      val truncate = "CREATE TABLE testcat.t (ID INT) USING foo ORDERED BY truncate(4, id)"
      checkError(
        exception = intercept[AnalysisException](analyze(truncate)),
        condition = "WRITE_ORDERING_WITH_UNKNOWN_COLUMN",
        sqlState = "42703",
        parameters = Map("cols" -> "`id`"),
        context = ExpectedContext(truncate, 0, truncate.length - 1))

      // references are normalized independently, so only the unresolvable one is reported
      val multi = "CREATE TABLE testcat.t (ID INT) USING foo ORDERED BY bucket(4, id, nope)"
      checkError(
        exception = intercept[AnalysisException](analyze(multi)),
        condition = "WRITE_ORDERING_WITH_UNKNOWN_COLUMN",
        sqlState = "42703",
        parameters = Map("cols" -> "`nope`"),
        context = ExpectedContext(multi, 0, multi.length - 1))
    }
  }

  test("REPLACE TABLE and RTAS normalize the ordering too") {
    withSQLConf("spark.sql.catalog.testcat" -> classOf[RecordingInMemoryTableCatalog].getName) {
      assert(orderingOf(analyze("REPLACE TABLE testcat.t (ID INT) USING foo ORDERED BY id")) ===
        Seq("ID ASC NULLS FIRST"))
      assert(orderingOf(analyze(
        "REPLACE TABLE testcat.t USING foo ORDERED BY id AS SELECT 1 AS ID")) ===
        Seq("ID ASC NULLS FIRST"))
    }
  }

  test("a case-sensitive session resolves the ordering case-sensitively") {
    withSQLConf(
      "spark.sql.catalog.testcat" -> classOf[RecordingInMemoryTableCatalog].getName,
      SQLConf.CASE_SENSITIVE.key -> "true") {
      assert(orderingOf(analyze("CREATE TABLE testcat.t (ID INT) USING foo ORDERED BY ID")) ===
        Seq("ID ASC NULLS FIRST"))
      val stmt = "CREATE TABLE testcat.t (ID INT) USING foo ORDERED BY id"
      checkError(
        exception = intercept[AnalysisException](analyze(stmt)),
        condition = "WRITE_ORDERING_WITH_UNKNOWN_COLUMN",
        sqlState = "42703",
        parameters = Map("cols" -> "`id`"),
        context = ExpectedContext(stmt, 0, stmt.length - 1))
    }
  }

  test("a case-insensitive session does not normalize an ApplyTransform's references") {
    withSQLConf("spark.sql.catalog.testcat" -> classOf[RecordingInMemoryTableCatalog].getName) {
      assert(orderingOf(analyze("CREATE TABLE testcat.t (id INT) USING foo ORDERED BY ID")) ===
        Seq("id ASC NULLS FIRST"))

      val ordered = "CREATE TABLE testcat.t (id INT) USING foo ORDERED BY truncate(4, ID)"
      checkError(
        exception = intercept[AnalysisException](analyze(ordered)),
        condition = "WRITE_ORDERING_WITH_UNKNOWN_COLUMN",
        sqlState = "42703",
        parameters = Map("cols" -> "`ID`"),
        context = ExpectedContext(ordered, 0, ordered.length - 1))

      val partitioned =
        "CREATE TABLE testcat.t (id INT) USING foo PARTITIONED BY (truncate(4, ID))"
      checkError(
        exception = intercept[AnalysisException](analyze(partitioned)),
        condition = "UNSUPPORTED_FEATURE.PARTITION_WITH_NESTED_COLUMN_IS_UNSUPPORTED",
        sqlState = "0A000",
        parameters = Map("cols" -> "`ID`"),
        context = ExpectedContext(partitioned, 0, partitioned.length - 1))

      assert(orderingOf(analyze("CREATE TABLE testcat.t (id INT, ts TIMESTAMP) USING foo " +
        "ORDERED BY days(TS), bucket(4, ID)")) ===
        Seq("days(ts) ASC NULLS FIRST", "bucket(4, id) ASC NULLS FIRST"))
      Seq("DAYS(TS)" -> "`TS`", "BUCKET(4, ID)" -> "`ID`").foreach { case (key, cols) =>
        val stmt = s"CREATE TABLE testcat.t (id INT, ts TIMESTAMP) USING foo ORDERED BY $key"
        checkError(
          exception = intercept[AnalysisException](analyze(stmt)),
          condition = "WRITE_ORDERING_WITH_UNKNOWN_COLUMN",
          sqlState = "42703",
          parameters = Map("cols" -> cols),
          context = ExpectedContext(stmt, 0, stmt.length - 1))
      }
    }
  }

  test("ORDERED BY needs a schema to resolve against") {
    withSQLConf("spark.sql.catalog.testcat" -> classOf[RecordingInMemoryTableCatalog].getName) {
      Seq("ORDERED BY id", "LOCALLY ORDERED BY id").foreach { clause =>
        val stmt = s"CREATE TABLE testcat.t USING foo $clause"
        checkError(
          exception = intercept[AnalysisException](analyze(stmt)),
          condition = "SPECIFY_WRITE_ORDERING_IS_NOT_ALLOWED",
          sqlState = "42601",
          parameters = Map.empty,
          context = ExpectedContext(stmt, 0, stmt.length - 1))
      }

      // UNORDERED asks for no ordering at all, so it has nothing to resolve and stays allowed
      analyze("CREATE TABLE testcat.t USING foo UNORDERED")
    }
  }

  test("an ordering on an unknown column is rejected during analysis") {
    withSQLConf("spark.sql.catalog.testcat" -> classOf[RecordingInMemoryTableCatalog].getName) {
      Seq(
        "CREATE TABLE testcat.t (id INT) USING foo ORDERED BY nope",
        "CREATE TABLE testcat.t (id INT) USING foo ORDERED BY days(nope)",
        "CREATE TABLE testcat.t (id INT) USING foo ORDERED BY truncate(4, nope)",
        "CREATE TABLE testcat.t USING foo ORDERED BY nope AS SELECT 1 AS id"
      ).foreach { stmt =>
        checkError(
          exception = intercept[AnalysisException](analyze(stmt)),
          condition = "WRITE_ORDERING_WITH_UNKNOWN_COLUMN",
          sqlState = "42703",
          parameters = Map("cols" -> "`nope`"),
          context = ExpectedContext(stmt, 0, stmt.length - 1))
      }

      // a nested struct field resolves
      analyze("CREATE TABLE testcat.t (p STRUCT<x: INT>) USING foo ORDERED BY p.x")

      val throughInt = "CREATE TABLE testcat.t (id INT) USING foo ORDERED BY id.x"
      checkError(
        exception = intercept[AnalysisException](analyze(throughInt)),
        condition = "WRITE_ORDERING_WITH_UNKNOWN_COLUMN",
        sqlState = "42703",
        parameters = Map("cols" -> "`id`.`x`"),
        context = ExpectedContext(throughInt, 0, throughInt.length - 1))
    }
  }

  test("unknown ordering columns are reported once each, in declaration order") {
    withSQLConf("spark.sql.catalog.testcat" -> classOf[RecordingInMemoryTableCatalog].getName) {
      val stmt = "CREATE TABLE testcat.t (id INT) USING foo " +
        "ORDERED BY e, d, bucket(4, c, a), id, b, days(f), d"
      checkError(
        exception = intercept[AnalysisException](analyze(stmt)),
        condition = "WRITE_ORDERING_WITH_UNKNOWN_COLUMN",
        sqlState = "42703",
        parameters = Map("cols" -> "`e`, `d`, `c`, `a`, `b`, `f`"),
        context = ExpectedContext(stmt, 0, stmt.length - 1))
    }
  }

  test("a repeated ordering column is accepted, unlike a repeated partition column") {
    withSQLConf("spark.sql.catalog.testcat" -> classOf[RecordingInMemoryTableCatalog].getName) {
      assert(orderingOf(analyze("CREATE TABLE testcat.t (id INT) USING foo " +
        "ORDERED BY bucket(4, id), bucket(8, id)")) ===
        Seq("bucket(4, id) ASC NULLS FIRST", "bucket(8, id) ASC NULLS FIRST"))
      analyze("CREATE TABLE testcat.t (id INT) USING foo ORDERED BY id, id DESC")
      analyze("CREATE TABLE testcat.t (ts TIMESTAMP) USING foo ORDERED BY days(ts), hours(ts)")
    }
  }

  test("DISTRIBUTED BY PARTITION is not satisfied by CLUSTER BY") {
    Seq(
      "CREATE TABLE t (id INT, c STRING) USING foo CLUSTER BY (c) DISTRIBUTED BY PARTITION",
      "REPLACE TABLE t USING foo CLUSTER BY (c) DISTRIBUTED BY PARTITION AS SELECT 'x' AS c"
    ).foreach { stmt =>
      checkError(
        exception = intercept[ParseException](parse(stmt)),
        condition = "SPECIFY_CLUSTER_BY_WITH_DISTRIBUTED_BY_PARTITION_IS_NOT_ALLOWED",
        sqlState = "42908",
        parameters = Map.empty,
        context = ExpectedContext(stmt, 0, stmt.length - 1))
    }

    parse("CREATE TABLE t (id INT, c STRING) USING foo CLUSTERED BY (c) INTO 4 BUCKETS " +
      "DISTRIBUTED BY PARTITION") match {
      case c: CreateTable => assert(c.writeDistributionMode === HASH)
      case other => fail(s"unexpected plan: $other")
    }
  }

  test("CLUSTER BY and the write clauses both reach the catalog") {
    withSQLConf("spark.sql.catalog.testcat" -> classOf[RecordingInMemoryTableCatalog].getName) {
      sql("CREATE TABLE testcat.ordered (a INT, b INT) USING foo CLUSTER BY (a) ORDERED BY (b)")
      sql("CREATE TABLE testcat.local (a INT, b INT) USING foo CLUSTER BY (a) " +
        "LOCALLY ORDERED BY (b)")
      sql("CREATE TABLE testcat.unordered (a INT, b INT) USING foo CLUSTER BY (a) UNORDERED")

      assert(calls("testcat") === Seq(
        WriteSpecCall("createTable", "ordered", RANGE, Seq("b ASC NULLS FIRST")),
        WriteSpecCall("createTable", "local", NONE, Seq("b ASC NULLS FIRST")),
        WriteSpecCall("createTable", "unordered", NONE, Seq.empty)))
      Seq("ordered", "local", "unordered").foreach { name =>
        assert(clusteringOf(loadTable("testcat", name)) === Seq(Seq("a")), name)
      }
    }
  }

  test("a catalog with the capability can still reject a combination it does not support") {
    val catalogClass = classOf[ClusterByUnorderedRejectingCatalog].getName
    withSQLConf("spark.sql.catalog.rejectcat" -> catalogClass) {
      Seq(
        "CREATE TABLE rejectcat.t (a INT, b INT) USING foo CLUSTER BY (a) UNORDERED",
        "CREATE TABLE rejectcat.t USING foo CLUSTER BY (a) UNORDERED AS SELECT 1 AS a, 2 AS b"
      ).foreach { stmt =>
        val e = intercept[IllegalArgumentException](sql(stmt))
        assert(e.getMessage === ClusterByUnorderedRejectingCatalog.MESSAGE, stmt)
        assert(sql("SHOW TABLES IN rejectcat").count() === 0, stmt)
      }

      sql("CREATE TABLE rejectcat.t (a INT, b INT) USING foo CLUSTER BY (a)")
      sql("INSERT INTO rejectcat.t VALUES (1, 2)")
      Seq(
        "REPLACE TABLE rejectcat.t (a INT, b INT, c INT) USING foo CLUSTER BY (a) UNORDERED",
        "CREATE OR REPLACE TABLE rejectcat.t (a INT, b INT, c INT) USING foo CLUSTER BY (a) " +
          "UNORDERED",
        "REPLACE TABLE rejectcat.t USING foo CLUSTER BY (a) UNORDERED " +
          "AS SELECT 3 AS a, 4 AS b, 5 AS c",
        "CREATE OR REPLACE TABLE rejectcat.t USING foo CLUSTER BY (a) UNORDERED " +
          "AS SELECT 3 AS a, 4 AS b, 5 AS c"
      ).foreach { stmt =>
        val e = intercept[IllegalArgumentException](sql(stmt))
        assert(e.getMessage === ClusterByUnorderedRejectingCatalog.MESSAGE, stmt)
        checkAnswer(sql("SELECT * FROM rejectcat.t"), Row(1, 2))
        val table = loadTable("rejectcat", "t")
        assert(table.columns().map(_.name).toSeq === Seq("a", "b"), stmt)
        assert(clusteringOf(table) === Seq(Seq("a")), stmt)
      }
    }
  }

  test("CREATE TABLE / CTAS / REPLACE TABLE / RTAS hand the distribution and ordering " +
    "to the catalog") {
    withSQLConf("spark.sql.catalog.testcat" -> classOf[RecordingInMemoryTableCatalog].getName) {
      sql("CREATE TABLE testcat.t (id INT, c STRING) USING foo PARTITIONED BY (c) " +
        "DISTRIBUTED BY PARTITION ORDERED BY id DESC")
      sql("CREATE TABLE testcat.ctas USING foo ORDERED BY id AS SELECT 1 AS id")
      // a non-staging catalog replaces by dropping and re-creating, so this is createTable as well
      sql("REPLACE TABLE testcat.t (id INT) USING foo LOCALLY ORDERED BY id")
      sql("REPLACE TABLE testcat.t USING foo PARTITIONED BY (c) DISTRIBUTED BY PARTITION " +
        "ORDERED BY id AS SELECT 1 AS id, 'a' AS c")

      assert(calls("testcat") === Seq(
        WriteSpecCall("createTable", "t", HASH, Seq("id DESC NULLS LAST")),
        WriteSpecCall("createTable", "ctas", RANGE, Seq("id ASC NULLS FIRST")),
        WriteSpecCall("createTable", "t", NONE, Seq("id ASC NULLS FIRST")),
        WriteSpecCall("createTable", "t", HASH, Seq("id ASC NULLS FIRST"))))
    }
  }

  test("CTAS and RTAS write their first load with the declared distribution and ordering") {
    withSQLConf(
      "spark.sql.catalog.layoutcat" -> classOf[LayoutEnforcingInMemoryTableCatalog].getName,
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      def innerWrite(stmt: String): SparkPlan = {
        val executions = QueryTest.withQueryExecutionsCaptured(spark)(sql(stmt))
        executions.map(_.executedPlan).collectFirst { case w: V2TableWriteExec => w.query }
          .getOrElse(fail(s"no inner write for $stmt"))
      }

      val global = innerWrite(
        "CREATE TABLE layoutcat.g USING foo ORDERED BY id DESC AS SELECT id FROM range(10)")
      assert(global.collect { case s: ShuffleExchangeExec => s.outputPartitioning }
        .exists(_.isInstanceOf[RangePartitioning]), global.treeString)
      assert(global.collect { case s: SortExec => s }.nonEmpty, global.treeString)

      val local = innerWrite(
        "CREATE TABLE layoutcat.l USING foo LOCALLY ORDERED BY id DESC AS SELECT id FROM range(10)")
      assert(local.collect { case s: ShuffleExchangeExec => s }.isEmpty, local.treeString)
      assert(local.collect { case s: SortExec => s }.nonEmpty, local.treeString)

      val clustered = innerWrite("CREATE OR REPLACE TABLE layoutcat.h USING foo " +
        "PARTITIONED BY (c) DISTRIBUTED BY PARTITION ORDERED BY id " +
        "AS SELECT id, CAST(id % 3 AS STRING) AS c FROM range(10)")
      assert(clustered.collect { case s: ShuffleExchangeExec => s.outputPartitioning }
        .exists(_.isInstanceOf[HashPartitioning]), clustered.treeString)
      assert(clustered.collect { case s: SortExec => s }.nonEmpty, clustered.treeString)
    }
  }

  test("a statement with no write clause carries neither a distribution nor an ordering") {
    withSQLConf("spark.sql.catalog.testcat" -> classOf[RecordingInMemoryTableCatalog].getName) {
      sql("CREATE TABLE testcat.plain (id INT) USING foo")
      sql("CREATE TABLE testcat.plain_ctas USING foo AS SELECT 1 AS id")
      sql("REPLACE TABLE testcat.plain (id INT) USING foo")

      assert(calls("testcat") === Seq(
        WriteSpecCall("createTable", "plain", null, Seq.empty),
        WriteSpecCall("createTable", "plain_ctas", null, Seq.empty),
        WriteSpecCall("createTable", "plain", null, Seq.empty)))
    }
  }

  test("the staging catalog gets them on stageCreate / stageReplace / stageCreateOrReplace") {
    val catalogClass = classOf[RecordingStagingInMemoryTableCatalog].getName
    withSQLConf("spark.sql.catalog.stagingcat" -> catalogClass) {
      sql("CREATE TABLE stagingcat.t USING foo ORDERED BY id AS SELECT 1 AS id")
      sql("REPLACE TABLE stagingcat.t USING foo LOCALLY ORDERED BY id AS SELECT 2 AS id")
      sql("CREATE OR REPLACE TABLE stagingcat.t USING foo UNORDERED AS SELECT 3 AS id")
      sql("REPLACE TABLE stagingcat.t (id INT) USING foo ORDERED BY id DESC NULLS FIRST")
      sql("CREATE OR REPLACE TABLE stagingcat.t (id INT) USING foo LOCALLY ORDERED BY id")
      sql("CREATE TABLE stagingcat.plain USING foo AS SELECT 1 AS id")

      assert(calls("stagingcat") === Seq(
        WriteSpecCall("stageCreate", "t", RANGE, Seq("id ASC NULLS FIRST")),
        WriteSpecCall("stageReplace", "t", NONE, Seq("id ASC NULLS FIRST")),
        WriteSpecCall("stageCreateOrReplace", "t", NONE, Seq.empty),
        WriteSpecCall("stageReplace", "t", RANGE, Seq("id DESC NULLS FIRST")),
        WriteSpecCall("stageCreateOrReplace", "t", NONE, Seq("id ASC NULLS FIRST")),
        WriteSpecCall("stageCreate", "plain", null, Seq.empty)))
    }
  }

  test("a staging catalog sees an unstaged CREATE TABLE through createTable") {
    val catalogClass = classOf[RecordingStagingInMemoryTableCatalog].getName
    withSQLConf("spark.sql.catalog.stagingcat" -> catalogClass) {
      sql("CREATE TABLE stagingcat.t (id INT) USING foo ORDERED BY id")

      assert(calls("stagingcat") ===
        Seq(WriteSpecCall("createTable", "t", RANGE, Seq("id ASC NULLS FIRST"))))
    }
  }

  test("a catalog that does not advertise the capability fails loudly") {
    withSQLConf("spark.sql.catalog.testcat" -> classOf[InMemoryTableCatalog].getName) {
      sql("CREATE TABLE testcat.plain (id INT) USING foo")
      assert(sql("SHOW TABLES IN testcat").count() === 1)

      Seq(
        ("CREATE TABLE testcat.ordered (id INT) USING foo LOCALLY ORDERED BY id",
          "ordered", "CREATE TABLE"),
        ("CREATE TABLE testcat.dist (id INT, c STRING) USING foo PARTITIONED BY (c) " +
          "DISTRIBUTED BY PARTITION", "dist", "CREATE TABLE"),
        ("CREATE TABLE testcat.ctas USING foo ORDERED BY id AS SELECT 1 AS id",
          "ctas", "CREATE TABLE AS SELECT"),
        ("REPLACE TABLE testcat.plain (id INT) USING foo ORDERED BY id",
          "plain", "REPLACE TABLE"),
        ("REPLACE TABLE testcat.plain USING foo ORDERED BY id AS SELECT 1 AS id",
          "plain", "REPLACE TABLE AS SELECT")
      ).foreach { case (stmt, table, operation) =>
        checkError(
          exception = intercept[AnalysisException](sql(stmt)),
          condition = "UNSUPPORTED_FEATURE.TABLE_OPERATION",
          sqlState = "0A000",
          parameters = Map(
            "tableName" -> s"`testcat`.`$table`",
            "operation" -> s"$operation ... DISTRIBUTED BY/ORDERED BY/UNORDERED"))
      }

      assert(sql("SHOW TABLES IN testcat").count() === 1)
    }
  }

  test("UNORDERED is a request too, not a no-op") {
    withSQLConf("spark.sql.catalog.testcat" -> classOf[InMemoryTableCatalog].getName) {
      checkError(
        exception = intercept[AnalysisException] {
          sql("CREATE TABLE testcat.unordered (id INT) USING foo UNORDERED")
        },
        condition = "UNSUPPORTED_FEATURE.TABLE_OPERATION",
        sqlState = "0A000",
        parameters = Map(
          "tableName" -> "`testcat`.`unordered`",
          "operation" -> "CREATE TABLE ... DISTRIBUTED BY/ORDERED BY/UNORDERED"))
      assert(sql("SHOW TABLES IN testcat").count() === 0)
    }
  }

  test("REPLACE TABLE is rejected before the existing table is dropped") {
    withSQLConf("spark.sql.catalog.testcat" -> classOf[InMemoryTableCatalog].getName) {
      sql("CREATE TABLE testcat.t (id INT) USING foo")
      sql("INSERT INTO testcat.t VALUES (1)")

      checkError(
        exception = intercept[AnalysisException] {
          sql("REPLACE TABLE testcat.t (id INT) USING foo ORDERED BY id")
        },
        condition = "UNSUPPORTED_FEATURE.TABLE_OPERATION",
        sqlState = "0A000",
        parameters = Map(
          "tableName" -> "`testcat`.`t`",
          "operation" -> "REPLACE TABLE ... DISTRIBUTED BY/ORDERED BY/UNORDERED"))
      checkAnswer(sql("SELECT * FROM testcat.t"), Row(1))
    }
  }

  test("the v1 session-catalog path also fails loudly instead of dropping the clause") {
    Seq("ORDERED BY id", "LOCALLY ORDERED BY id", "UNORDERED").foreach { clause =>
      withTable("v1_ordered") {
        checkError(
          exception = intercept[AnalysisException] {
            sql(s"CREATE TABLE v1_ordered (id INT) USING parquet $clause")
          },
          condition = "UNSUPPORTED_FEATURE.TABLE_OPERATION",
          sqlState = "0A000",
          parameters = Map(
            "tableName" -> "`spark_catalog`.`default`.`v1_ordered`",
            "operation" -> "CREATE TABLE ... DISTRIBUTED BY/ORDERED BY/UNORDERED"))
      }
    }

    withTable("v1_ctas") {
      checkError(
        exception = intercept[AnalysisException] {
          sql("CREATE TABLE v1_ctas USING parquet ORDERED BY id AS SELECT 1 AS id")
        },
        condition = "UNSUPPORTED_FEATURE.TABLE_OPERATION",
        sqlState = "0A000",
        parameters = Map(
          "tableName" -> "`spark_catalog`.`default`.`v1_ctas`",
          "operation" -> "CREATE TABLE AS SELECT ... DISTRIBUTED BY/ORDERED BY/UNORDERED"))
    }
  }

  test("a DelegatingCatalogExtension does not forward the capability from its delegate") {
    withSQLConf(
      "spark.sql.catalog.delegatingcat" -> classOf[DelegatingWriteSpecCatalog].getName) {
      val catalog = spark.sessionState.catalogManager.catalog("delegatingcat")
        .asInstanceOf[DelegatingWriteSpecCatalog]
      val delegate = catalog.recordingDelegate
      val capability =
        TableCatalogCapability.SUPPORTS_CREATE_TABLE_WITH_WRITE_DISTRIBUTION_AND_ORDERING
      assert(delegate.capabilities.contains(capability))
      assert(
        catalog.capabilities.asScala.toSet === delegate.capabilities.asScala.toSet - capability)
      assert(catalog.capabilities.contains(TableCatalogCapability.SUPPORT_TABLE_CONSTRAINT))

      checkError(
        exception = intercept[AnalysisException] {
          sql("CREATE TABLE delegatingcat.t (id INT) USING foo ORDERED BY id")
        },
        condition = "UNSUPPORTED_FEATURE.TABLE_OPERATION",
        sqlState = "0A000",
        parameters = Map(
          "tableName" -> "`delegatingcat`.`t`",
          "operation" -> "CREATE TABLE ... DISTRIBUTED BY/ORDERED BY/UNORDERED"))
      assert(delegate.recordedCalls.isEmpty)
      assert(sql("SHOW TABLES IN delegatingcat").count() === 0)
    }
  }

  test("CREATE TEMPORARY TABLE ... USING cannot carry the clauses") {
    Seq("ORDERED BY id", "UNORDERED").foreach { clause =>
      val stmt = s"CREATE TEMPORARY TABLE t (id INT) USING parquet $clause"
      checkError(
        exception = intercept[ParseException](parse(stmt)),
        condition = "INVALID_STATEMENT_OR_CLAUSE",
        sqlState = "42601",
        parameters = Map(
          "operation" -> "CREATE TEMPORARY TABLE ... DISTRIBUTED BY/ORDERED BY/UNORDERED"),
        context = ExpectedContext(stmt, 0, stmt.length - 1))
    }
  }

  test("a Table realized from a TableInfo reports the declared default") {
    val writeOrdering = Array(LogicalExpressions.sort(
      FieldReference("id"), SortDirection.DESCENDING, NullOrdering.NULLS_LAST))
    val info = new TableInfo.Builder()
      .withColumns(Array(Column.create("id", IntegerType)))
      .withWriteDistributionMode(HASH)
      .withWriteOrdering(writeOrdering)
      .build()

    val table: Table = new DelegatingTable(info, "t")
    assert(table.writeDistributionMode() === HASH)
    assert(WriteSpecCall.render(table.writeOrdering().toSeq) === Seq("id DESC NULLS LAST"))

    val plain: Table = new DelegatingTable(
      new TableInfo.Builder().withColumns(Array(Column.create("id", IntegerType))).build(), "t")
    assert(plain.writeDistributionMode() === null)
    assert(plain.writeOrdering().isEmpty)
  }

  test("TableInfo.Builder rejects a null write ordering") {
    val e = intercept[NullPointerException] {
      new TableInfo.Builder()
        .withColumns(Array(Column.create("id", IntegerType)))
        .withWriteOrdering(null)
        .build()
    }
    assert(e.getMessage === "writeOrdering should not be null")
  }

  test("SHOW CREATE TABLE keeps the types of literals in a sort key") {
    withSQLConf("spark.sql.catalog.reportcat" -> classOf[ReportingInMemoryTableCatalog].getName) {
      def declared(): (WriteDistributionMode, Seq[SortOrder]) = {
        val table = loadTable("reportcat", "t")
        (table.writeDistributionMode(), table.writeOrdering().toSeq)
      }

      withTable("reportcat.t") {
        sql("CREATE TABLE reportcat.t (id INT) USING foo ORDERED BY " +
          "f(id, DATE '1970-01-01', TIMESTAMP '2020-01-01 10:00:00', TIME '12:00:00', 1.5BD, " +
          "1.5F, 10L, 'x')")
        val before = declared()
        val ddl = sql("SHOW CREATE TABLE reportcat.t").head().getString(0)
        assert(ddl.contains("DATE '1970-01-01'"), ddl)

        sql("DROP TABLE reportcat.t")
        sql(ddl)
        assert(declared() === before)
      }
    }
  }

  test("SHOW CREATE TABLE keeps a timestamp in a sort key across time zones and types") {
    withSQLConf("spark.sql.catalog.reportcat" -> classOf[ReportingInMemoryTableCatalog].getName) {
      withTable("reportcat.t") {
        val ddl = withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Los_Angeles") {
          sql("CREATE TABLE reportcat.t (id INT) USING foo ORDERED BY " +
            "f(id, TIMESTAMP '2020-11-01 01:30:00-08:00')")
          sql("SHOW CREATE TABLE reportcat.t").head().getString(0)
        }
        val before = loadTable("reportcat", "t").writeOrdering().toSeq

        Seq(
          SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Los_Angeles",
          SQLConf.SESSION_LOCAL_TIMEZONE.key -> "Asia/Tokyo",
          SQLConf.TIMESTAMP_TYPE.key -> "TIMESTAMP_NTZ"
        ).foreach { conf =>
          sql("DROP TABLE reportcat.t")
          withSQLConf(conf) {
            sql(ddl)
          }
          assert(loadTable("reportcat", "t").writeOrdering().toSeq === before, s"under $conf")
        }
      }
    }
  }

  test("SHOW CREATE TABLE quotes a connector's own column reference in a sort key") {
    withSQLConf("spark.sql.catalog.reportcat" -> classOf[ReportingInMemoryTableCatalog].getName) {
      withTable("reportcat.t") {
        sql("CREATE TABLE reportcat.t (`order-id` INT) USING foo TBLPROPERTIES (" +
          s"'${ReportingInMemoryTable.MODE_OVERRIDE}' = 'range', " +
          s"'${ReportingInMemoryTable.ORDERING_OVERRIDE}' = 'connectorref')")
        val ddl = sql("SHOW CREATE TABLE reportcat.t").head().getString(0)
        assert(writeClauseLines(ddl) === Seq("ORDERED BY (f(`order-id`) ASC NULLS FIRST)"), ddl)
        val described = sql("DESCRIBE TABLE EXTENDED reportcat.t").collect()
          .map(r => r.getString(0) -> r.getString(1)).toMap
        assert(described.get("Ordering") === Some("f(`order-id`) ASC NULLS FIRST"))

        sql("DROP TABLE reportcat.t")
        sql(ddl)
        assert(orderingOf(parse(ddl)) === Seq("f(`order-id`) ASC NULLS FIRST"))
      }
    }
  }

  test("SHOW CREATE TABLE omits the clauses when the statement would create a v1 table") {
    val catalogClass = classOf[V1ProviderSessionCatalog].getName
    withSQLConf(SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION.key -> catalogClass) {
      spark.sessionState.catalogManager.reset()
      val ddl = sql("SHOW CREATE TABLE spark_catalog.default.t").head().getString(0)
      assert(ddl.split("\n").contains("USING parquet"), ddl)
      assert(writeClauseLines(ddl).isEmpty, ddl)
      val described = sql("DESCRIBE TABLE EXTENDED spark_catalog.default.t").collect()
        .map(r => r.getString(0) -> r.getString(1)).toMap
      assert(described.get("Ordering") === Some("id ASC NULLS FIRST"))
    }
  }

  test("SHOW CREATE TABLE omits the clauses for a catalog that does not accept them") {
    withSQLConf(
        "spark.sql.catalog.reportcat" -> classOf[ReportingInMemoryTableCatalog].getName,
        "spark.sql.catalog.plaincat" -> classOf[NonAcceptingReportingCatalog].getName) {
      Seq("reportcat" -> true, "plaincat" -> false).foreach { case (catalog, emitted) =>
        withTable(s"$catalog.t") {
          sql(s"CREATE TABLE $catalog.t (id INT) USING foo TBLPROPERTIES (" +
            s"'${ReportingInMemoryTable.MODE_OVERRIDE}' = 'range', " +
            s"'${ReportingInMemoryTable.ORDERING_OVERRIDE}' = 'string')")
          val ddl = sql(s"SHOW CREATE TABLE $catalog.t").head().getString(0)
          assert(writeClauseLines(ddl) ===
            (if (emitted) Seq("ORDERED BY (f(id, 'x') ASC NULLS FIRST)") else Seq.empty), ddl)
          val described = sql(s"DESCRIBE TABLE EXTENDED $catalog.t").collect()
            .map(r => r.getString(0) -> r.getString(1)).toMap
          assert(described.get("Ordering") === Some("f(id, 'x') ASC NULLS FIRST"), catalog)

          sql(s"DROP TABLE $catalog.t")
          sql(ddl)
        }
      }
    }
  }

  test("SHOW CREATE TABLE quotes a reserved keyword when the session reserves it") {
    withSQLConf(
        "spark.sql.catalog.reportcat" -> classOf[ReportingInMemoryTableCatalog].getName,
        SQLConf.ANSI_ENABLED.key -> "true",
        SQLConf.ENFORCE_RESERVED_KEYWORDS.key -> "true") {
      withTable("reportcat.t") {
        sql("CREATE TABLE reportcat.t (`order` INT) USING foo ORDERED BY `order`")
        val ddl = sql("SHOW CREATE TABLE reportcat.t").head().getString(0)
        assert(writeClauseLines(ddl) === Seq("ORDERED BY (`order` ASC NULLS FIRST)"), ddl)
      }
    }
  }

  test("SHOW CREATE TABLE quotes a transform name that needs it") {
    withSQLConf("spark.sql.catalog.reportcat" -> classOf[ReportingInMemoryTableCatalog].getName) {
      withTable("reportcat.t") {
        sql("CREATE TABLE reportcat.t (id INT, p INT) USING foo PARTITIONED BY (p) " +
          "DISTRIBUTED BY PARTITION ORDERED BY `z-order`(id), `+`(id, 1)")
        val before = loadTable("reportcat", "t").writeOrdering().toSeq
        val ddl = sql("SHOW CREATE TABLE reportcat.t").head().getString(0)
        assert(writeClauseLines(ddl) === Seq("DISTRIBUTED BY PARTITION ORDERED BY " +
          "(`z-order`(id) ASC NULLS FIRST, `+`(id, 1) ASC NULLS FIRST)"), ddl)

        sql("DROP TABLE reportcat.t")
        sql(ddl)
        assert(loadTable("reportcat", "t").writeOrdering().toSeq === before)
      }
    }
  }

  test("SHOW CREATE TABLE reproduces the clauses and DESCRIBE EXTENDED reports them") {
    withSQLConf("spark.sql.catalog.reportcat" -> classOf[ReportingInMemoryTableCatalog].getName) {
      Seq(
        ("ORDERED BY (id DESC)", "ORDERED BY (id DESC NULLS LAST)"),
        ("LOCALLY ORDERED BY (id)", "LOCALLY ORDERED BY (id ASC NULLS FIRST)"),
        ("UNORDERED", "UNORDERED"),
        ("PARTITIONED BY (c) ORDERED BY (id)", "ORDERED BY (id ASC NULLS FIRST)"),
        ("PARTITIONED BY (c) DISTRIBUTED BY PARTITION", "DISTRIBUTED BY PARTITION"),
        ("PARTITIONED BY (c) DISTRIBUTED BY PARTITION ORDERED BY (id)",
          "DISTRIBUTED BY PARTITION ORDERED BY (id ASC NULLS FIRST)")
      ).foreach { case (clauses, expected) =>
        withTable("reportcat.t") {
          sql(s"CREATE TABLE reportcat.t (id INT, c STRING) USING foo $clauses")
          val ddl = sql("SHOW CREATE TABLE reportcat.t").head().getString(0)
          assert(writeClauseLines(ddl) === Seq(expected), s"for $clauses, got:\n$ddl")
        }
      }

      withTable("reportcat.plain") {
        sql("CREATE TABLE reportcat.plain (id INT) USING foo")
        val ddl = sql("SHOW CREATE TABLE reportcat.plain").head().getString(0)
        assert(writeClauseLines(ddl).isEmpty, ddl)
      }

      withTable("reportcat.t") {
        sql("CREATE TABLE reportcat.t (id INT, c STRING) USING foo " +
          "PARTITIONED BY (c) DISTRIBUTED BY PARTITION ORDERED BY (id DESC)")
        val described = sql("DESCRIBE TABLE EXTENDED reportcat.t").collect()
          .map(r => r.getString(0) -> r.getString(1)).toMap
        assert(described.get("Distribution") === Some("hash"))
        assert(described.get("Ordering") === Some("id DESC NULLS LAST"))
      }
      withTable("reportcat.plain") {
        sql("CREATE TABLE reportcat.plain (id INT) USING foo")
        val described = sql("DESCRIBE TABLE EXTENDED reportcat.plain").collect().map(_.getString(0))
        assert(!described.contains("# Write Distribution and Ordering"))
      }
    }
  }

  test("SHOW CREATE TABLE omits a pair the syntax cannot spell, and stays runnable") {
    withSQLConf("spark.sql.catalog.reportcat" -> classOf[ReportingInMemoryTableCatalog].getName) {
      Seq(
        // (fabricated mode, clauses)
        ("hash", ""),
        ("hash", "ORDERED BY (id)"),
        ("range", "")
      ).foreach { case (mode, clauses) =>
        withTable("reportcat.t") {
          sql(s"CREATE TABLE reportcat.t (id INT) USING foo $clauses " +
            s"TBLPROPERTIES ('${ReportingInMemoryTable.MODE_OVERRIDE}' = '$mode')")
          val ddl = sql("SHOW CREATE TABLE reportcat.t").head().getString(0)
          assert(writeClauseLines(ddl).isEmpty, s"for mode=$mode clauses=[$clauses], got:\n$ddl")

          sql(s"DROP TABLE reportcat.t")
          sql(ddl)
          assert(sql("SHOW CREATE TABLE reportcat.t").head().getString(0) === ddl)
        }
      }

      // With an unspellable ordering, dropping only ORDERED BY would still emit UNORDERED under
      // mode `none`, declaring no ordering on a table that has one.
      Seq(
        ("range", "nested"),
        ("none", "nested"),
        ("range", "nan"),
        ("range", "bucketswapped"),
        ("range", "daystz"),
        ("range", "missing"),
        ("range", "nestedmissing")
      ).foreach { case (mode, key) =>
        withTable("reportcat.t") {
          sql(s"CREATE TABLE reportcat.t (id INT) USING foo TBLPROPERTIES (" +
            s"'${ReportingInMemoryTable.MODE_OVERRIDE}' = '$mode', " +
            s"'${ReportingInMemoryTable.ORDERING_OVERRIDE}' = '$key')")
          val ddl = sql("SHOW CREATE TABLE reportcat.t").head().getString(0)
          assert(writeClauseLines(ddl).isEmpty, s"for mode=$mode key=$key, got:\n$ddl")

          sql(s"DROP TABLE reportcat.t")
          sql(ddl)
          assert(sql("SHOW CREATE TABLE reportcat.t").head().getString(0) === ddl)
        }
      }

      withTable("reportcat.t") {
        sql("CREATE TABLE reportcat.t (id INT) USING foo TBLPROPERTIES (" +
          s"'${ReportingInMemoryTable.MODE_OVERRIDE}' = 'range', " +
          s"'${ReportingInMemoryTable.ORDERING_OVERRIDE}' = 'nan')")
        val described = sql("DESCRIBE TABLE EXTENDED reportcat.t").collect()
          .map(r => r.getString(0) -> r.getString(1)).toMap
        assert(described.get("Distribution") === Some("range"))
        assert(described.get("Ordering") === Some("f(id, CAST('NaN' AS FLOAT)) ASC NULLS FIRST"))
      }
    }
  }

  test("SHOW CREATE TABLE treats a connector's cluster_by transform as clustering") {
    withSQLConf(
      "spark.sql.catalog.reportcat" -> classOf[ReportingInMemoryTableCatalog].getName,
      "spark.sql.catalog.gencat" -> classOf[GenericClusterByTableCatalog].getName) {
      for (catalog <- Seq("reportcat", "gencat"); ordering <- Seq("", "ORDERED BY (b)")) {
        withTable(s"$catalog.t") {
          sql(s"CREATE TABLE $catalog.t (a INT, b INT) USING foo CLUSTER BY (a) $ordering " +
            s"TBLPROPERTIES ('${ReportingInMemoryTable.MODE_OVERRIDE}' = 'hash')")
          val ddl = sql(s"SHOW CREATE TABLE $catalog.t").head().getString(0)
          assert(writeClauseLines(ddl).isEmpty, s"for $catalog [$ordering], got:\n$ddl")
        }
      }

      Seq("PARTITIONED BY (a)", "CLUSTERED BY (a) INTO 4 BUCKETS").foreach { layout =>
        withTable("gencat.t") {
          sql(s"CREATE TABLE gencat.t (a INT, b INT) USING foo $layout " +
            "DISTRIBUTED BY PARTITION ORDERED BY (b)")
          val ddl = sql("SHOW CREATE TABLE gencat.t").head().getString(0)
          assert(writeClauseLines(ddl) ===
            Seq("DISTRIBUTED BY PARTITION ORDERED BY (b ASC NULLS FIRST)"),
            s"for $layout, got:\n$ddl")
        }
      }
    }
  }

  test("the parser and SHOW CREATE TABLE agree that a cluster_by transform is not partitioning") {
    withSQLConf("spark.sql.catalog.gencat" -> classOf[GenericClusterByTableCatalog].getName) {
      val stmt = "CREATE TABLE gencat.t (a INT, b INT) USING foo PARTITIONED BY (cluster_by(a)) " +
        "DISTRIBUTED BY PARTITION"
      checkError(
        exception = intercept[ParseException](sql(stmt)),
        condition = "SPECIFY_DISTRIBUTED_BY_PARTITION_WITHOUT_PARTITIONING_IS_NOT_ALLOWED",
        sqlState = "42908",
        parameters = Map.empty,
        context = ExpectedContext(fragment = stmt, start = 0, stop = stmt.length - 1))

      withTable("gencat.t") {
        sql("CREATE TABLE gencat.t (a INT, b INT) USING foo PARTITIONED BY (cluster_by(a)) " +
          s"TBLPROPERTIES ('${ReportingInMemoryTable.MODE_OVERRIDE}' = 'hash')")
        val ddl = sql("SHOW CREATE TABLE gencat.t").head().getString(0)
        assert(ddl.split("\n").contains("PARTITIONED BY (cluster_by(a))"), ddl)
        assert(writeClauseLines(ddl).isEmpty, ddl)
      }

      withTable("gencat.t") {
        sql("CREATE TABLE gencat.t (a INT, b INT) USING foo PARTITIONED BY (cluster_by(a), b) " +
          "DISTRIBUTED BY PARTITION")
        val ddl = sql("SHOW CREATE TABLE gencat.t").head().getString(0)
        assert(writeClauseLines(ddl) === Seq("DISTRIBUTED BY PARTITION"), ddl)

        sql("DROP TABLE gencat.t")
        sql(ddl)
        assert(sql("SHOW CREATE TABLE gencat.t").head().getString(0) === ddl)
      }
    }
  }

  test("SHOW CREATE TABLE prints a connector's cluster_by transform with a literal argument") {
    withSQLConf("spark.sql.catalog.gencat" -> classOf[GenericClusterByTableCatalog].getName) {
      Seq("", s"TBLPROPERTIES ('${ReportingInMemoryTable.MODE_OVERRIDE}' = 'hash')").foreach {
        props =>
          withTable("gencat.t") {
            sql(s"CREATE TABLE gencat.t (a INT) USING foo PARTITIONED BY (cluster_by(4)) $props")
            val ddl = sql("SHOW CREATE TABLE gencat.t").head().getString(0)
            assert(ddl.split("\n").contains("PARTITIONED BY (cluster_by(4))"), s"[$props]: $ddl")
            assert(writeClauseLines(ddl).isEmpty, s"[$props]: $ddl")
          }
      }
    }
  }

  test("the new keywords stay usable as identifiers") {
    Seq(false, true).foreach { ansi =>
      withSQLConf(
          SQLConf.ANSI_ENABLED.key -> ansi.toString,
          SQLConf.ENFORCE_RESERVED_KEYWORDS.key -> ansi.toString) {
        withTable("ordered", "unordered") {
          sql("CREATE TABLE ordered (distributed INT, locally INT, ordered INT, unordered INT) " +
            "USING parquet")
          sql("INSERT INTO ordered VALUES (1, 2, 3, 4)")
          checkAnswer(
            sql("SELECT distributed, locally, ordered, unordered FROM ordered"),
            Row(1, 2, 3, 4))
          checkAnswer(sql("SELECT unordered.ordered FROM ordered AS unordered"), Row(3))
          sql("CREATE TABLE unordered (ordered INT) USING parquet")
          checkAnswer(sql("SELECT count(*) FROM unordered"), Row(0))
        }
      }
    }
  }
}

/** One recorded catalog call, with the requested write distribution and ordering as text. */
case class WriteSpecCall(
    method: String,
    table: String,
    writeDistributionMode: WriteDistributionMode,
    writeOrdering: Seq[String])

object WriteSpecCall {
  def render(writeOrdering: Seq[SortOrder]): Seq[String] = {
    writeOrdering.map(o => s"${o.expression().describe()} ${o.direction()} ${o.nullOrdering()}")
  }

  def apply(
      method: String,
      ident: Identifier,
      writeDistributionMode: WriteDistributionMode,
      writeOrdering: Array[SortOrder]): WriteSpecCall = {
    WriteSpecCall(method, ident.name, writeDistributionMode, render(writeOrdering.toSeq))
  }
}

/** Adds the create-time write distribution and ordering capability to a catalog's own set. */
object WriteSpecCapability {
  def add(
      capabilities: util.Set[TableCatalogCapability]): util.Set[TableCatalogCapability] = {
    (capabilities.asScala.toSet +
      TableCatalogCapability.SUPPORTS_CREATE_TABLE_WITH_WRITE_DISTRIBUTION_AND_ORDERING).asJava
  }
}

trait RecordsWriteSpecs {
  private val calls = new ArrayBuffer[WriteSpecCall]

  def recordedCalls: Seq[WriteSpecCall] = calls.toSeq

  protected def record(method: String, ident: Identifier, tableInfo: TableInfo): Unit = {
    calls += WriteSpecCall(
      method, ident, tableInfo.writeDistributionMode(), tableInfo.writeOrdering())
  }
}

/** A catalog that supports the create-time write distribution and ordering and records both. */
class RecordingInMemoryTableCatalog extends InMemoryTableCatalog with RecordsWriteSpecs {

  override def capabilities: util.Set[TableCatalogCapability] =
    WriteSpecCapability.add(super.capabilities)

  override def createTable(ident: Identifier, tableInfo: TableInfo): Table = {
    record("createTable", ident, tableInfo)
    super.createTable(ident, tableInfo)
  }
}

/** The same, for a staging catalog, which gets CTAS and every REPLACE TABLE through stage*. */
class RecordingStagingInMemoryTableCatalog
  extends StagingInMemoryTableCatalog with RecordsWriteSpecs {

  override def capabilities: util.Set[TableCatalogCapability] =
    WriteSpecCapability.add(super.capabilities)

  // A staging catalog needs this one too: a CREATE TABLE without AS SELECT is not staged, so it
  // arrives here rather than at stageCreate.
  override def createTable(ident: Identifier, tableInfo: TableInfo): Table = {
    record("createTable", ident, tableInfo)
    super.createTable(ident, tableInfo)
  }

  override def stageCreate(ident: Identifier, tableInfo: TableInfo): StagedTable = {
    record("stageCreate", ident, tableInfo)
    super.stageCreate(ident, tableInfo)
  }

  override def stageReplace(ident: Identifier, tableInfo: TableInfo): StagedTable = {
    record("stageReplace", ident, tableInfo)
    super.stageReplace(ident, tableInfo)
  }

  override def stageCreateOrReplace(ident: Identifier, tableInfo: TableInfo): StagedTable = {
    record("stageCreateOrReplace", ident, tableInfo)
    super.stageCreateOrReplace(ident, tableInfo)
  }
}

/** A DelegatingCatalogExtension over a delegate that reports the capability. */
class DelegatingWriteSpecCatalog extends DelegatingCatalogExtension {

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    val recording = new RecordingInMemoryTableCatalog
    recording.initialize(name, options)
    setDelegateCatalog(recording)
  }

  def recordingDelegate: RecordingInMemoryTableCatalog =
    delegate.asInstanceOf[RecordingInMemoryTableCatalog]
}

/** A catalog whose tables require the declared write distribution and ordering on each write. */
class LayoutEnforcingInMemoryTableCatalog extends InMemoryTableCatalog {

  override def capabilities: util.Set[TableCatalogCapability] =
    WriteSpecCapability.add(super.capabilities)

  override def createTable(ident: Identifier, tableInfo: TableInfo): Table = {
    val distribution = tableInfo.writeDistributionMode() match {
      case RANGE => Distributions.ordered(tableInfo.writeOrdering())
      case HASH => Distributions.clustered(tableInfo.partitions().map(t => t: Expression))
      case _ => Distributions.unspecified()
    }
    createTable(ident, tableInfo.columns(), tableInfo.partitions(), tableInfo.properties(),
      distribution, tableInfo.writeOrdering(), None, None, tableInfo.constraints())
  }
}

/** An in-memory table that reports the declared write distribution and ordering back. */
class ReportingInMemoryTable(tableName: String, tableInfo: TableInfo)
  extends InMemoryTable(
    tableName,
    tableInfo.columns(),
    tableInfo.partitions(),
    tableInfo.properties(),
    tableInfo.constraints()) {

  override def writeDistributionMode(): WriteDistributionMode =
    ReportingInMemoryTable.writeDistributionMode(tableInfo)

  // Reports a sort key the parser cannot produce: `nested` has a transform as an argument; `nan`
  // has a literal with no constant form; `bucketswapped` and `daystz` have an argument shape the
  // parser rejects for their name; `missing` and `nestedmissing` reference a column the table does
  // not have, the second inside an identity transform; `string` has a `java.lang.String` literal;
  // and `connectorref` has a connector's own column reference.
  override def writeOrdering(): Array[SortOrder] = {
    def f(arg: Expression): Transform = LogicalExpressions.apply("f", FieldReference("id"), arg)
    val key = Option(tableInfo.properties().get(ReportingInMemoryTable.ORDERING_OVERRIDE)).map {
      case "nested" =>
        LogicalExpressions.apply("f", LogicalExpressions.apply("g", FieldReference("id")))
      case "nan" => f(literal(Float.NaN))
      case "string" => f(literal("x"))
      case "missing" => FieldReference("missing")
      case "nestedmissing" =>
        LogicalExpressions.apply("f", LogicalExpressions.identity(FieldReference("missing")))
      case "bucketswapped" =>
        LogicalExpressions.apply("bucket", FieldReference("id"), literal(16))
      case "daystz" => LogicalExpressions.apply("days", FieldReference("id"), literal("UTC"))
      case "connectorref" => LogicalExpressions.apply("f", new NamedReference {
        override def fieldNames(): Array[String] = Array("order-id")
      })
    }
    key.map(k =>
      Array(LogicalExpressions.sort(k, SortDirection.ASCENDING, NullOrdering.NULLS_FIRST)))
      .getOrElse(tableInfo.writeOrdering())
  }
}

object ReportingInMemoryTable {
  val MODE_OVERRIDE = "test.write-distribution-mode"
  val ORDERING_OVERRIDE = "test.write-ordering"

  // Reports a mode the syntax cannot request, such as `hash` without partitioning.
  def writeDistributionMode(tableInfo: TableInfo): WriteDistributionMode = {
    Option(tableInfo.properties().get(MODE_OVERRIDE))
      .map(m => WriteDistributionMode.valueOf(m.toUpperCase(Locale.ROOT)))
      .getOrElse(tableInfo.writeDistributionMode())
  }
}

/** A catalog whose tables report the declared write distribution and ordering back. */
class ReportingInMemoryTableCatalog extends InMemoryTableCatalog {

  override def capabilities: util.Set[TableCatalogCapability] =
    WriteSpecCapability.add(super.capabilities)

  protected def newTable(ident: Identifier, tableInfo: TableInfo): Table =
    new ReportingInMemoryTable(s"$name.${ident.name}", tableInfo)

  override def createTable(ident: Identifier, tableInfo: TableInfo): Table = {
    if (tables.containsKey(ident)) {
      throw new TableAlreadyExistsException(ident.asMultipartIdentifier)
    }
    val table = newTable(ident, tableInfo)
    tables.put(ident, table)
    namespaces.putIfAbsent(ident.namespace.toList, Map())
    table
  }
}

/** The same, for a catalog that does not report the capability, so it cannot accept the clauses. */
class NonAcceptingReportingCatalog extends ReportingInMemoryTableCatalog {

  override def capabilities: util.Set[TableCatalogCapability] = (super.capabilities.asScala.toSet -
    TableCatalogCapability.SUPPORTS_CREATE_TABLE_WITH_WRITE_DISTRIBUTION_AND_ORDERING).asJava
}

/**
 * A catalog whose tables report `CLUSTER BY` as a generic `cluster_by` transform, as a connector
 * may, and the mode set with `ReportingInMemoryTable.MODE_OVERRIDE`.
 */
class GenericClusterByTableCatalog extends ReportingInMemoryTableCatalog {

  override protected def newTable(ident: Identifier, tableInfo: TableInfo): Table =
    new DelegatingTable(tableInfo, s"$name.${ident.name}") {
      override def partitioning(): Array[Transform] = super.partitioning().map {
        case c: ClusterByTransform => LogicalExpressions.apply("cluster_by", c.columnNames: _*)
        case other => other
      }

      override def writeDistributionMode(): WriteDistributionMode =
        ReportingInMemoryTable.writeDistributionMode(tableInfo)
    }
}

/**
 * A session catalog extension that reports the capability and loads every table as one with a
 * v1 provider, `parquet`, and a declared range ordering on `id`.
 */
class V1ProviderSessionCatalog extends DelegatingCatalogExtension {

  override def capabilities: util.Set[TableCatalogCapability] =
    WriteSpecCapability.add(super.capabilities)

  override def loadTable(ident: Identifier): Table = {
    val info = new TableInfo.Builder()
      .withColumns(Array(Column.create("id", IntegerType)))
      .withProvider("parquet")
      .withWriteDistributionMode(RANGE)
      .withWriteOrdering(Array(LogicalExpressions.sort(
        FieldReference("id"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST)))
      .build()
    new DelegatingTable(info, ident.name)
  }
}

/** A staging catalog that supports the clauses but rejects `CLUSTER BY` with `UNORDERED`. */
class ClusterByUnorderedRejectingCatalog extends StagingInMemoryTableCatalog {

  override def capabilities: util.Set[TableCatalogCapability] =
    WriteSpecCapability.add(super.capabilities)

  private def check(tableInfo: TableInfo): Unit = {
    val clustered = tableInfo.partitions().exists {
      case ClusterByTransform(_) => true
      case _ => false
    }
    if (clustered && tableInfo.writeDistributionMode() == NONE &&
        tableInfo.writeOrdering().isEmpty) {
      throw new IllegalArgumentException(ClusterByUnorderedRejectingCatalog.MESSAGE)
    }
  }

  override def createTable(ident: Identifier, tableInfo: TableInfo): Table = {
    check(tableInfo)
    super.createTable(ident, tableInfo)
  }

  override def stageCreate(ident: Identifier, tableInfo: TableInfo): StagedTable = {
    check(tableInfo)
    super.stageCreate(ident, tableInfo)
  }

  override def stageReplace(ident: Identifier, tableInfo: TableInfo): StagedTable = {
    check(tableInfo)
    super.stageReplace(ident, tableInfo)
  }

  override def stageCreateOrReplace(ident: Identifier, tableInfo: TableInfo): StagedTable = {
    check(tableInfo)
    super.stageCreateOrReplace(ident, tableInfo)
  }
}

object ClusterByUnorderedRejectingCatalog {
  val MESSAGE = "CLUSTER BY with UNORDERED is not supported"
}
