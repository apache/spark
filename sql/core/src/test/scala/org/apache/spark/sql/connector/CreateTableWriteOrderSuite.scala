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
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import org.apache.spark.SparkUnsupportedOperationException
import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.{NoSuchTableException, TableAlreadyExistsException}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.{CreateTable, CreateTableAsSelect, LogicalPlan, ReplaceTable, ReplaceTableAsSelect, V2CreateTablePlan}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, RangePartitioning}
import org.apache.spark.sql.catalyst.util.WriteDistributionAndOrdering
import org.apache.spark.sql.connector.catalog.{Column, DelegatingCatalogExtension, DelegatingTable, Identifier, InMemoryRowLevelOperationTableCatalog, InMemoryTable, InMemoryTableCatalog, StagedTable, StagingInMemoryTableCatalog, StagingTableCatalog, Table, TableCatalog, TableCatalogCapability, TableInfo, Txn, TxnTableCatalog, WriteDistributionMode}
import org.apache.spark.sql.connector.catalog.CatalogManager.SESSION_CATALOG_NAME
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.connector.catalog.WriteDistributionMode.{HASH, NONE, RANGE}
import org.apache.spark.sql.connector.catalog.transactions.{Transaction, TransactionInfo}
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

  private def checkUnknownColumns(stmt: String, cols: String): Unit = {
    checkError(
      exception = intercept[AnalysisException](analyze(stmt)),
      condition = "WRITE_ORDERING_WITH_UNKNOWN_COLUMN",
      sqlState = "42703",
      parameters = Map("cols" -> cols),
      context = ExpectedContext(stmt, 0, stmt.length - 1))
  }

  private def checkRejected(stmt: String, tableName: String): Unit = {
    checkError(
      exception = intercept[SparkUnsupportedOperationException](sql(stmt)),
      condition = "UNSUPPORTED_FEATURE.TABLE_OPERATION",
      sqlState = "0A000",
      parameters = Map(
        "tableName" -> tableName,
        "operation" -> WriteDistributionAndOrdering.CLAUSES))
  }

  private def assertCreatedInTransaction(catalog: RecordingTransactionalTableCatalog): Unit = {
    assert(catalog.observedTransactions.size === 1)
    assert(catalog.lastTransaction.catalog.txnTables.contains(Identifier.of(Array.empty, "t")))
  }

  private def checkUnsupported(stmt: String, tableName: String, operation: String): Unit = {
    checkError(
      exception = intercept[AnalysisException](sql(stmt)),
      condition = "UNSUPPORTED_FEATURE.TABLE_OPERATION",
      sqlState = "0A000",
      parameters = Map(
        "tableName" -> tableName,
        "operation" -> s"$operation ... DISTRIBUTED BY/ORDERED BY/UNORDERED"))
  }

  private def showCreate(table: String): String = {
    sql(s"SHOW CREATE TABLE $table").head().getString(0)
  }

  private def described(table: String): Map[String, String] = {
    sql(s"DESCRIBE TABLE EXTENDED $table").collect()
      .map(r => r.getString(0) -> r.getString(1)).toMap
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

  test("each combination of the write clauses parses to its distribution and ordering") {
    Seq(
      ("", null, 0),
      ("ORDERED BY id", RANGE, 1),
      ("LOCALLY ORDERED BY id", NONE, 1),
      ("UNORDERED", NONE, 0),
      ("PARTITIONED BY (c) DISTRIBUTED BY PARTITION", HASH, 0),
      ("PARTITIONED BY (c) DISTRIBUTED BY PARTITION ORDERED BY id", HASH, 1),
      ("PARTITIONED BY (c) DISTRIBUTED BY PARTITION LOCALLY ORDERED BY id", HASH, 1),
      ("PARTITIONED BY (c) DISTRIBUTED BY PARTITION UNORDERED", HASH, 0),
      ("ORDERED BY id PARTITIONED BY (c) DISTRIBUTED BY PARTITION", HASH, 1),
      ("DISTRIBUTED BY PARTITION PARTITIONED BY (c) LOCALLY ORDERED BY id", HASH, 1),
      ("PARTITIONED BY (c) ORDERED BY id COMMENT 'a table' DISTRIBUTED BY PARTITION", HASH, 1)
    ).foreach { case (clauses, mode, orderingSize) =>
      parse(s"CREATE TABLE t (id INT, c STRING) USING foo $clauses") match {
        case c: CreateTable =>
          assert(c.writeDistributionMode === mode, s"for [$clauses]")
          assert(c.writeOrdering.length === orderingSize, s"for [$clauses]")
        case other => fail(s"unexpected plan for [$clauses]: $other")
      }
    }
  }

  test("a column written as identity(col) reaches the catalog as a plain column") {
    parse("CREATE TABLE t (id INT) USING foo ORDERED BY identity(id)") match {
      case c: CreateTable =>
        assert(c.writeOrdering.map(_.expression()) === Seq(FieldReference("id")))
      case other => fail(s"unexpected plan: $other")
    }
    withSQLConf("spark.sql.catalog.testcat" -> classOf[RecordingInMemoryTableCatalog].getName) {
      assert(orderingOf(analyze(
        "CREATE TABLE testcat.t (id INT) USING foo ORDERED BY identity(ID)")) ===
        Seq("id ASC NULLS FIRST"))
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
      checkUnknownColumns(truncate, "`id`")

      // references are normalized independently, so only the unresolvable one is reported
      val multi = "CREATE TABLE testcat.t (ID INT) USING foo ORDERED BY bucket(4, id, nope)"
      checkUnknownColumns(multi, "`nope`")
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
      checkUnknownColumns(stmt, "`id`")
    }
  }

  test("a case-insensitive session does not normalize an ApplyTransform's references") {
    withSQLConf("spark.sql.catalog.testcat" -> classOf[RecordingInMemoryTableCatalog].getName) {
      assert(orderingOf(analyze("CREATE TABLE testcat.t (id INT) USING foo ORDERED BY ID")) ===
        Seq("id ASC NULLS FIRST"))

      val ordered = "CREATE TABLE testcat.t (id INT) USING foo ORDERED BY truncate(4, ID)"
      checkUnknownColumns(ordered, "`ID`")

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
        checkUnknownColumns(stmt, cols)
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
        checkUnknownColumns(stmt, "`nope`")
      }

      // a nested struct field resolves
      analyze("CREATE TABLE testcat.t (p STRUCT<x: INT>) USING foo ORDERED BY p.x")

      val throughInt = "CREATE TABLE testcat.t (id INT) USING foo ORDERED BY id.x"
      checkUnknownColumns(throughInt, "`id`.`x`")
    }
  }

  test("unknown ordering columns are reported once each, in declaration order") {
    withSQLConf("spark.sql.catalog.testcat" -> classOf[RecordingInMemoryTableCatalog].getName) {
      val stmt = "CREATE TABLE testcat.t (id INT) USING foo " +
        "ORDERED BY e, d, bucket(4, c, a), id, b, days(f), d"
      checkUnknownColumns(stmt, "`e`, `d`, `c`, `a`, `b`, `f`")
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

  test("a transactional catalog gets them through the catalog of its transaction") {
    val catalogClass = classOf[RecordingTransactionalTableCatalog].getName
    withSQLConf("spark.sql.catalog.txncat" -> catalogClass) {
      sql("CREATE TABLE txncat.t USING foo ORDERED BY id AS SELECT 1 AS id")

      val catalog = spark.sessionState.catalogManager.catalog("txncat")
        .asInstanceOf[RecordingTransactionalTableCatalog]
      assertCreatedInTransaction(catalog)
      assert(calls("txncat") ===
        Seq(WriteSpecCall("createTable", "t", RANGE, Seq("id ASC NULLS FIRST"))))
    }
  }

  test("a transactional catalog is checked through the catalog of its transaction") {
    val catalogClass = classOf[TxnCatalogWithoutCapability].getName
    withSQLConf("spark.sql.catalog.txncat" -> catalogClass) {
      checkUnsupported(
        "CREATE TABLE txncat.t USING foo ORDERED BY id AS SELECT 1 AS id",
        "`txncat`.`t`",
        "CREATE TABLE AS SELECT")
      assert(calls("txncat").isEmpty)
    }
  }

  test("a transactional catalog gets REPLACE TABLE AS SELECT through the catalog of its " +
    "transaction") {
    val catalogClass = classOf[RecordingTransactionalTableCatalog].getName
    withSQLConf("spark.sql.catalog.txncat" -> catalogClass) {
      sql("CREATE TABLE txncat.t (id INT) USING foo")
      sql("REPLACE TABLE txncat.t USING foo ORDERED BY id AS SELECT 1 AS id")

      val catalog = spark.sessionState.catalogManager.catalog("txncat")
        .asInstanceOf[RecordingTransactionalTableCatalog]
      assertCreatedInTransaction(catalog)
      assert(calls("txncat") === Seq(
        WriteSpecCall("createTable", "t", null, Seq.empty),
        WriteSpecCall("createTable", "t", RANGE, Seq("id ASC NULLS FIRST"))))
    }
  }

  test("a transactional catalog is checked through the catalog of its transaction for REPLACE " +
    "TABLE AS SELECT") {
    val catalogClass = classOf[TxnCatalogWithoutCapability].getName
    withSQLConf("spark.sql.catalog.txncat" -> catalogClass) {
      sql("CREATE TABLE txncat.t (id INT) USING foo")
      checkUnsupported(
        "REPLACE TABLE txncat.t USING foo ORDERED BY id AS SELECT 1 AS id",
        "`txncat`.`t`",
        "REPLACE TABLE AS SELECT")
      assert(calls("txncat").size === 1)
      checkAnswer(sql("SELECT * FROM txncat.t"), Seq.empty)
    }
  }

  test("a transactional catalog is called directly when the statement runs without a transaction") {
    val catalogClass = classOf[TxnCatalogWithoutCapability].getName
    withSQLConf("spark.sql.catalog.txncat" -> catalogClass) {
      sql("CREATE TABLE IDENTIFIER('txncat.' || 't') USING foo ORDERED BY id AS SELECT 1 AS id")

      assert(calls("txncat") ===
        Seq(WriteSpecCall("createTable", "t", RANGE, Seq("id ASC NULLS FIRST"))))
    }
  }

  test("a transactional catalog is called directly when EXECUTE IMMEDIATE runs the statement") {
    val catalogClass = classOf[TxnCatalogWithoutCapability].getName
    withSQLConf("spark.sql.catalog.txncat" -> catalogClass) {
      sql("EXECUTE IMMEDIATE 'CREATE TABLE txncat.t USING foo ORDERED BY id AS SELECT 1 AS id'")

      assert(calls("txncat") ===
        Seq(WriteSpecCall("createTable", "t", RANGE, Seq("id ASC NULLS FIRST"))))
    }
  }

  test("a catalog that does not advertise the capability fails before creating or dropping") {
    withSQLConf("spark.sql.catalog.testcat" -> classOf[InMemoryTableCatalog].getName) {
      sql("CREATE TABLE testcat.plain (id INT) USING foo")
      sql("INSERT INTO testcat.plain VALUES (1)")
      assert(sql("SHOW TABLES IN testcat").count() === 1)

      Seq(
        ("CREATE TABLE testcat.ordered (id INT) USING foo LOCALLY ORDERED BY id",
          "ordered", "CREATE TABLE"),
        ("CREATE TABLE testcat.unordered (id INT) USING foo UNORDERED",
          "unordered", "CREATE TABLE"),
        ("CREATE TABLE testcat.dist (id INT, c STRING) USING foo PARTITIONED BY (c) " +
          "DISTRIBUTED BY PARTITION", "dist", "CREATE TABLE"),
        ("CREATE TABLE testcat.ctas USING foo ORDERED BY id AS SELECT 1 AS id",
          "ctas", "CREATE TABLE AS SELECT"),
        ("REPLACE TABLE testcat.plain (id INT) USING foo ORDERED BY id",
          "plain", "REPLACE TABLE"),
        ("REPLACE TABLE testcat.plain USING foo ORDERED BY id AS SELECT 1 AS id",
          "plain", "REPLACE TABLE AS SELECT")
      ).foreach { case (stmt, table, operation) =>
        checkUnsupported(stmt, s"`testcat`.`$table`", operation)
      }

      assert(sql("SHOW TABLES IN testcat").count() === 1)
      checkAnswer(sql("SELECT * FROM testcat.plain"), Row(1))
    }
  }

  test("the v1 session-catalog path also fails loudly instead of dropping the clause") {
    Seq("ORDERED BY id", "LOCALLY ORDERED BY id", "UNORDERED").foreach { clause =>
      withTable("v1_ordered") {
        checkUnsupported(s"CREATE TABLE v1_ordered (id INT) USING parquet $clause",
          "`spark_catalog`.`default`.`v1_ordered`", "CREATE TABLE")
      }
    }

    withTable("v1_ctas") {
      checkUnsupported("CREATE TABLE v1_ctas USING parquet ORDERED BY id AS SELECT 1 AS id",
        "`spark_catalog`.`default`.`v1_ctas`", "CREATE TABLE AS SELECT")
    }
  }

  test("the built-in session catalog and a delegating extension reject a request they get") {
    Seq(classOf[ForwardingSessionCatalog], classOf[SuperCallingSessionCatalog]).foreach { cls =>
      withSQLConf(SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION.key -> cls.getName) {
        spark.sessionState.catalogManager.reset()
        val provider = classOf[FakeV2Provider].getName
        val create = "CREATE TABLE spark_catalog.default.t"
        Seq(
          s"$create (id INT) USING $provider ORDERED BY id",
          s"CREATE OR REPLACE TABLE spark_catalog.default.t (id INT) USING $provider ORDERED BY id",
          s"$create USING $provider ORDERED BY id AS SELECT 1 AS id"
        ).foreach(checkRejected(_, "`spark_catalog`.`default`.`t`"))
      }
    }
  }

  test("the TableInfo defaults reject a request instead of ignoring it") {
    withSQLConf("spark.sql.catalog.defaultcat" -> classOf[DefaultsOnlyStagingCatalog].getName) {
      sql("CREATE TABLE defaultcat.t (id INT) USING foo")
      Seq(
        ("CREATE TABLE defaultcat.u (id INT) USING foo ORDERED BY id", "u"),
        ("CREATE TABLE defaultcat.u (id INT) USING foo UNORDERED", "u"),
        ("CREATE TABLE defaultcat.u (id INT, c INT) USING foo PARTITIONED BY (c) " +
          "DISTRIBUTED BY PARTITION", "u"),
        ("CREATE TABLE defaultcat.u USING foo ORDERED BY id AS SELECT 1 AS id", "u"),
        ("REPLACE TABLE defaultcat.t USING foo ORDERED BY id AS SELECT 2 AS id", "t"),
        ("CREATE OR REPLACE TABLE defaultcat.u (id INT) USING foo ORDERED BY id", "u"),
        ("CREATE OR REPLACE TABLE defaultcat.t USING foo UNORDERED AS SELECT 2 AS id", "t")
      ).foreach { case (stmt, table) => checkRejected(stmt, s"`defaultcat`.`$table`") }

      assert(sql("SHOW TABLES IN defaultcat").count() === 1)
      checkAnswer(sql("SELECT * FROM defaultcat.t"), Seq.empty)
    }
  }

  test("a catalog that leaves createTable to the default loses a replaced table") {
    withSQLConf("spark.sql.catalog.defaultcat" -> classOf[DefaultsOnlyCatalog].getName) {
      sql("CREATE TABLE defaultcat.t (id INT) USING foo")
      checkRejected("REPLACE TABLE defaultcat.t (id INT) USING foo ORDERED BY id",
        "`defaultcat`.`t`")
      assert(sql("SHOW TABLES IN defaultcat").count() === 0)
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

      checkUnsupported("CREATE TABLE delegatingcat.t (id INT) USING foo ORDERED BY id",
        "`delegatingcat`.`t`", "CREATE TABLE")
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

  test("TableInfo.Builder rejects a null write ordering or a null element") {
    val e = intercept[NullPointerException] {
      new TableInfo.Builder()
        .withColumns(Array(Column.create("id", IntegerType)))
        .withWriteOrdering(null)
        .build()
    }
    assert(e.getMessage === "writeOrdering should not be null")

    val element = intercept[NullPointerException] {
      new TableInfo.Builder()
        .withColumns(Array(Column.create("id", IntegerType)))
        .withWriteOrdering(Array[SortOrder](null))
        .build()
    }
    assert(element.getMessage === "writeOrdering should not contain null")
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
        val ddl = showCreate("reportcat.t")
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
          showCreate("reportcat.t")
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
        val ddl = showCreate("reportcat.t")
        assert(writeClauseLines(ddl) === Seq("ORDERED BY (f(`order-id`) ASC NULLS FIRST)"), ddl)
        val rows = described("reportcat.t")
        assert(rows.get("Ordering") === Some("f(`order-id`) ASC NULLS FIRST"))

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
      val ddl = showCreate("spark_catalog.default.t")
      assert(ddl.split("\n").contains("USING parquet"), ddl)
      assert(writeClauseLines(ddl).isEmpty, ddl)
      val rows = described("spark_catalog.default.t")
      assert(rows.get("Ordering") === Some("id ASC NULLS FIRST"))

      checkUnsupported("CREATE TABLE spark_catalog.default.u (id INT) USING parquet ORDERED BY id",
        "`spark_catalog`.`default`.`u`", "CREATE TABLE")
      assert(calls(SESSION_CATALOG_NAME).isEmpty)
    }
  }

  test("SHOW CREATE TABLE does not fail on a provider that cannot be found or linked") {
    Seq(
      classOf[MissingProviderSessionCatalog] -> MissingProviderSessionCatalog.PROVIDER,
      classOf[UnlinkableProviderSessionCatalog] -> classOf[UnlinkableProvider].getName
    ).foreach { case (catalogClass, provider) =>
      withSQLConf(SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION.key -> catalogClass.getName) {
        spark.sessionState.catalogManager.reset()
        val ddl = showCreate("spark_catalog.default.t")
        assert(ddl.split("\n").contains(s"USING $provider"), ddl)
        assert(writeClauseLines(ddl).isEmpty, ddl)
      }
    }
  }

  test("SHOW CREATE TABLE looks up the provider only for clauses that are emitted") {
    val catalogClass = classOf[CountingProviderSessionCatalog].getName
    withSQLConf(SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION.key -> catalogClass) {
      spark.sessionState.catalogManager.reset()
      Seq(
        "plain" -> false,
        "unmoded" -> false,
        "unparsable" -> false,
        "missingcol" -> false,
        "t" -> true
      ).foreach { case (table, looksUp) =>
        val before = CountingProvider.instances.get()
        showCreate(s"spark_catalog.default.$table")
        assert((CountingProvider.instances.get() > before) === looksUp, table)
      }
    }
  }

  test("SHOW CREATE TABLE resolves a missing provider as CREATE TABLE does") {
    val catalogClass = classOf[NoProviderSessionCatalog].getName
    Seq(false -> Seq("ORDERED BY (id ASC NULLS FIRST)"), true -> Seq.empty).foreach {
      case (hiveByDefault, clauses) =>
        withSQLConf(
            SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION.key -> catalogClass,
            SQLConf.DEFAULT_DATA_SOURCE_NAME.key -> classOf[FakeV2Provider].getName,
            SQLConf.LEGACY_CREATE_HIVE_TABLE_BY_DEFAULT.key -> hiveByDefault.toString) {
          spark.sessionState.catalogManager.reset()
          val ddl = showCreate("spark_catalog.default.t")
          assert(!ddl.contains("USING"), ddl)
          assert(writeClauseLines(ddl) === clauses, s"hiveByDefault=$hiveByDefault: $ddl")

          val create = "CREATE TABLE spark_catalog.default.u (id INT) ORDERED BY id"
          if (hiveByDefault) {
            checkUnsupported(create, "`spark_catalog`.`default`.`u`", "CREATE TABLE")
            assert(calls(SESSION_CATALOG_NAME).isEmpty)
          } else {
            sql(create)
            assert(calls(SESSION_CATALOG_NAME) ===
              Seq(WriteSpecCall("createTable", "u", RANGE, Seq("id ASC NULLS FIRST"))))
          }
        }
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
          val ddl = showCreate(s"$catalog.t")
          assert(writeClauseLines(ddl) ===
            (if (emitted) Seq("ORDERED BY (f(id, 'x') ASC NULLS FIRST)") else Seq.empty), ddl)
          val rows = described(s"$catalog.t")
          assert(rows.get("Ordering") === Some("f(id, 'x') ASC NULLS FIRST"), catalog)

          sql(s"DROP TABLE $catalog.t")
          sql(ddl)
        }
      }
    }
  }

  test("SHOW CREATE TABLE quotes a reserved keyword when the session reserves it") {
    val enforced = Seq(
      SQLConf.ANSI_ENABLED.key -> "true",
      SQLConf.ENFORCE_RESERVED_KEYWORDS.key -> "true")
    withSQLConf("spark.sql.catalog.reportcat" -> classOf[ReportingInMemoryTableCatalog].getName) {
      Seq(
        ("(`order` INT) USING foo ORDERED BY `order`",
          "ORDERED BY (`order` ASC NULLS FIRST)", false),
        ("(id INT) USING foo ORDERED BY `select`(id)",
          "ORDERED BY (`select`(`id`) ASC NULLS FIRST)", true)
      ).foreach { case (definition, clause, replaysWhereReserved) =>
        withTable("reportcat.t") {
          val ddl = withSQLConf(enforced: _*) {
            sql(s"CREATE TABLE reportcat.t $definition")
            showCreate("reportcat.t")
          }
          assert(writeClauseLines(ddl) === Seq(clause), ddl)
          val before = loadTable("reportcat", "t").writeOrdering().toSeq

          sql("DROP TABLE reportcat.t")
          if (replaysWhereReserved) withSQLConf(enforced: _*)(sql(ddl)) else sql(ddl)
          assert(loadTable("reportcat", "t").writeOrdering().toSeq === before, ddl)
        }
      }
    }
  }

  test("SHOW CREATE TABLE quotes a transform name that needs it") {
    withSQLConf("spark.sql.catalog.reportcat" -> classOf[ReportingInMemoryTableCatalog].getName) {
      withTable("reportcat.t") {
        sql("CREATE TABLE reportcat.t (id INT, p INT) USING foo PARTITIONED BY (p) " +
          "DISTRIBUTED BY PARTITION ORDERED BY `z-order`(id), `+`(id, 1)")
        val before = loadTable("reportcat", "t").writeOrdering().toSeq
        val ddl = showCreate("reportcat.t")
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
      val missingColumn =
        s"TBLPROPERTIES ('${ReportingInMemoryTable.ORDERING_OVERRIDE}' = 'missing')"
      Seq(
        // (clauses, clauses shown by SHOW CREATE TABLE, Distribution row, Ordering row)
        ("ORDERED BY (id DESC)", Some("ORDERED BY (id DESC NULLS LAST)"),
          Some("range"), Some("id DESC NULLS LAST")),
        ("LOCALLY ORDERED BY (id)", Some("LOCALLY ORDERED BY (id ASC NULLS FIRST)"),
          Some("none"), Some("id ASC NULLS FIRST")),
        ("UNORDERED", Some("UNORDERED"), Some("none"), None),
        ("PARTITIONED BY (c) ORDERED BY (id)", Some("ORDERED BY (id ASC NULLS FIRST)"),
          Some("range"), Some("id ASC NULLS FIRST")),
        ("PARTITIONED BY (c) DISTRIBUTED BY PARTITION", Some("DISTRIBUTED BY PARTITION"),
          Some("hash"), None),
        ("PARTITIONED BY (c) DISTRIBUTED BY PARTITION ORDERED BY (id)",
          Some("DISTRIBUTED BY PARTITION ORDERED BY (id ASC NULLS FIRST)"),
          Some("hash"), Some("id ASC NULLS FIRST")),
        (missingColumn, None, None, Some("missing ASC NULLS FIRST")),
        ("", None, None, None)
      ).foreach { case (clauses, shown, distribution, ordering) =>
        withTable("reportcat.t") {
          sql(s"CREATE TABLE reportcat.t (id INT, c STRING) USING foo $clauses")
          val ddl = showCreate("reportcat.t")
          assert(writeClauseLines(ddl) === shown.toSeq, s"for $clauses, got:\n$ddl")

          val rows = described("reportcat.t")
          assert(rows.contains("# Write Distribution and Ordering") ===
            (distribution.isDefined || ordering.isDefined), clauses)
          assert(rows.get("Distribution") === distribution, clauses)
          assert(rows.get("Ordering") === ordering, clauses)
        }
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
          val ddl = showCreate("reportcat.t")
          assert(writeClauseLines(ddl).isEmpty, s"for mode=$mode clauses=[$clauses], got:\n$ddl")

          sql(s"DROP TABLE reportcat.t")
          sql(ddl)
          assert(showCreate("reportcat.t") === ddl)
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
        ("range", "nestedmissing"),
        ("range", "broken")
      ).foreach { case (mode, key) =>
        withTable("reportcat.t") {
          sql(s"CREATE TABLE reportcat.t (id INT) USING foo TBLPROPERTIES (" +
            s"'${ReportingInMemoryTable.MODE_OVERRIDE}' = '$mode', " +
            s"'${ReportingInMemoryTable.ORDERING_OVERRIDE}' = '$key')")
          val ddl = showCreate("reportcat.t")
          assert(writeClauseLines(ddl).isEmpty, s"for mode=$mode key=$key, got:\n$ddl")

          sql(s"DROP TABLE reportcat.t")
          sql(ddl)
          assert(showCreate("reportcat.t") === ddl)
        }
      }

      Seq(
        "nan" -> "f(id, CAST('NaN' AS FLOAT)) ASC NULLS FIRST",
        "broken" -> "f(id, broken) ASC NULLS FIRST"
      ).foreach { case (key, ordering) =>
        withTable("reportcat.t") {
          sql("CREATE TABLE reportcat.t (id INT) USING foo TBLPROPERTIES (" +
            s"'${ReportingInMemoryTable.MODE_OVERRIDE}' = 'range', " +
            s"'${ReportingInMemoryTable.ORDERING_OVERRIDE}' = '$key')")
          val rows = described("reportcat.t")
          assert(rows.get("Distribution") === Some("range"))
          assert(rows.get("Ordering") === Some(ordering))
        }
      }
    }
  }

  test("a null write ordering array is read as no ordering") {
    withSQLConf(
        "spark.sql.catalog.reportcat" -> classOf[ReportingInMemoryTableCatalog].getName,
        "spark.sql.catalog.plaincat" -> classOf[NonAcceptingReportingCatalog].getName) {
      Seq("reportcat" -> Some("range"), "plaincat" -> None).foreach { case (catalog, mode) =>
        withTable(s"$catalog.t") {
          val modeProperty = mode.map(m => s"'${ReportingInMemoryTable.MODE_OVERRIDE}' = '$m', ")
          sql(s"CREATE TABLE $catalog.t (id INT) USING foo TBLPROPERTIES (" +
            modeProperty.getOrElse("") +
            s"'${ReportingInMemoryTable.ORDERING_OVERRIDE}' = 'nullarray')")

          assert(writeClauseLines(showCreate(s"$catalog.t")).isEmpty, catalog)
          val rows = described(s"$catalog.t")
          assert(rows.get("Distribution") === mode, catalog)
          assert(!rows.contains("Ordering"), catalog)
        }
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
          val ddl = showCreate(s"$catalog.t")
          assert(writeClauseLines(ddl).isEmpty, s"for $catalog [$ordering], got:\n$ddl")
        }
      }

      Seq("PARTITIONED BY (a)", "CLUSTERED BY (a) INTO 4 BUCKETS").foreach { layout =>
        withTable("gencat.t") {
          sql(s"CREATE TABLE gencat.t (a INT, b INT) USING foo $layout " +
            "DISTRIBUTED BY PARTITION ORDERED BY (b)")
          val ddl = showCreate("gencat.t")
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
        val ddl = showCreate("gencat.t")
        assert(ddl.split("\n").contains("PARTITIONED BY (cluster_by(a))"), ddl)
        assert(writeClauseLines(ddl).isEmpty, ddl)
      }

      withTable("gencat.t") {
        sql("CREATE TABLE gencat.t (a INT, b INT) USING foo PARTITIONED BY (cluster_by(a), b) " +
          "DISTRIBUTED BY PARTITION")
        val ddl = showCreate("gencat.t")
        assert(writeClauseLines(ddl) === Seq("DISTRIBUTED BY PARTITION"), ddl)

        sql("DROP TABLE gencat.t")
        sql(ddl)
        assert(showCreate("gencat.t") === ddl)
      }
    }
  }

  test("SHOW CREATE TABLE prints a connector's cluster_by transform with a literal argument") {
    withSQLConf("spark.sql.catalog.gencat" -> classOf[GenericClusterByTableCatalog].getName) {
      Seq("", s"TBLPROPERTIES ('${ReportingInMemoryTable.MODE_OVERRIDE}' = 'hash')").foreach {
        props =>
          withTable("gencat.t") {
            sql(s"CREATE TABLE gencat.t (a INT) USING foo PARTITIONED BY (cluster_by(4)) $props")
            val ddl = showCreate("gencat.t")
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

/** Adds or removes the create-time write distribution and ordering capability of a set. */
object WriteSpecCapability {
  private val capability =
    TableCatalogCapability.SUPPORTS_CREATE_TABLE_WITH_WRITE_DISTRIBUTION_AND_ORDERING

  def add(
      capabilities: util.Set[TableCatalogCapability]): util.Set[TableCatalogCapability] = {
    (capabilities.asScala.toSet + capability).asJava
  }

  def remove(
      capabilities: util.Set[TableCatalogCapability]): util.Set[TableCatalogCapability] = {
    (capabilities.asScala.toSet - capability).asJava
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

/**
 * A staging catalog that supports the clauses and routes each `TableInfo` overload to the interface
 * default. The mixins repeat what it already is, so that `super[X]` can name them.
 */
class DefaultsOnlyStagingCatalog
  extends StagingInMemoryTableCatalog with StagingTableCatalog with TableCatalog {

  override def capabilities: util.Set[TableCatalogCapability] =
    WriteSpecCapability.add(super.capabilities)

  override def createTable(ident: Identifier, tableInfo: TableInfo): Table =
    super[TableCatalog].createTable(ident, tableInfo)

  override def stageCreate(ident: Identifier, tableInfo: TableInfo): StagedTable =
    super[StagingTableCatalog].stageCreate(ident, tableInfo)

  override def stageReplace(ident: Identifier, tableInfo: TableInfo): StagedTable =
    super[StagingTableCatalog].stageReplace(ident, tableInfo)

  override def stageCreateOrReplace(ident: Identifier, tableInfo: TableInfo): StagedTable =
    super[StagingTableCatalog].stageCreateOrReplace(ident, tableInfo)
}

/** The same for a catalog that is not a `StagingTableCatalog`. */
class DefaultsOnlyCatalog extends InMemoryTableCatalog with TableCatalog {

  override def capabilities: util.Set[TableCatalogCapability] =
    WriteSpecCapability.add(super.capabilities)

  override def createTable(ident: Identifier, tableInfo: TableInfo): Table =
    super[TableCatalog].createTable(ident, tableInfo)
}

/**
 * A transactional catalog that supports the clauses and records them. Its transaction's catalog
 * reports the same capabilities and creates tables through this catalog's `createTable`.
 */
class RecordingTransactionalTableCatalog
  extends InMemoryRowLevelOperationTableCatalog with RecordsWriteSpecs {

  override def capabilities: util.Set[TableCatalogCapability] =
    WriteSpecCapability.add(super.capabilities)

  override def createTable(ident: Identifier, tableInfo: TableInfo): Table = {
    record("createTable", ident, tableInfo)
    super.createTable(ident, tableInfo)
  }
}

/**
 * The same, but the catalog of its transaction does not report the capability, so only a check
 * made through that catalog rejects a request.
 */
class TxnCatalogWithoutCapability extends RecordingTransactionalTableCatalog {

  override def beginTransaction(info: TransactionInfo): Transaction = {
    val txn = new Txn(new TxnTableCatalog(this) {
      override def capabilities: util.Set[TableCatalogCapability] =
        WriteSpecCapability.remove(super.capabilities)
    })
    transaction = txn
    txn
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
  // `connectorref` has a connector's own column reference; `broken` has one that throws; and
  // `nullarray` makes the whole array null.
  override def writeOrdering(): Array[SortOrder] = {
    def f(arg: Expression): Transform = LogicalExpressions.apply("f", FieldReference("id"), arg)
    val overrideKey = Option(tableInfo.properties().get(ReportingInMemoryTable.ORDERING_OVERRIDE))
    if (overrideKey.contains("nullarray")) {
      null
    } else {
      overrideKey.map {
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
        case "broken" => f(new NamedReference {
          override def fieldNames(): Array[String] = throw new IllegalStateException("broken")
          override def describe(): String = "broken"
        })
      }.map(k =>
        Array(LogicalExpressions.sort(k, SortDirection.ASCENDING, NullOrdering.NULLS_FIRST)))
        .getOrElse(tableInfo.writeOrdering())
    }
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

  override def capabilities: util.Set[TableCatalogCapability] =
    WriteSpecCapability.remove(super.capabilities)
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
 * A session catalog extension that reports the capability and records the request it receives.
 * It loads five tables with a v1 provider, `parquet`: `t` declares a range ordering on `id`,
 * `plain` declares nothing, `unmoded` declares an ordering without a distribution mode,
 * `unparsable` declares a key that has no clause form because it nests a transform in another,
 * and `missingcol` declares an ordering on a column that the table does not have.
 */
class V1ProviderSessionCatalog extends DelegatingCatalogExtension with RecordsWriteSpecs {

  protected def provider: Option[String] = Some("parquet")

  override def capabilities: util.Set[TableCatalogCapability] =
    WriteSpecCapability.add(super.capabilities)

  override def createTable(ident: Identifier, tableInfo: TableInfo): Table = {
    record("createTable", ident, tableInfo)
    new DelegatingTable(tableInfo, ident.name)
  }

  override def tableExists(ident: Identifier): Boolean = {
    Set("t", "plain", "unmoded", "unparsable", "missingcol").contains(ident.name)
  }

  override def loadTable(ident: Identifier): Table = {
    if (!tableExists(ident)) throw new NoSuchTableException(ident)
    val builder = new TableInfo.Builder()
    provider.foreach(builder.withProvider)
    builder.withColumns(Array(Column.create("id", IntegerType)))
    val ordering = Array(LogicalExpressions.sort(
      FieldReference("id"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST))
    ident.name match {
      case "t" => builder.withWriteDistributionMode(RANGE).withWriteOrdering(ordering)
      case "unmoded" => builder.withWriteOrdering(ordering)
      case "missingcol" =>
        builder.withWriteDistributionMode(RANGE).withWriteOrdering(Array(LogicalExpressions.sort(
          FieldReference("missing"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST)))
      case "unparsable" =>
        builder.withWriteDistributionMode(RANGE).withWriteOrdering(Array(LogicalExpressions.sort(
          LogicalExpressions.apply("f", LogicalExpressions.apply("g", FieldReference("id"))),
          SortDirection.ASCENDING, NullOrdering.NULLS_FIRST)))
      case _ =>
    }
    new DelegatingTable(builder.build(), ident.name)
  }
}

/** The same session catalog, for tables whose provider does not exist. */
class MissingProviderSessionCatalog extends V1ProviderSessionCatalog {
  override protected def provider: Option[String] = Some(MissingProviderSessionCatalog.PROVIDER)
}

object MissingProviderSessionCatalog {
  val PROVIDER = "org.example.NoSuchProvider"
}

/** A provider that cannot be linked, as when a class it depends on is missing. */
class UnlinkableProvider {
  // scalastyle:off throwerror
  throw new NoClassDefFoundError("org/example/MissingSuperclass")
  // scalastyle:on throwerror
}

/** The same session catalog, for tables whose provider cannot be linked. */
class UnlinkableProviderSessionCatalog extends V1ProviderSessionCatalog {
  override protected def provider: Option[String] = Some(classOf[UnlinkableProvider].getName)
}

/** A provider that counts how many times it is instantiated, as a provider lookup does. */
class CountingProvider {
  CountingProvider.instances.incrementAndGet()
}

object CountingProvider {
  val instances = new AtomicInteger(0)
}

/** The same session catalog, for tables whose provider is a [[CountingProvider]]. */
class CountingProviderSessionCatalog extends V1ProviderSessionCatalog {
  override protected def provider: Option[String] = Some(classOf[CountingProvider].getName)
}

/** The same, for tables that report no provider. */
class NoProviderSessionCatalog extends V1ProviderSessionCatalog {
  override protected def provider: Option[String] = None
}

/**
 * A session catalog extension that reports the capability and passes a request on to the built-in
 * session catalog, as an extension that routes a table by its provider does.
 */
class ForwardingSessionCatalog extends DelegatingCatalogExtension {

  override def capabilities: util.Set[TableCatalogCapability] =
    WriteSpecCapability.add(super.capabilities)

  override def createTable(ident: Identifier, tableInfo: TableInfo): Table =
    delegate.asTableCatalog.createTable(ident, tableInfo)
}

/** A session catalog extension that reports the capability and calls `super.createTable`. */
class SuperCallingSessionCatalog extends DelegatingCatalogExtension {

  override def capabilities: util.Set[TableCatalogCapability] =
    WriteSpecCapability.add(super.capabilities)

  override def createTable(ident: Identifier, tableInfo: TableInfo): Table =
    super.createTable(ident, tableInfo)
}

/** A staging catalog that supports the clauses but rejects `CLUSTER BY` with `UNORDERED`. */
class ClusterByUnorderedRejectingCatalog extends RecordingStagingInMemoryTableCatalog {

  override protected def record(method: String, ident: Identifier, tableInfo: TableInfo): Unit = {
    val clustered = tableInfo.partitions().exists {
      case ClusterByTransform(_) => true
      case _ => false
    }
    if (clustered && tableInfo.writeDistributionMode() == NONE &&
        tableInfo.writeOrdering().isEmpty) {
      throw new IllegalArgumentException(ClusterByUnorderedRejectingCatalog.MESSAGE)
    }
    super.record(method, ident, tableInfo)
  }
}

object ClusterByUnorderedRejectingCatalog {
  val MESSAGE = "CLUSTER BY with UNORDERED is not supported"
}
