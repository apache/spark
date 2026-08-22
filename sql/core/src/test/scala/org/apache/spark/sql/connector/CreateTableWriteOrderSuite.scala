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

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.{CreateTable, CreateTableAsSelect, LogicalPlan, ReplaceTable, ReplaceTableAsSelect, V2CreateTablePlan}
import org.apache.spark.sql.connector.catalog.{Column, DelegatingTable, Identifier, InMemoryTable, InMemoryTableCatalog, StagedTable, StagingInMemoryTableCatalog, Table, TableCatalogCapability, TableInfo}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.connector.expressions.{FieldReference, IdentityTransform, LogicalExpressions, NullOrdering, SortDirection, SortOrder}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.IntegerType

/**
 * Tests the create-time write distribution and ordering clauses: CREATE/REPLACE TABLE ...
 * DISTRIBUTED BY PARTITION ... [LOCALLY] ORDERED BY ... | UNORDERED.
 *
 * These cover the Spark side only -- what the parser produces, what reaches the catalog, and how a
 * catalog that has not advertised support for the clauses is rejected. The end-to-end coverage
 * against a catalog that does implement them lives in Iceberg's TestDistributedAndOrderedTables.
 */
class CreateTableWriteOrderSuite extends QueryTest with SharedSparkSession {

  // The catalog manager caches a catalog instance under its name for the whole session, so without
  // this a later test would get an earlier test's catalog -- with its tables and its recordings.
  override def afterEach(): Unit = {
    spark.sessionState.catalogManager.reset()
    super.afterEach()
  }

  private def parse(sql: String): LogicalPlan = spark.sessionState.sqlParser.parsePlan(sql)

  /** Analyzes without running the command, so the normalization rules can be inspected. */
  private def analyze(sql: String): LogicalPlan =
    spark.sessionState.executePlan(parse(sql)).analyzed

  private def orderingOf(plan: LogicalPlan): Seq[String] = plan match {
    case create: V2CreateTablePlan => WriteSpecCall.render(create.writeOrdering)
    case other => fail(s"unexpected plan: $other")
  }

  private def calls(catalogName: String): Seq[WriteSpecCall] =
    spark.sessionState.catalogManager.catalog(catalogName)
      .asInstanceOf[RecordsWriteSpecs].recordedCalls

  test("parse DISTRIBUTED BY PARTITION / ORDERED BY on CREATE TABLE") {
    parse("CREATE TABLE t (id INT, c STRING) USING foo PARTITIONED BY (c) " +
      "DISTRIBUTED BY PARTITION ORDERED BY id ASC NULLS FIRST") match {
      case c: CreateTable =>
        assert(c.writeDistributionMode === "hash")
        assert(c.writeOrdering.length === 1)
        val order = c.writeOrdering.head
        assert(order.expression().isInstanceOf[IdentityTransform])
        assert(order.direction() === SortDirection.ASCENDING)
        assert(order.nullOrdering() === NullOrdering.NULLS_FIRST)
      case other => fail(s"unexpected plan: $other")
    }
  }

  test("bare ORDERED BY implies range, LOCALLY implies none, UNORDERED implies none") {
    Seq(
      ("ORDERED BY id", "range", 1),
      ("LOCALLY ORDERED BY id", "none", 1),
      ("UNORDERED", "none", 0)
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
    // The ordering clause only implies a distribution when DISTRIBUTED BY PARTITION is absent, so
    // beside it LOCALLY has no effect and UNORDERED contributes only "no sort order" -- which is
    // the whole point of allowing that combination: cluster each write by partition, unsorted.
    Seq(
      ("ORDERED BY id", 1),
      ("LOCALLY ORDERED BY id", 1),
      ("UNORDERED", 0)
    ).foreach { case (clause, expectedOrderingSize) =>
      parse(s"CREATE TABLE t (id INT, c STRING) USING foo PARTITIONED BY (c) " +
        s"DISTRIBUTED BY PARTITION $clause") match {
        case c: CreateTable =>
          assert(c.writeDistributionMode === "hash", s"for $clause")
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
        assert(c.writeDistributionMode === "range" && c.writeOrdering.length == 1)
      case other => fail(s"unexpected plan: $other")
    }
    parse("REPLACE TABLE t (id INT) USING foo ORDERED BY id DESC NULLS LAST") match {
      case r: ReplaceTable =>
        assert(r.writeDistributionMode === "range")
        assert(r.writeOrdering.head.direction() === SortDirection.DESCENDING)
        assert(r.writeOrdering.head.nullOrdering() === NullOrdering.NULLS_LAST)
      case other => fail(s"unexpected plan: $other")
    }
    parse("REPLACE TABLE t USING foo UNORDERED AS SELECT 1 AS id") match {
      case r: ReplaceTableAsSelect =>
        assert(r.writeDistributionMode === "none" && r.writeOrdering.isEmpty)
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
    // Iceberg's own ALTER TABLE ... WRITE ORDERED BY accepts both, so accept both here too.
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
    // createTableClauses is an order-insensitive loop, so the two write clauses are independent
    // members of it rather than one combined clause -- otherwise anything between them would count
    // as a duplicate.
    Seq(
      "ORDERED BY id PARTITIONED BY (c) DISTRIBUTED BY PARTITION",
      "DISTRIBUTED BY PARTITION PARTITIONED BY (c) LOCALLY ORDERED BY id",
      "PARTITIONED BY (c) ORDERED BY id COMMENT 'a table' DISTRIBUTED BY PARTITION"
    ).foreach { clauses =>
      parse(s"CREATE TABLE t (id INT, c STRING) USING foo $clauses") match {
        case c: CreateTable =>
          assert(c.writeDistributionMode === "hash", s"for $clauses")
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
      val e = intercept[ParseException] {
        parse(s"CREATE TABLE t (id INT, c STRING) USING foo PARTITIONED BY (c) $clauses")
      }
      assert(e.getCondition === "DUPLICATE_CLAUSES", s"for $clauses")
      assert(e.getMessageParameters.get("clauseName") === clauseName, s"for $clauses")
    }
  }

  test("DISTRIBUTED BY PARTITION requires a partitioned table") {
    // The check sits in the parser, so it has to be wired up on all four statement forms.
    Seq(
      "CREATE TABLE t (id INT) USING foo DISTRIBUTED BY PARTITION",
      "CREATE TABLE t USING foo DISTRIBUTED BY PARTITION AS SELECT 1 AS id",
      "REPLACE TABLE t (id INT) USING foo DISTRIBUTED BY PARTITION",
      "REPLACE TABLE t USING foo DISTRIBUTED BY PARTITION AS SELECT 1 AS id"
    ).foreach { stmt =>
      val e = intercept[ParseException](parse(stmt))
      assert(e.getCondition ===
        "SPECIFY_DISTRIBUTED_BY_PARTITION_WITHOUT_PARTITIONING_IS_NOT_ALLOWED", s"for $stmt")
    }
  }

  test("ordering references are normalized to the schema's spelling") {
    // The parser cannot know the schema, so ORDERED BY id on a column `ID` arrives as `id`.
    // PreprocessTableCreation rewrites it, exactly as it does for the partitioning -- a connector
    // that stores the ordering by name would otherwise record a name its own schema does not have.
    withSQLConf("spark.sql.catalog.testcat" -> classOf[RecordingInMemoryTableCatalog].getName) {
      assert(orderingOf(analyze("CREATE TABLE testcat.t (ID INT, TS TIMESTAMP) USING foo " +
        "ORDERED BY id, days(ts) DESC")) === Seq("ID ASC NULLS FIRST", "days(TS) DESC NULLS LAST"))

      // and on the CTAS path, where the schema comes from the query
      assert(orderingOf(analyze(
        "CREATE TABLE testcat.t USING foo ORDERED BY id AS SELECT 1 AS ID")) ===
        Seq("ID ASC NULLS FIRST"))

      // A transform Spark does not model is an ApplyTransform, which is not rewritable, so nothing
      // can fix its case. Rather than hand the connector a name its schema does not have, the
      // reference has to match exactly -- the same rule the partitioning check applies.
      assert(orderingOf(analyze("CREATE TABLE testcat.t (ID INT) USING foo " +
        "ORDERED BY truncate(4, ID)")) === Seq("truncate(4, ID) ASC NULLS FIRST"))
      val e = intercept[AnalysisException] {
        analyze("CREATE TABLE testcat.t (ID INT) USING foo ORDERED BY truncate(4, id)")
      }
      assert(e.getCondition === "UNSUPPORTED_FEATURE.WRITE_ORDERING_WITH_UNKNOWN_COLUMN")
      assert(e.getMessageParameters.get("cols") === "`id`")

      // and normalization is per reference, not per transform: one unresolvable reference must not
      // stop its siblings being rewritten, or they would be reported as missing along with it
      val multi = intercept[AnalysisException] {
        analyze("CREATE TABLE testcat.t (ID INT) USING foo ORDERED BY bucket(4, id, nope)")
      }
      assert(multi.getMessageParameters.get("cols") === "`nope`")
    }
  }

  test("REPLACE TABLE and RTAS normalize the ordering too") {
    // Each of the four plans carries its own copy of the ordering and its own withWriteOrdering, so
    // covering the CREATE pair above says nothing about the REPLACE pair.
    withSQLConf("spark.sql.catalog.testcat" -> classOf[RecordingInMemoryTableCatalog].getName) {
      assert(orderingOf(analyze("REPLACE TABLE testcat.t (ID INT) USING foo ORDERED BY id")) ===
        Seq("ID ASC NULLS FIRST"))
      assert(orderingOf(analyze(
        "REPLACE TABLE testcat.t USING foo ORDERED BY id AS SELECT 1 AS ID")) ===
        Seq("ID ASC NULLS FIRST"))
    }
  }

  test("a case-sensitive session resolves the ordering case-sensitively") {
    // Both the normalization guard and the CheckAnalysis check consult the conf's resolver, so the
    // strict mode needs its own case: what normalization fixes up above has to be rejected here.
    withSQLConf(
      "spark.sql.catalog.testcat" -> classOf[RecordingInMemoryTableCatalog].getName,
      SQLConf.CASE_SENSITIVE.key -> "true") {
      assert(orderingOf(analyze("CREATE TABLE testcat.t (ID INT) USING foo ORDERED BY ID")) ===
        Seq("ID ASC NULLS FIRST"))
      val e = intercept[AnalysisException] {
        analyze("CREATE TABLE testcat.t (ID INT) USING foo ORDERED BY id")
      }
      assert(e.getCondition === "UNSUPPORTED_FEATURE.WRITE_ORDERING_WITH_UNKNOWN_COLUMN")
      assert(e.getMessageParameters.get("cols") === "`id`")
    }
  }

  test("ORDERED BY needs a schema to resolve against") {
    // With no column list and no query there is nothing to resolve the sort keys against, so say
    // that rather than blaming each key for being absent from a schema that does not exist. Same
    // position PARTITIONED BY takes on a schemaless CREATE TABLE.
    withSQLConf("spark.sql.catalog.testcat" -> classOf[RecordingInMemoryTableCatalog].getName) {
      Seq("ORDERED BY id", "LOCALLY ORDERED BY id").foreach { clause =>
        val e = intercept[AnalysisException] {
          analyze(s"CREATE TABLE testcat.t USING foo $clause")
        }
        assert(e.getCondition === "SPECIFY_WRITE_ORDERING_IS_NOT_ALLOWED", s"for $clause")
      }

      // UNORDERED asks for no ordering at all, so it has nothing to resolve and stays allowed
      analyze("CREATE TABLE testcat.t USING foo UNORDERED")
    }
  }

  test("an ordering on an unknown column is rejected during analysis") {
    withSQLConf("spark.sql.catalog.testcat" -> classOf[RecordingInMemoryTableCatalog].getName) {
      // CheckAnalysis validates the ordering's references the same way it validates the
      // partitioning's, so this holds for every transform, not just the rewritable ones that
      // PreprocessTableCreation normalizes, and regardless of whether the schema is defined.
      Seq(
        "CREATE TABLE testcat.t (id INT) USING foo ORDERED BY nope",
        "CREATE TABLE testcat.t (id INT) USING foo ORDERED BY days(nope)",
        // truncate() is an ApplyTransform, i.e. NOT a RewritableTransform: this is the case that
        // PreprocessTableCreation's normalization cannot see at all
        "CREATE TABLE testcat.t (id INT) USING foo ORDERED BY truncate(4, nope)",
        // and on the CTAS path, where the schema comes from the query
        "CREATE TABLE testcat.t USING foo ORDERED BY nope AS SELECT 1 AS id"
      ).foreach { stmt =>
        val e = intercept[AnalysisException](analyze(stmt))
        assert(e.getCondition === "UNSUPPORTED_FEATURE.WRITE_ORDERING_WITH_UNKNOWN_COLUMN", stmt)
        assert(e.getMessageParameters.get("cols") === "`nope`", stmt)
      }

      // a nested struct field is fine, though -- a reference through a map or array key fails
      // its own way, with INVALID_FIELD_NAME, rather than reaching this condition
      analyze("CREATE TABLE testcat.t (p STRUCT<x: INT>) USING foo ORDERED BY p.x")
    }
  }

  test("a repeated ordering column is accepted, unlike a repeated partition column") {
    // Reusing SchemaUtils.checkTransformDuplication here would have rejected two bucket transforms
    // of different widths, which are legitimately different sort keys. And a repeated sort key is
    // redundant rather than contradictory, so nothing needs rejecting.
    withSQLConf("spark.sql.catalog.testcat" -> classOf[RecordingInMemoryTableCatalog].getName) {
      assert(orderingOf(analyze("CREATE TABLE testcat.t (id INT) USING foo " +
        "ORDERED BY bucket(4, id), bucket(8, id)")) ===
        Seq("bucket(4, id) ASC NULLS FIRST", "bucket(8, id) ASC NULLS FIRST"))
      analyze("CREATE TABLE testcat.t (id INT) USING foo ORDERED BY id, id DESC")
      analyze("CREATE TABLE testcat.t (ts TIMESTAMP) USING foo ORDERED BY days(ts), hours(ts)")
    }
  }

  test("DISTRIBUTED BY PARTITION is not satisfied by CLUSTER BY") {
    // CLUSTER BY lands in `partitioning` as a ClusterByTransform, but it carries clustering columns
    // for the connector to interpret rather than a partition spec, so there is nothing to
    // distribute by. Bucketing does count -- a bucket transform is a real partition transform.
    val e = intercept[ParseException] {
      parse("CREATE TABLE t (id INT, c STRING) USING foo CLUSTER BY (c) DISTRIBUTED BY PARTITION")
    }
    assert(e.getCondition ===
      "SPECIFY_DISTRIBUTED_BY_PARTITION_WITHOUT_PARTITIONING_IS_NOT_ALLOWED")

    parse("CREATE TABLE t (id INT, c STRING) USING foo CLUSTERED BY (c) INTO 4 BUCKETS " +
      "DISTRIBUTED BY PARTITION") match {
      case c: CreateTable => assert(c.writeDistributionMode === "hash")
      case other => fail(s"unexpected plan: $other")
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
      // RTAS builds its TableInfo in an exec of its own, so it needs its own case
      sql("REPLACE TABLE testcat.t USING foo PARTITIONED BY (c) DISTRIBUTED BY PARTITION " +
        "ORDERED BY id AS SELECT 1 AS id, 'a' AS c")

      assert(calls("testcat") === Seq(
        WriteSpecCall("createTable", "t", "hash", Seq("id DESC NULLS LAST")),
        WriteSpecCall("createTable", "ctas", "range", Seq("id ASC NULLS FIRST")),
        WriteSpecCall("createTable", "t", "none", Seq("id ASC NULLS FIRST")),
        WriteSpecCall("createTable", "t", "hash", Seq("id ASC NULLS FIRST"))))
    }
  }

  test("a statement with no write clause carries neither a distribution nor an ordering") {
    // The TableInfo a plain statement builds leaves both unset, so a catalog can tell "the user
    // said nothing" from "the user asked for none" without Spark guessing.
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
    // Iceberg's SparkCatalog is a StagingTableCatalog, so an atomic CTAS goes through stageCreate
    // rather than createTable. That is a separate set of default methods, each with its own
    // argument list to get right.
    val catalogClass = classOf[RecordingStagingInMemoryTableCatalog].getName
    withSQLConf("spark.sql.catalog.stagingcat" -> catalogClass) {
      sql("CREATE TABLE stagingcat.t USING foo ORDERED BY id AS SELECT 1 AS id")
      sql("REPLACE TABLE stagingcat.t USING foo LOCALLY ORDERED BY id AS SELECT 2 AS id")
      sql("CREATE OR REPLACE TABLE stagingcat.t USING foo UNORDERED AS SELECT 3 AS id")
      sql("REPLACE TABLE stagingcat.t (id INT) USING foo ORDERED BY id DESC NULLS FIRST")
      sql("CREATE OR REPLACE TABLE stagingcat.t (id INT) USING foo LOCALLY ORDERED BY id")
      // a plain statement reaches the same method, carrying neither value
      sql("CREATE TABLE stagingcat.plain USING foo AS SELECT 1 AS id")

      assert(calls("stagingcat") === Seq(
        WriteSpecCall("stageCreate", "t", "range", Seq("id ASC NULLS FIRST")),
        WriteSpecCall("stageReplace", "t", "none", Seq("id ASC NULLS FIRST")),
        WriteSpecCall("stageCreateOrReplace", "t", "none", Seq.empty),
        WriteSpecCall("stageReplace", "t", "range", Seq("id DESC NULLS FIRST")),
        WriteSpecCall("stageCreateOrReplace", "t", "none", Seq("id ASC NULLS FIRST")),
        WriteSpecCall("stageCreate", "plain", null, Seq.empty)))
    }
  }

  test("a staging catalog sees an unstaged CREATE TABLE through createTable") {
    // A CREATE TABLE with an explicit column list is not staged: it goes through CreateTableExec,
    // so a staging catalog has to read the request from createTable as well as the three stage*
    // methods. StagingTableCatalog's javadoc says so; this pins it.
    val catalogClass = classOf[RecordingStagingInMemoryTableCatalog].getName
    withSQLConf("spark.sql.catalog.stagingcat" -> catalogClass) {
      sql("CREATE TABLE stagingcat.t (id INT) USING foo ORDERED BY id")

      assert(calls("stagingcat") ===
        Seq(WriteSpecCall("createTable", "t", "range", Seq("id ASC NULLS FIRST"))))
    }
  }

  test("a catalog that does not advertise the capability fails loudly") {
    withSQLConf("spark.sql.catalog.testcat" -> classOf[InMemoryTableCatalog].getName) {
      // plain CREATE TABLE keeps working -- no regression for catalogs that ignore the new args
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
          parameters = Map(
            "tableName" -> s"`testcat`.`$table`",
            "operation" -> s"$operation ... DISTRIBUTED BY/ORDERED BY"))
      }

      // nothing was created, and the pre-existing table is untouched
      assert(sql("SHOW TABLES IN testcat").count() === 1)
    }
  }

  test("UNORDERED is a request too, not a no-op") {
    // A catalog's own default may well be a distribution; only the catalog knows. So asking for
    // none is asking for something, and a catalog that cannot record it must say so rather than
    // hand back a table that still uses its default.
    withSQLConf("spark.sql.catalog.testcat" -> classOf[InMemoryTableCatalog].getName) {
      checkError(
        exception = intercept[AnalysisException] {
          sql("CREATE TABLE testcat.unordered (id INT) USING foo UNORDERED")
        },
        condition = "UNSUPPORTED_FEATURE.TABLE_OPERATION",
        parameters = Map(
          "tableName" -> "`testcat`.`unordered`",
          "operation" -> "CREATE TABLE ... DISTRIBUTED BY/ORDERED BY"))
      assert(sql("SHOW TABLES IN testcat").count() === 0)
    }
  }

  test("REPLACE TABLE is rejected before the existing table is dropped") {
    // ReplaceTableExec drops and re-creates, so a check inside createTable would come too late:
    // the table would already be gone. The capability check runs while planning instead.
    withSQLConf("spark.sql.catalog.testcat" -> classOf[InMemoryTableCatalog].getName) {
      sql("CREATE TABLE testcat.t (id INT) USING foo")
      sql("INSERT INTO testcat.t VALUES (1)")

      val e = intercept[AnalysisException] {
        sql("REPLACE TABLE testcat.t (id INT) USING foo ORDERED BY id")
      }
      assert(e.getCondition === "UNSUPPORTED_FEATURE.TABLE_OPERATION")
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
          parameters = Map(
            "tableName" -> "`spark_catalog`.`default`.`v1_ordered`",
            "operation" -> "CREATE TABLE ... DISTRIBUTED BY/ORDERED BY"))
      }
    }

    // the CTAS branch of the v1 conversion is a separate call site
    withTable("v1_ctas") {
      checkError(
        exception = intercept[AnalysisException] {
          sql("CREATE TABLE v1_ctas USING parquet ORDERED BY id AS SELECT 1 AS id")
        },
        condition = "UNSUPPORTED_FEATURE.TABLE_OPERATION",
        parameters = Map(
          "tableName" -> "`spark_catalog`.`default`.`v1_ctas`",
          "operation" -> "CREATE TABLE AS SELECT ... DISTRIBUTED BY/ORDERED BY"))
    }
  }

  test("CREATE TEMPORARY TABLE ... USING cannot carry the clauses") {
    // This builds a temp view, which has nowhere to record either, so it must not silently drop
    // the clause.
    Seq("ORDERED BY id", "UNORDERED").foreach { clause =>
      val e = intercept[ParseException] {
        parse(s"CREATE TEMPORARY TABLE t (id INT) USING parquet $clause")
      }
      assert(e.getCondition === "INVALID_STATEMENT_OR_CLAUSE", s"for $clause")
      assert(e.getMessageParameters.get("operation") ===
        "CREATE TEMPORARY TABLE ... DISTRIBUTED BY/ORDERED BY", s"for $clause")
    }
  }

  test("a Table realized from a TableInfo reports the declared default") {
    // The accessors are a declared default for later writes -- not a claim about existing data.
    // DelegatingTable is how Spark realizes a TableInfo whose catalog has no Table of its own, so
    // it has to surface them the same way it surfaces columns, partitioning and constraints.
    val writeOrdering = Array(LogicalExpressions.sort(
      FieldReference("id"), SortDirection.DESCENDING, NullOrdering.NULLS_LAST))
    val info = new TableInfo.Builder()
      .withColumns(Array(Column.create("id", IntegerType)))
      .withWriteDistributionMode(TableInfo.DISTRIBUTION_MODE_HASH)
      .withWriteOrdering(writeOrdering)
      .build()

    val table: Table = new DelegatingTable(info, "t")
    assert(table.writeDistributionMode() === TableInfo.DISTRIBUTION_MODE_HASH)
    assert(WriteSpecCall.render(table.writeOrdering().toSeq) === Seq("id DESC NULLS LAST"))

    // and a table that declares nothing keeps the defaults, so an unaware catalog is unaffected
    val plain: Table = new DelegatingTable(
      new TableInfo.Builder().withColumns(Array(Column.create("id", IntegerType))).build(), "t")
    assert(plain.writeDistributionMode() === null)
    assert(plain.writeOrdering().isEmpty)
  }

  test("SHOW CREATE TABLE reproduces the clauses and DESCRIBE EXTENDED reports them") {
    // A table the clauses cannot be recovered from is a table you cannot recreate, so the accessors
    // have to reach both display paths -- the same two `constraints()` already reaches.
    withSQLConf("spark.sql.catalog.reportcat" -> classOf[ReportingInMemoryTableCatalog].getName) {
      Seq(
        ("ORDERED BY (id DESC)", "ORDERED BY (id DESC NULLS LAST)"),
        ("LOCALLY ORDERED BY (id)", "LOCALLY ORDERED BY (id ASC NULLS FIRST)"),
        ("UNORDERED", "UNORDERED"),
        ("PARTITIONED BY (c) DISTRIBUTED BY PARTITION", "DISTRIBUTED BY PARTITION"),
        ("PARTITIONED BY (c) DISTRIBUTED BY PARTITION ORDERED BY (id)",
          "DISTRIBUTED BY PARTITION ORDERED BY (id ASC NULLS FIRST)")
      ).foreach { case (clauses, expected) =>
        withTable("reportcat.t") {
          sql(s"CREATE TABLE reportcat.t (id INT, c STRING) USING foo $clauses")
          val ddl = sql("SHOW CREATE TABLE reportcat.t").head().getString(0)
          assert(ddl.contains(expected), s"for $clauses, got:\n$ddl")
        }
      }

      // and a table that declares nothing gains no clause
      withTable("reportcat.plain") {
        sql("CREATE TABLE reportcat.plain (id INT) USING foo")
        val ddl = sql("SHOW CREATE TABLE reportcat.plain").head().getString(0)
        assert(!ddl.contains("ORDERED BY") && !ddl.contains("DISTRIBUTED BY"), ddl)
      }

      // DESCRIBE reports the values verbatim, so it covers the pairs SHOW CREATE TABLE cannot spell
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
    // A connector can report combinations no statement could have asked for -- `hash` without any
    // partitioning is the dangerous one, because DISTRIBUTED BY PARTITION does not parse there, so
    // emitting it would break the whole statement rather than just lose a clause.
    withSQLConf("spark.sql.catalog.reportcat" -> classOf[ReportingInMemoryTableCatalog].getName) {
      Seq(
        // (fabricated mode, clauses, what must not appear)
        ("hash", "", "DISTRIBUTED BY"),
        ("hash", "ORDERED BY (id)", "DISTRIBUTED BY"),
        ("range", "", "ORDERED BY"),
        ("zigzag", "", "ORDERED BY")
      ).foreach { case (mode, clauses, absent) =>
        withTable("reportcat.t") {
          sql(s"CREATE TABLE reportcat.t (id INT) USING foo $clauses " +
            s"TBLPROPERTIES ('${ReportingInMemoryTable.MODE_OVERRIDE}' = '$mode')")
          val ddl = sql("SHOW CREATE TABLE reportcat.t").head().getString(0)
          assert(!ddl.contains(absent), s"for mode=$mode clauses=[$clauses], got:\n$ddl")

          // and what it does emit has to parse -- otherwise the table cannot be recreated at all
          sql(s"DROP TABLE reportcat.t")
          sql(ddl)
          assert(sql("SHOW CREATE TABLE reportcat.t").head().getString(0) === ddl)
        }
      }

      // DESCRIBE still reports the value, so an omitted clause is not a hidden one
      withTable("reportcat.t") {
        sql("CREATE TABLE reportcat.t (id INT) USING foo " +
          s"TBLPROPERTIES ('${ReportingInMemoryTable.MODE_OVERRIDE}' = 'range')")
        val described = sql("DESCRIBE TABLE EXTENDED reportcat.t").collect()
          .map(r => r.getString(0) -> r.getString(1)).toMap
        assert(described.get("Distribution") === Some("range"))
      }
    }
  }

  test("the new keywords stay usable as identifiers") {
    // The four keywords added for these clauses (DISTRIBUTED, LOCALLY, ORDERED, UNORDERED) are
    // non-reserved, so tables and columns may still be named after them. This is the actual risk of
    // adding keywords to the shared lexer, so pin it down -- in ANSI mode too, since that is where
    // the reserved/non-reserved split actually bites and the four are added to `ansiNonReserved`.
    Seq(false, true).foreach { ansi =>
      withSQLConf(SQLConf.ANSI_ENABLED.key -> ansi.toString) {
        withTable("ordered", "unordered") {
          sql("CREATE TABLE ordered (distributed INT, locally INT, ordered INT, unordered INT) " +
            "USING parquet")
          sql("INSERT INTO ordered VALUES (1, 2, 3, 4)")
          checkAnswer(
            sql("SELECT distributed, locally, ordered, unordered FROM ordered"),
            Row(1, 2, 3, 4))
          // also as a table name, an alias and a qualified reference
          checkAnswer(sql("SELECT unordered.ordered FROM ordered AS unordered"), Row(3))
          sql("CREATE TABLE unordered (ordered INT) USING parquet")
          checkAnswer(sql("SELECT count(*) FROM unordered"), Row(0))
        }
      }
    }
  }
}

/**
 * One catalog call, with the requested write distribution and ordering rendered as text. An
 * in-memory table has nowhere to store either, so recording the calls is the only way to check
 * that the values arrive at the connector at all, and in the right argument positions.
 */
case class WriteSpecCall(
    method: String,
    table: String,
    writeDistributionMode: String,
    writeOrdering: Seq[String])

object WriteSpecCall {
  def render(writeOrdering: Seq[SortOrder]): Seq[String] = {
    writeOrdering.map(o => s"${o.expression().describe()} ${o.direction()} ${o.nullOrdering()}")
  }

  def apply(
      method: String,
      ident: Identifier,
      writeDistributionMode: String,
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

/** The same, for the staging path that an atomic CTAS/RTAS takes. */
class RecordingStagingInMemoryTableCatalog
  extends StagingInMemoryTableCatalog with RecordsWriteSpecs {

  override def capabilities: util.Set[TableCatalogCapability] =
    WriteSpecCapability.add(super.capabilities)

  // A staging catalog needs this one too: a CREATE/REPLACE TABLE with an explicit column list is
  // not staged, so it arrives here rather than at any of the stage* methods.
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
 * An in-memory table that reports the declared write distribution and ordering back, the way a
 * connector that stores them does. The plain InMemoryTable drops everything a TableInfo carries
 * beyond columns, partitioning and constraints, so nothing in-tree can exercise the display paths.
 */
class ReportingInMemoryTable(tableName: String, tableInfo: TableInfo)
  extends InMemoryTable(
    tableName,
    tableInfo.columns(),
    tableInfo.partitions(),
    tableInfo.properties(),
    tableInfo.constraints()) {

  // A (mode, ordering) pair the syntax cannot express -- `hash` with no partitioning, say -- can
  // still reach Spark from a connector, and this property is the only way a test can build one,
  // since every route through the parser is closed by design.
  override def writeDistributionMode(): String = {
    Option(tableInfo.properties().get(ReportingInMemoryTable.MODE_OVERRIDE))
      .getOrElse(tableInfo.writeDistributionMode())
  }

  override def writeOrdering(): Array[SortOrder] = tableInfo.writeOrdering()
}

object ReportingInMemoryTable {
  val MODE_OVERRIDE = "test.write-distribution-mode"
}

/**
 * A catalog whose tables report the declared write distribution and ordering back.
 *
 * NOTE for whoever adds `ALTER TABLE` support: `InMemoryTableCatalog.alterTable` rebuilds a plain
 * `InMemoryTable`, so an ALTER through this fixture silently drops the declared layout. That is the
 * fixture's limitation, not Spark's -- override `alterTable` here before writing such a test.
 */
class ReportingInMemoryTableCatalog extends InMemoryTableCatalog {

  override def capabilities: util.Set[TableCatalogCapability] =
    WriteSpecCapability.add(super.capabilities)

  override def createTable(ident: Identifier, tableInfo: TableInfo): Table = {
    // Same bookkeeping the overridden method does, so a test sees the real catalog's behaviour.
    if (tables.containsKey(ident)) {
      throw new TableAlreadyExistsException(ident.asMultipartIdentifier)
    }
    val table = new ReportingInMemoryTable(s"$name.${ident.name}", tableInfo)
    tables.put(ident, table)
    namespaces.putIfAbsent(ident.namespace.toList, Map())
    table
  }
}
