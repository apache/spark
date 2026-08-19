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

package org.apache.spark.sql.pipelines.graph

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.functions
import org.apache.spark.sql.pipelines.autocdc.{
  ColumnSelection,
  Scd2BatchProcessor,
  ScdType,
  UnqualifiedColumnName
}
import org.apache.spark.sql.pipelines.utils.{ExecutionTest, TestGraphRegistrationContext}
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Tests covering the durability of the SCD Type 2 AutoCDC auxiliary table across pipeline runs:
 * the per-key history recorded in the auxiliary table must persist between incremental runs, and
 * the auxiliary table must be transparently recreated if it is deleted out-of-band. The SCD2
 * analog of [[AutoCdcScd1AuxiliaryTableDurabilitySuite]]. Unlike SCD1, the SCD2 auxiliary table's
 * schema is the full target row schema plus the aux-only deleted-by-batch-id marker, so the
 * schema-layout assertions differ accordingly.
 */
class AutoCdcScd2AuxiliaryTableDurabilitySuite
    extends ExecutionTest
    with SharedSparkSession
    with AutoCdcGraphExecutionTestMixin {

  import testImplicits._

  /** The SCD2 target's `_cdc_metadata` struct value for a given recordStartAt. */
  private def scd2Meta(recordStartAt: Long): Row = Row(recordStartAt)

  test("a higher-sequence event in a later pipeline run correctly closes and opens records") {
    spark.sql(
      s"CREATE TABLE $catalog.$namespace.target " +
      s"(id INT NOT NULL, name STRING, version BIGINT NOT NULL, $scd2MetadataDdl)"
    )

    // Single MemoryStream reused across both pipeline runs so the streaming checkpoint can
    // resume cleanly.
    val changeDataFeedStream = MemoryStream[(Int, String, Long)]
    def buildGraphRegistrationContext(): TestGraphRegistrationContext =
      singleAutoCdcFlowPipeline(
        flowName = "auto_cdc_flow",
        target = "target",
        sourceDf = changeDataFeedStream.toDF().toDF("id", "name", "version"),
        keys = Seq("id"),
        sequencing = functions.col("version"),
        scdType = ScdType.Type2)

    // Run #1: insert id=1 at seq=1.
    changeDataFeedStream.addData((1, "alice", 1L))
    runPipeline(buildGraphRegistrationContext())
    checkAnswer(
      spark.table(s"$catalog.$namespace.target"),
      Seq(Row(1, "alice", 1L, 1L, null, scd2Meta(1L)))
    )

    // Run #2: upsert id=1 at seq=2 (closes the seq=1 record, opens a new one) and insert id=2 at
    // seq=1 (new key). The auxiliary table from run #1 persists and supplies the prior history.
    changeDataFeedStream.addData((1, "alice2", 2L), (2, "bob", 1L))
    runPipeline(buildGraphRegistrationContext())
    checkAnswer(
      spark.table(s"$catalog.$namespace.target"),
      Seq(
        Row(1, "alice", 1L, 1L, 2L, scd2Meta(1L)),
        Row(1, "alice2", 2L, 2L, null, scd2Meta(2L)),
        Row(2, "bob", 1L, 1L, null, scd2Meta(1L))
      )
    )
  }

  test("an event with a sequence lower than what was applied in a prior pipeline run " +
    "is woven in as a closed prior record") {
    spark.sql(
      s"CREATE TABLE $catalog.$namespace.target " +
      s"(id INT NOT NULL, name STRING, version BIGINT NOT NULL, $scd2MetadataDdl)"
    )

    // Single MemoryStream reused across both runs so the streaming checkpoint can resume.
    val stream = MemoryStream[(Int, String, Long)]
    def buildCtx(): TestGraphRegistrationContext =
      singleAutoCdcFlowPipeline(
        flowName = "auto_cdc_flow",
        target = "target",
        sourceDf = stream.toDF().toDF("id", "name", "version"),
        keys = Seq("id"),
        sequencing = functions.col("version"),
        scdType = ScdType.Type2)

    // Run #1: upsert id=1 at seq=10. Auxiliary table records the open record at seq=10.
    stream.addData((1, "alice", 10L))
    runPipeline(buildCtx())
    checkAnswer(
      spark.table(s"$catalog.$namespace.target"),
      Seq(Row(1, "alice", 10L, 10L, null, scd2Meta(10L)))
    )

    // Run #2: late upsert at seq=5 (< the persisted seq=10). Unlike SCD1, SCD2 does not suppress
    // it: the aux history lets reconciliation weave it in as a closed prior record ending at 10,
    // while the seq=10 record stays open.
    stream.addData((1, "early", 5L))
    runPipeline(buildCtx())
    checkAnswer(
      spark.table(s"$catalog.$namespace.target"),
      Seq(
        Row(1, "early", 5L, 5L, 10L, scd2Meta(5L)),
        Row(1, "alice", 10L, 10L, null, scd2Meta(10L))
      )
    )
  }

  test("the SCD2 auxiliary table schema is the full target row schema plus the " +
    "deleted-by-batch-id marker, and records the key columns property") {
    // Source DF column order is (name, id, version): the AutoCDC key column `id` does NOT appear
    // first in the source DF. The SCD2 auxiliary table mirrors the full target row schema (all
    // user + framework columns) with the aux-only deleted-by-batch-id marker appended, and records
    // the key columns in the key-column-names property.
    spark.sql(
      s"CREATE TABLE $catalog.$namespace.target " +
      s"(name STRING, id INT NOT NULL, version BIGINT NOT NULL, $scd2MetadataDdl)"
    )

    val stream = MemoryStream[(String, Int, Long)]
    stream.addData(("alice", 1, 1L))
    runPipeline(singleAutoCdcFlowPipeline(
      flowName = "auto_cdc_flow",
      target = "target",
      sourceDf = stream.toDF().toDF("name", "id", "version"),
      keys = Seq("id"),
      sequencing = functions.col("version"),
      scdType = ScdType.Type2))

    val targetSchema = spark.table(s"$catalog.$namespace.target").schema.fieldNames.toSeq
    val auxSchema = spark.table(auxTableNameFor("target")).schema.fieldNames.toSeq
    // The aux schema is the full target row schema with the marker appended.
    assert(auxSchema == targetSchema :+ Scd2BatchProcessor.deletedByBatchIdColName)
    assert(getAuxTableKeyColumnNames(target = "target") == Seq("id"))
  }

  test("the auxiliary table preserves the user's declared key order in the key-columns " +
    "property, independent of the source DataFrame and target table column orders") {
    // The user declares `keys = Seq("region", "id")` -- the OPPOSITE order from how those columns
    // appear in both the source DF and the target. The recorded key-column-names property should
    // honor the user's declared key order so subsequent runs compare keys against the same layout.
    spark.sql(
      s"CREATE TABLE $catalog.$namespace.target " +
      s"(value STRING, id INT NOT NULL, region STRING NOT NULL, " +
      s"version BIGINT NOT NULL, $scd2MetadataDdl)"
    )

    val stream = MemoryStream[(String, Int, String, Long)]
    stream.addData(("v", 1, "us", 1L))
    runPipeline(singleAutoCdcFlowPipeline(
      flowName = "auto_cdc_flow",
      target = "target",
      sourceDf = stream.toDF().toDF("value", "id", "region", "version"),
      keys = Seq("region", "id"),
      sequencing = functions.col("version"),
      scdType = ScdType.Type2))

    assert(getAuxTableKeyColumnNames(target = "target") == Seq("region", "id"))
  }

  test("a dry run resolves and validates the graph without provisioning the auxiliary " +
    "table") {
    spark.sql(
      s"CREATE TABLE $catalog.$namespace.target " +
      s"(id INT NOT NULL, version BIGINT NOT NULL, $scd2MetadataDdl)"
    )

    val stream = MemoryStream[(Int, Long)]
    stream.addData((1, 1L))
    val ctx = singleAutoCdcFlowPipeline(
      flowName = "auto_cdc_flow",
      target = "target",
      sourceDf = stream.toDF().toDF("id", "version"),
      keys = Seq("id"),
      sequencing = functions.col("version"),
      scdType = ScdType.Type2)

    val updateCtx = TestPipelineUpdateContext(spark, ctx.toDataflowGraph, storageRoot)
    updateCtx.pipelineExecution.dryRunPipeline()

    assert(!spark.catalog.tableExists(auxTableNameFor("target")))
  }

  test("if the SCD2 AutoCDC auxiliary table is dropped between runs, it is transparently " +
    "recreated") {
    spark.sql(
      s"CREATE TABLE $catalog.$namespace.target " +
      s"(id INT NOT NULL, version BIGINT NOT NULL, $scd2MetadataDdl)"
    )

    // Single MemoryStream reused across both runs so the streaming checkpoint can resume.
    val stream = MemoryStream[(Int, Long)]
    def buildCtx(): TestGraphRegistrationContext =
      singleAutoCdcFlowPipeline(
        flowName = "auto_cdc_flow",
        target = "target",
        sourceDf = stream.toDF().toDF("id", "version"),
        keys = Seq("id"),
        sequencing = functions.col("version"),
        scdType = ScdType.Type2)

    stream.addData((1, 1L))
    runPipeline(buildCtx())
    assert(spark.catalog.tableExists(auxTableNameFor("target")))

    // Manually drop the auxiliary table.
    spark.sql(s"DROP TABLE ${auxTableNameFor("target")}")
    assert(!spark.catalog.tableExists(auxTableNameFor("target")))

    stream.addData((1, 2L))
    runPipeline(buildCtx())

    // The dropped auxiliary table must be transparently recreated. Here the seq=1 record also
    // lives in the target as a visible row, and SCD2 reconciliation reads affected rows from the
    // target as well as the aux table, so this particular history survives the drop: the seq=2
    // event still closes the seq=1 record and opens a new one. (This is NOT a general guarantee
    // that the aux table is disposable -- state the aux holds that is NOT mirrored in the target,
    // e.g. a tombstone from a delete-only run, is lost on a drop; see the aux-sole-holder test
    // below.)
    assert(spark.catalog.tableExists(auxTableNameFor("target")))
    checkAnswer(
      spark.table(s"$catalog.$namespace.target"),
      Seq(
        Row(1, 1L, 1L, 2L, scd2Meta(1L)),
        Row(1, 2L, 2L, null, scd2Meta(2L))
      )
    )
  }

  test("the auxiliary table durably holds state absent from the target: a tombstone from a " +
    "delete-only run closes a later lower-sequence upsert") {
    // Unlike the transparently-recreated test above (where the surviving state also lived in the
    // target as a visible row), here the auxiliary table is the SOLE holder of the state. A
    // delete-only first run leaves the target empty but records a tombstone at seq=10 in the aux;
    // the durability of THAT aux-only row is what lets a later, lower-sequence upsert land as a
    // closed prior record. Drop the aux and this history is gone (the upsert would instead open a
    // current record) -- which is exactly why the aux is not disposable.
    spark.sql(
      s"CREATE TABLE $catalog.$namespace.target " +
      s"(id INT NOT NULL, name STRING, version BIGINT NOT NULL, $scd2MetadataDdl)"
    )

    // Single MemoryStream reused across both runs so the streaming checkpoint can resume.
    val stream = MemoryStream[(Int, String, Long, Boolean)]
    def buildCtx(): TestGraphRegistrationContext =
      singleAutoCdcFlowPipeline(
        flowName = "auto_cdc_flow",
        target = "target",
        sourceDf = stream.toDF().toDF("id", "name", "version", "is_delete"),
        keys = Seq("id"),
        sequencing = functions.col("version"),
        deleteCondition = Some(functions.col("is_delete") === true),
        columnSelection = Some(ColumnSelection.ExcludeColumns(
          Seq(UnqualifiedColumnName("is_delete"))
        )),
        scdType = ScdType.Type2)

    // Run #1: a delete at seq=10. The target stays empty; the aux records a tombstone at seq=10.
    stream.addData((1, "alice", 10L, true))
    runPipeline(buildCtx())
    checkAnswer(spark.table(s"$catalog.$namespace.target"), Seq.empty)
    // The tombstone lives only in the aux, as a live row (its deleted-by-batch-id marker is null;
    // a non-null marker is what flags a row logically deleted), with no matching visible target
    // row.
    assert(spark.table(auxTableNameFor("target")).count() == 1,
      "the delete-only run should record exactly one aux tombstone row")

    // Run #2 (aux retained): a later upsert at seq=5, BELOW the recorded seq=10. Because the aux
    // still holds the seq=10 tombstone, reconciliation weaves seq=5 in as a closed prior record
    // ending at 10 rather than an open current record -- state the target alone could not supply.
    stream.addData((1, "early", 5L, false))
    runPipeline(buildCtx())
    checkAnswer(
      spark.table(s"$catalog.$namespace.target"),
      Seq(Row(1, "early", 5L, 5L, 10L, scd2Meta(5L)))
    )
  }

  test("auxiliary key-column-names property survives identifiers containing special " +
    "characters that exercise both JSON and SQL string-literal escaping") {
    // This test exercises the full identifier-text persistence path with composite keys whose
    // names collectively cover every escape class:
    //   - `it's`              -- single quote: not escaped by JSON; the writer must double it
    //                            to `''` to keep the SQL TBLPROPERTIES literal well-formed.
    //   - `name with spaces`  -- whitespace identifier: backtick-quoted in DDL, no escaping
    //                            needed in the JSON or the property value.
    //   - `a"b`               -- literal double quote: JSON escapes as `\"`.
    //   - `c\d`               -- literal backslash: JSON escapes as `\\`.
    // If any layer drops, splits, or misescapes a name, the post-run lookup of the
    // [[AutoCdcAuxiliaryTable.keyColumnNamesProperty]] property either fails to read or
    // returns a value that is no longer a parseable JSON array of strings.
    val keyNames = Seq("it's", "name with spaces", "a\"b", "c\\d")

    // SQL DDL identifier rendering: backticks delimit each identifier; an embedded backtick
    // would have to be escaped by doubling, but none of these names contain one.
    val targetTableDdl = keyNames
      .map(name => s"`$name` STRING NOT NULL")
      .mkString(", ") + s", version BIGINT NOT NULL, $scd2MetadataDdl"
    spark.sql(s"CREATE TABLE $catalog.$namespace.target ($targetTableDdl)")

    // The AutoCDC API runs every key through `UnqualifiedColumnName.apply`, which calls
    // `CatalystSqlParser.parseMultipartIdentifier`. To get a single-part identifier whose
    // text includes special characters, the API caller has to backtick-quote at the boundary;
    // we mirror that here by wrapping each name in backticks (and doubling any embedded
    // backtick -- not needed for these names but kept for parity with how a user would call
    // the API).
    val backtickQuotedKeys = keyNames.map(name => s"`${name.replace("`", "``")}`")

    // Single MemoryStream reused across both runs so the streaming checkpoint can resume.
    val stream = MemoryStream[(String, String, String, String, Long)]
    def buildCtx(): TestGraphRegistrationContext =
      singleAutoCdcFlowPipeline(
        flowName = "auto_cdc_flow",
        target = "target",
        sourceDf = stream.toDF().toDF((keyNames :+ "version"): _*),
        keys = backtickQuotedKeys,
        sequencing = functions.col("version"),
        scdType = ScdType.Type2)

    // Run #1: a single insert with arbitrary non-empty key values.
    stream.addData(("v1", "v2", "v3", "v4", 1L))
    runPipeline(buildCtx())

    // The persisted property must round-trip every name byte-for-byte.
    assert(getAuxTableKeyColumnNames(target = "target") == keyNames)

    // Run #2: same keys, a higher sequence -- drift validation reads the property back, parses
    // the JSON, and looks up each recorded name in the aux schema. If any layer mangled the
    // identifier text (lost an escape, dropped a `'`, split on a `.`, ...), validation would
    // either throw KEY_SCHEMA_DRIFT (name lookup miss) or INTERNAL_ERROR (recorded name absent
    // from aux schema). Reaching the second run successfully proves the round-trip works.
    stream.addData(("v1", "v2", "v3", "v4", 2L))
    runPipeline(buildCtx())

    // The persisted property is immutable across non-full-refresh runs, so it must still be
    // intact after run #2.
    assert(getAuxTableKeyColumnNames(target = "target") == keyNames)
  }

  private def getAuxTableKeyColumnNames(target: String): Seq[String] = {
    val auxName = auxTableNameFor(target)
    val rows = spark.sql(s"SHOW TBLPROPERTIES $auxName").collect()
    val prop = rows
      .find(_.getString(0) == AutoCdcAuxiliaryTable.keyColumnNamesProperty)
      .getOrElse(fail(
        s"auxiliary table $auxName is missing the " +
        s"${AutoCdcAuxiliaryTable.keyColumnNamesProperty} property; got: ${rows.toSeq}"
      ))
    AutoCdcAuxiliaryTable.parseColumnNames(prop.getString(1))
      .getOrElse(fail(
        s"auxiliary table $auxName has a malformed " +
        s"${AutoCdcAuxiliaryTable.keyColumnNamesProperty} property: '${prop.getString(1)}'"
      ))
  }
}
