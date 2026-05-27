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
  Scd1BatchProcessor,
  UnqualifiedColumnName
}
import org.apache.spark.sql.pipelines.utils.{ExecutionTest, TestGraphRegistrationContext}
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Tests covering the durability of AutoCDC's auxiliary table across pipeline runs:
 * the per-key sequence watermarks recorded in the auxiliary table must persist between
 * incremental runs, and the auxiliary table must be transparently recreated if it is
 * deleted out-of-band.
 */
class AutoCdcScd1AuxiliaryTableDurabilitySuite
    extends ExecutionTest
    with SharedSparkSession
    with AutoCdcGraphExecutionTestMixin {

  test("a higher-sequence event in a later pipeline run correctly upserts the row") {
    val session = spark
    import session.implicits._

    spark.sql(
      s"CREATE TABLE $catalog.$namespace.target " +
      s"(id INT NOT NULL, name STRING, version BIGINT NOT NULL, $cdcMetadataDdl)"
    )

    // Single MemoryStream reused across both pipeline runs so the streaming checkpoint can
    // resume cleanly.
    val changeDataFeedStream = MemoryStream[(Int, String, Long)]
    def buildGraphRegistrationContext(): TestGraphRegistrationContext =
      new TestGraphRegistrationContext(spark) {
        registerTable("target", catalog = Some(catalog), database = Some(namespace))
        registerFlow(autoCdcFlow(
          name = "auto_cdc_flow",
          target = "target",
          query = dfFlowFunc(
            changeDataFeedStream.toDF().toDF("id", "name", "version")
          ),
          keys = Seq("id"),
          sequencing = functions.col("version")
        ))
      }

    // Run #1: insert id=1 at seq=1.
    changeDataFeedStream.addData((1, "alice", 1L))
    runPipeline(buildGraphRegistrationContext())
    checkAnswer(
      spark.table(s"$catalog.$namespace.target"),
      Seq(Row(1, "alice", 1L, cdcMeta(None, Some(1L))))
    )

    // Run #2: upsert id=1 at seq=2 (must replace) and insert id=2 at seq=1 (new key).
    // The auxiliary table from run #1 persists and continues to gate seq comparisons.
    changeDataFeedStream.addData((1, "alice2", 2L), (2, "bob", 1L))
    runPipeline(buildGraphRegistrationContext())
    checkAnswer(
      spark.table(s"$catalog.$namespace.target"),
      Seq(
        Row(1, "alice2", 2L, cdcMeta(None, Some(2L))),
        Row(2, "bob", 1L, cdcMeta(None, Some(1L)))
      )
    )
  }

  test("an event with a sequence lower than what was applied in a prior pipeline run " +
    "is suppressed") {
    val session = spark
    import session.implicits._

    spark.sql(
      s"CREATE TABLE $catalog.$namespace.target " +
      s"(id INT NOT NULL, name STRING, version BIGINT NOT NULL, $cdcMetadataDdl)"
    )

    // Single MemoryStream reused across both runs so the streaming checkpoint can resume.
    val stream = MemoryStream[(Int, String, Long, Boolean)]
    def buildCtx(): TestGraphRegistrationContext = new TestGraphRegistrationContext(spark) {
      registerTable("target", catalog = Some(catalog), database = Some(namespace))
      registerFlow(autoCdcFlow(
        name = "auto_cdc_flow",
        target = "target",
        query = dfFlowFunc(stream.toDF().toDF("id", "name", "version", "is_delete")),
        keys = Seq("id"),
        sequencing = functions.col("version"),
        deleteCondition = Some(functions.col("is_delete") === true),
        columnSelection = Some(ColumnSelection.ExcludeColumns(
          Seq(UnqualifiedColumnName("is_delete"))
        ))
      ))
    }

    // Run #1: delete id=1 at seq=10. Auxiliary table records seq=10 as the watermark.
    stream.addData((1, "alice", 10L, true))
    runPipeline(buildCtx())
    checkAnswer(spark.table(s"$catalog.$namespace.target"), Seq.empty)

    // Run #2: late upsert at seq=5 (< the persisted seq=10 watermark). Must be rejected.
    stream.addData((1, "stale", 5L, false))
    runPipeline(buildCtx())

    // Auxiliary table watermark from run #1 (seq=10) should keep rejecting the seq=5 event.
    checkAnswer(spark.table(s"$catalog.$namespace.target"), Seq.empty)
  }

  test("the auxiliary table places the AutoCDC key column first, ahead of any non-key " +
    "source columns") {
    val session = spark
    import session.implicits._

    // Source DF column order is (name, id, version): the AutoCDC key column `id` does NOT
    // appear first in the source DF. The auxiliary table must still write `id` as its
    // leading column.
    spark.sql(
      s"CREATE TABLE $catalog.$namespace.target " +
      s"(name STRING, id INT NOT NULL, version BIGINT NOT NULL, $cdcMetadataDdl)"
    )

    val stream = MemoryStream[(String, Int, Long)]
    stream.addData(("alice", 1, 1L))
    val ctx = new TestGraphRegistrationContext(spark) {
      registerTable("target", catalog = Some(catalog), database = Some(namespace))
      registerFlow(autoCdcFlow(
        name = "auto_cdc_flow",
        target = "target",
        query = dfFlowFunc(stream.toDF().toDF("name", "id", "version")),
        keys = Seq("id"),
        sequencing = functions.col("version")
      ))
    }
    runPipeline(ctx)

    val auxSchema = spark.table(auxTableNameFor("target")).schema

    // The auxiliary table only contains keys and the metadata column, hence "name" should not be
    // included.
    assert(auxSchema.fieldNames.toSeq == Seq("id", Scd1BatchProcessor.cdcMetadataColName))
    assert(getAuxTableKeyColumnNames(target = "target") == Seq("id"))
  }

  test("the auxiliary table preserves the user's declared key order, independent of the " +
    "source DataFrame and target table column orders") {
    val session = spark
    import session.implicits._

    // Source DF: (value, id, region, version). Target table: (value, id, region, version,
    // _cdc_metadata) -- same ordering as the source. The user, however, declares
    // `keys = Seq("region", "id")` -- the OPPOSITE order from how those columns appear in
    // both the source DF and the target. The auxiliary table should honor the user's
    // declared key order, both in the persisted aux schema layout and in the
    // [[AutoCdcAuxiliaryTable.keyColumnNamesProperty]] property value, so subsequent runs
    // compare keys against the same recorded layout.
    spark.sql(
      s"CREATE TABLE $catalog.$namespace.target " +
      s"(value STRING, id INT NOT NULL, region STRING NOT NULL, " +
      s"version BIGINT NOT NULL, $cdcMetadataDdl)"
    )

    val stream = MemoryStream[(String, Int, String, Long)]
    stream.addData(("v", 1, "us", 1L))
    val ctx = new TestGraphRegistrationContext(spark) {
      registerTable("target", catalog = Some(catalog), database = Some(namespace))
      registerFlow(autoCdcFlow(
        name = "auto_cdc_flow",
        target = "target",
        query = dfFlowFunc(stream.toDF().toDF("value", "id", "region", "version")),
        keys = Seq("region", "id"),
        sequencing = functions.col("version")
      ))
    }
    runPipeline(ctx)

    val auxSchema = spark.table(auxTableNameFor("target")).schema
    assert(auxSchema.fieldNames.toSeq ==
      Seq("region", "id", Scd1BatchProcessor.cdcMetadataColName))
    assert(getAuxTableKeyColumnNames(target = "target") == Seq("region", "id"))
  }

  test("if the AutoCDC auxiliary table is dropped between runs, it is transparently " +
    "recreated") {
    val session = spark
    import session.implicits._

    spark.sql(
      s"CREATE TABLE $catalog.$namespace.target " +
      s"(id INT NOT NULL, version BIGINT NOT NULL, $cdcMetadataDdl)"
    )

    // Single MemoryStream reused across both runs so the streaming checkpoint can resume.
    val stream = MemoryStream[(Int, Long)]
    def buildCtx(): TestGraphRegistrationContext = new TestGraphRegistrationContext(spark) {
      registerTable("target", catalog = Some(catalog), database = Some(namespace))
      registerFlow(autoCdcFlow(
        name = "auto_cdc_flow",
        target = "target",
        query = dfFlowFunc(stream.toDF().toDF("id", "version")),
        keys = Seq("id"),
        sequencing = functions.col("version")
      ))
    }

    stream.addData((1, 1L))
    runPipeline(buildCtx())
    assert(spark.catalog.tableExists(auxTableNameFor("target")))

    // Manually drop the auxiliary table.
    spark.sql(s"DROP TABLE ${auxTableNameFor("target")}")
    assert(!spark.catalog.tableExists(auxTableNameFor("target")))

    stream.addData((1, 2L))
    runPipeline(buildCtx())

    // The dropped auxiliary table must be transparently recreated.
    assert(spark.catalog.tableExists(auxTableNameFor("target")))
    checkAnswer(
      spark.table(s"$catalog.$namespace.target"),
      Seq(Row(1, 2L, cdcMeta(None, Some(2L))))
    )
  }

  test("auxiliary key-column-names property survives identifiers containing special " +
    "characters that exercise both JSON and SQL string-literal escaping") {
    val session = spark
    import session.implicits._

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
      .mkString(", ") + s", version BIGINT NOT NULL, $cdcMetadataDdl"
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
    def buildCtx(): TestGraphRegistrationContext = new TestGraphRegistrationContext(spark) {
      registerTable("target", catalog = Some(catalog), database = Some(namespace))
      registerFlow(autoCdcFlow(
        name = "auto_cdc_flow",
        target = "target",
        query = dfFlowFunc(
          stream.toDF().toDF((keyNames :+ "version"): _*)
        ),
        keys = backtickQuotedKeys,
        sequencing = functions.col("version")
      ))
    }

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
      .getOrElse(throw new AssertionError(
        s"auxiliary table $auxName is missing the " +
        s"${AutoCdcAuxiliaryTable.keyColumnNamesProperty} property; got: ${rows.toSeq}"
      ))
    AutoCdcAuxiliaryTable.parseKeyColumnNames(prop.getString(1))
      .getOrElse(throw new AssertionError(
        s"auxiliary table $auxName has a malformed " +
        s"${AutoCdcAuxiliaryTable.keyColumnNamesProperty} property: '${prop.getString(1)}'"
      ))
  }
}
