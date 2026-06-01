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

import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.pipelines.utils.{ExecutionTest, TestGraphRegistrationContext}
import org.apache.spark.sql.test.SharedSparkSession

/**
 * End-to-end tests covering AutoCDC SCD1 key-drift validation: the AutoCDC flow's declared
 * keys are validated against the auxiliary table's recorded keys at flow execution-init
 * time. A change in keys across runs without a full refresh corrupts the merge semantics
 * (rows mis-routed between insert/update); validation detects this and fails fast with a
 * structured [[AUTOCDC_INVALID_STATE]] error.
 *
 * Each test seeds the auxiliary table by running a first pipeline with one set of keys, then
 * runs a second pipeline with a different shape (new keys, dropped keys, swapped keys, drifted
 * dataType, or with a tampered auxiliary table) and asserts on the structured failure.
 */
class AutoCdcScd1KeyDriftSuite
    extends ExecutionTest
    with SharedSparkSession
    with AutoCdcGraphExecutionTestMixin {

  test("a pipeline execution that adds a key column to an existing AutoCDC flow triggers " +
    "KEY_SCHEMA_DRIFT") {
    val session = spark
    import session.implicits._

    // Target table carries both candidate key columns up-front so only the AutoCDC `keys`
    // declaration differs between the two pipelines.
    spark.sql(
      s"CREATE TABLE $catalog.$namespace.target " +
      s"(id INT NOT NULL, region STRING NOT NULL, version BIGINT NOT NULL, $cdcMetadataDdl)"
    )

    // Pipeline #1 declares one key (`id`). Aux table is created with schema (id, _cdc_metadata).
    val stream1 = MemoryStream[(Int, String, Long)]
    stream1.addData((1, "us", 1L))
    runPipeline(buildPipeline("flow_v1", stream1.toDF().toDF("id", "region", "version"), Seq("id")))

    // Pipeline #2 declares two keys (`region` + `id`) - arity drift.
    val stream2 = MemoryStream[(Int, String, Long)]
    stream2.addData((1, "us", 2L))
    val ctx2 = buildPipeline(
      "flow_v2", stream2.toDF().toDF("id", "region", "version"), Seq("region", "id"))

    val ex = intercept[RuntimeException] { runPipeline(ctx2) }
    checkErrorInPipelineFailure(
      failure = ex,
      condition = "AUTOCDC_INVALID_STATE.KEY_SCHEMA_DRIFT",
      sqlState = Some("42000"),
      parameters = Map(
        "flowName" ->
          fullyQualifiedIdentifier("flow_v2", Some(catalog), Some(namespace)).unquotedString,
        "auxTableName" -> auxTableNameFor("target"),
        // `region` is nullable here because Scala `String` is a reference type and the
        // [[MemoryStream]] tuple encoder treats reference types as nullable. Only Scala
        // primitives (`Int`, `Long`, ...) yield `NOT NULL` columns.
        "expectedKeySchema" -> "region STRING,id INT NOT NULL",
        "recordedKeySchema" -> "id INT NOT NULL"
      )
    )
  }

  test("a pipeline execution that drops a key column from an existing AutoCDC flow triggers " +
    "KEY_SCHEMA_DRIFT") {
    val session = spark
    import session.implicits._

    spark.sql(
      s"CREATE TABLE $catalog.$namespace.target " +
      s"(region STRING NOT NULL, id INT NOT NULL, version BIGINT NOT NULL, $cdcMetadataDdl)"
    )

    // Pipeline #1 declares two keys [region, id]. Without strict-equality, the dropped `region`
    // would slip through with `id` silently matching at position 0 of the recorded schema.
    val stream1 = MemoryStream[(String, Int, Long)]
    stream1.addData(("us", 1, 1L))
    runPipeline(buildPipeline(
      "flow_v1", stream1.toDF().toDF("region", "id", "version"), Seq("region", "id")))

    // Pipeline #2 declares only [id] - arity drift.
    val stream2 = MemoryStream[(String, Int, Long)]
    stream2.addData(("us", 1, 2L))
    val ctx2 = buildPipeline("flow_v2", stream2.toDF().toDF("region", "id", "version"), Seq("id"))

    val ex = intercept[RuntimeException] { runPipeline(ctx2) }
    checkErrorInPipelineFailure(
      failure = ex,
      condition = "AUTOCDC_INVALID_STATE.KEY_SCHEMA_DRIFT",
      sqlState = Some("42000"),
      parameters = Map(
        "flowName" ->
          fullyQualifiedIdentifier("flow_v2", Some(catalog), Some(namespace)).unquotedString,
        "auxTableName" -> auxTableNameFor("target"),
        "expectedKeySchema" -> "id INT NOT NULL",
        // `region` is nullable here because Scala `String` is a reference type; see the
        // analogous comment in the "adds a key column" test above.
        "recordedKeySchema" -> "region STRING,id INT NOT NULL"
      )
    )
  }

  test("a pipeline execution that swaps a key in an existing AutoCDC flow for a different name " +
    "(same arity) triggers KEY_SCHEMA_DRIFT") {
    val session = spark
    import session.implicits._

    spark.sql(
      s"CREATE TABLE $catalog.$namespace.target " +
      s"(id INT NOT NULL, region STRING NOT NULL, country STRING NOT NULL, " +
      s"version BIGINT NOT NULL, $cdcMetadataDdl)"
    )

    // Pipeline #1 declares [id, region].
    val stream1 = MemoryStream[(Int, String, String, Long)]
    stream1.addData((1, "us", "USA", 1L))
    runPipeline(buildPipeline(
      "flow_v1", stream1.toDF().toDF("id", "region", "country", "version"), Seq("id", "region")))

    // Pipeline #2 declares [id, country] - same arity, different key set. An arity-only check
    // would silently match `id` at position 0 and the swapped `region`/`country` would slip
    // through; the by-name set comparison must catch it.
    val stream2 = MemoryStream[(Int, String, String, Long)]
    stream2.addData((1, "us", "USA", 2L))
    val ctx2 = buildPipeline(
      "flow_v2", stream2.toDF().toDF("id", "region", "country", "version"), Seq("id", "country"))

    val ex = intercept[RuntimeException] { runPipeline(ctx2) }
    checkErrorInPipelineFailure(
      failure = ex,
      condition = "AUTOCDC_INVALID_STATE.KEY_SCHEMA_DRIFT",
      sqlState = Some("42000"),
      parameters = Map(
        "flowName" ->
          fullyQualifiedIdentifier("flow_v2", Some(catalog), Some(namespace)).unquotedString,
        "auxTableName" -> auxTableNameFor("target"),
        // `country` and `region` are nullable here because Scala `String` is a reference type;
        // see the analogous comment in the "adds a key column" test above.
        "expectedKeySchema" -> "id INT NOT NULL,country STRING",
        "recordedKeySchema" -> "id INT NOT NULL,region STRING"
      )
    )
  }

  test("a pipeline whose recorded aux key dataType differs from the flow's source dataType " +
    "triggers KEY_SCHEMA_DRIFT") {
    spark.sql(
      s"CREATE TABLE $catalog.$namespace.target " +
      s"(id INT NOT NULL, version BIGINT NOT NULL, $cdcMetadataDdl)"
    )
    spark.sql(
      s"""CREATE TABLE ${auxTableNameFor("target")} (id BIGINT NOT NULL, $cdcMetadataDdl) """ +
      s"""TBLPROPERTIES ('${AutoCdcAuxiliaryTable.keyColumnNamesProperty}' = '["id"]')"""
    )

    val session = spark
    import session.implicits._
    val stream = MemoryStream[(Int, Long)]
    stream.addData((1, 1L))
    val ctx = buildPipeline("flow", stream.toDF().toDF("id", "version"), Seq("id"))

    val ex = intercept[RuntimeException] { runPipeline(ctx) }
    checkErrorInPipelineFailure(
      failure = ex,
      condition = "AUTOCDC_INVALID_STATE.KEY_SCHEMA_DRIFT",
      sqlState = Some("42000"),
      parameters = Map(
        "flowName" ->
          fullyQualifiedIdentifier("flow", Some(catalog), Some(namespace)).unquotedString,
        "auxTableName" -> auxTableNameFor("target"),
        "expectedKeySchema" -> "id INT NOT NULL",
        "recordedKeySchema" -> "id BIGINT NOT NULL"
      )
    )
  }

  test("a composite key reorder ([a,b] -> [b,a]) does NOT trigger drift validation") {
    val session = spark
    import session.implicits._

    spark.sql(
      s"CREATE TABLE $catalog.$namespace.target " +
      s"(a INT NOT NULL, b STRING NOT NULL, version BIGINT NOT NULL, $cdcMetadataDdl)"
    )

    // Pipeline #1 declares keys [a, b] (in that order). Drift validation is order-independent:
    // the recorded ordering is purely cosmetic for human-readable error messages and must not
    // gate semantic equivalence, since the merge semantics depend only on the *set* of key
    // columns and their dataTypes.
    val stream1 = MemoryStream[(Int, String, Long)]
    stream1.addData((1, "x", 1L))
    runPipeline(buildPipeline("flow_v1", stream1.toDF().toDF("a", "b", "version"), Seq("a", "b")))

    // Pipeline #2 declares the same key set in the reversed order [b, a]. Must NOT throw.
    val stream2 = MemoryStream[(Int, String, Long)]
    stream2.addData((2, "y", 1L))
    runPipeline(buildPipeline("flow_v2", stream2.toDF().toDF("a", "b", "version"), Seq("b", "a")))
  }

  test("a pipeline execution that changes a key column's nullability or metadata in an " +
    "existing AutoCDC flow does NOT trigger drift") {
    val session = spark
    import session.implicits._

    // Drift validation compares (name, dataType) pairs as a set. Nullability and column
    // metadata are part of [[StructField]] but not part of [[DataType]], so they do not gate
    // semantic equivalence: only the wire-format data type matters for merge correctness.
    // Target's `id` is nullable so the second pipeline's nullable-`id` source is accepted.
    spark.sql(
      s"CREATE TABLE $catalog.$namespace.target " +
      s"(id INT, version BIGINT NOT NULL, $cdcMetadataDdl)"
    )

    // Pipeline #1: source carries `id INT NOT NULL` (Scala primitive `Int`), no metadata.
    val stream1 = MemoryStream[(Int, Long)]
    stream1.addData((1, 1L))
    runPipeline(buildPipeline("flow_v1", stream1.toDF().toDF("id", "version"), Seq("id")))

    // Pipeline #2: source carries `id INT` (nullable, via `Option[Int]`) AND attaches
    // non-empty column metadata. Same name and `dataType` as the recorded key, but every
    // [[StructField]] aspect outside `dataType` differs.
    val stream2 = MemoryStream[(Option[Int], Long)]
    stream2.addData((Some(2), 2L))
    val baseDf = stream2.toDF().toDF("id", "version")
    val md = new org.apache.spark.sql.types.MetadataBuilder()
      .putString("description", "primary key")
      .build()
    val sourceDfWithMetadata = baseDf.select(baseDf("id").as("id", md), baseDf("version"))
    runPipeline(buildPipeline("flow_v2", sourceDfWithMetadata, Seq("id")))
  }

  test("a pipeline execution that wraps an existing AutoCDC flow's key in backticks does NOT " +
    "trigger drift") {
    val session = spark
    import session.implicits._

    // Backticks are a SQL-parse syntactic device, not part of the identifier itself. A user
    // adding or removing backticks around the same logical column must NOT be detected as drift.
    spark.sql(
      s"CREATE TABLE $catalog.$namespace.target " +
      s"(id INT NOT NULL, version BIGINT NOT NULL, $cdcMetadataDdl)"
    )

    val stream1 = MemoryStream[(Int, Long)]
    stream1.addData((1, 1L))
    runPipeline(buildPipeline("flow_v1", stream1.toDF().toDF("id", "version"), Seq("id")))

    val stream2 = MemoryStream[(Int, Long)]
    stream2.addData((2, 1L))
    runPipeline(buildPipeline("flow_v2", stream2.toDF().toDF("id", "version"), Seq("`id`")))
  }

  test("a pipeline execution that drops backticks around an existing AutoCDC flow's " +
    "previously-backtick-quoted key does NOT trigger drift") {
    val session = spark
    import session.implicits._

    // The reverse direction of the previous test: drift validation must be backtick-invariant
    // on both the WRITE side (recorded property strips backticks when serializing the key
    // names in pipeline #1) and the READ side (resolver-aware lookup strips backticks when
    // pipeline #2's expected keys are matched against the recorded set).
    spark.sql(
      s"CREATE TABLE $catalog.$namespace.target " +
      s"(id INT NOT NULL, version BIGINT NOT NULL, $cdcMetadataDdl)"
    )

    val stream1 = MemoryStream[(Int, Long)]
    stream1.addData((1, 1L))
    runPipeline(buildPipeline("flow_v1", stream1.toDF().toDF("id", "version"), Seq("`id`")))

    val stream2 = MemoryStream[(Int, Long)]
    stream2.addData((2, 1L))
    runPipeline(buildPipeline("flow_v2", stream2.toDF().toDF("id", "version"), Seq("id")))
  }

  test("under spark.sql.caseSensitive = true, an AutoCDC flow whose key differs only in case " +
    "from the recorded key triggers KEY_SCHEMA_DRIFT") {
    val session = spark
    import session.implicits._

    // validateNoAutoCdcKeyDrift uses spark.sessionState.conf.resolver, so its behavior on
    // `Id` vs `id` flips with the session conf. Pin the case-sensitive direction: pipeline #1
    // seeds the aux table under the default resolver with recorded key `["id"]`, then
    // pipeline #2 runs under the case-sensitive resolver with key `["Id"]`. Because `Id` and
    // `id` are distinct identifiers under that resolver, drift validation must fail.
    spark.sql(
      s"CREATE TABLE $catalog.$namespace.target " +
      s"(id INT NOT NULL, version BIGINT NOT NULL, $cdcMetadataDdl)"
    )

    val stream1 = MemoryStream[(Int, Long)]
    stream1.addData((1, 1L))
    runPipeline(buildPipeline("flow_v1", stream1.toDF().toDF("id", "version"), Seq("id")))

    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      // Source DF column is `Id` (capital) so the AutoCDC flow's own key-presence check
      // (`requireKeysPresentInSelectedSchema`) succeeds under case-sensitive analysis.
      // Drift validation is then the only remaining failure mode and it must fire.
      val stream2 = MemoryStream[(Int, Long)]
      stream2.addData((1, 2L))
      val ctx2 = buildPipeline("flow_v2", stream2.toDF().toDF("Id", "version"), Seq("Id"))

      val ex = intercept[RuntimeException] { runPipeline(ctx2) }
      checkErrorInPipelineFailure(
        failure = ex,
        condition = "AUTOCDC_INVALID_STATE.KEY_SCHEMA_DRIFT",
        sqlState = Some("42000"),
        parameters = Map(
          "flowName" ->
            fullyQualifiedIdentifier("flow_v2", Some(catalog), Some(namespace)).unquotedString,
          "auxTableName" -> auxTableNameFor("target"),
          "expectedKeySchema" -> "Id INT NOT NULL",
          "recordedKeySchema" -> "id INT NOT NULL"
        )
      )
    }
  }

  test("under the default (case-insensitive) resolver, an AutoCDC flow whose key differs only " +
    "in case from the recorded key does NOT trigger drift") {
    val session = spark
    import session.implicits._

    // Pairs with the case-sensitive test above: same recorded key, but under the default
    // resolver the two identifiers are equivalent so drift validation must accept pipeline
    // #2. This pins the negative direction so a regression that accidentally hard-codes a
    // case-sensitive resolver in the validator is caught.
    //
    // Note that only the *key declaration* (`Seq("Id")`) has different casing here -- the
    // source DF column name still matches the target's `id` exactly. Differing the source DF
    // column casing as well would not exercise drift: [[SchemaMergingUtils.mergeSchemas]] is
    // case-sensitive on column names and would add `Id` as a new column to the target,
    // producing AMBIGUOUS_REFERENCE during the streaming write rather than letting drift
    // validation make the call.
    spark.sql(
      s"CREATE TABLE $catalog.$namespace.target " +
      s"(id INT NOT NULL, version BIGINT NOT NULL, $cdcMetadataDdl)"
    )

    val stream1 = MemoryStream[(Int, Long)]
    stream1.addData((1, 1L))
    runPipeline(buildPipeline("flow_v1", stream1.toDF().toDF("id", "version"), Seq("id")))

    val stream2 = MemoryStream[(Int, Long)]
    stream2.addData((1, 2L))
    runPipeline(buildPipeline("flow_v2", stream2.toDF().toDF("id", "version"), Seq("Id")))
  }

  test("a pipeline whose aux table is missing the keyColumnNames property fails with " +
    "AUXILIARY_TABLE_PROPERTY_MISSING") {
    // Pre-create the aux table directly without the [[keyColumnNamesProperty]] to simulate
    // corrupt metadata (e.g. user ran `ALTER TABLE ... UNSET TBLPROPERTIES`). Validation must
    // surface a structured AUTOCDC_INVALID_STATE error rather than silently mis-validating keys.
    spark.sql(
      s"CREATE TABLE $catalog.$namespace.target " +
      s"(id INT NOT NULL, version BIGINT NOT NULL, $cdcMetadataDdl)"
    )
    spark.sql(
      s"CREATE TABLE ${auxTableNameFor("target")} (id INT NOT NULL, $cdcMetadataDdl)"
    )

    val session = spark
    import session.implicits._
    val stream = MemoryStream[(Int, Long)]
    stream.addData((1, 1L))
    val ctx = buildPipeline("flow", stream.toDF().toDF("id", "version"), Seq("id"))

    val ex = intercept[RuntimeException] { runPipeline(ctx) }
    checkErrorInPipelineFailure(
      failure = ex,
      condition = "AUTOCDC_INVALID_STATE.AUXILIARY_TABLE_PROPERTY_MISSING",
      sqlState = Some("42000"),
      parameters = Map(
        "flowName" ->
          fullyQualifiedIdentifier("flow", Some(catalog), Some(namespace)).unquotedString,
        "auxTableName" -> auxTableNameFor("target"),
        "propertyName" -> AutoCdcAuxiliaryTable.keyColumnNamesProperty
      )
    )
  }

  test("a pipeline whose aux table has a malformed keyColumnNames property fails with " +
    "AUXILIARY_TABLE_PROPERTY_MALFORMED") {
    // Pre-create the aux table directly with a non-JSON-array property value to simulate
    // corrupt metadata. Validation must surface a structured AUTOCDC_INVALID_STATE error
    // rather than letting a parse exception leak.
    val malformedKeysArray = "not-a-json-array"
    spark.sql(
      s"CREATE TABLE $catalog.$namespace.target " +
      s"(id INT NOT NULL, version BIGINT NOT NULL, $cdcMetadataDdl)"
    )
    spark.sql(
      s"CREATE TABLE ${auxTableNameFor("target")} (id INT NOT NULL, $cdcMetadataDdl) " +
      s"TBLPROPERTIES ('${AutoCdcAuxiliaryTable.keyColumnNamesProperty}' = '$malformedKeysArray')"
    )

    val session = spark
    import session.implicits._
    val stream = MemoryStream[(Int, Long)]
    stream.addData((1, 1L))
    val ctx = buildPipeline("flow", stream.toDF().toDF("id", "version"), Seq("id"))

    val ex = intercept[RuntimeException] { runPipeline(ctx) }
    checkErrorInPipelineFailure(
      failure = ex,
      condition = "AUTOCDC_INVALID_STATE.AUXILIARY_TABLE_PROPERTY_MALFORMED",
      sqlState = Some("42000"),
      parameters = Map(
        "flowName" ->
          fullyQualifiedIdentifier("flow", Some(catalog), Some(namespace)).unquotedString,
        "auxTableName" -> auxTableNameFor("target"),
        "propertyName" -> AutoCdcAuxiliaryTable.keyColumnNamesProperty,
        "rawValue" -> malformedKeysArray
      )
    )
  }

  test("a pipeline whose aux table records a key absent from its schema fails with " +
    "AUXILIARY_TABLE_KEY_COLUMN_MISSING") {
    // Pre-create the aux table directly with the [[keyColumnNamesProperty]] pointing at a
    // column that does not exist in the aux schema. This is either a write-path implementation
    // bug or external user tampering (e.g. dropping the key column); validation must surface a
    // structured AUTOCDC_INVALID_STATE error rather than KEY_SCHEMA_DRIFT, because the drift
    // validator cannot run without resolving every recorded key first.
    spark.sql(
      s"CREATE TABLE $catalog.$namespace.target " +
      s"(id INT NOT NULL, version BIGINT NOT NULL, $cdcMetadataDdl)"
    )
    spark.sql(
      s"""CREATE TABLE ${auxTableNameFor("target")} (id INT NOT NULL, $cdcMetadataDdl) """ +
      s"""TBLPROPERTIES ('${AutoCdcAuxiliaryTable.keyColumnNamesProperty}' = '["region"]')"""
    )

    val session = spark
    import session.implicits._
    val stream = MemoryStream[(Int, Long)]
    stream.addData((1, 1L))
    val ctx = buildPipeline("flow", stream.toDF().toDF("id", "version"), Seq("id"))

    val ex = intercept[RuntimeException] { runPipeline(ctx) }
    checkErrorInPipelineFailure(
      failure = ex,
      condition = "AUTOCDC_INVALID_STATE.AUXILIARY_TABLE_KEY_COLUMN_MISSING",
      sqlState = Some("42000"),
      parameters = Map(
        "flowName" ->
          fullyQualifiedIdentifier("flow", Some(catalog), Some(namespace)).unquotedString,
        "auxTableName" -> auxTableNameFor("target"),
        "keyColumnName" -> "region",
        "propertyName" -> AutoCdcAuxiliaryTable.keyColumnNamesProperty
      )
    )
  }

  /**
   * Build a single-flow pipeline targeting `cat.ns1.target` with the given source DF and key
   * column list. Thin wrapper over [[singleAutoCdcFlowPipeline]] since every drift test targets
   * the same `target` table.
   */
  private def buildPipeline(
      flowName: String,
      sourceDf: org.apache.spark.sql.classic.DataFrame,
      keys: Seq[String]): TestGraphRegistrationContext =
    singleAutoCdcFlowPipeline(flowName, "target", sourceDf, keys)
}
