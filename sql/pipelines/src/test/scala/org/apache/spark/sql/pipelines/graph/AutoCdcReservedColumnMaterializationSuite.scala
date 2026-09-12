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

import java.util.Locale

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.functions
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.pipelines.autocdc.{AutoCdcReservedNames, Scd1BatchProcessor}
import org.apache.spark.sql.pipelines.utils.{ExecutionTest, TestGraphRegistrationContext}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}

/**
 * Materialization-level tests for AUTO CDC's engine-owned reserved metadata column (SPARK-58118).
 *
 * The reserved `__spark_autocdc_metadata` column is engine-owned: a user-declared schema may omit
 * it, and materialization appends the engine-owned shape so the created table matches what the
 * AUTO CDC MERGE writes at runtime. Reserved-column matching goes through the flow's effective
 * case sensitivity -- a pipeline-level `SET spark.sql.caseSensitive` can differ from the session --
 * so these tests inspect the created table's schema rather than only validation.
 */
class AutoCdcReservedColumnMaterializationSuite
    extends ExecutionTest
    with SharedSparkSession
    with AutoCdcGraphExecutionTestMixin {

  import testImplicits._

  /** The engine's SCD1 metadata struct shape: `deleteSequence` / `upsertSequence`, both long. */
  private def scd1MetadataType: StructType = new StructType()
    .add(Scd1BatchProcessor.cdcDeleteSequenceFieldName, LongType)
    .add(Scd1BatchProcessor.cdcUpsertSequenceFieldName, LongType)

  test("materialization appends the engine-owned reserved metadata column when the user " +
    "schema omits it") {
    // The user declares only the logical data columns and omits the engine-owned reserved
    // metadata column. Materialization must append it so the created target has exactly what the
    // AUTO CDC MERGE writes; otherwise the MERGE fails with an unresolved metadata column.
    val declaredSchema = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("name", StringType)
      .add("version", LongType, nullable = false)

    val stream = MemoryStream[(Int, String, Long)]
    stream.addData((1, "alice", 5L))
    val ctx = new TestGraphRegistrationContext(spark) {
      registerTable(
        "target",
        catalog = Some(catalog),
        database = Some(namespace),
        specifiedSchema = Some(declaredSchema))
      registerFlow(autoCdcFlow(
        name = "auto_cdc_flow",
        target = "target",
        query = dfFlowFunc(stream.toDF().toDF("id", "name", "version")),
        keys = Seq("id"),
        sequencing = functions.col("version")))
    }
    runPipeline(ctx)

    val targetSchema = spark.table(s"$catalog.$namespace.target").schema
    assert(
      targetSchema.fieldNames.contains(AutoCdcReservedNames.cdcMetadataColName),
      "target should carry the engine-owned reserved metadata column, got " +
        targetSchema.fieldNames.mkString(", "))
    checkAnswer(
      spark.table(s"$catalog.$namespace.target"),
      Seq(Row(1, "alice", 5L, cdcMeta(None, Some(5L))))
    )
  }

  test("materialization matches the reserved metadata column through the flow's effective case " +
    "sensitivity, not the session's") {
    // The session is case-sensitive, but the flow sets spark.sql.caseSensitive=false, so the
    // flow's effective analysis is case-insensitive -- the same resolver the AUTO CDC MERGE uses.
    // A user who declares the reserved column as __SPARK_AUTOCDC_METADATA (upper case) is then
    // declaring the engine-owned column, so materialization must recognize it as reserved and end
    // up with a single engine-owned column, not keep the upper-case one and append a lower-case
    // duplicate. Matching against the session resolver (case-sensitive) here would leave two.
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      val declaredSchema = new StructType()
        .add("id", IntegerType, nullable = false)
        .add("version", LongType, nullable = false)
        .add(AutoCdcReservedNames.cdcMetadataColName.toUpperCase(Locale.ROOT), scd1MetadataType)

      val stream = MemoryStream[(Int, Long)]
      stream.addData((1, 5L))
      val ctx = new TestGraphRegistrationContext(
        spark,
        Map(SQLConf.CASE_SENSITIVE.key -> "false")) {
        registerTable(
          "target",
          catalog = Some(catalog),
          database = Some(namespace),
          specifiedSchema = Some(declaredSchema))
        registerFlow(autoCdcFlow(
          name = "auto_cdc_flow",
          target = "target",
          query = dfFlowFunc(stream.toDF().toDF("id", "version")),
          keys = Seq("id"),
          sequencing = functions.col("version")))
      }
      runPipeline(ctx)

      val reserved = spark.table(s"$catalog.$namespace.target").schema.fieldNames
        .filter(_.toLowerCase(Locale.ROOT).startsWith(AutoCdcReservedNames.prefix))
      // Exactly one reserved column (no duplicate), which is the point of the effective-resolver
      // fix. Its spelling is the one the user declared -- the engine substitutes only the type,
      // keeping the declared casing and position.
      assert(reserved.length == 1,
        "expected exactly one engine-owned metadata column, got " + reserved.mkString(", "))
      checkAnswer(
        spark.table(s"$catalog.$namespace.target"),
        Seq(Row(1, 5L, cdcMeta(None, Some(5L))))
      )
    }
  }

  test("materialization keeps a differently-cased user column distinct from the engine's " +
    "reserved column when the flow is case-sensitive") {
    // Reverse of the previous test: the session is case-insensitive but the flow sets
    // spark.sql.caseSensitive=true. Under the flow's case-sensitive analysis a user-declared
    // __SPARK_AUTOCDC_METADATA is a genuinely distinct column from the engine's lower-case
    // __spark_autocdc_metadata, so both survive. The engine still owns and writes only the
    // lower-case column (nullable=false); the user's upper-case column is left unpopulated.
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      val userColName = AutoCdcReservedNames.cdcMetadataColName.toUpperCase(Locale.ROOT)
      val declaredSchema = new StructType()
        .add("id", IntegerType, nullable = false)
        .add("version", LongType, nullable = false)
        .add(userColName, scd1MetadataType)

      val stream = MemoryStream[(Int, Long)]
      stream.addData((1, 5L))
      val ctx = new TestGraphRegistrationContext(
        spark,
        Map(SQLConf.CASE_SENSITIVE.key -> "true")) {
        registerTable(
          "target",
          catalog = Some(catalog),
          database = Some(namespace),
          specifiedSchema = Some(declaredSchema))
        registerFlow(autoCdcFlow(
          name = "auto_cdc_flow",
          target = "target",
          query = dfFlowFunc(stream.toDF().toDF("id", "version")),
          keys = Seq("id"),
          sequencing = functions.col("version")))
      }
      runPipeline(ctx)

      val schema = spark.table(s"$catalog.$namespace.target").schema
      // Both the user column and the engine column survive; case-sensitive analysis keeps them
      // distinct.
      assert(schema.fieldNames.contains(userColName), schema.fieldNames.mkString(", "))
      assert(
        schema.fieldNames.contains(AutoCdcReservedNames.cdcMetadataColName),
        schema.fieldNames.mkString(", "))
      // Read under case-sensitive analysis so the two case-differing columns stay distinct. The
      // engine writes its metadata to the lower-case column; the user's upper-case column is left
      // unpopulated (null).
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
        checkAnswer(
          spark.table(s"$catalog.$namespace.target"),
          Seq(Row(1, 5L, null, cdcMeta(None, Some(5L))))
        )
      }
    }
  }

  test("a same-graph downstream SELECT * sees the target's materialized schema, including the " +
    "engine-owned reserved column") {
    // The target declares a data-only schema (omits the reserved metadata column). A downstream
    // dataset in the same graph reads it with SELECT *. Its plan-time view of the target (through
    // VirtualTableInput) must match the schema the target is actually materialized with, otherwise
    // the plan carries the 3-column declaration while execution reads the 4-column table.
    val declaredSchema = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("name", StringType)
      .add("version", LongType, nullable = false)
    val stream = MemoryStream[(Int, String, Long)]
    stream.addData((1, "alice", 5L))
    val ctx = new TestGraphRegistrationContext(spark) {
      registerTable("target", catalog = Some(catalog), database = Some(namespace),
        specifiedSchema = Some(declaredSchema))
      registerFlow(autoCdcFlow(name = "writer", target = "target",
        query = dfFlowFunc(stream.toDF().toDF("id", "name", "version")),
        keys = Seq("id"), sequencing = functions.col("version")))
      registerMaterializedView("enriched", catalog = Some(catalog), database = Some(namespace),
        query = readFlowFunc(s"$catalog.$namespace.target"))
    }
    runPipeline(ctx)

    val targetFields = spark.table(s"$catalog.$namespace.target").schema.fieldNames.toSeq
    val enrichedFields = spark.table(s"$catalog.$namespace.enriched").schema.fieldNames.toSeq
    assert(
      enrichedFields == targetFields,
      s"downstream consumer schema ${enrichedFields.mkString(",")} should match the materialized " +
        s"target schema ${targetFields.mkString(",")}")
    assert(enrichedFields.contains(AutoCdcReservedNames.cdcMetadataColName))
    checkAnswer(
      spark.table(s"$catalog.$namespace.enriched"),
      Seq(Row(1, "alice", 5L, cdcMeta(None, Some(5L))))
    )
  }

  test("the engine-owned reserved column is nullable when the user declares a data-only schema, " +
    "matching the omitted-schema path") {
    // A target created from a data-only declaration must get the same nullable metadata column as
    // one created with no declared schema (that path materializes the inferred schema asNullable),
    // so adding or removing a declaration between runs does not surface as a nullability change.
    val declaredSchema = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("version", LongType, nullable = false)
    val stream = MemoryStream[(Int, Long)]
    stream.addData((1, 5L))
    val ctx = new TestGraphRegistrationContext(spark) {
      registerTable("target", catalog = Some(catalog), database = Some(namespace),
        specifiedSchema = Some(declaredSchema))
      registerFlow(autoCdcFlow(name = "auto_cdc_flow", target = "target",
        query = dfFlowFunc(stream.toDF().toDF("id", "version")),
        keys = Seq("id"), sequencing = functions.col("version")))
    }
    runPipeline(ctx)
    val metaField = spark.table(s"$catalog.$namespace.target")
      .schema(AutoCdcReservedNames.cdcMetadataColName)
    assert(metaField.nullable, "engine-owned reserved metadata column should be nullable")
  }

  test("a data-only declaration on an already-materialized target is stable across an " +
    "incremental re-run") {
    // The second run reaches the target through evolveTable/mergeWithExistingSchema rather than a
    // fresh create. Because the declared-schema path produces the same engine-owned shape the
    // target already carries, the schema must not churn between runs.
    val declaredSchema = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("version", LongType, nullable = false)
    def buildCtx(rows: Seq[(Int, Long)]): TestGraphRegistrationContext = {
      val stream = MemoryStream[(Int, Long)]
      stream.addData(rows: _*)
      new TestGraphRegistrationContext(spark) {
        registerTable("target", catalog = Some(catalog), database = Some(namespace),
          specifiedSchema = Some(declaredSchema))
        registerFlow(autoCdcFlow(name = "auto_cdc_flow", target = "target",
          query = dfFlowFunc(stream.toDF().toDF("id", "version")),
          keys = Seq("id"), sequencing = functions.col("version")))
      }
    }
    runPipeline(buildCtx(Seq((1, 5L))))
    val schemaAfterRun1 = spark.table(s"$catalog.$namespace.target").schema
    runPipeline(buildCtx(Seq((2, 3L))))
    val schemaAfterRun2 = spark.table(s"$catalog.$namespace.target").schema
    assert(
      schemaAfterRun2 == schemaAfterRun1,
      s"schema changed across incremental runs: $schemaAfterRun1 -> $schemaAfterRun2")
  }

  test("an already-materialized target's reserved-column casing is preserved for a " +
    "case-sensitive downstream SELECT *") {
    // Upgrade path: a target already exists in the catalog with the reserved metadata column
    // declared upper-cased (allowed under case-insensitive AUTO CDC). evolveTable merges that
    // existing spelling in, so the read path must report the SAME casing, otherwise a
    // case-sensitive downstream `SELECT *` plans the canonical lower-case name and cannot resolve
    // the upper-case column the target actually has.
    val upperMeta = AutoCdcReservedNames.cdcMetadataColName.toUpperCase(Locale.ROOT)
    val del = Scd1BatchProcessor.cdcDeleteSequenceFieldName
    val ups = Scd1BatchProcessor.cdcUpsertSequenceFieldName
    spark.sql(
      s"CREATE TABLE $catalog.$namespace.target " +
      s"(id INT NOT NULL, version BIGINT NOT NULL, " +
      s"$upperMeta STRUCT<$del:BIGINT,$ups:BIGINT> NOT NULL)")

    val metadataType = new StructType().add(del, LongType).add(ups, LongType)
    val declaredSchema = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("version", LongType, nullable = false)
      .add(upperMeta, metadataType, nullable = false)

    val stream = MemoryStream[(Int, Long)]
    stream.addData((1, 5L))
    val ctx = new TestGraphRegistrationContext(spark) {
      registerTable("target", catalog = Some(catalog), database = Some(namespace),
        specifiedSchema = Some(declaredSchema))
      registerFlow(autoCdcFlow(name = "auto_cdc_flow", target = "target",
        query = dfFlowFunc(stream.toDF().toDF("id", "version")),
        keys = Seq("id"), sequencing = functions.col("version")))
      registerMaterializedView("copy", catalog = Some(catalog), database = Some(namespace),
        sqlConf = Map(SQLConf.CASE_SENSITIVE.key -> "true"),
        query = readFlowFunc(s"$catalog.$namespace.target"))
    }
    runPipeline(ctx)

    val targetFields = spark.table(s"$catalog.$namespace.target").schema.fieldNames.toSeq
    val copyFields = spark.table(s"$catalog.$namespace.copy").schema.fieldNames.toSeq
    assert(targetFields.contains(upperMeta),
      "target should keep the declared upper-case reserved column, got " +
        targetFields.mkString(","))
    assert(copyFields == targetFields,
      s"downstream copy ${copyFields.mkString(",")} should match " +
        s"target ${targetFields.mkString(",")}")
  }
}
