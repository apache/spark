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
import org.apache.spark.sql.types.{ArrayType, IntegerType, LongType, StringType, StructType}

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

  test("an already-materialized target with an upper-cased reserved column rejects a data-only " +
    "declaration that omits it") {
    // Upgrade path, the omitted-declaration case: a target already exists with the reserved
    // metadata column upper-cased (allowed under case-insensitive AUTO CDC). The user adopts the
    // new data-only declaration and drops the column, so this run resolves it to the canonical
    // lower-case name while the incremental merge keeps the existing upper-case one. A
    // case-sensitive downstream `SELECT *` could not match the two names, so the run is rejected
    // up front with an actionable error rather than left to fail opaquely during execution.
    // Reconciling at analysis time is not possible: the read path cannot see the existing catalog
    // casing, nor whether this run keeps it (incremental) or rewrites it to canonical (refresh).
    val upperMeta = AutoCdcReservedNames.cdcMetadataColName.toUpperCase(Locale.ROOT)
    val del = Scd1BatchProcessor.cdcDeleteSequenceFieldName
    val ups = Scd1BatchProcessor.cdcUpsertSequenceFieldName
    spark.sql(
      s"CREATE TABLE $catalog.$namespace.target " +
      s"(id INT NOT NULL, version BIGINT NOT NULL, " +
      s"$upperMeta STRUCT<$del:BIGINT,$ups:BIGINT> NOT NULL)")

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

    val ex = intercept[RuntimeException] { runPipeline(ctx) }
    checkErrorInPipelineFailure(
      failure = ex,
      condition = "AUTOCDC_INVALID_STATE.RESERVED_METADATA_COLUMN_CASING_DRIFT",
      sqlState = Some("42000"),
      parameters = Map(
        "tableName" ->
          fullyQualifiedIdentifier("target", Some(catalog), Some(namespace)).unquotedString,
        "existingColumnName" -> upperMeta,
        "resolvedColumnName" -> AutoCdcReservedNames.cdcMetadataColName
      )
    )
  }

  test("an already-materialized target's reserved-column nested casing is preserved for a " +
    "case-sensitive downstream SELECT *") {
    // Upgrade path at the nested level: the target already carries the reserved metadata column
    // with its nested fields declared upper-cased (allowed under case-insensitive AUTO CDC).
    // evolveTable merges that existing spelling in, so the read path must report the same nested
    // casing, otherwise a case-sensitive downstream `SELECT *` plans the canonical lower-case
    // nested names and cannot resolve the upper-case fields the target actually has.
    val meta = AutoCdcReservedNames.cdcMetadataColName
    val upperDel = Scd1BatchProcessor.cdcDeleteSequenceFieldName.toUpperCase(Locale.ROOT)
    val upperUps = Scd1BatchProcessor.cdcUpsertSequenceFieldName.toUpperCase(Locale.ROOT)
    spark.sql(
      s"CREATE TABLE $catalog.$namespace.target " +
      s"(id INT NOT NULL, version BIGINT NOT NULL, " +
      s"$meta STRUCT<$upperDel:BIGINT,$upperUps:BIGINT> NOT NULL)")

    val metadataType = new StructType().add(upperDel, LongType).add(upperUps, LongType)
    val declaredSchema = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("version", LongType, nullable = false)
      .add(meta, metadataType, nullable = false)

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

    def nestedNames(table: String): Seq[String] =
      spark.table(table).schema(meta).dataType.asInstanceOf[StructType].fieldNames.toSeq
    val targetNested = nestedNames(s"$catalog.$namespace.target")
    val copyNested = nestedNames(s"$catalog.$namespace.copy")
    assert(targetNested == Seq(upperDel, upperUps),
      s"target should keep the declared upper-case nested names, got ${targetNested.mkString(",")}")
    assert(copyNested == targetNested,
      s"downstream copy nested ${copyNested.mkString(",")} should match " +
        s"target ${targetNested.mkString(",")}")
  }

  test("an already-materialized target with upper-cased reserved nested fields rejects a " +
    "data-only declaration that omits the metadata column") {
    // Upgrade path, the omitted-declaration case at the nested level: the target already carries
    // the reserved metadata column with upper-cased nested fields. The user adopts a data-only
    // declaration and drops the column, so this run resolves the nested fields to their canonical
    // lower-case names while the incremental merge keeps the existing upper-case ones. A
    // case-sensitive downstream `SELECT *` could not match the two, so the run is rejected up front
    // with the same actionable error used for the outer column.
    val meta = AutoCdcReservedNames.cdcMetadataColName
    val del = Scd1BatchProcessor.cdcDeleteSequenceFieldName
    val upperDel = del.toUpperCase(Locale.ROOT)
    val upperUps = Scd1BatchProcessor.cdcUpsertSequenceFieldName.toUpperCase(Locale.ROOT)
    spark.sql(
      s"CREATE TABLE $catalog.$namespace.target " +
      s"(id INT NOT NULL, version BIGINT NOT NULL, " +
      s"$meta STRUCT<$upperDel:BIGINT,$upperUps:BIGINT> NOT NULL)")

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

    val ex = intercept[RuntimeException] { runPipeline(ctx) }
    checkErrorInPipelineFailure(
      failure = ex,
      condition = "AUTOCDC_INVALID_STATE.RESERVED_METADATA_COLUMN_CASING_DRIFT",
      sqlState = Some("42000"),
      parameters = Map(
        "tableName" ->
          fullyQualifiedIdentifier("target", Some(catalog), Some(namespace)).unquotedString,
        "existingColumnName" -> s"$meta.$upperDel",
        "resolvedColumnName" -> s"$meta.$del"
      )
    )
  }

  test("a declared reserved column's array-of-struct nested casing is preserved for a " +
    "case-sensitive downstream SELECT *") {
    // A sequence field takes the SEQUENCE BY type, which can be ARRAY<STRUCT<...>>. A declared
    // reserved column may spell the array's nested fields in a different case under
    // case-insensitive AUTO CDC. Reconciliation has to preserve that declared spelling through the
    // array element, otherwise the read path reports the engine's canonical lower-case name and a
    // case-sensitive downstream SELECT * cannot resolve the field the target actually has. No
    // existing target or upgrade is needed; the fresh declared schema alone exercises it.
    val meta = AutoCdcReservedNames.cdcMetadataColName
    val del = Scd1BatchProcessor.cdcDeleteSequenceFieldName
    val ups = Scd1BatchProcessor.cdcUpsertSequenceFieldName
    val upperOrdinal = "ORDINAL"
    val seqType = ArrayType(new StructType().add(upperOrdinal, LongType))
    val metadataType = new StructType().add(del, seqType).add(ups, seqType)
    val declaredSchema = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("version", LongType)
      .add(meta, metadataType)

    val stream = MemoryStream[(Int, Option[Long])]
    stream.addData((1, Some(5L)))
    val ctx = new TestGraphRegistrationContext(spark) {
      registerTable("target", catalog = Some(catalog), database = Some(namespace),
        specifiedSchema = Some(declaredSchema))
      registerFlow(autoCdcFlow(name = "auto_cdc_flow", target = "target",
        query = dfFlowFunc(stream.toDF().toDF("id", "version")),
        keys = Seq("id"),
        sequencing = functions.array(functions.struct(functions.col("version").as("ordinal")))))
      registerMaterializedView("copy", catalog = Some(catalog), database = Some(namespace),
        sqlConf = Map(SQLConf.CASE_SENSITIVE.key -> "true"),
        query = readFlowFunc(s"$catalog.$namespace.target"))
    }
    runPipeline(ctx)

    def seqElemNames(table: String, seqField: String): Seq[String] = {
      val metaType = spark.table(table).schema(meta).dataType.asInstanceOf[StructType]
      metaType(seqField).dataType.asInstanceOf[ArrayType]
        .elementType.asInstanceOf[StructType].fieldNames.toSeq
    }
    for (seqField <- Seq(del, ups)) {
      val target = seqElemNames(s"$catalog.$namespace.target", seqField)
      val copy = seqElemNames(s"$catalog.$namespace.copy", seqField)
      assert(target == Seq(upperOrdinal),
        s"target $seqField element should keep declared $upperOrdinal, got ${target.mkString(",")}")
      assert(copy == target,
        s"downstream $seqField ${copy.mkString(",")} != target ${target.mkString(",")}")
    }
  }

  test("materialization preserves a user COMMENT on a declared reserved metadata column") {
    // A declared schema may list the engine-owned reserved metadata column to attach metadata such
    // as a COMMENT. reconcileReservedField builds the materialized column from the declared field,
    // substituting only the engine's type and nullability, so the comment must survive onto the
    // created target rather than be dropped.
    val declaredSchema = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("version", LongType, nullable = false)
      .add(AutoCdcReservedNames.cdcMetadataColName, scd1MetadataType, nullable = true,
        "cdc bookkeeping")

    val stream = MemoryStream[(Int, Long)]
    stream.addData((1, 5L))
    val ctx = new TestGraphRegistrationContext(spark) {
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

    val metaField = spark.table(s"$catalog.$namespace.target").schema
      .find(_.name == AutoCdcReservedNames.cdcMetadataColName).get
    assert(metaField.getComment().contains("cdc bookkeeping"),
      "the declared COMMENT on the reserved metadata column should survive materialization, got " +
        metaField.getComment())
  }

  test("materialization appends engine nested fields a declared reserved column omits") {
    // A declared reserved metadata column may list only some of the engine's nested fields.
    // reconcileReservedStruct appends the omitted ones so the created target carries the full
    // engine-owned struct the AUTO CDC MERGE writes.
    val partialMetadata = new StructType()
      .add(Scd1BatchProcessor.cdcUpsertSequenceFieldName, LongType)
    val declaredSchema = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("version", LongType, nullable = false)
      .add(AutoCdcReservedNames.cdcMetadataColName, partialMetadata)

    val stream = MemoryStream[(Int, Long)]
    stream.addData((1, 5L))
    val ctx = new TestGraphRegistrationContext(spark) {
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

    val metaStruct = spark.table(s"$catalog.$namespace.target").schema
      .find(_.name == AutoCdcReservedNames.cdcMetadataColName).get
      .dataType.asInstanceOf[StructType]
    // The appended field must land in the engine's order. Reconciliation normalizes every reserved
    // struct to the engine's field order, not the declaration's: the same code governs the nested
    // sequencing struct, where order is load-bearing (SCD1 compares sequencing values positionally
    // and the target's recorded sequencing type must match the flow's), so a declared-order layout
    // would break those comparisons. This pins the ordering at the metadata level (SPARK-58118).
    assert(
      metaStruct.fieldNames.toSeq == Seq(
        Scd1BatchProcessor.cdcDeleteSequenceFieldName,
        Scd1BatchProcessor.cdcUpsertSequenceFieldName),
      "materialization should append the omitted engine nested field in the engine's order, got " +
        metaStruct.fieldNames.mkString(", "))
  }

  test("a partial declaration of a struct sequencing field keeps the engine's nested field order") {
    // When SEQUENCE BY yields a struct, the reserved upsertSequence/deleteSequence fields are
    // themselves structs, and SCD1 compares them positionally (struct ordering is name-blind). A
    // declared reserved column may list the sequencing sub-struct partially, or in a different
    // order. If reconciliation kept declared order, the target would store the sequencing struct
    // in a different field order than the engine-ordered microbatch: the recorded sequencing type
    // then diverges from the flow's (breaking incremental re-runs, see the two-run test below) and
    // the positional comparison mis-pairs the fields. Reconciliation must emit the engine's field
    // order so the stored struct matches what the merge compares against (SPARK-58118).
    val ups = Scd1BatchProcessor.cdcUpsertSequenceFieldName
    val del = Scd1BatchProcessor.cdcDeleteSequenceFieldName
    // The engine's sequencing struct is STRUCT<major, minor>; the declaration lists only `minor`.
    val partialUpsert = new StructType().add("minor", LongType)
    val metadataType = new StructType().add(ups, partialUpsert)
    val declaredSchema = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("major", LongType)
      .add("minor", LongType)
      .add(AutoCdcReservedNames.cdcMetadataColName, metadataType)

    // Nullable sequencing fields: the engine makes the reserved sequencing struct nullable
    // throughout (`asNullable`), so non-null source fields would fail the merge's nullability cast.
    val stream = MemoryStream[(Int, Option[Long], Option[Long])]
    stream.addData((1, Some(2L), Some(9L)))
    val ctx = new TestGraphRegistrationContext(spark) {
      registerTable("target", catalog = Some(catalog), database = Some(namespace),
        specifiedSchema = Some(declaredSchema))
      registerFlow(autoCdcFlow(name = "auto_cdc_flow", target = "target",
        query = dfFlowFunc(stream.toDF().toDF("id", "major", "minor")),
        keys = Seq("id"),
        sequencing = functions.struct(functions.col("major"), functions.col("minor"))))
    }
    runPipeline(ctx)

    val metaStruct = spark.table(s"$catalog.$namespace.target").schema
      .find(_.name == AutoCdcReservedNames.cdcMetadataColName).get
      .dataType.asInstanceOf[StructType]
    def seqFieldNames(seqField: String): Seq[String] =
      metaStruct(seqField).dataType.asInstanceOf[StructType].fieldNames.toSeq
    // The partially-declared upsertSequence is the negative control: declared order would make it
    // <minor, major>, which mis-pairs positionally against the engine's <major, minor> microbatch.
    assert(seqFieldNames(ups) == Seq("major", "minor"),
      s"upsertSequence should keep the engine order [major, minor], got " +
        seqFieldNames(ups).mkString(","))
    // The fully-omitted deleteSequence is appended canonically; it must carry the engine order too.
    assert(seqFieldNames(del) == Seq("major", "minor"),
      s"deleteSequence should keep the engine order [major, minor], got " +
        seqFieldNames(del).mkString(","))
  }

  test("a reordered full declaration of a struct sequencing field is normalized to engine order") {
    // The declaration lists BOTH sequencing sub-fields, but swapped relative to the engine. The
    // field order of an engine-owned sequencing struct is not user-chosen: SCD1 compares it
    // positionally, so reconciliation must emit the engine's order regardless of the declared order
    // (SPARK-58118).
    val ups = Scd1BatchProcessor.cdcUpsertSequenceFieldName
    // Engine sequencing struct is STRUCT<major, minor>; declared swapped as <minor, major>.
    val reorderedUpsert = new StructType().add("minor", LongType).add("major", LongType)
    val metadataType = new StructType().add(ups, reorderedUpsert)
    val declaredSchema = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("major", LongType)
      .add("minor", LongType)
      .add(AutoCdcReservedNames.cdcMetadataColName, metadataType)

    val stream = MemoryStream[(Int, Option[Long], Option[Long])]
    stream.addData((1, Some(2L), Some(9L)))
    val ctx = new TestGraphRegistrationContext(spark) {
      registerTable("target", catalog = Some(catalog), database = Some(namespace),
        specifiedSchema = Some(declaredSchema))
      registerFlow(autoCdcFlow(name = "auto_cdc_flow", target = "target",
        query = dfFlowFunc(stream.toDF().toDF("id", "major", "minor")),
        keys = Seq("id"),
        sequencing = functions.struct(functions.col("major"), functions.col("minor"))))
    }
    runPipeline(ctx)

    val seqStruct = spark.table(s"$catalog.$namespace.target").schema
      .find(_.name == AutoCdcReservedNames.cdcMetadataColName).get
      .dataType.asInstanceOf[StructType](ups).dataType.asInstanceOf[StructType]
    assert(seqStruct.fieldNames.toSeq == Seq("major", "minor"),
      s"a swapped declaration should still materialize engine order [major, minor], got " +
        seqStruct.fieldNames.mkString(","))
  }

  test("a partial declaration of an array-of-struct sequencing field keeps the engine's element " +
    "field order") {
    // SEQUENCE BY can yield ARRAY<STRUCT<...>>, which SCD1 compares element-wise and positionally,
    // so the array's element struct carries the same reorder hazard as a plain struct. The reserved
    // array element may be declared partially; reconciliation recurses through the array and must
    // keep the engine's element field order (SPARK-58118).
    val ups = Scd1BatchProcessor.cdcUpsertSequenceFieldName
    val del = Scd1BatchProcessor.cdcDeleteSequenceFieldName
    // Engine element struct is STRUCT<major, minor>; the declaration lists only `minor`.
    val partialElem = new StructType().add("minor", LongType)
    val metadataType = new StructType().add(ups, ArrayType(partialElem))
    val declaredSchema = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("major", LongType)
      .add("minor", LongType)
      .add(AutoCdcReservedNames.cdcMetadataColName, metadataType)

    val stream = MemoryStream[(Int, Option[Long], Option[Long])]
    stream.addData((1, Some(2L), Some(9L)))
    val ctx = new TestGraphRegistrationContext(spark) {
      registerTable("target", catalog = Some(catalog), database = Some(namespace),
        specifiedSchema = Some(declaredSchema))
      registerFlow(autoCdcFlow(name = "auto_cdc_flow", target = "target",
        query = dfFlowFunc(stream.toDF().toDF("id", "major", "minor")),
        keys = Seq("id"),
        sequencing = functions.array(
          functions.struct(functions.col("major"), functions.col("minor")))))
    }
    runPipeline(ctx)

    val metaStruct = spark.table(s"$catalog.$namespace.target").schema
      .find(_.name == AutoCdcReservedNames.cdcMetadataColName).get
      .dataType.asInstanceOf[StructType]
    def elemFieldNames(seqField: String): Seq[String] =
      metaStruct(seqField).dataType.asInstanceOf[ArrayType]
        .elementType.asInstanceOf[StructType].fieldNames.toSeq
    // The partially-declared upsertSequence element is the negative control: declared order would
    // make it <minor, major>.
    assert(elemFieldNames(ups) == Seq("major", "minor"),
      s"upsertSequence element should keep the engine order [major, minor], got " +
        elemFieldNames(ups).mkString(","))
    assert(elemFieldNames(del) == Seq("major", "minor"),
      s"deleteSequence element should keep the engine order [major, minor], got " +
        elemFieldNames(del).mkString(","))
  }

  test("a declared COMMENT on a nested reserved sequencing field survives the engine-order " +
    "rebuild") {
    // reconcileReservedStruct rebuilds a declared reserved struct in the engine's field order,
    // routing each matched declared field through reconcileReservedField, which keeps the declared
    // field's name AND metadata (comment) while taking the engine's type/nullability. A declaration
    // may carry a COMMENT on a NESTED sequencing field; rebuilding in engine order must not drop it
    // (rebuilding from the engine side instead of the declared field is the nested-metadata-drop
    // hazard). SEQUENCE BY yields STRUCT<major, minor>; the declaration lists only `minor`, with a
    // comment, so this also exercises the engine-order append of `major` (SPARK-58118).
    val ups = Scd1BatchProcessor.cdcUpsertSequenceFieldName
    val partialUpsert = new StructType()
      .add("minor", LongType, nullable = true, "minor tiebreak")
    val metadataType = new StructType().add(ups, partialUpsert)
    val declaredSchema = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("major", LongType)
      .add("minor", LongType)
      .add(AutoCdcReservedNames.cdcMetadataColName, metadataType)

    val stream = MemoryStream[(Int, Option[Long], Option[Long])]
    stream.addData((1, Some(2L), Some(9L)))
    val ctx = new TestGraphRegistrationContext(spark) {
      registerTable("target", catalog = Some(catalog), database = Some(namespace),
        specifiedSchema = Some(declaredSchema))
      registerFlow(autoCdcFlow(name = "auto_cdc_flow", target = "target",
        query = dfFlowFunc(stream.toDF().toDF("id", "major", "minor")),
        keys = Seq("id"),
        sequencing = functions.struct(functions.col("major"), functions.col("minor"))))
    }
    runPipeline(ctx)

    val seqStruct = spark.table(s"$catalog.$namespace.target").schema
      .find(_.name == AutoCdcReservedNames.cdcMetadataColName).get
      .dataType.asInstanceOf[StructType](ups).dataType.asInstanceOf[StructType]
    // Order is still normalized to the engine's [major, minor]...
    assert(seqStruct.fieldNames.toSeq == Seq("major", "minor"),
      s"nested sequencing struct should keep engine order [major, minor], got " +
        seqStruct.fieldNames.mkString(","))
    // ...and the declared COMMENT on the nested `minor` field survives the rebuild. A rebuild from
    // the engine field would drop it -- the negative control for nested-metadata preservation.
    assert(seqStruct("minor").getComment().contains("minor tiebreak"),
      "the declared COMMENT on the nested sequencing field should survive the engine-order" +
        " rebuild, got " + seqStruct("minor").getComment())
  }

  test("a partial struct-sequencing declaration supports an incremental re-run and keeps the " +
    "newer row") {
    // SPARK-58118 end-to-end across two runs. SEQUENCE BY yields STRUCT<major, minor>; the reserved
    // upsertSequence is declared partially (only `minor`). Run 1 creates the target and writes the
    // newer row; run 2 re-runs with an older event. On the old declared-first order the
    // target records the sequencing type as STRUCT<minor, major>, which mismatches the flow's
    // STRUCT<major, minor>, so run 2 fails with AUTOCDC_INVALID_STATE.SEQUENCING_TYPE_DRIFT. With
    // engine-order normalization the types agree, the re-run succeeds, and the older event does not
    // overwrite the newer row. (The intra-run positional mis-pair the ordering guards against --
    // an older event winning the >= comparison -- needs multiple microbatches in one run against a
    // this-run-created target, which this one-run-per-microbatch harness cannot express; the
    // schema-order tests above pin that root cause directly.)
    val ups = Scd1BatchProcessor.cdcUpsertSequenceFieldName
    val partialUpsert = new StructType().add("minor", LongType)
    val metadataType = new StructType().add(ups, partialUpsert)
    val declaredSchema = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("payload", StringType)
      .add("major", LongType)
      .add("minor", LongType)
      .add(AutoCdcReservedNames.cdcMetadataColName, metadataType)

    def buildCtx(
        rows: Seq[(Int, String, Option[Long], Option[Long])]): TestGraphRegistrationContext = {
      val stream = MemoryStream[(Int, String, Option[Long], Option[Long])]
      stream.addData(rows: _*)
      new TestGraphRegistrationContext(spark) {
        registerTable("target", catalog = Some(catalog), database = Some(namespace),
          specifiedSchema = Some(declaredSchema))
        registerFlow(autoCdcFlow(name = "auto_cdc_flow", target = "target",
          query = dfFlowFunc(stream.toDF().toDF("id", "payload", "major", "minor")),
          keys = Seq("id"),
          sequencing = functions.struct(functions.col("major"), functions.col("minor"))))
      }
    }

    // Run 1 writes the newer row: a higher major is newer.
    runPipeline(buildCtx(Seq((1, "newer", Some(10L), Some(1L)))))
    // Run 2 delivers an older event (major 9 < 10); it must not overwrite the newer row. On the old
    // declared-first order the target recorded the sequencing type as STRUCT<minor, major>, so this
    // re-run is rejected with SEQUENCING_TYPE_DRIFT before any merge runs. With engine-order
    // normalization the types agree, the merge runs, and the newer row survives.
    runPipeline(buildCtx(Seq((1, "older", Some(9L), Some(100L)))))

    checkAnswer(
      spark.table(s"$catalog.$namespace.target").select("id", "payload"),
      Seq(Row(1, "newer")))
  }

  test("a same-graph downstream reader surfaces a case-duplicate reserved declaration as a " +
    "user-facing error, not an internal error") {
    // VirtualTableInput.load reconciles the declared reserved column on the read path during flow
    // analysis, which runs before GraphValidations.validateUserSpecifiedSchemas. A malformed
    // declaration -- here two case-differing reserved columns that both resolve to the single
    // engine-owned column under case-insensitive analysis -- must surface as the user-facing
    // AUTOCDC_INVALID_RESERVED_SCHEMA.DUPLICATE_COLUMN, not an opaque internal error from
    // appendEngineOwnedReservedFields's defense-in-depth guard (SPARK-58118). The downstream reader
    // is what forces the read path during analysis; without it, validateUserSpecifiedSchemas would
    // already raise the clean error.
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      val meta = AutoCdcReservedNames.cdcMetadataColName
      val upperMeta = meta.toUpperCase(Locale.ROOT)
      val declaredSchema = new StructType()
        .add("id", IntegerType, nullable = false)
        .add("version", LongType, nullable = false)
        .add(meta, scd1MetadataType)
        .add(upperMeta, scd1MetadataType)

      val stream = MemoryStream[(Int, Long)]
      stream.addData((1, 5L))
      val ctx = new TestGraphRegistrationContext(spark) {
        registerTable("target", catalog = Some(catalog), database = Some(namespace),
          specifiedSchema = Some(declaredSchema))
        registerFlow(autoCdcFlow(name = "auto_cdc_flow", target = "target",
          query = dfFlowFunc(stream.toDF().toDF("id", "version")),
          keys = Seq("id"), sequencing = functions.col("version")))
        registerMaterializedView("copy", catalog = Some(catalog), database = Some(namespace),
          query = readFlowFunc(s"$catalog.$namespace.target"))
      }

      val ex = intercept[RuntimeException] { runPipeline(ctx) }
      checkErrorInPipelineFailure(
        failure = ex,
        condition = "AUTOCDC_INVALID_RESERVED_SCHEMA.DUPLICATE_COLUMN",
        parameters = Map(
          "tableName" ->
            fullyQualifiedIdentifier("target", Some(catalog), Some(namespace)).unquotedString,
          "columnNames" -> s"`$meta`, `$upperMeta`",
          "engineColumnName" -> meta))
    }
  }
}
