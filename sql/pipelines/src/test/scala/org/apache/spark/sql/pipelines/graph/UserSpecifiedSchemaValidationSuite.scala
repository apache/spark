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

import org.apache.spark.sql.{AnalysisException, Column}
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.functions
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.pipelines.autocdc.{
  AutoCdcReservedNames,
  ChangeArgs,
  ScdType,
  UnqualifiedColumnName
}
import org.apache.spark.sql.pipelines.utils.{PipelineTest, TestGraphRegistrationContext}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{ArrayType, DataType, IntegerType, LongType, StringType,
  StructField, StructType}

/**
 * Tests for `GraphValidations.validateUserSpecifiedSchemas`, which requires a table's
 * user-declared schema to match the schema inferred from its incoming flows.
 *
 * The validation must apply regardless of whether the incoming flow's identifier equals the
 * destination table's identifier (an implicit/default flow) or differs from it (a separately-named
 * flow). Named flows previously bypassed the check because the table lookup was keyed on the flow
 * identifier rather than the destination identifier (SPARK-58116). The validation operates on the
 * resolved dataflow graph, so the flow type is immaterial to that lookup; this suite covers both
 * plain flows and AUTO CDC flows, in the implicit and named forms, for compatible and incompatible
 * declared schemas.
 *
 * For an AUTO CDC flow the inferred schema is the source's data columns plus an appended reserved
 * `__spark_autocdc_metadata` column. The reserved column is engine-owned (SPARK-58118): a declared
 * schema listing only the data columns is accepted, with the engine appending the reserved
 * column(s) at materialization. A declared schema that also includes the metadata column stays
 * accepted, and any mismatch in the data columns themselves is still rejected.
 */
class UserSpecifiedSchemaValidationSuite extends PipelineTest with SharedSparkSession {

  /** Source change feed with data columns `(id, name, version)`. */
  private def sourceDf = {
    val session = spark
    import session.implicits._
    val stream = MemoryStream[(Int, String, Long)]
    stream.addData((1, "alice", 1L))
    stream.toDF().toDF("id", "name", "version")
  }

  /** The data-column schema produced by a plain flow (and the pre-metadata AUTO CDC schema). */
  private def dataSchema: StructType = sourceDf.schema

  /** A declared schema that omits a data column the flow produces, hence incompatible. */
  private def dataSchemaMissingColumn: StructType = StructType(dataSchema.dropRight(1))

  private def targetIdentifier = fullyQualifiedIdentifier("target")

  /** Registers a plain flow into `target`; `flowName == "target"` yields an implicit flow. */
  private def plainGraph(flowName: String, declaredSchema: Option[StructType]): DataflowGraph = {
    val ctx = new TestGraphRegistrationContext(spark)
    if (flowName == "target") {
      ctx.registerTable(
        "target",
        query = Some(ctx.dfFlowFunc(sourceDf)),
        specifiedSchema = declaredSchema)
    } else {
      ctx.registerTable("target", specifiedSchema = declaredSchema)
      ctx.registerFlow(
        destinationName = "target", name = flowName, query = ctx.dfFlowFunc(sourceDf))
    }
    ctx.resolveToDataflowGraph()
  }

  /** Registers an AUTO CDC flow into `target`; `flowName == "target"` yields an implicit flow. */
  private def autoCdcGraph(
      flowName: String,
      declaredSchema: Option[StructType],
      scdType: ScdType = ScdType.Type1,
      sequencing: Column = functions.col("version"),
      confs: Map[String, String] = Map.empty): DataflowGraph = {
    val ctx = new TestGraphRegistrationContext(spark, confs)
    ctx.registerTable("target", specifiedSchema = declaredSchema)
    ctx.registerFlow(AutoCdcFlow(
      identifier = fullyQualifiedIdentifier(flowName),
      destinationIdentifier = targetIdentifier,
      func = ctx.dfFlowFunc(sourceDf),
      queryContext = QueryContext(
        currentCatalog = catalogInPipelineSpec,
        currentDatabase = databaseInPipelineSpec),
      origin = QueryOrigin.empty,
      changeArgs = ChangeArgs(
        keys = Seq(UnqualifiedColumnName(Seq("id"))),
        sequencing = sequencing,
        columnSelection = None,
        deleteCondition = None,
        storedAsScdType = scdType)))
    ctx.resolveToDataflowGraph()
  }

  /** The full inferred AUTO CDC output schema (data columns plus the reserved metadata column). */
  private def autoCdcInferredSchema(
      flowName: String,
      scdType: ScdType = ScdType.Type1,
      sequencing: Column = functions.col("version")): StructType =
    autoCdcGraph(flowName, declaredSchema = None, scdType, sequencing)
      .inferSchemas(spark.sessionState.conf.caseSensitiveAnalysis)(targetIdentifier)

  private def validateGraph(graph: DataflowGraph): DataflowGraph =
    graph.validate(spark.sessionState.conf.caseSensitiveAnalysis)

  private def assertSchemaIncompatible(graph: DataflowGraph): Unit = {
    val ex = intercept[AnalysisException](validateGraph(graph))
    assert(ex.getCondition == "USER_SPECIFIED_AND_INFERRED_SCHEMA_NOT_COMPATIBLE")
    assert(ex.getMessage.contains(targetIdentifier.unquotedString))
  }

  // Plain flows: the inferred schema is exactly the source's data columns.

  test("compatible user-specified schema is accepted for an implicit plain flow") {
    validateGraph(plainGraph(flowName = "target", declaredSchema = Some(dataSchema)))
  }

  test("incompatible user-specified schema is rejected for an implicit plain flow") {
    assertSchemaIncompatible(plainGraph(flowName = "target", declaredSchema = Some(
      dataSchemaMissingColumn)))
  }

  test("compatible user-specified schema is accepted for a named plain flow") {
    validateGraph(plainGraph(flowName = "plain_flow", declaredSchema = Some(dataSchema)))
  }

  test("incompatible user-specified schema is rejected for a named plain flow") {
    assertSchemaIncompatible(plainGraph(flowName = "plain_flow", declaredSchema = Some(
      dataSchemaMissingColumn)))
  }

  test("user-specified schema validation uses pipeline case sensitivity, not session default") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      val ctx = new TestGraphRegistrationContext(
        spark,
        Map(SQLConf.CASE_SENSITIVE.key -> "true")) {
        val session = spark
        import session.implicits._

        registerView("src", query = dfFlowFunc(Seq((1, "alice")).toDF("id", "value")))
        registerTable(
          "target",
          specifiedSchema = Some(
            new StructType().add("id", IntegerType).add("value", StringType)))
        registerFlow(
          destinationName = "target",
          name = "case_sensitive_flow",
          query = sqlFlowFunc(spark, "SELECT id, value AS Value FROM src"))
      }

      assertSchemaIncompatible(ctx.resolveToDataflowGraph())
    }
  }

  test("user-specified schema validation uses case sensitivity inherited from upstream view") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      val ctx = new TestGraphRegistrationContext(spark) {
        val session = spark
        import session.implicits._

        registerPersistedView(
          "src",
          query = dfFlowFunc(Seq((1, "alice")).toDF("id", "value")),
          sqlConf = Map(SQLConf.CASE_SENSITIVE.key -> "true"))
        registerTable(
          "target",
          specifiedSchema = Some(
            new StructType().add("id", IntegerType).add("value", StringType)))
        registerFlow(
          destinationName = "target",
          name = "case_sensitive_flow",
          query = sqlFlowFunc(spark, "SELECT id, value AS Value FROM src"))
      }

      assertSchemaIncompatible(ctx.resolveToDataflowGraph())
    }
  }

  test("a non-AUTO CDC table's declared schema is compared exactly, so omitting a reserved-" +
    "prefixed column the flow produces is rejected") {
    val session = spark
    import session.implicits._
    // A plain (non-AUTO CDC) flow that produces a reserved-prefixed column, with a declared
    // schema that omits it. For a plain flow the reserved prefix is not special, so this must fail
    // validation the same as omitting any other produced column -- the AUTO CDC strip-both
    // relaxation must not apply here (otherwise the column would be accepted and later dropped at
    // materialization).
    val reservedCol = s"${AutoCdcReservedNames.prefix}x"
    val src = Seq((1, "alice", "m")).toDF("id", "name", reservedCol)
    val ctx = new TestGraphRegistrationContext(spark)
    ctx.registerTable(
      "target",
      query = Some(ctx.dfFlowFunc(src)),
      specifiedSchema = Some(new StructType().add("id", IntegerType, nullable = false)
        .add("name", StringType)))
    assertSchemaIncompatible(ctx.resolveToDataflowGraph())
  }

  // AUTO CDC flows: the inferred schema appends a reserved metadata column to the data columns.

  test("data-only user-specified schema is accepted for an implicit AUTO CDC flow") {
    // Schema lists only the data columns; the reserved metadata column is engine-owned and may
    // be omitted from the declared schema.
    autoCdcGraph(flowName = "target", declaredSchema = Some(dataSchema))
      .validate(spark.sessionState.conf.caseSensitiveAnalysis)
  }

  test("data-only user-specified schema is accepted for a named AUTO CDC flow") {
    autoCdcGraph(flowName = "auto_cdc_flow", declaredSchema = Some(dataSchema))
      .validate(spark.sessionState.conf.caseSensitiveAnalysis)
  }

  test("user-specified schema with wrong data columns is still rejected " +
    "for an AUTO CDC flow") {
    // Omitting the reserved metadata column is allowed, but a mismatch in the data columns
    // themselves (here: a missing data column) remains incompatible.
    assertSchemaIncompatible(
      autoCdcGraph(flowName = "auto_cdc_flow", declaredSchema = Some(dataSchemaMissingColumn)))
  }

  test("full user-specified schema is accepted for an implicit AUTO CDC flow") {
    // Schema includes the appended metadata column, matching the inferred schema exactly.
    autoCdcGraph(
      flowName = "target",
      declaredSchema = Some(autoCdcInferredSchema("target"))).validate(
        spark.sessionState.conf.caseSensitiveAnalysis)
  }

  test("full user-specified schema is accepted for a named AUTO CDC flow") {
    autoCdcGraph(
      flowName = "auto_cdc_flow",
      declaredSchema = Some(autoCdcInferredSchema("auto_cdc_flow"))).validate(
        spark.sessionState.conf.caseSensitiveAnalysis)
  }

  test("a user-specified schema that declares the reserved metadata column with the wrong " +
    "type is rejected for an AUTO CDC flow") {
    // Declaring the engine-owned metadata column is allowed, and a partial or reordered nested
    // shape is normalized to the engine's -- but a bare conflicting type (here STRING where the
    // engine produces a struct) is caught when the declared schema is merged with the inferred
    // one, before the AUTO CDC MERGE ever runs, so the target can never be created with a
    // malformed metadata column.
    val wrongTypeMetadata = StructType(
      dataSchema.fields :+ StructField(AutoCdcReservedNames.cdcMetadataColName, StringType))
    assertSchemaIncompatible(
      autoCdcGraph(flowName = "auto_cdc_flow", declaredSchema = Some(wrongTypeMetadata)))
  }

  test("data-only user-specified schema is accepted for an SCD2 AUTO CDC flow") {
    // An SCD2 flow's inferred schema is the data columns plus the SCD2 interval bounds
    // __START_AT / __END_AT plus the reserved metadata column. The interval bounds are part of the
    // SCD2 contract and stay user-visible; only the prefixed metadata column is engine-reserved,
    // so a declared schema that omits just that column is accepted.
    val declaredWithoutReserved = StructType(
      autoCdcInferredSchema("target", ScdType.Type2)
        .filterNot(_.name.startsWith(AutoCdcReservedNames.prefix)))
    autoCdcGraph("target", Some(declaredWithoutReserved), ScdType.Type2)
      .validate(spark.sessionState.conf.caseSensitiveAnalysis)
  }

  /** The engine-owned reserved metadata field from the inferred AUTO CDC schema. */
  private def metadataField: StructField =
    autoCdcInferredSchema("target").fields
      .find(_.name == AutoCdcReservedNames.cdcMetadataColName).get

  test("a declared schema with an unknown reserved-prefixed column is rejected for an AUTO CDC " +
    "flow") {
    // A reserved-prefixed column the engine never produces (a typo, or an extra reserved-namespace
    // column) is rejected up front rather than surviving reconciliation into the target.
    val schema = StructType(
      dataSchema.fields :+ StructField(s"${AutoCdcReservedNames.prefix}bogus", IntegerType))
    val ex = intercept[AnalysisException](
      validateGraph(autoCdcGraph("auto_cdc_flow", Some(schema))))
    assert(ex.getCondition == "AUTOCDC_INVALID_RESERVED_SCHEMA.UNKNOWN_COLUMN")
  }

  test("a declared schema with case-duplicate reserved metadata columns is rejected for an " +
    "AUTO CDC flow") {
    // Under case-insensitive analysis both spellings resolve to the one engine-owned metadata
    // column, which reconciliation cannot honor.
    val meta = metadataField
    val schema = StructType(dataSchema.fields ++
      Array(meta, meta.copy(name = meta.name.toUpperCase(java.util.Locale.ROOT))))
    val ex = intercept[AnalysisException](
      validateGraph(autoCdcGraph("auto_cdc_flow", Some(schema))))
    assert(ex.getCondition == "AUTOCDC_INVALID_RESERVED_SCHEMA.DUPLICATE_COLUMN")
  }

  test("a reserved-prefixed column is matched with the flow's case sensitivity, not the " +
    "session's") {
    // Discriminates the flow-effective resolver from the session one. The session is case-sensitive
    // while the flow sets spark.sql.caseSensitive=false, and the declaration carries an upper-cased
    // reserved-prefixed column the engine never produces. Only the flow's case-insensitive resolver
    // recognizes `__SPARK_AUTOCDC_BOGUS` as reserved and rejects it as UNKNOWN_COLUMN; the
    // session's case-sensitive resolver would miss the lower-case prefix and the column would fall
    // through to a plain schema-mismatch error, so this pins that reserved-field validation uses
    // the flow's resolver (as the rest of AUTO CDC does), not the session default.
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      val bogus = s"${AutoCdcReservedNames.prefix}bogus".toUpperCase(java.util.Locale.ROOT)
      val schema = StructType(dataSchema.fields :+ StructField(bogus, IntegerType))
      val ex = intercept[AnalysisException](validateGraph(autoCdcGraph(
        "auto_cdc_flow", Some(schema), confs = Map(SQLConf.CASE_SENSITIVE.key -> "false"))))
      assert(ex.getCondition == "AUTOCDC_INVALID_RESERVED_SCHEMA.UNKNOWN_COLUMN")
    }
  }

  test("a declared reserved metadata column with an unknown nested field is rejected for an " +
    "AUTO CDC flow") {
    val meta = metadataField
    val metaStruct = meta.dataType.asInstanceOf[StructType]
    val schema = StructType(
      dataSchema.fields :+ meta.copy(dataType = metaStruct.add("bogus", IntegerType)))
    val ex = intercept[AnalysisException](
      validateGraph(autoCdcGraph("auto_cdc_flow", Some(schema))))
    assert(ex.getCondition == "AUTOCDC_INVALID_RESERVED_SCHEMA.UNKNOWN_NESTED_FIELD")
  }

  test("a declared reserved metadata column with case-duplicate nested fields is rejected for an " +
    "AUTO CDC flow") {
    val meta = metadataField
    val metaStruct = meta.dataType.asInstanceOf[StructType]
    val nested = metaStruct.fields.head
    val schema = StructType(dataSchema.fields :+ meta.copy(
      dataType = metaStruct.add(nested.name.toUpperCase(java.util.Locale.ROOT), nested.dataType)))
    val ex = intercept[AnalysisException](
      validateGraph(autoCdcGraph("auto_cdc_flow", Some(schema))))
    assert(ex.getCondition == "AUTOCDC_INVALID_RESERVED_SCHEMA.DUPLICATE_NESTED_FIELD")
  }

  test("a declared reserved metadata column with an unknown nested field under consecutive " +
    "array levels is rejected for an AUTO CDC flow") {
    // AUTO CDC supports a sequencing expression whose type nests a struct under more than one array
    // level, e.g. ARRAY<ARRAY<STRUCT<ordinal: BIGINT>>>. Validation must descend through every
    // array level, matching reconcileReservedType, so a bogus field in the innermost struct is
    // rejected up front instead of being reconciled into the target, where the SCD1 MERGE would
    // then fail on the incompatible sequencing type.
    val arraySeq = functions.expr("array(array(named_struct('ordinal', version)))")
    val meta = autoCdcInferredSchema("target", sequencing = arraySeq).fields
      .find(_.name == AutoCdcReservedNames.cdcMetadataColName).get
    val metaStruct = meta.dataType.asInstanceOf[StructType]

    // The array levels from `dt` down to the first struct, or None if it holds no nested struct.
    def arrayDepthToStruct(dt: DataType): Option[Int] = dt match {
      case a: ArrayType => arrayDepthToStruct(a.elementType).map(_ + 1)
      case _: StructType => Some(0)
      case _ => None
    }
    // Guard against a vacuous pass: the engine must really nest a struct under >= 2 array levels
    // here, otherwise this would exercise the already-covered single-array path.
    val seqField = metaStruct.fields.find(f => arrayDepthToStruct(f.dataType).exists(_ >= 2))
      .getOrElse(fail(s"expected a struct nested under >= 2 array levels in ${metaStruct.sql}"))

    // Declare the same column with a bogus field added to that innermost struct.
    def addBogus(dt: DataType): DataType = dt match {
      case a: ArrayType => a.copy(elementType = addBogus(a.elementType))
      case s: StructType => s.add("bogus", LongType)
      case other => other
    }
    val mutatedMeta = meta.copy(dataType = StructType(metaStruct.fields.map { f =>
      if (f.name == seqField.name) f.copy(dataType = addBogus(f.dataType)) else f
    }))
    val schema = StructType(dataSchema.fields :+ mutatedMeta)

    val ex = intercept[AnalysisException](
      validateGraph(autoCdcGraph("auto_cdc_flow", Some(schema), sequencing = arraySeq)))
    assert(ex.getCondition == "AUTOCDC_INVALID_RESERVED_SCHEMA.UNKNOWN_NESTED_FIELD")
  }
}
