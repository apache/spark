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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.functions
import org.apache.spark.sql.pipelines.autocdc.{ChangeArgs, ScdType, UnqualifiedColumnName}
import org.apache.spark.sql.pipelines.utils.{PipelineTest, TestGraphRegistrationContext}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType

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
 * `__spark_autocdc_metadata` column, so a declared schema listing only the data columns is
 * incompatible (it is missing the metadata column) while one that also includes the metadata
 * column is compatible.
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
  private def autoCdcGraph(flowName: String, declaredSchema: Option[StructType]): DataflowGraph = {
    val ctx = new TestGraphRegistrationContext(spark)
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
        sequencing = functions.col("version"),
        columnSelection = None,
        deleteCondition = None,
        storedAsScdType = ScdType.Type1)))
    ctx.resolveToDataflowGraph()
  }

  /** The full inferred AUTO CDC output schema (data columns plus the reserved metadata column). */
  private def autoCdcInferredSchema(flowName: String): StructType =
    autoCdcGraph(flowName, declaredSchema = None).inferredSchema(targetIdentifier)

  private def assertSchemaIncompatible(graph: DataflowGraph): Unit = {
    val ex = intercept[AnalysisException](graph.validate())
    assert(ex.getCondition == "USER_SPECIFIED_AND_INFERRED_SCHEMA_NOT_COMPATIBLE")
  }

  // Plain flows: the inferred schema is exactly the source's data columns.

  test("compatible user-specified schema is accepted for an implicit plain flow") {
    plainGraph(flowName = "target", declaredSchema = Some(dataSchema)).validate()
  }

  test("incompatible user-specified schema is rejected for an implicit plain flow") {
    assertSchemaIncompatible(plainGraph(flowName = "target", declaredSchema = Some(
      dataSchemaMissingColumn)))
  }

  test("compatible user-specified schema is accepted for a named plain flow") {
    plainGraph(flowName = "plain_flow", declaredSchema = Some(dataSchema)).validate()
  }

  test("incompatible user-specified schema is rejected for a named plain flow") {
    assertSchemaIncompatible(plainGraph(flowName = "plain_flow", declaredSchema = Some(
      dataSchemaMissingColumn)))
  }

  // AUTO CDC flows: the inferred schema appends a reserved metadata column to the data columns.

  test("data-only user-specified schema is rejected for an implicit AUTO CDC flow") {
    // Schema lists the data columns but omits the appended metadata column, so it is incompatible.
    assertSchemaIncompatible(autoCdcGraph(flowName = "target", declaredSchema = Some(dataSchema)))
  }

  test("data-only user-specified schema is rejected for a named AUTO CDC flow") {
    assertSchemaIncompatible(
      autoCdcGraph(flowName = "auto_cdc_flow", declaredSchema = Some(dataSchema)))
  }

  test("full user-specified schema is accepted for an implicit AUTO CDC flow") {
    // Schema includes the appended metadata column, matching the inferred schema exactly.
    autoCdcGraph(
      flowName = "target",
      declaredSchema = Some(autoCdcInferredSchema("target"))).validate()
  }

  test("full user-specified schema is accepted for a named AUTO CDC flow") {
    autoCdcGraph(
      flowName = "auto_cdc_flow",
      declaredSchema = Some(autoCdcInferredSchema("auto_cdc_flow"))).validate()
  }
}
