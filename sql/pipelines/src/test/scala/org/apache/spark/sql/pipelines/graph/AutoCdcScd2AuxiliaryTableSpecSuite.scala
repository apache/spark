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
import org.apache.spark.sql.functions
import org.apache.spark.sql.pipelines.autocdc.{
  AutoCdcReservedNames,
  ChangeArgs,
  ColumnSelection,
  Scd2BatchProcessor,
  ScdType,
  UnqualifiedColumnName
}
import org.apache.spark.sql.pipelines.utils.{PipelineTest, TestGraphRegistrationContext}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{LongType, StructField, StructType}

/**
 * Tests for [[AutoCdcAuxiliaryTable.buildAuxiliaryTableSpecFor]] on the SCD2 branch
 * (`buildScd2AuxiliaryTableSpecFor`), exercised through the graph the same way production does:
 * a resolved SCD2 AutoCDC flow yields an [[AutoCdcAuxiliaryTableSpec]] via
 * [[DataflowGraph.auxiliaryTableSpecs]].
 *
 * The SCD2 auxiliary table stores full hidden rows (tombstones and coalesced no-op upserts), so
 * its schema is the entire SCD2 target row schema plus the aux-only
 * [[Scd2BatchProcessor.deletedByBatchIdColName]] logical-delete marker -- richer than the SCD1
 * auxiliary table, which stores only keys + CDC metadata.
 */
class AutoCdcScd2AuxiliaryTableSpecSuite extends PipelineTest with SharedSparkSession {

  private def targetIdentifier = fullyQualifiedIdentifier("target")

  /** Source change feed with data columns `(id, name, version)`. */
  private def sourceDf = {
    val session = spark
    import session.implicits._
    val stream = MemoryStream[(Int, String, Long)]
    stream.addData((1, "alice", 1L))
    stream.toDF().toDF("id", "name", "version")
  }

  /** Resolve a graph with a single SCD2 AUTO CDC flow into `target` and return its aux spec. */
  private def scd2AuxSpec(
      keys: Seq[String] = Seq("id"),
      columnSelection: Option[ColumnSelection] = None): AutoCdcAuxiliaryTableSpec = {
    val ctx = new TestGraphRegistrationContext(spark)
    ctx.registerTable("target")
    ctx.registerFlow(AutoCdcFlow(
      identifier = targetIdentifier,
      destinationIdentifier = targetIdentifier,
      func = ctx.dfFlowFunc(sourceDf),
      queryContext = QueryContext(
        currentCatalog = catalogInPipelineSpec,
        currentDatabase = databaseInPipelineSpec),
      origin = QueryOrigin.empty,
      changeArgs = ChangeArgs(
        keys = keys.map(k => UnqualifiedColumnName(Seq(k))),
        sequencing = functions.col("version"),
        columnSelection = columnSelection,
        deleteCondition = None,
        storedAsScdType = ScdType.Type2)))
    val graph = ctx.resolveToDataflowGraph()
    graph.auxiliaryTableSpecs(targetIdentifier).asInstanceOf[AutoCdcAuxiliaryTableSpec]
  }

  /** The SCD2 target (inferred) schema for the default single-flow graph. */
  private def scd2TargetSchema(): StructType = {
    val ctx = new TestGraphRegistrationContext(spark)
    ctx.registerTable("target")
    ctx.registerFlow(AutoCdcFlow(
      identifier = targetIdentifier,
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
        storedAsScdType = ScdType.Type2)))
    ctx.resolveToDataflowGraph().inferredSchema(targetIdentifier)
  }

  test("SCD2 aux schema is the full target schema plus the deleted-by-batch-id marker") {
    val targetSchema = scd2TargetSchema()
    val spec = scd2AuxSpec()

    // The target schema already carries the SCD2 framework columns (__START_AT / __END_AT /
    // _cdc_metadata); the aux schema is exactly that, with the logical-delete marker appended.
    val expected = StructType(
      targetSchema.fields :+
        StructField(Scd2BatchProcessor.deletedByBatchIdColName, LongType, nullable = true)
    )
    assert(spec.schema == expected)
  }

  test("SCD2 aux schema ends with a nullable long deleted-by-batch-id column") {
    val spec = scd2AuxSpec()
    val marker = spec.schema.fields.last
    assert(marker.name == Scd2BatchProcessor.deletedByBatchIdColName)
    assert(marker.dataType == LongType)
    assert(marker.nullable, "the logical-delete marker must be nullable (null on live rows)")
  }

  test("SCD2 aux schema retains the framework columns from the target row schema") {
    val fieldNames = scd2AuxSpec().schema.fieldNames.toSeq
    Seq(
      Scd2BatchProcessor.startAtColName,
      Scd2BatchProcessor.endAtColName,
      AutoCdcReservedNames.cdcMetadataColName,
      Scd2BatchProcessor.deletedByBatchIdColName
    ).foreach { c =>
      assert(fieldNames.contains(c), s"expected $c in aux schema $fieldNames")
    }
  }

  test("SCD2 aux spec records the SCD2 type, key names, and matching identifier") {
    val spec = scd2AuxSpec()
    assert(spec.expectedScdType == ScdType.Type2)
    assert(spec.properties(AutoCdcAuxiliaryTable.scdTypePropertyKey) == ScdType.Type2.label)
    assert(spec.expectedKeyFields.map(_.name) == Seq("id"))
    assert(
      AutoCdcAuxiliaryTable.parseKeyColumnNames(
        spec.properties(AutoCdcAuxiliaryTable.keyColumnNamesProperty)).contains(Seq("id")))
    assert(spec.identifier == AutoCdcAuxiliaryTable.identifier(targetIdentifier))
    assert(spec.targetTableIdentifier == targetIdentifier)
  }

  test("SCD2 aux spec carries all declared key fields for a composite key") {
    val spec = scd2AuxSpec(keys = Seq("id", "name"))
    assert(spec.expectedKeyFields.map(_.name) == Seq("id", "name"))
    assert(
      AutoCdcAuxiliaryTable.parseKeyColumnNames(
        spec.properties(AutoCdcAuxiliaryTable.keyColumnNamesProperty)).contains(Seq("id", "name")))
    // Both keys survive into the aux schema.
    assert(spec.schema.fieldNames.contains("id"))
    assert(spec.schema.fieldNames.contains("name"))
  }
}
