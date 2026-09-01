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
  Scd1BatchProcessor,
  ScdType,
  UnqualifiedColumnName
}
import org.apache.spark.sql.pipelines.utils.{PipelineTest, TestGraphRegistrationContext}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.LongType

/**
 * Tests for [[AutoCdcAuxiliaryTable.buildAuxiliaryTableSpecFor]] on the SCD1 branch
 * (`buildScd1AuxiliaryTableSpecFor`), exercised through the graph the same way production does:
 * a resolved SCD1 AutoCDC flow yields an [[AutoCdcAuxiliaryTableSpec]] via
 * [[DataflowGraph.auxiliaryTableSpecs]].
 *
 * Unlike the SCD2 auxiliary table (which stores full hidden rows), the SCD1 auxiliary table
 * stores only the key columns plus the CDC metadata column: it tracks per-key tombstones, not
 * historical rows. Its schema is therefore keys + `_cdc_metadata`, with no user data columns.
 */
class AutoCdcScd1AuxiliaryTableSpecSuite extends PipelineTest with SharedSparkSession {

  import testImplicits._

  private def targetIdentifier = fullyQualifiedIdentifier("target")

  /** Source change feed with data columns `(id, name, version)`. */
  private def sourceDf = {
    val stream = MemoryStream[(Int, String, Long)]
    stream.addData((1, "alice", 1L))
    stream.toDF().toDF("id", "name", "version")
  }

  /** Resolve a graph with a single SCD1 AUTO CDC flow into `target` and return its aux spec. */
  private def scd1AuxSpec(
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
        storedAsScdType = ScdType.Type1)))
    val graph = ctx.resolveToDataflowGraph()
    val inferredSchemas = graph.inferSchemas(spark.sessionState.conf.caseSensitiveAnalysis)
    graph.auxiliaryTableSpecs(inferredSchemas)(targetIdentifier)
      .asInstanceOf[AutoCdcAuxiliaryTableSpec]
  }

  test("SCD1 aux schema is exactly the key columns plus the CDC metadata column") {
    val spec = scd1AuxSpec()
    // Only the key (id) and the metadata column -- no user data columns (name / version) and no
    // SCD2 framework columns.
    assert(
      spec.schema.fieldNames.toSeq == Seq("id", AutoCdcReservedNames.cdcMetadataColName))
  }

  test("SCD1 aux metadata column uses the SCD1 metadata struct schema") {
    val spec = scd1AuxSpec()
    val metaField = spec.schema(AutoCdcReservedNames.cdcMetadataColName)
    // version is a Long, so the SCD1 metadata struct is typed on LongType.
    assert(metaField.dataType == Scd1BatchProcessor.cdcMetadataColSchema(LongType))
    assert(!metaField.nullable, "the CDC metadata column is non-null")
  }

  test("SCD1 aux schema omits user data columns") {
    val fieldNames = scd1AuxSpec().schema.fieldNames.toSeq
    assert(!fieldNames.contains("name"))
    assert(!fieldNames.contains("version"))
  }

  test("SCD1 aux spec records the SCD1 type, key names, and matching identifier") {
    val spec = scd1AuxSpec()
    assert(spec.expectedScdType == ScdType.Type1)
    assert(spec.properties(AutoCdcAuxiliaryTable.scdTypePropertyKey) == ScdType.Type1.label)
    assert(spec.expectedKeyFields.map(_.name) == Seq("id"))
    assert(
      AutoCdcAuxiliaryTable.parseColumnNames(
        spec.properties(AutoCdcAuxiliaryTable.keyColumnNamesProperty)).contains(Seq("id")))
    assert(spec.identifier == AutoCdcAuxiliaryTable.identifier(targetIdentifier))
    assert(spec.targetTableIdentifier == targetIdentifier)
  }

  test("SCD1 aux spec carries all declared key fields for a composite key") {
    val spec = scd1AuxSpec(keys = Seq("id", "name"))
    assert(spec.expectedKeyFields.map(_.name) == Seq("id", "name"))
    assert(
      AutoCdcAuxiliaryTable.parseColumnNames(
        spec.properties(AutoCdcAuxiliaryTable.keyColumnNamesProperty)).contains(Seq("id", "name")))
    // The aux schema is the two keys followed by the metadata column, and nothing else.
    assert(
      spec.schema.fieldNames.toSeq ==
      Seq("id", "name", AutoCdcReservedNames.cdcMetadataColName))
  }
}
