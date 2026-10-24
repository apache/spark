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

package org.apache.spark.sql.execution.datasources.v2

import java.util

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.ProjectingInternalRow
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, ReplaceData, Subquery}
import org.apache.spark.sql.catalyst.util.ReplaceDataProjections
import org.apache.spark.sql.connector.catalog.{Changelog, ChangelogContext, ChangelogRange, Column, SupportsRead, SupportsRowLevelOperations, Table, TableCapability}
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, ScanContext}
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, RowLevelOperation, RowLevelOperationBuilder, RowLevelOperationInfo, RowLevelOperationTable, WriteBuilder}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType, TimestampType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class ScanContextPropagationSuite extends PlanTest {
  private val readPaths = Seq("ordinary", "row-level", "changelog")

  private def options: CaseInsensitiveStringMap = {
    new CaseInsensitiveStringMap(Map("MiXeD" -> "value").asJava)
  }

  private def readableTable(
      path: String,
      recorder: Recorder,
      contextAware: Boolean): Table with SupportsRead = path match {
    case "ordinary" =>
      if (contextAware) {
        new LegacyTable(recorder) {
          override def newScanBuilder(
              context: ScanContext,
              options: CaseInsensitiveStringMap): ScanBuilder = {
            recorder.newBuilder(Some(context), options)
          }
        }
      } else {
        new LegacyTable(recorder)
      }

    case "row-level" =>
      val operation = if (contextAware) {
        new LegacyOperation(recorder) {
          override def newScanBuilder(
              context: ScanContext,
              options: CaseInsensitiveStringMap): ScanBuilder = {
            recorder.newBuilder(Some(context), options)
          }
        }
      } else {
        new LegacyOperation(recorder)
      }
      RowLevelOperationTable(new LegacyTable(recorder), operation)

    case "changelog" =>
      val changelog = if (contextAware) {
        new LegacyChangelog(recorder) {
          override def newScanBuilder(
              context: ScanContext,
              options: CaseInsensitiveStringMap): ScanBuilder = {
            recorder.newBuilder(Some(context), options)
          }
        }
      } else {
        new LegacyChangelog(recorder)
      }
      val context = new ChangelogContext(
        new ChangelogRange.UnboundedRange(), ChangelogContext.DeduplicationMode.NONE, false)
      ChangelogTable(changelog, context)

    case other => fail(s"Unknown read path: $other")
  }

  for (path <- readPaths) {
    test(s"$path context overload preserves legacy options-only implementations") {
      val recorder = new Recorder
      val table = readableTable(path, recorder, contextAware = false)
      val readOptions = options

      assert(table.newScanBuilder(new ScanContext(true), readOptions).build() eq recorder.scan)
      assert(recorder.calls.size == 1)
      assert(recorder.calls.head._1.isEmpty)
      assert(recorder.calls.head._2 eq readOptions)
    }

    test(s"$path forwards explicit context and preserves options-only dispatch") {
      val recorder = new Recorder
      val table = readableTable(path, recorder, contextAware = true)
      val readOptions = options
      val context = new ScanContext(true)

      assert(table.newScanBuilder(context, readOptions).build() eq recorder.scan)
      assert(recorder.calls.size == 1)
      assert(recorder.calls.head._1.get eq context)
      assert(recorder.calls.head._2 eq readOptions)

      assert(table.newScanBuilder(readOptions).build() eq recorder.scan)
      assert(recorder.calls.size == 2)
      assert(recorder.calls.last._1.isEmpty)
      assert(recorder.calls.last._2 eq readOptions)
    }

    for (inSubquery <- Seq(false, true)) {
      test(s"$path V2 pushdown supplies scan context: inSubquery=$inSubquery") {
        val recorder = new Recorder
        val table = readableTable(path, recorder, contextAware = true)
        val readOptions = options
        val relation = DataSourceV2Relation.create(table, None, None, readOptions)
        val plan = if (inSubquery) Subquery(relation, correlated = false) else relation

        val optimized = V2ScanRelationPushDown(plan)

        val scans = optimized.collect { case scan: DataSourceV2ScanRelation => scan }
        assert(scans.size == 1)
        assert(scans.head.scan eq recorder.scan)
        assert(recorder.calls.size == 1)
        assert(recorder.calls.head._1.get.isInSubquery() == inSubquery)
        assert(recorder.calls.head._2 eq readOptions)
      }
    }
  }

  for (inSubquery <- Seq(false, true)) {
    test(s"group-based row-level planning supplies fresh context: inSubquery=$inSubquery") {
      val recorder = new Recorder
      val table = readableTable("row-level", recorder, contextAware = true)
      val readOptions = options
      val relation = DataSourceV2Relation.create(table, None, None, readOptions)
      val rowProjection = ProjectingInternalRow(recorder.schema, recorder.schema.indices)
      val replace = ReplaceData(
        relation, TrueLiteral, relation, relation, ReplaceDataProjections(rowProjection, None))
      val plan: LogicalPlan = if (inSubquery) Subquery(replace, correlated = false) else replace

      for (_ <- 0 until 2) {
        val optimized = GroupBasedRowLevelOperationScanPlanning(plan)
        val scans = optimized.collect { case scan: DataSourceV2ScanRelation => scan }
        assert(scans.size == 1)
        assert(scans.head.scan eq recorder.scan)
      }

      assert(recorder.calls.size == 2)
      assert(recorder.calls.forall(_._1.get.isInSubquery() == inSubquery))
      assert(recorder.calls.forall(_._2 eq readOptions))
      assert(recorder.calls.head._1.get ne recorder.calls.last._1.get)
    }
  }

  private class Recorder {
    val schema: StructType = new StructType()
      .add("id", IntegerType)
      .add("_change_type", StringType)
      .add("_commit_version", LongType)
      .add("_commit_timestamp", TimestampType)
    val calls: ArrayBuffer[(Option[ScanContext], CaseInsensitiveStringMap)] = ArrayBuffer.empty
    val scan: Scan = new Scan {
      override def readSchema(): StructType = schema
    }

    def columns: Array[Column] = schema.fields.map(f => Column.create(f.name, f.dataType))

    def newBuilder(
        context: Option[ScanContext],
        options: CaseInsensitiveStringMap): ScanBuilder = {
      calls += ((context, options))
      new ScanBuilder {
        override def build(): Scan = scan
      }
    }
  }

  private class LegacyTable(recorder: Recorder)
    extends Table with SupportsRead with SupportsRowLevelOperations {

    override def name(): String = "scan_context_test"
    override def columns(): Array[Column] = recorder.columns
    override def capabilities(): util.Set[TableCapability] = {
      util.EnumSet.of(TableCapability.BATCH_READ)
    }
    override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
      recorder.newBuilder(None, options)
    }
    override def newRowLevelOperationBuilder(
        info: RowLevelOperationInfo): RowLevelOperationBuilder = {
      throw new UnsupportedOperationException("operation is supplied by the test")
    }
  }

  private class LegacyOperation(recorder: Recorder) extends RowLevelOperation {
    override def command(): RowLevelOperation.Command = RowLevelOperation.Command.DELETE
    override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
      recorder.newBuilder(None, options)
    }
    override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
      throw new UnsupportedOperationException("scan planning does not build a write")
    }
  }

  private class LegacyChangelog(recorder: Recorder) extends Changelog {
    override def name(): String = "scan_context_changes"
    override def columns(): Array[Column] = recorder.columns
    override def containsCarryoverRows(): Boolean = false
    override def containsIntermediateChanges(): Boolean = false
    override def representsUpdateAsDeleteAndInsert(): Boolean = false
    override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
      recorder.newBuilder(None, options)
    }
  }
}
