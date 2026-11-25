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

import scala.collection.mutable

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.classic.DataFrame
import org.apache.spark.sql.connector.catalog.{
  CatalogV2Util,
  Identifier,
  InMemoryRowLevelOperationTableCatalog,
  Table => V2Table,
  TableChange
}
import org.apache.spark.sql.functions
import org.apache.spark.sql.pipelines.autocdc.{ChangeArgs, ScdType, UnqualifiedColumnName}
import org.apache.spark.sql.pipelines.utils.{BaseCoreExecutionTest, TestGraphRegistrationContext}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

/**
 * Materialization tests for the AutoCDC auxiliary table, the internal table created and evolved
 * alongside an AutoCDC target. Its schema is derived from the target's inferred schema, not from
 * the target's declared schema, so it can differ from the persisted auxiliary table in ways the
 * declared target does not.
 *
 * Only the dataset materialization step runs here, against a catalog that records every
 * `alterTable`, so a test can assert whether a run altered the auxiliary table at all.
 */
class AutoCdcAuxiliaryTableMaterializationSuite
    extends BaseCoreExecutionTest
    with SharedSparkSession {

  private val catalogName = "aux_recording_cat"
  private val namespace = "aux_ns"

  private def targetIdentifier: TableIdentifier =
    fullyQualifiedIdentifier("target", Some(catalogName), Some(namespace))

  private def auxiliaryIdentifier: Identifier =
    Identifier.of(Array(namespace), AutoCdcAuxiliaryTable.identifier(targetIdentifier).table)

  /**
   * Registers [[RecordingInMemoryRowLevelOperationTableCatalog]] under `catalogName`, creates
   * `namespace`, runs `body`, then tears the registration back down.
   */
  private def withRecordingCatalog(body: => Unit): Unit = {
    spark.conf.set(
      s"spark.sql.catalog.$catalogName",
      classOf[RecordingInMemoryRowLevelOperationTableCatalog].getName)
    try {
      spark.sql(s"CREATE NAMESPACE IF NOT EXISTS $catalogName.$namespace")
      body
    } finally {
      spark.sessionState.catalogManager.reset()
      spark.sessionState.conf.unsetConf(s"spark.sql.catalog.$catalogName")
    }
  }

  private def recordingCatalog: RecordingInMemoryRowLevelOperationTableCatalog =
    spark.sessionState.catalogManager
      .catalog(catalogName)
      .asInstanceOf[RecordingInMemoryRowLevelOperationTableCatalog]

  private def auxiliaryTable: V2Table = recordingCatalog.loadTable(auxiliaryIdentifier)

  private def auxiliarySchema: StructType =
    CatalogV2Util.v2ColumnsToStructType(auxiliaryTable.columns())

  /**
   * A streaming change feed whose schema is exactly `sourceSchema`, field comments included. Every
   * column is a typed null cast to the field's type, so that nested field comments come from the
   * cast's type, aliased with the field's own metadata. Only the schema matters here: nothing is
   * ever read from the feed. (A `MemoryStream` would not do: its schema is rebuilt from the encoder
   * and loses field metadata.)
   */
  private def changeFeed(sourceSchema: StructType): DataFrame =
    spark.readStream.format("rate").load().select(sourceSchema.fields.toSeq.map { field =>
      functions.lit(null).cast(field.dataType).as(field.name, field.metadata)
    }: _*)

  /**
   * An SCD type 2 AutoCDC target with the given declared schema, written by a single AutoCDC flow
   * reading a change feed of `sourceSchema`, resolved and validated as a pipeline update would.
   */
  private def scd2Graph(
      sourceSchema: StructType,
      declaredSchema: Option[StructType]): DataflowGraph = {
    val ctx = new TestGraphRegistrationContext(spark)
    ctx.registerTable(
      "target",
      specifiedSchema = declaredSchema,
      catalog = Some(catalogName),
      database = Some(namespace))
    ctx.registerFlow(AutoCdcFlow(
      identifier = fullyQualifiedIdentifier("cdc_flow", Some(catalogName), Some(namespace)),
      destinationIdentifier = targetIdentifier,
      func = ctx.dfFlowFunc(changeFeed(sourceSchema)),
      queryContext = QueryContext(
        currentCatalog = Some(catalogName),
        currentDatabase = Some(namespace)),
      origin = QueryOrigin.empty,
      changeArgs = ChangeArgs(
        keys = Seq(UnqualifiedColumnName("id")),
        sequencing = functions.col("version"),
        storedAsScdType = ScdType.Type2)))
    ctx.resolveToDataflowGraph().validate(spark.sessionState.conf.caseSensitiveAnalysis)
  }

  /**
   * The target schema inferred from a change feed of `sourceSchema`: its columns plus the SCD2
   * framework columns.
   */
  private def inferredTargetSchema(sourceSchema: StructType): StructType =
    scd2Graph(sourceSchema, declaredSchema = None)
      .inferSchemas(spark.sessionState.conf.caseSensitiveAnalysis)(targetIdentifier)

  private def materialize(graph: DataflowGraph): Unit =
    materializeGraph(graph, storageRoot = storageRoot)

  test("SPARK-59268: a comment change under consecutive arrays in the change feed of a " +
    "declared SCD2 target neither fails the update nor alters the auxiliary table") {
    withRecordingCatalog {
      def sourceSchema(nestedComment: String): StructType = new StructType()
        .add("id", IntegerType)
        .add("version", LongType)
        .add("arr", ArrayType(ArrayType(
          new StructType().add("a", IntegerType, nullable = true, nestedComment))))
      def innerComment(schema: StructType): Option[String] = schema("arr").dataType match {
        case ArrayType(ArrayType(inner: StructType, _), _) => inner("a").getComment()
      }
      // The target declares its full schema, reserved AutoCDC columns included, with the comment
      // the change feed starts out with.
      val declaredSchema = inferredTargetSchema(sourceSchema("c1"))
      assert(innerComment(declaredSchema) === Some("c1"))

      materialize(scd2Graph(sourceSchema("c1"), Some(declaredSchema)))
      assert(recordingCatalog.recordedAlters.isEmpty)
      val auxiliaryColumns = auxiliaryTable.columns()
      assert(innerComment(auxiliarySchema) === Some("c1"))

      // The source comment on `arr.element.element.a` changes. The declared target still validates
      // and has nothing to change, while the derived auxiliary schema now differs from the
      // persisted auxiliary table only in that comment, which sits under array<array<struct>> and
      // cannot be evolved in place. The auxiliary table keeps its own metadata, so the run
      // succeeds and does not alter it.
      materialize(scd2Graph(sourceSchema("c2"), Some(declaredSchema)))
      assert(recordingCatalog.recordedAlters.isEmpty,
        s"expected no alter, got: ${recordingCatalog.recordedAlters}")
      assert(auxiliaryTable.columns() sameElements auxiliaryColumns)
      assert(innerComment(auxiliarySchema) === Some("c1"))
    }
  }

  test("SPARK-59268: a top-level comment change in the change feed of a declared SCD2 target " +
    "does not alter the auxiliary table") {
    withRecordingCatalog {
      def sourceSchema(nameComment: String): StructType = new StructType()
        .add("id", IntegerType)
        .add("version", LongType)
        .add("name", StringType, nullable = true, nameComment)
      val declaredSchema = inferredTargetSchema(sourceSchema("n1"))
      assert(declaredSchema("name").getComment() === Some("n1"))

      materialize(scd2Graph(sourceSchema("n1"), Some(declaredSchema)))
      assert(recordingCatalog.recordedAlters.isEmpty)
      val auxiliaryColumns = auxiliaryTable.columns()
      assert(auxiliarySchema("name").getComment() === Some("n1"))

      // Only the derived auxiliary schema carries the new comment on `name`; the auxiliary table
      // keeps the comment it was created with rather than being altered to follow the source.
      materialize(scd2Graph(sourceSchema("n2"), Some(declaredSchema)))
      assert(recordingCatalog.recordedAlters.isEmpty,
        s"expected no alter, got: ${recordingCatalog.recordedAlters}")
      assert(auxiliaryTable.columns() sameElements auxiliaryColumns)
      assert(auxiliarySchema("name").getComment() === Some("n1"))
    }
  }
}

/**
 * An [[InMemoryRowLevelOperationTableCatalog]] (so that it can host an AutoCDC target) that
 * records every `alterTable` invocation, with the altered table's identifier, while still applying
 * it.
 */
class RecordingInMemoryRowLevelOperationTableCatalog
    extends InMemoryRowLevelOperationTableCatalog {
  val recordedAlters: mutable.ArrayBuffer[(Identifier, Seq[TableChange])] =
    mutable.ArrayBuffer.empty

  override def alterTable(ident: Identifier, changes: TableChange*): V2Table = {
    recordedAlters += ident -> changes.toSeq
    super.alterTable(ident, changes: _*)
  }
}
