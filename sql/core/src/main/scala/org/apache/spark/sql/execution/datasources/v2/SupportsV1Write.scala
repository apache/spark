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

import java.util.UUID

import scala.collection.JavaConverters._

import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SaveMode}
import org.apache.spark.sql.catalog.v2.{Identifier, StagingTableCatalog, TableCatalog}
import org.apache.spark.sql.catalog.v2.expressions.Transform
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{CannotReplaceMissingTableException, NoSuchTableException, TableAlreadyExistsException}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.sources.v2.SupportsWrite
import org.apache.spark.sql.sources.v2.writer.{V1WriteBuilder, WriteBuilder}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.Utils

/**
 * Physical plan node for v2 create table as select when the catalog does not support staging
 * the table creation.
 *
 * A new table will be created using the schema of the query, and rows from the query are appended.
 * If either table creation or the append fails, the table will be deleted. This implementation is
 * not atomic; for an atomic variant for catalogs that support the appropriate features, see
 * CreateTableAsSelectStagingExec.
 */
case class CreateV1TableAsSelectExec(
    catalog: TableCatalog,
    ident: Identifier,
    partitioning: Seq[Transform],
    plan: LogicalPlan,
    properties: Map[String, String],
    writeOptions: CaseInsensitiveStringMap,
    ifNotExists: Boolean) extends SupportsV1Write {

  import org.apache.spark.sql.catalog.v2.CatalogV2Implicits.IdentifierHelper

  override protected def doExecute(): RDD[InternalRow] = {
    if (catalog.tableExists(ident)) {
      if (ifNotExists) {
        return sparkContext.parallelize(Seq.empty, 1)
      }

      throw new TableAlreadyExistsException(ident)
    }

    Utils.tryWithSafeFinallyAndFailureCallbacks({
      catalog.createTable(
        ident, plan.schema, partitioning.toArray, properties.asJava) match {
        case table: SupportsWrite =>
          writeWithV1(table, SaveMode.Append)

        case _ =>
          // table does not support writes
          throw new SparkException(
            s"Table implementation does not support writes: ${ident.quoted}")
      }
    })(catchBlock = {
      catalog.dropTable(ident)
    })
  }
}

/**
 * Physical plan node for v2 create table as select, when the catalog is determined to support
 * staging table creation.
 *
 * A new table will be created using the schema of the query, and rows from the query are appended.
 * The CTAS operation is atomic. The creation of the table is staged and the commit of the write
 * should bundle the commitment of the metadata and the table contents in a single unit. If the
 * write fails, the table is instructed to roll back all staged changes.
 */
case class AtomicCreateV1TableAsSelectExec(
    catalog: StagingTableCatalog,
    ident: Identifier,
    partitioning: Seq[Transform],
    plan: LogicalPlan,
    properties: Map[String, String],
    writeOptions: CaseInsensitiveStringMap,
    ifNotExists: Boolean) extends SupportsV1Write {

  override protected def doExecute(): RDD[InternalRow] = {
    if (catalog.tableExists(ident)) {
      if (ifNotExists) {
        return sparkContext.parallelize(Seq.empty, 1)
      }

      throw new TableAlreadyExistsException(ident)
    }
    val stagedTable = catalog.stageCreate(
      ident, plan.schema, partitioning.toArray, properties.asJava)
    Utils.tryWithSafeFinallyAndFailureCallbacks({
      stagedTable match {
        case table: SupportsWrite =>
          val written = writeWithV1(table, SaveMode.Append)
          stagedTable.commitStagedChanges()
          written

        case _ =>
          // table does not support writes
          throw new SparkException(
            s"Table implementation does not support writes: $ident")
      }
    })({
      stagedTable.abortStagedChanges()
    })
  }
}

/**
 * Physical plan node for v2 replace table as select when the catalog does not support staging
 * table replacement.
 *
 * A new table will be created using the schema of the query, and rows from the query are appended.
 * If the table exists, its contents and schema should be replaced with the schema and the contents
 * of the query. This is a non-atomic implementation that drops the table and then runs non-atomic
 * CTAS. For an atomic implementation for catalogs with the appropriate support, see
 * ReplaceTableAsSelectStagingExec.
 */
case class ReplaceV1TableAsSelectExec(
    catalog: TableCatalog,
    ident: Identifier,
    partitioning: Seq[Transform],
    plan: LogicalPlan,
    properties: Map[String, String],
    writeOptions: CaseInsensitiveStringMap,
    orCreate: Boolean) extends SupportsV1Write {

  import org.apache.spark.sql.catalog.v2.CatalogV2Implicits.IdentifierHelper

  override protected def doExecute(): RDD[InternalRow] = {
    // Note that this operation is potentially unsafe, but these are the strict semantics of
    // RTAS if the catalog does not support atomic operations.
    //
    // There are numerous cases we concede to where the table will be dropped and irrecoverable:
    //
    // 1. Creating the new table fails,
    // 2. Writing to the new table fails,
    // 3. The table returned by catalog.createTable doesn't support writing.
    if (catalog.tableExists(ident)) {
      catalog.dropTable(ident)
    } else if (!orCreate) {
      throw new CannotReplaceMissingTableException(ident)
    }
    val createdTable = catalog.createTable(
      ident, plan.schema, partitioning.toArray, properties.asJava)
    Utils.tryWithSafeFinallyAndFailureCallbacks({
      createdTable match {
        case table: SupportsWrite =>
          writeWithV1(table, SaveMode.Overwrite)

        case _ =>
          // table does not support writes
          throw new SparkException(
            s"Table implementation does not support writes: ${ident.quoted}")
      }
    })(catchBlock = {
      catalog.dropTable(ident)
    })
  }
}

/**
 *
 * Physical plan node for v2 replace table as select when the catalog supports staging
 * table replacement.
 *
 * A new table will be created using the schema of the query, and rows from the query are appended.
 * If the table exists, its contents and schema should be replaced with the schema and the contents
 * of the query. This implementation is atomic. The table replacement is staged, and the commit
 * operation at the end should perform tne replacement of the table's metadata and contents. If the
 * write fails, the table is instructed to roll back staged changes and any previously written table
 * is left untouched.
 */
case class AtomicReplaceV1TableAsSelectExec(
    catalog: StagingTableCatalog,
    ident: Identifier,
    partitioning: Seq[Transform],
    plan: LogicalPlan,
    properties: Map[String, String],
    writeOptions: CaseInsensitiveStringMap,
    orCreate: Boolean) extends SupportsV1Write {

  override protected def doExecute(): RDD[InternalRow] = {
    val staged = if (orCreate) {
      catalog.stageCreateOrReplace(
        ident, plan.schema, partitioning.toArray, properties.asJava)
    } else if (catalog.tableExists(ident)) {
      try {
        catalog.stageReplace(
          ident, plan.schema, partitioning.toArray, properties.asJava)
      } catch {
        case e: NoSuchTableException =>
          throw new CannotReplaceMissingTableException(ident, Some(e))
      }
    } else {
      throw new CannotReplaceMissingTableException(ident)
    }
    Utils.tryWithSafeFinallyAndFailureCallbacks({
      staged match {
        case table: SupportsWrite =>
          val written = writeWithV1(table, SaveMode.Overwrite)
          staged.commitStagedChanges()
          written

        case _ =>
          // table does not support writes
          throw new SparkException(
            s"Table implementation does not support writes: $ident")
      }
    })({
      staged.abortStagedChanges()
    })
  }
}

/**
 * Physical plan node for append into a v2 table.
 *
 * Rows in the output data set are appended.
 */
case class AppendDataExecV1(
    table: SupportsWrite,
    writeOptions: CaseInsensitiveStringMap,
    plan: LogicalPlan) extends SupportsV1Write {

  override protected def doExecute(): RDD[InternalRow] = {
    writeWithV1(table, SaveMode.Append)
  }
}

/**
 * A trait that allows Tables that use V1 Writer interfaces to write data.
 */
sealed trait SupportsV1Write extends LeafExecNode {
  def plan: LogicalPlan
  def writeOptions: CaseInsensitiveStringMap

  override def output: Seq[Attribute] = Nil

  private implicit class WriteBuilderConverter(wb: WriteBuilder) {
    def asV1WriteBuilder: V1WriteBuilder = wb match {
      case v1: V1WriteBuilder => v1
      case other => throw new IllegalArgumentException(s"${other} is not a V1WriteBuilder.")
    }
  }

  protected def writeWithV1(table: SupportsWrite, mode: SaveMode): RDD[InternalRow] = {
    val relation = table.newWriteBuilder(writeOptions).asV1WriteBuilder.buildForV1Write()
    relation.createRelation(
      sqlContext,
      mode,
      writeOptions.asCaseSensitiveMap().asScala.toMap,
      Dataset.ofRows(sqlContext.sparkSession, plan))
    sparkContext.emptyRDD
  }
}
