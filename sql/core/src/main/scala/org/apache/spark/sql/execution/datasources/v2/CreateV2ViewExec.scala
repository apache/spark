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

import scala.jdk.CollectionConverters._

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{NoSuchTableException, ResolvedIdentifier, SchemaEvolution, TableAlreadyExistsException, ViewSchemaMode}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.{CharVarcharUtils, QuotingUtils}
import org.apache.spark.sql.connector.catalog.{Identifier, MetadataOnlyTable, StagedTable, StagingTableCatalog, Table, TableCatalog, TableInfo, TableSummary}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.command.{CommandUtils, ViewHelper}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.util.SchemaUtils
import org.apache.spark.util.Utils

/**
 * Shared validation + TableInfo construction for v2 CREATE VIEW execs.
 *
 * Mirrors the persistent-view portion of v1 [[ViewHelper.prepareTable]] + the execution-time
 * checks in [[CreateViewCommand.run]]. Any future addition on the v1 side -- new view-specific
 * reserved property, new validation, new schema-mode handling -- must be mirrored here.
 * Post-analysis checks for temp-object references and auto-generated aliases run once for both
 * v1 and v2 in [[CheckViewReferences]].
 */
private[v2] trait V2ViewPreparation extends LeafV2CommandExec {
  def catalog: TableCatalog
  def identifier: Identifier
  def userSpecifiedColumns: Seq[(String, Option[String])]
  def comment: Option[String]
  def collation: Option[String]
  def userProperties: Map[String, String]
  def originalText: String
  def query: LogicalPlan
  def viewSchemaMode: ViewSchemaMode

  // Build a synthetic v1 TableIdentifier for error messages and for ViewHelper methods that
  // accept it purely for rendering. This carries no semantic weight - the v2 Identifier is the
  // actual target.
  protected lazy val legacyName: TableIdentifier = TableIdentifier(
    table = identifier.name(),
    database = identifier.namespace().lastOption,
    catalog = Some(catalog.name()))

  override def output: Seq[Attribute] = Seq.empty

  protected def buildTableInfo(): TableInfo = {
    import ViewHelper._
    import TableCatalog._

    if (userSpecifiedColumns.nonEmpty) {
      if (userSpecifiedColumns.length > query.output.length) {
        throw QueryCompilationErrors.cannotCreateViewNotEnoughColumnsError(
          legacyName, userSpecifiedColumns.map(_._1), query)
      } else if (userSpecifiedColumns.length < query.output.length) {
        throw QueryCompilationErrors.cannotCreateViewTooManyColumnsError(
          legacyName, userSpecifiedColumns.map(_._1), query)
      }
      if (viewSchemaMode == SchemaEvolution) {
        throw SparkException.internalError(
          "View with user column list has viewSchemaMode EVOLUTION")
      }
    }

    SchemaUtils.checkIndeterminateCollationInSchema(query.schema)

    val aliasedSchema = CharVarcharUtils.getRawSchema(
      aliasPlan(session, query, userSpecifiedColumns).schema, session.sessionState.conf)

    // Emit PROP_VIEW_CURRENT_CATALOG_AND_NAMESPACE (single quoted multi-part identifier,
    // catalog as first part) instead of v1's numbered view.catalogAndNamespace.* keys.
    val v2Encoder: (String, Seq[String]) => Map[String, String] = { (cat, ns) =>
      val parts = (cat +: ns).toArray
      Map(PROP_VIEW_CURRENT_CATALOG_AND_NAMESPACE -> QuotingUtils.quoted(parts))
    }

    // Temp-object collection arguments are omitted: persistent-view semantics are enforced by
    // CheckViewReferences before this runs, so any referenced temp view/function/variable has
    // already caused analysis to fail. This matches v1 ViewHelper.prepareTable, which also
    // calls generateViewProperties without them on the persistent-view path.
    val viewProps = generateViewProperties(
      properties = userProperties,
      session = session,
      queryOutput = query.output.map(_.name).toArray,
      fieldNames = aliasedSchema.fieldNames,
      viewSchemaMode = viewSchemaMode,
      catalogAndNamespaceEncoder = v2Encoder)

    val builder = new TableInfo.Builder()
      .withSchema(aliasedSchema)
      .withProperties(viewProps.asJava)
      .withViewText(originalText)
    comment.foreach(builder.withComment)
    collation.foreach(builder.withCollation)
    builder.build()
  }

  protected def viewAlreadyExists(): Throwable =
    QueryCompilationErrors.viewAlreadyExistsError(legacyName)

  // Loads the existing entry at `identifier` or returns None if it does not exist. Combines
  // the existence check and type check into a single catalog round-trip (vs. the previous
  // tableExists + implicit assume-view flow).
  protected def tryLoadTable(): Option[Table] = {
    try {
      Some(catalog.loadTable(identifier))
    } catch {
      case _: NoSuchTableException => None
    }
  }

  // A catalog with SUPPORTS_VIEW round-trips views as MetadataOnlyTable with PROP_TABLE_TYPE
  // set to VIEW. Anything else at the same identifier is a non-view table -- REPLACE'ing it as
  // a view would silently destroy the table's data, so we reject at the exec layer.
  protected def isViewTable(table: Table): Boolean = table match {
    case mot: MetadataOnlyTable =>
      TableSummary.VIEW_TABLE_TYPE.equals(
        mot.getTableInfo.properties.get(TableCatalog.PROP_TABLE_TYPE))
    case _ => false
  }
}

/**
 * Physical plan node for CREATE VIEW on a v2 `TableCatalog` that does NOT support staging.
 * REPLACE is implemented as a non-atomic drop + create.
 */
case class CreateV2ViewExec(
    catalog: TableCatalog,
    identifier: Identifier,
    userSpecifiedColumns: Seq[(String, Option[String])],
    comment: Option[String],
    collation: Option[String],
    userProperties: Map[String, String],
    originalText: String,
    query: LogicalPlan,
    allowExisting: Boolean,
    replace: Boolean,
    viewSchemaMode: ViewSchemaMode) extends V2ViewPreparation {

  override protected def run(): Seq[InternalRow] = {
    // Probe the catalog before preparing the view body so `IF NOT EXISTS` short-circuits
    // without running `aliasPlan` / `generateViewProperties`, matching v1
    // `CreateViewCommand.run`. Cyclic-reference detection is done at analysis time in
    // `CheckViewReferences`.
    val existing = tryLoadTable()
    if (allowExisting && existing.isDefined) {
      return Seq.empty
    }
    existing.foreach { table =>
      if (!isViewTable(table)) {
        throw QueryCompilationErrors.unsupportedCreateOrReplaceViewOnTableError(
          legacyName, replace)
      }
      if (!replace) throw viewAlreadyExists()
    }
    val info = buildTableInfo()
    if (existing.isDefined) {
      CommandUtils.uncacheTableOrView(session, ResolvedIdentifier(catalog, identifier))
      catalog.dropTable(identifier)
    }
    // TOCTOU: if another writer creates an entry between tryLoadTable and createTable, a bare
    // TableAlreadyExistsException is unhelpful; present the same viewAlreadyExists error the
    // atomic path uses.
    try {
      catalog.createTable(identifier, info)
    } catch {
      case _: TableAlreadyExistsException => throw viewAlreadyExists()
    }
    Seq.empty
  }
}

/**
 * Physical plan node for CREATE VIEW on a v2 `StagingTableCatalog`. Uses the staging API to
 * commit the metadata swap atomically.
 */
case class AtomicCreateV2ViewExec(
    catalog: StagingTableCatalog,
    identifier: Identifier,
    userSpecifiedColumns: Seq[(String, Option[String])],
    comment: Option[String],
    collation: Option[String],
    userProperties: Map[String, String],
    originalText: String,
    query: LogicalPlan,
    allowExisting: Boolean,
    replace: Boolean,
    viewSchemaMode: ViewSchemaMode) extends V2ViewPreparation {

  override val metrics: Map[String, SQLMetric] =
    DataSourceV2Utils.commitMetrics(sparkContext, catalog)

  override protected def run(): Seq[InternalRow] = {
    // Probe the catalog before preparing the view body so `IF NOT EXISTS` short-circuits
    // without running `aliasPlan` / `generateViewProperties`, matching v1
    // `CreateViewCommand.run`. Cyclic-reference detection is done at analysis time in
    // `CheckViewReferences`.
    val existing = tryLoadTable()
    if (allowExisting && existing.isDefined) {
      return Seq.empty
    }
    existing.foreach { table =>
      if (!isViewTable(table)) {
        throw QueryCompilationErrors.unsupportedCreateOrReplaceViewOnTableError(
          legacyName, replace)
      }
    }
    val info = buildTableInfo()
    val staged: StagedTable = if (replace) {
      if (existing.isDefined) {
        CommandUtils.uncacheTableOrView(session, ResolvedIdentifier(catalog, identifier))
      }
      catalog.stageCreateOrReplace(identifier, info)
    } else {
      try {
        catalog.stageCreate(identifier, info)
      } catch {
        case _: TableAlreadyExistsException => throw viewAlreadyExists()
      }
    }
    Utils.tryWithSafeFinallyAndFailureCallbacks({
      DataSourceV2Utils.commitStagedChanges(sparkContext, staged, metrics)
    })(catchBlock = {
      staged.abortStagedChanges()
    })
    Seq.empty
  }
}
