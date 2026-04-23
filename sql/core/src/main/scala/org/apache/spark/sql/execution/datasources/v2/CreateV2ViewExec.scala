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
import org.apache.spark.sql.catalyst.{CurrentUserContext, InternalRow}
import org.apache.spark.sql.catalyst.analysis.{NoSuchTableException, ResolvedIdentifier, SchemaEvolution, TableAlreadyExistsException, ViewSchemaMode}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.connector.catalog.{Identifier, MetadataOnlyTable, StagedTable, StagingTableCatalog, Table, TableCatalog, ViewInfo}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.IdentifierHelper
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.command.{CommandUtils, ViewHelper}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.util.SchemaUtils
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.Utils

/**
 * Shared validation + ViewInfo construction for v2 CREATE VIEW execs.
 *
 * Mirrors the persistent-view portion of v1 [[ViewHelper.prepareTable]] + the execution-time
 * checks in [[CreateViewCommand.run]]. Post-analysis checks for temp-object references and
 * auto-generated aliases run once for both v1 and v2 in [[CheckViewReferences]].
 */
private[v2] trait V2ViewPreparation extends LeafV2CommandExec {
  def catalog: TableCatalog
  def identifier: Identifier
  def userSpecifiedColumns: Seq[(String, Option[String])]
  def comment: Option[String]
  def collation: Option[String]
  def owner: Option[String]
  def userProperties: Map[String, String]
  def originalText: String
  def query: LogicalPlan
  def viewSchemaMode: ViewSchemaMode

  // Full multi-part identifier used for error rendering. Built once so we can avoid routing
  // through the lossy v1 `TableIdentifier` for multi-level-namespace v2 catalogs.
  protected lazy val fullNameParts: Seq[String] =
    (catalog.name() +: identifier.asMultipartIdentifier).toSeq

  override def output: Seq[Attribute] = Seq.empty

  protected def buildViewInfo(): ViewInfo = {
    import ViewHelper._

    if (userSpecifiedColumns.nonEmpty) {
      if (userSpecifiedColumns.length > query.output.length) {
        throw QueryCompilationErrors.cannotCreateViewNotEnoughColumnsError(
          fullNameParts, userSpecifiedColumns.map(_._1), query)
      } else if (userSpecifiedColumns.length < query.output.length) {
        throw QueryCompilationErrors.cannotCreateViewTooManyColumnsError(
          fullNameParts, userSpecifiedColumns.map(_._1), query)
      }
      if (viewSchemaMode == SchemaEvolution) {
        throw SparkException.internalError(
          "View with user column list has viewSchemaMode EVOLUTION")
      }
    }

    SchemaUtils.checkIndeterminateCollationInSchema(query.schema)

    val aliasedSchema = CharVarcharUtils.getRawSchema(
      aliasPlan(session, query, userSpecifiedColumns).schema, session.sessionState.conf)
    SchemaUtils.checkColumnNameDuplication(
      aliasedSchema.fieldNames.toImmutableArraySeq, session.sessionState.conf.resolver)

    val manager = session.sessionState.catalogManager
    val queryColumnNames = if (viewSchemaMode == SchemaEvolution) {
      Array.empty[String]
    } else {
      query.output.map(_.name).toArray
    }

    val builder = new ViewInfo.Builder()
      .withSchema(aliasedSchema)
      .withProperties(userProperties.asJava)
      .withQueryText(originalText)
      .withCurrentCatalog(manager.currentCatalog.name)
      .withCurrentNamespace(manager.currentNamespace)
      .withSqlConfigs(sqlConfigsToProps(session.sessionState.conf, "").asJava)
      .withSchemaMode(viewSchemaMode.toString)
      .withQueryColumnNames(queryColumnNames)
    // CREATE stamps the current user into PROP_OWNER (matching v2 CREATE TABLE via
    // CatalogV2Util.withDefaultOwnership and v1 CREATE VIEW via CatalogTable.owner's default);
    // ALTER preserves the existing view's owner (v1-parity with AlterViewAsCommand's
    // viewMeta.copy). Both cases are expressed via the `owner` hook provided by the subclass.
    owner.foreach(builder.withOwner)
    comment.foreach(builder.withComment)
    collation.foreach(builder.withCollation)
    builder.build()
  }

  protected def viewAlreadyExists(): Throwable =
    QueryCompilationErrors.viewAlreadyExistsError(fullNameParts)

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

  // A SUPPORTS_VIEW catalog round-trips views as MetadataOnlyTable wrapping a ViewInfo.
  // Anything else at the same identifier is a non-view table -- REPLACE'ing it as a view would
  // silently destroy the table's data, so we reject at the exec layer.
  protected def isViewTable(table: Table): Boolean = table match {
    case mot: MetadataOnlyTable => mot.getTableInfo.isInstanceOf[ViewInfo]
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

  override def owner: Option[String] = Some(CurrentUserContext.getCurrentUser)

  override protected def run(): Seq[InternalRow] = {
    // Probe the catalog before preparing the view body so `IF NOT EXISTS` short-circuits
    // without running `aliasPlan` / config capture, matching v1 `CreateViewCommand.run`.
    // Cyclic-reference detection is done at analysis time in `CheckViewReferences`.
    val existing = tryLoadTable()
    if (allowExisting && existing.isDefined) {
      return Seq.empty
    }
    existing.foreach { table =>
      if (!isViewTable(table)) {
        throw QueryCompilationErrors.unsupportedCreateOrReplaceViewOnTableError(
          fullNameParts, replace)
      }
      if (!replace) throw viewAlreadyExists()
    }
    val info = buildViewInfo()
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

  override def owner: Option[String] = Some(CurrentUserContext.getCurrentUser)

  override val metrics: Map[String, SQLMetric] =
    DataSourceV2Utils.commitMetrics(sparkContext, catalog)

  override protected def run(): Seq[InternalRow] = {
    // Probe the catalog before preparing the view body so `IF NOT EXISTS` short-circuits
    // without running `aliasPlan` / config capture, matching v1 `CreateViewCommand.run`.
    // Cyclic-reference detection is done at analysis time in `CheckViewReferences`.
    val existing = tryLoadTable()
    if (allowExisting && existing.isDefined) {
      return Seq.empty
    }
    existing.foreach { table =>
      if (!isViewTable(table)) {
        throw QueryCompilationErrors.unsupportedCreateOrReplaceViewOnTableError(
          fullNameParts, replace)
      }
      // Match the non-atomic exec: reject plain CREATE against an existing view up front
      // rather than relying on `stageCreate` to throw.
      if (!replace) throw viewAlreadyExists()
    }
    val info = buildViewInfo()
    val staged: StagedTable = if (replace) {
      if (existing.isDefined) {
        CommandUtils.uncacheTableOrView(session, ResolvedIdentifier(catalog, identifier))
      }
      catalog.stageCreateOrReplace(identifier, info)
    } else {
      // TOCTOU: a concurrent writer can create an entry between `tryLoadTable` and
      // `stageCreate`; translate the catalog's `TableAlreadyExistsException` into the same
      // view-already-exists error the fast-path uses.
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
