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
import org.apache.spark.sql.catalyst.analysis.{NoSuchViewException, ResolvedIdentifier, SchemaEvolution, ViewAlreadyExistsException, ViewSchemaMode}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog, ViewCatalog, ViewInfo}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.IdentifierHelper
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.command.{CommandUtils, ViewHelper}
import org.apache.spark.sql.util.SchemaUtils
import org.apache.spark.util.ArrayImplicits._

/**
 * Shared validation + ViewInfo construction for v2 CREATE VIEW / ALTER VIEW execs.
 *
 * Mirrors the persistent-view portion of v1 [[ViewHelper.prepareTable]] + the execution-time
 * checks in [[org.apache.spark.sql.execution.command.CreateViewCommand.run]]. Post-analysis
 * checks for temp-object references and auto-generated aliases run once for both v1 and v2 in
 * [[org.apache.spark.sql.execution.command.CheckViewReferences]].
 */
private[v2] trait V2ViewPreparation extends LeafV2CommandExec {
  def catalog: ViewCatalog
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

  // For mixed catalogs (also TableCatalog), reject if the identifier names a non-view table:
  // CREATE VIEW must not silently destroy a table's data, and CREATE OR REPLACE VIEW must not
  // either.
  protected def rejectIfTable(): Unit = catalog match {
    case tc: TableCatalog if tc.tableExists(identifier) =>
      throw QueryCompilationErrors.unsupportedCreateOrReplaceViewOnTableError(
        fullNameParts, replaceArg)
    case _ =>
  }

  protected def replaceArg: Boolean
}

/**
 * Physical plan node for CREATE VIEW on a v2 [[ViewCatalog]]. Dispatches to
 * [[ViewCatalog#createView]] for plain CREATE / `IF NOT EXISTS`, and to
 * [[ViewCatalog#replaceView]] for `OR REPLACE`. CREATE OR REPLACE on a non-existent view falls
 * back to `createView`.
 */
case class CreateV2ViewExec(
    catalog: ViewCatalog,
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
  override protected def replaceArg: Boolean = replace

  override protected def run(): Seq[InternalRow] = {
    // Probe before preparing the view body so `IF NOT EXISTS` and the type-collision check can
    // short-circuit before running `aliasPlan` / config capture (matches v1
    // `CreateViewCommand.run`). Cyclic-reference detection runs at analysis time in
    // `CheckViewReferences`.
    val viewExists = catalog.viewExists(identifier)
    if (allowExisting && viewExists) {
      return Seq.empty
    }
    rejectIfTable()
    if (viewExists && !replace) throw viewAlreadyExists()

    val info = buildViewInfo()
    if (replace && viewExists) {
      // CREATE OR REPLACE on an existing view: replaceView is the single atomic-swap call.
      CommandUtils.uncacheTableOrView(session, ResolvedIdentifier(catalog, identifier))
      try {
        catalog.replaceView(identifier, info)
      } catch {
        case _: NoSuchViewException =>
          // Race: the view disappeared between the existence probe and replaceView. Fall back
          // to createView to honor REPLACE-or-create semantics.
          catalog.createView(identifier, info)
      }
    } else {
      // Plain CREATE (or CREATE OR REPLACE on a non-existent view).
      try {
        catalog.createView(identifier, info)
      } catch {
        case _: ViewAlreadyExistsException => throw viewAlreadyExists()
      }
    }
    Seq.empty
  }
}
