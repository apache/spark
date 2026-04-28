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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{ResolvedIdentifier, ViewSchemaMode}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog, ViewCatalog, ViewInfo}
import org.apache.spark.sql.execution.command.CommandUtils

private[v2] object V2ViewMetadataMutation {
  /**
   * Construct a [[ViewInfo.Builder]] seeded from an existing view's metadata. Mutating execs
   * (SET / UNSET TBLPROPERTIES, ALTER VIEW ... WITH SCHEMA BINDING) start here, override the
   * one field they're changing, and call [[ViewInfo.Builder#build]] to produce the replacement
   * payload for [[ViewCatalog#replaceView]]. Everything else -- columns, queryText, captured
   * resolution context, captured SQL configs, queryColumnNames -- flows through unchanged so
   * a metadata-only mutation does not perturb the view body.
   */
  def builderFrom(existing: ViewInfo): ViewInfo.Builder = {
    val builder = new ViewInfo.Builder()
    builder
      .withSchema(existing.schema)
      .withProperties(existing.properties)
      .withQueryText(existing.queryText)
      .withSqlConfigs(existing.sqlConfigs)
      .withCurrentNamespace(existing.currentNamespace)
      .withQueryColumnNames(existing.queryColumnNames)
    Option(existing.currentCatalog).foreach(builder.withCurrentCatalog)
    Option(existing.schemaMode).foreach(builder.withSchemaMode)
    builder
  }
}

/**
 * Shared bits for the v2 ALTER VIEW ... AS exec. The replacement [[ViewInfo]] is constructed by
 * [[V2ViewPreparation.buildViewInfo]]; the existing view's payload is provided at analysis time
 * via the `existingView` field so we can preserve user-set TBLPROPERTIES, comment, collation,
 * owner, and schema binding mode without re-loading at runtime.
 *
 * Transient fields (SQL configs, query column names) are re-captured from the current session,
 * matching v1 `AlterViewAsCommand.alterPermanentView`. PROP_OWNER and user TBLPROPERTIES flow
 * through unchanged. If the view has been dropped or replaced with a non-view table between
 * analysis and exec, the catalog's `replaceView` surfaces `NoSuchViewException` and the error
 * propagates.
 */
private[v2] trait V2AlterViewPreparation extends V2ViewPreparation {
  protected def existingView: ViewInfo

  protected lazy val existingProps: Map[String, String] =
    existingView.properties.asScala.toMap

  private def existingProp(key: String): Option[String] = existingProps.get(key)

  // ALTER VIEW ... AS does not accept a user column list.
  override def userSpecifiedColumns: Seq[(String, Option[String])] = Seq.empty
  override def comment: Option[String] = existingProp(TableCatalog.PROP_COMMENT)
  override def collation: Option[String] = existingProp(TableCatalog.PROP_COLLATION)
  // Preserve the existing view's owner (v1-parity with AlterViewAsCommand's viewMeta.copy,
  // which leaves `owner` untouched). If the existing view has no PROP_OWNER, pass it through
  // as None so the replacement ViewInfo also has no owner.
  override def owner: Option[String] = existingProp(TableCatalog.PROP_OWNER)
  override def userProperties: Map[String, String] = existingProps

  // Preserve the existing view's schema binding mode. Reuse `viewSchemaModeFromProperties`
  // for a v1-identical decode -- it honors `viewSchemaBindingEnabled` and defaults missing
  // values to SchemaBinding. We feed the typed `ViewInfo.schemaMode` String in via a
  // single-key map so the decode logic stays in one place.
  override def viewSchemaMode: ViewSchemaMode =
    CatalogTable.viewSchemaModeFromProperties(
      Option(existingView.schemaMode)
        .map(CatalogTable.VIEW_SCHEMA_MODE -> _)
        .toMap)
}

/**
 * Physical plan node for ALTER VIEW ... AS on a v2 [[ViewCatalog]]. Dispatches to
 * [[ViewCatalog#replaceView]], which is contractually atomic.
 */
case class AlterV2ViewExec(
    catalog: ViewCatalog,
    identifier: Identifier,
    existingView: ViewInfo,
    originalText: String,
    query: LogicalPlan) extends V2AlterViewPreparation {

  override protected def run(): Seq[InternalRow] = {
    val info = buildViewInfo()
    // Cyclic reference detection is done at analysis time in CheckViewReferences.
    CommandUtils.uncacheTableOrView(session, ResolvedIdentifier(catalog, identifier))
    catalog.replaceView(identifier, info)
    Seq.empty
  }
}

/**
 * Physical plan node for ALTER VIEW ... SET TBLPROPERTIES on a v2 [[ViewCatalog]]. Merges the
 * user-supplied properties on top of the analysis-time view properties and dispatches to
 * [[ViewCatalog#replaceView]] -- views carry no data, so a single atomic-swap call is sufficient.
 */
case class AlterV2ViewSetPropertiesExec(
    catalog: ViewCatalog,
    identifier: Identifier,
    existingView: ViewInfo,
    properties: Map[String, String]) extends LeafV2CommandExec {

  override def output: Seq[org.apache.spark.sql.catalyst.expressions.Attribute] = Seq.empty

  override protected def run(): Seq[InternalRow] = {
    val merged = existingView.properties.asScala.toMap ++ properties
    val info = V2ViewMetadataMutation.builderFrom(existingView)
      .withProperties(merged.asJava)
      .build()
    catalog.replaceView(identifier, info)
    Seq.empty
  }
}

/**
 * Physical plan node for ALTER VIEW ... UNSET TBLPROPERTIES on a v2 [[ViewCatalog]]. Drops the
 * listed property keys from the analysis-time view properties and dispatches to
 * [[ViewCatalog#replaceView]]. Missing keys are silently dropped, matching v1
 * `AlterTableUnsetPropertiesCommand` for views (`ifExists` is unused on the view path -- the
 * v1 view command never errors on missing keys).
 */
case class AlterV2ViewUnsetPropertiesExec(
    catalog: ViewCatalog,
    identifier: Identifier,
    existingView: ViewInfo,
    propertyKeys: Seq[String]) extends LeafV2CommandExec {

  override def output: Seq[org.apache.spark.sql.catalyst.expressions.Attribute] = Seq.empty

  override protected def run(): Seq[InternalRow] = {
    val remaining = existingView.properties.asScala.toMap -- propertyKeys
    val info = V2ViewMetadataMutation.builderFrom(existingView)
      .withProperties(remaining.asJava)
      .build()
    catalog.replaceView(identifier, info)
    Seq.empty
  }
}

/**
 * Physical plan node for ALTER VIEW ... WITH SCHEMA BINDING on a v2 [[ViewCatalog]]. Replaces
 * the schema-binding mode on the analysis-time view payload and dispatches to
 * [[ViewCatalog#replaceView]]. The view body itself is not re-analyzed -- only the binding mode
 * field changes.
 */
case class AlterV2ViewSchemaBindingExec(
    catalog: ViewCatalog,
    identifier: Identifier,
    existingView: ViewInfo,
    viewSchemaMode: ViewSchemaMode) extends LeafV2CommandExec {

  override def output: Seq[org.apache.spark.sql.catalyst.expressions.Attribute] = Seq.empty

  override protected def run(): Seq[InternalRow] = {
    val info = V2ViewMetadataMutation.builderFrom(existingView)
      .withSchemaMode(viewSchemaMode.toString)
      .build()
    CommandUtils.uncacheTableOrView(session, ResolvedIdentifier(catalog, identifier))
    catalog.replaceView(identifier, info)
    Seq.empty
  }
}

/**
 * Physical plan node for ALTER VIEW ... RENAME TO on a v2 [[ViewCatalog]]. Dispatches to
 * [[ViewCatalog#renameView]]; if the source view is missing or has been replaced with a non-view
 * table between analysis and exec, the catalog throws `NoSuchViewException` and the error
 * propagates.
 */
case class RenameV2ViewExec(
    catalog: ViewCatalog,
    oldIdent: Identifier,
    newIdent: Identifier) extends LeafV2CommandExec {

  override def output: Seq[org.apache.spark.sql.catalyst.expressions.Attribute] = Seq.empty

  override protected def run(): Seq[InternalRow] = {
    // If the new identifier consists of a name only, rename in place within the source
    // namespace -- matches `RenameTableExec`'s v1-parity behavior.
    val qualifiedNewIdent = if (newIdent.namespace.isEmpty) {
      Identifier.of(oldIdent.namespace, newIdent.name)
    } else newIdent
    CommandUtils.uncacheTableOrView(session, ResolvedIdentifier(catalog, oldIdent))
    catalog.invalidateView(oldIdent)
    catalog.renameView(oldIdent, qualifiedNewIdent)
    Seq.empty
  }
}
