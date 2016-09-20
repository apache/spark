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

package org.apache.spark.sql.execution.command

import scala.util.control.NonFatal

import org.apache.spark.sql.{AnalysisException, Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.{SQLBuilder, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.execution.datasources.{DataSource, LogicalRelation}
import org.apache.spark.sql.types.{MetadataBuilder, StructType}


/**
 * ViewType is used to specify the expected view type when we want to create or replace a view in
 * [[CreateViewCommand]].
 */
sealed trait ViewType

/**
 * LocalTempView means session-scoped local temporary views. Its lifetime is the lifetime of the
 * session that created it, i.e. it will be automatically dropped when the session terminates. It's
 * not tied to any databases, i.e. we can't use `db1.view1` to reference a local temporary view.
 */
object LocalTempView extends ViewType

/**
 * GlobalTempView means cross-session global temporary views. Its lifetime is the lifetime of the
 * Spark application, i.e. it will be automatically dropped when the application terminates. It's
 * tied to a system preserved database `_global_temp`, and we must use the qualified name to refer a
 * global temp view, e.g. SELECT * FROM _global_temp.view1.
 */
object GlobalTempView extends ViewType

/**
 * PermanentView means cross-session permanent views. Permanent views stay until they are
 * explicitly dropped by user command. It's always tied to a database, default to the current
 * database if not specified.
 *
 * Note that, Existing permanent view with the same name are not visible to the current session
 * while the local temporary view exists, unless the view name is qualified by database.
 */
object PermanentView extends ViewType


/**
 * Create or replace a view with given query plan. This command will convert the query plan to
 * canonicalized SQL string, and store it as view text in metastore, if we need to create a
 * permanent view.
 *
 * @param name the name of this view.
 * @param userSpecifiedColumns the output column names and optional comments specified by users,
 *                             can be Nil if not specified.
 * @param comment the comment of this view.
 * @param properties the properties of this view.
 * @param originalText the original SQL text of this view, can be None if this view is created via
 *                     Dataset API.
 * @param child the logical plan that represents the view; this is used to generate a canonicalized
 *              version of the SQL that can be saved in the catalog.
 * @param allowExisting if true, and if the view already exists, noop; if false, and if the view
 *                already exists, throws analysis exception.
 * @param replace if true, and if the view already exists, updates it; if false, and if the view
 *                already exists, throws analysis exception.
 * @param viewType the expected view type to be created with this command.
 */
case class CreateViewCommand(
    name: TableIdentifier,
    userSpecifiedColumns: Seq[(String, Option[String])],
    comment: Option[String],
    properties: Map[String, String],
    originalText: Option[String],
    child: LogicalPlan,
    allowExisting: Boolean,
    replace: Boolean,
    viewType: ViewType)
  extends RunnableCommand {

  override protected def innerChildren: Seq[QueryPlan[_]] = Seq(child)

  if (viewType == PermanentView) {
    require(originalText.isDefined, "'originalText' must be provided to create permanent view")
  }

  if (allowExisting && replace) {
    throw new AnalysisException("CREATE VIEW with both IF NOT EXISTS and REPLACE is not allowed.")
  }

  private def isTemporary = viewType == LocalTempView || viewType == GlobalTempView

  // Disallows 'CREATE TEMPORARY VIEW IF NOT EXISTS' to be consistent with 'CREATE TEMPORARY TABLE'
  if (allowExisting && isTemporary) {
    throw new AnalysisException(
      "It is not allowed to define a TEMPORARY view with IF NOT EXISTS.")
  }

  // Temporary view names should NOT contain database prefix like "database.table"
  if (isTemporary && name.database.isDefined) {
    val database = name.database.get
    throw new AnalysisException(
      s"It is not allowed to add database prefix `$database` for the TEMPORARY view name.")
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    // If the plan cannot be analyzed, throw an exception and don't proceed.
    val qe = sparkSession.sessionState.executePlan(child)
    qe.assertAnalyzed()
    val analyzedPlan = qe.analyzed

    if (userSpecifiedColumns.nonEmpty &&
        userSpecifiedColumns.length != analyzedPlan.output.length) {
      throw new AnalysisException(s"The number of columns produced by the SELECT clause " +
        s"(num: `${analyzedPlan.output.length}`) does not match the number of column names " +
        s"specified by CREATE VIEW (num: `${userSpecifiedColumns.length}`).")
    }

    val aliasedPlan = if (userSpecifiedColumns.isEmpty) {
      analyzedPlan
    } else {
      val projectList = analyzedPlan.output.zip(userSpecifiedColumns).map {
        case (attr, (colName, None)) => Alias(attr, colName)()
        case (attr, (colName, Some(colComment))) =>
          val meta = new MetadataBuilder().putString("comment", colComment).build()
          Alias(attr, colName)(explicitMetadata = Some(meta))
      }
      sparkSession.sessionState.executePlan(Project(projectList, analyzedPlan)).analyzed
    }

    val catalog = sparkSession.sessionState.catalog
    if (viewType == LocalTempView) {
      catalog.createTempView(name.table, aliasedPlan, overrideIfExists = replace)
    } else if (viewType == GlobalTempView) {
      catalog.createGlobalTempView(name.table, aliasedPlan, overrideIfExists = replace)
    } else if (catalog.tableExists(name)) {
      val tableMetadata = catalog.getTableMetadata(name)
      if (allowExisting) {
        // Handles `CREATE VIEW IF NOT EXISTS v0 AS SELECT ...`. Does nothing when the target view
        // already exists.
      } else if (tableMetadata.tableType != CatalogTableType.VIEW) {
        throw new AnalysisException(s"$name is not a view")
      } else if (replace) {
        // Handles `CREATE OR REPLACE VIEW v0 AS SELECT ...`
        catalog.alterTable(prepareTable(sparkSession, aliasedPlan))
      } else {
        // Handles `CREATE VIEW v0 AS SELECT ...`. Throws exception when the target view already
        // exists.
        throw new AnalysisException(
          s"View $name already exists. If you want to update the view definition, " +
            "please use ALTER VIEW AS or CREATE OR REPLACE VIEW AS")
      }
    } else {
      // Create the view if it doesn't exist.
      catalog.createTable(prepareTable(sparkSession, aliasedPlan), ignoreIfExists = false)
    }
    Seq.empty[Row]
  }

  /**
   * Returns a [[CatalogTable]] that can be used to save in the catalog. This comment canonicalize
   * SQL based on the analyzed plan, and also creates the proper schema for the view.
   */
  private def prepareTable(sparkSession: SparkSession, aliasedPlan: LogicalPlan): CatalogTable = {
    val viewSQL: String = new SQLBuilder(aliasedPlan).toSQL

    // Validate the view SQL - make sure we can parse it and analyze it.
    // If we cannot analyze the generated query, there is probably a bug in SQL generation.
    try {
      sparkSession.sql(viewSQL).queryExecution.assertAnalyzed()
    } catch {
      case NonFatal(e) =>
        throw new RuntimeException(s"Failed to analyze the canonicalized SQL: $viewSQL", e)
    }

    CatalogTable(
      identifier = name,
      tableType = CatalogTableType.VIEW,
      storage = CatalogStorageFormat.empty,
      schema = aliasedPlan.schema,
      properties = properties,
      viewOriginalText = originalText,
      viewText = Some(viewSQL),
      comment = comment
    )
  }
}


/**
 * Create or replace a local/global temporary view with given data source.
 */
case class CreateTempViewUsing(
    tableIdent: TableIdentifier,
    userSpecifiedSchema: Option[StructType],
    replace: Boolean,
    global: Boolean,
    provider: String,
    options: Map[String, String]) extends RunnableCommand {

  if (tableIdent.database.isDefined) {
    throw new AnalysisException(
      s"Temporary view '$tableIdent' should not have specified a database")
  }

  def run(sparkSession: SparkSession): Seq[Row] = {
    val dataSource = DataSource(
      sparkSession,
      userSpecifiedSchema = userSpecifiedSchema,
      className = provider,
      options = options)

    val catalog = sparkSession.sessionState.catalog
    val viewDefinition = Dataset.ofRows(
      sparkSession, LogicalRelation(dataSource.resolveRelation())).logicalPlan

    if (global) {
      catalog.createGlobalTempView(tableIdent.table, viewDefinition, replace)
    } else {
      catalog.createTempView(tableIdent.table, viewDefinition, replace)
    }

    Seq.empty[Row]
  }
}


/**
 * Alter a view with given query plan. If the view name contains database prefix, this command will
 * alter a permanent view matching the given name, or throw an exception if view not exist. Else,
 * this command will try to alter a temporary view first, if view not exist, try permanent view
 * next, if still not exist, throw an exception.
 *
 * @param name the name of this view.
 * @param originalText the original SQL text of this view. Note that we can only alter a view by
 *                     SQL API, which means we always have originalText.
 * @param query the logical plan that represents the view; this is used to generate a canonicalized
 *              version of the SQL that can be saved in the catalog.
 */
case class AlterViewAsCommand(
    name: TableIdentifier,
    originalText: String,
    query: LogicalPlan) extends RunnableCommand {

  override protected def innerChildren: Seq[QueryPlan[_]] = Seq(query)

  override def run(session: SparkSession): Seq[Row] = {
    // If the plan cannot be analyzed, throw an exception and don't proceed.
    val qe = session.sessionState.executePlan(query)
    qe.assertAnalyzed()
    val analyzedPlan = qe.analyzed

    if (session.sessionState.catalog.alterTempViewDefinition(name, analyzedPlan)) {
      // a local/global temp view has been altered, we are done.
    } else {
      alterPermanentView(session, analyzedPlan)
    }

    Seq.empty[Row]
  }

  private def alterPermanentView(session: SparkSession, analyzedPlan: LogicalPlan): Unit = {
    val viewMeta = session.sessionState.catalog.getTableMetadata(name)
    if (viewMeta.tableType != CatalogTableType.VIEW) {
      throw new AnalysisException(s"${viewMeta.identifier} is not a view.")
    }

    val viewSQL: String = new SQLBuilder(analyzedPlan).toSQL
    // Validate the view SQL - make sure we can parse it and analyze it.
    // If we cannot analyze the generated query, there is probably a bug in SQL generation.
    try {
      session.sql(viewSQL).queryExecution.assertAnalyzed()
    } catch {
      case NonFatal(e) =>
        throw new RuntimeException(s"Failed to analyze the canonicalized SQL: $viewSQL", e)
    }

    val updatedViewMeta = viewMeta.copy(
      schema = analyzedPlan.schema,
      viewOriginalText = Some(originalText),
      viewText = Some(viewSQL))

    session.sessionState.catalog.alterTable(updatedViewMeta)
  }
}
