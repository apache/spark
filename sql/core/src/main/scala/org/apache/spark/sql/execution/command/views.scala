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

import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.{SQLBuilder, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.types.StructType


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
 * @param isTemporary if true, the view is created as a temporary view. Temporary views are dropped
 *                 at the end of current Spark session. Existing permanent relations with the same
 *                 name are not visible to the current session while the temporary view exists,
 *                 unless they are specified with full qualified table name with database prefix.
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
    isTemporary: Boolean)
  extends RunnableCommand {

  override protected def innerChildren: Seq[QueryPlan[_]] = Seq(child)

  if (!isTemporary) {
    require(originalText.isDefined,
      "The table to created with CREATE VIEW must have 'originalText'.")
  }

  if (allowExisting && replace) {
    throw new AnalysisException("CREATE VIEW with both IF NOT EXISTS and REPLACE is not allowed.")
  }

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
    val sessionState = sparkSession.sessionState

    if (isTemporary) {
      createTemporaryView(sparkSession, analyzedPlan)
    } else {
      // Adds default database for permanent table if it doesn't exist, so that tableExists()
      // only check permanent tables.
      val database = name.database.getOrElse(sessionState.catalog.getCurrentDatabase)
      val qualifiedName = name.copy(database = Option(database))

      if (sessionState.catalog.tableExists(qualifiedName)) {
        val tableMetadata = sessionState.catalog.getTableMetadata(qualifiedName)
        if (allowExisting) {
          // Handles `CREATE VIEW IF NOT EXISTS v0 AS SELECT ...`. Does nothing when the target view
          // already exists.
        } else if (tableMetadata.tableType != CatalogTableType.VIEW) {
          throw new AnalysisException(s"$qualifiedName is not a view")
        } else if (replace) {
          // Handles `CREATE OR REPLACE VIEW v0 AS SELECT ...`
          sessionState.catalog.alterTable(prepareTable(sparkSession, analyzedPlan))
        } else {
          // Handles `CREATE VIEW v0 AS SELECT ...`. Throws exception when the target view already
          // exists.
          throw new AnalysisException(
            s"View $qualifiedName already exists. If you want to update the view definition, " +
              "please use ALTER VIEW AS or CREATE OR REPLACE VIEW AS")
        }
      } else {
        // Create the view if it doesn't exist.
        sessionState.catalog.createTable(
          prepareTable(sparkSession, analyzedPlan), ignoreIfExists = false)
      }
    }
    Seq.empty[Row]
  }

  private def createTemporaryView(sparkSession: SparkSession, analyzedPlan: LogicalPlan): Unit = {
    val catalog = sparkSession.sessionState.catalog

    // Projects column names to alias names
    val logicalPlan = if (userSpecifiedColumns.isEmpty) {
      analyzedPlan
    } else {
      val projectList = analyzedPlan.output.zip(userSpecifiedColumns).map {
        case (attr, (colName, _)) => Alias(attr, colName)()
      }
      sparkSession.sessionState.executePlan(Project(projectList, analyzedPlan)).analyzed
    }

    catalog.createTempView(name.table, logicalPlan, replace)
  }

  /**
   * Returns a [[CatalogTable]] that can be used to save in the catalog. This comment canonicalize
   * SQL based on the analyzed plan, and also creates the proper schema for the view.
   */
  private def prepareTable(sparkSession: SparkSession, analyzedPlan: LogicalPlan): CatalogTable = {
    val aliasedPlan = if (userSpecifiedColumns.isEmpty) {
      analyzedPlan
    } else {
      val projectList = analyzedPlan.output.zip(userSpecifiedColumns).map {
        case (attr, (colName, _)) => Alias(attr, colName)()
      }
      sparkSession.sessionState.executePlan(Project(projectList, analyzedPlan)).analyzed
    }

    val viewSQL: String = new SQLBuilder(aliasedPlan).toSQL

    // Validate the view SQL - make sure we can parse it and analyze it.
    // If we cannot analyze the generated query, there is probably a bug in SQL generation.
    try {
      sparkSession.sql(viewSQL).queryExecution.assertAnalyzed()
    } catch {
      case NonFatal(e) =>
        throw new RuntimeException(s"Failed to analyze the canonicalized SQL: $viewSQL", e)
    }

    val viewSchema = if (userSpecifiedColumns.isEmpty) {
      aliasedPlan.schema
    } else {
      StructType(aliasedPlan.schema.zip(userSpecifiedColumns).map {
        case (field, (_, comment)) => comment.map(field.withComment).getOrElse(field)
      })
    }

    CatalogTable(
      identifier = name,
      tableType = CatalogTableType.VIEW,
      storage = CatalogStorageFormat.empty,
      schema = viewSchema,
      properties = properties,
      viewOriginalText = originalText,
      viewText = Some(viewSQL),
      comment = comment
    )
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
case class AlterViewCommand(
    name: TableIdentifier,
    originalText: String,
    query: LogicalPlan) extends RunnableCommand {

  override protected def innerChildren: Seq[QueryPlan[_]] = Seq(query)

  override def run(session: SparkSession): Seq[Row] = {
    // If the plan cannot be analyzed, throw an exception and don't proceed.
    val qe = session.sessionState.executePlan(query)
    qe.assertAnalyzed()
    val analyzedPlan = qe.analyzed

    val tableMeta = session.sessionState.catalog.getTableMetadata(name)
    if (tableMeta.tableType != CatalogTableType.VIEW) {
      throw new AnalysisException(s"${tableMeta.identifier} is not a view.")
    }

    if (name.database.isDefined) {
      alterPermanentView(session, analyzedPlan)
    } else if (session.sessionState.catalog.isTemporaryTable(name)) {
      session.sessionState.catalog.createTempView(name.table, analyzedPlan, overrideIfExists = true)
    } else {
      alterPermanentView(session, analyzedPlan)
    }

    Seq.empty[Row]
  }

  private def alterPermanentView(session: SparkSession, analyzedPlan: LogicalPlan): Unit = {
    val viewSQL: String = new SQLBuilder(analyzedPlan).toSQL
    // Validate the view SQL - make sure we can parse it and analyze it.
    // If we cannot analyze the generated query, there is probably a bug in SQL generation.
    try {
      session.sql(viewSQL).queryExecution.assertAnalyzed()
    } catch {
      case NonFatal(e) =>
        throw new RuntimeException(s"Failed to analyze the canonicalized SQL: $viewSQL", e)
    }

    val viewTableMeta = CatalogTable(
      identifier = name,
      tableType = CatalogTableType.VIEW,
      storage = CatalogStorageFormat.empty,
      schema = analyzedPlan.schema,
      viewOriginalText = Some(originalText),
      viewText = Some(viewSQL))

    session.sessionState.catalog.alterTable(viewTableMeta)
  }
}
