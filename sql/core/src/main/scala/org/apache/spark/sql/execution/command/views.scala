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

import scala.collection.mutable

import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.types.MetadataBuilder


/**
 * ViewType is used to specify the expected view type when we want to create or replace a view in
 * [[CreateViewCommand]].
 */
sealed trait ViewType {
  override def toString: String = getClass.getSimpleName.stripSuffix("$")
}

/**
 * LocalTempView means session-scoped local temporary views. Its lifetime is the lifetime of the
 * session that created it, i.e. it will be automatically dropped when the session terminates. It's
 * not tied to any databases, i.e. we can't use `db1.view1` to reference a local temporary view.
 */
object LocalTempView extends ViewType

/**
 * GlobalTempView means cross-session global temporary views. Its lifetime is the lifetime of the
 * Spark application, i.e. it will be automatically dropped when the application terminates. It's
 * tied to a system preserved database `global_temp`, and we must use the qualified name to refer a
 * global temp view, e.g. SELECT * FROM global_temp.view1.
 */
object GlobalTempView extends ViewType

/**
 * PersistedView means cross-session persisted views. Persisted views stay until they are
 * explicitly dropped by user command. It's always tied to a database, default to the current
 * database if not specified.
 *
 * Note that, Existing persisted view with the same name are not visible to the current session
 * while the local temporary view exists, unless the view name is qualified by database.
 */
object PersistedView extends ViewType


/**
 * Create or replace a view with given query plan. This command will generate some view-specific
 * properties(e.g. view default database, view query output column names) and store them as
 * properties in metastore, if we need to create a permanent view.
 *
 * @param name the name of this view.
 * @param userSpecifiedColumns the output column names and optional comments specified by users,
 *                             can be Nil if not specified.
 * @param comment the comment of this view.
 * @param properties the properties of this view.
 * @param originalText the original SQL text of this view, can be None if this view is created via
 *                     Dataset API.
 * @param child the logical plan that represents the view; this is used to generate the logical
 *              plan for temporary view and the view schema.
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

  import ViewHelper._

  override def innerChildren: Seq[QueryPlan[_]] = Seq(child)

  if (viewType == PersistedView) {
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
    if (userSpecifiedColumns.nonEmpty &&
        userSpecifiedColumns.length != child.output.length) {
      throw new AnalysisException(s"The number of columns produced by the SELECT clause " +
        s"(num: `${child.output.length}`) does not match the number of column names " +
        s"specified by CREATE VIEW (num: `${userSpecifiedColumns.length}`).")
    }

    val catalog = sparkSession.sessionState.catalog
    if (viewType == LocalTempView) {
      val aliasedPlan = aliasPlan(sparkSession, child)
      catalog.createTempView(name.table, aliasedPlan, overrideIfExists = replace)
    } else if (viewType == GlobalTempView) {
      val aliasedPlan = aliasPlan(sparkSession, child)
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
        // Nothing we need to retain from the old view, so just drop and create a new one
        val viewIdent = tableMetadata.identifier
        catalog.dropTable(viewIdent, ignoreIfNotExists = false, purge = false)
        catalog.createTable(prepareTable(sparkSession, child), ignoreIfExists = false)
      } else {
        // Handles `CREATE VIEW v0 AS SELECT ...`. Throws exception when the target view already
        // exists.
        throw new AnalysisException(
          s"View $name already exists. If you want to update the view definition, " +
            "please use ALTER VIEW AS or CREATE OR REPLACE VIEW AS")
      }
    } else {
      // Create the view if it doesn't exist.
      catalog.createTable(prepareTable(sparkSession, child), ignoreIfExists = false)
    }
    Seq.empty[Row]
  }

  /**
   * If `userSpecifiedColumns` is defined, alias the analyzed plan to the user specified columns,
   * else return the analyzed plan directly.
   */
  private def aliasPlan(session: SparkSession, analyzedPlan: LogicalPlan): LogicalPlan = {
    if (userSpecifiedColumns.isEmpty) {
      analyzedPlan
    } else {
      val projectList = analyzedPlan.output.zip(userSpecifiedColumns).map {
        case (attr, (colName, None)) => Alias(attr, colName)()
        case (attr, (colName, Some(colComment))) =>
          val meta = new MetadataBuilder().putString("comment", colComment).build()
          Alias(attr, colName)(explicitMetadata = Some(meta))
      }
      session.sessionState.executePlan(Project(projectList, analyzedPlan)).analyzed
    }
  }

  /**
   * Returns a [[CatalogTable]] that can be used to save in the catalog. Generate the view-specific
   * properties(e.g. view default database, view query output column names) and store them as
   * properties in the CatalogTable, and also creates the proper schema for the view.
   */
  private def prepareTable(session: SparkSession, analyzedPlan: LogicalPlan): CatalogTable = {
    if (originalText.isEmpty) {
      throw new AnalysisException(
        "It is not allowed to create a persisted view from the Dataset API")
    }

    val newProperties = generateViewProperties(properties, session, analyzedPlan)

    CatalogTable(
      identifier = name,
      tableType = CatalogTableType.VIEW,
      storage = CatalogStorageFormat.empty,
      schema = aliasPlan(session, analyzedPlan).schema,
      properties = newProperties,
      viewText = originalText,
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
 * @param query the logical plan that represents the view; this is used to generate the new view
 *              schema.
 */
case class AlterViewAsCommand(
    name: TableIdentifier,
    originalText: String,
    query: LogicalPlan) extends RunnableCommand {

  import ViewHelper._

  override def innerChildren: Seq[QueryPlan[_]] = Seq(query)

  override def run(session: SparkSession): Seq[Row] = {
    if (session.sessionState.catalog.alterTempViewDefinition(name, query)) {
      // a local/global temp view has been altered, we are done.
    } else {
      alterPermanentView(session, query)
    }

    Seq.empty[Row]
  }

  private def alterPermanentView(session: SparkSession, analyzedPlan: LogicalPlan): Unit = {
    val viewMeta = session.sessionState.catalog.getTableMetadata(name)
    if (viewMeta.tableType != CatalogTableType.VIEW) {
      throw new AnalysisException(s"${viewMeta.identifier} is not a view.")
    }

    val newProperties = generateViewProperties(viewMeta.properties, session, analyzedPlan)

    val updatedViewMeta = viewMeta.copy(
      schema = analyzedPlan.schema,
      properties = newProperties,
      viewText = Some(originalText))

    session.sessionState.catalog.alterTable(updatedViewMeta)
  }
}

object ViewHelper {

  import CatalogTable._

  /**
   * Generate the view default database in `properties`.
   */
  private def generateViewDefaultDatabase(databaseName: String): Map[String, String] = {
    Map(VIEW_DEFAULT_DATABASE -> databaseName)
  }

  /**
   * Generate the view query output column names in `properties`.
   */
  private def generateQueryColumnNames(columns: Seq[String]): Map[String, String] = {
    val props = new mutable.HashMap[String, String]
    if (columns.nonEmpty) {
      props.put(VIEW_QUERY_OUTPUT_NUM_COLUMNS, columns.length.toString)
      columns.zipWithIndex.foreach { case (colName, index) =>
        props.put(s"$VIEW_QUERY_OUTPUT_COLUMN_NAME_PREFIX$index", colName)
      }
    }
    props.toMap
  }

  /**
   * Remove the view query output column names in `properties`.
   */
  private def removeQueryColumnNames(properties: Map[String, String]): Map[String, String] = {
    // We can't use `filterKeys` here, as the map returned by `filterKeys` is not serializable,
    // while `CatalogTable` should be serializable.
    properties.filterNot { case (key, _) =>
      key.startsWith(VIEW_QUERY_OUTPUT_PREFIX)
    }
  }

  /**
   * Generate the view properties in CatalogTable, including:
   * 1. view default database that is used to provide the default database name on view resolution.
   * 2. the output column names of the query that creates a view, this is used to map the output of
   *    the view child to the view output during view resolution.
   *
   * @param properties the `properties` in CatalogTable.
   * @param session the spark session.
   * @param analyzedPlan the analyzed logical plan that represents the child of a view.
   * @return new view properties including view default database and query column names properties.
   */
  def generateViewProperties(
      properties: Map[String, String],
      session: SparkSession,
      analyzedPlan: LogicalPlan): Map[String, String] = {
    // Generate the view default database name.
    val queryOutput = analyzedPlan.schema.fieldNames
    val viewDefaultDatabase = session.sessionState.catalog.getCurrentDatabase

    removeQueryColumnNames(properties) ++
      generateViewDefaultDatabase(viewDefaultDatabase) ++
      generateQueryColumnNames(queryOutput)
  }
}
