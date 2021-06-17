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

import org.json4s.JsonAST.{JArray, JString}
import org.json4s.jackson.JsonMethods._

import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{GlobalTempView, LocalTempView, PersistedView, UnresolvedFunction, UnresolvedRelation, ViewType}
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType, SessionCatalog, TemporaryViewRelation}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, View, With}
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.NamespaceHelper
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.types.{BooleanType, MetadataBuilder, StringType, StructType}
import org.apache.spark.sql.util.SchemaUtils

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

    val catalog = sparkSession.sessionState.catalog

    // When creating a permanent view, not allowed to reference temporary objects.
    // This should be called after `qe.assertAnalyzed()` (i.e., `child` can be resolved)
    verifyTemporaryObjectsNotExists(catalog, isTemporary, name, child)

    if (viewType == LocalTempView) {
      val aliasedPlan = aliasPlan(sparkSession, analyzedPlan)
      if (replace && catalog.getRawTempView(name.table).isDefined &&
          !catalog.getRawTempView(name.table).get.sameResult(aliasedPlan)) {
        logInfo(s"Try to uncache ${name.quotedString} before replacing.")
        checkCyclicViewReference(analyzedPlan, Seq(name), name)
        CommandUtils.uncacheTableOrView(sparkSession, name.quotedString)
      }
      // If there is no sql text (e.g. from Dataset API), we will always store the analyzed plan
      val tableDefinition = if (!conf.storeAnalyzedPlanForView && originalText.nonEmpty) {
        TemporaryViewRelation(
          prepareTemporaryView(
            name,
            sparkSession,
            analyzedPlan,
            aliasedPlan.schema,
            originalText,
            child))
      } else {
        aliasedPlan
      }
      catalog.createTempView(name.table, tableDefinition, overrideIfExists = replace)
    } else if (viewType == GlobalTempView) {
      val db = sparkSession.sessionState.conf.getConf(StaticSQLConf.GLOBAL_TEMP_DATABASE)
      val viewIdent = TableIdentifier(name.table, Option(db))
      val aliasedPlan = aliasPlan(sparkSession, analyzedPlan)
      if (replace && catalog.getRawGlobalTempView(name.table).isDefined &&
          !catalog.getRawGlobalTempView(name.table).get.sameResult(aliasedPlan)) {
        logInfo(s"Try to uncache ${viewIdent.quotedString} before replacing.")
        checkCyclicViewReference(analyzedPlan, Seq(viewIdent), viewIdent)
        CommandUtils.uncacheTableOrView(sparkSession, viewIdent.quotedString)
      }
      val tableDefinition = if (!conf.storeAnalyzedPlanForView && originalText.nonEmpty) {
        TemporaryViewRelation(
          prepareTemporaryView(
            viewIdent,
            sparkSession,
            analyzedPlan,
            aliasedPlan.schema,
            originalText,
            child))
      } else {
        aliasedPlan
      }
      catalog.createGlobalTempView(name.table, tableDefinition, overrideIfExists = replace)
    } else if (catalog.tableExists(name)) {
      val tableMetadata = catalog.getTableMetadata(name)
      if (allowExisting) {
        // Handles `CREATE VIEW IF NOT EXISTS v0 AS SELECT ...`. Does nothing when the target view
        // already exists.
      } else if (tableMetadata.tableType != CatalogTableType.VIEW) {
        throw new AnalysisException(s"$name is not a view")
      } else if (replace) {
        // Detect cyclic view reference on CREATE OR REPLACE VIEW.
        val viewIdent = tableMetadata.identifier
        checkCyclicViewReference(analyzedPlan, Seq(viewIdent), viewIdent)

        // uncache the cached data before replacing an exists view
        logDebug(s"Try to uncache ${viewIdent.quotedString} before replacing.")
        CommandUtils.uncacheTableOrView(sparkSession, viewIdent.quotedString)

        // Handles `CREATE OR REPLACE VIEW v0 AS SELECT ...`
        // Nothing we need to retain from the old view, so just drop and create a new one
        catalog.dropTable(viewIdent, ignoreIfNotExists = false, purge = false)
        catalog.createTable(prepareTable(sparkSession, analyzedPlan), ignoreIfExists = false)
      } else {
        // Handles `CREATE VIEW v0 AS SELECT ...`. Throws exception when the target view already
        // exists.
        throw new AnalysisException(
          s"View $name already exists. If you want to update the view definition, " +
            "please use ALTER VIEW AS or CREATE OR REPLACE VIEW AS")
      }
    } else {
      // Create the view if it doesn't exist.
      catalog.createTable(prepareTable(sparkSession, analyzedPlan), ignoreIfExists = false)
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
    val aliasedSchema = CharVarcharUtils.getRawSchema(
      aliasPlan(session, analyzedPlan).schema)
    val newProperties = generateViewProperties(
      properties, session, analyzedPlan, aliasedSchema.fieldNames)

    CatalogTable(
      identifier = name,
      tableType = CatalogTableType.VIEW,
      storage = CatalogStorageFormat.empty,
      schema = aliasedSchema,
      properties = newProperties,
      viewOriginalText = originalText,
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
    // If the plan cannot be analyzed, throw an exception and don't proceed.
    val qe = session.sessionState.executePlan(query)
    qe.assertAnalyzed()
    val analyzedPlan = qe.analyzed

    if (session.sessionState.catalog.isTemporaryTable(name)) {
      alterTemporaryView(session, analyzedPlan)
    } else {
      alterPermanentView(session, analyzedPlan)
    }
    Seq.empty[Row]
  }

  private def alterTemporaryView(session: SparkSession, analyzedPlan: LogicalPlan): Unit = {
    val tableDefinition = if (conf.storeAnalyzedPlanForView) {
      analyzedPlan
    } else {
      checkCyclicViewReference(analyzedPlan, Seq(name), name)
      TemporaryViewRelation(
        prepareTemporaryView(
          name, session, analyzedPlan, analyzedPlan.schema, Some(originalText), query))
    }
    session.sessionState.catalog.alterTempViewDefinition(name, tableDefinition)
  }

  private def alterPermanentView(session: SparkSession, analyzedPlan: LogicalPlan): Unit = {
    val viewMeta = session.sessionState.catalog.getTableMetadata(name)
    if (viewMeta.tableType != CatalogTableType.VIEW) {
      throw new AnalysisException(s"${viewMeta.identifier} is not a view.")
    }

    // Detect cyclic view reference on ALTER VIEW.
    val viewIdent = viewMeta.identifier
    checkCyclicViewReference(analyzedPlan, Seq(viewIdent), viewIdent)

    val newProperties = generateViewProperties(
      viewMeta.properties, session, analyzedPlan, analyzedPlan.schema.fieldNames)

    val newSchema = CharVarcharUtils.getRawSchema(analyzedPlan.schema)
    val updatedViewMeta = viewMeta.copy(
      schema = newSchema,
      properties = newProperties,
      viewOriginalText = Some(originalText),
      viewText = Some(originalText))

    session.sessionState.catalog.alterTable(updatedViewMeta)
  }
}

/**
 * A command for users to get views in the given database.
 * If a databaseName is not given, the current database will be used.
 * The syntax of using this command in SQL is:
 * {{{
 *   SHOW VIEWS [(IN|FROM) database_name] [[LIKE] 'identifier_with_wildcards'];
 * }}}
 */
case class ShowViewsCommand(
    databaseName: String,
    tableIdentifierPattern: Option[String]) extends RunnableCommand {

  // The result of SHOW VIEWS has three basic columns: namespace, viewName and isTemporary.
  override val output: Seq[Attribute] = Seq(
    AttributeReference("namespace", StringType, nullable = false)(),
    AttributeReference("viewName", StringType, nullable = false)(),
    AttributeReference("isTemporary", BooleanType, nullable = false)())

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog

    // Show the information of views.
    val views = tableIdentifierPattern.map(catalog.listViews(databaseName, _))
      .getOrElse(catalog.listViews(databaseName, "*"))
    views.map { tableIdent =>
      val namespace = tableIdent.database.toArray.quoted
      val tableName = tableIdent.table
      val isTemp = catalog.isTemporaryTable(tableIdent)

      Row(namespace, tableName, isTemp)
    }
  }
}

object ViewHelper {

  private val configPrefixDenyList = Seq(
    SQLConf.MAX_NESTED_VIEW_DEPTH.key,
    "spark.sql.optimizer.",
    "spark.sql.codegen.",
    "spark.sql.execution.",
    "spark.sql.shuffle.",
    "spark.sql.adaptive.",
    // ignore optimization configs used in `RelationConversions`
    "spark.sql.hive.convertMetastoreParquet",
    "spark.sql.hive.convertMetastoreOrc",
    "spark.sql.hive.convertInsertingPartitionedTable",
    "spark.sql.hive.convertMetastoreCtas",
    SQLConf.ADDITIONAL_REMOTE_REPOSITORIES.key)

  private val configAllowList = Seq(
    SQLConf.DISABLE_HINTS.key
  )

  /**
   * Capture view config either of:
   * 1. exists in allowList
   * 2. do not exists in denyList
   */
  private def shouldCaptureConfig(key: String): Boolean = {
    configAllowList.exists(prefix => key.equals(prefix)) ||
      !configPrefixDenyList.exists(prefix => key.startsWith(prefix))
  }

  import CatalogTable._

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
   * Convert the view SQL configs to `properties`.
   */
  private def sqlConfigsToProps(conf: SQLConf): Map[String, String] = {
    val modifiedConfs = conf.getAllConfs.filter { case (k, _) =>
      conf.isModifiable(k) && shouldCaptureConfig(k)
    }
    val props = new mutable.HashMap[String, String]
    for ((key, value) <- modifiedConfs) {
      props.put(s"$VIEW_SQL_CONFIG_PREFIX$key", value)
    }
    props.toMap
  }

  /**
   * Remove the view SQL configs in `properties`.
   */
  private def removeSQLConfigs(properties: Map[String, String]): Map[String, String] = {
    // We can't use `filterKeys` here, as the map returned by `filterKeys` is not serializable,
    // while `CatalogTable` should be serializable.
    properties.filterNot { case (key, _) =>
      key.startsWith(VIEW_SQL_CONFIG_PREFIX)
    }
  }

  /**
   * Convert the temporary object names to `properties`.
   */
  private def referredTempNamesToProps(
      viewNames: Seq[Seq[String]], functionsNames: Seq[String]): Map[String, String] = {
    val viewNamesJson =
      JArray(viewNames.map(nameParts => JArray(nameParts.map(JString).toList)).toList)
    val functionsNamesJson = JArray(functionsNames.map(JString).toList)

    val props = new mutable.HashMap[String, String]
    props.put(VIEW_REFERRED_TEMP_VIEW_NAMES, compact(render(viewNamesJson)))
    props.put(VIEW_REFERRED_TEMP_FUNCTION_NAMES, compact(render(functionsNamesJson)))
    props.toMap
  }

  /**
   * Remove the temporary object names in `properties`.
   */
  private def removeReferredTempNames(properties: Map[String, String]): Map[String, String] = {
    // We can't use `filterKeys` here, as the map returned by `filterKeys` is not serializable,
    // while `CatalogTable` should be serializable.
    properties.filterNot { case (key, _) =>
      key.startsWith(VIEW_REFERRED_TEMP_VIEW_NAMES) ||
        key.startsWith(VIEW_REFERRED_TEMP_FUNCTION_NAMES)
    }
  }


  /**
   * Generate the view properties in CatalogTable, including:
   * 1. view default database that is used to provide the default database name on view resolution.
   * 2. the output column names of the query that creates a view, this is used to map the output of
   *    the view child to the view output during view resolution.
   * 3. the SQL configs when creating the view.
   *
   * @param properties the `properties` in CatalogTable.
   * @param session the spark session.
   * @param analyzedPlan the analyzed logical plan that represents the child of a view.
   * @return new view properties including view default database and query column names properties.
   */
  def generateViewProperties(
      properties: Map[String, String],
      session: SparkSession,
      analyzedPlan: LogicalPlan,
      fieldNames: Array[String],
      tempViewNames: Seq[Seq[String]] = Seq.empty,
      tempFunctionNames: Seq[String] = Seq.empty): Map[String, String] = {
    // for createViewCommand queryOutput may be different from fieldNames
    val queryOutput = analyzedPlan.schema.fieldNames

    val conf = session.sessionState.conf

    // Generate the query column names, throw an AnalysisException if there exists duplicate column
    // names.
    SchemaUtils.checkColumnNameDuplication(
      fieldNames, "in the view definition", conf.resolver)

    // Generate the view default catalog and namespace, as well as captured SQL configs.
    val manager = session.sessionState.catalogManager
    removeReferredTempNames(removeSQLConfigs(removeQueryColumnNames(properties))) ++
      catalogAndNamespaceToProps(manager.currentCatalog.name, manager.currentNamespace) ++
      sqlConfigsToProps(conf) ++
      generateQueryColumnNames(queryOutput) ++
      referredTempNamesToProps(tempViewNames, tempFunctionNames)
  }

  /**
   * Recursively search the logical plan to detect cyclic view references, throw an
   * AnalysisException if cycle detected.
   *
   * A cyclic view reference is a cycle of reference dependencies, for example, if the following
   * statements are executed:
   * CREATE VIEW testView AS SELECT id FROM tbl
   * CREATE VIEW testView2 AS SELECT id FROM testView
   * ALTER VIEW testView AS SELECT * FROM testView2
   * The view `testView` references `testView2`, and `testView2` also references `testView`,
   * therefore a reference cycle (testView -> testView2 -> testView) exists.
   *
   * @param plan the logical plan we detect cyclic view references from.
   * @param path the path between the altered view and current node.
   * @param viewIdent the table identifier of the altered view, we compare two views by the
   *                  `desc.identifier`.
   */
  def checkCyclicViewReference(
      plan: LogicalPlan,
      path: Seq[TableIdentifier],
      viewIdent: TableIdentifier): Unit = {
    plan match {
      case v: View =>
        val ident = v.desc.identifier
        val newPath = path :+ ident
        // If the table identifier equals to the `viewIdent`, current view node is the same with
        // the altered view. We detect a view reference cycle, should throw an AnalysisException.
        if (ident == viewIdent) {
          throw new AnalysisException(s"Recursive view $viewIdent detected " +
            s"(cycle: ${newPath.mkString(" -> ")})")
        } else {
          v.children.foreach { child =>
            checkCyclicViewReference(child, newPath, viewIdent)
          }
        }
      case _ =>
        plan.children.foreach(child => checkCyclicViewReference(child, path, viewIdent))
    }

    // Detect cyclic references from subqueries.
    plan.expressions.foreach { expr =>
      expr match {
        case s: SubqueryExpression =>
          checkCyclicViewReference(s.plan, path, viewIdent)
        case _ => // Do nothing.
      }
    }
  }


  /**
   * Permanent views are not allowed to reference temp objects, including temp function and views
   */
  def verifyTemporaryObjectsNotExists(
      catalog: SessionCatalog,
      isTemporary: Boolean,
      name: TableIdentifier,
      child: LogicalPlan): Unit = {
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
    if (!isTemporary) {
      val (tempViews, tempFunctions) = collectTemporaryObjects(catalog, child)
      tempViews.foreach { nameParts =>
        throw new AnalysisException(s"Not allowed to create a permanent view $name by " +
          s"referencing a temporary view ${nameParts.quoted}. " +
          "Please create a temp view instead by CREATE TEMP VIEW")
      }
      tempFunctions.foreach { funcName =>
        throw new AnalysisException(s"Not allowed to create a permanent view $name by " +
          s"referencing a temporary function `${funcName}`")
      }
    }
  }

  /**
   * Collect all temporary views and functions and return the identifiers separately
   * This func traverses the unresolved plan `child`. Below are the reasons:
   * 1) Analyzer replaces unresolved temporary views by a SubqueryAlias with the corresponding
   * logical plan. After replacement, it is impossible to detect whether the SubqueryAlias is
   * added/generated from a temporary view.
   * 2) The temp functions are represented by multiple classes. Most are inaccessible from this
   * package (e.g., HiveGenericUDF).
   */
  private def collectTemporaryObjects(
      catalog: SessionCatalog, child: LogicalPlan): (Seq[Seq[String]], Seq[String]) = {
    def collectTempViews(child: LogicalPlan): Seq[Seq[String]] = {
      child.flatMap {
        case UnresolvedRelation(nameParts, _, _) if catalog.isTempView(nameParts) =>
          Seq(nameParts)
        case w: With if !w.resolved => w.innerChildren.flatMap(collectTempViews)
        case plan if !plan.resolved => plan.expressions.flatMap(_.flatMap {
          case e: SubqueryExpression => collectTempViews(e.plan)
          case _ => Seq.empty
        })
        case _ => Seq.empty
      }.distinct
    }

    def collectTempFunctions(child: LogicalPlan): Seq[String] = {
      child.flatMap {
        case w: With if !w.resolved => w.innerChildren.flatMap(collectTempFunctions)
        case plan if !plan.resolved =>
          plan.expressions.flatMap(_.flatMap {
            case e: SubqueryExpression => collectTempFunctions(e.plan)
            case e: UnresolvedFunction if catalog.isTemporaryFunction(e.name) =>
              Seq(e.name.funcName)
            case _ => Seq.empty
          })
        case _ => Seq.empty
      }.distinct
    }
    (collectTempViews(child), collectTempFunctions(child))
  }


  /**
   * Returns a [[CatalogTable]] that contains information for temporary view.
   * Generate the view-specific properties(e.g. view default database, view query output
   * column names) and store them as properties in the CatalogTable, and also creates
   * the proper schema for the view.
   */
  def prepareTemporaryView(
      viewName: TableIdentifier,
      session: SparkSession,
      analyzedPlan: LogicalPlan,
      viewSchema: StructType,
      originalText: Option[String],
      child: LogicalPlan): CatalogTable = {

    val catalog = session.sessionState.catalog
    val (tempViews, tempFunctions) = collectTemporaryObjects(catalog, child)
    // TBLPROPERTIES is not allowed for temporary view, so we don't use it for
    // generating temporary view properties
    val newProperties = generateViewProperties(
      Map.empty, session, analyzedPlan, viewSchema.fieldNames, tempViews, tempFunctions)

    CatalogTable(
      identifier = viewName,
      tableType = CatalogTableType.VIEW,
      storage = CatalogStorageFormat.empty,
      schema = viewSchema,
      viewText = originalText,
      properties = newProperties)
  }
}
