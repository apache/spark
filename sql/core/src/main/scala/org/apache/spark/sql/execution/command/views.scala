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

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.{SQLConfHelper, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{AnalysisContext, GlobalTempView, LocalTempView, SchemaEvolution, SchemaUnsupported, ViewSchemaMode, ViewType}
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType, TemporaryViewRelation}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, SubqueryExpression, VariableReference}
import org.apache.spark.sql.catalyst.plans.logical.{AnalysisOnlyCommand, CTEInChildren, CTERelationDef, LogicalPlan, Project, View, WithCTE}
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.NamespaceHelper
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.types.{MetadataBuilder, StructType}
import org.apache.spark.sql.util.SchemaUtils
import org.apache.spark.util.ArrayImplicits._

/**
 * Create or replace a view with given query plan. This command will generate some view-specific
 * properties(e.g. view default database, view query output column names) and store them as
 * properties in metastore, if we need to create a permanent view.
 *
 * @param name the name of this view.
 * @param userSpecifiedColumns the output column names and optional comments specified by users,
 *                             can be Nil if not specified.
 * @param comment the comment of this view.
 * @param collation the collation of this view.
 * @param properties the properties of this view.
 * @param originalText the original SQL text of this view, can be None if this view is created via
 *                     Dataset API.
 * @param plan the logical plan that represents the view; this is used to generate the logical
 *             plan for temporary view and the view schema.
 * @param allowExisting if true, and if the view already exists, noop; if false, and if the view
 *                already exists, throws analysis exception.
 * @param replace if true, and if the view already exists, updates it; if false, and if the view
 *                already exists, throws analysis exception.
 * @param viewType the expected view type to be created with this command.
 * @param viewSchemaMode the tolerance of the view towards schema changes
 * @param isAnalyzed whether this command is analyzed or not.
 */
case class CreateViewCommand(
    name: TableIdentifier,
    userSpecifiedColumns: Seq[(String, Option[String])],
    comment: Option[String],
    collation: Option[String],
    properties: Map[String, String],
    originalText: Option[String],
    plan: LogicalPlan,
    allowExisting: Boolean,
    replace: Boolean,
    viewType: ViewType,
    viewSchemaMode: ViewSchemaMode = SchemaUnsupported,
    isAnalyzed: Boolean = false,
    referredTempFunctions: Seq[String] = Seq.empty)
  extends RunnableCommand with AnalysisOnlyCommand with CTEInChildren {

  import ViewHelper._

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[LogicalPlan]): CreateViewCommand = {
    assert(!isAnalyzed)
    copy(plan = newChildren.head)
  }

  // `plan` needs to be analyzed, but shouldn't be optimized so that caching works correctly.
  override def childrenToAnalyze: Seq[LogicalPlan] = plan :: Nil

  def markAsAnalyzed(analysisContext: AnalysisContext): LogicalPlan = {
    copy(
      isAnalyzed = true,
      // Collect the referred temporary functions from AnalysisContext
      referredTempFunctions = analysisContext.referredTempFunctionNames.toSeq)
  }

  private def isTemporary = viewType == LocalTempView || viewType == GlobalTempView

  override def run(sparkSession: SparkSession): Seq[Row] = {
    if (!isAnalyzed) {
      throw QueryCompilationErrors.logicalPlanForViewNotAnalyzedError()
    }
    val analyzedPlan = plan

    if (userSpecifiedColumns.nonEmpty) {
      if (userSpecifiedColumns.length > analyzedPlan.output.length) {
        throw QueryCompilationErrors.cannotCreateViewNotEnoughColumnsError(
          name, userSpecifiedColumns.map(_._1), analyzedPlan)
      } else if (userSpecifiedColumns.length < analyzedPlan.output.length) {
        throw QueryCompilationErrors.cannotCreateViewTooManyColumnsError(
          name, userSpecifiedColumns.map(_._1), analyzedPlan)
      }
      if (viewSchemaMode == SchemaEvolution) {
        throw SparkException.internalError(
          "View with user column list has viewSchemaMode EVOLUTION")
      }
    }

    val catalog = sparkSession.sessionState.catalog

    // When creating a permanent view, not allowed to reference temporary objects.
    // This should be called after `qe.assertAnalyzed()` (i.e., `child` can be resolved)
    verifyTemporaryObjectsNotExists(isTemporary, name, analyzedPlan, referredTempFunctions)
    verifyAutoGeneratedAliasesNotExists(analyzedPlan, isTemporary, name)

    if (viewType == LocalTempView) {
      val aliasedPlan = aliasPlan(sparkSession, analyzedPlan)
      val tableDefinition = createTemporaryViewRelation(
        name,
        sparkSession,
        replace,
        catalog.getRawTempView,
        originalText,
        analyzedPlan,
        aliasedPlan,
        referredTempFunctions)
      catalog.createTempView(name.table, tableDefinition, overrideIfExists = replace)
    } else if (viewType == GlobalTempView) {
      val db = sparkSession.sessionState.conf.getConf(StaticSQLConf.GLOBAL_TEMP_DATABASE)
      val viewIdent = TableIdentifier(name.table, Option(db))
      val aliasedPlan = aliasPlan(sparkSession, analyzedPlan)
      val tableDefinition = createTemporaryViewRelation(
        viewIdent,
        sparkSession,
        replace,
        catalog.getRawGlobalTempView,
        originalText,
        analyzedPlan,
        aliasedPlan,
        referredTempFunctions)
      catalog.createGlobalTempView(name.table, tableDefinition, overrideIfExists = replace)
    } else if (catalog.tableExists(name)) {
      val tableMetadata = catalog.getTableMetadata(name)
      if (allowExisting) {
        // Handles `CREATE VIEW IF NOT EXISTS v0 AS SELECT ...`. Does nothing when the target view
        // already exists.
      } else if (tableMetadata.tableType != CatalogTableType.VIEW) {
        throw QueryCompilationErrors.unsupportedCreateOrReplaceViewOnTableError(name, replace)
      } else if (replace) {
        // Detect cyclic view reference on CREATE OR REPLACE VIEW.
        val viewIdent = tableMetadata.identifier
        checkCyclicViewReference(analyzedPlan, Seq(viewIdent), viewIdent)

        // uncache the cached data before replacing an exists view
        logDebug(s"Try to uncache ${viewIdent.quotedString} before replacing.")
        CommandUtils.uncacheTableOrView(sparkSession, viewIdent)

        // Handles `CREATE OR REPLACE VIEW v0 AS SELECT ...`
        // Nothing we need to retain from the old view, so just drop and create a new one
        catalog.dropTable(viewIdent, ignoreIfNotExists = false, purge = false)
        catalog.createTable(prepareTable(sparkSession, analyzedPlan), ignoreIfExists = false)
      } else {
        // Handles `CREATE VIEW v0 AS SELECT ...`. Throws exception when the target view already
        // exists.
        throw QueryCompilationErrors.viewAlreadyExistsError(name)
      }
    } else {
      // Create the view if it doesn't exist.
      catalog.createTable(prepareTable(sparkSession, analyzedPlan), ignoreIfExists = allowExisting)
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
      throw QueryCompilationErrors.createPersistedViewFromDatasetAPINotAllowedError()
    }
    val aliasedSchema = CharVarcharUtils.getRawSchema(
      aliasPlan(session, analyzedPlan).schema, session.sessionState.conf)
    val newProperties = generateViewProperties(
      properties, session, analyzedPlan.schema.fieldNames, aliasedSchema.fieldNames, viewSchemaMode)

    CatalogTable(
      identifier = name,
      tableType = CatalogTableType.VIEW,
      storage = CatalogStorageFormat.empty,
      schema = aliasedSchema,
      properties = newProperties,
      viewOriginalText = originalText,
      viewText = originalText,
      comment = comment,
      collation = collation
    )
  }

  override def withCTEDefs(cteDefs: Seq[CTERelationDef]): LogicalPlan = {
    copy(plan = WithCTE(plan, cteDefs))
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
    query: LogicalPlan,
    isAnalyzed: Boolean = false,
    referredTempFunctions: Seq[String] = Seq.empty)
  extends RunnableCommand with AnalysisOnlyCommand with CTEInChildren {

  import ViewHelper._

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[LogicalPlan]): AlterViewAsCommand = {
    assert(!isAnalyzed)
    copy(query = newChildren.head)
  }

  override def childrenToAnalyze: Seq[LogicalPlan] = query :: Nil

  def markAsAnalyzed(analysisContext: AnalysisContext): LogicalPlan = {
    copy(
      isAnalyzed = true,
      // Collect the referred temporary functions from AnalysisContext
      referredTempFunctions = analysisContext.referredTempFunctionNames.toSeq)
  }

  override def run(session: SparkSession): Seq[Row] = {
    val isTemporary = session.sessionState.catalog.isTempView(name)
    verifyTemporaryObjectsNotExists(isTemporary, name, query, referredTempFunctions)
    verifyAutoGeneratedAliasesNotExists(query, isTemporary, name)
    if (isTemporary) {
      alterTemporaryView(session, query)
    } else {
      alterPermanentView(session, query)
    }
    Seq.empty[Row]
  }

  private def alterTemporaryView(session: SparkSession, analyzedPlan: LogicalPlan): Unit = {
    val catalog = session.sessionState.catalog
    val getRawTempView: String => Option[TemporaryViewRelation] = if (name.database.isEmpty) {
      catalog.getRawTempView
    } else {
      catalog.getRawGlobalTempView
    }
    val tableDefinition = createTemporaryViewRelation(
      name,
      session,
      replace = true,
      getRawTempView,
      Some(originalText),
      analyzedPlan,
      aliasedPlan = analyzedPlan,
      referredTempFunctions)
    session.sessionState.catalog.alterTempViewDefinition(name, tableDefinition)
  }

  private def alterPermanentView(session: SparkSession, analyzedPlan: LogicalPlan): Unit = {
    val viewMeta = session.sessionState.catalog.getTableMetadata(name)

    // Detect cyclic view reference on ALTER VIEW.
    val viewIdent = viewMeta.identifier
    checkCyclicViewReference(analyzedPlan, Seq(viewIdent), viewIdent)

    logDebug(s"Try to uncache ${viewIdent.quotedString} before replacing.")
    CommandUtils.uncacheTableOrView(session, viewIdent)

    val newProperties = generateViewProperties(
      viewMeta.properties,
      session,
      analyzedPlan.schema.fieldNames, // The query output column names
      analyzedPlan.schema.fieldNames, // Will match the view schema names
      viewMeta.viewSchemaMode)

    val newSchema = CharVarcharUtils.getRawSchema(analyzedPlan.schema)
    val updatedViewMeta = viewMeta.copy(
      schema = newSchema,
      properties = newProperties,
      viewOriginalText = Some(originalText),
      viewText = Some(originalText))

    session.sessionState.catalog.alterTable(updatedViewMeta)
  }

  override def withCTEDefs(cteDefs: Seq[CTERelationDef]): LogicalPlan = {
    copy(query = WithCTE(query, cteDefs))
  }
}

/**
 * Alter a view with given schema binding. If the view name contains database prefix, this command
 * will alter a permanent view matching the given name, or throw an exception if view not exist.
 * Else, this command will try to alter a temporary view first, if view not exist, try permanent
 * view next, if still not exist, throw an exception.
 *
 * @param name the name of this view.
 * @param viewSchemaMode The new schema binding mode.
 */
case class AlterViewSchemaBindingCommand(name: TableIdentifier, viewSchemaMode: ViewSchemaMode)
  extends LeafRunnableCommand {

  import ViewHelper._

  override def run(session: SparkSession): Seq[Row] = {
    val isTemporary = session.sessionState.catalog.isTempView(name)
    if (isTemporary) {
      throw QueryCompilationErrors.cannotAlterTempViewWithSchemaBindingError()
    }
    alterPermanentView(session, viewSchemaMode)
    Seq.empty[Row]
  }

  private def alterPermanentView(session: SparkSession, viewSchemaMode: ViewSchemaMode): Unit = {
    val viewMeta = session.sessionState.catalog.getTableMetadata(name)

    val viewIdent = viewMeta.identifier

    logDebug(s"Try to uncache ${viewIdent.quotedString} before replacing.")
    CommandUtils.uncacheTableOrView(session, viewIdent)

    val newProperties = generateViewProperties(
      viewMeta.properties,
      session,
      viewMeta.viewQueryColumnNames.toArray,
      viewMeta.schema.fieldNames,
      viewSchemaMode)

    val updatedViewMeta = viewMeta.copy(properties = newProperties)

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
    tableIdentifierPattern: Option[String],
    override val output: Seq[Attribute]) extends LeafRunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog

    // Show the information of views.
    val views = tableIdentifierPattern.map(catalog.listViews(databaseName, _))
      .getOrElse(catalog.listViews(databaseName, "*"))
    views.map { tableIdent =>
      val namespace = tableIdent.database.toArray.quoted
      val tableName = tableIdent.table
      val isTemp = catalog.isTempView(tableIdent)

      Row(namespace, tableName, isTemp)
    }
  }
}

object ViewHelper extends SQLConfHelper with Logging {

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
    "spark.sql.hive.convertInsertingUnpartitionedTable",
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
   * Get all configurations that are modifiable and should be captured.
   */
  def getModifiedConf(conf: SQLConf): Map[String, String] = {
    conf.getAllConfs.filter { case (k, _) =>
      conf.isModifiable(k) && shouldCaptureConfig(k)
    }
  }

  /**
   * Convert the view SQL configs to `properties`.
   */
  private def sqlConfigsToProps(conf: SQLConf): Map[String, String] = {
    val modifiedConfs = getModifiedConf(conf)
    // Some configs have dynamic default values, such as SESSION_LOCAL_TIMEZONE whose
    // default value relies on the JVM system timezone. We need to always capture them to
    // to make sure we apply the same configs when reading the view.
    val alwaysCaptured = Seq(SQLConf.SESSION_LOCAL_TIMEZONE)
      .filter(c => !modifiedConfs.contains(c.key))
      .map(c => (c.key, conf.getConf(c)))

    val props = new mutable.HashMap[String, String]
    for ((key, value) <- modifiedConfs ++ alwaysCaptured) {
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
   * Convert the viewSchemaMode to `properties`.
   * If the mode UNSUPPORTED do not store anything for backward compatibility.
   */
  private def viewSchemaModeToProps(viewSchemaMode: ViewSchemaMode): Map[String, String] = {
    if (viewSchemaMode == SchemaUnsupported) {
      Map.empty
    } else {
      val props = new mutable.HashMap[String, String]
      props.put(VIEW_SCHEMA_MODE, viewSchemaMode.toString)
      props.toMap
    }
  }

  /**
   * Convert the temporary object names to `properties`.
   */
  private def referredTempNamesToProps(
      viewNames: Seq[Seq[String]],
      functionsNames: Seq[String],
      variablesNames: Seq[Seq[String]]): Map[String, String] = {
    val viewNamesJson =
      JArray(viewNames.map(nameParts => JArray(nameParts.map(JString).toList)).toList)
    val functionsNamesJson = JArray(functionsNames.map(JString).toList)
    val variablesNamesJson =
      JArray(variablesNames.map(nameParts => JArray(nameParts.map(JString).toList)).toList)

    val props = new mutable.HashMap[String, String]
    props.put(VIEW_REFERRED_TEMP_VIEW_NAMES, compact(render(viewNamesJson)))
    props.put(VIEW_REFERRED_TEMP_FUNCTION_NAMES, compact(render(functionsNamesJson)))
    props.put(VIEW_REFERRED_TEMP_VARIABLE_NAMES, compact(render(variablesNamesJson)))
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
        key.startsWith(VIEW_REFERRED_TEMP_FUNCTION_NAMES) ||
        key.startsWith(VIEW_REFERRED_TEMP_VARIABLE_NAMES)
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
      queryOutput: Array[String],
      fieldNames: Array[String],
      viewSchemaMode: ViewSchemaMode,
      tempViewNames: Seq[Seq[String]] = Seq.empty,
      tempFunctionNames: Seq[String] = Seq.empty,
      tempVariableNames: Seq[Seq[String]] = Seq.empty): Map[String, String] = {

    val conf = session.sessionState.conf

    // Generate the query column names, throw an AnalysisException if there exists duplicate column
    // names.
    SchemaUtils.checkColumnNameDuplication(fieldNames.toImmutableArraySeq, conf.resolver)

    // Generate the view default catalog and namespace, as well as captured SQL configs.
    val manager = session.sessionState.catalogManager
    removeReferredTempNames(removeSQLConfigs(removeQueryColumnNames(properties))) ++
      catalogAndNamespaceToProps(
        manager.currentCatalog.name, manager.currentNamespace.toImmutableArraySeq) ++
      sqlConfigsToProps(conf) ++
      generateQueryColumnNames(queryOutput.toImmutableArraySeq) ++
      referredTempNamesToProps(tempViewNames, tempFunctionNames, tempVariableNames) ++
      viewSchemaModeToProps(viewSchemaMode)
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
          throw QueryCompilationErrors.recursiveViewDetectedError(viewIdent, newPath)
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

  def verifyAutoGeneratedAliasesNotExists(
      child: LogicalPlan, isTemporary: Boolean, name: TableIdentifier): Unit = {
    if (!isTemporary && !conf.allowAutoGeneratedAliasForView) {
      child.output.foreach { attr =>
        if (attr.metadata.contains("__autoGeneratedAlias")) {
          throw QueryCompilationErrors
            .notAllowedToCreatePermanentViewWithoutAssigningAliasForExpressionError(name, attr)
        }
      }
    }
  }

  /**
   * Permanent views are not allowed to reference temp objects, including temp function and views
   */
  def verifyTemporaryObjectsNotExists(
      isTemporary: Boolean,
      name: TableIdentifier,
      child: LogicalPlan,
      referredTempFunctions: Seq[String]): Unit = {
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
    if (!isTemporary) {
      val tempViews = collectTemporaryViews(child)
      tempViews.foreach { nameParts =>
        throw QueryCompilationErrors.notAllowedToCreatePermanentViewByReferencingTempViewError(
          name, nameParts.quoted)
      }
      referredTempFunctions.foreach { funcName =>
        throw QueryCompilationErrors.notAllowedToCreatePermanentViewByReferencingTempFuncError(
          name, funcName)
      }
      val tempVars = collectTemporaryVariables(child)
      tempVars.foreach { nameParts =>
        throw QueryCompilationErrors.notAllowedToCreatePermanentViewByReferencingTempVarError(
          name, nameParts.quoted)
      }
    }
  }

  /**
   * Collect all temporary views and return the identifiers separately.
   */
  private def collectTemporaryViews(child: LogicalPlan): Seq[Seq[String]] = {
    def collectTempViews(child: LogicalPlan): Seq[Seq[String]] = {
      child.flatMap {
        case view: View if view.isTempView =>
          val ident = view.desc.identifier
          Seq(ident.database.toSeq :+ ident.table)
        case plan => plan.expressions.flatMap(_.flatMap {
          case e: SubqueryExpression => collectTempViews(e.plan)
          case _ => Seq.empty
        })
      }.distinct
    }
    collectTempViews(child)
  }

  /**
   * Collect all temporary SQL variables and return the identifiers separately.
   */
  def collectTemporaryVariables(child: LogicalPlan): Seq[Seq[String]] = {
    def collectTempVars(child: LogicalPlan): Seq[Seq[String]] = {
      child.flatMap { plan =>
        plan.expressions.flatMap(_.flatMap {
          case e: SubqueryExpression => collectTempVars(e.plan)
          case r: VariableReference => Seq(r.originalNameParts)
          case _ => Seq.empty
        })
      }.distinct
    }
    collectTempVars(child)
  }

  /**
   * Returns a [[TemporaryViewRelation]] that contains information about a temporary view
   * to create, given an analyzed plan of the view. If a temp view is to be replaced and it is
   * cached, it will be uncached before being replaced.
   *
   * @param name the name of the temporary view to create/replace.
   * @param session the spark session.
   * @param replace if true and the existing view is cached, it will be uncached.
   * @param getRawTempView the function that returns an optional raw plan of the local or
   *                       global temporary view.
   * @param originalText the original SQL text of this view, can be None if this view is created via
   *                     Dataset API or spark.sql.legacy.storeAnalyzedPlanForView is set to true.
   * @param analyzedPlan the logical plan that represents the view; this is used to generate the
   *                     logical plan for temporary view and the view schema.
   * @param aliasedPlan the aliased logical plan based on the user specified columns. If there are
   *                    no user specified plans, this should be same as `analyzedPlan`.
   */
  def createTemporaryViewRelation(
      name: TableIdentifier,
      session: SparkSession,
      replace: Boolean,
      getRawTempView: String => Option[TemporaryViewRelation],
      originalText: Option[String],
      analyzedPlan: LogicalPlan,
      aliasedPlan: LogicalPlan,
      referredTempFunctions: Seq[String]): TemporaryViewRelation = {
    val uncache = getRawTempView(name.table).map { r =>
      needsToUncache(r, aliasedPlan)
    }.getOrElse(false)
    val storeAnalyzedPlanForView = session.sessionState.conf.storeAnalyzedPlanForView ||
      originalText.isEmpty
    if (replace && uncache) {
      logDebug(s"Try to uncache ${name.quotedString} before replacing.")
      if (!storeAnalyzedPlanForView) {
        // Skip cyclic check because when stored analyzed plan for view, the depended
        // view is already converted to the underlying tables. So no cyclic views.
        checkCyclicViewReference(analyzedPlan, Seq(name), name)
      }
      CommandUtils.uncacheTableOrView(session, name)
    }
    if (!storeAnalyzedPlanForView) {
      TemporaryViewRelation(
        prepareTemporaryView(
          name,
          session,
          analyzedPlan,
          aliasedPlan.schema,
          originalText.get,
          referredTempFunctions))
    } else {
      TemporaryViewRelation(
        prepareTemporaryViewStoringAnalyzedPlan(name, aliasedPlan),
        Some(aliasedPlan))
    }
  }

  /**
   * Checks if need to uncache the temp view being replaced.
   */
  private def needsToUncache(
      rawTempView: TemporaryViewRelation,
      aliasedPlan: LogicalPlan): Boolean = rawTempView.plan match {
    // Do not need to uncache if the to-be-replaced temp view plan and the new plan are the
    // same-result plans.
    case Some(p) => !p.sameResult(aliasedPlan)
    // If TemporaryViewRelation doesn't store the analyzed view, always uncache.
    case None => true
  }

  /**
   * Returns a [[CatalogTable]] that contains information for temporary view.
   * Generate the view-specific properties(e.g. view default database, view query output
   * column names) and store them as properties in the CatalogTable, and also creates
   * the proper schema for the view.
   */
  private def prepareTemporaryView(
      viewName: TableIdentifier,
      session: SparkSession,
      analyzedPlan: LogicalPlan,
      viewSchema: StructType,
      originalText: String,
      tempFunctions: Seq[String]): CatalogTable = {

    val catalog = session.sessionState.catalog
    val tempViews = collectTemporaryViews(analyzedPlan)
    val tempVariables = collectTemporaryVariables(analyzedPlan)
    // TBLPROPERTIES is not allowed for temporary view, so we don't use it for
    // generating temporary view properties
    val newProperties = generateViewProperties(
      Map.empty, session, analyzedPlan.schema.fieldNames, viewSchema.fieldNames, SchemaUnsupported,
      tempViews, tempFunctions, tempVariables)

    CatalogTable(
      identifier = viewName,
      tableType = CatalogTableType.VIEW,
      storage = CatalogStorageFormat.empty,
      schema = viewSchema,
      viewText = Some(originalText),
      properties = newProperties)
  }

  /**
   * Returns a [[CatalogTable]] that contains information for the temporary view storing
   * an analyzed plan.
   */
  private def prepareTemporaryViewStoringAnalyzedPlan(
      viewName: TableIdentifier,
      analyzedPlan: LogicalPlan): CatalogTable = {
    CatalogTable(
      identifier = viewName,
      tableType = CatalogTableType.VIEW,
      storage = CatalogStorageFormat.empty,
      schema = analyzedPlan.schema,
      properties = Map((VIEW_STORING_ANALYZED_PLAN, "true")))
  }
}
