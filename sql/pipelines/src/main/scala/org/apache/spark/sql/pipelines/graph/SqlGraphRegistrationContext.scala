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
package org.apache.spark.sql.pipelines.graph

import scala.collection.mutable

import org.apache.spark.{SparkException, SparkRuntimeException}
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical.{CreateFlowCommand, CreateMaterializedViewAsSelect, CreateStreamingTable, CreateStreamingTableAsSelect, CreateView, InsertIntoStatement, LogicalPlan}
import org.apache.spark.sql.catalyst.util.StringUtils
import org.apache.spark.sql.execution.command.{CreateViewCommand, SetCatalogCommand, SetCommand, SetNamespaceCommand}
import org.apache.spark.sql.pipelines.{Language, QueryOriginType}
import org.apache.spark.sql.types.StructType

/**
 * Data class for all state that is accumulated while processing a particular
 * [[SqlGraphRegistrationContext]].
 *
 * @param initialCatalogOpt The initial catalog to assume.
 * @param initialDatabaseOpt The initial database to assume.
 * @param initialSqlConf The initial sql confs to assume.
 */
class SqlGraphRegistrationContextState(
    initialCatalogOpt: Option[String],
    initialDatabaseOpt: Option[String],
    initialSqlConf: Map[String, String]) {
  private val sqlConf = mutable.HashMap[String, String](initialSqlConf.toSeq: _*)
  private var currentCatalogOpt: Option[String] = initialCatalogOpt
  private var currentDatabaseOpt: Option[String] = initialDatabaseOpt

  def getSqlConf: Map[String, String] = sqlConf.toMap
  def getCurrentCatalogOpt: Option[String] = currentCatalogOpt
  def getCurrentDatabaseOpt: Option[String] = currentDatabaseOpt

  def setSqlConf(k: String, v: String): Unit = sqlConf.put(k, v)
  def setCurrentCatalog(catalogName: String): Unit = {
    currentCatalogOpt = Option(catalogName)
  }
  def setCurrentDatabase(databaseName: String): Unit = {
    currentDatabaseOpt = Option(databaseName)
  }
  def clearCurrentDatabase(): Unit = {
    currentDatabaseOpt = None
  }
}

case class SqlGraphElementRegistrationException(
    msg: String,
    queryOrigin: QueryOrigin) extends AnalysisException(
  errorClass = "PIPELINE_SQL_GRAPH_ELEMENT_REGISTRATION_ERROR",
  messageParameters = Map(
    "message" -> msg,
    "offendingQuery" -> SqlGraphElementRegistrationException.offendingQueryString(queryOrigin),
    "codeLocation" -> SqlGraphElementRegistrationException.codeLocationStr(queryOrigin)
  )
)

object SqlGraphElementRegistrationException {
  private def codeLocationStr(queryOrigin: QueryOrigin): String = queryOrigin.filePath match {
    case Some(fileName) =>
      queryOrigin.line match {
        case Some(lineNumber) =>
          s"Query defined at $fileName:$lineNumber"
        case None =>
          s"Query defined in file $fileName"
      }
    case None => ""
  }

  private def offendingQueryString(queryOrigin: QueryOrigin): String = queryOrigin.sqlText match {
    case Some(sqlText) =>
      s"""
         |Offending query:
         |${sqlText}
         |""".stripMargin
    case None => ""
  }
}

/**
 * SQL statement processor context. At any instant, an instance of this class holds the "active"
 * catalog/schema in use within this SQL statement processing context, and tables/views/flows that
 * have been registered from SQL statements within this context.
 */
class SqlGraphRegistrationContext(
    graphRegistrationContext: GraphRegistrationContext) {
  import SqlGraphRegistrationContext._

  private val defaultDatabase = graphRegistrationContext.defaultDatabase
  private val defaultCatalog = graphRegistrationContext.defaultCatalog

  private val context = new SqlGraphRegistrationContextState(
    initialCatalogOpt = Option(defaultCatalog),
    initialDatabaseOpt = Option(defaultDatabase),
    initialSqlConf = graphRegistrationContext.defaultSqlConf
  )

  def processSqlFile(sqlText: String, sqlFilePath: String, spark: SparkSession): Unit = {
    // Create a registration context for this SQL registration request
    val sqlGraphElementRegistrationContext = new SqlGraphRegistrationContext(
      graphRegistrationContext
    )

    splitSqlFileIntoQueries(
      spark = spark,
      sqlFileText = sqlText,
      sqlFilePath = sqlFilePath
    ).foreach { case SqlQueryPlanWithOrigin(logicalPlan, queryOrigin) =>
      sqlGraphElementRegistrationContext.processSqlQuery(logicalPlan, queryOrigin)
    }
  }

  private def processSqlQuery(queryPlan: LogicalPlan, queryOrigin: QueryOrigin): Unit = {
    queryPlan match {
      case setCommand: SetCommand =>
        // SET [ key | 'key' ] [ value | 'value' ]
        // Sets (or overrides if already set) the value for a spark conf key. Once set, this conf
        // is applied for all flow functions registered afterward, until unset/overwritten.
        SetCommandHandler.handle(setCommand)
      case setNamespaceCommand: SetNamespaceCommand =>
        // USE { NAMESPACE | DATABASE | SCHEMA } [ schema_name | 'schema_name' ]
        // Sets the current schema. After the current schema is set, unqualified references to
        // objects such as tables are resolved from said schema, until overwritten, within this
        // SQL processor scope.
        SetNamespaceCommandHandler.handle(setNamespaceCommand)
      case setCatalogCommand: SetCatalogCommand =>
        // USE { CATALOG } [ catalog_name | 'catalog_name' ]
        // Sets the current catalog. After the current catalog is set, unqualified references to
        // objects such as tables are resolved from said catalog, until overwritten, within this
        // SQL processor scope. Note that the schema is cleared when the catalog is set, and must
        // be explicitly set again in order to implicitly qualify identifiers.
        SetCatalogCommandHandler.handle(setCatalogCommand)
      case createPersistedViewCommand: CreateView =>
        // CREATE VIEW [ persisted_view_name ] [ options ] AS [ query ]
        CreatePersistedViewCommandHandler.handle(createPersistedViewCommand, queryOrigin)
      case createTemporaryViewCommand: CreateViewCommand =>
        // CREATE TEMPORARY VIEW [ temporary_view_name ] [ options ] AS [ query ]
        CreateTemporaryViewHandler.handle(createTemporaryViewCommand, queryOrigin)
      case createMaterializedViewAsSelectCommand: CreateMaterializedViewAsSelect =>
        // CREATE MATERIALIZED VIEW [ materialized_view_name ] [ options ] AS [ query ]
        CreateMaterializedViewAsSelectHandler.handle(
          createMaterializedViewAsSelectCommand,
          queryOrigin
        )
      case createStreamingTableAsSelectCommand: CreateStreamingTableAsSelect =>
        // CREATE STREAMING TABLE [ streaming_table_name ] [ options ] AS [ query ]
        CreateStreamingTableAsSelectHandler.handle(createStreamingTableAsSelectCommand, queryOrigin)
      case createStreamingTableCommand: CreateStreamingTable =>
        // CREATE STREAMING TABLE [ streaming_table_name ] [ options ]
        CreateStreamingTableHandler.handle(createStreamingTableCommand, queryOrigin)
      case createFlowCommand: CreateFlowCommand =>
        // CREATE FLOW [ flow_name ] AS INSERT INTO [ destination_name ] BY NAME
        CreateFlowHandler.handle(createFlowCommand, queryOrigin)
      case unsupportedLogicalPlan: LogicalPlan =>
        throw SqlGraphElementRegistrationException(
          msg = s"Unsupported plan ${unsupportedLogicalPlan.nodeName} parsed from SQL query",
          queryOrigin = queryOrigin
        )
    }
  }

  private object CreateStreamingTableHandler {
    def handle(cst: CreateStreamingTable, queryOrigin: QueryOrigin): Unit = {
      val stIdentifier = GraphIdentifierManager
        .parseAndQualifyTableIdentifier(
          rawTableIdentifier = IdentifierHelper.toTableIdentifier(cst.name),
          currentCatalog = context.getCurrentCatalogOpt,
          currentDatabase = context.getCurrentDatabaseOpt
        )
        .identifier

      // Register streaming table as a table.
      graphRegistrationContext.registerTable(
        Table(
          identifier = stIdentifier,
          comment = cst.tableSpec.comment,
          specifiedSchema =
            Option.when(cst.columns.nonEmpty)(StructType(cst.columns.map(_.toV1Column))),
          partitionCols = Option(PartitionHelper.applyPartitioning(cst.partitioning, queryOrigin)),
          properties = cst.tableSpec.properties,
          baseOrigin = queryOrigin.copy(
            objectName = Option(stIdentifier.unquotedString),
            objectType = Option(QueryOriginType.Table.toString)
          ),
          format = cst.tableSpec.provider,
          normalizedPath = None,
          isStreamingTable = true
        )
      )
    }
  }

  private object CreateStreamingTableAsSelectHandler {
    def handle(cst: CreateStreamingTableAsSelect, queryOrigin: QueryOrigin): Unit = {
      val stIdentifier = GraphIdentifierManager
        .parseAndQualifyTableIdentifier(
          rawTableIdentifier = IdentifierHelper.toTableIdentifier(cst.name),
          currentCatalog = context.getCurrentCatalogOpt,
          currentDatabase = context.getCurrentDatabaseOpt
        )
        .identifier

      // Register streaming table as a table.
      graphRegistrationContext.registerTable(
        Table(
          identifier = stIdentifier,
          comment = cst.tableSpec.comment,
          specifiedSchema =
            Option.when(cst.columns.nonEmpty)(StructType(cst.columns.map(_.toV1Column))),
          partitionCols = Option(PartitionHelper.applyPartitioning(cst.partitioning, queryOrigin)),
          properties = cst.tableSpec.properties,
          baseOrigin = queryOrigin.copy(
            objectName = Option(stIdentifier.unquotedString),
            objectType = Option(QueryOriginType.Table.toString)
          ),
          format = cst.tableSpec.provider,
          normalizedPath = None,
          isStreamingTable = true
        )
      )

      // Register flow that backs this streaming table.
      graphRegistrationContext.registerFlow(
        UnresolvedFlow(
          identifier = stIdentifier,
          destinationIdentifier = stIdentifier,
          func = FlowAnalysis.createFlowFunctionFromLogicalPlan(cst.query),
          sqlConf = context.getSqlConf,
          once = false,
          queryContext = QueryContext(
            currentCatalog = context.getCurrentCatalogOpt,
            currentDatabase = context.getCurrentDatabaseOpt
          ),
          origin = queryOrigin.copy(
            objectName = Option(stIdentifier.unquotedString),
            objectType = Option(QueryOriginType.Flow.toString)
          )
        )
      )
    }
  }

  private object CreateMaterializedViewAsSelectHandler {
    def handle(cmv: CreateMaterializedViewAsSelect, queryOrigin: QueryOrigin): Unit = {
      val mvIdentifier = GraphIdentifierManager
        .parseAndQualifyTableIdentifier(
          rawTableIdentifier = IdentifierHelper.toTableIdentifier(cmv.name),
          currentCatalog = context.getCurrentCatalogOpt,
          currentDatabase = context.getCurrentDatabaseOpt
        )
        .identifier

      // Register materialized view as a table.
      graphRegistrationContext.registerTable(
        Table(
          identifier = mvIdentifier,
          comment = cmv.tableSpec.comment,
          specifiedSchema =
            Option.when(cmv.columns.nonEmpty)(StructType(cmv.columns.map(_.toV1Column))),
          partitionCols = Option(PartitionHelper.applyPartitioning(cmv.partitioning, queryOrigin)),
          properties = cmv.tableSpec.properties,
          baseOrigin = queryOrigin.copy(
            objectName = Option(mvIdentifier.unquotedString),
            objectType = Option(QueryOriginType.Table.toString)
          ),
          format = cmv.tableSpec.provider,
          normalizedPath = None,
          isStreamingTable = false
        )
      )

      // Register flow that backs this materialized view.
      graphRegistrationContext.registerFlow(
        UnresolvedFlow(
          identifier = mvIdentifier,
          destinationIdentifier = mvIdentifier,
          func = FlowAnalysis.createFlowFunctionFromLogicalPlan(cmv.query),
          sqlConf = context.getSqlConf,
          once = false,
          queryContext = QueryContext(
            currentCatalog = context.getCurrentCatalogOpt,
            currentDatabase = context.getCurrentDatabaseOpt
          ),
          origin = queryOrigin.copy(
            objectName = Option(mvIdentifier.unquotedString),
            objectType = Option(QueryOriginType.Flow.toString)
          )
        )
      )
    }
  }

  private object CreatePersistedViewCommandHandler {
    def handle(cv: CreateView, queryOrigin: QueryOrigin): Unit = {
      val viewIdentifier = GraphIdentifierManager.parseAndValidatePersistedViewIdentifier(
        rawViewIdentifier = IdentifierHelper.toTableIdentifier(cv.child),
        currentCatalog = context.getCurrentCatalogOpt,
        currentDatabase = context.getCurrentDatabaseOpt
      )

      // Register persisted view definition.
      graphRegistrationContext.registerView(
        PersistedView(
          identifier = viewIdentifier,
          comment = cv.comment,
          origin = queryOrigin.copy(
            objectName = Option(viewIdentifier.unquotedString),
            objectType = Option(QueryOriginType.View.toString)
          ),
          properties = cv.properties
        )
      )

      // Register flow that backs this persisted view.
      graphRegistrationContext.registerFlow(
        UnresolvedFlow(
          identifier = viewIdentifier,
          destinationIdentifier = viewIdentifier,
          func = FlowAnalysis.createFlowFunctionFromLogicalPlan(cv.query),
          sqlConf = context.getSqlConf,
          once = false,
          queryContext = QueryContext(
            currentCatalog = context.getCurrentCatalogOpt,
            currentDatabase = context.getCurrentDatabaseOpt
          ),
          origin = queryOrigin.copy(
            objectName = Option(viewIdentifier.unquotedString),
            objectType = Option(QueryOriginType.Flow.toString)
          )
        )
      )
    }
  }

  private object CreateTemporaryViewHandler {
    def handle(cvc: CreateViewCommand, queryOrigin: QueryOrigin): Unit = {
      // Validate the temporary view is not fully qualified, and then qualify it with the pipeline
      // catalog/database.
      val viewIdentifier = GraphIdentifierManager
        .parseAndValidateTemporaryViewIdentifier(
          rawViewIdentifier = cvc.name
        )

      // Register temporary view definition.
      graphRegistrationContext.registerView(
        TemporaryView(
          identifier = viewIdentifier,
          comment = cvc.comment,
          origin = queryOrigin.copy(
            objectName = Option(viewIdentifier.unquotedString),
            objectType = Option(QueryOriginType.View.toString)
          ),
          properties = Map.empty
        )
      )

      // Register flow definition that backs this temporary view.
      graphRegistrationContext.registerFlow(
        UnresolvedFlow(
          identifier = viewIdentifier,
          destinationIdentifier = viewIdentifier,
          func = FlowAnalysis.createFlowFunctionFromLogicalPlan(cvc.plan),
          sqlConf = context.getSqlConf,
          once = false,
          queryContext = QueryContext(
            currentCatalog = context.getCurrentCatalogOpt,
            currentDatabase = context.getCurrentDatabaseOpt
          ),
          origin = queryOrigin.copy(
            objectName = Option(viewIdentifier.unquotedString),
            objectType = Option(QueryOriginType.Flow.toString)
          )
        )
      )
    }
  }

  private object CreateFlowHandler {
    def handle(cf: CreateFlowCommand, queryOrigin: QueryOrigin): Unit = {
      val rawFlowIdentifier =
        IdentifierHelper.toTableIdentifier(cf.name)
      if (!IdentifierHelper.isSinglePartIdentifier(
        rawFlowIdentifier
      )) {
        throw new AnalysisException(
          "MULTIPART_FLOW_NAME_NOT_SUPPORTED",
          Map("flowName" -> rawFlowIdentifier.unquotedString)
        )
      }

      val flowIdentifier = GraphIdentifierManager
        .parseAndQualifyFlowIdentifier(
          rawFlowIdentifier = rawFlowIdentifier,
          currentCatalog = context.getCurrentCatalogOpt,
          currentDatabase = context.getCurrentDatabaseOpt
        )
        .identifier

      val (flowTargetDatasetIdentifier, flowQueryLogicalPlan, isOnce) = cf.flowOperation match {
        case i: InsertIntoStatement =>
          validateInsertIntoFlow(i, queryOrigin)
          val flowTargetDatasetName = i.table match {
            case u: UnresolvedRelation =>
              IdentifierHelper.toTableIdentifier(u.multipartIdentifier)
            case _ =>
              throw SqlGraphElementRegistrationException(
                msg = "Unable to resolve target dataset name for INSERT INTO flow",
                queryOrigin = queryOrigin
              )
          }
          val qualifiedFlowTargetDatasetName = GraphIdentifierManager
            .parseAndQualifyTableIdentifier(
              rawTableIdentifier = flowTargetDatasetName,
              currentCatalog = context.getCurrentCatalogOpt,
              currentDatabase = context.getCurrentDatabaseOpt
            )
            .identifier
          (qualifiedFlowTargetDatasetName, i.query, false)
        case _ =>
          throw SqlGraphElementRegistrationException(
            msg = "Unable flow type. Only INSERT INTO flows are supported.",
            queryOrigin = queryOrigin
          )
      }

      val qualifiedDestinationIdentifier = GraphIdentifierManager
        .parseAndQualifyFlowIdentifier(
          rawFlowIdentifier = flowTargetDatasetIdentifier,
          currentCatalog = context.getCurrentCatalogOpt,
          currentDatabase = context.getCurrentDatabaseOpt
        )
        .identifier

      graphRegistrationContext.registerFlow(
        UnresolvedFlow(
          identifier = flowIdentifier,
          destinationIdentifier = qualifiedDestinationIdentifier,
          func = FlowAnalysis.createFlowFunctionFromLogicalPlan(flowQueryLogicalPlan),
          sqlConf = context.getSqlConf,
          once = isOnce,
          queryContext = QueryContext(
            currentCatalog = context.getCurrentCatalogOpt,
            currentDatabase = context.getCurrentDatabaseOpt
          ),
          origin = queryOrigin
        )
      )
    }

    private def validateInsertIntoFlow(
        insertIntoStatement: InsertIntoStatement,
        queryOrigin: QueryOrigin
    ): Unit = {
      if (insertIntoStatement.partitionSpec.nonEmpty) {
        throw SqlGraphElementRegistrationException(
          msg = "Partition spec may not be specified for flow target.",
          queryOrigin = queryOrigin
        )
      }
      if (insertIntoStatement.userSpecifiedCols.nonEmpty) {
        throw SqlGraphElementRegistrationException(
          msg = "Column schema may not be specified for flow target.",
          queryOrigin = queryOrigin
        )
      }
      if (insertIntoStatement.overwrite) {
        throw SqlGraphElementRegistrationException(
          msg = "INSERT OVERWRITE flows not supported.",
          queryOrigin = queryOrigin
        )
      }
      if (insertIntoStatement.ifPartitionNotExists) {
        throw SqlGraphElementRegistrationException(
          msg = "IF NOT EXISTS not supported for flows.",
          queryOrigin = queryOrigin
        )
      }
      if (!insertIntoStatement.byName) {
        throw SqlGraphElementRegistrationException(
          msg = "Only INSERT INTO by name flows supported.",
          queryOrigin = queryOrigin
        )
      }
    }
  }

  private object SetCommandHandler {
    def handle(setCommand: SetCommand): Unit = {
      val sqlConfKvPair = setCommand.kv.getOrElse(
        throw new RuntimeException("Invalid SET command without key-value pair")
      )
      val sqlConfKey = sqlConfKvPair._1
      val sqlConfValue = sqlConfKvPair._2.getOrElse(
        throw new RuntimeException("Invalid SET command without value")
      )
      context.setSqlConf(sqlConfKey, sqlConfValue)
    }
  }

  private object SetNamespaceCommandHandler {
    def handle(setNamespaceCommand: SetNamespaceCommand): Unit = {
      setNamespaceCommand.namespace match {
        case Seq(database) =>
          context.setCurrentDatabase(database)
        case Seq(catalog, database) =>
          context.setCurrentCatalog(catalog)
          context.setCurrentDatabase(database)
        case invalidSchemaIdentifier =>
          throw new SparkException(
            "Invalid schema identifier provided on USE command: " +
              s"$invalidSchemaIdentifier"
          )
      }
    }
  }

  private object SetCatalogCommandHandler {
    def handle(setCatalogCommand: SetCatalogCommand): Unit = {
      context.setCurrentCatalog(setCatalogCommand.catalogName)
      context.clearCurrentDatabase()
    }
  }
}

object PartitionHelper {
  import org.apache.spark.sql.connector.expressions.{IdentityTransform, Transform}

  def applyPartitioning(partitioning: Seq[Transform], queryOrigin: QueryOrigin): Seq[String] = {
    partitioning.foreach {
      case _: IdentityTransform =>
      case other =>
        throw SqlGraphElementRegistrationException(
          msg = s"Invalid partitioning transform ($other)",
          queryOrigin = queryOrigin
        )
    }
    partitioning.collect {
      case t: IdentityTransform =>
        if (t.references.length != 1) {
          throw SqlGraphElementRegistrationException(
            msg = "Only single column based partitioning is supported.",
            queryOrigin = queryOrigin
          )
        }
        if (t.ref.fieldNames().length != 1) {
          throw SqlGraphElementRegistrationException(
            msg = "Multipart partition identifier not allowed.",
            queryOrigin = queryOrigin
          )
        }
        t.ref.fieldNames().head
    }
  }
}

object SqlGraphRegistrationContext {
  /**
   * Split SQL statements by semicolon.
   *
   * Note that an input SQL text/blob like:
   * "-- comment 1
   * SELECT 1;
   *
   * SELECT 2 ; -- comment 2"
   *
   * Will be split into the two following strings:
   * "-- comment 1
   * SELECT 1",
   * "
   * SELECT 2 "
   *
   * The semicolon that terminates a statement is not included in the returned string for that
   * statement, any white space/comments surrounding a statement is included in the returned
   * string for that statement, and any white space/comments following the last semicolon
   * terminated statement is not returned.
   */
  private def splitSqlTextBySemicolon(sqlText: String): List[String] = StringUtils
    .splitSemiColonWithIndex(line = sqlText, enableSqlScripting = false)

  /** Class that holds the logical plan and query origin parsed from a SQL statement. */
  case class SqlQueryPlanWithOrigin(plan: LogicalPlan, queryOrigin: QueryOrigin)

  /**
   * Given a SQL file (raw text content and path), return the parsed logical plan and query origin
   * per SQL statement in the file contents.
   *
   * Note that the returned origins will not be complete - origin information like object name and
   * type will only be determined and populate when the logical plan is inspected during SQL
   * element registration.
   *
   * @param spark the spark session to use to parse SQL statements.
   * @param sqlFileText the raw text content of the SQL file.
   * @param sqlFilePath the file path to the SQL file. Only used to populate the query origin.
   * @return a [[SqlQueryPlanWithOrigin]] object per SQL statement, in the same order the SQL
   *         statements were defined in the file contents.
   */
  def splitSqlFileIntoQueries(
      spark: SparkSession,
      sqlFileText: String,
      sqlFilePath: String
  ): Seq[SqlQueryPlanWithOrigin] = {
    // The index in the file we've processed up to at this point
    var currentCharIndexInFile = 0

    val rawSqlStatements = splitSqlTextBySemicolon(sqlFileText)
    rawSqlStatements.map { rawSqlStatement =>
      val rawSqlStatementText = rawSqlStatement
      val logicalPlanFromSqlQuery = spark.sessionState.sqlParser.parsePlan(rawSqlStatementText)

      // Update and return the query origin, accounting for the position of the statement with
      // respect to the entire file.

      // The actual start position of the SQL query in this sqlText string. Within sqlText it's
      // possible that whitespace or comments precede the start of the query, and is accounted for
      // in the parsed logical plan's origin.
      val sqlStatementStartIdxInString = logicalPlanFromSqlQuery.origin.startIndex.getOrElse(
        throw new SparkRuntimeException(
          errorClass = "INTERNAL_ERROR",
          messageParameters = Map(
            "message" ->
              s"""Unable to retrieve start index of logical plan parsed by the following
                 |SQL text:
                 |
                 |$rawSqlStatementText""".stripMargin)
        )
      )

      // The actual start position of the SQL query in the entire file.
      val sqlStatementStartIndexInFile = currentCharIndexInFile + sqlStatementStartIdxInString

      // The line number is the number of new lines characters found prior to the start of this sql
      // statement, plus 1 for 1-indexing. Ex. "SELECT 1;" should be on line 1, not line 0.
      val sqlStatementLineNumber = 1 + sqlFileText.substring(0, sqlStatementStartIndexInFile)
        .count(_ == '\n')

      // Move the current char index/ptr by the length of the raw SQL text we just processed, plus
      // 1 to account for delimiting semicolon.
      currentCharIndexInFile += rawSqlStatementText.length + 1

      // Return the updated query origin with line number and start position.
      SqlQueryPlanWithOrigin(
        plan = logicalPlanFromSqlQuery,
        queryOrigin = QueryOrigin(
          language = Option(Language.Sql()),
          filePath = Option(sqlFilePath),
          // Raw SQL text, after stripping away preceding whitespace
          sqlText = Option(rawSqlStatementText.substring(sqlStatementStartIdxInString)),
          line = Option(sqlStatementLineNumber),
          startPosition = Option(sqlStatementStartIndexInFile)
        )
      )
    }
  }
}
