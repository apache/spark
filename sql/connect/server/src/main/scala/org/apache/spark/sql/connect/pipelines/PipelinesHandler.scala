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

package org.apache.spark.sql.connect.pipelines

import scala.collection.Seq
import scala.jdk.CollectionConverters._
import scala.util.Using

import io.grpc.stub.StreamObserver

import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.{ExecutePlanResponse, PipelineCommandResult, Relation, ResolvedIdentifier}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical.{Command, CreateNamespace, CreateTable, CreateTableAsSelect, CreateView, DescribeRelation, DropView, InsertIntoStatement, LogicalPlan, RenameTable, ShowColumns, ShowCreateTable, ShowFunctions, ShowTableProperties, ShowTables, ShowViews}
import org.apache.spark.sql.connect.common.DataTypeProtoConverter
import org.apache.spark.sql.connect.service.SessionHolder
import org.apache.spark.sql.execution.command.{ShowCatalogsCommand, ShowNamespacesCommand}
import org.apache.spark.sql.pipelines.Language.Python
import org.apache.spark.sql.pipelines.common.RunState.{CANCELED, FAILED}
import org.apache.spark.sql.pipelines.graph.{AllTables, FlowAnalysis, GraphIdentifierManager, GraphRegistrationContext, IdentifierHelper, NoTables, PipelineUpdateContextImpl, QueryContext, QueryOrigin, QueryOriginType, Sink, SinkImpl, SomeTables, SqlGraphRegistrationContext, Table, TableFilter, TemporaryView, UnresolvedFlow}
import org.apache.spark.sql.pipelines.logging.{PipelineEvent, RunProgress}
import org.apache.spark.sql.types.StructType

/** Handler for SparkConnect PipelineCommands */
private[connect] object PipelinesHandler extends Logging {

  /**
   * Handles the pipeline command
   * @param sessionHolder
   *   Context about the session state
   * @param cmd
   *   Command to be handled
   * @param responseObserver
   *   The response observer where the response will be sent
   * @param transformRelationFunc
   *   Function used to convert a relation to a LogicalPlan. This is used when determining the
   *   LogicalPlan that a flow returns.
   * @return
   *   The response after handling the command
   */
  def handlePipelinesCommand(
      sessionHolder: SessionHolder,
      cmd: proto.PipelineCommand,
      responseObserver: StreamObserver[ExecutePlanResponse],
      transformRelationFunc: Relation => LogicalPlan): PipelineCommandResult = {
    // Currently most commands do not include any information in the response. We just send back
    // an empty response to the client to indicate that the command was handled successfully
    val defaultResponse = PipelineCommandResult.getDefaultInstance
    cmd.getCommandTypeCase match {
      case proto.PipelineCommand.CommandTypeCase.CREATE_DATAFLOW_GRAPH =>
        val createdGraphId =
          createDataflowGraph(cmd.getCreateDataflowGraph, sessionHolder)
        PipelineCommandResult
          .newBuilder()
          .setCreateDataflowGraphResult(
            PipelineCommandResult.CreateDataflowGraphResult.newBuilder
              .setDataflowGraphId(createdGraphId)
              .build())
          .build()
      case proto.PipelineCommand.CommandTypeCase.DROP_DATAFLOW_GRAPH =>
        logInfo(s"Drop pipeline cmd received: $cmd")
        sessionHolder.dataflowGraphRegistry
          .dropDataflowGraph(cmd.getDropDataflowGraph.getDataflowGraphId)
        defaultResponse
      case proto.PipelineCommand.CommandTypeCase.DEFINE_OUTPUT =>
        logInfo(s"Define pipelines output cmd received: $cmd")
        val resolvedDataset =
          defineOutput(cmd.getDefineOutput, sessionHolder)
        val identifierBuilder = ResolvedIdentifier.newBuilder()
        resolvedDataset.catalog.foreach(identifierBuilder.setCatalogName)
        resolvedDataset.database.foreach(identifierBuilder.addNamespace)
        identifierBuilder.setTableName(resolvedDataset.identifier)
        val identifier = identifierBuilder.build()
        PipelineCommandResult
          .newBuilder()
          .setDefineOutputResult(
            PipelineCommandResult.DefineOutputResult
              .newBuilder()
              .setResolvedIdentifier(identifier)
              .build())
          .build()
      case proto.PipelineCommand.CommandTypeCase.DEFINE_FLOW =>
        logInfo(s"Define pipelines flow cmd received: $cmd")
        val resolvedFlow =
          defineFlow(cmd.getDefineFlow, transformRelationFunc, sessionHolder)
        val identifierBuilder = ResolvedIdentifier.newBuilder()
        resolvedFlow.catalog.foreach(identifierBuilder.setCatalogName)
        resolvedFlow.database.foreach { ns =>
          identifierBuilder.addNamespace(ns)
        }
        identifierBuilder.setTableName(resolvedFlow.identifier)
        val identifier = identifierBuilder.build()
        PipelineCommandResult
          .newBuilder()
          .setDefineFlowResult(
            PipelineCommandResult.DefineFlowResult
              .newBuilder()
              .setResolvedIdentifier(identifier)
              .build())
          .build()
      case proto.PipelineCommand.CommandTypeCase.START_RUN =>
        logInfo(s"Start pipeline cmd received: $cmd")
        startRun(cmd.getStartRun, responseObserver, sessionHolder)
        defaultResponse
      case proto.PipelineCommand.CommandTypeCase.DEFINE_SQL_GRAPH_ELEMENTS =>
        logInfo(s"Register sql datasets cmd received: $cmd")
        defineSqlGraphElements(cmd.getDefineSqlGraphElements, sessionHolder)
        defaultResponse
      case other => throw new UnsupportedOperationException(s"$other not supported")
    }
  }

  /**
   * Block SQL commands that have side effects or modify data.
   *
   * Pipeline definitions should be declarative and side-effect free. This prevents users from
   * inadvertently modifying catalogs, creating tables, or performing other stateful operations
   * outside the pipeline API boundary during pipeline registration or analysis.
   *
   * This is a best-effort approach: we block known problematic commands while allowing a curated
   * set of read-only operations (e.g., SHOW, DESCRIBE).
   */
  def blockUnsupportedSqlCommand(queryPlan: LogicalPlan): Unit = {
    val allowlistedCommands = Set(
      classOf[DescribeRelation],
      classOf[ShowTables],
      classOf[ShowTableProperties],
      classOf[ShowNamespacesCommand],
      classOf[ShowColumns],
      classOf[ShowFunctions],
      classOf[ShowViews],
      classOf[ShowCatalogsCommand],
      classOf[ShowCreateTable])
    val isSqlCommandExplicitlyAllowlisted = allowlistedCommands.exists(_.isInstance(queryPlan))
    val isUnsupportedSqlPlan = if (isSqlCommandExplicitlyAllowlisted) {
      false
    } else {
      // Disable all [[Command]] except the ones that are explicitly allowlisted
      // in "allowlistedCommands".
      queryPlan.isInstanceOf[Command] ||
      // Following commands are not subclasses of [[Command]] but have side effects.
      queryPlan.isInstanceOf[CreateTableAsSelect] ||
      queryPlan.isInstanceOf[CreateTable] ||
      queryPlan.isInstanceOf[CreateView] ||
      queryPlan.isInstanceOf[InsertIntoStatement] ||
      queryPlan.isInstanceOf[RenameTable] ||
      queryPlan.isInstanceOf[CreateNamespace] ||
      queryPlan.isInstanceOf[DropView]
    }
    if (isUnsupportedSqlPlan) {
      throw new AnalysisException(
        "UNSUPPORTED_PIPELINE_SPARK_SQL_COMMAND",
        Map("command" -> queryPlan.getClass.getSimpleName))
    }
  }

  private def createDataflowGraph(
      cmd: proto.PipelineCommand.CreateDataflowGraph,
      sessionHolder: SessionHolder): String = {
    val defaultCatalog = Option
      .when(cmd.hasDefaultCatalog)(cmd.getDefaultCatalog)
      .getOrElse {
        val currentCatalog = sessionHolder.session.catalog.currentCatalog()
        logInfo(
          "No default catalog was supplied. " +
            s"Falling back to the current catalog: $currentCatalog.")
        currentCatalog
      }

    val defaultDatabase = Option
      .when(cmd.hasDefaultDatabase)(cmd.getDefaultDatabase)
      .getOrElse {
        val currentDatabase = sessionHolder.session.catalog.currentDatabase
        logInfo(
          "No default database was supplied. " +
            s"Falling back to the current database: $currentDatabase.")
        currentDatabase
      }

    val defaultSqlConf = cmd.getSqlConfMap.asScala.toMap

    sessionHolder.session.catalog.setCurrentCatalog(defaultCatalog)
    sessionHolder.session.catalog.setCurrentDatabase(defaultDatabase)

    sessionHolder.dataflowGraphRegistry.createDataflowGraph(
      defaultCatalog = defaultCatalog,
      defaultDatabase = defaultDatabase,
      defaultSqlConf = defaultSqlConf)
  }

  private def defineSqlGraphElements(
      cmd: proto.PipelineCommand.DefineSqlGraphElements,
      sessionHolder: SessionHolder): Unit = {
    val dataflowGraphId = cmd.getDataflowGraphId

    val graphElementRegistry =
      sessionHolder.dataflowGraphRegistry.getDataflowGraphOrThrow(dataflowGraphId)
    val sqlGraphElementRegistrationContext = new SqlGraphRegistrationContext(graphElementRegistry)
    sqlGraphElementRegistrationContext.processSqlFile(
      cmd.getSqlText,
      cmd.getSqlFilePath,
      sessionHolder.session)
  }

  private def defineOutput(
      output: proto.PipelineCommand.DefineOutput,
      sessionHolder: SessionHolder): TableIdentifier = {
    val dataflowGraphId = output.getDataflowGraphId
    val graphElementRegistry =
      sessionHolder.dataflowGraphRegistry.getDataflowGraphOrThrow(dataflowGraphId)

    output.getOutputType match {
      case proto.OutputType.MATERIALIZED_VIEW | proto.OutputType.TABLE =>
        val qualifiedIdentifier = GraphIdentifierManager
          .parseAndQualifyTableIdentifier(
            rawTableIdentifier = GraphIdentifierManager
              .parseTableIdentifier(output.getOutputName, sessionHolder.session),
            currentCatalog = Some(graphElementRegistry.defaultCatalog),
            currentDatabase = Some(graphElementRegistry.defaultDatabase))
          .identifier

        val tableDetails = output.getTableDetails
        graphElementRegistry.registerTable(
          Table(
            identifier = qualifiedIdentifier,
            comment = Option(output.getComment),
            specifiedSchema = tableDetails.getSchemaCase match {
              case proto.PipelineCommand.DefineOutput.TableDetails.SchemaCase.SCHEMA_DATA_TYPE =>
                Some(
                  DataTypeProtoConverter
                    .toCatalystType(tableDetails.getSchemaDataType)
                    .asInstanceOf[StructType])
              case proto.PipelineCommand.DefineOutput.TableDetails.SchemaCase.SCHEMA_STRING =>
                Some(StructType.fromDDL(tableDetails.getSchemaString))
              case proto.PipelineCommand.DefineOutput.TableDetails.SchemaCase.SCHEMA_NOT_SET =>
                None
            },
            partitionCols = Option(tableDetails.getPartitionColsList.asScala.toSeq)
              .filter(_.nonEmpty),
            clusterCols = Option(tableDetails.getClusteringColumnsList.asScala.toSeq)
              .filter(_.nonEmpty),
            properties = tableDetails.getTablePropertiesMap.asScala.toMap,
            origin = QueryOrigin(
              filePath = Option.when(output.getSourceCodeLocation.hasFileName)(
                output.getSourceCodeLocation.getFileName),
              line = Option.when(output.getSourceCodeLocation.hasLineNumber)(
                output.getSourceCodeLocation.getLineNumber),
              objectType = Option(QueryOriginType.Table.toString),
              objectName = Option(qualifiedIdentifier.unquotedString),
              language = Option(Python())),
            format = Option.when(tableDetails.hasFormat)(tableDetails.getFormat),
            normalizedPath = None,
            isStreamingTable = output.getOutputType == proto.OutputType.TABLE))
        qualifiedIdentifier
      case proto.OutputType.TEMPORARY_VIEW =>
        val viewIdentifier = GraphIdentifierManager
          .parseAndValidateTemporaryViewIdentifier(rawViewIdentifier = GraphIdentifierManager
            .parseTableIdentifier(output.getOutputName, sessionHolder.session))
        graphElementRegistry.registerView(
          TemporaryView(
            identifier = viewIdentifier,
            comment = Option(output.getComment),
            origin = QueryOrigin(
              filePath = Option.when(output.getSourceCodeLocation.hasFileName)(
                output.getSourceCodeLocation.getFileName),
              line = Option.when(output.getSourceCodeLocation.hasLineNumber)(
                output.getSourceCodeLocation.getLineNumber),
              objectType = Some(QueryOriginType.View.toString),
              objectName = Option(viewIdentifier.unquotedString),
              language = Some(Python())),
            properties = Map.empty,
            sqlText = None))
        viewIdentifier
      case proto.OutputType.SINK =>
        val identifier = GraphIdentifierManager
          .parseTableIdentifier(output.getOutputName, sessionHolder.session)
        val sinkDetails = output.getSinkDetails
        graphElementRegistry.registerSink(
          SinkImpl(
            identifier = identifier,
            format = sinkDetails.getFormat,
            options = sinkDetails.getOptionsMap.asScala.toMap,
            origin = QueryOrigin(
              filePath = Option.when(output.getSourceCodeLocation.hasFileName)(
                output.getSourceCodeLocation.getFileName),
              line = Option.when(output.getSourceCodeLocation.hasLineNumber)(
                output.getSourceCodeLocation.getLineNumber),
              objectType = Option(QueryOriginType.Sink.toString),
              objectName = Option(identifier.unquotedString),
              language = Some(Python()))))
        identifier
      case _ =>
        throw new IllegalArgumentException(s"Unknown output type: ${output.getOutputType}")
    }
  }

  private def defineFlow(
      flow: proto.PipelineCommand.DefineFlow,
      transformRelationFunc: Relation => LogicalPlan,
      sessionHolder: SessionHolder): TableIdentifier = {
    if (flow.hasOnce) {
      throw new AnalysisException(
        "DEFINE_FLOW_ONCE_OPTION_NOT_SUPPORTED",
        Map("flowName" -> flow.getFlowName))
    }
    val dataflowGraphId = flow.getDataflowGraphId
    val graphElementRegistry =
      sessionHolder.dataflowGraphRegistry.getDataflowGraphOrThrow(dataflowGraphId)
    val defaultCatalog = graphElementRegistry.defaultCatalog
    val defaultDatabase = graphElementRegistry.defaultDatabase

    val isImplicitFlow = flow.getFlowName == flow.getTargetDatasetName
    val rawFlowIdentifier = GraphIdentifierManager
      .parseTableIdentifier(name = flow.getFlowName, spark = sessionHolder.session)

    // If the flow is not an implicit flow (i.e. one defined as part of dataset creation), then
    // it must be a single-part identifier.
    if (!isImplicitFlow && !IdentifierHelper.isSinglePartIdentifier(rawFlowIdentifier)) {
      throw new AnalysisException(
        "MULTIPART_FLOW_NAME_NOT_SUPPORTED",
        Map("flowName" -> flow.getFlowName))
    }

    val rawDestinationIdentifier = GraphIdentifierManager
      .parseTableIdentifier(name = flow.getTargetDatasetName, spark = sessionHolder.session)
    val flowWritesToView =
      graphElementRegistry.getViews
        .filter(_.isInstanceOf[TemporaryView])
        .exists(_.identifier == rawDestinationIdentifier)
    val flowWritesToSink =
      graphElementRegistry.getSinks
        .filter(_.isInstanceOf[Sink])
        .exists(_.identifier == rawDestinationIdentifier)
    // If the flow is created implicitly as part of defining a view or that it writes to a sink,
    // then we do not qualify the flow identifier and the flow destination. This is because
    // views and sinks are not permitted to have multipart
    val isImplicitFlowForTempView = isImplicitFlow && flowWritesToView
    val Seq(flowIdentifier, destinationIdentifier) =
      Seq(rawFlowIdentifier, rawDestinationIdentifier).map { rawIdentifier =>
        if (isImplicitFlowForTempView || flowWritesToSink) {
          rawIdentifier
        } else {
          GraphIdentifierManager
            .parseAndQualifyFlowIdentifier(
              rawFlowIdentifier = rawIdentifier,
              currentCatalog = Some(defaultCatalog),
              currentDatabase = Some(defaultDatabase))
            .identifier
        }
      }

    val relationFlowDetails = flow.getRelationFlowDetails
    graphElementRegistry.registerFlow(
      UnresolvedFlow(
        identifier = flowIdentifier,
        destinationIdentifier = destinationIdentifier,
        func = FlowAnalysis.createFlowFunctionFromLogicalPlan(
          transformRelationFunc(relationFlowDetails.getRelation)),
        sqlConf = flow.getSqlConfMap.asScala.toMap,
        once = false,
        queryContext = QueryContext(Option(defaultCatalog), Option(defaultDatabase)),
        origin = QueryOrigin(
          filePath = Option.when(flow.getSourceCodeLocation.hasFileName)(
            flow.getSourceCodeLocation.getFileName),
          line = Option.when(flow.getSourceCodeLocation.hasLineNumber)(
            flow.getSourceCodeLocation.getLineNumber),
          objectType = Some(QueryOriginType.Flow.toString),
          objectName = Option(flowIdentifier.unquotedString),
          language = Some(Python()))))
    flowIdentifier
  }

  private def startRun(
      cmd: proto.PipelineCommand.StartRun,
      responseObserver: StreamObserver[ExecutePlanResponse],
      sessionHolder: SessionHolder): Unit = {
    val dataflowGraphId = cmd.getDataflowGraphId
    val graphElementRegistry =
      sessionHolder.dataflowGraphRegistry.getDataflowGraphOrThrow(dataflowGraphId)
    val tableFiltersResult = createTableFilters(cmd, graphElementRegistry, sessionHolder)

    // Use the PipelineEventSender to send events back to the client asynchronously.
    Using.resource(new PipelineEventSender(responseObserver, sessionHolder)) { eventSender =>
      // We will use this variable to store the run failure event if it occurs. This will be set
      // by the event callback.
      @volatile var runFailureEvent = Option.empty[PipelineEvent]
      // Define a callback which will stream logs back to the SparkConnect client when an internal
      // pipeline event is emitted during pipeline execution. We choose to pass a callback rather
      // the responseObserver to the pipelines execution code so that the pipelines module does not
      // need to take a dependency on SparkConnect.
      val eventCallback = { event: PipelineEvent =>
        event.details match {
          // Failed runs are recorded in the event log. We do not pass these to the SparkConnect
          // client since the failed run will already result in an unhandled exception that is
          // propagated to the SparkConnect client. This special handling ensures that the client
          // does not see the same error twice for a failed run.
          case RunProgress(state) if state == FAILED => runFailureEvent = Some(event)
          case RunProgress(state) if state == CANCELED =>
            throw new RuntimeException("Pipeline run was canceled.")
          case _ =>
            eventSender.sendEvent(event)
        }
      }

      if (cmd.getStorage.isEmpty) {
        // server-side validation to ensure that storage is always specified
        throw new IllegalArgumentException("Storage must be specified to start a run.")
      }

      val pipelineUpdateContext = new PipelineUpdateContextImpl(
        graphElementRegistry.toDataflowGraph,
        eventCallback,
        tableFiltersResult.refresh,
        tableFiltersResult.fullRefresh,
        cmd.getStorage)
      sessionHolder.cachePipelineExecution(dataflowGraphId, pipelineUpdateContext)

      if (cmd.getDry) {
        pipelineUpdateContext.pipelineExecution.dryRunPipeline()
      } else {
        pipelineUpdateContext.pipelineExecution.runPipeline()
      }

      // Rethrow any exceptions that caused the pipeline run to fail so that the exception is
      // propagated back to the SC client / CLI.
      runFailureEvent.foreach { event =>
        throw event.error.get
      }
    }
  }

  /**
   * Creates the table filters for the full refresh and refresh operations based on the StartRun
   * command user provided. Also validates the command parameters to ensure that they are
   * consistent and do not conflict with each other.
   *
   * If `fullRefreshAll` is true, create `AllTables` filter for full refresh.
   *
   * If `fullRefreshTables` and `refreshTables` are both empty, create `AllTables` filter for
   * refresh as a default behavior.
   *
   * If both non-empty, verifies that there is no overlap and creates SomeTables filters for both.
   *
   * If one non-empty and the other empty, create `SomeTables` filter for the non-empty one, and
   * `NoTables` filter for the empty one.
   */
  private def createTableFilters(
      startRunCommand: proto.PipelineCommand.StartRun,
      graphElementRegistry: GraphRegistrationContext,
      sessionHolder: SessionHolder): TableFilters = {
    // Convert table names to fully qualified TableIdentifier objects
    def parseTableNames(tableNames: Seq[String]): Set[TableIdentifier] = {
      tableNames.map { name =>
        GraphIdentifierManager
          .parseAndQualifyTableIdentifier(
            rawTableIdentifier =
              GraphIdentifierManager.parseTableIdentifier(name, sessionHolder.session),
            currentCatalog = Some(graphElementRegistry.defaultCatalog),
            currentDatabase = Some(graphElementRegistry.defaultDatabase))
          .identifier
      }.toSet
    }

    val fullRefreshTables = startRunCommand.getFullRefreshSelectionList.asScala.toSeq
    val fullRefreshAll = startRunCommand.getFullRefreshAll
    val refreshTables = startRunCommand.getRefreshSelectionList.asScala.toSeq

    if (refreshTables.nonEmpty && fullRefreshAll) {
      throw new IllegalArgumentException(
        "Cannot specify a subset to refresh when full refresh all is set to true.")
    }

    if (fullRefreshTables.nonEmpty && fullRefreshAll) {
      throw new IllegalArgumentException(
        "Cannot specify a subset to full refresh when full refresh all is set to true.")
    }
    val refreshTableNames = parseTableNames(refreshTables)
    val fullRefreshTableNames = parseTableNames(fullRefreshTables)

    if (refreshTables.nonEmpty && fullRefreshTables.nonEmpty) {
      // check if there is an intersection between the subset
      val intersection = refreshTableNames.intersect(fullRefreshTableNames)
      if (intersection.nonEmpty) {
        throw new IllegalArgumentException(
          "Datasets specified for refresh and full refresh cannot overlap: " +
            s"${intersection.mkString(", ")}")
      }
    }

    if (fullRefreshAll) {
      return TableFilters(fullRefresh = AllTables, refresh = NoTables)
    }

    (fullRefreshTables, refreshTables) match {
      case (Nil, Nil) =>
        // If both are empty, we default to refreshing all tables
        TableFilters(fullRefresh = NoTables, refresh = AllTables)
      case (_, Nil) =>
        TableFilters(fullRefresh = SomeTables(fullRefreshTableNames), refresh = NoTables)
      case (Nil, _) =>
        TableFilters(fullRefresh = NoTables, refresh = SomeTables(refreshTableNames))
      case (_, _) =>
        // If both are specified, we create filters for both after validation
        TableFilters(
          fullRefresh = SomeTables(fullRefreshTableNames),
          refresh = SomeTables(refreshTableNames))
    }
  }

  /**
   * A case class to hold the table filters for full refresh and refresh operations.
   */
  private case class TableFilters(fullRefresh: TableFilter, refresh: TableFilter)
}
