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

import scala.jdk.CollectionConverters._

import com.google.protobuf.{Timestamp => ProtoTimestamp}
import io.grpc.stub.StreamObserver

import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.{ExecutePlanResponse, PipelineCommandResult, Relation}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.connect.common.DataTypeProtoConverter
import org.apache.spark.sql.connect.service.SessionHolder
import org.apache.spark.sql.pipelines.Language.Python
import org.apache.spark.sql.pipelines.QueryOriginType
import org.apache.spark.sql.pipelines.common.RunState.{CANCELED, FAILED}
import org.apache.spark.sql.pipelines.graph.{AllTables, FlowAnalysis, GraphIdentifierManager, GraphRegistrationContext, IdentifierHelper, NoTables, PipelineUpdateContextImpl, QueryContext, QueryOrigin, SomeTables, SqlGraphRegistrationContext, Table, TableFilter, TemporaryView, UnresolvedFlow}
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
   * @param sparkSession
   *   The spark session
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
          createDataflowGraph(cmd.getCreateDataflowGraph, sessionHolder.session)
        PipelineCommandResult
          .newBuilder()
          .setCreateDataflowGraphResult(
            PipelineCommandResult.CreateDataflowGraphResult.newBuilder
              .setDataflowGraphId(createdGraphId)
              .build())
          .build()
      case proto.PipelineCommand.CommandTypeCase.DROP_DATAFLOW_GRAPH =>
        logInfo(s"Drop pipeline cmd received: $cmd")
        DataflowGraphRegistry.dropDataflowGraph(cmd.getDropDataflowGraph.getDataflowGraphId)
        defaultResponse
      case proto.PipelineCommand.CommandTypeCase.DEFINE_DATASET =>
        logInfo(s"Define pipelines dataset cmd received: $cmd")
        defineDataset(cmd.getDefineDataset, sessionHolder.session)
        defaultResponse
      case proto.PipelineCommand.CommandTypeCase.DEFINE_FLOW =>
        logInfo(s"Define pipelines flow cmd received: $cmd")
        defineFlow(cmd.getDefineFlow, transformRelationFunc, sessionHolder.session)
        defaultResponse
      case proto.PipelineCommand.CommandTypeCase.START_RUN =>
        logInfo(s"Start pipeline cmd received: $cmd")
        startRun(cmd.getStartRun, responseObserver, sessionHolder)
        defaultResponse
      case proto.PipelineCommand.CommandTypeCase.DEFINE_SQL_GRAPH_ELEMENTS =>
        logInfo(s"Register sql datasets cmd received: $cmd")
        defineSqlGraphElements(cmd.getDefineSqlGraphElements, sessionHolder.session)
        defaultResponse
      case other => throw new UnsupportedOperationException(s"$other not supported")
    }
  }

  private def createDataflowGraph(
      cmd: proto.PipelineCommand.CreateDataflowGraph,
      spark: SparkSession): String = {
    val defaultCatalog = Option
      .when(cmd.hasDefaultCatalog)(cmd.getDefaultCatalog)
      .getOrElse {
        logInfo(s"No default catalog was supplied. Falling back to the current catalog.")
        spark.catalog.currentCatalog()
      }

    val defaultDatabase = Option
      .when(cmd.hasDefaultDatabase)(cmd.getDefaultDatabase)
      .getOrElse {
        logInfo(s"No default database was supplied. Falling back to the current database.")
        spark.catalog.currentDatabase
      }

    val defaultSqlConf = cmd.getSqlConfMap.asScala.toMap

    DataflowGraphRegistry.createDataflowGraph(
      defaultCatalog = defaultCatalog,
      defaultDatabase = defaultDatabase,
      defaultSqlConf = defaultSqlConf)
  }

  private def defineSqlGraphElements(
      cmd: proto.PipelineCommand.DefineSqlGraphElements,
      session: SparkSession): Unit = {
    val dataflowGraphId = cmd.getDataflowGraphId

    val graphElementRegistry = DataflowGraphRegistry.getDataflowGraphOrThrow(dataflowGraphId)
    val sqlGraphElementRegistrationContext = new SqlGraphRegistrationContext(graphElementRegistry)
    sqlGraphElementRegistrationContext.processSqlFile(cmd.getSqlText, cmd.getSqlFilePath, session)
  }

  private def defineDataset(
      dataset: proto.PipelineCommand.DefineDataset,
      sparkSession: SparkSession): Unit = {
    val dataflowGraphId = dataset.getDataflowGraphId
    val graphElementRegistry = DataflowGraphRegistry.getDataflowGraphOrThrow(dataflowGraphId)

    dataset.getDatasetType match {
      case proto.DatasetType.MATERIALIZED_VIEW | proto.DatasetType.TABLE =>
        val tableIdentifier =
          GraphIdentifierManager.parseTableIdentifier(dataset.getDatasetName, sparkSession)
        graphElementRegistry.registerTable(
          Table(
            identifier = tableIdentifier,
            comment = Option(dataset.getComment),
            specifiedSchema = Option.when(dataset.hasSchema)(
              DataTypeProtoConverter
                .toCatalystType(dataset.getSchema)
                .asInstanceOf[StructType]),
            partitionCols = Option(dataset.getPartitionColsList.asScala.toSeq)
              .filter(_.nonEmpty),
            properties = dataset.getTablePropertiesMap.asScala.toMap,
            baseOrigin = QueryOrigin(
              objectType = Option(QueryOriginType.Table.toString),
              objectName = Option(tableIdentifier.unquotedString),
              language = Option(Python())),
            format = Option.when(dataset.hasFormat)(dataset.getFormat),
            normalizedPath = None,
            isStreamingTable = dataset.getDatasetType == proto.DatasetType.TABLE))
      case proto.DatasetType.TEMPORARY_VIEW =>
        val viewIdentifier =
          GraphIdentifierManager.parseTableIdentifier(dataset.getDatasetName, sparkSession)

        graphElementRegistry.registerView(
          TemporaryView(
            identifier = viewIdentifier,
            comment = Option(dataset.getComment),
            origin = QueryOrigin(
              objectType = Option(QueryOriginType.View.toString),
              objectName = Option(viewIdentifier.unquotedString),
              language = Option(Python())),
            properties = Map.empty))
      case _ =>
        throw new IllegalArgumentException(s"Unknown dataset type: ${dataset.getDatasetType}")
    }
  }

  private def defineFlow(
      flow: proto.PipelineCommand.DefineFlow,
      transformRelationFunc: Relation => LogicalPlan,
      sparkSession: SparkSession): Unit = {
    val dataflowGraphId = flow.getDataflowGraphId
    val graphElementRegistry = DataflowGraphRegistry.getDataflowGraphOrThrow(dataflowGraphId)

    val isImplicitFlow = flow.getFlowName == flow.getTargetDatasetName

    val flowIdentifier = GraphIdentifierManager
      .parseTableIdentifier(name = flow.getFlowName, spark = sparkSession)

    // If the flow is not an implicit flow (i.e. one defined as part of dataset creation), then
    // it must be a single-part identifier.
    if (!isImplicitFlow && !IdentifierHelper.isSinglePartIdentifier(flowIdentifier)) {
      throw new AnalysisException(
        "MULTIPART_FLOW_NAME_NOT_SUPPORTED",
        Map("flowName" -> flow.getFlowName))
    }

    graphElementRegistry.registerFlow(
      new UnresolvedFlow(
        identifier = flowIdentifier,
        destinationIdentifier = GraphIdentifierManager
          .parseTableIdentifier(name = flow.getTargetDatasetName, spark = sparkSession),
        func =
          FlowAnalysis.createFlowFunctionFromLogicalPlan(transformRelationFunc(flow.getRelation)),
        sqlConf = flow.getSqlConfMap.asScala.toMap,
        once = flow.getOnce,
        queryContext = QueryContext(
          Option(graphElementRegistry.defaultCatalog),
          Option(graphElementRegistry.defaultDatabase)),
        origin = QueryOrigin(
          objectType = Option(QueryOriginType.Flow.toString),
          objectName = Option(flowIdentifier.unquotedString),
          language = Option(Python()))))
  }

  private def startRun(
      cmd: proto.PipelineCommand.StartRun,
      responseObserver: StreamObserver[ExecutePlanResponse],
      sessionHolder: SessionHolder): Unit = {
    val dataflowGraphId = cmd.getDataflowGraphId
    val graphElementRegistry = DataflowGraphRegistry.getDataflowGraphOrThrow(dataflowGraphId)
    val tableFiltersResult = createTableFilters(cmd, graphElementRegistry, sessionHolder)

    // We will use this variable to store the run failure event if it occurs. This will be set
    // by the event callback.
    @volatile var runFailureEvent = Option.empty[PipelineEvent]
    // Define a callback which will stream logs back to the SparkConnect client when an internal
    // pipeline event is emitted during pipeline execution. We choose to pass a callback rather the
    // responseObserver to the pipelines execution code so that the pipelines module does not need
    // to take a dependency on SparkConnect.
    val eventCallback = { event: PipelineEvent =>
      val message = if (event.error.nonEmpty) {
        // Returns the message associated with a Throwable and all its causes
        def getExceptionMessages(throwable: Throwable): Seq[String] = {
          throwable.getMessage +:
            Option(throwable.getCause).map(getExceptionMessages).getOrElse(Nil)
        }
        val errorMessages = getExceptionMessages(event.error.get)
        s"""${event.message}
           |Error: ${errorMessages.mkString("\n")}""".stripMargin
      } else {
        event.message
      }
      event.details match {
        // Failed runs are recorded in the event log. We do not pass these to the SparkConnect
        // client since the failed run will already result in an unhandled exception that is
        // propagated to the SparkConnect client. This special handling ensures that the client
        // does not see the same error twice for a failed run.
        case RunProgress(state) if state == FAILED => runFailureEvent = Some(event)
        case RunProgress(state) if state == CANCELED =>
          throw new RuntimeException("Pipeline run was canceled.")
        case _ =>
          responseObserver.onNext(
            proto.ExecutePlanResponse
              .newBuilder()
              .setSessionId(sessionHolder.sessionId)
              .setServerSideSessionId(sessionHolder.serverSessionId)
              .setPipelineEventResult(
                proto.PipelineEventResult.newBuilder
                  .setEvent(
                    proto.PipelineEvent
                      .newBuilder()
                      .setTimestamp(
                        ProtoTimestamp
                          .newBuilder()
                          // java.sql.Timestamp normalizes its internal fields: getTime() returns
                          // the full timestamp in milliseconds, while getNanos() returns the
                          // fractional seconds (0-999,999,999 ns). This ensures no precision is
                          // lost or double-counted.
                          .setSeconds(event.timestamp.getTime / 1000)
                          .setNanos(event.timestamp.getNanos)
                          .build())
                      .setMessage(message)
                      .build())
                  .build())
              .build())
      }
    }
    val pipelineUpdateContext = new PipelineUpdateContextImpl(
      graphElementRegistry.toDataflowGraph,
      eventCallback,
      tableFiltersResult.refresh,
      tableFiltersResult.fullRefresh)
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
