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

package org.apache.spark.sql.connect.planner

import scala.collection.JavaConverters._

import com.google.protobuf.{Any => ProtoAny, ByteString}
import org.apache.commons.lang3.exception.ExceptionUtils

import org.apache.spark.SparkEnv
import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.{ExecutePlanResponse, SqlCommand, StreamingQueryCommand, StreamingQueryCommandResult, StreamingQueryInstanceId, StreamingQueryManagerCommand, StreamingQueryManagerCommandResult, WriteStreamOperationStart, WriteStreamOperationStartResult}
import org.apache.spark.connect.proto.ExecutePlanResponse.SqlCommandResult
import org.apache.spark.connect.proto.StreamingQueryManagerCommandResult.StreamingQueryInstance
import org.apache.spark.connect.proto.WriteStreamOperationStart.TriggerCase
import org.apache.spark.sql.{Column, Dataset, ForeachWriter, SparkSession}
import org.apache.spark.sql.catalyst.analysis.{GlobalTempView, LocalTempView}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.{CommandResult, LocalRelation}
import org.apache.spark.sql.connect.common.InvalidPlanInput
import org.apache.spark.sql.connect.config.Connect.CONNECT_GRPC_ARROW_MAX_BATCH_SIZE
import org.apache.spark.sql.connect.plugin.SparkConnectPluginRegistry
import org.apache.spark.sql.connect.service.{SessionHolder, SparkConnectService, SparkConnectStreamHandler}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.arrow.ArrowConverters
import org.apache.spark.sql.execution.command.CreateViewCommand
import org.apache.spark.sql.execution.python.{PythonForeachWriter, UserDefinedPythonFunction}
import org.apache.spark.sql.execution.streaming.StreamingQueryWrapper
import org.apache.spark.sql.streaming.{StreamingQuery, StreamingQueryProgress, Trigger}

class SparkConnectHandler(session: SparkSession, val streamHandler: SparkConnectStreamHandler)
    extends SparkConnectPlanner(session) {

  def process(command: proto.Command, userId: String, sessionId: String): Unit = {
    command.getCommandTypeCase match {
      case proto.Command.CommandTypeCase.REGISTER_FUNCTION =>
        handleRegisterUserDefinedFunction(command.getRegisterFunction)
      case proto.Command.CommandTypeCase.WRITE_OPERATION =>
        handleWriteOperation(command.getWriteOperation)
      case proto.Command.CommandTypeCase.CREATE_DATAFRAME_VIEW =>
        handleCreateViewCommand(command.getCreateDataframeView)
      case proto.Command.CommandTypeCase.WRITE_OPERATION_V2 =>
        handleWriteOperationV2(command.getWriteOperationV2)
      case proto.Command.CommandTypeCase.EXTENSION =>
        handleCommandPlugin(command.getExtension)
      case proto.Command.CommandTypeCase.SQL_COMMAND =>
        handleSqlCommand(command.getSqlCommand, sessionId)
      case proto.Command.CommandTypeCase.WRITE_STREAM_OPERATION_START =>
        handleWriteStreamOperationStart(command.getWriteStreamOperationStart, userId, sessionId)
      case proto.Command.CommandTypeCase.STREAMING_QUERY_COMMAND =>
        handleStreamingQueryCommand(command.getStreamingQueryCommand, sessionId)
      case proto.Command.CommandTypeCase.STREAMING_QUERY_MANAGER_COMMAND =>
        handleStreamingQueryManagerCommand(command.getStreamingQueryManagerCommand, sessionId)
      case proto.Command.CommandTypeCase.GET_RESOURCES_COMMAND =>
        handleGetResourcesCommand(sessionId)
      case _ => throw new UnsupportedOperationException(s"$command not supported.")
    }
  }

  private def handleRegisterUserDefinedFunction(
      fun: proto.CommonInlineUserDefinedFunction): Unit = {
    fun.getFunctionCase match {
      case proto.CommonInlineUserDefinedFunction.FunctionCase.PYTHON_UDF =>
        handleRegisterPythonUDF(fun)
      case proto.CommonInlineUserDefinedFunction.FunctionCase.JAVA_UDF =>
        handleRegisterJavaUDF(fun)
      case _ =>
        throw InvalidPlanInput(
          s"Function with ID: ${fun.getFunctionCase.getNumber} is not supported")
    }
  }

  private def handleRegisterPythonUDF(fun: proto.CommonInlineUserDefinedFunction): Unit = {
    val udf = fun.getPythonUdf
    val function = transformPythonFunction(udf)
    val udpf = UserDefinedPythonFunction(
      name = fun.getFunctionName,
      func = function,
      dataType = transformDataType(udf.getOutputType),
      pythonEvalType = udf.getEvalType,
      udfDeterministic = fun.getDeterministic)

    session.udf.registerPython(fun.getFunctionName, udpf)
  }

  private def handleRegisterJavaUDF(fun: proto.CommonInlineUserDefinedFunction): Unit = {
    val udf = fun.getJavaUdf
    val dataType = if (udf.hasOutputType) {
      transformDataType(udf.getOutputType)
    } else {
      null
    }
    if (udf.getAggregate) {
      session.udf.registerJavaUDAF(fun.getFunctionName, udf.getClassName)
    } else {
      session.udf.registerJava(fun.getFunctionName, udf.getClassName, dataType)
    }
  }

  /**
   * Transforms the write operation and executes it.
   *
   * The input write operation contains a reference to the input plan and transforms it to the
   * corresponding logical plan. Afterwards, creates the DataFrameWriter and translates the
   * parameters of the WriteOperation into the corresponding methods calls.
   *
   * @param writeOperation
   */
  private def handleWriteOperation(writeOperation: proto.WriteOperation): Unit = {
    // Transform the input plan into the logical plan.
    val plan = transformRelation(writeOperation.getInput)
    // And create a Dataset from the plan.
    val dataset = Dataset.ofRows(session, logicalPlan = plan)

    val w = dataset.write
    if (writeOperation.getMode != proto.WriteOperation.SaveMode.SAVE_MODE_UNSPECIFIED) {
      w.mode(SaveModeConverter.toSaveMode(writeOperation.getMode))
    }

    if (writeOperation.getOptionsCount > 0) {
      writeOperation.getOptionsMap.asScala.foreach { case (key, value) => w.option(key, value) }
    }

    if (writeOperation.getSortColumnNamesCount > 0) {
      val names = writeOperation.getSortColumnNamesList.asScala
      w.sortBy(names.head, names.tail.toSeq: _*)
    }

    if (writeOperation.hasBucketBy) {
      val op = writeOperation.getBucketBy
      val cols = op.getBucketColumnNamesList.asScala
      if (op.getNumBuckets <= 0) {
        throw InvalidCommandInput(
          s"BucketBy must specify a bucket count > 0, received ${op.getNumBuckets} instead.")
      }
      w.bucketBy(op.getNumBuckets, cols.head, cols.tail.toSeq: _*)
    }

    if (writeOperation.getPartitioningColumnsCount > 0) {
      val names = writeOperation.getPartitioningColumnsList.asScala
      w.partitionBy(names.toSeq: _*)
    }

    if (writeOperation.hasSource) {
      w.format(writeOperation.getSource)
    }

    writeOperation.getSaveTypeCase match {
      case proto.WriteOperation.SaveTypeCase.SAVETYPE_NOT_SET => w.save()
      case proto.WriteOperation.SaveTypeCase.PATH => w.save(writeOperation.getPath)
      case proto.WriteOperation.SaveTypeCase.TABLE =>
        val tableName = writeOperation.getTable.getTableName
        writeOperation.getTable.getSaveMethod match {
          case proto.WriteOperation.SaveTable.TableSaveMethod.TABLE_SAVE_METHOD_SAVE_AS_TABLE =>
            w.saveAsTable(tableName)
          case proto.WriteOperation.SaveTable.TableSaveMethod.TABLE_SAVE_METHOD_INSERT_INTO =>
            w.insertInto(tableName)
          case _ =>
            throw new UnsupportedOperationException(
              "WriteOperation:SaveTable:TableSaveMethod not supported "
                + s"${writeOperation.getTable.getSaveMethodValue}")
        }
      case _ =>
        throw new UnsupportedOperationException(
          "WriteOperation:SaveTypeCase not supported "
            + s"${writeOperation.getSaveTypeCase.getNumber}")
    }
  }

  private def handleCreateViewCommand(createView: proto.CreateDataFrameViewCommand): Unit = {
    val viewType = if (createView.getIsGlobal) GlobalTempView else LocalTempView

    val tableIdentifier =
      try {
        session.sessionState.sqlParser.parseTableIdentifier(createView.getName)
      } catch {
        case _: ParseException =>
          throw QueryCompilationErrors.invalidViewNameError(createView.getName)
      }

    val plan = CreateViewCommand(
      name = tableIdentifier,
      userSpecifiedColumns = Nil,
      comment = None,
      properties = Map.empty,
      originalText = None,
      plan = transformRelation(createView.getInput),
      allowExisting = false,
      replace = createView.getReplace,
      viewType = viewType)

    Dataset.ofRows(session, plan).queryExecution.commandExecuted
  }

  /**
   * Transforms the write operation and executes it.
   *
   * The input write operation contains a reference to the input plan and transforms it to the
   * corresponding logical plan. Afterwards, creates the DataFrameWriter and translates the
   * parameters of the WriteOperation into the corresponding methods calls.
   *
   * @param writeOperation
   */
  private def handleWriteOperationV2(writeOperation: proto.WriteOperationV2): Unit = {
    // Transform the input plan into the logical plan.
    val plan = transformRelation(writeOperation.getInput)
    // And create a Dataset from the plan.
    val dataset = Dataset.ofRows(session, logicalPlan = plan)

    val w = dataset.writeTo(table = writeOperation.getTableName)

    if (writeOperation.getOptionsCount > 0) {
      writeOperation.getOptionsMap.asScala.foreach { case (key, value) => w.option(key, value) }
    }

    if (writeOperation.getTablePropertiesCount > 0) {
      writeOperation.getTablePropertiesMap.asScala.foreach { case (key, value) =>
        w.tableProperty(key, value)
      }
    }

    if (writeOperation.getPartitioningColumnsCount > 0) {
      val names = writeOperation.getPartitioningColumnsList.asScala
        .map(transformExpression)
        .map(Column(_))
        .toSeq
      w.partitionedBy(names.head, names.tail: _*)
    }

    writeOperation.getMode match {
      case proto.WriteOperationV2.Mode.MODE_CREATE =>
        if (writeOperation.hasProvider) {
          w.using(writeOperation.getProvider).create()
        } else {
          w.create()
        }
      case proto.WriteOperationV2.Mode.MODE_OVERWRITE =>
        w.overwrite(Column(transformExpression(writeOperation.getOverwriteCondition)))
      case proto.WriteOperationV2.Mode.MODE_OVERWRITE_PARTITIONS =>
        w.overwritePartitions()
      case proto.WriteOperationV2.Mode.MODE_APPEND =>
        w.append()
      case proto.WriteOperationV2.Mode.MODE_REPLACE =>
        if (writeOperation.hasProvider) {
          w.using(writeOperation.getProvider).replace()
        } else {
          w.replace()
        }
      case proto.WriteOperationV2.Mode.MODE_CREATE_OR_REPLACE =>
        if (writeOperation.hasProvider) {
          w.using(writeOperation.getProvider).createOrReplace()
        } else {
          w.createOrReplace()
        }
      case _ =>
        throw new UnsupportedOperationException(
          s"WriteOperationV2:ModeValue not supported ${writeOperation.getModeValue}")
    }
  }

  private def handleCommandPlugin(extension: ProtoAny): Unit = {
    SparkConnectPluginRegistry.commandRegistry
      // Lazily traverse the collection.
      .view
      // Apply the transformation.
      .map(p => p.process(extension, this))
      // Find the first non-empty transformation or throw.
      .find(_.nonEmpty)
      .flatten
      .getOrElse(throw InvalidPlanInput("No handler found for extension"))
  }

  private def handleSqlCommand(getSqlCommand: SqlCommand, sessionId: String): Unit = {
    // Eagerly execute commands of the provided SQL string.
    val df = session.sql(
      getSqlCommand.getSql,
      getSqlCommand.getArgsMap.asScala.mapValues(transformLiteral).toMap)
    // Check if commands have been executed.
    val isCommand = df.queryExecution.commandExecuted.isInstanceOf[CommandResult]
    val rows = df.logicalPlan match {
      case lr: LocalRelation => lr.data
      case cr: CommandResult => cr.rows
      case _ => Seq.empty
    }

    // Convert the results to Arrow.
    val schema = df.schema
    val maxRecordsPerBatch = session.sessionState.conf.arrowMaxRecordsPerBatch
    val maxBatchSize = (SparkEnv.get.conf.get(CONNECT_GRPC_ARROW_MAX_BATCH_SIZE) * 0.7).toLong
    val timeZoneId = session.sessionState.conf.sessionLocalTimeZone

    // Convert the data.
    val bytes = if (rows.isEmpty) {
      ArrowConverters.createEmptyArrowBatch(
        schema,
        timeZoneId,
        errorOnDuplicatedFieldNames = false)
    } else {
      val batches = ArrowConverters.toBatchWithSchemaIterator(
        rows.iterator,
        schema,
        maxRecordsPerBatch,
        maxBatchSize,
        timeZoneId,
        errorOnDuplicatedFieldNames = false)
      assert(batches.hasNext)
      val bytes = batches.next()
      assert(!batches.hasNext, s"remaining batches: ${batches.size}")
      bytes
    }

    // To avoid explicit handling of the result on the client, we build the expected input
    // of the relation on the server. The client has to simply forward the result.
    val result = SqlCommandResult.newBuilder()
    if (isCommand) {
      result.setRelation(
        proto.Relation
          .newBuilder()
          .setLocalRelation(
            proto.LocalRelation
              .newBuilder()
              .setData(ByteString.copyFrom(bytes))))
    } else {
      result.setRelation(
        proto.Relation
          .newBuilder()
          .setSql(
            proto.SQL
              .newBuilder()
              .setQuery(getSqlCommand.getSql)
              .putAllArgs(getSqlCommand.getArgsMap)))
    }
    // Exactly one SQL Command Result Batch
    streamHandler.sendResponse(
      ExecutePlanResponse
        .newBuilder()
        .setSessionId(sessionId)
        .setSqlCommandResult(result)
        .build())

    // Send Metrics
    streamHandler.sendResponse(SparkConnectStreamHandler.createMetricsResponse(sessionId, df))
  }

  private def handleWriteStreamOperationStart(
      writeOp: WriteStreamOperationStart,
      userId: String,
      sessionId: String): Unit = {
    val plan = transformRelation(writeOp.getInput)
    val dataset = Dataset.ofRows(session, logicalPlan = plan)

    val writer = dataset.writeStream

    if (writeOp.getFormat.nonEmpty) {
      writer.format(writeOp.getFormat)
    }

    writer.options(writeOp.getOptionsMap)

    if (writeOp.getPartitioningColumnNamesCount > 0) {
      writer.partitionBy(writeOp.getPartitioningColumnNamesList.asScala.toList: _*)
    }

    writeOp.getTriggerCase match {
      case TriggerCase.PROCESSING_TIME_INTERVAL =>
        writer.trigger(Trigger.ProcessingTime(writeOp.getProcessingTimeInterval))
      case TriggerCase.AVAILABLE_NOW =>
        writer.trigger(Trigger.AvailableNow())
      case TriggerCase.ONCE =>
        writer.trigger(Trigger.Once())
      case TriggerCase.CONTINUOUS_CHECKPOINT_INTERVAL =>
        writer.trigger(Trigger.Continuous(writeOp.getContinuousCheckpointInterval))
      case TriggerCase.TRIGGER_NOT_SET =>
    }

    if (writeOp.getOutputMode.nonEmpty) {
      writer.outputMode(writeOp.getOutputMode)
    }

    if (writeOp.getQueryName.nonEmpty) {
      writer.queryName(writeOp.getQueryName)
    }

    if (writeOp.hasForeachWriter) {
      val foreach = writeOp.getForeachWriter.getPythonWriter
      val pythonFcn = transformPythonFunction(foreach)
      writer.foreachImplementation(
        new PythonForeachWriter(pythonFcn, dataset.schema).asInstanceOf[ForeachWriter[Any]])
    }

    val query = writeOp.getPath match {
      case "" if writeOp.hasTableName => writer.toTable(writeOp.getTableName)
      case "" => writer.start()
      case path => writer.start(path)
    }

    // Register the new query so that the session and query references are cached.
    SparkConnectService.streamingSessionManager.registerNewStreamingQuery(
      sessionHolder = SessionHolder(userId = userId, sessionId = sessionId, session),
      query = query)

    val result = WriteStreamOperationStartResult
      .newBuilder()
      .setQueryId(
        StreamingQueryInstanceId
          .newBuilder()
          .setId(query.id.toString)
          .setRunId(query.runId.toString)
          .build())
      .setName(Option(query.name).getOrElse(""))
      .build()

    streamHandler.sendResponse(
      ExecutePlanResponse
        .newBuilder()
        .setSessionId(sessionId)
        .setWriteStreamOperationStartResult(result)
        .build())
  }

  private def handleStreamingQueryCommand(
      command: StreamingQueryCommand,
      sessionId: String): Unit = {

    val id = command.getQueryId.getId
    val runId = command.getQueryId.getRunId

    val respBuilder = StreamingQueryCommandResult
      .newBuilder()
      .setQueryId(command.getQueryId)

    // Find the query in connect service level cache, otherwise check session's active streams.
    val query = SparkConnectService.streamingSessionManager
      .getCachedQuery(id, runId, session) // Common case: query is cached in the cache.
      .orElse { // Else try to find it in active streams. Mostly will not be found here either.
        Option(session.streams.get(id))
      } match {
      case Some(query) if query.runId.toString == runId =>
        query
      case Some(query) =>
        throw new IllegalArgumentException(
          s"Run id mismatch for query id $id. Run id in the request $runId " +
            s"does not match one on the server ${query.runId}. The query might have restarted.")
      case None =>
        throw new IllegalArgumentException(s"Streaming query $id is not found")
    }

    command.getCommandCase match {
      case StreamingQueryCommand.CommandCase.STATUS =>
        val queryStatus = query.status

        val statusResult = StreamingQueryCommandResult.StatusResult
          .newBuilder()
          .setStatusMessage(queryStatus.message)
          .setIsDataAvailable(queryStatus.isDataAvailable)
          .setIsTriggerActive(queryStatus.isTriggerActive)
          .setIsActive(query.isActive)
          .build()

        respBuilder.setStatus(statusResult)

      case StreamingQueryCommand.CommandCase.LAST_PROGRESS |
          StreamingQueryCommand.CommandCase.RECENT_PROGRESS =>
        val progressReports = if (command.getLastProgress) {
          Option(query.lastProgress).toSeq
        } else {
          query.recentProgress.toSeq
        }
        respBuilder.setRecentProgress(
          StreamingQueryCommandResult.RecentProgressResult
            .newBuilder()
            .addAllRecentProgressJson(
              progressReports.map(StreamingQueryProgress.jsonString).asJava)
            .build())

      case StreamingQueryCommand.CommandCase.STOP =>
        query.stop()

      case StreamingQueryCommand.CommandCase.PROCESS_ALL_AVAILABLE =>
        // This might take a long time, Spark-connect client keeps this connection alive.
        query.processAllAvailable()

      case StreamingQueryCommand.CommandCase.EXPLAIN =>
        val result = query match {
          case q: StreamingQueryWrapper =>
            q.streamingQuery.explainInternal(command.getExplain.getExtended)
          case _ =>
            throw new IllegalStateException(s"Unexpected type for streaming query: $query")
        }
        val explain = StreamingQueryCommandResult.ExplainResult
          .newBuilder()
          .setResult(result)
          .build()
        respBuilder.setExplain(explain)

      case StreamingQueryCommand.CommandCase.EXCEPTION =>
        val result = query.exception
        if (result.isDefined) {
          val e = result.get
          val exception_builder = StreamingQueryCommandResult.ExceptionResult
            .newBuilder()
          exception_builder
            .setExceptionMessage(e.toString)
            .setErrorClass(e.getErrorClass)

          val stackTrace = Option(ExceptionUtils.getStackTrace(e))
          stackTrace.foreach { s =>
            exception_builder.setStackTrace(s)
          }
          respBuilder.setException(exception_builder.build())
        }

      case StreamingQueryCommand.CommandCase.AWAIT_TERMINATION =>
        if (command.getAwaitTermination.hasTimeoutMs) {
          val terminated = query.awaitTermination(command.getAwaitTermination.getTimeoutMs)
          respBuilder.getAwaitTerminationBuilder
            .setTerminated(terminated)
        } else {
          query.awaitTermination()
          respBuilder.getAwaitTerminationBuilder
            .setTerminated(true)
        }

      case StreamingQueryCommand.CommandCase.COMMAND_NOT_SET =>
        throw new IllegalArgumentException("Missing command in StreamingQueryCommand")
    }

    streamHandler.sendResponse(
      ExecutePlanResponse
        .newBuilder()
        .setSessionId(sessionId)
        .setStreamingQueryCommandResult(respBuilder.build())
        .build())
  }

  private def handleStreamingQueryManagerCommand(
      command: StreamingQueryManagerCommand,
      sessionId: String): Unit = {

    val respBuilder = StreamingQueryManagerCommandResult.newBuilder()

    command.getCommandCase match {
      case StreamingQueryManagerCommand.CommandCase.ACTIVE =>
        val active_queries = session.streams.active
        respBuilder.getActiveBuilder.addAllActiveQueries(
          active_queries
            .map(query => buildStreamingQueryInstance(query))
            .toIterable
            .asJava)

      case StreamingQueryManagerCommand.CommandCase.GET_QUERY =>
        val query = session.streams.get(command.getGetQuery)
        respBuilder.setQuery(buildStreamingQueryInstance(query))

      case StreamingQueryManagerCommand.CommandCase.AWAIT_ANY_TERMINATION =>
        if (command.getAwaitAnyTermination.hasTimeoutMs) {
          val terminated =
            session.streams.awaitAnyTermination(command.getAwaitAnyTermination.getTimeoutMs)
          respBuilder.getAwaitAnyTerminationBuilder.setTerminated(terminated)
        } else {
          session.streams.awaitAnyTermination()
          respBuilder.getAwaitAnyTerminationBuilder.setTerminated(true)
        }

      case StreamingQueryManagerCommand.CommandCase.RESET_TERMINATED =>
        session.streams.resetTerminated()
        respBuilder.setResetTerminated(true)

      case StreamingQueryManagerCommand.CommandCase.COMMAND_NOT_SET =>
        throw new IllegalArgumentException("Missing command in StreamingQueryManagerCommand")
    }

    streamHandler.sendResponse(
      ExecutePlanResponse
        .newBuilder()
        .setSessionId(sessionId)
        .setStreamingQueryManagerCommandResult(respBuilder.build())
        .build())
  }

  private def buildStreamingQueryInstance(query: StreamingQuery): StreamingQueryInstance = {
    val builder = StreamingQueryInstance
      .newBuilder()
      .setId(
        StreamingQueryInstanceId
          .newBuilder()
          .setId(query.id.toString)
          .setRunId(query.runId.toString)
          .build())
    if (query.name != null) {
      builder.setName(query.name)
    }
    builder.build()
  }

  private def handleGetResourcesCommand(sessionId: String): Unit = {
    streamHandler.sendResponse(
      proto.ExecutePlanResponse
        .newBuilder()
        .setSessionId(sessionId)
        .setGetResourcesCommandResult(
          proto.GetResourcesCommandResult
            .newBuilder()
            .putAllResources(
              session.sparkContext.resources
                .mapValues(resource =>
                  proto.ResourceInformation
                    .newBuilder()
                    .setName(resource.name)
                    .addAllAddresses(resource.addresses.toIterable.asJava)
                    .build())
                .toMap
                .asJava)
            .build())
        .build())
  }

}
