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

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.Try
import scala.util.control.NonFatal

import com.google.common.base.Throwables
import com.google.common.collect.{Lists, Maps}
import com.google.protobuf.{Any => ProtoAny, ByteString}
import io.grpc.{Context, Status, StatusRuntimeException}
import io.grpc.stub.StreamObserver
import org.apache.commons.lang3.exception.ExceptionUtils

import org.apache.spark.{Partition, SparkEnv, TaskContext}
import org.apache.spark.api.python.{PythonEvalType, SimplePythonFunction}
import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.{CreateResourceProfileCommand, ExecutePlanResponse, SqlCommand, StreamingForeachFunction, StreamingQueryCommand, StreamingQueryCommandResult, StreamingQueryInstanceId, StreamingQueryManagerCommand, StreamingQueryManagerCommandResult, WriteStreamOperationStart, WriteStreamOperationStartResult}
import org.apache.spark.connect.proto.ExecutePlanResponse.SqlCommandResult
import org.apache.spark.connect.proto.Parse.ParseFormat
import org.apache.spark.connect.proto.StreamingQueryManagerCommandResult.StreamingQueryInstance
import org.apache.spark.connect.proto.WriteStreamOperationStart.TriggerCase
import org.apache.spark.internal.Logging
import org.apache.spark.ml.{functions => MLFunctions}
import org.apache.spark.resource.{ExecutorResourceRequest, ResourceProfile, TaskResourceProfile, TaskResourceRequest}
import org.apache.spark.sql.{Column, Dataset, Encoders, ForeachWriter, Observation, RelationalGroupedDataset, SparkSession}
import org.apache.spark.sql.avro.{AvroDataToCatalyst, CatalystDataToAvro}
import org.apache.spark.sql.catalyst.{expressions, AliasIdentifier, FunctionIdentifier}
import org.apache.spark.sql.catalyst.analysis.{GlobalTempView, LocalTempView, MultiAlias, NameParameterizedQuery, PosParameterizedQuery, UnresolvedAlias, UnresolvedAttribute, UnresolvedDataFrameStar, UnresolvedDeserializer, UnresolvedExtractValue, UnresolvedFunction, UnresolvedRegex, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.encoders.{AgnosticEncoder, ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.UnboundRowEncoder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.BloomFilterAggregate
import org.apache.spark.sql.catalyst.parser.{ParseException, ParserUtils}
import org.apache.spark.sql.catalyst.plans.{Cross, FullOuter, Inner, JoinType, LeftAnti, LeftOuter, LeftSemi, RightOuter, UsingJoin}
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.{AppendColumns, CoGroup, CollectMetrics, CommandResult, Deduplicate, DeduplicateWithinWatermark, DeserializeToObject, Except, FlatMapGroupsWithState, Intersect, JoinWith, LocalRelation, LogicalGroupState, LogicalPlan, MapGroups, MapPartitions, Project, Sample, SerializeFromObject, Sort, SubqueryAlias, TypedFilter, Union, Unpivot, UnresolvedHint}
import org.apache.spark.sql.catalyst.streaming.InternalOutputModes
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, CharVarcharUtils}
import org.apache.spark.sql.connect.common.{DataTypeProtoConverter, ForeachWriterPacket, InvalidPlanInput, LiteralValueProtoConverter, StorageLevelProtoConverter, StreamingListenerPacket, UdfPacket}
import org.apache.spark.sql.connect.config.Connect.CONNECT_GRPC_ARROW_MAX_BATCH_SIZE
import org.apache.spark.sql.connect.plugin.SparkConnectPluginRegistry
import org.apache.spark.sql.connect.service.{ExecuteHolder, SessionHolder, SparkConnectService}
import org.apache.spark.sql.connect.utils.MetricGenerator
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.arrow.ArrowConverters
import org.apache.spark.sql.execution.command.CreateViewCommand
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JDBCPartition, JDBCRelation}
import org.apache.spark.sql.execution.datasources.v2.python.UserDefinedPythonDataSource
import org.apache.spark.sql.execution.python.{PythonForeachWriter, UserDefinedPythonFunction, UserDefinedPythonTableFunction}
import org.apache.spark.sql.execution.stat.StatFunctions
import org.apache.spark.sql.execution.streaming.GroupStateImpl.groupStateTimeoutFromString
import org.apache.spark.sql.execution.streaming.StreamingQueryWrapper
import org.apache.spark.sql.expressions.{ReduceAggregator, SparkUserDefinedFunction}
import org.apache.spark.sql.internal.{CatalogImpl, TypedAggUtils}
import org.apache.spark.sql.protobuf.{CatalystDataToProtobuf, ProtobufDataToCatalyst}
import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode, StreamingQuery, StreamingQueryListener, StreamingQueryProgress, Trigger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.storage.CacheId
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.Utils

final case class InvalidCommandInput(
    private val message: String = "",
    private val cause: Throwable = null)
    extends Exception(message, cause)

class SparkConnectPlanner(
    val sessionHolder: SessionHolder,
    val executeHolderOpt: Option[ExecuteHolder] = None)
    extends Logging {

  def this(executeHolder: ExecuteHolder) = {
    this(executeHolder.sessionHolder, Some(executeHolder))
  }

  if (!executeHolderOpt.forall { e => e.sessionHolder == sessionHolder }) {
    throw new IllegalArgumentException("executeHolder does not belong to sessionHolder")
  }

  private[connect] def session: SparkSession = sessionHolder.session

  private[connect] def parser = session.sessionState.sqlParser

  private[connect] def userId: String = sessionHolder.userId

  private[connect] def sessionId: String = sessionHolder.sessionId

  private lazy val executeHolder = executeHolderOpt.getOrElse {
    throw new IllegalArgumentException("executeHolder is not set")
  }

  private lazy val pythonExec =
    sys.env.getOrElse("PYSPARK_PYTHON", sys.env.getOrElse("PYSPARK_DRIVER_PYTHON", "python3"))

  // The root of the query plan is a relation and we apply the transformations to it.
  def transformRelation(rel: proto.Relation): LogicalPlan = {
    val plan = rel.getRelTypeCase match {
      // DataFrame API
      case proto.Relation.RelTypeCase.SHOW_STRING => transformShowString(rel.getShowString)
      case proto.Relation.RelTypeCase.HTML_STRING => transformHtmlString(rel.getHtmlString)
      case proto.Relation.RelTypeCase.READ => transformReadRel(rel.getRead)
      case proto.Relation.RelTypeCase.PROJECT => transformProject(rel.getProject)
      case proto.Relation.RelTypeCase.FILTER => transformFilter(rel.getFilter)
      case proto.Relation.RelTypeCase.LIMIT => transformLimit(rel.getLimit)
      case proto.Relation.RelTypeCase.OFFSET => transformOffset(rel.getOffset)
      case proto.Relation.RelTypeCase.TAIL => transformTail(rel.getTail)
      case proto.Relation.RelTypeCase.JOIN => transformJoinOrJoinWith(rel.getJoin)
      case proto.Relation.RelTypeCase.AS_OF_JOIN => transformAsOfJoin(rel.getAsOfJoin)
      case proto.Relation.RelTypeCase.DEDUPLICATE => transformDeduplicate(rel.getDeduplicate)
      case proto.Relation.RelTypeCase.SET_OP => transformSetOperation(rel.getSetOp)
      case proto.Relation.RelTypeCase.SORT => transformSort(rel.getSort)
      case proto.Relation.RelTypeCase.DROP => transformDrop(rel.getDrop)
      case proto.Relation.RelTypeCase.AGGREGATE => transformAggregate(rel.getAggregate)
      case proto.Relation.RelTypeCase.SQL => transformSql(rel.getSql)
      case proto.Relation.RelTypeCase.LOCAL_RELATION =>
        transformLocalRelation(rel.getLocalRelation)
      case proto.Relation.RelTypeCase.SAMPLE => transformSample(rel.getSample)
      case proto.Relation.RelTypeCase.RANGE => transformRange(rel.getRange)
      case proto.Relation.RelTypeCase.SUBQUERY_ALIAS =>
        transformSubqueryAlias(rel.getSubqueryAlias)
      case proto.Relation.RelTypeCase.REPARTITION => transformRepartition(rel.getRepartition)
      case proto.Relation.RelTypeCase.FILL_NA => transformNAFill(rel.getFillNa)
      case proto.Relation.RelTypeCase.DROP_NA => transformNADrop(rel.getDropNa)
      case proto.Relation.RelTypeCase.REPLACE => transformReplace(rel.getReplace)
      case proto.Relation.RelTypeCase.SUMMARY => transformStatSummary(rel.getSummary)
      case proto.Relation.RelTypeCase.DESCRIBE => transformStatDescribe(rel.getDescribe)
      case proto.Relation.RelTypeCase.COV => transformStatCov(rel.getCov)
      case proto.Relation.RelTypeCase.CORR => transformStatCorr(rel.getCorr)
      case proto.Relation.RelTypeCase.APPROX_QUANTILE =>
        transformStatApproxQuantile(rel.getApproxQuantile)
      case proto.Relation.RelTypeCase.CROSSTAB =>
        transformStatCrosstab(rel.getCrosstab)
      case proto.Relation.RelTypeCase.FREQ_ITEMS => transformStatFreqItems(rel.getFreqItems)
      case proto.Relation.RelTypeCase.SAMPLE_BY =>
        transformStatSampleBy(rel.getSampleBy)
      case proto.Relation.RelTypeCase.TO_SCHEMA => transformToSchema(rel.getToSchema)
      case proto.Relation.RelTypeCase.TO_DF =>
        transformToDF(rel.getToDf)
      case proto.Relation.RelTypeCase.WITH_COLUMNS_RENAMED =>
        transformWithColumnsRenamed(rel.getWithColumnsRenamed)
      case proto.Relation.RelTypeCase.WITH_COLUMNS => transformWithColumns(rel.getWithColumns)
      case proto.Relation.RelTypeCase.WITH_WATERMARK =>
        transformWithWatermark(rel.getWithWatermark)
      case proto.Relation.RelTypeCase.CACHED_LOCAL_RELATION =>
        transformCachedLocalRelation(rel.getCachedLocalRelation)
      case proto.Relation.RelTypeCase.HINT => transformHint(rel.getHint)
      case proto.Relation.RelTypeCase.UNPIVOT => transformUnpivot(rel.getUnpivot)
      case proto.Relation.RelTypeCase.REPARTITION_BY_EXPRESSION =>
        transformRepartitionByExpression(rel.getRepartitionByExpression)
      case proto.Relation.RelTypeCase.MAP_PARTITIONS =>
        transformMapPartitions(rel.getMapPartitions)
      case proto.Relation.RelTypeCase.GROUP_MAP =>
        transformGroupMap(rel.getGroupMap)
      case proto.Relation.RelTypeCase.CO_GROUP_MAP =>
        transformCoGroupMap(rel.getCoGroupMap)
      case proto.Relation.RelTypeCase.APPLY_IN_PANDAS_WITH_STATE =>
        transformApplyInPandasWithState(rel.getApplyInPandasWithState)
      case proto.Relation.RelTypeCase.COMMON_INLINE_USER_DEFINED_TABLE_FUNCTION =>
        transformCommonInlineUserDefinedTableFunction(rel.getCommonInlineUserDefinedTableFunction)
      case proto.Relation.RelTypeCase.CACHED_REMOTE_RELATION =>
        transformCachedRemoteRelation(rel.getCachedRemoteRelation)
      case proto.Relation.RelTypeCase.COLLECT_METRICS =>
        transformCollectMetrics(rel.getCollectMetrics, rel.getCommon.getPlanId)
      case proto.Relation.RelTypeCase.PARSE => transformParse(rel.getParse)
      case proto.Relation.RelTypeCase.RELTYPE_NOT_SET =>
        throw new IndexOutOfBoundsException("Expected Relation to be set, but is empty.")

      // Catalog API (internal-only)
      case proto.Relation.RelTypeCase.CATALOG => transformCatalog(rel.getCatalog)

      // Handle plugins for Spark Connect Relation types.
      case proto.Relation.RelTypeCase.EXTENSION =>
        transformRelationPlugin(rel.getExtension)
      case _ => throw InvalidPlanInput(s"${rel.getUnknown} not supported.")
    }

    if (rel.hasCommon && rel.getCommon.hasPlanId) {
      plan.setTagValue(LogicalPlan.PLAN_ID_TAG, rel.getCommon.getPlanId)
    }
    plan
  }

  private def transformRelationPlugin(extension: ProtoAny): LogicalPlan = {
    SparkConnectPluginRegistry.relationRegistry
      // Lazily traverse the collection.
      .view
      // Apply the transformation.
      .map(p => p.transform(extension.toByteArray, this))
      // Find the first non-empty transformation or throw.
      .find(_.isPresent)
      .getOrElse(throw InvalidPlanInput("No handler found for extension"))
      .get()
  }

  private def transformCatalog(catalog: proto.Catalog): LogicalPlan = {
    catalog.getCatTypeCase match {
      case proto.Catalog.CatTypeCase.CURRENT_DATABASE => transformCurrentDatabase()
      case proto.Catalog.CatTypeCase.SET_CURRENT_DATABASE =>
        transformSetCurrentDatabase(catalog.getSetCurrentDatabase)
      case proto.Catalog.CatTypeCase.LIST_DATABASES =>
        transformListDatabases(catalog.getListDatabases)
      case proto.Catalog.CatTypeCase.LIST_TABLES => transformListTables(catalog.getListTables)
      case proto.Catalog.CatTypeCase.LIST_FUNCTIONS =>
        transformListFunctions(catalog.getListFunctions)
      case proto.Catalog.CatTypeCase.LIST_COLUMNS => transformListColumns(catalog.getListColumns)
      case proto.Catalog.CatTypeCase.GET_DATABASE => transformGetDatabase(catalog.getGetDatabase)
      case proto.Catalog.CatTypeCase.GET_TABLE => transformGetTable(catalog.getGetTable)
      case proto.Catalog.CatTypeCase.GET_FUNCTION => transformGetFunction(catalog.getGetFunction)
      case proto.Catalog.CatTypeCase.DATABASE_EXISTS =>
        transformDatabaseExists(catalog.getDatabaseExists)
      case proto.Catalog.CatTypeCase.TABLE_EXISTS => transformTableExists(catalog.getTableExists)
      case proto.Catalog.CatTypeCase.FUNCTION_EXISTS =>
        transformFunctionExists(catalog.getFunctionExists)
      case proto.Catalog.CatTypeCase.CREATE_EXTERNAL_TABLE =>
        transformCreateExternalTable(catalog.getCreateExternalTable)
      case proto.Catalog.CatTypeCase.CREATE_TABLE => transformCreateTable(catalog.getCreateTable)
      case proto.Catalog.CatTypeCase.DROP_TEMP_VIEW =>
        transformDropTempView(catalog.getDropTempView)
      case proto.Catalog.CatTypeCase.DROP_GLOBAL_TEMP_VIEW =>
        transformDropGlobalTempView(catalog.getDropGlobalTempView)
      case proto.Catalog.CatTypeCase.RECOVER_PARTITIONS =>
        transformRecoverPartitions(catalog.getRecoverPartitions)
      case proto.Catalog.CatTypeCase.IS_CACHED => transformIsCached(catalog.getIsCached)
      case proto.Catalog.CatTypeCase.CACHE_TABLE => transformCacheTable(catalog.getCacheTable)
      case proto.Catalog.CatTypeCase.UNCACHE_TABLE =>
        transformUncacheTable(catalog.getUncacheTable)
      case proto.Catalog.CatTypeCase.CLEAR_CACHE => transformClearCache()
      case proto.Catalog.CatTypeCase.REFRESH_TABLE =>
        transformRefreshTable(catalog.getRefreshTable)
      case proto.Catalog.CatTypeCase.REFRESH_BY_PATH =>
        transformRefreshByPath(catalog.getRefreshByPath)
      case proto.Catalog.CatTypeCase.CURRENT_CATALOG =>
        transformCurrentCatalog()
      case proto.Catalog.CatTypeCase.SET_CURRENT_CATALOG =>
        transformSetCurrentCatalog(catalog.getSetCurrentCatalog)
      case proto.Catalog.CatTypeCase.LIST_CATALOGS =>
        transformListCatalogs(catalog.getListCatalogs)
      case other => throw InvalidPlanInput(s"$other not supported.")
    }
  }

  private def transformShowString(rel: proto.ShowString): LogicalPlan = {
    val showString = Dataset
      .ofRows(session, transformRelation(rel.getInput))
      .showString(rel.getNumRows, rel.getTruncate, rel.getVertical)
    LocalRelation.fromProduct(
      output = AttributeReference("show_string", StringType, false)() :: Nil,
      data = Tuple1.apply(showString) :: Nil)
  }

  private def transformHtmlString(rel: proto.HtmlString): LogicalPlan = {
    val htmlString = Dataset
      .ofRows(session, transformRelation(rel.getInput))
      .htmlString(rel.getNumRows, rel.getTruncate)
    LocalRelation.fromProduct(
      output = AttributeReference("html_string", StringType, false)() :: Nil,
      data = Tuple1.apply(htmlString) :: Nil)
  }

  private def transformSql(sql: proto.SQL): LogicalPlan = {
    val args = sql.getArgsMap
    val namedArguments = sql.getNamedArgumentsMap
    val posArgs = sql.getPosArgsList
    val posArguments = sql.getPosArgumentsList
    val parsedPlan = parser.parsePlan(sql.getQuery)
    if (!namedArguments.isEmpty) {
      NameParameterizedQuery(
        parsedPlan,
        namedArguments.asScala.toMap.transform((_, v) => transformExpression(v)))
    } else if (!posArguments.isEmpty) {
      PosParameterizedQuery(parsedPlan, posArguments.asScala.map(transformExpression).toSeq)
    } else if (!args.isEmpty) {
      NameParameterizedQuery(
        parsedPlan,
        args.asScala.toMap.transform((_, v) => transformLiteral(v)))
    } else if (!posArgs.isEmpty) {
      PosParameterizedQuery(parsedPlan, posArgs.asScala.map(transformLiteral).toSeq)
    } else {
      parsedPlan
    }
  }

  private def transformSubqueryAlias(alias: proto.SubqueryAlias): LogicalPlan = {
    val aliasIdentifier =
      if (alias.getQualifierCount > 0) {
        AliasIdentifier.apply(alias.getAlias, alias.getQualifierList.asScala.toSeq)
      } else {
        AliasIdentifier.apply(alias.getAlias)
      }
    SubqueryAlias(aliasIdentifier, transformRelation(alias.getInput))
  }

  /**
   * All fields of [[proto.Sample]] are optional. However, given those are proto primitive types,
   * we cannot differentiate if the field is not or set when the field's value equals to the type
   * default value. In the future if this ever become a problem, one solution could be that to
   * wrap such fields into proto messages.
   */
  private def transformSample(rel: proto.Sample): LogicalPlan = {
    val plan = if (rel.getDeterministicOrder) {
      val input = Dataset.ofRows(session, transformRelation(rel.getInput))

      // It is possible that the underlying dataframe doesn't guarantee the ordering of rows in its
      // constituent partitions each time a split is materialized which could result in
      // overlapping splits. To prevent this, we explicitly sort each input partition to make the
      // ordering deterministic. Note that MapTypes cannot be sorted and are explicitly pruned out
      // from the sort order.
      val sortOrder = input.logicalPlan.output
        .filter(attr => RowOrdering.isOrderable(attr.dataType))
        .map(SortOrder(_, Ascending))
      if (sortOrder.nonEmpty) {
        Sort(sortOrder, global = false, input.logicalPlan)
      } else {
        input.cache()
        input.logicalPlan
      }
    } else {
      transformRelation(rel.getInput)
    }

    Sample(
      rel.getLowerBound,
      rel.getUpperBound,
      rel.getWithReplacement,
      if (rel.hasSeed) rel.getSeed else Utils.random.nextLong,
      plan)
  }

  private def transformRepartition(rel: proto.Repartition): LogicalPlan = {
    logical.Repartition(rel.getNumPartitions, rel.getShuffle, transformRelation(rel.getInput))
  }

  private def transformRange(rel: proto.Range): LogicalPlan = {
    val start = rel.getStart
    val end = rel.getEnd
    val step = rel.getStep
    val numPartitions = if (rel.hasNumPartitions) {
      rel.getNumPartitions
    } else {
      session.leafNodeDefaultParallelism
    }
    logical.Range(start, end, step, numPartitions)
  }

  private def transformNAFill(rel: proto.NAFill): LogicalPlan = {
    if (rel.getValuesCount == 0) {
      throw InvalidPlanInput(s"values must contains at least 1 item!")
    }
    if (rel.getValuesCount > 1 && rel.getValuesCount != rel.getColsCount) {
      throw InvalidPlanInput(
        s"When values contains more than 1 items, " +
          s"values and cols should have the same length!")
    }

    val dataset = Dataset.ofRows(session, transformRelation(rel.getInput))

    val cols = rel.getColsList.asScala.toArray
    val values = rel.getValuesList.asScala.toArray
    if (values.length == 1) {
      val value = LiteralValueProtoConverter.toCatalystValue(values.head)
      val columns = if (cols.nonEmpty) Some(cols.toImmutableArraySeq) else None
      dataset.na.fillValue(value, columns).logicalPlan
    } else {
      val valueMap = mutable.Map.empty[String, Any]
      cols.zip(values).foreach { case (col, value) =>
        valueMap.update(col, LiteralValueProtoConverter.toCatalystValue(value))
      }
      dataset.na.fill(valueMap = valueMap.toMap).logicalPlan
    }
  }

  private def transformNADrop(rel: proto.NADrop): LogicalPlan = {
    val dataset = Dataset.ofRows(session, transformRelation(rel.getInput))

    val cols = rel.getColsList.asScala.toArray

    (cols.nonEmpty, rel.hasMinNonNulls) match {
      case (true, true) =>
        dataset.na.drop(minNonNulls = rel.getMinNonNulls, cols = cols).logicalPlan
      case (true, false) =>
        dataset.na.drop(cols = cols).logicalPlan
      case (false, true) =>
        dataset.na.drop(minNonNulls = rel.getMinNonNulls).logicalPlan
      case (false, false) =>
        dataset.na.drop().logicalPlan
    }
  }

  private def transformReplace(rel: proto.NAReplace): LogicalPlan = {
    val replacement = mutable.Map.empty[Any, Any]
    rel.getReplacementsList.asScala.foreach { replace =>
      replacement.update(
        LiteralValueProtoConverter.toCatalystValue(replace.getOldValue),
        LiteralValueProtoConverter.toCatalystValue(replace.getNewValue))
    }

    if (rel.getColsCount == 0) {
      Dataset
        .ofRows(session, transformRelation(rel.getInput))
        .na
        .replace("*", replacement.toMap)
        .logicalPlan
    } else {
      Dataset
        .ofRows(session, transformRelation(rel.getInput))
        .na
        .replace(rel.getColsList.asScala.toSeq, replacement.toMap)
        .logicalPlan
    }
  }

  private def transformStatSummary(rel: proto.StatSummary): LogicalPlan = {
    Dataset
      .ofRows(session, transformRelation(rel.getInput))
      .summary(rel.getStatisticsList.asScala.toSeq: _*)
      .logicalPlan
  }

  private def transformStatDescribe(rel: proto.StatDescribe): LogicalPlan = {
    Dataset
      .ofRows(session, transformRelation(rel.getInput))
      .describe(rel.getColsList.asScala.toSeq: _*)
      .logicalPlan
  }

  private def transformStatCov(rel: proto.StatCov): LogicalPlan = {
    val df = Dataset.ofRows(session, transformRelation(rel.getInput))
    StatFunctions
      .calculateCovImpl(df, Seq(rel.getCol1, rel.getCol2))
      .logicalPlan
  }

  private def transformStatCorr(rel: proto.StatCorr): LogicalPlan = {
    val df = Dataset.ofRows(session, transformRelation(rel.getInput))
    if (rel.hasMethod) {
      StatFunctions
        .calculateCorrImpl(df, Seq(rel.getCol1, rel.getCol2), rel.getMethod)
        .logicalPlan
    } else {
      StatFunctions
        .calculateCorrImpl(df, Seq(rel.getCol1, rel.getCol2))
        .logicalPlan
    }
  }

  private def transformStatApproxQuantile(rel: proto.StatApproxQuantile): LogicalPlan = {
    val cols = rel.getColsList.asScala.toArray
    val probabilities = rel.getProbabilitiesList.asScala.map(_.doubleValue()).toArray
    val approxQuantile = Dataset
      .ofRows(session, transformRelation(rel.getInput))
      .stat
      .approxQuantile(cols, probabilities, rel.getRelativeError)
    LocalRelation.fromProduct(
      output =
        AttributeReference("approx_quantile", ArrayType(ArrayType(DoubleType)), false)() :: Nil,
      data = Tuple1.apply(approxQuantile) :: Nil)
  }

  private def transformStatCrosstab(rel: proto.StatCrosstab): LogicalPlan = {
    Dataset
      .ofRows(session, transformRelation(rel.getInput))
      .stat
      .crosstab(rel.getCol1, rel.getCol2)
      .logicalPlan
  }

  private def transformStatFreqItems(rel: proto.StatFreqItems): LogicalPlan = {
    val cols = rel.getColsList.asScala.toSeq
    val df = Dataset.ofRows(session, transformRelation(rel.getInput))
    if (rel.hasSupport) {
      df.stat.freqItems(cols, rel.getSupport).logicalPlan
    } else {
      df.stat.freqItems(cols).logicalPlan
    }
  }

  private def transformStatSampleBy(rel: proto.StatSampleBy): LogicalPlan = {
    val fractions = rel.getFractionsList.asScala.map { protoFraction =>
      val stratum = transformLiteral(protoFraction.getStratum) match {
        case Literal(s, StringType) if s != null => s.toString
        case literal => literal.value
      }
      (stratum, protoFraction.getFraction)
    }

    Dataset
      .ofRows(session, transformRelation(rel.getInput))
      .stat
      .sampleBy(
        col = Column(transformExpression(rel.getCol)),
        fractions = fractions.toMap,
        seed = if (rel.hasSeed) rel.getSeed else Utils.random.nextLong)
      .logicalPlan
  }

  private def transformToSchema(rel: proto.ToSchema): LogicalPlan = {
    val schema = transformDataType(rel.getSchema)
    assert(schema.isInstanceOf[StructType])

    Dataset
      .ofRows(session, transformRelation(rel.getInput))
      .to(schema.asInstanceOf[StructType])
      .logicalPlan
  }

  private def transformToDF(rel: proto.ToDF): LogicalPlan = {
    Dataset
      .ofRows(session, transformRelation(rel.getInput))
      .toDF(rel.getColumnNamesList.asScala.toSeq: _*)
      .logicalPlan
  }

  private def transformMapPartitions(rel: proto.MapPartitions): LogicalPlan = {
    val baseRel = transformRelation(rel.getInput)
    val commonUdf = rel.getFunc
    commonUdf.getFunctionCase match {
      case proto.CommonInlineUserDefinedFunction.FunctionCase.SCALAR_SCALA_UDF =>
        val analyzed = session.sessionState.executePlan(baseRel).analyzed
        transformTypedMapPartitions(commonUdf, analyzed)
      case proto.CommonInlineUserDefinedFunction.FunctionCase.PYTHON_UDF =>
        val pythonUdf = transformPythonUDF(commonUdf)
        val isBarrier = if (rel.hasIsBarrier) rel.getIsBarrier else false
        val profile = if (rel.hasProfileId) {
          val profileId = rel.getProfileId
          Some(session.sparkContext.resourceProfileManager.resourceProfileFromId(profileId))
        } else {
          None
        }
        pythonUdf.evalType match {
          case PythonEvalType.SQL_MAP_PANDAS_ITER_UDF =>
            logical.MapInPandas(
              pythonUdf,
              DataTypeUtils.toAttributes(pythonUdf.dataType.asInstanceOf[StructType]),
              baseRel,
              isBarrier,
              profile)
          case PythonEvalType.SQL_MAP_ARROW_ITER_UDF =>
            logical.MapInArrow(
              pythonUdf,
              DataTypeUtils.toAttributes(pythonUdf.dataType.asInstanceOf[StructType]),
              baseRel,
              isBarrier,
              profile)
          case _ =>
            throw InvalidPlanInput(
              s"Function with EvalType: ${pythonUdf.evalType} is not supported")
        }
      case _ =>
        throw InvalidPlanInput(
          s"Function with ID: ${commonUdf.getFunctionCase.getNumber} is not supported")
    }
  }

  private def generateObjAttr[T](enc: ExpressionEncoder[T]): Attribute = {
    val dataType = enc.deserializer.dataType
    val nullable = !enc.clsTag.runtimeClass.isPrimitive
    AttributeReference("obj", dataType, nullable)()
  }

  private def transformTypedMapPartitions(
      fun: proto.CommonInlineUserDefinedFunction,
      child: LogicalPlan): LogicalPlan = {
    val udf = TypedScalaUdf(fun, Some(child.output))
    val deserialized = DeserializeToObject(udf.inputDeserializer(), udf.inputObjAttr, child)
    val mapped = MapPartitions(
      udf.function.asInstanceOf[Iterator[Any] => Iterator[Any]],
      udf.outputObjAttr,
      deserialized)
    SerializeFromObject(udf.outputNamedExpression, mapped)
  }

  private def transformGroupMap(rel: proto.GroupMap): LogicalPlan = {
    val commonUdf = rel.getFunc
    commonUdf.getFunctionCase match {
      case proto.CommonInlineUserDefinedFunction.FunctionCase.SCALAR_SCALA_UDF =>
        transformTypedGroupMap(rel, commonUdf)

      case proto.CommonInlineUserDefinedFunction.FunctionCase.PYTHON_UDF =>
        val pythonUdf = transformPythonUDF(commonUdf)
        val cols =
          rel.getGroupingExpressionsList.asScala.toSeq.map(expr =>
            Column(transformExpression(expr)))
        val group = Dataset
          .ofRows(session, transformRelation(rel.getInput))
          .groupBy(cols: _*)

        pythonUdf.evalType match {
          case PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF =>
            group.flatMapGroupsInPandas(pythonUdf).logicalPlan

          case PythonEvalType.SQL_GROUPED_MAP_ARROW_UDF =>
            group.flatMapGroupsInArrow(pythonUdf).logicalPlan

          case _ =>
            throw InvalidPlanInput(
              s"Function with EvalType: ${pythonUdf.evalType} is not supported")
        }

      case _ =>
        throw InvalidPlanInput(
          s"Function with ID: ${commonUdf.getFunctionCase.getNumber} is not supported")
    }
  }

  private def transformTypedGroupMap(
      rel: proto.GroupMap,
      commonUdf: proto.CommonInlineUserDefinedFunction): LogicalPlan = {
    val udf = TypedScalaUdf(commonUdf)
    val ds = UntypedKeyValueGroupedDataset(
      rel.getInput,
      rel.getGroupingExpressionsList,
      rel.getSortingExpressionsList)

    if (rel.hasIsMapGroupsWithState) {
      val hasInitialState = !rel.getInitialGroupingExpressionsList.isEmpty && rel.hasInitialInput
      val initialDs = if (hasInitialState) {
        UntypedKeyValueGroupedDataset(
          rel.getInitialInput,
          rel.getInitialGroupingExpressionsList,
          rel.getSortingExpressionsList)
      } else {
        UntypedKeyValueGroupedDataset(
          rel.getInput,
          rel.getGroupingExpressionsList,
          rel.getSortingExpressionsList)
      }
      val timeoutConf = if (!rel.hasTimeoutConf) {
        GroupStateTimeout.NoTimeout
      } else {
        groupStateTimeoutFromString(rel.getTimeoutConf)
      }
      val outputMode = if (!rel.hasOutputMode) {
        OutputMode.Update
      } else {
        InternalOutputModes(rel.getOutputMode)
      }

      val flatMapGroupsWithState = if (hasInitialState) {
        new FlatMapGroupsWithState(
          udf.function
            .asInstanceOf[(Any, Iterator[Any], LogicalGroupState[Any]) => Iterator[Any]],
          udf.inputDeserializer(ds.groupingAttributes),
          ds.valueDeserializer,
          ds.groupingAttributes,
          ds.dataAttributes,
          udf.outputObjAttr,
          initialDs.vEncoder.asInstanceOf[ExpressionEncoder[Any]],
          outputMode,
          rel.getIsMapGroupsWithState,
          timeoutConf,
          hasInitialState,
          initialDs.groupingAttributes,
          initialDs.dataAttributes,
          initialDs.valueDeserializer,
          initialDs.analyzed,
          ds.analyzed)
      } else {
        new FlatMapGroupsWithState(
          udf.function
            .asInstanceOf[(Any, Iterator[Any], LogicalGroupState[Any]) => Iterator[Any]],
          udf.inputDeserializer(ds.groupingAttributes),
          ds.valueDeserializer,
          ds.groupingAttributes,
          ds.dataAttributes,
          udf.outputObjAttr,
          initialDs.vEncoder.asInstanceOf[ExpressionEncoder[Any]],
          outputMode,
          rel.getIsMapGroupsWithState,
          timeoutConf,
          hasInitialState,
          ds.groupingAttributes,
          ds.dataAttributes,
          udf.inputDeserializer(ds.groupingAttributes),
          LocalRelation(initialDs.vEncoder.schema), // empty data set
          ds.analyzed)
      }
      SerializeFromObject(udf.outputNamedExpression, flatMapGroupsWithState)
    } else {
      val mapped = new MapGroups(
        udf.function.asInstanceOf[(Any, Iterator[Any]) => IterableOnce[Any]],
        udf.inputDeserializer(ds.groupingAttributes),
        ds.valueDeserializer,
        ds.groupingAttributes,
        ds.dataAttributes,
        ds.sortOrder,
        udf.outputObjAttr,
        ds.analyzed)
      SerializeFromObject(udf.outputNamedExpression, mapped)
    }
  }

  private def transformCoGroupMap(rel: proto.CoGroupMap): LogicalPlan = {
    val commonUdf = rel.getFunc
    commonUdf.getFunctionCase match {
      case proto.CommonInlineUserDefinedFunction.FunctionCase.SCALAR_SCALA_UDF =>
        transformTypedCoGroupMap(rel, commonUdf)

      case proto.CommonInlineUserDefinedFunction.FunctionCase.PYTHON_UDF =>
        val inputCols =
          rel.getInputGroupingExpressionsList.asScala.toSeq.map(expr =>
            Column(transformExpression(expr)))
        val otherCols =
          rel.getOtherGroupingExpressionsList.asScala.toSeq.map(expr =>
            Column(transformExpression(expr)))

        val input = Dataset
          .ofRows(session, transformRelation(rel.getInput))
          .groupBy(inputCols: _*)
        val other = Dataset
          .ofRows(session, transformRelation(rel.getOther))
          .groupBy(otherCols: _*)

        val pythonUdf = createUserDefinedPythonFunction(commonUdf)
          .builder(input.df.logicalPlan.output ++ other.df.logicalPlan.output)
          .asInstanceOf[PythonUDF]

        pythonUdf.evalType match {
          case PythonEvalType.SQL_COGROUPED_MAP_PANDAS_UDF =>
            input.flatMapCoGroupsInPandas(other, pythonUdf).logicalPlan

          case PythonEvalType.SQL_COGROUPED_MAP_ARROW_UDF =>
            input.flatMapCoGroupsInArrow(other, pythonUdf).logicalPlan

          case _ =>
            throw InvalidPlanInput(
              s"Function with EvalType: ${pythonUdf.evalType} is not supported")
        }

      case _ =>
        throw InvalidPlanInput(
          s"Function with ID: ${commonUdf.getFunctionCase.getNumber} is not supported")
    }
  }

  private def transformTypedCoGroupMap(
      rel: proto.CoGroupMap,
      commonUdf: proto.CommonInlineUserDefinedFunction): LogicalPlan = {
    val udf = TypedScalaUdf(commonUdf)
    val left = UntypedKeyValueGroupedDataset(
      rel.getInput,
      rel.getInputGroupingExpressionsList,
      rel.getInputSortingExpressionsList)
    val right = UntypedKeyValueGroupedDataset(
      rel.getOther,
      rel.getOtherGroupingExpressionsList,
      rel.getOtherSortingExpressionsList)

    val mapped = CoGroup(
      udf.function.asInstanceOf[(Any, Iterator[Any], Iterator[Any]) => IterableOnce[Any]],
      // The `leftGroup` and `rightGroup` are guaranteed te be of same schema, so it's safe to
      // resolve the `keyDeserializer` based on either of them, here we pick the left one.
      udf.inputDeserializer(left.groupingAttributes),
      left.valueDeserializer,
      right.valueDeserializer,
      left.groupingAttributes,
      right.groupingAttributes,
      left.dataAttributes,
      right.dataAttributes,
      left.sortOrder,
      right.sortOrder,
      udf.outputObjAttr,
      left.analyzed,
      right.analyzed)
    SerializeFromObject(udf.outputNamedExpression, mapped)
  }

  /**
   * This is the untyped version of [[org.apache.spark.sql.KeyValueGroupedDataset]].
   */
  private case class UntypedKeyValueGroupedDataset(
      kEncoder: ExpressionEncoder[_],
      vEncoder: ExpressionEncoder[_],
      analyzed: LogicalPlan,
      dataAttributes: Seq[Attribute],
      groupingAttributes: Seq[Attribute],
      sortOrder: Seq[SortOrder]) {
    val valueDeserializer: Expression =
      UnresolvedDeserializer(vEncoder.deserializer, dataAttributes)
  }

  private object UntypedKeyValueGroupedDataset {
    def apply(
        input: proto.Relation,
        groupingExprs: java.util.List[proto.Expression],
        sortingExprs: java.util.List[proto.Expression]): UntypedKeyValueGroupedDataset = {

      // Compute sort order
      val sortExprs =
        sortingExprs.asScala.toSeq.map(expr => transformExpression(expr))
      val sortOrder: Seq[SortOrder] = MapGroups.sortOrder(sortExprs)

      apply(transformRelation(input), groupingExprs, sortOrder)
    }

    def apply(
        logicalPlan: LogicalPlan,
        groupingExprs: java.util.List[proto.Expression],
        sortOrder: Seq[SortOrder]): UntypedKeyValueGroupedDataset = {
      // If created via ds#groupByKey, then there should be only one groupingFunc.
      // If created via relationalGroupedDS#as, then we are expecting a dummy groupingFuc
      // (for types) + groupingExprs
      if (groupingExprs.size() == 1) {
        createFromGroupByKeyFunc(logicalPlan, groupingExprs, sortOrder)
      } else if (groupingExprs.size() > 1) {
        createFromRelationalDataset(logicalPlan, groupingExprs, sortOrder)
      } else {
        throw InvalidPlanInput(
          "The grouping expression cannot be absent for KeyValueGroupedDataset")
      }
    }

    private def createFromRelationalDataset(
        logicalPlan: LogicalPlan,
        groupingExprs: java.util.List[proto.Expression],
        sortOrder: Seq[SortOrder]): UntypedKeyValueGroupedDataset = {
      assert(groupingExprs.size() >= 1)
      val dummyFunc = TypedScalaUdf(groupingExprs.get(0), None)
      val groupExprs = groupingExprs.asScala.toSeq.drop(1).map(expr => transformExpression(expr))

      val (qe, aliasedGroupings) =
        RelationalGroupedDataset.handleGroupingExpression(logicalPlan, session, groupExprs)

      UntypedKeyValueGroupedDataset(
        dummyFunc.outEnc,
        dummyFunc.inEnc,
        qe.analyzed,
        logicalPlan.output,
        aliasedGroupings,
        sortOrder)
    }

    private def createFromGroupByKeyFunc(
        logicalPlan: LogicalPlan,
        groupingExprs: java.util.List[proto.Expression],
        sortOrder: Seq[SortOrder]): UntypedKeyValueGroupedDataset = {
      assert(groupingExprs.size() == 1)
      val groupFunc = TypedScalaUdf(groupingExprs.get(0), Some(logicalPlan.output))
      val vEnc = groupFunc.inEnc
      val kEnc = groupFunc.outEnc

      val withGroupingKey = AppendColumns(groupFunc.function, vEnc, kEnc, logicalPlan)
      // The input logical plan of KeyValueGroupedDataset need to be executed and analyzed
      val analyzed = session.sessionState.executePlan(withGroupingKey).analyzed

      UntypedKeyValueGroupedDataset(
        kEnc,
        vEnc,
        analyzed,
        logicalPlan.output,
        withGroupingKey.newColumns,
        sortOrder)
    }
  }

  /**
   * The UDF used in typed APIs, where the input column is absent.
   */
  private case class TypedScalaUdf(
      function: AnyRef,
      funcOutEnc: AgnosticEncoder[_],
      funcInEnc: AgnosticEncoder[_],
      inputAttrs: Option[Seq[Attribute]]) {
    import TypedScalaUdf.encoderFor
    def outputNamedExpression: Seq[NamedExpression] = outEnc.namedExpressions
    def inputDeserializer(inputAttributes: Seq[Attribute] = Nil): Expression =
      UnresolvedDeserializer(inEnc.deserializer, inputAttributes)

    def outEnc: ExpressionEncoder[_] = encoderFor(funcOutEnc, "output")
    def outputObjAttr: Attribute = generateObjAttr(outEnc)
    def inEnc: ExpressionEncoder[_] = encoderFor(funcInEnc, "input", inputAttrs)
    def inputObjAttr: Attribute = generateObjAttr(inEnc)
  }

  private object TypedScalaUdf {
    def apply(expr: proto.Expression, inputAttrs: Option[Seq[Attribute]]): TypedScalaUdf = {
      if (expr.hasCommonInlineUserDefinedFunction
        && expr.getCommonInlineUserDefinedFunction.hasScalarScalaUdf) {
        apply(expr.getCommonInlineUserDefinedFunction, inputAttrs)
      } else {
        throw InvalidPlanInput(s"Expecting a Scala UDF, but get ${expr.getExprTypeCase}")
      }
    }

    def apply(
        commonUdf: proto.CommonInlineUserDefinedFunction,
        inputAttrs: Option[Seq[Attribute]] = None): TypedScalaUdf = {
      val udf = unpackUdf(commonUdf)
      // There might be more than one inputs, but we only interested in the first one.
      // Most typed API takes one UDF input.
      // For the few that takes more than one inputs, e.g. grouping function mapping UDFs,
      // the first input which is the key of the grouping function.
      assert(udf.inputEncoders.nonEmpty)
      val inEnc = udf.inputEncoders.head // single input encoder or key encoder
      TypedScalaUdf(udf.function, udf.outputEncoder, inEnc, inputAttrs)
    }

    def encoderFor(
        encoder: AgnosticEncoder[_],
        errorType: String,
        inputAttrs: Option[Seq[Attribute]] = None): ExpressionEncoder[_] = {
      // TODO: handle nested unbound row encoders
      if (encoder == UnboundRowEncoder) {
        inputAttrs
          .map(attrs =>
            ExpressionEncoder(RowEncoder.encoderFor(StructType(attrs.map(a =>
              StructField(a.name, a.dataType, a.nullable))))))
          .getOrElse(
            throw InvalidPlanInput(s"Row is not a supported $errorType type for this UDF."))
      } else {
        ExpressionEncoder(encoder)
      }
    }
  }

  private def transformApplyInPandasWithState(rel: proto.ApplyInPandasWithState): LogicalPlan = {
    val pythonUdf = transformPythonUDF(rel.getFunc)
    val cols =
      rel.getGroupingExpressionsList.asScala.toSeq.map(expr => Column(transformExpression(expr)))

    val outputSchema = parseSchema(rel.getOutputSchema)

    val stateSchema = parseSchema(rel.getStateSchema)

    Dataset
      .ofRows(session, transformRelation(rel.getInput))
      .groupBy(cols: _*)
      .applyInPandasWithState(
        pythonUdf,
        outputSchema,
        stateSchema,
        rel.getOutputMode,
        rel.getTimeoutConf)
      .logicalPlan
  }

  private def transformCommonInlineUserDefinedTableFunction(
      fun: proto.CommonInlineUserDefinedTableFunction): LogicalPlan = {
    fun.getFunctionCase match {
      case proto.CommonInlineUserDefinedTableFunction.FunctionCase.PYTHON_UDTF =>
        val function = createPythonUserDefinedTableFunction(fun)
        function.builder(
          fun.getArgumentsList.asScala.map(transformExpression).toSeq,
          session.sessionState.sqlParser)
      case _ =>
        throw InvalidPlanInput(
          s"Function with ID: ${fun.getFunctionCase.getNumber} is not supported")
    }
  }

  private def transformPythonTableFunction(fun: proto.PythonUDTF): SimplePythonFunction = {
    SimplePythonFunction(
      command = fun.getCommand.toByteArray.toImmutableArraySeq,
      // Empty environment variables
      envVars = Maps.newHashMap(),
      pythonIncludes = sessionHolder.artifactManager.getPythonIncludes.asJava,
      pythonExec = pythonExec,
      pythonVer = fun.getPythonVer,
      // Empty broadcast variables
      broadcastVars = Lists.newArrayList(),
      // Accumulator if available
      accumulator = sessionHolder.pythonAccumulator.orNull)
  }

  private def transformPythonDataSource(ds: proto.PythonDataSource): SimplePythonFunction = {
    SimplePythonFunction(
      command = ds.getCommand.toByteArray.toImmutableArraySeq,
      // Empty environment variables
      envVars = Maps.newHashMap(),
      pythonIncludes = sessionHolder.artifactManager.getPythonIncludes.asJava,
      pythonExec = pythonExec,
      pythonVer = ds.getPythonVer,
      // Empty broadcast variables
      broadcastVars = Lists.newArrayList(),
      // Accumulator if available
      accumulator = sessionHolder.pythonAccumulator.orNull)
  }

  private def transformCachedRemoteRelation(rel: proto.CachedRemoteRelation): LogicalPlan = {
    sessionHolder
      .getDataFrameOrThrow(rel.getRelationId)
      .logicalPlan
  }

  private def transformWithColumnsRenamed(rel: proto.WithColumnsRenamed): LogicalPlan = {
    if (rel.getRenamesCount > 0) {
      val (colNames, newColNames) = rel.getRenamesList.asScala.toSeq.map { rename =>
        (rename.getColName, rename.getNewColName)
      }.unzip
      Dataset
        .ofRows(session, transformRelation(rel.getInput))
        .withColumnsRenamed(colNames, newColNames)
        .logicalPlan
    } else {
      // for backward compatibility
      Dataset
        .ofRows(session, transformRelation(rel.getInput))
        .withColumnsRenamed(rel.getRenameColumnsMapMap)
        .logicalPlan
    }
  }

  private def transformWithColumns(rel: proto.WithColumns): LogicalPlan = {
    val (colNames, cols, metadata) =
      rel.getAliasesList.asScala.toSeq.map { alias =>
        if (alias.getNameCount != 1) {
          throw InvalidPlanInput(s"""WithColumns require column name only contains one name part,
             |but got ${alias.getNameList.toString}""".stripMargin)
        }

        val metadata = if (alias.hasMetadata && alias.getMetadata.nonEmpty) {
          Metadata.fromJson(alias.getMetadata)
        } else {
          Metadata.empty
        }

        (alias.getName(0), Column(transformExpression(alias.getExpr)), metadata)
      }.unzip3

    Dataset
      .ofRows(session, transformRelation(rel.getInput))
      .withColumns(colNames, cols, metadata)
      .logicalPlan
  }

  private def transformWithWatermark(rel: proto.WithWatermark): LogicalPlan = {
    Dataset
      .ofRows(session, transformRelation(rel.getInput))
      .withWatermark(rel.getEventTime, rel.getDelayThreshold)
      .logicalPlan
  }

  private def transformCachedLocalRelation(rel: proto.CachedLocalRelation): LogicalPlan = {
    val blockManager = session.sparkContext.env.blockManager
    val blockId = CacheId(sessionHolder.session.sessionUUID, rel.getHash)
    val bytes = blockManager.getLocalBytes(blockId)
    bytes
      .map { blockData =>
        try {
          val localRelation = proto.Relation
            .newBuilder()
            .getLocalRelation
            .getParserForType
            .parseFrom(blockData.toInputStream())
          transformLocalRelation(localRelation)
        } finally {
          blockManager.releaseLock(blockId)
        }
      }
      .getOrElse {
        throw InvalidPlanInput(
          s"Not found any cached local relation with the hash: ${blockId.hash} in " +
            s"the session with sessionUUID ${blockId.sessionUUID}.")
      }
  }

  private def transformHint(rel: proto.Hint): LogicalPlan = {

    val params = rel.getParametersList.asScala.toSeq.map(transformExpression)
    UnresolvedHint(rel.getName, params, transformRelation(rel.getInput))
  }

  private def transformUnpivot(rel: proto.Unpivot): LogicalPlan = {
    val ids = rel.getIdsList.asScala.toArray.map { expr =>
      Column(transformExpression(expr))
    }

    if (!rel.hasValues) {
      Unpivot(
        Some(ids.map(_.named).toImmutableArraySeq),
        None,
        None,
        rel.getVariableColumnName,
        Seq(rel.getValueColumnName),
        transformRelation(rel.getInput))
    } else {
      val values = rel.getValues.getValuesList.asScala.toArray.map { expr =>
        Column(transformExpression(expr))
      }

      Unpivot(
        Some(ids.map(_.named).toImmutableArraySeq),
        Some(values.map(v => Seq(v.named)).toImmutableArraySeq),
        None,
        rel.getVariableColumnName,
        Seq(rel.getValueColumnName),
        transformRelation(rel.getInput))
    }
  }

  private def transformRepartitionByExpression(
      rel: proto.RepartitionByExpression): LogicalPlan = {
    val numPartitionsOpt = if (rel.hasNumPartitions) {
      Some(rel.getNumPartitions)
    } else {
      None
    }
    val partitionExpressions = rel.getPartitionExprsList.asScala.map(transformExpression).toSeq
    logical.RepartitionByExpression(
      partitionExpressions,
      transformRelation(rel.getInput),
      numPartitionsOpt)
  }

  private def transformCollectMetrics(rel: proto.CollectMetrics, planId: Long): LogicalPlan = {
    val metrics = rel.getMetricsList.asScala.toSeq.map { expr =>
      Column(transformExpression(expr))
    }
    val name = rel.getName
    val input = transformRelation(rel.getInput)

    if (input.isStreaming || executeHolderOpt.isEmpty) {
      CollectMetrics(name, metrics.map(_.named), transformRelation(rel.getInput), planId)
    } else {
      val observation = Observation(name)
      observation.register(session, planId)
      executeHolderOpt.get.addObservation(name, observation)
      CollectMetrics(name, metrics.map(_.named), transformRelation(rel.getInput), planId)
    }
  }

  private def transformDeduplicate(rel: proto.Deduplicate): LogicalPlan = {
    if (!rel.hasInput) {
      throw InvalidPlanInput("Deduplicate needs a plan input")
    }
    if (rel.getAllColumnsAsKeys && rel.getColumnNamesCount > 0) {
      throw InvalidPlanInput("Cannot deduplicate on both all columns and a subset of columns")
    }
    if (!rel.getAllColumnsAsKeys && rel.getColumnNamesCount == 0) {
      throw InvalidPlanInput(
        "Deduplicate requires to either deduplicate on all columns or a subset of columns")
    }
    val queryExecution = new QueryExecution(session, transformRelation(rel.getInput))
    val resolver = session.sessionState.analyzer.resolver
    val allColumns = queryExecution.analyzed.output
    if (rel.getAllColumnsAsKeys) {
      if (rel.getWithinWatermark) DeduplicateWithinWatermark(allColumns, queryExecution.analyzed)
      else Deduplicate(allColumns, queryExecution.analyzed)
    } else {
      val toGroupColumnNames = rel.getColumnNamesList.asScala.toSeq
      val groupCols = toGroupColumnNames.flatMap { (colName: String) =>
        // It is possibly there are more than one columns with the same name,
        // so we call filter instead of find.
        val cols = allColumns.filter(col => resolver(col.name, colName))
        if (cols.isEmpty) {
          throw InvalidPlanInput(s"Invalid deduplicate column ${colName}")
        }
        cols
      }
      if (rel.getWithinWatermark) DeduplicateWithinWatermark(groupCols, queryExecution.analyzed)
      else Deduplicate(groupCols, queryExecution.analyzed)
    }
  }

  private def transformDataType(t: proto.DataType): DataType = {
    t.getKindCase match {
      case proto.DataType.KindCase.UNPARSED =>
        parseDatatypeString(t.getUnparsed.getDataTypeString)
      case _ => DataTypeProtoConverter.toCatalystType(t)
    }
  }

  private[connect] def parseDatatypeString(sqlText: String): DataType = {
    try {
      parser.parseTableSchema(sqlText)
    } catch {
      case e: ParseException =>
        try {
          parser.parseDataType(sqlText)
        } catch {
          case _: ParseException =>
            try {
              parser.parseDataType(s"struct<${sqlText.trim}>")
            } catch {
              case _: ParseException =>
                throw e
            }
        }
    }
  }

  private def transformLocalRelation(rel: proto.LocalRelation): LogicalPlan = {
    var schema: StructType = null
    if (rel.hasSchema) {
      val schemaType = DataType.parseTypeWithFallback(
        rel.getSchema,
        parseDatatypeString,
        fallbackParser = DataType.fromJson)
      schema = schemaType match {
        case s: StructType => s
        case d => StructType(Seq(StructField("value", d)))
      }
    }

    if (rel.hasData) {
      val (rows, structType) = ArrowConverters.fromBatchWithSchemaIterator(
        Iterator(rel.getData.toByteArray),
        TaskContext.get())
      if (structType == null) {
        throw InvalidPlanInput(s"Input data for LocalRelation does not produce a schema.")
      }
      val attributes = DataTypeUtils.toAttributes(structType)
      val proj = UnsafeProjection.create(attributes, attributes)
      val data = rows.map(proj)

      if (schema == null) {
        logical.LocalRelation(attributes, data.map(_.copy()).toSeq)
      } else {
        def normalize(dt: DataType): DataType = dt match {
          case udt: UserDefinedType[_] => normalize(udt.sqlType)
          case StructType(fields) =>
            val newFields = fields.zipWithIndex.map {
              case (StructField(_, dataType, nullable, metadata), i) =>
                StructField(s"col_$i", normalize(dataType), nullable, metadata)
            }
            StructType(newFields)
          case ArrayType(elementType, containsNull) =>
            ArrayType(normalize(elementType), containsNull)
          case MapType(keyType, valueType, valueContainsNull) =>
            MapType(normalize(keyType), normalize(valueType), valueContainsNull)
          case _ => dt
        }

        val normalized = normalize(schema).asInstanceOf[StructType]

        import org.apache.spark.util.ArrayImplicits._
        val project = Dataset
          .ofRows(
            session,
            logicalPlan = logical.LocalRelation(normalize(structType).asInstanceOf[StructType]))
          .toDF(normalized.names.toImmutableArraySeq: _*)
          .to(normalized)
          .logicalPlan
          .asInstanceOf[Project]

        val proj = UnsafeProjection.create(project.projectList, project.child.output)
        logical.LocalRelation(
          DataTypeUtils.toAttributes(schema),
          data.map(proj).map(_.copy()).toSeq)
      }
    } else {
      if (schema == null) {
        throw InvalidPlanInput(
          s"Schema for LocalRelation is required when the input data is not provided.")
      }
      LocalRelation(schema)
    }
  }

  /** Parse as DDL, with a fallback to JSON. Throws an exception if if fails to parse. */
  private def parseSchema(schema: String): StructType = {
    DataType.parseTypeWithFallback(
      schema,
      StructType.fromDDL,
      fallbackParser = DataType.fromJson) match {
      case s: StructType => s
      case other => throw InvalidPlanInput(s"Invalid schema $other")
    }
  }

  private def transformReadRel(rel: proto.Read): LogicalPlan = {

    rel.getReadTypeCase match {
      case proto.Read.ReadTypeCase.NAMED_TABLE =>
        UnresolvedRelation(
          parser.parseMultipartIdentifier(rel.getNamedTable.getUnparsedIdentifier),
          new CaseInsensitiveStringMap(rel.getNamedTable.getOptionsMap),
          isStreaming = rel.getIsStreaming)

      case proto.Read.ReadTypeCase.DATA_SOURCE if !rel.getIsStreaming =>
        val localMap = CaseInsensitiveMap[String](rel.getDataSource.getOptionsMap.asScala.toMap)
        val reader = session.read
        if (rel.getDataSource.hasFormat) {
          reader.format(rel.getDataSource.getFormat)
        }
        localMap.foreach { case (key, value) => reader.option(key, value) }
        if (rel.getDataSource.getFormat == "jdbc" && rel.getDataSource.getPredicatesCount > 0) {
          if (!localMap.contains(JDBCOptions.JDBC_URL) ||
            !localMap.contains(JDBCOptions.JDBC_TABLE_NAME)) {
            throw InvalidPlanInput(s"Invalid jdbc params, please specify jdbc url and table.")
          }

          val url = rel.getDataSource.getOptionsMap.get(JDBCOptions.JDBC_URL)
          val table = rel.getDataSource.getOptionsMap.get(JDBCOptions.JDBC_TABLE_NAME)
          val options = new JDBCOptions(url, table, localMap)
          val predicates = rel.getDataSource.getPredicatesList.asScala.toArray
          val parts: Array[Partition] = predicates.zipWithIndex.map { case (part, i) =>
            JDBCPartition(part, i): Partition
          }
          val relation = JDBCRelation(parts, options)(session)
          LogicalRelation(relation)
        } else if (rel.getDataSource.getPredicatesCount == 0) {
          if (rel.getDataSource.hasSchema && rel.getDataSource.getSchema.nonEmpty) {
            reader.schema(parseSchema(rel.getDataSource.getSchema))
          }
          if (rel.getDataSource.getPathsCount == 0) {
            reader.load().queryExecution.analyzed
          } else if (rel.getDataSource.getPathsCount == 1) {
            reader.load(rel.getDataSource.getPaths(0)).queryExecution.analyzed
          } else {
            reader.load(rel.getDataSource.getPathsList.asScala.toSeq: _*).queryExecution.analyzed
          }
        } else {
          throw InvalidPlanInput(
            s"Predicates are not supported for ${rel.getDataSource.getFormat} data sources.")
        }

      case proto.Read.ReadTypeCase.DATA_SOURCE if rel.getIsStreaming =>
        val streamSource = rel.getDataSource
        val reader = session.readStream
        if (streamSource.hasFormat) {
          reader.format(streamSource.getFormat)
        }
        reader.options(streamSource.getOptionsMap.asScala)
        if (streamSource.getSchema.nonEmpty) {
          reader.schema(parseSchema(streamSource.getSchema))
        }
        val streamDF = streamSource.getPathsCount match {
          case 0 => reader.load()
          case 1 => reader.load(streamSource.getPaths(0))
          case _ =>
            throw InvalidPlanInput(s"Multiple paths are not supported for streaming source")
        }

        streamDF.queryExecution.analyzed

      case _ => throw InvalidPlanInput(s"Does not support ${rel.getReadTypeCase.name()}")
    }
  }

  private def transformParse(rel: proto.Parse): LogicalPlan = {
    def dataFrameReader = {
      val localMap = CaseInsensitiveMap[String](rel.getOptionsMap.asScala.toMap)
      val reader = session.read
      if (rel.hasSchema) {
        DataTypeProtoConverter.toCatalystType(rel.getSchema) match {
          case s: StructType => reader.schema(s)
          case other => throw InvalidPlanInput(s"Invalid schema dataType $other")
        }
      }
      localMap.foreach { case (key, value) => reader.option(key, value) }
      reader
    }
    def ds: Dataset[String] = Dataset(session, transformRelation(rel.getInput))(Encoders.STRING)

    rel.getFormat match {
      case ParseFormat.PARSE_FORMAT_CSV =>
        dataFrameReader.csv(ds).queryExecution.analyzed
      case ParseFormat.PARSE_FORMAT_JSON =>
        dataFrameReader.json(ds).queryExecution.analyzed
      case _ => throw InvalidPlanInput("Does not support " + rel.getFormat.name())
    }
  }

  private def transformFilter(rel: proto.Filter): LogicalPlan = {
    assert(rel.hasInput)
    val baseRel = transformRelation(rel.getInput)
    val cond = rel.getCondition
    if (isTypedScalaUdfExpr(cond)) {
      transformTypedFilter(cond.getCommonInlineUserDefinedFunction, baseRel)
    } else {
      logical.Filter(condition = transformExpression(cond), child = baseRel)
    }
  }

  private def isTypedScalaUdfExpr(expr: proto.Expression): Boolean = {
    expr.getExprTypeCase match {
      case proto.Expression.ExprTypeCase.COMMON_INLINE_USER_DEFINED_FUNCTION =>
        val udf = expr.getCommonInlineUserDefinedFunction
        // A typed scala udf is a scala udf && the udf argument is an unresolved start.
        udf.getFunctionCase ==
          proto.CommonInlineUserDefinedFunction.FunctionCase.SCALAR_SCALA_UDF &&
          udf.getArgumentsCount == 1 &&
          udf.getArguments(0).getExprTypeCase == proto.Expression.ExprTypeCase.UNRESOLVED_STAR
      case _ =>
        false
    }
  }

  private def transformTypedFilter(
      fun: proto.CommonInlineUserDefinedFunction,
      child: LogicalPlan): TypedFilter = {
    val udf = TypedScalaUdf(fun, Some(child.output))
    TypedFilter(udf.function, child)(udf.inEnc)
  }

  private def transformProject(rel: proto.Project): LogicalPlan = {
    val baseRel = if (rel.hasInput) {
      transformRelation(rel.getInput)
    } else {
      logical.OneRowRelation()
    }

    val projection = rel.getExpressionsList.asScala.toSeq
      .map(transformExpression)
      .map(toNamedExpression)

    logical.Project(projectList = projection, child = baseRel)
  }

  /**
   * Transforms an input protobuf expression into the Catalyst expression. This is usually not
   * called directly. Typically the planner will traverse the expressions automatically, only
   * plugins are expected to manually perform expression transformations.
   *
   * @param exp
   *   the input expression
   * @return
   *   Catalyst expression
   */
  def transformExpression(exp: proto.Expression): Expression = {
    exp.getExprTypeCase match {
      case proto.Expression.ExprTypeCase.LITERAL => transformLiteral(exp.getLiteral)
      case proto.Expression.ExprTypeCase.UNRESOLVED_ATTRIBUTE =>
        transformUnresolvedAttribute(exp.getUnresolvedAttribute)
      case proto.Expression.ExprTypeCase.UNRESOLVED_FUNCTION =>
        transformUnregisteredFunction(exp.getUnresolvedFunction)
          .getOrElse(transformUnresolvedFunction(exp.getUnresolvedFunction))
      case proto.Expression.ExprTypeCase.ALIAS => transformAlias(exp.getAlias)
      case proto.Expression.ExprTypeCase.EXPRESSION_STRING =>
        transformExpressionString(exp.getExpressionString)
      case proto.Expression.ExprTypeCase.UNRESOLVED_STAR =>
        transformUnresolvedStar(exp.getUnresolvedStar)
      case proto.Expression.ExprTypeCase.CAST => transformCast(exp.getCast)
      case proto.Expression.ExprTypeCase.UNRESOLVED_REGEX =>
        transformUnresolvedRegex(exp.getUnresolvedRegex)
      case proto.Expression.ExprTypeCase.UNRESOLVED_EXTRACT_VALUE =>
        transformUnresolvedExtractValue(exp.getUnresolvedExtractValue)
      case proto.Expression.ExprTypeCase.UPDATE_FIELDS =>
        transformUpdateFields(exp.getUpdateFields)
      case proto.Expression.ExprTypeCase.SORT_ORDER => transformSortOrder(exp.getSortOrder)
      case proto.Expression.ExprTypeCase.LAMBDA_FUNCTION =>
        transformLambdaFunction(exp.getLambdaFunction)
      case proto.Expression.ExprTypeCase.UNRESOLVED_NAMED_LAMBDA_VARIABLE =>
        transformUnresolvedNamedLambdaVariable(exp.getUnresolvedNamedLambdaVariable)
      case proto.Expression.ExprTypeCase.WINDOW =>
        transformWindowExpression(exp.getWindow)
      case proto.Expression.ExprTypeCase.EXTENSION =>
        transformExpressionPlugin(exp.getExtension)
      case proto.Expression.ExprTypeCase.COMMON_INLINE_USER_DEFINED_FUNCTION =>
        transformCommonInlineUserDefinedFunction(exp.getCommonInlineUserDefinedFunction)
      case proto.Expression.ExprTypeCase.CALL_FUNCTION =>
        transformCallFunction(exp.getCallFunction)
      case proto.Expression.ExprTypeCase.NAMED_ARGUMENT_EXPRESSION =>
        transformNamedArgumentExpression(exp.getNamedArgumentExpression)
      case _ =>
        throw InvalidPlanInput(
          s"Expression with ID: ${exp.getExprTypeCase.getNumber} is not supported")
    }
  }

  private def toNamedExpression(expr: Expression): NamedExpression = expr match {
    case named: NamedExpression => named
    case expr => UnresolvedAlias(expr)
  }

  private def transformUnresolvedAttribute(
      attr: proto.Expression.UnresolvedAttribute): UnresolvedAttribute = {
    val expr = UnresolvedAttribute.quotedString(attr.getUnparsedIdentifier)
    if (attr.hasPlanId) {
      expr.setTagValue(LogicalPlan.PLAN_ID_TAG, attr.getPlanId)
    }
    if (attr.hasIsMetadataColumn && attr.getIsMetadataColumn) {
      expr.setTagValue(LogicalPlan.IS_METADATA_COL, ())
    }
    expr
  }

  private def transformExpressionPlugin(extension: ProtoAny): Expression = {
    SparkConnectPluginRegistry.expressionRegistry
      // Lazily traverse the collection.
      .view
      // Apply the transformation.
      .map(p => p.transform(extension.toByteArray, this))
      // Find the first non-empty transformation or throw.
      .find(_.isPresent)
      .getOrElse(throw InvalidPlanInput("No handler found for extension"))
      .get
  }

  /**
   * Transforms the protocol buffers literals into the appropriate Catalyst literal expression.
   * @return
   *   Expression
   */
  private def transformLiteral(lit: proto.Expression.Literal): Literal = {
    LiteralExpressionProtoConverter.toCatalystExpression(lit)
  }

  private def transformLimit(limit: proto.Limit): LogicalPlan = {
    logical.Limit(
      limitExpr = expressions.Literal(limit.getLimit, IntegerType),
      transformRelation(limit.getInput))
  }

  private def transformTail(tail: proto.Tail): LogicalPlan = {
    logical.Tail(
      limitExpr = expressions.Literal(tail.getLimit, IntegerType),
      transformRelation(tail.getInput))
  }

  private def transformOffset(offset: proto.Offset): LogicalPlan = {
    logical.Offset(
      offsetExpr = expressions.Literal(offset.getOffset, IntegerType),
      transformRelation(offset.getInput))
  }

  /**
   * Translates a scalar function from proto to the Catalyst expression.
   */
  private def transformUnresolvedFunction(
      fun: proto.Expression.UnresolvedFunction): Expression = {
    if (fun.getIsUserDefinedFunction) {
      UnresolvedFunction(
        parser.parseFunctionIdentifier(fun.getFunctionName),
        fun.getArgumentsList.asScala.map(transformExpression).toSeq,
        isDistinct = fun.getIsDistinct)
    } else {
      UnresolvedFunction(
        FunctionIdentifier(fun.getFunctionName),
        fun.getArgumentsList.asScala.map(transformExpression).toSeq,
        isDistinct = fun.getIsDistinct)
    }
  }

  /**
   * Translates a user-defined function from proto to the Catalyst expression.
   *
   * @param fun
   *   Proto representation of the function call.
   * @return
   *   Expression.
   */
  private def transformCommonInlineUserDefinedFunction(
      fun: proto.CommonInlineUserDefinedFunction): Expression = {
    fun.getFunctionCase match {
      case proto.CommonInlineUserDefinedFunction.FunctionCase.PYTHON_UDF =>
        transformPythonFuncExpression(fun)
      case proto.CommonInlineUserDefinedFunction.FunctionCase.SCALAR_SCALA_UDF =>
        transformScalarScalaUDF(fun)
      case _ =>
        throw InvalidPlanInput(
          s"Function with ID: ${fun.getFunctionCase.getNumber} is not supported")
    }
  }

  /**
   * Translates a SQL function from proto to the Catalyst expression.
   *
   * @param fun
   *   Proto representation of the function call.
   * @return
   *   Expression.
   */
  private def transformCallFunction(fun: proto.CallFunction): Expression = {
    val funcName = fun.getFunctionName
    UnresolvedFunction(
      parser.parseMultipartIdentifier(funcName),
      fun.getArgumentsList.asScala.map(transformExpression).toSeq,
      false)
  }

  private def transformNamedArgumentExpression(
      namedArg: proto.NamedArgumentExpression): Expression = {
    NamedArgumentExpression(namedArg.getKey, transformExpression(namedArg.getValue))
  }

  private def unpackUdf(fun: proto.CommonInlineUserDefinedFunction): UdfPacket = {
    unpackScalarScalaUDF[UdfPacket](fun.getScalarScalaUdf)
  }

  private def unpackForeachWriter(fun: proto.ScalarScalaUDF): ForeachWriterPacket = {
    unpackScalarScalaUDF[ForeachWriterPacket](fun)
  }

  private def unpackScalarScalaUDF[T](fun: proto.ScalarScalaUDF): T = {
    try {
      logDebug(s"Unpack using class loader: ${Utils.getContextOrSparkClassLoader}")
      Utils.deserialize[T](fun.getPayload.toByteArray, Utils.getContextOrSparkClassLoader)
    } catch {
      case t: Throwable =>
        Throwables.getRootCause(t) match {
          case nsm: NoSuchMethodException =>
            throw new ClassNotFoundException(
              s"Failed to load class correctly due to $nsm. " +
                "Make sure the artifact where the class is defined is installed by calling" +
                " session.addArtifact.")
          case cnf: ClassNotFoundException =>
            throw new ClassNotFoundException(
              s"Failed to load class: ${cnf.getMessage}. " +
                "Make sure the artifact where the class is defined is installed by calling" +
                " session.addArtifact.")
          case _ => throw t
        }
    }
  }

  /**
   * Translates a Scalar Scala user-defined function from proto to the Catalyst expression.
   *
   * @param fun
   *   Proto representation of the Scalar Scalar user-defined function.
   * @return
   *   ScalaUDF.
   */
  private def transformScalarScalaUDF(fun: proto.CommonInlineUserDefinedFunction): ScalaUDF = {
    val udf = fun.getScalarScalaUdf
    val udfPacket = unpackUdf(fun)
    ScalaUDF(
      function = udfPacket.function,
      dataType = transformDataType(udf.getOutputType),
      children = fun.getArgumentsList.asScala.map(transformExpression).toSeq,
      inputEncoders = udfPacket.inputEncoders.map(e => Try(ExpressionEncoder(e)).toOption),
      outputEncoder = Option(ExpressionEncoder(udfPacket.outputEncoder)),
      udfName = Option(fun.getFunctionName),
      nullable = udf.getNullable,
      udfDeterministic = fun.getDeterministic)
  }

  private def transformScalarScalaFunction(
      fun: proto.CommonInlineUserDefinedFunction): SparkUserDefinedFunction = {
    val udf = fun.getScalarScalaUdf
    val udfPacket = unpackUdf(fun)
    SparkUserDefinedFunction(
      f = udfPacket.function,
      dataType = transformDataType(udf.getOutputType),
      inputEncoders = udfPacket.inputEncoders.map(e => Try(ExpressionEncoder(e)).toOption),
      outputEncoder = Option(ExpressionEncoder(udfPacket.outputEncoder)),
      name = Option(fun.getFunctionName),
      nullable = udf.getNullable,
      deterministic = fun.getDeterministic)
  }

  /**
   * Translates a Python user-defined function from proto to the Catalyst expression.
   *
   * @param fun
   *   Proto representation of the Python user-defined function.
   * @return
   *   PythonUDF.
   */
  private def transformPythonUDF(fun: proto.CommonInlineUserDefinedFunction): PythonUDF = {
    transformPythonFuncExpression(fun).asInstanceOf[PythonUDF]
  }

  private def transformPythonFuncExpression(
      fun: proto.CommonInlineUserDefinedFunction): Expression = {
    createUserDefinedPythonFunction(fun)
      .builder(fun.getArgumentsList.asScala.map(transformExpression).toSeq) match {
      case udaf: PythonUDAF => udaf.toAggregateExpression()
      case other => other
    }
  }

  private def createUserDefinedPythonFunction(
      fun: proto.CommonInlineUserDefinedFunction): UserDefinedPythonFunction = {
    val udf = fun.getPythonUdf
    val function = transformPythonFunction(udf)
    UserDefinedPythonFunction(
      name = fun.getFunctionName,
      func = function,
      dataType = transformDataType(udf.getOutputType),
      pythonEvalType = udf.getEvalType,
      udfDeterministic = fun.getDeterministic)
  }

  private def transformPythonFunction(fun: proto.PythonUDF): SimplePythonFunction = {
    SimplePythonFunction(
      command = fun.getCommand.toByteArray.toImmutableArraySeq,
      // Empty environment variables
      envVars = Maps.newHashMap(),
      pythonIncludes = sessionHolder.artifactManager.getPythonIncludes.asJava,
      pythonExec = pythonExec,
      pythonVer = fun.getPythonVer,
      // Empty broadcast variables
      broadcastVars = Lists.newArrayList(),
      // Accumulator if available
      accumulator = sessionHolder.pythonAccumulator.orNull)
  }

  /**
   * Translates a LambdaFunction from proto to the Catalyst expression.
   */
  private def transformLambdaFunction(lambda: proto.Expression.LambdaFunction): LambdaFunction = {
    if (lambda.getArgumentsCount == 0 || lambda.getArgumentsCount > 3) {
      throw InvalidPlanInput(
        "LambdaFunction requires 1 ~ 3 arguments, " +
          s"but got ${lambda.getArgumentsCount} ones!")
    }

    LambdaFunction(
      function = transformExpression(lambda.getFunction),
      arguments = lambda.getArgumentsList.asScala.toSeq
        .map(transformUnresolvedNamedLambdaVariable))
  }

  private def transformUnresolvedNamedLambdaVariable(
      variable: proto.Expression.UnresolvedNamedLambdaVariable): UnresolvedNamedLambdaVariable = {
    if (variable.getNamePartsCount == 0) {
      throw InvalidPlanInput("UnresolvedNamedLambdaVariable requires at least one name part!")
    }

    UnresolvedNamedLambdaVariable(variable.getNamePartsList.asScala.toSeq)
  }

  /**
   * For some reason, not all functions are registered in 'FunctionRegistry'. For a unregistered
   * function, we can still wrap it under the proto 'UnresolvedFunction', and then resolve it in
   * this method.
   */
  private def transformUnregisteredFunction(
      fun: proto.Expression.UnresolvedFunction): Option[Expression] = {
    fun.getFunctionName match {
      case "product" if fun.getArgumentsCount == 1 =>
        Some(
          aggregate
            .Product(transformExpression(fun.getArgumentsList.asScala.head))
            .toAggregateExpression())

      case "when" if fun.getArgumentsCount > 0 =>
        val children = fun.getArgumentsList.asScala.toSeq.map(transformExpression)
        Some(CaseWhen.createFromParser(children))

      case "in" if fun.getArgumentsCount > 0 =>
        val children = fun.getArgumentsList.asScala.toSeq.map(transformExpression)
        Some(In(children.head, children.tail))

      case "nth_value" if fun.getArgumentsCount == 3 =>
        // NthValue does not have a constructor which accepts Expression typed 'ignoreNulls'
        val children = fun.getArgumentsList.asScala.map(transformExpression)
        val ignoreNulls = extractBoolean(children(2), "ignoreNulls")
        Some(NthValue(children(0), children(1), ignoreNulls))

      case "like" if fun.getArgumentsCount == 3 =>
        // Like does not have a constructor which accepts Expression typed 'escapeChar'
        val children = fun.getArgumentsList.asScala.map(transformExpression)
        val escapeChar = extractString(children(2), "escapeChar")
        Some(Like(children(0), children(1), escapeChar.charAt(0)))

      case "ilike" if fun.getArgumentsCount == 3 =>
        // ILike does not have a constructor which accepts Expression typed 'escapeChar'
        val children = fun.getArgumentsList.asScala.map(transformExpression)
        val escapeChar = extractString(children(2), "escapeChar")
        Some(ILike(children(0), children(1), escapeChar.charAt(0)))

      case "lag" if fun.getArgumentsCount == 4 =>
        // Lag does not have a constructor which accepts Expression typed 'ignoreNulls'
        val children = fun.getArgumentsList.asScala.map(transformExpression)
        val ignoreNulls = extractBoolean(children(3), "ignoreNulls")
        Some(Lag(children.head, children(1), children(2), ignoreNulls))

      case "lead" if fun.getArgumentsCount == 4 =>
        // Lead does not have a constructor which accepts Expression typed 'ignoreNulls'
        val children = fun.getArgumentsList.asScala.map(transformExpression)
        val ignoreNulls = extractBoolean(children(3), "ignoreNulls")
        Some(Lead(children.head, children(1), children(2), ignoreNulls))

      case "bloom_filter_agg" if fun.getArgumentsCount == 3 =>
        // [col, expectedNumItems: Long, numBits: Long]
        val children = fun.getArgumentsList.asScala.map(transformExpression)
        Some(
          new BloomFilterAggregate(children(0), children(1), children(2))
            .toAggregateExpression())

      case "window" if Seq(2, 3, 4).contains(fun.getArgumentsCount) =>
        val children = fun.getArgumentsList.asScala.map(transformExpression)
        val timeCol = children.head
        val windowDuration = extractString(children(1), "windowDuration")
        var slideDuration = windowDuration
        if (fun.getArgumentsCount >= 3) {
          slideDuration = extractString(children(2), "slideDuration")
        }
        var startTime = "0 second"
        if (fun.getArgumentsCount == 4) {
          startTime = extractString(children(3), "startTime")
        }
        Some(
          Alias(TimeWindow(timeCol, windowDuration, slideDuration, startTime), "window")(
            nonInheritableMetadataKeys = Seq(Dataset.DATASET_ID_KEY, Dataset.COL_POS_KEY)))

      case "session_window" if fun.getArgumentsCount == 2 =>
        val children = fun.getArgumentsList.asScala.map(transformExpression)
        val timeCol = children.head
        val sessionWindow = children.last match {
          case Literal(s, StringType) if s != null => SessionWindow(timeCol, s.toString)
          case other => SessionWindow(timeCol, other)
        }
        Some(
          Alias(sessionWindow, "session_window")(nonInheritableMetadataKeys =
            Seq(Dataset.DATASET_ID_KEY, Dataset.COL_POS_KEY)))

      case "bucket" if fun.getArgumentsCount == 2 =>
        val children = fun.getArgumentsList.asScala.map(transformExpression)
        (children.head, children.last) match {
          case (numBuckets: Literal, child) if numBuckets.dataType == IntegerType =>
            Some(Bucket(numBuckets, child))
          case (other, _) =>
            throw InvalidPlanInput(s"numBuckets should be a literal integer, but got $other")
        }

      case "years" if fun.getArgumentsCount == 1 =>
        Some(Years(transformExpression(fun.getArguments(0))))

      case "months" if fun.getArgumentsCount == 1 =>
        Some(Months(transformExpression(fun.getArguments(0))))

      case "days" if fun.getArgumentsCount == 1 =>
        Some(Days(transformExpression(fun.getArguments(0))))

      case "hours" if fun.getArgumentsCount == 1 =>
        Some(Hours(transformExpression(fun.getArguments(0))))

      case "unwrap_udt" if fun.getArgumentsCount == 1 =>
        Some(UnwrapUDT(transformExpression(fun.getArguments(0))))

      case "from_json" if Seq(2, 3).contains(fun.getArgumentsCount) =>
        // JsonToStructs constructor doesn't accept JSON-formatted schema.
        val children = fun.getArgumentsList.asScala.map(transformExpression)

        var schema: DataType = null
        children(1) match {
          case Literal(s, StringType) if s != null =>
            try {
              schema = DataType.fromJson(s.toString)
            } catch {
              case _: Exception =>
            }
          case _ =>
        }

        if (schema != null) {
          var options = Map.empty[String, String]
          if (children.length == 3) {
            options = extractMapData(children(2), "Options")
          }
          Some(
            JsonToStructs(
              schema = CharVarcharUtils.failIfHasCharVarchar(schema),
              options = options,
              child = children.head))
        } else {
          None
        }

      // Avro-specific functions
      case "from_avro" if Seq(2, 3).contains(fun.getArgumentsCount) =>
        val children = fun.getArgumentsList.asScala.map(transformExpression)
        val jsonFormatSchema = extractString(children(1), "jsonFormatSchema")
        var options = Map.empty[String, String]
        if (fun.getArgumentsCount == 3) {
          options = extractMapData(children(2), "Options")
        }
        Some(AvroDataToCatalyst(children.head, jsonFormatSchema, options))

      case "to_avro" if Seq(1, 2).contains(fun.getArgumentsCount) =>
        val children = fun.getArgumentsList.asScala.map(transformExpression)
        var jsonFormatSchema = Option.empty[String]
        if (fun.getArgumentsCount == 2) {
          jsonFormatSchema = Some(extractString(children(1), "jsonFormatSchema"))
        }
        Some(CatalystDataToAvro(children.head, jsonFormatSchema))

      // PS(Pandas API on Spark)-specific functions
      case "distributed_sequence_id" if fun.getArgumentsCount == 0 =>
        Some(DistributedSequenceID())

      case "pandas_product" if fun.getArgumentsCount == 2 =>
        val children = fun.getArgumentsList.asScala.map(transformExpression)
        val dropna = extractBoolean(children(1), "dropna")
        Some(aggregate.PandasProduct(children(0), dropna).toAggregateExpression(false))

      case "pandas_stddev" if fun.getArgumentsCount == 2 =>
        val children = fun.getArgumentsList.asScala.map(transformExpression)
        val ddof = extractInteger(children(1), "ddof")
        Some(aggregate.PandasStddev(children(0), ddof).toAggregateExpression(false))

      case "pandas_skew" if fun.getArgumentsCount == 1 =>
        val children = fun.getArgumentsList.asScala.map(transformExpression)
        Some(aggregate.PandasSkewness(children(0)).toAggregateExpression(false))

      case "pandas_kurt" if fun.getArgumentsCount == 1 =>
        val children = fun.getArgumentsList.asScala.map(transformExpression)
        Some(aggregate.PandasKurtosis(children(0)).toAggregateExpression(false))

      case "pandas_var" if fun.getArgumentsCount == 2 =>
        val children = fun.getArgumentsList.asScala.map(transformExpression)
        val ddof = extractInteger(children(1), "ddof")
        Some(aggregate.PandasVariance(children(0), ddof).toAggregateExpression(false))

      case "pandas_covar" if fun.getArgumentsCount == 3 =>
        val children = fun.getArgumentsList.asScala.map(transformExpression)
        val ddof = extractInteger(children(2), "ddof")
        Some(aggregate.PandasCovar(children(0), children(1), ddof).toAggregateExpression(false))

      case "pandas_mode" if fun.getArgumentsCount == 2 =>
        val children = fun.getArgumentsList.asScala.map(transformExpression)
        val ignoreNA = extractBoolean(children(1), "ignoreNA")
        Some(aggregate.PandasMode(children(0), ignoreNA).toAggregateExpression(false))

      case "ewm" if fun.getArgumentsCount == 3 =>
        val children = fun.getArgumentsList.asScala.map(transformExpression)
        val alpha = extractDouble(children(1), "alpha")
        val ignoreNA = extractBoolean(children(2), "ignoreNA")
        Some(EWM(children(0), alpha, ignoreNA))

      case "null_index" if fun.getArgumentsCount == 1 =>
        val children = fun.getArgumentsList.asScala.map(transformExpression)
        Some(NullIndex(children(0)))

      case "timestampdiff" if fun.getArgumentsCount == 3 =>
        val children = fun.getArgumentsList.asScala.map(transformExpression)
        val unit = extractString(children(0), "unit")
        Some(TimestampDiff(unit, children(1), children(2)))

      // ML-specific functions
      case "vector_to_array" if fun.getArgumentsCount == 2 =>
        val expr = transformExpression(fun.getArguments(0))
        val dtype = extractString(transformExpression(fun.getArguments(1)), "dtype")
        dtype match {
          case "float64" =>
            Some(transformUnregisteredUDF(MLFunctions.vectorToArrayUdf, Seq(expr)))
          case "float32" =>
            Some(transformUnregisteredUDF(MLFunctions.vectorToArrayFloatUdf, Seq(expr)))
          case other =>
            throw InvalidPlanInput(s"Unsupported dtype: $other. Valid values: float64, float32.")
        }

      case "array_to_vector" if fun.getArgumentsCount == 1 =>
        val expr = transformExpression(fun.getArguments(0))
        Some(transformUnregisteredUDF(MLFunctions.arrayToVectorUdf, Seq(expr)))

      // Protobuf-specific functions
      case "from_protobuf" if Seq(2, 3, 4).contains(fun.getArgumentsCount) =>
        val children = fun.getArgumentsList.asScala.map(transformExpression)
        val (msgName, desc, options) = extractProtobufArgs(children.toSeq)
        Some(ProtobufDataToCatalyst(children(0), msgName, desc, options))

      case "to_protobuf" if Seq(2, 3, 4).contains(fun.getArgumentsCount) =>
        val children = fun.getArgumentsList.asScala.map(transformExpression)
        val (msgName, desc, options) = extractProtobufArgs(children.toSeq)
        Some(CatalystDataToProtobuf(children(0), msgName, desc, options))

      case "uuid" if fun.getArgumentsCount == 1 =>
        // Uuid does not have a constructor which accepts Expression typed 'seed'
        val children = fun.getArgumentsList.asScala.map(transformExpression)
        val seed = extractLong(children(0), "seed")
        Some(Uuid(Some(seed)))

      case "shuffle" if fun.getArgumentsCount == 2 =>
        // Shuffle does not have a constructor which accepts Expression typed 'seed'
        val children = fun.getArgumentsList.asScala.map(transformExpression)
        val seed = extractLong(children(1), "seed")
        Some(Shuffle(children(0), Some(seed)))

      case _ => None
    }
  }

  /**
   * There are some built-in yet not registered UDFs, for example, 'ml.function.array_to_vector'.
   * This method is to convert them to ScalaUDF expressions.
   */
  private def transformUnregisteredUDF(
      fun: org.apache.spark.sql.expressions.UserDefinedFunction,
      exprs: Seq[Expression]): ScalaUDF = {
    val f = fun.asInstanceOf[org.apache.spark.sql.expressions.SparkUserDefinedFunction]
    ScalaUDF(
      function = f.f,
      dataType = f.dataType,
      children = exprs,
      inputEncoders = f.inputEncoders,
      outputEncoder = f.outputEncoder,
      udfName = f.name,
      nullable = f.nullable,
      udfDeterministic = f.deterministic)
  }

  private def extractProtobufArgs(children: Seq[Expression]) = {
    val msgName = extractString(children(1), "MessageClassName")
    var desc = Option.empty[Array[Byte]]
    var options = Map.empty[String, String]
    if (children.length == 3) {
      children(2) match {
        case b: Literal => desc = Some(extractBinary(b, "binaryFileDescriptorSet"))
        case o => options = extractMapData(o, "options")
      }
    } else if (children.length == 4) {
      desc = Some(extractBinary(children(2), "binaryFileDescriptorSet"))
      options = extractMapData(children(3), "options")
    }
    (msgName, desc, options)
  }

  private def extractBoolean(expr: Expression, field: String): Boolean = expr match {
    case Literal(bool: Boolean, BooleanType) => bool
    case other => throw InvalidPlanInput(s"$field should be a literal boolean, but got $other")
  }

  private def extractDouble(expr: Expression, field: String): Double = expr match {
    case Literal(double: Double, DoubleType) => double
    case other => throw InvalidPlanInput(s"$field should be a literal double, but got $other")
  }

  private def extractInteger(expr: Expression, field: String): Int = expr match {
    case Literal(int: Int, IntegerType) => int
    case other => throw InvalidPlanInput(s"$field should be a literal integer, but got $other")
  }

  private def extractLong(expr: Expression, field: String): Long = expr match {
    case Literal(long: Long, LongType) => long
    case other => throw InvalidPlanInput(s"$field should be a literal long, but got $other")
  }

  private def extractString(expr: Expression, field: String): String = expr match {
    case Literal(s, StringType) if s != null => s.toString
    case other => throw InvalidPlanInput(s"$field should be a literal string, but got $other")
  }

  private def extractBinary(expr: Expression, field: String): Array[Byte] = expr match {
    case Literal(b: Array[Byte], BinaryType) if b != null => b
    case other => throw InvalidPlanInput(s"$field should be a literal binary, but got $other")
  }

  @scala.annotation.tailrec
  private def extractMapData(expr: Expression, field: String): Map[String, String] = expr match {
    case map: CreateMap => ExprUtils.convertToMapData(map)
    case UnresolvedFunction(Seq("map"), args, _, _, _, _) =>
      extractMapData(CreateMap(args), field)
    case other => throw InvalidPlanInput(s"$field should be created by map, but got $other")
  }

  private def transformAlias(alias: proto.Expression.Alias): NamedExpression = {
    if (alias.getNameCount == 1) {
      val metadata = if (alias.hasMetadata() && alias.getMetadata.nonEmpty) {
        Some(Metadata.fromJson(alias.getMetadata))
      } else {
        None
      }
      Alias(transformExpression(alias.getExpr), alias.getName(0))(explicitMetadata = metadata)
    } else {
      if (alias.hasMetadata) {
        throw InvalidPlanInput(
          "Alias expressions with more than 1 identifier must not use optional metadata.")
      }
      MultiAlias(transformExpression(alias.getExpr), alias.getNameList.asScala.toSeq)
    }
  }

  private def transformExpressionString(expr: proto.Expression.ExpressionString): Expression = {
    parser.parseExpression(expr.getExpression)
  }

  private def transformUnresolvedStar(star: proto.Expression.UnresolvedStar): Expression = {
    (star.hasUnparsedTarget, star.hasPlanId) match {
      case (false, false) =>
        // functions.col("*")
        UnresolvedStar(None)

      case (true, false) =>
        // functions.col("s.*")
        val target = star.getUnparsedTarget
        if (!target.endsWith(".*")) {
          throw InvalidPlanInput(
            s"UnresolvedStar requires a unparsed target ending with '.*', but got $target.")
        }
        val parts = UnresolvedAttribute.parseAttributeName(target.dropRight(2))
        UnresolvedStar(Some(parts))

      case (false, true) =>
        // dataframe.col("*")
        UnresolvedDataFrameStar(star.getPlanId)

      case _ =>
        throw InvalidPlanInput("UnresolvedStar with both target and plan id is not supported.")
    }
  }

  private def transformCast(cast: proto.Expression.Cast): Expression = {
    val dataType = cast.getCastToTypeCase match {
      case proto.Expression.Cast.CastToTypeCase.TYPE => transformDataType(cast.getType)
      case _ => parser.parseDataType(cast.getTypeStr)
    }
    val mode = cast.getEvalMode match {
      case proto.Expression.Cast.EvalMode.EVAL_MODE_LEGACY => Some(EvalMode.LEGACY)
      case proto.Expression.Cast.EvalMode.EVAL_MODE_ANSI => Some(EvalMode.ANSI)
      case proto.Expression.Cast.EvalMode.EVAL_MODE_TRY => Some(EvalMode.TRY)
      case _ => None
    }
    mode match {
      case Some(m) => Cast(transformExpression(cast.getExpr), dataType, None, m)
      case _ => Cast(transformExpression(cast.getExpr), dataType)
    }
  }

  private def transformUnresolvedRegex(regex: proto.Expression.UnresolvedRegex): Expression = {
    val caseSensitive = session.sessionState.conf.caseSensitiveAnalysis
    regex.getColName match {
      case ParserUtils.escapedIdentifier(columnNameRegex) =>
        UnresolvedRegex(columnNameRegex, None, caseSensitive)
      case ParserUtils.qualifiedEscapedIdentifier(nameParts, columnNameRegex) =>
        UnresolvedRegex(columnNameRegex, Some(nameParts), caseSensitive)
      case _ =>
        val expr = UnresolvedAttribute.quotedString(regex.getColName)
        if (regex.hasPlanId) {
          expr.setTagValue(LogicalPlan.PLAN_ID_TAG, regex.getPlanId)
        }
        expr
    }
  }

  private def transformUnresolvedExtractValue(
      extract: proto.Expression.UnresolvedExtractValue): UnresolvedExtractValue = {
    UnresolvedExtractValue(
      transformExpression(extract.getChild),
      transformExpression(extract.getExtraction))
  }

  private def transformUpdateFields(update: proto.Expression.UpdateFields): UpdateFields = {
    if (update.hasValueExpression) {
      // add or replace a field
      UpdateFields.apply(
        col = transformExpression(update.getStructExpression),
        fieldName = update.getFieldName,
        expr = transformExpression(update.getValueExpression))
    } else {
      // drop a field
      UpdateFields.apply(
        col = transformExpression(update.getStructExpression),
        fieldName = update.getFieldName)
    }
  }

  private def transformWindowExpression(window: proto.Expression.Window) = {
    if (!window.hasWindowFunction) {
      throw InvalidPlanInput(s"WindowFunction is required in WindowExpression")
    }

    val frameSpec = if (window.hasFrameSpec) {
      val protoFrameSpec = window.getFrameSpec

      val frameType = protoFrameSpec.getFrameType match {
        case proto.Expression.Window.WindowFrame.FrameType.FRAME_TYPE_ROW => RowFrame

        case proto.Expression.Window.WindowFrame.FrameType.FRAME_TYPE_RANGE => RangeFrame

        case other => throw InvalidPlanInput(s"Unknown FrameType $other")
      }

      if (!protoFrameSpec.hasLower) {
        throw InvalidPlanInput(s"LowerBound is required in WindowFrame")
      }
      val lower = protoFrameSpec.getLower.getBoundaryCase match {
        case proto.Expression.Window.WindowFrame.FrameBoundary.BoundaryCase.CURRENT_ROW =>
          CurrentRow

        case proto.Expression.Window.WindowFrame.FrameBoundary.BoundaryCase.UNBOUNDED =>
          UnboundedPreceding

        case proto.Expression.Window.WindowFrame.FrameBoundary.BoundaryCase.VALUE =>
          transformExpression(protoFrameSpec.getLower.getValue)

        case other => throw InvalidPlanInput(s"Unknown FrameBoundary $other")
      }

      if (!protoFrameSpec.hasUpper) {
        throw InvalidPlanInput(s"UpperBound is required in WindowFrame")
      }
      val upper = protoFrameSpec.getUpper.getBoundaryCase match {
        case proto.Expression.Window.WindowFrame.FrameBoundary.BoundaryCase.CURRENT_ROW =>
          CurrentRow

        case proto.Expression.Window.WindowFrame.FrameBoundary.BoundaryCase.UNBOUNDED =>
          UnboundedFollowing

        case proto.Expression.Window.WindowFrame.FrameBoundary.BoundaryCase.VALUE =>
          transformExpression(protoFrameSpec.getUpper.getValue)

        case other => throw InvalidPlanInput(s"Unknown FrameBoundary $other")
      }

      SpecifiedWindowFrame(frameType = frameType, lower = lower, upper = upper)

    } else {
      UnspecifiedFrame
    }

    val windowSpec = WindowSpecDefinition(
      partitionSpec = window.getPartitionSpecList.asScala.toSeq.map(transformExpression),
      orderSpec = window.getOrderSpecList.asScala.toSeq.map(transformSortOrder),
      frameSpecification = frameSpec)

    WindowExpression(
      windowFunction = transformExpression(window.getWindowFunction),
      windowSpec = windowSpec)
  }

  private def transformSetOperation(u: proto.SetOperation): LogicalPlan = {
    if (!u.hasLeftInput || !u.hasRightInput) {
      throw InvalidPlanInput("Set operation must have 2 inputs")
    }
    val leftPlan = transformRelation(u.getLeftInput)
    val rightPlan = transformRelation(u.getRightInput)
    val isAll = if (u.hasIsAll) u.getIsAll else false

    u.getSetOpType match {
      case proto.SetOperation.SetOpType.SET_OP_TYPE_EXCEPT =>
        if (u.getByName) {
          throw InvalidPlanInput("Except does not support union_by_name")
        }
        Except(leftPlan, rightPlan, isAll)
      case proto.SetOperation.SetOpType.SET_OP_TYPE_INTERSECT =>
        if (u.getByName) {
          throw InvalidPlanInput("Intersect does not support union_by_name")
        }
        Intersect(leftPlan, rightPlan, isAll)
      case proto.SetOperation.SetOpType.SET_OP_TYPE_UNION =>
        if (!u.getByName && u.getAllowMissingColumns) {
          throw InvalidPlanInput(
            "UnionByName `allowMissingCol` can be true only if `byName` is true.")
        }
        val union = Union(Seq(leftPlan, rightPlan), u.getByName, u.getAllowMissingColumns)
        if (isAll) {
          union
        } else {
          logical.Distinct(union)
        }

      case _ =>
        throw InvalidPlanInput(s"Unsupported set operation ${u.getSetOpTypeValue}")
    }
  }

  private def transformJoinWith(rel: proto.Join): LogicalPlan = {
    val joined =
      session.sessionState.executePlan(transformJoin(rel)).analyzed.asInstanceOf[logical.Join]

    JoinWith.typedJoinWith(
      joined,
      session.sessionState.conf.dataFrameSelfJoinAutoResolveAmbiguity,
      session.sessionState.analyzer.resolver,
      rel.getJoinDataType.getIsLeftStruct,
      rel.getJoinDataType.getIsRightStruct)
  }

  private def transformJoinOrJoinWith(rel: proto.Join): LogicalPlan = {
    if (rel.hasJoinDataType) {
      transformJoinWith(rel)
    } else {
      transformJoin(rel)
    }
  }

  private def transformJoin(rel: proto.Join): LogicalPlan = {
    assert(rel.hasLeft && rel.hasRight, "Both join sides must be present")
    if (rel.hasJoinCondition && rel.getUsingColumnsCount > 0) {
      throw InvalidPlanInput(
        s"Using columns or join conditions cannot be set at the same time in Join")
    }
    val joinCondition =
      if (rel.hasJoinCondition) Some(transformExpression(rel.getJoinCondition)) else None
    val catalystJointype = transformJoinType(
      if (rel.getJoinType != null) rel.getJoinType else proto.Join.JoinType.JOIN_TYPE_INNER)
    val joinType = if (rel.getUsingColumnsCount > 0) {
      UsingJoin(catalystJointype, rel.getUsingColumnsList.asScala.toSeq)
    } else {
      catalystJointype
    }
    logical.Join(
      left = transformRelation(rel.getLeft),
      right = transformRelation(rel.getRight),
      joinType = joinType,
      condition = joinCondition,
      hint = logical.JoinHint.NONE)
  }

  private def transformJoinType(t: proto.Join.JoinType): JoinType = {
    t match {
      case proto.Join.JoinType.JOIN_TYPE_INNER => Inner
      case proto.Join.JoinType.JOIN_TYPE_LEFT_ANTI => LeftAnti
      case proto.Join.JoinType.JOIN_TYPE_FULL_OUTER => FullOuter
      case proto.Join.JoinType.JOIN_TYPE_LEFT_OUTER => LeftOuter
      case proto.Join.JoinType.JOIN_TYPE_RIGHT_OUTER => RightOuter
      case proto.Join.JoinType.JOIN_TYPE_LEFT_SEMI => LeftSemi
      case proto.Join.JoinType.JOIN_TYPE_CROSS => Cross
      case _ => throw InvalidPlanInput(s"Join type ${t} is not supported")
    }
  }

  private def transformAsOfJoin(rel: proto.AsOfJoin): LogicalPlan = {
    val left = Dataset.ofRows(session, transformRelation(rel.getLeft))
    val right = Dataset.ofRows(session, transformRelation(rel.getRight))
    val leftAsOf = Column(transformExpression(rel.getLeftAsOf))
    val rightAsOf = Column(transformExpression(rel.getRightAsOf))
    val joinType = rel.getJoinType
    val tolerance = if (rel.hasTolerance) Column(transformExpression(rel.getTolerance)) else null
    val allowExactMatches = rel.getAllowExactMatches
    val direction = rel.getDirection

    val joined = if (rel.getUsingColumnsCount > 0) {
      val usingColumns = rel.getUsingColumnsList.asScala.toSeq
      left.joinAsOf(
        other = right,
        leftAsOf = leftAsOf,
        rightAsOf = rightAsOf,
        usingColumns = usingColumns,
        joinType = joinType,
        tolerance = tolerance,
        allowExactMatches = allowExactMatches,
        direction = direction)
    } else {
      val joinExprs = if (rel.hasJoinExpr) Column(transformExpression(rel.getJoinExpr)) else null
      left.joinAsOf(
        other = right,
        leftAsOf = leftAsOf,
        rightAsOf = rightAsOf,
        joinExprs = joinExprs,
        joinType = joinType,
        tolerance = tolerance,
        allowExactMatches = allowExactMatches,
        direction = direction)
    }
    joined.logicalPlan
  }

  private def transformSort(sort: proto.Sort): LogicalPlan = {
    assert(sort.getOrderCount > 0, "'order' must be present and contain elements.")
    logical.Sort(
      child = transformRelation(sort.getInput),
      global = sort.getIsGlobal,
      order = sort.getOrderList.asScala.toSeq.map(transformSortOrder))
  }

  private def transformSortOrder(order: proto.Expression.SortOrder) = {
    expressions.SortOrder(
      child = transformExpression(order.getChild),
      direction = order.getDirection match {
        case proto.Expression.SortOrder.SortDirection.SORT_DIRECTION_ASCENDING =>
          expressions.Ascending
        case _ => expressions.Descending
      },
      nullOrdering = order.getNullOrdering match {
        case proto.Expression.SortOrder.NullOrdering.SORT_NULLS_FIRST =>
          expressions.NullsFirst
        case _ => expressions.NullsLast
      },
      sameOrderExpressions = Seq.empty)
  }

  private def transformDrop(rel: proto.Drop): LogicalPlan = {
    var output = Dataset.ofRows(session, transformRelation(rel.getInput))
    if (rel.getColumnsCount > 0) {
      val cols = rel.getColumnsList.asScala.toSeq.map(expr => Column(transformExpression(expr)))
      output = output.drop(cols.head, cols.tail: _*)
    }
    if (rel.getColumnNamesCount > 0) {
      val colNames = rel.getColumnNamesList.asScala.toSeq
      output = output.drop(colNames: _*)
    }
    output.logicalPlan
  }

  private def transformAggregate(rel: proto.Aggregate): LogicalPlan = {
    rel.getGroupType match {
      case proto.Aggregate.GroupType.GROUP_TYPE_GROUPBY
          // This relies on the assumption that a KVGDS always requires the head to be a Typed UDF.
          // This is the case for datasets created via groupByKey,
          // and also via RelationalGroupedDS#as, as the first is a dummy UDF currently.
          if rel.getGroupingExpressionsList.size() >= 1 &&
            isTypedScalaUdfExpr(rel.getGroupingExpressionsList.get(0)) =>
        transformKeyValueGroupedAggregate(rel)
      case _ =>
        transformRelationalGroupedAggregate(rel)
    }
  }

  private def transformKeyValueGroupedAggregate(rel: proto.Aggregate): LogicalPlan = {
    val input = transformRelation(rel.getInput)
    val ds = UntypedKeyValueGroupedDataset(input, rel.getGroupingExpressionsList, Seq.empty)

    val keyColumn = TypedAggUtils.aggKeyColumn(ds.kEncoder, ds.groupingAttributes)
    val namedColumns = rel.getAggregateExpressionsList.asScala.toSeq
      .map(expr => transformExpressionWithTypedReduceExpression(expr, input))
      .map(toNamedExpression)
    logical.Aggregate(ds.groupingAttributes, keyColumn +: namedColumns, ds.analyzed)
  }

  private def transformRelationalGroupedAggregate(rel: proto.Aggregate): LogicalPlan = {
    if (!rel.hasInput) {
      throw InvalidPlanInput("Aggregate needs a plan input")
    }
    val input = transformRelation(rel.getInput)

    val groupingExprs = rel.getGroupingExpressionsList.asScala.toSeq.map(transformExpression)
    val aggExprs = rel.getAggregateExpressionsList.asScala.toSeq
      .map(expr => transformExpressionWithTypedReduceExpression(expr, input))
    val aliasedAgg = (groupingExprs ++ aggExprs).map(toNamedExpression)

    rel.getGroupType match {
      case proto.Aggregate.GroupType.GROUP_TYPE_GROUPBY =>
        logical.Aggregate(
          groupingExpressions = groupingExprs,
          aggregateExpressions = aliasedAgg,
          child = input)

      case proto.Aggregate.GroupType.GROUP_TYPE_ROLLUP =>
        logical.Aggregate(
          groupingExpressions = Seq(Rollup(groupingExprs.map(Seq(_)))),
          aggregateExpressions = aliasedAgg,
          child = input)

      case proto.Aggregate.GroupType.GROUP_TYPE_CUBE =>
        logical.Aggregate(
          groupingExpressions = Seq(Cube(groupingExprs.map(Seq(_)))),
          aggregateExpressions = aliasedAgg,
          child = input)

      case proto.Aggregate.GroupType.GROUP_TYPE_PIVOT =>
        if (!rel.hasPivot) {
          throw InvalidPlanInput("Aggregate with GROUP_TYPE_PIVOT requires a Pivot")
        }

        val pivotExpr = transformExpression(rel.getPivot.getCol)

        var valueExprs = rel.getPivot.getValuesList.asScala.toSeq.map(transformLiteral)
        if (valueExprs.isEmpty) {
          // This is to prevent unintended OOM errors when the number of distinct values is large
          val maxValues = session.sessionState.conf.dataFramePivotMaxValues
          // Get the distinct values of the column and sort them so its consistent
          val pivotCol = Column(pivotExpr)
          valueExprs = Dataset
            .ofRows(session, input)
            .select(pivotCol)
            .distinct()
            .limit(maxValues + 1)
            .sort(pivotCol) // ensure that the output columns are in a consistent logical order
            .collect()
            .map(_.get(0))
            .toImmutableArraySeq
            .map(expressions.Literal.apply)
        }

        logical.Pivot(
          groupByExprsOpt = Some(groupingExprs.map(toNamedExpression)),
          pivotColumn = pivotExpr,
          pivotValues = valueExprs,
          aggregates = aggExprs,
          child = input)

      case proto.Aggregate.GroupType.GROUP_TYPE_GROUPING_SETS =>
        val groupingSetsExprs = rel.getGroupingSetsList.asScala.toSeq.map { getGroupingSets =>
          getGroupingSets.getGroupingSetList.asScala.toSeq.map(transformExpression)
        }
        logical.Aggregate(
          groupingExpressions = Seq(
            GroupingSets(
              groupingSets = groupingSetsExprs,
              userGivenGroupByExprs = groupingExprs)),
          aggregateExpressions = aliasedAgg,
          child = input)
      case other => throw InvalidPlanInput(s"Unknown Group Type $other")
    }
  }

  private def transformTypedReduceExpression(
      fun: proto.Expression.UnresolvedFunction,
      dataAttributes: Seq[Attribute]): Expression = {
    assert(fun.getFunctionName == "reduce")
    if (fun.getArgumentsCount != 1) {
      throw InvalidPlanInput("reduce requires single child expression")
    }
    val udf = fun.getArgumentsList.asScala.map(transformExpression) match {
      case collection.Seq(f: ScalaUDF) =>
        f
      case other =>
        throw InvalidPlanInput(s"reduce should carry a scalar scala udf, but got $other")
    }
    assert(udf.outputEncoder.isDefined)
    val tEncoder = udf.outputEncoder.get // (T, T) => T
    val reduce = ReduceAggregator(udf.function)(tEncoder).toColumn.expr
    TypedAggUtils.withInputType(reduce, tEncoder, dataAttributes)
  }

  private def transformExpressionWithTypedReduceExpression(
      expr: proto.Expression,
      plan: LogicalPlan): Expression = {
    expr.getExprTypeCase match {
      case proto.Expression.ExprTypeCase.UNRESOLVED_FUNCTION
          if expr.getUnresolvedFunction.getFunctionName == "reduce" =>
        // The reduce func needs the input data attribute, thus handle it specially here
        transformTypedReduceExpression(expr.getUnresolvedFunction, plan.output)
      case _ => transformExpression(expr)
    }
  }

  def process(
      command: proto.Command,
      responseObserver: StreamObserver[ExecutePlanResponse]): Unit = {
    command.getCommandTypeCase match {
      case proto.Command.CommandTypeCase.REGISTER_FUNCTION =>
        handleRegisterUserDefinedFunction(command.getRegisterFunction)
      case proto.Command.CommandTypeCase.REGISTER_TABLE_FUNCTION =>
        handleRegisterUserDefinedTableFunction(command.getRegisterTableFunction)
      case proto.Command.CommandTypeCase.REGISTER_DATA_SOURCE =>
        handleRegisterUserDefinedDataSource(command.getRegisterDataSource)
      case proto.Command.CommandTypeCase.WRITE_OPERATION =>
        handleWriteOperation(command.getWriteOperation)
      case proto.Command.CommandTypeCase.CREATE_DATAFRAME_VIEW =>
        handleCreateViewCommand(command.getCreateDataframeView)
      case proto.Command.CommandTypeCase.WRITE_OPERATION_V2 =>
        handleWriteOperationV2(command.getWriteOperationV2)
      case proto.Command.CommandTypeCase.EXTENSION =>
        handleCommandPlugin(command.getExtension)
      case proto.Command.CommandTypeCase.SQL_COMMAND =>
        handleSqlCommand(command.getSqlCommand, responseObserver)
      case proto.Command.CommandTypeCase.WRITE_STREAM_OPERATION_START =>
        handleWriteStreamOperationStart(command.getWriteStreamOperationStart, responseObserver)
      case proto.Command.CommandTypeCase.STREAMING_QUERY_COMMAND =>
        handleStreamingQueryCommand(command.getStreamingQueryCommand, responseObserver)
      case proto.Command.CommandTypeCase.STREAMING_QUERY_MANAGER_COMMAND =>
        handleStreamingQueryManagerCommand(
          command.getStreamingQueryManagerCommand,
          responseObserver)
      case proto.Command.CommandTypeCase.GET_RESOURCES_COMMAND =>
        handleGetResourcesCommand(responseObserver)
      case proto.Command.CommandTypeCase.CREATE_RESOURCE_PROFILE_COMMAND =>
        handleCreateResourceProfileCommand(
          command.getCreateResourceProfileCommand,
          responseObserver)

      case _ => throw new UnsupportedOperationException(s"$command not supported.")
    }
  }

  private def handleSqlCommand(
      getSqlCommand: SqlCommand,
      responseObserver: StreamObserver[ExecutePlanResponse]): Unit = {
    // Eagerly execute commands of the provided SQL string.
    val args = getSqlCommand.getArgsMap
    val namedArguments = getSqlCommand.getNamedArgumentsMap
    val posArgs = getSqlCommand.getPosArgsList
    val posArguments = getSqlCommand.getPosArgumentsList
    val tracker = executeHolder.eventsManager.createQueryPlanningTracker()
    val df = if (!namedArguments.isEmpty) {
      session.sql(
        getSqlCommand.getSql,
        namedArguments.asScala.toMap.transform((_, e) => Column(transformExpression(e))),
        tracker)
    } else if (!posArguments.isEmpty) {
      session.sql(
        getSqlCommand.getSql,
        posArguments.asScala.map(e => Column(transformExpression(e))).toArray,
        tracker)
    } else if (!args.isEmpty) {
      session.sql(
        getSqlCommand.getSql,
        args.asScala.toMap.transform((_, v) => transformLiteral(v)),
        tracker)
    } else if (!posArgs.isEmpty) {
      session.sql(getSqlCommand.getSql, posArgs.asScala.map(transformLiteral).toArray, tracker)
    } else {
      session.sql(getSqlCommand.getSql, Map.empty[String, Any], tracker)
    }
    // Check if commands have been executed.
    val isCommand = df.queryExecution.commandExecuted.isInstanceOf[CommandResult]
    val rows = df.logicalPlan match {
      case lr: LocalRelation => lr.data
      case cr: CommandResult => cr.rows
      case _ => Seq.empty
    }

    // To avoid explicit handling of the result on the client, we build the expected input
    // of the relation on the server. The client has to simply forward the result.
    val result = SqlCommandResult.newBuilder()
    // Only filled when isCommand
    val metrics = ExecutePlanResponse.Metrics.newBuilder()
    if (isCommand) {
      // Convert the results to Arrow.
      val schema = df.schema
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
          rowIter = rows.iterator,
          schema = schema,
          maxRecordsPerBatch = -1,
          maxEstimatedBatchSize = maxBatchSize,
          timeZoneId = timeZoneId,
          errorOnDuplicatedFieldNames = false)
        assert(batches.hasNext)
        val bytes = batches.next()
        assert(!batches.hasNext, s"remaining batches: ${batches.size}")
        bytes
      }

      result.setRelation(
        proto.Relation
          .newBuilder()
          .setLocalRelation(
            proto.LocalRelation
              .newBuilder()
              .setData(ByteString.copyFrom(bytes))))
      metrics.addAllMetrics(MetricGenerator.transformPlan(df).asJava)
    } else {
      // No execution triggered for relations. Manually set ready
      tracker.setReadyForExecution()
      result.setRelation(
        proto.Relation
          .newBuilder()
          .setSql(
            proto.SQL
              .newBuilder()
              .setQuery(getSqlCommand.getSql)
              .putAllNamedArguments(getSqlCommand.getNamedArgumentsMap)
              .addAllPosArguments(getSqlCommand.getPosArgumentsList)
              .putAllArgs(getSqlCommand.getArgsMap)
              .addAllPosArgs(getSqlCommand.getPosArgsList)))
    }
    executeHolder.eventsManager.postFinished(Some(rows.size))
    // Exactly one SQL Command Result Batch
    responseObserver.onNext(
      ExecutePlanResponse
        .newBuilder()
        .setSessionId(sessionId)
        .setServerSideSessionId(sessionHolder.serverSessionId)
        .setSqlCommandResult(result)
        .build())

    // Send Metrics when isCommand (i.e. show tables) which is eagerly executed & has metrics
    // Skip metrics when !isCommand (i.e. select 1) which is not executed & doesn't have metrics
    if (isCommand) {
      responseObserver.onNext(
        ExecutePlanResponse
          .newBuilder()
          .setSessionId(sessionHolder.sessionId)
          .setServerSideSessionId(sessionHolder.serverSessionId)
          .setMetrics(metrics.build)
          .build)
    }
  }

  private def handleRegisterUserDefinedFunction(
      fun: proto.CommonInlineUserDefinedFunction): Unit = {
    fun.getFunctionCase match {
      case proto.CommonInlineUserDefinedFunction.FunctionCase.PYTHON_UDF =>
        handleRegisterPythonUDF(fun)
      case proto.CommonInlineUserDefinedFunction.FunctionCase.JAVA_UDF =>
        handleRegisterJavaUDF(fun)
      case proto.CommonInlineUserDefinedFunction.FunctionCase.SCALAR_SCALA_UDF =>
        handleRegisterScalarScalaUDF(fun)
      case _ =>
        throw InvalidPlanInput(
          s"Function with ID: ${fun.getFunctionCase.getNumber} is not supported")
    }
    executeHolder.eventsManager.postFinished()
  }

  private def handleRegisterUserDefinedTableFunction(
      fun: proto.CommonInlineUserDefinedTableFunction): Unit = {
    fun.getFunctionCase match {
      case proto.CommonInlineUserDefinedTableFunction.FunctionCase.PYTHON_UDTF =>
        val function = createPythonUserDefinedTableFunction(fun)
        session.udtf.registerPython(fun.getFunctionName, function)
      case _ =>
        throw InvalidPlanInput(
          s"Function with ID: ${fun.getFunctionCase.getNumber} is not supported")
    }
    executeHolder.eventsManager.postFinished()
  }

  private def handleRegisterUserDefinedDataSource(
      fun: proto.CommonInlineUserDefinedDataSource): Unit = {
    fun.getDataSourceCase match {
      case proto.CommonInlineUserDefinedDataSource.DataSourceCase.PYTHON_DATA_SOURCE =>
        val ds = fun.getPythonDataSource
        val dataSource = UserDefinedPythonDataSource(transformPythonDataSource(ds))
        session.dataSource.registerPython(fun.getName, dataSource)
      case _ =>
        throw InvalidPlanInput(
          s"Data source with ID: ${fun.getDataSourceCase.getNumber} is not supported")
    }
    executeHolder.eventsManager.postFinished()
  }

  private def createPythonUserDefinedTableFunction(
      fun: proto.CommonInlineUserDefinedTableFunction): UserDefinedPythonTableFunction = {
    val udtf = fun.getPythonUdtf
    val returnType = if (udtf.hasReturnType) {
      transformDataType(udtf.getReturnType) match {
        case s: StructType => Some(s)
        case dt =>
          throw InvalidPlanInput(
            "Invalid Python user-defined table function return type. " +
              s"Expect a struct type, but got ${dt.typeName}.")
      }
    } else {
      None
    }

    UserDefinedPythonTableFunction(
      name = fun.getFunctionName,
      func = transformPythonTableFunction(udtf),
      returnType = returnType,
      pythonEvalType = udtf.getEvalType,
      udfDeterministic = fun.getDeterministic)
  }

  private def handleRegisterPythonUDF(fun: proto.CommonInlineUserDefinedFunction): Unit = {
    val udpf = createUserDefinedPythonFunction(fun)
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

  private def handleRegisterScalarScalaUDF(fun: proto.CommonInlineUserDefinedFunction): Unit = {
    val udf = transformScalarScalaFunction(fun)
    session.udf.register(fun.getFunctionName, udf)
  }

  private def handleCommandPlugin(extension: ProtoAny): Unit = {
    SparkConnectPluginRegistry.commandRegistry
      // Lazily traverse the collection.
      .view
      // Apply the transformation.
      .map(p => p.process(extension.toByteArray, this))
      // Find the first non-empty transformation or throw.
      .find(_ == true)
      .getOrElse(throw InvalidPlanInput("No handler found for extension"))
    executeHolder.eventsManager.postFinished()
  }

  private def handleCreateViewCommand(createView: proto.CreateDataFrameViewCommand): Unit = {
    val viewType = if (createView.getIsGlobal) GlobalTempView else LocalTempView

    val tableIdentifier =
      try {
        parser.parseTableIdentifier(createView.getName)
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

    val tracker = executeHolder.eventsManager.createQueryPlanningTracker()
    Dataset.ofRows(session, plan, tracker).queryExecution.commandExecuted
    executeHolder.eventsManager.postFinished()
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
    val tracker = executeHolder.eventsManager.createQueryPlanningTracker()
    val dataset = Dataset.ofRows(session, plan, tracker)

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
    executeHolder.eventsManager.postFinished()
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
    val tracker = executeHolder.eventsManager.createQueryPlanningTracker()
    val dataset = Dataset.ofRows(session, plan, tracker)

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
    executeHolder.eventsManager.postFinished()
  }

  private def handleWriteStreamOperationStart(
      writeOp: WriteStreamOperationStart,
      responseObserver: StreamObserver[ExecutePlanResponse]): Unit = {
    val plan = transformRelation(writeOp.getInput)
    val tracker = executeHolder.eventsManager.createQueryPlanningTracker()
    val dataset = Dataset.ofRows(session, plan, tracker)
    // Call manually as writeStream does not trigger ReadyForExecution
    tracker.setReadyForExecution()

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
      if (writeOp.getForeachWriter.hasPythonFunction) {
        val foreach = writeOp.getForeachWriter.getPythonFunction
        val pythonFcn = transformPythonFunction(foreach)
        writer.foreachImplementation(
          new PythonForeachWriter(pythonFcn, dataset.schema).asInstanceOf[ForeachWriter[Any]])
      } else {
        val foreachWriterPkt = unpackForeachWriter(writeOp.getForeachWriter.getScalaFunction)
        val clientWriter = foreachWriterPkt.foreachWriter
        val encoder: Option[ExpressionEncoder[Any]] = Try(
          ExpressionEncoder(
            foreachWriterPkt.datasetEncoder.asInstanceOf[AgnosticEncoder[Any]])).toOption
        writer.foreachImplementation(clientWriter.asInstanceOf[ForeachWriter[Any]], encoder)
      }
    }

    // This is filled when a foreach batch runner started for Python.
    var foreachBatchRunnerCleaner: Option[AutoCloseable] = None

    if (writeOp.hasForeachBatch) {
      val foreachBatchFn = writeOp.getForeachBatch.getFunctionCase match {
        case StreamingForeachFunction.FunctionCase.PYTHON_FUNCTION =>
          val pythonFn = transformPythonFunction(writeOp.getForeachBatch.getPythonFunction)
          val (fn, cleaner) =
            StreamingForeachBatchHelper.pythonForeachBatchWrapper(pythonFn, sessionHolder)
          foreachBatchRunnerCleaner = Some(cleaner)
          fn

        case StreamingForeachFunction.FunctionCase.SCALA_FUNCTION =>
          val scalaFn = Utils.deserialize[StreamingForeachBatchHelper.ForeachBatchFnType](
            writeOp.getForeachBatch.getScalaFunction.getPayload.toByteArray,
            Utils.getContextOrSparkClassLoader)
          StreamingForeachBatchHelper.scalaForeachBatchWrapper(scalaFn, sessionHolder)

        case StreamingForeachFunction.FunctionCase.FUNCTION_NOT_SET =>
          throw InvalidPlanInput("Unexpected foreachBatch function") // Unreachable
      }

      writer.foreachBatch(foreachBatchFn)
    }

    val query =
      try {
        writeOp.getPath match {
          case "" if writeOp.hasTableName => writer.toTable(writeOp.getTableName)
          case "" => writer.start()
          case path => writer.start(path)
        }
      } catch {
        case NonFatal(ex) => // Failed to start the query, clean up foreach runner if any.
          logInfo(s"Removing foreachBatch worker, query failed to start for session $sessionId.")
          foreachBatchRunnerCleaner.foreach(_.close())
          throw ex
      }

    // Register the new query so that its reference is cached and is stopped on session timeout.
    SparkConnectService.streamingSessionManager.registerNewStreamingQuery(sessionHolder, query)
    // Register the runner with the query if Python foreachBatch is enabled.
    foreachBatchRunnerCleaner.foreach { cleaner =>
      sessionHolder.streamingForeachBatchRunnerCleanerCache.registerCleanerForQuery(
        query,
        cleaner)
    }
    executeHolder.eventsManager.postFinished()

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

    responseObserver.onNext(
      ExecutePlanResponse
        .newBuilder()
        .setSessionId(sessionId)
        .setServerSideSessionId(sessionHolder.serverSessionId)
        .setWriteStreamOperationStartResult(result)
        .build())
  }

  private def handleStreamingQueryCommand(
      command: StreamingQueryCommand,
      responseObserver: StreamObserver[ExecutePlanResponse]): Unit = {

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
          query.recentProgress.toImmutableArraySeq
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
          // Throw StreamingQueryException directly and rely on error translation on the
          // client-side to reconstruct the exception. Keep the remaining implementation
          // for backward-compatibility
          if (!command.getException) {
            throw result.get
          }
          val e = result.get
          val exception_builder = StreamingQueryCommandResult.ExceptionResult
            .newBuilder()
          exception_builder
            .setExceptionMessage(e.toString())
            .setErrorClass(e.getErrorClass)

          val stackTrace = Option(ExceptionUtils.getStackTrace(e))
          stackTrace.foreach { s =>
            exception_builder.setStackTrace(s)
          }
          respBuilder.setException(exception_builder.build())
        }

      case StreamingQueryCommand.CommandCase.AWAIT_TERMINATION =>
        val timeout = if (command.getAwaitTermination.hasTimeoutMs) {
          Some(command.getAwaitTermination.getTimeoutMs)
        } else {
          None
        }
        val terminated = handleStreamingAwaitTermination(query, timeout)
        respBuilder.getAwaitTerminationBuilder.setTerminated(terminated)

      case StreamingQueryCommand.CommandCase.COMMAND_NOT_SET =>
        throw new IllegalArgumentException("Missing command in StreamingQueryCommand")
    }

    executeHolder.eventsManager.postFinished()
    responseObserver.onNext(
      ExecutePlanResponse
        .newBuilder()
        .setSessionId(sessionId)
        .setServerSideSessionId(sessionHolder.serverSessionId)
        .setStreamingQueryCommandResult(respBuilder.build())
        .build())
  }

  /**
   * A helper function to handle streaming awaitTermination(). awaitTermination() can be a long
   * running command. In this function, we periodically check if the RPC call has been cancelled.
   * If so, we can stop the operation and release resources early.
   * @param query
   *   the query waits to be terminated
   * @param timeoutOptionMs
   *   optional. Timeout to wait for termination. If None, no timeout is set
   * @return
   *   if the query has terminated
   */
  private def handleStreamingAwaitTermination(
      query: StreamingQuery,
      timeoutOptionMs: Option[Long]): Boolean = {
    // How often to check if RPC is cancelled and call awaitTermination()
    val awaitTerminationIntervalMs = 10000
    val startTimeMs = System.currentTimeMillis()

    val timeoutTotalMs = timeoutOptionMs.getOrElse(Long.MaxValue)
    var timeoutLeftMs = timeoutTotalMs
    require(timeoutLeftMs > 0, "Timeout has to be positive")

    val grpcContext = Context.current
    while (!grpcContext.isCancelled) {
      val awaitTimeMs = math.min(awaitTerminationIntervalMs, timeoutLeftMs)

      val terminated = query.awaitTermination(awaitTimeMs)
      if (terminated) {
        return true
      }

      timeoutLeftMs = timeoutTotalMs - (System.currentTimeMillis() - startTimeMs)
      if (timeoutLeftMs <= 0) {
        return false
      }
    }

    // gRPC is cancelled
    logWarning("RPC context is cancelled when executing awaitTermination()")
    throw new StatusRuntimeException(Status.CANCELLED)
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

  private def handleStreamingQueryManagerCommand(
      command: StreamingQueryManagerCommand,
      responseObserver: StreamObserver[ExecutePlanResponse]): Unit = {
    val respBuilder = StreamingQueryManagerCommandResult.newBuilder()

    command.getCommandCase match {
      case StreamingQueryManagerCommand.CommandCase.ACTIVE =>
        val active_queries = session.streams.active
        respBuilder.getActiveBuilder.addAllActiveQueries(
          active_queries
            .map(query => buildStreamingQueryInstance(query))
            .toImmutableArraySeq
            .asJava)

      case StreamingQueryManagerCommand.CommandCase.GET_QUERY =>
        Option(session.streams.get(command.getGetQuery)).foreach { q =>
          respBuilder.setQuery(buildStreamingQueryInstance(q))
        }

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

      case StreamingQueryManagerCommand.CommandCase.ADD_LISTENER =>
        val listener = if (command.getAddListener.hasPythonListenerPayload) {
          new PythonStreamingQueryListener(
            transformPythonFunction(command.getAddListener.getPythonListenerPayload),
            sessionHolder)
        } else {
          val listenerPacket = Utils
            .deserialize[StreamingListenerPacket](
              command.getAddListener.getListenerPayload.toByteArray,
              Utils.getContextOrSparkClassLoader)

          listenerPacket.listener.asInstanceOf[StreamingQueryListener]
        }

        val id = command.getAddListener.getId
        sessionHolder.cacheListenerById(id, listener)
        session.streams.addListener(listener)
        respBuilder.setAddListener(true)

      case StreamingQueryManagerCommand.CommandCase.REMOVE_LISTENER =>
        val listenerId = command.getRemoveListener.getId
        sessionHolder.getListener(listenerId) match {
          case Some(listener) =>
            session.streams.removeListener(listener)
            sessionHolder.removeCachedListener(listenerId)
            respBuilder.setRemoveListener(true)
          case None =>
            respBuilder.setRemoveListener(false)
        }

      case StreamingQueryManagerCommand.CommandCase.LIST_LISTENERS =>
        respBuilder.getListListenersBuilder
          .addAllListenerIds(sessionHolder.listListenerIds().asJava)

      case StreamingQueryManagerCommand.CommandCase.COMMAND_NOT_SET =>
        throw new IllegalArgumentException("Missing command in StreamingQueryManagerCommand")
    }

    executeHolder.eventsManager.postFinished()
    responseObserver.onNext(
      ExecutePlanResponse
        .newBuilder()
        .setSessionId(sessionId)
        .setServerSideSessionId(sessionHolder.serverSessionId)
        .setStreamingQueryManagerCommandResult(respBuilder.build())
        .build())
  }

  private def handleGetResourcesCommand(
      responseObserver: StreamObserver[proto.ExecutePlanResponse]): Unit = {
    executeHolder.eventsManager.postFinished()
    responseObserver.onNext(
      proto.ExecutePlanResponse
        .newBuilder()
        .setSessionId(sessionId)
        .setServerSideSessionId(sessionHolder.serverSessionId)
        .setGetResourcesCommandResult(
          proto.GetResourcesCommandResult
            .newBuilder()
            .putAllResources(
              session.sparkContext.resources.toMap
                .transform((_, resource) =>
                  proto.ResourceInformation
                    .newBuilder()
                    .setName(resource.name)
                    .addAllAddresses(resource.addresses.toImmutableArraySeq.asJava)
                    .build())
                .asJava)
            .build())
        .build())
  }

  def handleCreateResourceProfileCommand(
      createResourceProfileCommand: CreateResourceProfileCommand,
      responseObserver: StreamObserver[proto.ExecutePlanResponse]): Unit = {
    val rp = createResourceProfileCommand.getProfile
    val ereqs = rp.getExecutorResourcesMap.asScala.map { case (name, res) =>
      name -> new ExecutorResourceRequest(
        res.getResourceName,
        res.getAmount,
        res.getDiscoveryScript,
        res.getVendor)
    }.toMap
    val treqs = rp.getTaskResourcesMap.asScala.map { case (name, res) =>
      name -> new TaskResourceRequest(res.getResourceName, res.getAmount)
    }.toMap

    // Create ResourceProfile add add it to ResourceProfileManager
    val profile = if (ereqs.isEmpty) {
      new TaskResourceProfile(treqs)
    } else {
      new ResourceProfile(ereqs, treqs)
    }
    session.sparkContext.resourceProfileManager.addResourceProfile(profile)

    executeHolder.eventsManager.postFinished()
    responseObserver.onNext(
      proto.ExecutePlanResponse
        .newBuilder()
        .setSessionId(sessionId)
        .setServerSideSessionId(sessionHolder.serverSessionId)
        .setCreateResourceProfileCommandResult(
          proto.CreateResourceProfileCommandResult
            .newBuilder()
            .setProfileId(profile.id)
            .build())
        .build())
  }

  private val emptyLocalRelation = LocalRelation(
    output = AttributeReference("value", StringType, false)() :: Nil,
    data = Seq.empty)

  private def transformCurrentDatabase(): LogicalPlan = {
    session.createDataset(session.catalog.currentDatabase :: Nil)(Encoders.STRING).logicalPlan
  }

  private def transformSetCurrentDatabase(
      getSetCurrentDatabase: proto.SetCurrentDatabase): LogicalPlan = {
    session.catalog.setCurrentDatabase(getSetCurrentDatabase.getDbName)
    emptyLocalRelation
  }

  private def transformListDatabases(getListDatabases: proto.ListDatabases): LogicalPlan = {
    if (getListDatabases.hasPattern) {
      session.catalog.listDatabases(getListDatabases.getPattern).logicalPlan
    } else {
      session.catalog.listDatabases().logicalPlan
    }
  }

  private def transformListTables(getListTables: proto.ListTables): LogicalPlan = {
    if (getListTables.hasDbName) {
      if (getListTables.hasPattern) {
        session.catalog.listTables(getListTables.getDbName, getListTables.getPattern).logicalPlan
      } else {
        session.catalog.listTables(getListTables.getDbName).logicalPlan
      }
    } else if (getListTables.hasPattern) {
      val currentDatabase = session.catalog.currentDatabase
      session.catalog.listTables(currentDatabase, getListTables.getPattern).logicalPlan
    } else {
      session.catalog.listTables().logicalPlan
    }
  }

  private def transformListFunctions(getListFunctions: proto.ListFunctions): LogicalPlan = {
    if (getListFunctions.hasDbName) {
      if (getListFunctions.hasPattern) {
        session.catalog
          .listFunctions(getListFunctions.getDbName, getListFunctions.getPattern)
          .logicalPlan
      } else {
        session.catalog.listFunctions(getListFunctions.getDbName).logicalPlan
      }
    } else if (getListFunctions.hasPattern) {
      val currentDatabase = session.catalog.currentDatabase
      session.catalog.listFunctions(currentDatabase, getListFunctions.getPattern).logicalPlan
    } else {
      session.catalog.listFunctions().logicalPlan
    }
  }

  private def transformListColumns(getListColumns: proto.ListColumns): LogicalPlan = {
    if (getListColumns.hasDbName) {
      session.catalog
        .listColumns(dbName = getListColumns.getDbName, tableName = getListColumns.getTableName)
        .logicalPlan
    } else {
      session.catalog.listColumns(getListColumns.getTableName).logicalPlan
    }
  }

  private def transformGetDatabase(getGetDatabase: proto.GetDatabase): LogicalPlan = {
    CatalogImpl
      .makeDataset(session.catalog.getDatabase(getGetDatabase.getDbName) :: Nil, session)
      .logicalPlan
  }

  private def transformGetTable(getGetTable: proto.GetTable): LogicalPlan = {
    if (getGetTable.hasDbName) {
      CatalogImpl
        .makeDataset(
          session.catalog.getTable(
            dbName = getGetTable.getDbName,
            tableName = getGetTable.getTableName) :: Nil,
          session)
        .logicalPlan
    } else {
      CatalogImpl
        .makeDataset(session.catalog.getTable(getGetTable.getTableName) :: Nil, session)
        .logicalPlan
    }
  }

  private def transformGetFunction(getGetFunction: proto.GetFunction): LogicalPlan = {
    if (getGetFunction.hasDbName) {
      CatalogImpl
        .makeDataset(
          session.catalog.getFunction(
            dbName = getGetFunction.getDbName,
            functionName = getGetFunction.getFunctionName) :: Nil,
          session)
        .logicalPlan
    } else {
      CatalogImpl
        .makeDataset(session.catalog.getFunction(getGetFunction.getFunctionName) :: Nil, session)
        .logicalPlan
    }
  }

  private def transformDatabaseExists(getDatabaseExists: proto.DatabaseExists): LogicalPlan = {
    session
      .createDataset(session.catalog.databaseExists(getDatabaseExists.getDbName) :: Nil)(
        Encoders.scalaBoolean)
      .logicalPlan
  }

  private def transformTableExists(getTableExists: proto.TableExists): LogicalPlan = {
    if (getTableExists.hasDbName) {
      session
        .createDataset(
          session.catalog.tableExists(
            dbName = getTableExists.getDbName,
            tableName = getTableExists.getTableName) :: Nil)(Encoders.scalaBoolean)
        .logicalPlan
    } else {
      session
        .createDataset(session.catalog.tableExists(getTableExists.getTableName) :: Nil)(
          Encoders.scalaBoolean)
        .logicalPlan
    }
  }

  private def transformFunctionExists(getFunctionExists: proto.FunctionExists): LogicalPlan = {
    if (getFunctionExists.hasDbName) {
      session
        .createDataset(
          session.catalog.functionExists(
            dbName = getFunctionExists.getDbName,
            functionName = getFunctionExists.getFunctionName) :: Nil)(Encoders.scalaBoolean)
        .logicalPlan
    } else {
      session
        .createDataset(session.catalog.functionExists(getFunctionExists.getFunctionName) :: Nil)(
          Encoders.scalaBoolean)
        .logicalPlan
    }
  }

  private def transformCreateExternalTable(
      getCreateExternalTable: proto.CreateExternalTable): LogicalPlan = {
    val schema = if (getCreateExternalTable.hasSchema) {
      val struct = transformDataType(getCreateExternalTable.getSchema)
      assert(struct.isInstanceOf[StructType])
      struct.asInstanceOf[StructType]
    } else {
      new StructType
    }

    val source = if (getCreateExternalTable.hasSource) {
      getCreateExternalTable.getSource
    } else {
      session.sessionState.conf.defaultDataSourceName
    }

    val options = if (getCreateExternalTable.hasPath) {
      (getCreateExternalTable.getOptionsMap.asScala ++
        Map("path" -> getCreateExternalTable.getPath)).asJava
    } else {
      getCreateExternalTable.getOptionsMap
    }
    session.catalog
      .createTable(
        tableName = getCreateExternalTable.getTableName,
        source = source,
        schema = schema,
        options = options)
      .logicalPlan
  }

  private def transformCreateTable(getCreateTable: proto.CreateTable): LogicalPlan = {
    val schema = if (getCreateTable.hasSchema) {
      val struct = transformDataType(getCreateTable.getSchema)
      assert(struct.isInstanceOf[StructType])
      struct.asInstanceOf[StructType]
    } else {
      new StructType
    }

    val source = if (getCreateTable.hasSource) {
      getCreateTable.getSource
    } else {
      session.sessionState.conf.defaultDataSourceName
    }

    val description = if (getCreateTable.hasDescription) {
      getCreateTable.getDescription
    } else {
      ""
    }

    val options = if (getCreateTable.hasPath) {
      (getCreateTable.getOptionsMap.asScala ++
        Map("path" -> getCreateTable.getPath)).asJava
    } else {
      getCreateTable.getOptionsMap
    }

    session.catalog
      .createTable(
        tableName = getCreateTable.getTableName,
        source = source,
        schema = schema,
        description = description,
        options = options)
      .logicalPlan
  }

  private def transformDropTempView(getDropTempView: proto.DropTempView): LogicalPlan = {
    session
      .createDataset(session.catalog.dropTempView(getDropTempView.getViewName) :: Nil)(
        Encoders.scalaBoolean)
      .logicalPlan
  }

  private def transformDropGlobalTempView(
      getDropGlobalTempView: proto.DropGlobalTempView): LogicalPlan = {
    session
      .createDataset(
        session.catalog.dropGlobalTempView(getDropGlobalTempView.getViewName) :: Nil)(
        Encoders.scalaBoolean)
      .logicalPlan
  }

  private def transformRecoverPartitions(
      getRecoverPartitions: proto.RecoverPartitions): LogicalPlan = {
    session.catalog.recoverPartitions(getRecoverPartitions.getTableName)
    emptyLocalRelation
  }

  private def transformIsCached(getIsCached: proto.IsCached): LogicalPlan = {
    session
      .createDataset(session.catalog.isCached(getIsCached.getTableName) :: Nil)(
        Encoders.scalaBoolean)
      .logicalPlan
  }

  private def transformCacheTable(getCacheTable: proto.CacheTable): LogicalPlan = {
    if (getCacheTable.hasStorageLevel) {
      session.catalog.cacheTable(
        getCacheTable.getTableName,
        StorageLevelProtoConverter.toStorageLevel(getCacheTable.getStorageLevel))
    } else {
      session.catalog.cacheTable(getCacheTable.getTableName)
    }
    emptyLocalRelation
  }

  private def transformUncacheTable(getUncacheTable: proto.UncacheTable): LogicalPlan = {
    session.catalog.uncacheTable(getUncacheTable.getTableName)
    emptyLocalRelation
  }

  private def transformClearCache(): LogicalPlan = {
    session.catalog.clearCache()
    emptyLocalRelation
  }

  private def transformRefreshTable(getRefreshTable: proto.RefreshTable): LogicalPlan = {
    session.catalog.refreshTable(getRefreshTable.getTableName)
    emptyLocalRelation
  }

  private def transformRefreshByPath(getRefreshByPath: proto.RefreshByPath): LogicalPlan = {
    session.catalog.refreshByPath(getRefreshByPath.getPath)
    emptyLocalRelation
  }

  private def transformCurrentCatalog(): LogicalPlan = {
    session.createDataset(session.catalog.currentCatalog() :: Nil)(Encoders.STRING).logicalPlan
  }

  private def transformSetCurrentCatalog(
      getSetCurrentCatalog: proto.SetCurrentCatalog): LogicalPlan = {
    session.catalog.setCurrentCatalog(getSetCurrentCatalog.getCatalogName)
    emptyLocalRelation
  }

  private def transformListCatalogs(getListCatalogs: proto.ListCatalogs): LogicalPlan = {
    if (getListCatalogs.hasPattern) {
      session.catalog.listCatalogs(getListCatalogs.getPattern).logicalPlan
    } else {
      session.catalog.listCatalogs().logicalPlan
    }
  }
}
