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
import scala.collection.mutable

import com.google.common.collect.{Lists, Maps}
import com.google.protobuf.{Any => ProtoAny}

import org.apache.spark.TaskContext
import org.apache.spark.api.python.{PythonEvalType, SimplePythonFunction}
import org.apache.spark.connect.proto
import org.apache.spark.sql.{Column, Dataset, Encoders, SparkSession}
import org.apache.spark.sql.catalyst.{expressions, AliasIdentifier, FunctionIdentifier}
import org.apache.spark.sql.catalyst.analysis.{GlobalTempView, LocalTempView, MultiAlias, UnresolvedAlias, UnresolvedAttribute, UnresolvedExtractValue, UnresolvedFunction, UnresolvedRegex, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.parser.{CatalystSqlParser, ParseException, ParserUtils}
import org.apache.spark.sql.catalyst.plans.{Cross, FullOuter, Inner, JoinType, LeftAnti, LeftOuter, LeftSemi, RightOuter, UsingJoin}
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.{Deduplicate, Except, Intersect, LocalRelation, LogicalPlan, Sample, Sort, SubqueryAlias, Union, Unpivot, UnresolvedHint}
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, CharVarcharUtils}
import org.apache.spark.sql.connect.common.{DataTypeProtoConverter, InvalidPlanInput, UdfPacket}
import org.apache.spark.sql.connect.planner.LiteralValueProtoConverter.{toCatalystExpression, toCatalystValue}
import org.apache.spark.sql.connect.plugin.SparkConnectPluginRegistry
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.arrow.ArrowConverters
import org.apache.spark.sql.execution.command.CreateViewCommand
import org.apache.spark.sql.execution.python.UserDefinedPythonFunction
import org.apache.spark.sql.internal.CatalogImpl
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

final case class InvalidCommandInput(
    private val message: String = "",
    private val cause: Throwable = null)
    extends Exception(message, cause)

class SparkConnectPlanner(val session: SparkSession) {
  private lazy val pythonExec =
    sys.env.getOrElse("PYSPARK_PYTHON", sys.env.getOrElse("PYSPARK_DRIVER_PYTHON", "python3"))

  // The root of the query plan is a relation and we apply the transformations to it.
  def transformRelation(rel: proto.Relation): LogicalPlan = {
    val plan = rel.getRelTypeCase match {
      // DataFrame API
      case proto.Relation.RelTypeCase.SHOW_STRING => transformShowString(rel.getShowString)
      case proto.Relation.RelTypeCase.READ => transformReadRel(rel.getRead)
      case proto.Relation.RelTypeCase.PROJECT => transformProject(rel.getProject)
      case proto.Relation.RelTypeCase.FILTER => transformFilter(rel.getFilter)
      case proto.Relation.RelTypeCase.LIMIT => transformLimit(rel.getLimit)
      case proto.Relation.RelTypeCase.OFFSET => transformOffset(rel.getOffset)
      case proto.Relation.RelTypeCase.TAIL => transformTail(rel.getTail)
      case proto.Relation.RelTypeCase.JOIN => transformJoin(rel.getJoin)
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
      case proto.Relation.RelTypeCase.HINT => transformHint(rel.getHint)
      case proto.Relation.RelTypeCase.UNPIVOT => transformUnpivot(rel.getUnpivot)
      case proto.Relation.RelTypeCase.REPARTITION_BY_EXPRESSION =>
        transformRepartitionByExpression(rel.getRepartitionByExpression)
      case proto.Relation.RelTypeCase.FRAME_MAP =>
        transformFrameMap(rel.getFrameMap)
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
      .map(p => p.transform(extension, this))
      // Find the first non-empty transformation or throw.
      .find(_.nonEmpty)
      .flatten
      .getOrElse(throw InvalidPlanInput("No handler found for extension"))
  }

  private def transformCatalog(catalog: proto.Catalog): LogicalPlan = {
    catalog.getCatTypeCase match {
      case proto.Catalog.CatTypeCase.CURRENT_DATABASE =>
        transformCurrentDatabase(catalog.getCurrentDatabase)
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
      case proto.Catalog.CatTypeCase.CLEAR_CACHE => transformClearCache(catalog.getClearCache)
      case proto.Catalog.CatTypeCase.REFRESH_TABLE =>
        transformRefreshTable(catalog.getRefreshTable)
      case proto.Catalog.CatTypeCase.REFRESH_BY_PATH =>
        transformRefreshByPath(catalog.getRefreshByPath)
      case proto.Catalog.CatTypeCase.CURRENT_CATALOG =>
        transformCurrentCatalog(catalog.getCurrentCatalog)
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

  private def transformSql(sql: proto.SQL): LogicalPlan = {
    val args = sql.getArgsMap.asScala.toMap
    val parser = session.sessionState.sqlParser
    val parsedArgs = args.mapValues(parser.parseExpression).toMap
    Parameter.bind(parser.parsePlan(sql.getQuery), parsedArgs)
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
      val value = values.head
      value.getLiteralTypeCase match {
        case proto.Expression.Literal.LiteralTypeCase.BOOLEAN =>
          if (cols.nonEmpty) {
            dataset.na.fill(value = value.getBoolean, cols = cols).logicalPlan
          } else {
            dataset.na.fill(value = value.getBoolean).logicalPlan
          }
        case proto.Expression.Literal.LiteralTypeCase.LONG =>
          if (cols.nonEmpty) {
            dataset.na.fill(value = value.getLong, cols = cols).logicalPlan
          } else {
            dataset.na.fill(value = value.getLong).logicalPlan
          }
        case proto.Expression.Literal.LiteralTypeCase.DOUBLE =>
          if (cols.nonEmpty) {
            dataset.na.fill(value = value.getDouble, cols = cols).logicalPlan
          } else {
            dataset.na.fill(value = value.getDouble).logicalPlan
          }
        case proto.Expression.Literal.LiteralTypeCase.STRING =>
          if (cols.nonEmpty) {
            dataset.na.fill(value = value.getString, cols = cols).logicalPlan
          } else {
            dataset.na.fill(value = value.getString).logicalPlan
          }
        case other => throw InvalidPlanInput(s"Unsupported value type: $other")
      }
    } else {
      val valueMap = mutable.Map.empty[String, Any]
      cols.zip(values).foreach { case (col, value) =>
        valueMap.update(col, toCatalystValue(value))
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
        toCatalystValue(replace.getOldValue),
        toCatalystValue(replace.getNewValue))
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
    val cov = Dataset
      .ofRows(session, transformRelation(rel.getInput))
      .stat
      .cov(rel.getCol1, rel.getCol2)
    LocalRelation.fromProduct(
      output = AttributeReference("cov", DoubleType, false)() :: Nil,
      data = Tuple1.apply(cov) :: Nil)
  }

  private def transformStatCorr(rel: proto.StatCorr): LogicalPlan = {
    val df = Dataset.ofRows(session, transformRelation(rel.getInput))
    val corr = if (rel.hasMethod) {
      df.stat.corr(rel.getCol1, rel.getCol2, rel.getMethod)
    } else {
      df.stat.corr(rel.getCol1, rel.getCol2)
    }

    LocalRelation.fromProduct(
      output = AttributeReference("corr", DoubleType, false)() :: Nil,
      data = Tuple1.apply(corr) :: Nil)
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
    val fractions = rel.getFractionsList.asScala.toSeq.map { protoFraction =>
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
    val schema = DataTypeProtoConverter.toCatalystType(rel.getSchema)
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

  private def transformFrameMap(rel: proto.FrameMap): LogicalPlan = {
    val commonUdf = rel.getFunc
    val pythonUdf = transformPythonUDF(commonUdf)
    pythonUdf.evalType match {
      case PythonEvalType.SQL_MAP_PANDAS_ITER_UDF =>
        logical.MapInPandas(
          pythonUdf,
          pythonUdf.dataType.asInstanceOf[StructType].toAttributes,
          transformRelation(rel.getInput))
      case _ =>
        throw InvalidPlanInput(s"Function with EvalType: ${pythonUdf.evalType} is not supported")
    }
  }

  private def transformWithColumnsRenamed(rel: proto.WithColumnsRenamed): LogicalPlan = {
    Dataset
      .ofRows(session, transformRelation(rel.getInput))
      .withColumnsRenamed(rel.getRenameColumnsMapMap)
      .logicalPlan
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

  private def transformHint(rel: proto.Hint): LogicalPlan = {

    def extractValue(expr: Expression): Any = {
      expr match {
        case Literal(s, StringType) if s != null =>
          UnresolvedAttribute.quotedString(s.toString)
        case literal: Literal => literal.value
        case UnresolvedFunction(Seq("array"), arguments, _, _, _) =>
          arguments.map(extractValue).toArray
        case other =>
          throw InvalidPlanInput(
            s"Expression should be a Literal or CreateMap or CreateArray, " +
              s"but got ${other.getClass} $other")
      }
    }

    val params = rel.getParametersList.asScala.toSeq.map(transformExpression).map(extractValue)
    UnresolvedHint(rel.getName, params, transformRelation(rel.getInput))
  }

  private def transformUnpivot(rel: proto.Unpivot): LogicalPlan = {
    val ids = rel.getIdsList.asScala.toArray.map { expr =>
      Column(transformExpression(expr))
    }

    if (!rel.hasValues) {
      Unpivot(
        Some(ids.map(_.named)),
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
        Some(ids.map(_.named)),
        Some(values.map(v => Seq(v.named))),
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
      Deduplicate(allColumns, queryExecution.analyzed)
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
      Deduplicate(groupCols, queryExecution.analyzed)
    }
  }

  private def parseDatatypeString(sqlText: String): DataType = {
    val parser = session.sessionState.sqlParser
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
      val attributes = structType.toAttributes
      val proj = UnsafeProjection.create(attributes, attributes)
      val relation = logical.LocalRelation(attributes, rows.map(r => proj(r).copy()).toSeq)

      if (schema == null) {
        relation
      } else {
        Dataset
          .ofRows(session, logicalPlan = relation)
          .toDF(schema.names: _*)
          .to(schema)
          .logicalPlan
      }
    } else {
      if (schema == null) {
        throw InvalidPlanInput(
          s"Schema for LocalRelation is required when the input data is not provided.")
      }
      LocalRelation(schema.toAttributes, data = Seq.empty)
    }
  }

  private def transformReadRel(rel: proto.Read): LogicalPlan = {
    rel.getReadTypeCase match {
      case proto.Read.ReadTypeCase.NAMED_TABLE =>
        val multipartIdentifier =
          CatalystSqlParser.parseMultipartIdentifier(rel.getNamedTable.getUnparsedIdentifier)
        UnresolvedRelation(multipartIdentifier)

      case proto.Read.ReadTypeCase.DATA_SOURCE =>
        val localMap = CaseInsensitiveMap[String](rel.getDataSource.getOptionsMap.asScala.toMap)
        val reader = session.read
        if (rel.getDataSource.hasFormat) {
          reader.format(rel.getDataSource.getFormat)
        }
        localMap.foreach { case (key, value) => reader.option(key, value) }
        if (rel.getDataSource.hasSchema && rel.getDataSource.getSchema.nonEmpty) {

          DataType.parseTypeWithFallback(
            rel.getDataSource.getSchema,
            StructType.fromDDL,
            fallbackParser = DataType.fromJson) match {
            case s: StructType => reader.schema(s)
            case other => throw InvalidPlanInput(s"Invalid schema $other")
          }
        }
        if (rel.getDataSource.getPathsCount == 0) {
          reader.load().queryExecution.analyzed
        } else if (rel.getDataSource.getPathsCount == 1) {
          reader.load(rel.getDataSource.getPaths(0)).queryExecution.analyzed
        } else {
          reader.load(rel.getDataSource.getPathsList.asScala.toSeq: _*).queryExecution.analyzed
        }

      case _ => throw InvalidPlanInput("Does not support " + rel.getReadTypeCase.name())
    }
  }

  private def transformFilter(rel: proto.Filter): LogicalPlan = {
    assert(rel.hasInput)
    val baseRel = transformRelation(rel.getInput)
    logical.Filter(condition = transformExpression(rel.getCondition), child = baseRel)
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
    expr
  }

  private def transformExpressionPlugin(extension: ProtoAny): Expression = {
    SparkConnectPluginRegistry.expressionRegistry
      // Lazily traverse the collection.
      .view
      // Apply the transformation.
      .map(p => p.transform(extension, this))
      // Find the first non-empty transformation or throw.
      .find(_.nonEmpty)
      .flatten
      .getOrElse(throw InvalidPlanInput("No handler found for extension"))
  }

  /**
   * Transforms the protocol buffers literals into the appropriate Catalyst literal expression.
   * @return
   *   Expression
   */
  private def transformLiteral(lit: proto.Expression.Literal): Literal = {
    toCatalystExpression(lit)
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
   *
   * TODO(SPARK-40546) We need to homogenize the function names for binary operators.
   *
   * @param fun
   *   Proto representation of the function call.
   * @return
   */
  private def transformUnresolvedFunction(
      fun: proto.Expression.UnresolvedFunction): Expression = {
    if (fun.getIsUserDefinedFunction) {
      UnresolvedFunction(
        session.sessionState.sqlParser.parseFunctionIdentifier(fun.getFunctionName),
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
        transformPythonUDF(fun)
      case proto.CommonInlineUserDefinedFunction.FunctionCase.SCALAR_SCALA_UDF =>
        transformScalarScalaUDF(fun)
      case _ =>
        throw InvalidPlanInput(
          s"Function with ID: ${fun.getFunctionCase.getNumber} is not supported")
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
    val udfPacket =
      Utils.deserialize[UdfPacket](udf.getPayload.toByteArray, Utils.getContextOrSparkClassLoader)
    ScalaUDF(
      function = udfPacket.function,
      dataType = udfPacket.outputEncoder.dataType,
      children = fun.getArgumentsList.asScala.map(transformExpression).toSeq,
      inputEncoders = udfPacket.inputEncoders.map(e => Option(ExpressionEncoder(e))),
      outputEncoder = Option(ExpressionEncoder(udfPacket.outputEncoder)),
      udfName = Option(fun.getFunctionName),
      nullable = udf.getNullable,
      udfDeterministic = fun.getDeterministic)
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
    val udf = fun.getPythonUdf
    PythonUDF(
      name = fun.getFunctionName,
      func = transformPythonFunction(udf),
      dataType = DataType.parseTypeWithFallback(
        schema = udf.getOutputType,
        parser = DataType.fromDDL,
        fallbackParser = DataType.fromJson) match {
        case s: DataType => s
        case other => throw InvalidPlanInput(s"Invalid return type $other")
      },
      children = fun.getArgumentsList.asScala.map(transformExpression).toSeq,
      evalType = udf.getEvalType,
      udfDeterministic = fun.getDeterministic)
  }

  private def transformPythonFunction(fun: proto.PythonUDF): SimplePythonFunction = {
    SimplePythonFunction(
      command = fun.getCommand.toByteArray,
      // Empty environment variables
      envVars = Maps.newHashMap(),
      // No imported Python libraries
      pythonIncludes = Lists.newArrayList(),
      pythonExec = pythonExec,
      pythonVer = fun.getPythonVer,
      // Empty broadcast variables
      broadcastVars = Lists.newArrayList(),
      // Null accumulator
      accumulator = null)
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
      case "product" =>
        if (fun.getArgumentsCount != 1) {
          throw InvalidPlanInput("Product requires single child expression")
        }
        Some(
          aggregate
            .Product(transformExpression(fun.getArgumentsList.asScala.head))
            .toAggregateExpression())

      case "when" =>
        if (fun.getArgumentsCount == 0) {
          throw InvalidPlanInput("CaseWhen requires at least one child expression")
        }
        val children = fun.getArgumentsList.asScala.toSeq.map(transformExpression)
        Some(CaseWhen.createFromParser(children))

      case "in" =>
        if (fun.getArgumentsCount == 0) {
          throw InvalidPlanInput("In requires at least one child expression")
        }
        val children = fun.getArgumentsList.asScala.toSeq.map(transformExpression)
        Some(In(children.head, children.tail))

      case "nth_value" if fun.getArgumentsCount == 3 =>
        // NthValue does not have a constructor which accepts Expression typed 'ignoreNulls'
        val children = fun.getArgumentsList.asScala.toSeq.map(transformExpression)
        val ignoreNulls = children.last match {
          case Literal(bool: Boolean, BooleanType) => bool
          case other =>
            throw InvalidPlanInput(s"ignoreNulls should be a literal boolean, but got $other")
        }
        Some(NthValue(children(0), children(1), ignoreNulls))

      case "lag" if fun.getArgumentsCount == 4 =>
        // Lag does not have a constructor which accepts Expression typed 'ignoreNulls'
        val children = fun.getArgumentsList.asScala.toSeq.map(transformExpression)
        val ignoreNulls = children.last match {
          case Literal(bool: Boolean, BooleanType) => bool
          case other =>
            throw InvalidPlanInput(s"ignoreNulls should be a literal boolean, but got $other")
        }
        Some(Lag(children.head, children(1), children(2), ignoreNulls))

      case "lead" if fun.getArgumentsCount == 4 =>
        // Lead does not have a constructor which accepts Expression typed 'ignoreNulls'
        val children = fun.getArgumentsList.asScala.toSeq.map(transformExpression)
        val ignoreNulls = children.last match {
          case Literal(bool: Boolean, BooleanType) => bool
          case other =>
            throw InvalidPlanInput(s"ignoreNulls should be a literal boolean, but got $other")
        }
        Some(Lead(children.head, children(1), children(2), ignoreNulls))

      case "window" if 2 <= fun.getArgumentsCount && fun.getArgumentsCount <= 4 =>
        val children = fun.getArgumentsList.asScala.toSeq.map(transformExpression)
        val timeCol = children.head
        val args = children.tail.map {
          case Literal(s, StringType) if s != null => s.toString
          case other =>
            throw InvalidPlanInput(
              s"windowDuration,slideDuration,startTime should be literal strings, but got $other")
        }
        var windowDuration: String = null
        var slideDuration: String = null
        var startTime: String = null
        if (args.length == 3) {
          windowDuration = args(0)
          slideDuration = args(1)
          startTime = args(2)
        } else if (args.length == 2) {
          windowDuration = args(0)
          slideDuration = args(1)
          startTime = "0 second"
        } else {
          windowDuration = args(0)
          slideDuration = args(0)
          startTime = "0 second"
        }
        Some(
          Alias(TimeWindow(timeCol, windowDuration, slideDuration, startTime), "window")(
            nonInheritableMetadataKeys = Seq(Dataset.DATASET_ID_KEY, Dataset.COL_POS_KEY)))

      case "session_window" if fun.getArgumentsCount == 2 =>
        val children = fun.getArgumentsList.asScala.toSeq.map(transformExpression)
        val timeCol = children.head
        val sessionWindow = children.last match {
          case Literal(s, StringType) if s != null => SessionWindow(timeCol, s.toString)
          case other => SessionWindow(timeCol, other)
        }
        Some(
          Alias(sessionWindow, "session_window")(nonInheritableMetadataKeys =
            Seq(Dataset.DATASET_ID_KEY, Dataset.COL_POS_KEY)))

      case "bucket" if fun.getArgumentsCount == 2 =>
        val children = fun.getArgumentsList.asScala.toSeq.map(transformExpression)
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
        val children = fun.getArgumentsList.asScala.toSeq.map(transformExpression)

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
          val options = if (children.length == 3) {
            // ExprUtils.convertToMapData requires the options to be resolved CreateMap,
            // but the options here is not resolved yet: UnresolvedFunction("map", ...)
            children(2) match {
              case UnresolvedFunction(Seq("map"), arguments, _, _, _) =>
                ExprUtils.convertToMapData(CreateMap(arguments))
              case other =>
                throw InvalidPlanInput(
                  s"Options in from_json should be created by map, but got $other")
            }
          } else {
            Map.empty[String, String]
          }

          Some(
            JsonToStructs(
              schema = CharVarcharUtils.failIfHasCharVarchar(schema),
              options = options,
              child = children.head))
        } else {
          None
        }

      case _ => None
    }
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
    session.sessionState.sqlParser.parseExpression(expr.getExpression)
  }

  private def transformUnresolvedStar(star: proto.Expression.UnresolvedStar): UnresolvedStar = {
    if (star.hasUnparsedTarget) {
      val target = star.getUnparsedTarget
      if (!target.endsWith(".*")) {
        throw InvalidPlanInput(
          s"UnresolvedStar requires a unparsed target ending with '.*', " +
            s"but got $target.")
      }

      UnresolvedStar(
        Some(UnresolvedAttribute.parseAttributeName(target.substring(0, target.length - 2))))
    } else {
      UnresolvedStar(None)
    }
  }

  private def transformCast(cast: proto.Expression.Cast): Expression = {
    cast.getCastToTypeCase match {
      case proto.Expression.Cast.CastToTypeCase.TYPE =>
        Cast(
          transformExpression(cast.getExpr),
          DataTypeProtoConverter.toCatalystType(cast.getType))
      case _ =>
        Cast(
          transformExpression(cast.getExpr),
          session.sessionState.sqlParser.parseDataType(cast.getTypeStr))
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
        UnresolvedAttribute.quotedString(regex.getColName)
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
    assert(rel.getColsCount > 0, s"cols must contains at least 1 item!")

    val cols = rel.getColsList.asScala.toArray.map { expr =>
      Column(transformExpression(expr))
    }

    Dataset
      .ofRows(session, transformRelation(rel.getInput))
      .drop(cols.head, cols.tail: _*)
      .logicalPlan
  }

  private def transformAggregate(rel: proto.Aggregate): LogicalPlan = {
    if (!rel.hasInput) {
      throw InvalidPlanInput("Aggregate needs a plan input")
    }
    val input = transformRelation(rel.getInput)

    val groupingExprs = rel.getGroupingExpressionsList.asScala.toSeq.map(transformExpression)
    val aggExprs = rel.getAggregateExpressionsList.asScala.toSeq.map(transformExpression)
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
            .toSeq
            .map(expressions.Literal.apply)
        }

        logical.Pivot(
          groupByExprsOpt = Some(groupingExprs.map(toNamedExpression)),
          pivotColumn = pivotExpr,
          pivotValues = valueExprs,
          aggregates = aggExprs,
          child = input)

      case other => throw InvalidPlanInput(s"Unknown Group Type $other")
    }
  }

  def process(command: proto.Command): Unit = {
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
      case _ => throw new UnsupportedOperationException(s"$command not supported.")
    }
  }

  private def handleRegisterUserDefinedFunction(
      fun: proto.CommonInlineUserDefinedFunction): Unit = {
    fun.getFunctionCase match {
      case proto.CommonInlineUserDefinedFunction.FunctionCase.PYTHON_UDF =>
        handleRegisterPythonUDF(fun)
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
      dataType = DataType.parseTypeWithFallback(
        schema = udf.getOutputType,
        parser = DataType.fromDDL,
        fallbackParser = DataType.fromJson) match {
        case s: DataType => s
        case other => throw InvalidPlanInput(s"Invalid return type $other")
      },
      pythonEvalType = udf.getEvalType,
      udfDeterministic = fun.getDeterministic)

    session.udf.registerPython(fun.getFunctionName, udpf)
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
  private def handleWriteOperation(writeOperation: proto.WriteOperation): Unit = {
    // Transform the input plan into the logical plan.
    val planner = new SparkConnectPlanner(session)
    val plan = planner.transformRelation(writeOperation.getInput)
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

  /**
   * Transforms the write operation and executes it.
   *
   * The input write operation contains a reference to the input plan and transforms it to the
   * corresponding logical plan. Afterwards, creates the DataFrameWriter and translates the
   * parameters of the WriteOperation into the corresponding methods calls.
   *
   * @param writeOperation
   */
  def handleWriteOperationV2(writeOperation: proto.WriteOperationV2): Unit = {
    // Transform the input plan into the logical plan.
    val planner = new SparkConnectPlanner(session)
    val plan = planner.transformRelation(writeOperation.getInput)
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
      w.partitionedBy(names.head, names.tail.toSeq: _*)
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

  private val emptyLocalRelation = LocalRelation(
    output = AttributeReference("value", StringType, false)() :: Nil,
    data = Seq.empty)

  private def transformCurrentDatabase(getCurrentDatabase: proto.CurrentDatabase): LogicalPlan = {
    session.createDataset(session.catalog.currentDatabase :: Nil)(Encoders.STRING).logicalPlan
  }

  private def transformSetCurrentDatabase(
      getSetCurrentDatabase: proto.SetCurrentDatabase): LogicalPlan = {
    session.catalog.setCurrentDatabase(getSetCurrentDatabase.getDbName)
    emptyLocalRelation
  }

  private def transformListDatabases(getListDatabases: proto.ListDatabases): LogicalPlan = {
    session.catalog.listDatabases().logicalPlan
  }

  private def transformListTables(getListTables: proto.ListTables): LogicalPlan = {
    if (getListTables.hasDbName) {
      session.catalog.listTables(getListTables.getDbName).logicalPlan
    } else {
      session.catalog.listTables().logicalPlan
    }
  }

  private def transformListFunctions(getListFunctions: proto.ListFunctions): LogicalPlan = {
    if (getListFunctions.hasDbName) {
      session.catalog.listFunctions(getListFunctions.getDbName).logicalPlan
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
      val struct = DataTypeProtoConverter.toCatalystType(getCreateExternalTable.getSchema)
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
      val struct = DataTypeProtoConverter.toCatalystType(getCreateTable.getSchema)
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
    session.catalog.cacheTable(getCacheTable.getTableName)
    emptyLocalRelation
  }

  private def transformUncacheTable(getUncacheTable: proto.UncacheTable): LogicalPlan = {
    session.catalog.uncacheTable(getUncacheTable.getTableName)
    emptyLocalRelation
  }

  private def transformClearCache(getClearCache: proto.ClearCache): LogicalPlan = {
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

  private def transformCurrentCatalog(getCurrentCatalog: proto.CurrentCatalog): LogicalPlan = {
    session.createDataset(session.catalog.currentCatalog() :: Nil)(Encoders.STRING).logicalPlan
  }

  private def transformSetCurrentCatalog(
      getSetCurrentCatalog: proto.SetCurrentCatalog): LogicalPlan = {
    session.catalog.setCurrentCatalog(getSetCurrentCatalog.getCatalogName)
    emptyLocalRelation
  }

  private def transformListCatalogs(getListCatalogs: proto.ListCatalogs): LogicalPlan = {
    session.catalog.listCatalogs().logicalPlan
  }
}
