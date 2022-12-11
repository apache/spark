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

import org.apache.spark.TaskContext
import org.apache.spark.api.python.{PythonEvalType, SimplePythonFunction}
import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.WriteOperation
import org.apache.spark.sql.{Column, Dataset, SparkSession}
import org.apache.spark.sql.catalyst.{expressions, AliasIdentifier, FunctionIdentifier}
import org.apache.spark.sql.catalyst.analysis.{GlobalTempView, LocalTempView, MultiAlias, UnresolvedAlias, UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer.CombineUnions
import org.apache.spark.sql.catalyst.parser.{CatalystSqlParser, ParseException}
import org.apache.spark.sql.catalyst.plans.{logical, Cross, FullOuter, Inner, JoinType, LeftAnti, LeftOuter, LeftSemi, RightOuter, UsingJoin}
import org.apache.spark.sql.catalyst.plans.logical.{Deduplicate, Except, Intersect, LocalRelation, LogicalPlan, Sample, SubqueryAlias, Union, Unpivot, UnresolvedHint}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connect.planner.LiteralValueProtoConverter.{toCatalystExpression, toCatalystValue}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.arrow.ArrowConverters
import org.apache.spark.sql.execution.command.CreateViewCommand
import org.apache.spark.sql.execution.python.UserDefinedPythonFunction
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

final case class InvalidPlanInput(
    private val message: String = "",
    private val cause: Throwable = None.orNull)
    extends Exception(message, cause)

final case class InvalidCommandInput(
    private val message: String = "",
    private val cause: Throwable = null)
    extends Exception(message, cause)

class SparkConnectPlanner(session: SparkSession) {
  lazy val pythonExec =
    sys.env.getOrElse("PYSPARK_PYTHON", sys.env.getOrElse("PYSPARK_DRIVER_PYTHON", "python3"))

  // The root of the query plan is a relation and we apply the transformations to it.
  def transformRelation(rel: proto.Relation): LogicalPlan = {
    rel.getRelTypeCase match {
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
      case proto.Relation.RelTypeCase.CROSSTAB =>
        transformStatCrosstab(rel.getCrosstab)
      case proto.Relation.RelTypeCase.RENAME_COLUMNS_BY_SAME_LENGTH_NAMES =>
        transformRenameColumnsBySamelenghtNames(rel.getRenameColumnsBySameLengthNames)
      case proto.Relation.RelTypeCase.RENAME_COLUMNS_BY_NAME_TO_NAME_MAP =>
        transformRenameColumnsByNameToNameMap(rel.getRenameColumnsByNameToNameMap)
      case proto.Relation.RelTypeCase.WITH_COLUMNS => transformWithColumns(rel.getWithColumns)
      case proto.Relation.RelTypeCase.HINT => transformHint(rel.getHint)
      case proto.Relation.RelTypeCase.UNPIVOT => transformUnpivot(rel.getUnpivot)
      case proto.Relation.RelTypeCase.RELTYPE_NOT_SET =>
        throw new IndexOutOfBoundsException("Expected Relation to be set, but is empty.")
      case _ => throw InvalidPlanInput(s"${rel.getUnknown} not supported.")
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
    session.sessionState.sqlParser.parsePlan(sql.getQuery)
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
    Sample(
      rel.getLowerBound,
      rel.getUpperBound,
      rel.getWithReplacement,
      if (rel.hasSeed) rel.getSeed else Utils.random.nextLong,
      transformRelation(rel.getInput))
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

  private def transformStatCrosstab(rel: proto.StatCrosstab): LogicalPlan = {
    Dataset
      .ofRows(session, transformRelation(rel.getInput))
      .stat
      .crosstab(rel.getCol1, rel.getCol2)
      .logicalPlan
  }

  private def transformRenameColumnsBySamelenghtNames(
      rel: proto.RenameColumnsBySameLengthNames): LogicalPlan = {
    Dataset
      .ofRows(session, transformRelation(rel.getInput))
      .toDF(rel.getColumnNamesList.asScala.toSeq: _*)
      .logicalPlan
  }

  private def transformRenameColumnsByNameToNameMap(
      rel: proto.RenameColumnsByNameToNameMap): LogicalPlan = {
    Dataset
      .ofRows(session, transformRelation(rel.getInput))
      .withColumnsRenamed(rel.getRenameColumnsMap)
      .logicalPlan
  }

  private def transformWithColumns(rel: proto.WithColumns): LogicalPlan = {
    val (names, cols) =
      rel.getNameExprListList.asScala
        .map(e => {
          if (e.getNameCount() == 1) {
            (e.getName(0), Column(transformExpression(e.getExpr)))
          } else {
            throw InvalidPlanInput(
              s"""WithColumns require column name only contains one name part,
                 |but got ${e.getNameList.toString}""".stripMargin)
          }
        })
        .unzip
    Dataset
      .ofRows(session, transformRelation(rel.getInput))
      .withColumns(names.toSeq, cols.toSeq)
      .logicalPlan
  }

  private def transformHint(rel: proto.Hint): LogicalPlan = {
    val params = rel.getParametersList.asScala.map(toCatalystValue).toSeq
    UnresolvedHint(rel.getName, params, transformRelation(rel.getInput))
  }

  private def transformUnpivot(rel: proto.Unpivot): LogicalPlan = {
    val ids = rel.getIdsList.asScala.toArray.map { expr =>
      Column(transformExpression(expr))
    }

    if (rel.getValuesList.isEmpty) {
      Unpivot(
        Some(ids.map(_.named)),
        None,
        None,
        rel.getVariableColumnName,
        Seq(rel.getValueColumnName),
        transformRelation(rel.getInput))
    } else {
      val values = rel.getValuesList.asScala.toArray.map { expr =>
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
      case _: ParseException =>
        try {
          parser.parseDataType(sqlText)
        } catch {
          case _: ParseException =>
            parser.parseDataType(s"struct<${sqlText.trim}>")
        }
    }
  }

  private def transformLocalRelation(rel: proto.LocalRelation): LogicalPlan = {
    val (rows, structType) = ArrowConverters.fromBatchWithSchemaIterator(
      Iterator(rel.getData.toByteArray),
      TaskContext.get())
    if (structType == null) {
      throw InvalidPlanInput(s"Input data for LocalRelation does not produce a schema.")
    }
    val attributes = structType.toAttributes
    val proj = UnsafeProjection.create(attributes, attributes)
    val relation = logical.LocalRelation(attributes, rows.map(r => proj(r).copy()).toSeq)

    if (!rel.hasDatatype && !rel.hasDatatypeStr) {
      return relation
    }

    val schemaType = if (rel.hasDatatype) {
      DataTypeProtoConverter.toCatalystType(rel.getDatatype)
    } else {
      parseDatatypeString(rel.getDatatypeStr)
    }

    val schemaStruct = schemaType match {
      case s: StructType => s
      case d => StructType(Seq(StructField("value", d)))
    }

    Dataset
      .ofRows(session, logicalPlan = relation)
      .toDF(schemaStruct.names: _*)
      .to(schemaStruct)
      .logicalPlan
  }

  private def transformReadRel(rel: proto.Read): LogicalPlan = {
    val baseRelation = rel.getReadTypeCase match {
      case proto.Read.ReadTypeCase.NAMED_TABLE =>
        val multipartIdentifier =
          CatalystSqlParser.parseMultipartIdentifier(rel.getNamedTable.getUnparsedIdentifier)
        UnresolvedRelation(multipartIdentifier)
      case proto.Read.ReadTypeCase.DATA_SOURCE =>
        if (rel.getDataSource.getFormat == "") {
          throw InvalidPlanInput("DataSource requires a format")
        }
        val localMap = CaseInsensitiveMap[String](rel.getDataSource.getOptionsMap.asScala.toMap)
        val reader = session.read
        reader.format(rel.getDataSource.getFormat)
        localMap.foreach { case (key, value) => reader.option(key, value) }
        if (rel.getDataSource.getSchema != null && !rel.getDataSource.getSchema.isEmpty) {
          reader.schema(rel.getDataSource.getSchema)
        }
        reader.load().queryExecution.analyzed
      case _ => throw InvalidPlanInput("Does not support " + rel.getReadTypeCase.name())
    }
    baseRelation
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
    val projection =
      rel.getExpressionsList.asScala.map(transformExpression).map(UnresolvedAlias(_))
    logical.Project(projectList = projection.toSeq, child = baseRel)
  }

  private def transformUnresolvedExpression(exp: proto.Expression): UnresolvedAttribute = {
    UnresolvedAttribute.quotedString(exp.getUnresolvedAttribute.getUnparsedIdentifier)
  }

  private def transformExpression(exp: proto.Expression): Expression = {
    exp.getExprTypeCase match {
      case proto.Expression.ExprTypeCase.LITERAL => transformLiteral(exp.getLiteral)
      case proto.Expression.ExprTypeCase.UNRESOLVED_ATTRIBUTE =>
        transformUnresolvedExpression(exp)
      case proto.Expression.ExprTypeCase.UNRESOLVED_FUNCTION =>
        transformUnregisteredFunction(exp.getUnresolvedFunction)
          .getOrElse(transformUnresolvedFunction(exp.getUnresolvedFunction))
      case proto.Expression.ExprTypeCase.ALIAS => transformAlias(exp.getAlias)
      case proto.Expression.ExprTypeCase.EXPRESSION_STRING =>
        transformExpressionString(exp.getExpressionString)
      case proto.Expression.ExprTypeCase.UNRESOLVED_STAR =>
        transformUnresolvedStar(exp.getUnresolvedStar)
      case proto.Expression.ExprTypeCase.CAST => transformCast(exp.getCast)
      case _ =>
        throw InvalidPlanInput(
          s"Expression with ID: ${exp.getExprTypeCase.getNumber} is not supported")
    }
  }

  /**
   * Transforms the protocol buffers literals into the appropriate Catalyst literal expression.
   * @return
   *   Expression
   */
  private def transformLiteral(lit: proto.Expression.Literal): Expression = {
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

      case _ => None
    }
  }

  private def transformAlias(alias: proto.Expression.Alias): NamedExpression = {
    if (alias.getNameCount == 1) {
      val md = if (alias.hasMetadata()) {
        Some(Metadata.fromJson(alias.getMetadata))
      } else {
        None
      }
      Alias(transformExpression(alias.getExpr), alias.getName(0))(explicitMetadata = md)
    } else {
      if (alias.hasMetadata) {
        throw new InvalidPlanInput(
          "Alias expressions with more than 1 identifier must not use optional metadata.")
      }
      MultiAlias(transformExpression(alias.getExpr), alias.getNameList.asScala.toSeq)
    }
  }

  private def transformExpressionString(expr: proto.Expression.ExpressionString): Expression = {
    session.sessionState.sqlParser.parseExpression(expr.getExpression)
  }

  private def transformUnresolvedStar(regex: proto.Expression.UnresolvedStar): Expression = {
    if (regex.getTargetList.isEmpty) {
      UnresolvedStar(Option.empty)
    } else {
      UnresolvedStar(Some(regex.getTargetList.asScala.toSeq))
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

  private def transformSetOperation(u: proto.SetOperation): LogicalPlan = {
    assert(u.hasLeftInput && u.hasRightInput, "Union must have 2 inputs")

    u.getSetOpType match {
      case proto.SetOperation.SetOpType.SET_OP_TYPE_EXCEPT =>
        if (u.getByName) {
          throw InvalidPlanInput("Except does not support union_by_name")
        }
        Except(transformRelation(u.getLeftInput), transformRelation(u.getRightInput), u.getIsAll)
      case proto.SetOperation.SetOpType.SET_OP_TYPE_INTERSECT =>
        if (u.getByName) {
          throw InvalidPlanInput("Intersect does not support union_by_name")
        }
        Intersect(
          transformRelation(u.getLeftInput),
          transformRelation(u.getRightInput),
          u.getIsAll)
      case proto.SetOperation.SetOpType.SET_OP_TYPE_UNION =>
        val combinedUnion = CombineUnions(
          Union(
            Seq(transformRelation(u.getLeftInput), transformRelation(u.getRightInput)),
            byName = u.getByName))
        if (u.getIsAll) {
          combinedUnion
        } else {
          logical.Deduplicate(combinedUnion.output, combinedUnion)
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

  private def transformSort(rel: proto.Sort): LogicalPlan = {
    assert(rel.getSortFieldsCount > 0, "'sort_fields' must be present and contain elements.")
    logical.Sort(
      child = transformRelation(rel.getInput),
      global = rel.getIsGlobal,
      order = rel.getSortFieldsList.asScala.map(transformSortOrderExpression).toSeq)
  }

  private def transformSortOrderExpression(so: proto.Sort.SortField): expressions.SortOrder = {
    expressions.SortOrder(
      child = transformUnresolvedExpression(so.getExpression),
      direction = so.getDirection match {
        case proto.Sort.SortDirection.SORT_DIRECTION_DESCENDING => expressions.Descending
        case _ => expressions.Ascending
      },
      nullOrdering = so.getNulls match {
        case proto.Sort.SortNulls.SORT_NULLS_LAST => expressions.NullsLast
        case _ => expressions.NullsFirst
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
    assert(rel.hasInput)

    val groupingExprs =
      rel.getGroupingExpressionsList.asScala
        .map(transformExpression)
        .map {
          case ua @ UnresolvedAttribute(_) => ua
          case a @ Alias(_, _) => a
          case x => UnresolvedAlias(x)
        }

    // Retain group columns in aggregate expressions:
    val aggExprs =
      groupingExprs ++ rel.getResultExpressionsList.asScala.map(transformResultExpression)

    logical.Aggregate(
      child = transformRelation(rel.getInput),
      groupingExpressions = groupingExprs.toSeq,
      aggregateExpressions = aggExprs.toSeq)
  }

  private def transformResultExpression(exp: proto.Expression): expressions.NamedExpression = {
    if (exp.hasAlias) {
      transformAlias(exp.getAlias)
    } else {
      UnresolvedAlias(transformExpression(exp))
    }
  }

  def process(command: proto.Command): Unit = {
    command.getCommandTypeCase match {
      case proto.Command.CommandTypeCase.CREATE_FUNCTION =>
        handleCreateScalarFunction(command.getCreateFunction)
      case proto.Command.CommandTypeCase.WRITE_OPERATION =>
        handleWriteOperation(command.getWriteOperation)
      case proto.Command.CommandTypeCase.CREATE_DATAFRAME_VIEW =>
        handleCreateViewCommand(command.getCreateDataframeView)
      case _ => throw new UnsupportedOperationException(s"$command not supported.")
    }
  }

  /**
   * This is a helper function that registers a new Python function in the SparkSession.
   *
   * Right now this function is very rudimentary and bare-bones just to showcase how it is
   * possible to remotely serialize a Python function and execute it on the Spark cluster. If the
   * Python version on the client and server diverge, the execution of the function that is
   * serialized will most likely fail.
   *
   * @param cf
   */
  def handleCreateScalarFunction(cf: proto.CreateScalarFunction): Unit = {
    val function = SimplePythonFunction(
      cf.getSerializedFunction.toByteArray,
      Maps.newHashMap(),
      Lists.newArrayList(),
      pythonExec,
      "3.9", // TODO(SPARK-40532) This needs to be an actual Python version.
      Lists.newArrayList(),
      null)

    val udf = UserDefinedPythonFunction(
      cf.getPartsList.asScala.head,
      function,
      StringType,
      PythonEvalType.SQL_BATCHED_UDF,
      udfDeterministic = false)

    session.udf.registerPython(cf.getPartsList.asScala.head, udf)
  }

  def handleCreateViewCommand(createView: proto.CreateDataFrameViewCommand): Unit = {
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
      viewType = viewType,
      isAnalyzed = true)

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
  def handleWriteOperation(writeOperation: WriteOperation): Unit = {
    // Transform the input plan into the logical plan.
    val planner = new SparkConnectPlanner(session)
    val plan = planner.transformRelation(writeOperation.getInput)
    // And create a Dataset from the plan.
    val dataset = Dataset.ofRows(session, logicalPlan = plan)

    val w = dataset.write
    if (writeOperation.getMode != proto.WriteOperation.SaveMode.SAVE_MODE_UNSPECIFIED) {
      w.mode(DataTypeProtoConverter.toSaveMode(writeOperation.getMode))
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

    if (writeOperation.getSource != null) {
      w.format(writeOperation.getSource)
    }

    writeOperation.getSaveTypeCase match {
      case proto.WriteOperation.SaveTypeCase.PATH => w.save(writeOperation.getPath)
      case proto.WriteOperation.SaveTypeCase.TABLE_NAME =>
        w.saveAsTable(writeOperation.getTableName)
      case _ =>
        throw new UnsupportedOperationException(
          "WriteOperation:SaveTypeCase not supported "
            + s"${writeOperation.getSaveTypeCase.getNumber}")
    }
  }

}
