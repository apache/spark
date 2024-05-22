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
package org.apache.spark.sql.connect

import scala.jdk.CollectionConverters._
import scala.language.implicitConversions

import org.apache.spark.connect.proto
import org.apache.spark.connect.proto._
import org.apache.spark.connect.proto.Expression.ExpressionString
import org.apache.spark.connect.proto.Join.JoinType
import org.apache.spark.connect.proto.SetOperation.SetOpType
import org.apache.spark.sql.{Observation, SaveMode}
import org.apache.spark.sql.connect.common.DataTypeProtoConverter
import org.apache.spark.sql.connect.common.LiteralValueProtoConverter.toLiteralProto
import org.apache.spark.sql.connect.planner.{SaveModeConverter, TableSaveMethodConverter}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.Utils

/**
 * A collection of implicit conversions that create a DSL for constructing connect protos.
 *
 * All classes in connect/dsl are considered an internal API to Spark Connect and are subject to
 * change between minor releases.
 */

package object dsl {

  class MockRemoteSession {}

  object expressions { // scalastyle:ignore
    implicit class DslString(val s: String) {
      def protoAttr: Expression =
        Expression
          .newBuilder()
          .setUnresolvedAttribute(
            Expression.UnresolvedAttribute
              .newBuilder()
              .setUnparsedIdentifier(s))
          .build()

      def colRegex: Expression =
        Expression
          .newBuilder()
          .setUnresolvedRegex(
            Expression.UnresolvedRegex
              .newBuilder()
              .setColName(s))
          .build()

      def asc: Expression =
        Expression
          .newBuilder()
          .setSortOrder(
            Expression.SortOrder
              .newBuilder()
              .setChild(protoAttr)
              .setDirectionValue(
                proto.Expression.SortOrder.SortDirection.SORT_DIRECTION_ASCENDING_VALUE)
              .setNullOrdering(proto.Expression.SortOrder.NullOrdering.SORT_NULLS_FIRST))
          .build()
    }

    implicit class DslExpression(val expr: Expression) {
      def as(alias: String): Expression = Expression
        .newBuilder()
        .setAlias(Expression.Alias.newBuilder().addName(alias).setExpr(expr))
        .build()

      def as(alias: String, metadata: String): Expression = Expression
        .newBuilder()
        .setAlias(
          Expression.Alias
            .newBuilder()
            .setExpr(expr)
            .addName(alias)
            .setMetadata(metadata)
            .build())
        .build()

      def as(alias: Seq[String]): Expression = Expression
        .newBuilder()
        .setAlias(
          Expression.Alias
            .newBuilder()
            .setExpr(expr)
            .addAllName(alias.asJava)
            .build())
        .build()

      def <(other: Expression): Expression =
        Expression
          .newBuilder()
          .setUnresolvedFunction(
            Expression.UnresolvedFunction
              .newBuilder()
              .setFunctionName("<")
              .addArguments(expr)
              .addArguments(other))
          .build()

      def cast(dataType: DataType): Expression =
        Expression
          .newBuilder()
          .setCast(
            Expression.Cast
              .newBuilder()
              .setExpr(expr)
              .setType(dataType))
          .build()

      def cast(dataType: String): Expression =
        Expression
          .newBuilder()
          .setCast(
            Expression.Cast
              .newBuilder()
              .setExpr(expr)
              .setTypeStr(dataType))
          .build()
    }

    def proto_min(e: Expression): Expression =
      Expression
        .newBuilder()
        .setUnresolvedFunction(
          Expression.UnresolvedFunction.newBuilder().setFunctionName("min").addArguments(e))
        .build()

    def proto_max(e: Expression): Expression =
      Expression
        .newBuilder()
        .setUnresolvedFunction(
          Expression.UnresolvedFunction.newBuilder().setFunctionName("max").addArguments(e))
        .build()

    def proto_sum(e: Expression): Expression =
      Expression
        .newBuilder()
        .setUnresolvedFunction(
          Expression.UnresolvedFunction.newBuilder().setFunctionName("sum").addArguments(e))
        .build()

    def proto_explode(e: Expression): Expression =
      Expression
        .newBuilder()
        .setUnresolvedFunction(
          Expression.UnresolvedFunction.newBuilder().setFunctionName("explode").addArguments(e))
        .build()

    /**
     * Create an unresolved function from name parts.
     *
     * @param nameParts
     * @param args
     * @return
     *   Expression wrapping the unresolved function.
     */
    def callFunction(nameParts: Seq[String], args: Seq[Expression]): Expression = {
      Expression
        .newBuilder()
        .setUnresolvedFunction(
          Expression.UnresolvedFunction
            .newBuilder()
            .setFunctionName(nameParts.mkString("."))
            .setIsUserDefinedFunction(true)
            .addAllArguments(args.asJava))
        .build()
    }

    /**
     * Creates an UnresolvedFunction from a single identifier.
     *
     * @param name
     * @param args
     * @return
     *   Expression wrapping the unresolved function.
     */
    def callFunction(name: String, args: Seq[Expression]): Expression = {
      Expression
        .newBuilder()
        .setUnresolvedFunction(
          Expression.UnresolvedFunction
            .newBuilder()
            .setFunctionName(name)
            .addAllArguments(args.asJava))
        .build()
    }

    implicit def intToLiteral(i: Int): Expression =
      Expression
        .newBuilder()
        .setLiteral(Expression.Literal.newBuilder().setInteger(i))
        .build()
  }

  object commands { // scalastyle:ignore
    implicit class DslCommands(val logicalPlan: Relation) {
      def write(
          format: Option[String] = None,
          path: Option[String] = None,
          tableName: Option[String] = None,
          tableSaveMethod: Option[String] = None,
          mode: Option[String] = None,
          sortByColumns: Seq[String] = Seq.empty,
          partitionByCols: Seq[String] = Seq.empty,
          bucketByCols: Seq[String] = Seq.empty,
          numBuckets: Option[Int] = None): Command = {
        val writeOp = WriteOperation.newBuilder()
        format.foreach(writeOp.setSource(_))

        mode
          .map(SaveMode.valueOf(_))
          .map(SaveModeConverter.toSaveModeProto)
          .foreach(writeOp.setMode(_))

        if (tableName.nonEmpty) {
          tableName.foreach { tn =>
            val saveTable = WriteOperation.SaveTable.newBuilder().setTableName(tn)
            tableSaveMethod
              .map(TableSaveMethodConverter.toTableSaveMethodProto(_))
              .foreach(saveTable.setSaveMethod(_))
            writeOp.setTable(saveTable.build())
          }
        } else {
          path.foreach(writeOp.setPath(_))
        }
        sortByColumns.foreach(writeOp.addSortColumnNames(_))
        partitionByCols.foreach(writeOp.addPartitioningColumns(_))

        if (numBuckets.nonEmpty && bucketByCols.nonEmpty) {
          val op = WriteOperation.BucketBy.newBuilder()
          numBuckets.foreach(op.setNumBuckets(_))
          bucketByCols.foreach(op.addBucketColumnNames(_))
          writeOp.setBucketBy(op.build())
        }
        writeOp.setInput(logicalPlan)
        Command.newBuilder().setWriteOperation(writeOp.build()).build()
      }

      def createView(name: String, global: Boolean, replace: Boolean): Command = {
        Command
          .newBuilder()
          .setCreateDataframeView(
            CreateDataFrameViewCommand
              .newBuilder()
              .setName(name)
              .setIsGlobal(global)
              .setReplace(replace)
              .setInput(logicalPlan))
          .build()
      }

      def writeV2(
          tableName: Option[String] = None,
          provider: Option[String] = None,
          options: Map[String, String] = Map.empty,
          tableProperties: Map[String, String] = Map.empty,
          partitionByCols: Seq[Expression] = Seq.empty,
          mode: Option[String] = None,
          overwriteCondition: Option[Expression] = None): Command = {
        val writeOp = WriteOperationV2.newBuilder()
        writeOp.setInput(logicalPlan)
        tableName.foreach(writeOp.setTableName)
        provider.foreach(writeOp.setProvider)
        partitionByCols.foreach(writeOp.addPartitioningColumns)
        options.foreach { case (k, v) =>
          writeOp.putOptions(k, v)
        }
        tableProperties.foreach { case (k, v) =>
          writeOp.putTableProperties(k, v)
        }
        mode.foreach { m =>
          if (m == "MODE_CREATE") {
            writeOp.setMode(WriteOperationV2.Mode.MODE_CREATE)
          } else if (m == "MODE_OVERWRITE") {
            writeOp.setMode(WriteOperationV2.Mode.MODE_OVERWRITE)
            overwriteCondition.foreach(writeOp.setOverwriteCondition)
          } else if (m == "MODE_OVERWRITE_PARTITIONS") {
            writeOp.setMode(WriteOperationV2.Mode.MODE_OVERWRITE_PARTITIONS)
          } else if (m == "MODE_APPEND") {
            writeOp.setMode(WriteOperationV2.Mode.MODE_APPEND)
          } else if (m == "MODE_REPLACE") {
            writeOp.setMode(WriteOperationV2.Mode.MODE_REPLACE)
          } else if (m == "MODE_CREATE_OR_REPLACE") {
            writeOp.setMode(WriteOperationV2.Mode.MODE_CREATE_OR_REPLACE)
          }
        }
        Command.newBuilder().setWriteOperationV2(writeOp.build()).build()
      }
    }
  }

  object plans { // scalastyle:ignore
    implicit class DslMockRemoteSession(val session: MockRemoteSession) {
      def range(
          start: Option[Long],
          end: Long,
          step: Option[Long],
          numPartitions: Option[Int]): Relation = {
        val range = proto.Range.newBuilder()
        if (start.isDefined) {
          range.setStart(start.get)
        }
        range.setEnd(end)
        if (step.isDefined) {
          range.setStep(step.get)
        } else {
          range.setStep(1L)
        }
        if (numPartitions.isDefined) {
          range.setNumPartitions(numPartitions.get)
        }
        Relation.newBuilder().setRange(range).build()
      }

      def sql(sqlText: String): Relation = {
        Relation.newBuilder().setSql(SQL.newBuilder().setQuery(sqlText)).build()
      }
    }

    implicit class DslNAFunctions(val logicalPlan: Relation) {

      def fillValue(value: Any): Relation = {
        Relation
          .newBuilder()
          .setFillNa(
            proto.NAFill
              .newBuilder()
              .setInput(logicalPlan)
              .addAllValues(Seq(toLiteralProto(value)).asJava)
              .build())
          .build()
      }

      def fillColumns(value: Any, cols: Seq[String]): Relation = {
        Relation
          .newBuilder()
          .setFillNa(
            proto.NAFill
              .newBuilder()
              .setInput(logicalPlan)
              .addAllCols(cols.asJava)
              .addAllValues(Seq(toLiteralProto(value)).asJava)
              .build())
          .build()
      }

      def fillValueMap(valueMap: Map[String, Any]): Relation = {
        val (cols, values) = valueMap.transform((_, v) => toLiteralProto(v)).toSeq.unzip
        Relation
          .newBuilder()
          .setFillNa(
            proto.NAFill
              .newBuilder()
              .setInput(logicalPlan)
              .addAllCols(cols.asJava)
              .addAllValues(values.asJava)
              .build())
          .build()
      }

      def drop(
          how: Option[String] = None,
          minNonNulls: Option[Int] = None,
          cols: Seq[String] = Seq.empty): Relation = {
        require(!(how.nonEmpty && minNonNulls.nonEmpty))
        require(how.isEmpty || Seq("any", "all").contains(how.get))

        val dropna = proto.NADrop
          .newBuilder()
          .setInput(logicalPlan)

        if (cols.nonEmpty) {
          dropna.addAllCols(cols.asJava)
        }

        var _minNonNulls = -1
        how match {
          case Some("all") => _minNonNulls = 1
          case _ =>
        }
        if (minNonNulls.nonEmpty) {
          _minNonNulls = minNonNulls.get
        }
        if (_minNonNulls > 0) {
          dropna.setMinNonNulls(_minNonNulls)
        }

        Relation
          .newBuilder()
          .setDropNa(dropna.build())
          .build()
      }

      def replace(cols: Seq[String], replacement: Map[Any, Any]): Relation = {
        require(cols.nonEmpty)

        val replace = proto.NAReplace
          .newBuilder()
          .setInput(logicalPlan)

        if (!(cols.length == 1 && cols.head == "*")) {
          replace.addAllCols(cols.asJava)
        }

        replacement.foreach { case (oldValue, newValue) =>
          replace.addReplacements(
            proto.NAReplace.Replacement
              .newBuilder()
              .setOldValue(toLiteralProto(oldValue))
              .setNewValue(toLiteralProto(newValue)))
        }

        Relation
          .newBuilder()
          .setReplace(replace.build())
          .build()
      }
    }

    implicit class DslStatFunctions(val logicalPlan: Relation) {
      def cov(col1: String, col2: String): Relation = {
        Relation
          .newBuilder()
          .setCov(
            proto.StatCov
              .newBuilder()
              .setInput(logicalPlan)
              .setCol1(col1)
              .setCol2(col2)
              .build())
          .build()
      }

      def corr(col1: String, col2: String, method: String): Relation = {
        Relation
          .newBuilder()
          .setCorr(
            proto.StatCorr
              .newBuilder()
              .setInput(logicalPlan)
              .setCol1(col1)
              .setCol2(col2)
              .setMethod(method)
              .build())
          .build()
      }

      def corr(col1: String, col2: String): Relation = corr(col1, col2, "pearson")

      def approxQuantile(
          cols: Array[String],
          probabilities: Array[Double],
          relativeError: Double): Relation = {
        Relation
          .newBuilder()
          .setApproxQuantile(
            proto.StatApproxQuantile
              .newBuilder()
              .setInput(logicalPlan)
              .addAllCols(cols.toImmutableArraySeq.asJava)
              .addAllProbabilities(probabilities.toImmutableArraySeq.map(Double.box).asJava)
              .setRelativeError(relativeError)
              .build())
          .build()
      }

      def crosstab(col1: String, col2: String): Relation = {
        Relation
          .newBuilder()
          .setCrosstab(
            proto.StatCrosstab
              .newBuilder()
              .setInput(logicalPlan)
              .setCol1(col1)
              .setCol2(col2)
              .build())
          .build()
      }

      def freqItems(cols: Array[String], support: Double): Relation = {
        Relation
          .newBuilder()
          .setFreqItems(
            proto.StatFreqItems
              .newBuilder()
              .setInput(logicalPlan)
              .addAllCols(cols.toImmutableArraySeq.asJava)
              .setSupport(support)
              .build())
          .build()
      }

      def freqItems(cols: Array[String]): Relation = freqItems(cols, 0.01)

      def freqItems(cols: Seq[String], support: Double): Relation =
        freqItems(cols.toArray, support)

      def freqItems(cols: Seq[String]): Relation = freqItems(cols, 0.01)

      def sampleBy(col: String, fractions: Map[Any, Double], seed: Long): Relation = {
        Relation
          .newBuilder()
          .setSampleBy(
            StatSampleBy
              .newBuilder()
              .setInput(logicalPlan)
              .addAllFractions(fractions.toSeq.map { case (k, v) =>
                StatSampleBy.Fraction
                  .newBuilder()
                  .setStratum(toLiteralProto(k))
                  .setFraction(v)
                  .build()
              }.asJava)
              .setSeed(seed)
              .build())
          .build()
      }
    }

    def select(exprs: Expression*): Relation = {
      Relation
        .newBuilder()
        .setProject(
          Project
            .newBuilder()
            .addAllExpressions(exprs.asJava)
            .build())
        .build()
    }

    implicit class DslLogicalPlan(val logicalPlan: Relation) {
      def select(exprs: Expression*): Relation = {
        Relation
          .newBuilder()
          .setProject(
            Project
              .newBuilder()
              .setInput(logicalPlan)
              .addAllExpressions(exprs.asJava)
              .build())
          .build()
      }

      def selectExpr(exprs: String*): Relation =
        select(exprs.map { expr =>
          Expression
            .newBuilder()
            .setExpressionString(ExpressionString.newBuilder().setExpression(expr))
            .build()
        }: _*)

      def tail(limit: Int): Relation = {
        Relation
          .newBuilder()
          .setTail(
            Tail
              .newBuilder()
              .setInput(logicalPlan)
              .setLimit(limit))
          .build()
      }

      def limit(limit: Int): Relation = {
        Relation
          .newBuilder()
          .setLimit(
            Limit
              .newBuilder()
              .setInput(logicalPlan)
              .setLimit(limit))
          .build()
      }

      def offset(offset: Int): Relation = {
        Relation
          .newBuilder()
          .setOffset(
            Offset
              .newBuilder()
              .setInput(logicalPlan)
              .setOffset(offset))
          .build()
      }

      def where(condition: Expression): Relation = {
        Relation
          .newBuilder()
          .setFilter(Filter.newBuilder().setInput(logicalPlan).setCondition(condition))
          .build()
      }

      def filter(condition: Expression): Relation = {
        where(condition)
      }

      def deduplicate(colNames: Seq[String]): Relation =
        Relation
          .newBuilder()
          .setDeduplicate(
            Deduplicate
              .newBuilder()
              .setInput(logicalPlan)
              .addAllColumnNames(colNames.asJava))
          .build()

      def deduplicateWithinWatermark(colNames: Seq[String]): Relation =
        Relation
          .newBuilder()
          .setDeduplicate(
            Deduplicate
              .newBuilder()
              .setInput(logicalPlan)
              .addAllColumnNames(colNames.asJava)
              .setWithinWatermark(true))
          .build()

      def distinct(): Relation =
        Relation
          .newBuilder()
          .setDeduplicate(
            Deduplicate
              .newBuilder()
              .setInput(logicalPlan)
              .setAllColumnsAsKeys(true))
          .build()

      def join(
          otherPlan: Relation,
          joinType: JoinType,
          condition: Option[Expression]): Relation = {
        join(otherPlan, joinType, Seq(), condition)
      }

      def join(otherPlan: Relation, condition: Option[Expression]): Relation = {
        join(otherPlan, JoinType.JOIN_TYPE_INNER, Seq(), condition)
      }

      def join(otherPlan: Relation): Relation = {
        join(otherPlan, JoinType.JOIN_TYPE_INNER, Seq(), None)
      }

      def join(otherPlan: Relation, joinType: JoinType): Relation = {
        join(otherPlan, joinType, Seq(), None)
      }

      def join(otherPlan: Relation, joinType: JoinType, usingColumns: Seq[String]): Relation = {
        join(otherPlan, joinType, usingColumns, None)
      }

      def crossJoin(otherPlan: Relation): Relation = {
        join(otherPlan, JoinType.JOIN_TYPE_CROSS, Seq(), None)
      }

      private def join(
          otherPlan: Relation,
          joinType: JoinType = JoinType.JOIN_TYPE_INNER,
          usingColumns: Seq[String],
          condition: Option[Expression]): Relation = {
        val relation = Relation.newBuilder()
        val join = Join.newBuilder()
        join
          .setLeft(logicalPlan)
          .setRight(otherPlan)
          .setJoinType(joinType)
        if (usingColumns.nonEmpty) {
          join.addAllUsingColumns(usingColumns.asJava)
        }
        if (condition.isDefined) {
          join.setJoinCondition(condition.get)
        }
        relation.setJoin(join).build()
      }

      def as(alias: String): Relation = {
        Relation
          .newBuilder()
          .setSubqueryAlias(SubqueryAlias.newBuilder().setAlias(alias).setInput(logicalPlan))
          .build()
      }

      def sample(
          lowerBound: Double,
          upperBound: Double,
          withReplacement: Boolean,
          seed: Long): Relation = {
        Relation
          .newBuilder()
          .setSample(
            Sample
              .newBuilder()
              .setInput(logicalPlan)
              .setUpperBound(upperBound)
              .setLowerBound(lowerBound)
              .setWithReplacement(withReplacement)
              .setSeed(seed)
              .build())
          .build()
      }

      private def createDefaultSortField(col: String): Expression.SortOrder = {
        Expression.SortOrder
          .newBuilder()
          .setNullOrdering(Expression.SortOrder.NullOrdering.SORT_NULLS_FIRST)
          .setDirection(Expression.SortOrder.SortDirection.SORT_DIRECTION_ASCENDING)
          .setChild(
            Expression
              .newBuilder()
              .setUnresolvedAttribute(
                Expression.UnresolvedAttribute.newBuilder().setUnparsedIdentifier(col).build())
              .build())
          .build()
      }

      def sort(columns: String*): Relation = {
        Relation
          .newBuilder()
          .setSort(
            Sort
              .newBuilder()
              .setInput(logicalPlan)
              .addAllOrder(columns.map(createDefaultSortField).asJava)
              .setIsGlobal(true)
              .build())
          .build()
      }

      def sortWithinPartitions(columns: String*): Relation = {
        Relation
          .newBuilder()
          .setSort(
            Sort
              .newBuilder()
              .setInput(logicalPlan)
              .addAllOrder(columns.map(createDefaultSortField).asJava)
              .setIsGlobal(false)
              .build())
          .build()
      }

      def drop(columns: String*): Relation = {
        assert(columns.nonEmpty)

        Relation
          .newBuilder()
          .setDrop(
            Drop
              .newBuilder()
              .setInput(logicalPlan)
              .addAllColumnNames(columns.toSeq.asJava)
              .build())
          .build()
      }

      def groupBy(groupingExprs: Expression*)(aggregateExprs: Expression*): Relation = {
        val agg = Aggregate.newBuilder()
        agg.setInput(logicalPlan)
        agg.setGroupType(proto.Aggregate.GroupType.GROUP_TYPE_GROUPBY)

        for (groupingExpr <- groupingExprs) {
          agg.addGroupingExpressions(groupingExpr)
        }
        for (aggregateExpr <- aggregateExprs) {
          agg.addAggregateExpressions(aggregateExpr)
        }
        Relation.newBuilder().setAggregate(agg.build()).build()
      }

      def rollup(groupingExprs: Expression*)(aggregateExprs: Expression*): Relation = {
        val agg = Aggregate.newBuilder()
        agg.setInput(logicalPlan)
        agg.setGroupType(proto.Aggregate.GroupType.GROUP_TYPE_ROLLUP)

        for (groupingExpr <- groupingExprs) {
          agg.addGroupingExpressions(groupingExpr)
        }
        for (aggregateExpr <- aggregateExprs) {
          agg.addAggregateExpressions(aggregateExpr)
        }
        Relation.newBuilder().setAggregate(agg.build()).build()
      }

      def cube(groupingExprs: Expression*)(aggregateExprs: Expression*): Relation = {
        val agg = Aggregate.newBuilder()
        agg.setInput(logicalPlan)
        agg.setGroupType(proto.Aggregate.GroupType.GROUP_TYPE_CUBE)

        for (groupingExpr <- groupingExprs) {
          agg.addGroupingExpressions(groupingExpr)
        }
        for (aggregateExpr <- aggregateExprs) {
          agg.addAggregateExpressions(aggregateExpr)
        }
        Relation.newBuilder().setAggregate(agg.build()).build()
      }

      def pivot(groupingExprs: Expression*)(
          pivotCol: Expression,
          pivotValues: Seq[proto.Expression.Literal])(aggregateExprs: Expression*): Relation = {
        val agg = Aggregate.newBuilder()
        agg.setInput(logicalPlan)
        agg.setGroupType(proto.Aggregate.GroupType.GROUP_TYPE_PIVOT)

        for (groupingExpr <- groupingExprs) {
          agg.addGroupingExpressions(groupingExpr)
        }
        for (aggregateExpr <- aggregateExprs) {
          agg.addAggregateExpressions(aggregateExpr)
        }
        agg.setPivot(
          Aggregate.Pivot.newBuilder().setCol(pivotCol).addAllValues(pivotValues.asJava).build())

        Relation.newBuilder().setAggregate(agg.build()).build()
      }

      def groupingSets(groupingSets: Seq[Seq[Expression]], groupingExprs: Expression*)(
          aggregateExprs: Expression*): Relation = {
        val agg = Aggregate.newBuilder()
        agg.setInput(logicalPlan)
        agg.setGroupType(proto.Aggregate.GroupType.GROUP_TYPE_GROUPING_SETS)
        for (groupingSet <- groupingSets) {
          val groupingSetMsg = Aggregate.GroupingSets.newBuilder()
          for (groupCol <- groupingSet) {
            groupingSetMsg.addGroupingSet(groupCol)
          }
          agg.addGroupingSets(groupingSetMsg)
        }
        for (groupingExpr <- groupingExprs) {
          agg.addGroupingExpressions(groupingExpr)
        }
        for (aggregateExpr <- aggregateExprs) {
          agg.addAggregateExpressions(aggregateExpr)
        }
        Relation.newBuilder().setAggregate(agg.build()).build()
      }

      def except(otherPlan: Relation, isAll: Boolean): Relation = {
        Relation
          .newBuilder()
          .setSetOp(
            createSetOperation(logicalPlan, otherPlan, SetOpType.SET_OP_TYPE_EXCEPT, isAll))
          .build()
      }

      def intersect(otherPlan: Relation, isAll: Boolean): Relation =
        Relation
          .newBuilder()
          .setSetOp(
            createSetOperation(logicalPlan, otherPlan, SetOpType.SET_OP_TYPE_INTERSECT, isAll))
          .build()

      def union(
          otherPlan: Relation,
          isAll: Boolean = true,
          byName: Boolean = false,
          allowMissingColumns: Boolean = false): Relation =
        Relation
          .newBuilder()
          .setSetOp(
            createSetOperation(
              logicalPlan,
              otherPlan,
              SetOpType.SET_OP_TYPE_UNION,
              isAll,
              byName,
              allowMissingColumns))
          .build()

      def coalesce(num: Integer): Relation =
        Relation
          .newBuilder()
          .setRepartition(
            Repartition
              .newBuilder()
              .setInput(logicalPlan)
              .setNumPartitions(num)
              .setShuffle(false))
          .build()

      def repartition(num: Int): Relation =
        Relation
          .newBuilder()
          .setRepartition(
            Repartition.newBuilder().setInput(logicalPlan).setNumPartitions(num).setShuffle(true))
          .build()

      @scala.annotation.varargs
      def repartition(partitionExprs: Expression*): Relation = {
        repartition(None, partitionExprs)
      }

      @scala.annotation.varargs
      def repartition(num: Int, partitionExprs: Expression*): Relation = {
        repartition(Some(num), partitionExprs)
      }

      private def repartition(numOpt: Option[Int], partitionExprs: Seq[Expression]): Relation = {
        val expressions = RepartitionByExpression
          .newBuilder()
          .setInput(logicalPlan)
        numOpt.foreach(expressions.setNumPartitions)
        for (expr <- partitionExprs) {
          expressions.addPartitionExprs(expr)
        }
        Relation
          .newBuilder()
          .setRepartitionByExpression(expressions)
          .build()
      }

      @scala.annotation.varargs
      def repartitionByRange(partitionExprs: Expression*): Relation = {
        repartitionByRange(None, partitionExprs)
      }

      @scala.annotation.varargs
      def repartitionByRange(num: Int, partitionExprs: Expression*): Relation = {
        repartitionByRange(Some(num), partitionExprs)
      }

      private def repartitionByRange(
          numOpt: Option[Int],
          partitionExprs: Seq[Expression]): Relation = {
        val expressions = RepartitionByExpression
          .newBuilder()
          .setInput(logicalPlan)
        numOpt.foreach(expressions.setNumPartitions)
        partitionExprs
          .map(expr =>
            expr.getExprTypeCase match {
              case Expression.ExprTypeCase.SORT_ORDER => expr
              case _ =>
                Expression
                  .newBuilder()
                  .setSortOrder(
                    Expression.SortOrder
                      .newBuilder()
                      .setChild(expr)
                      .setDirectionValue(
                        proto.Expression.SortOrder.SortDirection.SORT_DIRECTION_ASCENDING_VALUE)
                      .setNullOrdering(proto.Expression.SortOrder.NullOrdering.SORT_NULLS_FIRST))
                  .build()
            })
          .foreach(order => expressions.addPartitionExprs(order))
        Relation
          .newBuilder()
          .setRepartitionByExpression(expressions)
          .build()
      }

      def na: DslNAFunctions = new DslNAFunctions(logicalPlan)

      def stat: DslStatFunctions = new DslStatFunctions(logicalPlan)

      def summary(statistics: String*): Relation = {
        Relation
          .newBuilder()
          .setSummary(
            proto.StatSummary
              .newBuilder()
              .setInput(logicalPlan)
              .addAllStatistics(statistics.toSeq.asJava)
              .build())
          .build()
      }

      def describe(cols: String*): Relation = {
        Relation
          .newBuilder()
          .setDescribe(
            proto.StatDescribe
              .newBuilder()
              .setInput(logicalPlan)
              .addAllCols(cols.toSeq.asJava)
              .build())
          .build()
      }

      def to(schema: StructType): Relation =
        Relation
          .newBuilder()
          .setToSchema(
            ToSchema
              .newBuilder()
              .setInput(logicalPlan)
              .setSchema(DataTypeProtoConverter.toConnectProtoType(schema))
              .build())
          .build()

      def toDF(columnNames: String*): Relation =
        Relation
          .newBuilder()
          .setToDf(
            ToDF
              .newBuilder()
              .setInput(logicalPlan)
              .addAllColumnNames(columnNames.asJava))
          .build()

      def withColumnsRenamed(renameColumnsMap: Map[String, String]): Relation = {
        Relation
          .newBuilder()
          .setWithColumnsRenamed(
            WithColumnsRenamed
              .newBuilder()
              .setInput(logicalPlan)
              .addAllRenames(renameColumnsMap.toSeq.map { case (k, v) =>
                WithColumnsRenamed.Rename
                  .newBuilder()
                  .setColName(k)
                  .setNewColName(v)
                  .build()
              }.asJava))
          .build()
      }

      def withColumns(colsMap: Map[String, Expression]): Relation = {
        Relation
          .newBuilder()
          .setWithColumns(
            WithColumns
              .newBuilder()
              .setInput(logicalPlan)
              .addAllAliases(colsMap.map { case (k, v) =>
                Expression.Alias.newBuilder().addName(k).setExpr(v).build()
              }.asJava))
          .build()
      }

      def hint(name: String, parameters: Any*): Relation = {
        val expressions = parameters.map { parameter =>
          proto.Expression.newBuilder().setLiteral(toLiteralProto(parameter)).build()
        }

        Relation
          .newBuilder()
          .setHint(
            Hint
              .newBuilder()
              .setInput(logicalPlan)
              .setName(name)
              .addAllParameters(expressions.asJava))
          .build()
      }

      def unpivot(
          ids: Seq[Expression],
          values: Seq[Expression],
          variableColumnName: String,
          valueColumnName: String): Relation = {
        Relation
          .newBuilder()
          .setUnpivot(
            Unpivot
              .newBuilder()
              .setInput(logicalPlan)
              .addAllIds(ids.asJava)
              .setValues(Unpivot.Values
                .newBuilder()
                .addAllValues(values.asJava)
                .build())
              .setVariableColumnName(variableColumnName)
              .setValueColumnName(valueColumnName))
          .build()
      }

      def unpivot(
          ids: Seq[Expression],
          variableColumnName: String,
          valueColumnName: String): Relation = {
        Relation
          .newBuilder()
          .setUnpivot(
            Unpivot
              .newBuilder()
              .setInput(logicalPlan)
              .addAllIds(ids.asJava)
              .setVariableColumnName(variableColumnName)
              .setValueColumnName(valueColumnName))
          .build()
      }

      def melt(
          ids: Seq[Expression],
          values: Seq[Expression],
          variableColumnName: String,
          valueColumnName: String): Relation =
        unpivot(ids, values, variableColumnName, valueColumnName)

      def melt(
          ids: Seq[Expression],
          variableColumnName: String,
          valueColumnName: String): Relation =
        unpivot(ids, variableColumnName, valueColumnName)

      def randomSplit(weights: Array[Double], seed: Long): Array[Relation] = {
        require(
          weights.forall(_ >= 0),
          s"Weights must be non-negative, but got ${weights.mkString("[", ",", "]")}")
        require(
          weights.sum > 0,
          s"Sum of weights must be positive, but got ${weights.mkString("[", ",", "]")}")

        val sum = weights.toImmutableArraySeq.sum
        val normalizedCumWeights = weights.map(_ / sum).scanLeft(0.0d)(_ + _)
        normalizedCumWeights
          .sliding(2)
          .map { x =>
            Relation
              .newBuilder()
              .setSample(
                Sample
                  .newBuilder()
                  .setInput(logicalPlan)
                  .setLowerBound(x(0))
                  .setUpperBound(x(1))
                  .setWithReplacement(false)
                  .setSeed(seed)
                  .setDeterministicOrder(true)
                  .build())
              .build()
          }
          .toArray
      }

      def randomSplit(weights: Array[Double]): Array[Relation] =
        randomSplit(weights, Utils.random.nextLong)

      def observe(name: String, expr: Expression, exprs: Expression*): Relation = {
        Relation
          .newBuilder()
          .setCollectMetrics(
            CollectMetrics
              .newBuilder()
              .setInput(logicalPlan)
              .setName(name)
              .addAllMetrics((expr +: exprs).asJava))
          .build()
      }

      def observe(observation: Observation, expr: Expression, exprs: Expression*): Relation = {
        Relation
          .newBuilder()
          .setCollectMetrics(
            CollectMetrics
              .newBuilder()
              .setInput(logicalPlan)
              .setName(observation.name)
              .addAllMetrics((expr +: exprs).asJava))
          .build()
      }

      private def createSetOperation(
          left: Relation,
          right: Relation,
          t: SetOpType,
          isAll: Boolean = true,
          byName: Boolean = false,
          allowMissingColumns: Boolean = false): SetOperation.Builder = {
        val setOp = SetOperation
          .newBuilder()
          .setLeftInput(left)
          .setRightInput(right)
          .setSetOpType(t)
          .setIsAll(isAll)
          .setByName(byName)
          .setAllowMissingColumns(allowMissingColumns)
        setOp
      }
    }
  }
}
