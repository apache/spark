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

import scala.collection.JavaConverters._
import scala.language.implicitConversions

import org.apache.spark.connect.proto
import org.apache.spark.connect.proto._
import org.apache.spark.connect.proto.Expression.ExpressionString
import org.apache.spark.connect.proto.Join.JoinType
import org.apache.spark.connect.proto.SetOperation.SetOpType
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.connect.planner.DataTypeProtoConverter

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

      def struct(attrs: Expression.QualifiedAttribute*): Expression.QualifiedAttribute = {
        val structExpr = DataType.Struct.newBuilder()
        for (attr <- attrs) {
          val structField = DataType.StructField.newBuilder()
          structField.setName(attr.getName)
          structField.setType(attr.getType)
          structExpr.addFields(structField)
        }
        Expression.QualifiedAttribute
          .newBuilder()
          .setName(s)
          .setType(DataType.newBuilder().setStruct(structExpr))
          .build()
      }

      /** Creates a new AttributeReference of type int */
      def int: Expression.QualifiedAttribute = protoQualifiedAttrWithType(
        DataType.newBuilder().setI32(DataType.I32.newBuilder()).build())

      private def protoQualifiedAttrWithType(dataType: DataType): Expression.QualifiedAttribute =
        Expression.QualifiedAttribute
          .newBuilder()
          .setName(s)
          .setType(dataType)
          .build()
    }

    implicit class DslExpression(val expr: Expression) {
      def as(alias: String): Expression = Expression
        .newBuilder()
        .setAlias(Expression.Alias.newBuilder().setName(alias).setExpr(expr))
        .build()

      def <(other: Expression): Expression =
        Expression
          .newBuilder()
          .setUnresolvedFunction(
            Expression.UnresolvedFunction
              .newBuilder()
              .addParts("<")
              .addArguments(expr)
              .addArguments(other))
          .build()
    }

    def proto_min(e: Expression): Expression =
      Expression
        .newBuilder()
        .setUnresolvedFunction(
          Expression.UnresolvedFunction.newBuilder().addParts("min").addArguments(e))
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
            .addAllParts(nameParts.asJava)
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
            .addParts(name)
            .addAllArguments(args.asJava))
        .build()
    }

    implicit def intToLiteral(i: Int): Expression =
      Expression
        .newBuilder()
        .setLiteral(Expression.Literal.newBuilder().setI32(i))
        .build()
  }

  object commands { // scalastyle:ignore
    implicit class DslCommands(val logicalPlan: Relation) {
      def write(
          format: Option[String] = None,
          path: Option[String] = None,
          tableName: Option[String] = None,
          mode: Option[String] = None,
          sortByColumns: Seq[String] = Seq.empty,
          partitionByCols: Seq[String] = Seq.empty,
          bucketByCols: Seq[String] = Seq.empty,
          numBuckets: Option[Int] = None): Command = {
        val writeOp = WriteOperation.newBuilder()
        format.foreach(writeOp.setSource(_))

        mode
          .map(SaveMode.valueOf(_))
          .map(DataTypeProtoConverter.toSaveModeProto(_))
          .foreach(writeOp.setMode(_))

        if (tableName.nonEmpty) {
          tableName.foreach(writeOp.setTableName(_))
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

    implicit class DslStatFunctions(val logicalPlan: Relation) {
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
    }

    def select(exprs: Expression*): Relation = {
      Relation
        .newBuilder()
        .setProject(
          Project
            .newBuilder()
            .addAllExpressions(exprs.toIterable.asJava)
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
              .addAllExpressions(exprs.toIterable.asJava)
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

      def deduplicate(colNames: Seq[String]): Relation =
        Relation
          .newBuilder()
          .setDeduplicate(
            Deduplicate
              .newBuilder()
              .setInput(logicalPlan)
              .addAllColumnNames(colNames.asJava))
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
          .newBuilder(logicalPlan)
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

      def createDefaultSortField(col: String): Sort.SortField = {
        Sort.SortField
          .newBuilder()
          .setNulls(Sort.SortNulls.SORT_NULLS_FIRST)
          .setDirection(Sort.SortDirection.SORT_DIRECTION_ASCENDING)
          .setExpression(
            Expression.newBuilder
              .setUnresolvedAttribute(
                Expression.UnresolvedAttribute.newBuilder.setUnparsedIdentifier(col).build())
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
              .addAllSortFields(columns.map(createDefaultSortField).asJava)
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
              .addAllSortFields(columns.map(createDefaultSortField).asJava)
              .setIsGlobal(false)
              .build())
          .build()
      }

      def groupBy(groupingExprs: Expression*)(aggregateExprs: Expression*): Relation = {
        val agg = Aggregate.newBuilder()
        agg.setInput(logicalPlan)

        for (groupingExpr <- groupingExprs) {
          agg.addGroupingExpressions(groupingExpr)
        }
        for (aggregateExpr <- aggregateExprs) {
          agg.addResultExpressions(aggregateExpr)
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

      def union(otherPlan: Relation, isAll: Boolean = true, byName: Boolean = false): Relation =
        Relation
          .newBuilder()
          .setSetOp(
            createSetOperation(
              logicalPlan,
              otherPlan,
              SetOpType.SET_OP_TYPE_UNION,
              isAll,
              byName))
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

      def repartition(num: Integer): Relation =
        Relation
          .newBuilder()
          .setRepartition(
            Repartition.newBuilder().setInput(logicalPlan).setNumPartitions(num).setShuffle(true))
          .build()

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

      def toDF(columnNames: String*): Relation =
        Relation
          .newBuilder()
          .setRenameColumnsBySameLengthNames(
            RenameColumnsBySameLengthNames
              .newBuilder()
              .setInput(logicalPlan)
              .addAllColumnNames(columnNames.asJava))
          .build()

      def withColumnsRenamed(renameColumnsMap: Map[String, String]): Relation = {
        Relation
          .newBuilder()
          .setRenameColumnsByNameToNameMap(
            RenameColumnsByNameToNameMap
              .newBuilder()
              .setInput(logicalPlan)
              .putAllRenameColumnsMap(renameColumnsMap.asJava))
          .build()
      }

      private def createSetOperation(
          left: Relation,
          right: Relation,
          t: SetOpType,
          isAll: Boolean = true,
          byName: Boolean = false): SetOperation.Builder = {
        val setOp = SetOperation
          .newBuilder()
          .setLeftInput(left)
          .setRightInput(right)
          .setSetOpType(t)
          .setIsAll(isAll)
          .setByName(byName)
        setOp
      }
    }
  }
}
