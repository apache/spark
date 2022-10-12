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

import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.Join.JoinType
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser

/**
 * A collection of implicit conversions that create a DSL for constructing connect protos.
 */

package object dsl {

  object expressions { // scalastyle:ignore
    implicit class DslString(val s: String) {
      val identifier = CatalystSqlParser.parseMultipartIdentifier(s)

      def protoAttr: proto.Expression =
        proto.Expression.newBuilder()
          .setUnresolvedAttribute(
            proto.Expression.UnresolvedAttribute.newBuilder()
              .addAllParts(identifier.asJava)
              .build())
          .build()
    }

    implicit class DslExpression(val expr: proto.Expression) {
      def as(alias: String): proto.Expression = proto.Expression.newBuilder().setAlias(
        proto.Expression.Alias.newBuilder().setName(alias).setExpr(expr)).build()
    }
  }

  object plans { // scalastyle:ignore
    implicit class DslLogicalPlan(val logicalPlan: proto.Relation) {
      def select(exprs: proto.Expression*): proto.Relation = {
        proto.Relation.newBuilder().setProject(
          proto.Project.newBuilder()
            .setInput(logicalPlan)
            .addAllExpressions(exprs.toIterable.asJava)
            .build()
        ).build()
      }

      def join(
          otherPlan: proto.Relation,
          joinType: JoinType = JoinType.JOIN_TYPE_INNER,
          condition: Option[proto.Expression] = None): proto.Relation = {
        val relation = proto.Relation.newBuilder()
        val join = proto.Join.newBuilder()
        join.setLeft(logicalPlan)
          .setRight(otherPlan)
          .setJoinType(joinType)
        if (condition.isDefined) {
          join.setJoinCondition(condition.get)
        }
        relation.setJoin(join).build()
      }

      def groupBy(
          groupingExprs: proto.Expression*)(aggregateExprs: proto.Expression*): proto.Relation = {
        val agg = proto.Aggregate.newBuilder()
        agg.setInput(logicalPlan)

        for (groupingExpr <- groupingExprs) {
          agg.addGroupingExpressions(groupingExpr)
        }
        // TODO: support aggregateExprs, which is blocked by supporting any builtin function
        // resolution only by name in the analyzer.
        proto.Relation.newBuilder().setAggregate(agg.build()).build()
      }
    }
  }
}
