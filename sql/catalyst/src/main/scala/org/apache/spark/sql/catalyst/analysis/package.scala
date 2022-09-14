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

package org.apache.spark.sql.catalyst

import java.util.UUID

import scala.collection.mutable

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.errors.QueryErrorsBase
import org.apache.spark.sql.internal.SQLConf

/**
 * Provides a logical query plan [[Analyzer]] and supporting classes for performing analysis.
 * Analysis consists of translating [[UnresolvedAttribute]]s and [[UnresolvedRelation]]s
 * into fully typed objects using information in a schema [[Catalog]].
 */
package object analysis {

  /**
   * Resolver should return true if the first string refers to the same entity as the second string.
   * For example, by using case insensitive equality.
   */
  type Resolver = (String, String) => Boolean

  val caseInsensitiveResolution = (a: String, b: String) => a.equalsIgnoreCase(b)
  val caseSensitiveResolution = (a: String, b: String) => a == b

  implicit class AnalysisErrorAt(t: TreeNode[_]) extends QueryErrorsBase {
    /** Fails the analysis at the point where a specific tree node was parsed. */
    def failAnalysis(msg: String): Nothing = {
      throw new AnalysisException(msg, t.origin.line, t.origin.startPosition)
    }

    /** Fails the analysis at the point where a specific tree node was parsed with a given cause. */
    def failAnalysis(msg: String, cause: Throwable): Nothing = {
      throw new AnalysisException(msg, t.origin.line, t.origin.startPosition, cause = Some(cause))
    }

    /**
     * Fails the analysis at the point where a specific tree node was parsed using a provided
     * error class and message parameters.
     */
    def failAnalysis(errorClass: String, messageParameters: Map[String, String]): Nothing = {
      throw new AnalysisException(
        errorClass = errorClass,
        messageParameters = messageParameters,
        origin = t.origin)
    }

    /**
     * Fails the analysis at the point where a specific tree node was parsed using a provided
     * error class and subclass and message parameters.
     */
    def failAnalysis(
        errorClass: String,
        errorSubClass: String,
        messageParameters: Map[String, String] = Map.empty[String, String]): Nothing = {
      throw new AnalysisException(
        errorClass = errorClass,
        errorSubClass = errorSubClass,
        messageParameters = messageParameters,
        origin = t.origin)
    }

    /**
     * Fails the analysis at the point where a specific tree node was parsed using a provided
     * error class and subclass and one message parameter comprising a plan string. The plan string
     * will be printed in the error message if and only if the corresponding Spark configuration is
     * enabled.
     */
    def failAnalysis(
        errorClass: String,
        errorSubClass: String,
        treeNodes: Seq[TreeNode[_]]): Nothing = {
      // Normalize expression IDs in the query plan to keep tests deterministic.
      object IdGen {
        private val curId = new java.util.concurrent.atomic.AtomicLong()
        private val jvmId = UUID.randomUUID()
        private val idMap = mutable.Map.empty[ExprId, ExprId]
        def get(previous: ExprId): ExprId = {
          if (!idMap.contains(previous)) idMap.put(previous, ExprId(curId.getAndIncrement(), jvmId))
          idMap.get(previous).get
        }
      }
      def normalizeExpr(expr: Expression): Expression = {
        expr.withNewChildren(expr.children.map(normalizeExpr)) match {
          case a: Attribute => a.withExprId(IdGen.get(a.exprId))
          case a: Alias => a.withExprId(IdGen.get(a.exprId))
          case n: NamedLambdaVariable => n.copy(exprId = IdGen.get(n.exprId))
          case o: OuterReference => o.copy(e = normalizeExpr(o.e).asInstanceOf[NamedExpression])
          case s: ScalarSubquery => s.copy(plan = normalizePlan(s.plan),
            exprId = IdGen.get(s.exprId), outerAttrs = s.outerAttrs.map(normalizeExpr),
            joinCond = s.joinCond.map(normalizeExpr))
          case s: Exists => s.copy(plan = normalizePlan(s.plan), exprId = IdGen.get(s.exprId),
            outerAttrs = s.outerAttrs.map(normalizeExpr), joinCond = s.joinCond.map(normalizeExpr))
          case s: LateralSubquery => s.copy(plan = normalizePlan(s.plan),
            exprId = IdGen.get(s.exprId), outerAttrs = s.outerAttrs.map(normalizeExpr),
            joinCond = s.joinCond.map(normalizeExpr))
          case s: InSubquery => s.copy(values = s.values.map(normalizeExpr),
            query = s.query.copy(plan = normalizePlan(s.query.plan),
              exprId = IdGen.get(s.query.exprId),
              childOutputs = s.query.childOutputs.map(a => a.withExprId(IdGen.get(a.exprId))),
              outerAttrs = s.query.outerAttrs.map(normalizeExpr),
              joinCond = s.query.joinCond.map(normalizeExpr)))
          case other => other
        }
      }
      def normalizePlan(node: LogicalPlan): LogicalPlan = {
        node.withNewChildren(node.children.map(normalizePlan)).mapExpressions(normalizeExpr)
      }
      val treeNodeString =
        if (SQLConf.get.includePlansInErrors) {
          s": ${
            treeNodes.map {
              case plan: LogicalPlan => normalizePlan(plan)
              case expr: Expression => normalizeExpr(expr)
            }.mkString("\n")
          }"
        } else {
          ""
        }
      throw new AnalysisException(
        errorClass = errorClass,
        errorSubClass = errorSubClass,
        messageParameters = Map("treeNode" -> treeNodeString),
        origin = t.origin)
    }

    def dataTypeMismatch(expr: Expression, mismatch: DataTypeMismatch): Nothing = {
      throw new AnalysisException(
        errorClass = "DATATYPE_MISMATCH",
        errorSubClass = mismatch.errorSubClass,
        messageParameters = mismatch.messageParameters + ("sqlExpr" -> toSQLExpr(expr)),
        origin = t.origin)
    }
  }

  /** Catches any AnalysisExceptions thrown by `f` and attaches `t`'s position if any. */
  def withPosition[A](t: TreeNode[_])(f: => A): A = {
    try f catch {
      case a: AnalysisException =>
        throw a.withPosition(t.origin)
    }
  }
}
