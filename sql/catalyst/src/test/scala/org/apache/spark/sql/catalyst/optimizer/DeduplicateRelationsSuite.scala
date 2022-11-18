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

package org.apache.spark.sql.catalyst.optimizer

import scala.annotation.tailrec

import org.apache.spark.sql.catalyst.analysis.DeduplicateRelations
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.IntegerType

class DeduplicateRelationsSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
        Batch("Resolution", FixedPoint(10),
            DeduplicateRelations) :: Nil
  }

  val value = AttributeReference("value", IntegerType)()
  val testRelation = LocalRelation(value)


  test("SPARK-41162: deduplicate referenced expression ids in join") {
    withSQLConf(SQLConf.PLAN_CHANGE_LOG_LEVEL.key -> "error") {
      val relation = testRelation.select($"value".as("a")).deduplicate()
      val left = relation.select(($"a" + 1).as("a"))
      val right = relation
      val originalQuery = left.join(right, UsingJoin(Inner, Seq("a")))
      val optimized = Optimize.execute(originalQuery.analyze)

      def exprIds(plan: LogicalPlan): Set[Long] =
        plan.children.flatMap(exprIds).toSet ++ plan.expressions.map {
          case ne: NamedExpression => ne.exprId.id
          case _ => 0L
        }.toSet

      @tailrec
      def planDeduplicated(plan: LogicalPlan): Boolean = plan.children match {
        case Seq(child) => planDeduplicated(child)
        case children =>
          // collect all expression ids of each children and index children idx by exprId
          val childIdxByExprId = children.map(exprIds).zipWithIndex.flatMap {
            case (set, idx) => set.map(id => (id, idx))
          }.groupBy(_._1).mapValues(_.map(_._2))

          // each exprId should occur in exactly one child
          plan.resolved && childIdxByExprId.values.forall(_.length == 1)
      }

      assert(planDeduplicated(optimized), optimized)
    }
  }

  test("SPARK-41162: deduplicate referenced expression ids in lateral join") {
    withSQLConf(SQLConf.PLAN_CHANGE_LOG_LEVEL.key -> "error") {
      val relation = testRelation.select($"value".as("a")).deduplicate()
      val left = relation.select(($"a" + 1).as("a"))
      val right = relation
      val cond = Some(left.analyze.output.head === right.analyze.output.head)
      val originalQuery = left.lateralJoin(right, UsingJoin(Inner, Seq("a")))
      val optimized = Optimize.execute(originalQuery.analyze)

      def children(plan: LogicalPlan): Seq[LogicalPlan] = plan match {
        case lj: LateralJoin => lj.child :: lj.right.plan :: Nil
        case p: LogicalPlan => p.children
      }

      def exprIds(plan: LogicalPlan): Set[Long] =
        children(plan).flatMap(exprIds).toSet ++ plan.expressions.map {
          case ne: NamedExpression => ne.exprId.id
          case _ => 0L
        }.toSet

      @tailrec
      def planDeduplicated(plan: LogicalPlan): Boolean = children(plan) match {
        case Seq(child) => planDeduplicated(child)
        case children =>
          // collect all expression ids of each children and index children idx by exprId
          val childIdxByExprId = children.map(exprIds).zipWithIndex.flatMap {
            case (set, idx) => set.map(id => (id, idx))
          }.groupBy(_._1).mapValues(_.map(_._2))

          // each exprId should occur in exactly one child
          plan.resolved && childIdxByExprId.values.forall(_.length == 1)
      }

      assert(planDeduplicated(optimized), optimized)
    }
  }

  // Problem: deduplicating attributes already referenced will break those old references

}
