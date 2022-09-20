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

package org.apache.spark.sql.catalyst.plans

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.analysis.{Analyzer, EliminateSubqueryAliases,
  EmptyFunctionRegistry, FakeV2SessionCatalog}
import org.apache.spark.sql.catalyst.catalog.{InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{IsNotNull, _}
import org.apache.spark.sql.catalyst.optimizer.{CombineFilters, CombineUnions,
  InferFiltersFromConstraints, Optimizer, PruneFilters, PushDownPredicates,
  PushPredicateThroughJoin, PushProjectionThroughUnion}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.internal.SQLConf

class CompareNewAndOldConstraintsSuite extends SparkFunSuite with PlanTest with PredicateHelper {
  test("new filter pushed down on Join Node with multiple join conditions") {
    val tr1 = LocalRelation('a.int, 'b.string, 'c.int)
    val tr2 = LocalRelation('x.int, 'y.string, 'z.int)
    val query = tr1.where('c.attr + 'a.attr > 10 && 'a.attr > -15).
      select('a, 'a.as('a1), 'a.as('a2),
        'b.as('b1), 'c, 'c.as('c1)).
      join(tr2, Inner, Some("a2".attr === "x".attr && 'c1.attr === 'z.attr))
      .where('a1.attr + 'c1.attr > 10)

    withSQLConf() {
      val optimized = executePlan(query, OptimizerTypes.WITH_FILTER_PUSHDOWN_THRU_JOIN_AND_PRUNING)
      val correctAnswer = tr1.where('c.attr + 'a.attr > 10 && 'a.attr > -15 &&
        IsNotNull('a) && IsNotNull('c)).select('a, 'a.as('a1), 'a.as('a2),
        'b.as('b1), 'c,
        'c.as('c1)).join(tr2.where(IsNotNull('x) && IsNotNull('z) && 'x.attr > -15
        && 'z.attr + 'x.attr > 10),
        Inner, Some("a2".attr === "x".attr && 'c1.attr === 'z.attr)).analyze

      comparePlans(optimized, correctAnswer)
    }
  }

  test("test pruning using constraints with filters after project - 2") {
    def getTestPlan: LogicalPlan = {
      val tr1 = LocalRelation('a.int, 'b.string, 'c.int)
      tr1.select('a, 'a.as('a1), 'a.as('a2),
        'b.as('b1), 'c, 'c.as('c1)).where('c.attr + 'a.attr > 10 && 'a.attr > -15).
        where('c1.attr + 'a2.attr > 10 && 'a2.attr > -15)
    }

    withSQLConf() {
      val optimizedPlan = executePlan(getTestPlan, OptimizerTypes.NO_PUSH_DOWN_ONLY_PRUNING)
      val correctAnswer = LocalRelation('a.int, 'b.string, 'c.int).
        select('a, 'a.as('a1), 'a.as('a2),
          'b.as('b1), 'c, 'c.as('c1)).where('c.attr + 'a.attr > 10 && 'a.attr > -15
        && IsNotNull('a) && IsNotNull('c)).analyze
      comparePlans(correctAnswer, optimizedPlan)
    }
  }

  test("test pruning using constraints with filters after project - 3") {
    def getTestPlan: LogicalPlan = {
      val tr1 = LocalRelation('a.int, 'b.string, 'c.int)
      tr1.select('a, 'a.as('a1), 'a.as('a2),
        'b.as('b1), 'c, 'c.as('c1)).where('c1.attr + 'a1.attr > 10 && 'a2.attr > -15).
        where('c.attr + 'a.attr > 10 && 'a.attr > -15)
    }

    withSQLConf() {
      val optimizedPlan = executePlan(getTestPlan, OptimizerTypes.NO_PUSH_DOWN_ONLY_PRUNING)
      val correctAnswer = LocalRelation('a.int, 'b.string, 'c.int).
        select('a, 'a.as('a1), 'a.as('a2),
          'b.as('b1), 'c, 'c.as('c1)).where('c1.attr + 'a1.attr > 10 && 'a2.attr > -15
        && IsNotNull('a) && IsNotNull('c)).analyze
      comparePlans(correctAnswer, optimizedPlan)
    }
  }

  test("test new filter inference with decanonicalization for expression not" +
    " implementing NullIntolerant - 1") {
    def getTestPlan: LogicalPlan = {
      val tr1 = LocalRelation('a.int, 'b.int, 'c.int)
      tr1.select('a, 'a.as('a1), 'a.as('a2),
        'b.as('b1), 'c, 'c.as('c1),
        CaseWhen(Seq(('a.attr + 'b.attr + 'c.attr > Literal(1),
          Literal(1)), ('a.attr + 'b.attr + 'c.attr > Literal(2), Literal(2))),
          Option(Literal(null))).as("z")).where('z.attr > 10 && 'a2.attr > -15).
        where(CaseWhen(Seq(('a.attr + 'b.attr + 'c.attr > Literal(1),
          Literal(1)), ('a.attr + 'b.attr + 'c.attr > Literal(2), Literal(2))),
          Option(Literal(null))) > 10 && 'a.attr > -15).where('z.attr > 10)
    }

    withSQLConf() {
      val optimizedPlan = executePlan(getTestPlan, OptimizerTypes.NO_PUSH_DOWN_ONLY_PRUNING)
      val correctAnswer = LocalRelation('a.int, 'b.int, 'c.int).
        select('a, 'a.as('a1), 'a.as('a2),
          'b.as('b1), 'c, 'c.as('c1), CaseWhen(Seq(
            ('a.attr + 'b.attr + 'c.attr > Literal(1),
              Literal(1)), ('a.attr + 'b.attr + 'c.attr > Literal(2), Literal(2))),
            Option(Literal(null))).as("z"), 'b).where('z.attr > 10 && 'a2.attr > -15
        && IsNotNull('a) && IsNotNull('z)).select('a, 'a1, 'a2,
        'b1, 'c, 'c1, 'z).analyze
      comparePlans(correctAnswer, optimizedPlan)
    }
  }

  test("test new filter inference with decanonicalization for expression not" +
    " implementing NullIntolerant - 2") {
    def getTestPlan: LogicalPlan = {
      val tr1 = LocalRelation('a.int, 'b.int, 'c.int)
      tr1.select('a, 'a.as('a1), 'a.as('a2),
        'b.as('b1), 'c, 'c.as('c1),
        ('a.attr + CaseWhen(Seq(('a.attr + 'b.attr + 'c.attr > Literal(1),
          Literal(1)), ('a.attr + 'b.attr + 'c.attr > Literal(2), Literal(2))),
          Option(Literal(null)))).as("z")).where('z.attr > 10 && 'a2.attr > -15).
        where('a.attr + CaseWhen(Seq(('a.attr + 'b.attr + 'c.attr > Literal(1),
          Literal(1)), ('a.attr + 'b.attr + 'c.attr > Literal(2), Literal(2))),
          Option(Literal(null))) > 10 && 'a.attr > -15).where('z.attr > 10)
    }

    withSQLConf() {
      val optimizedPlan = executePlan(getTestPlan, OptimizerTypes.NO_PUSH_DOWN_ONLY_PRUNING)
      val correctAnswer = LocalRelation('a.int, 'b.int, 'c.int).
        select('a, 'a.as('a1), 'a.as('a2),
          'b.as('b1), 'c, 'c.as('c1), ('a.attr + CaseWhen(Seq(
            ('a.attr + 'b.attr + 'c.attr > Literal(1),
              Literal(1)), ('a.attr + 'b.attr + 'c.attr > Literal(2), Literal(2))),
            Option(Literal(null)))).as("z"), 'b).where('z.attr > 10 && 'a2.attr > -15
        && IsNotNull('a) && IsNotNull('z)).select('a, 'a1, 'a2,
        'b1, 'c, 'c1, 'z).analyze
      comparePlans(correctAnswer, optimizedPlan)
    }
  }

  test("test new filter inference with decanonicalization for expression" +
    "implementing NullIntolerant") {
    def getTestPlan: LogicalPlan = {
      val tr1 = LocalRelation('a.int, 'b.int, 'c.int)
      tr1.select('a, 'a.as('a1), 'a.as('a2),
        'b.as('b1), 'c, 'c.as('c1),
        ('a.attr + 'b.attr + 'c.attr).as("z")).where('z.attr > 10 && 'a2.attr > -15).
        where('a.attr + 'b1.attr + 'c.attr > 10 && 'a.attr > -15)
    }

    withSQLConf() {
      val optimizedPlan = executePlan(getTestPlan, OptimizerTypes.NO_PUSH_DOWN_ONLY_PRUNING)
      val correctAnswer = LocalRelation('a.int, 'b.int, 'c.int).
        select('a, 'a.as('a1), 'a.as('a2),
          'b.as('b1), 'c, 'c.as('c1),
          ('a.attr + 'b.attr + 'c.attr).as("z")).where('z.attr > 10 && 'a2.attr > -15
        && IsNotNull('a) && IsNotNull('b1) && IsNotNull('c)).analyze
      comparePlans(correctAnswer, optimizedPlan)
    }
  }

  test("test pruning using constraints with filters after project with expression in" +
    " alias.") {
    def getTestPlan: LogicalPlan = {
      val tr1 = LocalRelation('a.int, 'b.int, 'c.int)
      tr1.select('a, 'a.as('a1), 'a.as('a2), 'b,
        'b.as('b1), 'c, 'c.as('c1), ('a.attr + 'b.attr).as("z")).
        where('c1.attr + 'z.attr > 10 &&
          'a2.attr > -15).
        where('c.attr + 'a.attr + 'b.attr > 10 &&
          'a.attr > -15)
    }


    withSQLConf() {
      val optimizedPlan = executePlan(getTestPlan, OptimizerTypes.NO_PUSH_DOWN_ONLY_PRUNING)
      val correctAnswer = LocalRelation('a.int, 'b.int, 'c.int).
        select('a, 'a.as('a1), 'a.as('a2), 'b,
          'b.as('b1), 'c, 'c.as('c1), ('a.attr + 'b.attr).as("z")).
        where('c1.attr + 'z.attr > 10 &&
          'a2.attr > -15 && IsNotNull('b)
          && IsNotNull('a) && IsNotNull('c)).analyze
      comparePlans(correctAnswer, optimizedPlan)
    }
  }

  ignore("Disabled due to spark's canonicalization bug." +
    " test pruning using constraints with filters after project with expression in alias.") {
    val tr1 = LocalRelation('a.int, 'b.string, 'c.int)
    val query = tr1.select('a, 'a.as('a1), 'a.as('a2), 'b,
      'b.as('b1), 'c, 'c.as('c1), ('a.attr + 'b.attr).as("z")).
      where('c1.attr + 'z.attr > 10 && 'a2.attr > -15).
      where('c.attr + 'a.attr + 'b.attr > 10 && 'a.attr > -15)

    withSQLConf() {
      val optimizedPlan = executePlan(query, OptimizerTypes.NO_PUSH_DOWN_ONLY_PRUNING)
      val correctAnswer = LocalRelation('a.int, 'b.string, 'c.int).
        select('a, 'a.as('a1), 'a.as('a2), 'b,
          'b.as('b1), 'c, 'c.as('c1), ('a.attr + 'b.attr).as("z")).
        where('c1.attr + 'z.attr > 10 && 'a2.attr > -15
          && IsNotNull('a) && IsNotNull('c) && IsNotNull('b)).analyze
      comparePlans(correctAnswer, optimizedPlan)
    }
  }

  test("plan equivalence with case statements and performance comparison with benefit" +
    "of more than 10x conservatively") {
    val tr = LocalRelation('a.int, 'b.int, 'c.int, 'd.int, 'e.int, 'f.int, 'g.int, 'h.int, 'i.int,
      'j.int, 'k.int, 'l.int, 'm.int, 'n.int)
    val query = tr.select('a, 'b, 'c, 'd, 'e, 'f, 'g, 'h, 'i, 'j, 'k, 'l, 'm, 'n,
      CaseWhen(Seq(('a.attr + 'b.attr + 'c.attr + 'd.attr + 'e.attr + 'f.attr + 'g.attr
        + 'h.attr + 'i.attr + 'j.attr + 'k.attr + 'l.attr + 'm.attr + 'n.attr > Literal(1),
        Literal(1)),
        ('a.attr + 'b.attr + 'c.attr + 'd.attr + 'e.attr + 'f.attr + 'g.attr + 'h.attr +
          'i.attr + 'j.attr + 'k.attr + 'l.attr + 'm.attr + 'n.attr > Literal(2), Literal(2))),
        Option(Literal(0))).as("JoinKey1")
    ).select('a.attr.as("a1"), 'b.attr.as("b1"), 'c.attr.as("c1"),
      'd.attr.as("d1"), 'e.attr.as("e1"), 'f.attr.as("f1"),
      'g.attr.as("g1"), 'h.attr.as("h1"), 'i.attr.as("i1"),
      'j.attr.as("j1"), 'k.attr.as("k1"), 'l.attr.as("l1"),
      'm.attr.as("m1"), 'n.attr.as("n1"), 'JoinKey1.attr.as("cf1"),
      'JoinKey1.attr).select('a1, 'b1, 'c1, 'd1, 'e1, 'f1, 'g1, 'h1, 'i1, 'j1, 'k1,
      'l1, 'm1, 'n1, 'cf1, 'JoinKey1).join(tr, condition = Option('a.attr <=> 'JoinKey1.attr)).
      analyze

    withSQLConf() {
      val optimizedPlan = executePlan(query,
        OptimizerTypes.WITH_FILTER_PUSHDOWN_THRU_JOIN_AND_PRUNING)
      comparePlans(query, optimizedPlan)
    }
  }

  def executePlan(plan: LogicalPlan, optimizerType: OptimizerTypes.Value): LogicalPlan = {
    object SimpleAnalyzer extends Analyzer(
      new CatalogManager(FakeV2SessionCatalog,
        new SessionCatalog(new InMemoryCatalog, EmptyFunctionRegistry,
          SQLConf.get)))

    val optimizedPlan = GetOptimizer(optimizerType, Some(SQLConf.get)).
      execute(SimpleAnalyzer.execute(plan))
    optimizedPlan
  }

  private object OptimizerTypes extends Enumeration {
    val WITH_FILTER_PUSHDOWN_THRU_JOIN_AND_PRUNING,
    NO_PUSH_DOWN_ONLY_PRUNING, WITH_FILTER_PUSHDOWN_THRU_JOIN_AND_UNIONS_PRUNING = Value
  }

  private object GetOptimizer {
    def apply(optimizerType: OptimizerTypes.Value, useConf: Option[SQLConf] = None): Optimizer =
      optimizerType match {
        case OptimizerTypes.WITH_FILTER_PUSHDOWN_THRU_JOIN_AND_PRUNING =>
          new Optimizer(new CatalogManager(
            FakeV2SessionCatalog,
            new SessionCatalog(new InMemoryCatalog, EmptyFunctionRegistry,
              useConf.getOrElse(SQLConf.get)))) {
            override def defaultBatches: Seq[Batch] =
              Batch("Subqueries", Once,
                EliminateSubqueryAliases) ::
                Batch("Filter Pushdown and Pruning", FixedPoint(100),
                  PushPredicateThroughJoin,
                  PushDownPredicates,
                  InferFiltersFromConstraints,
                  CombineFilters,
                  PruneFilters) :: Nil

            override def nonExcludableRules: Seq[String] = Seq.empty[String]
          }

        case OptimizerTypes.NO_PUSH_DOWN_ONLY_PRUNING =>
          new Optimizer(new CatalogManager(
            FakeV2SessionCatalog,
            new SessionCatalog(new InMemoryCatalog, EmptyFunctionRegistry,
              useConf.getOrElse(SQLConf.get)))) {
            override def defaultBatches: Seq[Batch] =
              Batch("Subqueries", Once,
                EliminateSubqueryAliases) ::
                Batch("Filter Pruning", Once,
                  InferFiltersFromConstraints,
                  PruneFilters) :: Nil

            override def nonExcludableRules: Seq[String] = Seq.empty[String]
          }

        case OptimizerTypes.WITH_FILTER_PUSHDOWN_THRU_JOIN_AND_UNIONS_PRUNING =>
          new Optimizer(new CatalogManager(
            FakeV2SessionCatalog,
            new SessionCatalog(new InMemoryCatalog, EmptyFunctionRegistry,
              useConf.getOrElse(SQLConf.get)))) {
            override def defaultBatches: Seq[Batch] =
              Batch("Subqueries", Once,
                EliminateSubqueryAliases) ::
                Batch("Union Pushdown", FixedPoint(100),
                  CombineUnions,
                  PushProjectionThroughUnion,
                  PushDownPredicates,
                  InferFiltersFromConstraints,
                  CombineFilters,
                  PruneFilters) :: Nil

            override def nonExcludableRules: Seq[String] = Seq.empty[String]
          }
      }
  }
}
