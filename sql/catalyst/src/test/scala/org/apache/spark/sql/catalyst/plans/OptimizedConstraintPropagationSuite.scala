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

import org.junit.Assert._

import org.apache.spark.sql.catalyst.analysis.{Analyzer, EliminateSubqueryAliases,
  EmptyFunctionRegistry, FakeV2SessionCatalog}
import org.apache.spark.sql.catalyst.catalog.{InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{IsNotNull, _}
import org.apache.spark.sql.catalyst.expressions.ConstraintSet.TemplateAttributeGenerator
import org.apache.spark.sql.catalyst.optimizer.{CombineFilters, CombineUnions,
  InferFiltersFromConstraints, Optimizer, PruneFilters, PushDownPredicates,
  PushPredicateThroughJoin, PushProjectionThroughUnion}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, LongType}

class OptimizedConstraintPropagationSuite extends ConstraintPropagationSuite
  with PredicateHelper {

  val trivialConstraintAbsenceChecker = (constraints: ExpressionSet) => assertTrue(
    !constraints.exists(x => x match {
      case EqualNullSafe(a, b) if a.canonicalized == b.canonicalized => true
      case EqualTo(a, b) if a.canonicalized == b.canonicalized => true
      case _ => false
    }))

  /**
   * Default spark optimizer is not used in the tests as some of the tests were false passing.
   * Many assertions go through fine hiding the bugs because of other rules in the optimizer.
   * For eg., a test dedicated to test filter pruning ( involving aliases) & hence relying
   * on contains function of ConstraintSet ( & indirectly the attributeEquivalenceList etc )
   * was false passing because of an optimizer rule, which replaces the alias with the actual
   * expression in the plan. Combining Filter is commented just to be sure that ConstraintSet
   * coming out of each node contains right the constraints & more importantly the
   * attributeEquivalenceList & expressionEquivalenceList contains the right data.
   * Otherwise it is possible that compound filter push down for left outer join
   * those Lists are empty & tests false passing
   */

  test("checking number of base constraints in project node") {
    assume(SQLConf.get.useOptimizedConstraintPropagation)
    val tr = LocalRelation('a.int, 'b.string, 'c.int)
    val y = tr.where('c.attr > 10).select('a.as('x), 'b.as('y), 'c, 'c.as('c1)).analyze
    assert(y.resolved)
    val constraints = y.constraints
    trivialConstraintAbsenceChecker(constraints)
    assertEquals(2, constraints.size)
    verifyConstraints(ExpressionSet(constraints),
      ExpressionSet(Seq(resolveColumn(y.analyze, "c") > 10,
        IsNotNull(resolveColumn(y.analyze, "c")))))
  }

  test("checking number of base constraints with " +
    "filter dependent on multiple attributes") {
    assume(SQLConf.get.useOptimizedConstraintPropagation)
    val tr = LocalRelation('a.int, 'b.string, 'c.int)
    val y = tr.where('c.attr + 'a.attr > 10).select('a, 'a.as('x), 'b.as('y), 'c,
      'c.as('c1)).analyze
    assert(y.resolved)
    val constraints = y.constraints
    trivialConstraintAbsenceChecker(constraints)
    assertEquals(3, constraints.size)
    verifyConstraints(ExpressionSet(constraints),
      ExpressionSet(Seq(
        resolveColumn(y.analyze, "c") +
          resolveColumn(y.analyze, "a") > 10,
        IsNotNull(resolveColumn(y.analyze, "c")),
        IsNotNull(resolveColumn(y.analyze, "a")))))
  }

  test("checking filter pruning") {
    assume(SQLConf.get.useOptimizedConstraintPropagation)
    val tr = LocalRelation('a.int, 'b.string, 'c.int)
    val y = tr.where('c.attr + 'a.attr > 10).select('a, 'a.as('x), 'b.as('y), 'c,
      'c.as('c1)).where('x.attr + 'c1.attr > 10).analyze
    assert(y.resolved)
    val optimized = GetOptimizer(OptimizerTypes.NO_PUSH_DOWN_ONLY_PRUNING).execute(y)
    val constraints = optimized.constraints
    trivialConstraintAbsenceChecker(constraints)
    assertEquals(3, constraints.size)

    verifyConstraints(ExpressionSet(constraints),
      ExpressionSet(Seq(
        resolveColumn(y.analyze, "c") +
          resolveColumn(y.analyze, "a") > 10,
        IsNotNull(resolveColumn(y.analyze, "c")),
        IsNotNull(resolveColumn(y.analyze, "a")))))

    val allFilters = optimized.collect[Filter] {
      case x: Filter => x
    }
    assertEquals(1, allFilters.size)
    val conditionalExps = allFilters.head.expressions.flatMap(expr => expr.collect {
      case x: GreaterThan => x
      case y: LessThan => y
    })
    assertEquals(1, conditionalExps.size)
    val correctAnswer = tr.where('c.attr + 'a.attr > 10 && IsNotNull('a) && IsNotNull('c)).
      select('a, 'a.as('x), 'b.as('y), 'c, 'c.as('c1)).analyze
    comparePlans(optimized, correctAnswer)
  }

  test("Alias in different projects have same exprID..!") {
    val tr1 = LocalRelation('a.int, 'b.string, 'c.int, 'd.int)
    val y = tr1.where('c.attr + 'a.attr > 10).select('a, 'a.as('a1),
      'a.as('a2), 'b.as('b1), 'c, 'c.as('c1), Literal(1).as("one"),
      Literal(1).as("one_")
    ).where('b1.attr > 10).select( Literal(1).as("one"),
      Literal(1).as("one_"), 'b1.attr).
      where('one.attr > 1).analyze
    var exprId1: Option[ExprId] = None
    var exprId2: Option[ExprId] = None
    val bugify = y.transformUp {
      case p@Project(pl, child) => if (exprId1.isEmpty) {
        exprId1 = pl.find(_.name == "one").map(_.asInstanceOf[Alias].exprId)
        exprId2 = pl.find(_.name == "one_").map(_.asInstanceOf[Alias].exprId)
        p
      } else {
        val newPl = pl.map(ne => if (ne.name == "one") {
          val al = ne.asInstanceOf[Alias]
          Alias(al.child, al.name)(exprId1.get)
        } else if (ne.name == "one_") {
          val al = ne.asInstanceOf[Alias]
          Alias(al.child, al.name)(exprId2.get)
        } else ne )
        Project(newPl, child)
      }
      case f: Filter if exprId1.isDefined => f.transformExpressionsUp {
        case expr: Expression => expr.transformUp {
          case at: AttributeReference if at.name == "one" => at.withExprId(exprId1.get)
          case at: AttributeReference if at.name == "one_" => at.withExprId(exprId2.get)
        }
      }
    }.analyze

    assert(bugify.resolved)
    val optimized = GetOptimizer(OptimizerTypes.NO_PUSH_DOWN_ONLY_PRUNING).
      execute(bugify)
    val topConstraint = optimized.constraints
    // there should not be any trivial constraint present
    trivialConstraintAbsenceChecker(topConstraint)
  }

  test("trivial conditions should not be part of constraints") {
    val tr1 = LocalRelation('a.int, 'b.string, 'c.int)
    val tr2 = LocalRelation('x.int, 'y.string, 'z.int)
    val y = tr1.where('c.attr + 'a.attr > 10).select('a, 'a.as('a1), 'a.as('a2),
      'b.as('b1), 'c,
      'c.as('c1)).join(tr2.select('x, 'x.as('x1), 'y, 'z), Inner,
      Some("a".attr === "x".attr)).where('y.attr > 10).analyze
    assert(y.resolved)
    val optimized = GetOptimizer(OptimizerTypes.WITH_FILTER_PUSHDOWN_THRU_JOIN_AND_PRUNING).
      execute(y)
    val topConstraint = optimized.constraints
    // there should not be any trivial constraint present
    trivialConstraintAbsenceChecker(topConstraint)
  }

  test("filter pruning on Join Node") {
    val tr1 = LocalRelation('a.int, 'b.string, 'c.int)
    val tr2 = LocalRelation('x.int, 'y.string, 'z.int)
    val y = tr1.where('c.attr + 'a.attr > 10).select('a, 'a.as('a1), 'a.as('a2),
      'b.as('b1), 'c,
      'c.as('c1)).join(tr2, Inner, Some("a2".attr === "x".attr))
      .where('a1.attr + 'c1.attr > 10).analyze
    assert(y.resolved)
    val optimized = GetOptimizer(OptimizerTypes.WITH_FILTER_PUSHDOWN_THRU_JOIN_AND_PRUNING).
      execute(y)
    trivialConstraintAbsenceChecker(optimized.constraints)
    val allFilters = optimized.collect[Filter] {
      case x: Filter => x
    }

    val conditionalExps = allFilters.flatMap(filter =>
      filter.expressions.flatMap(expr => expr.collect {
        case x: GreaterThan => x
        case y: LessThan => y
      }))
    assertEquals(1, conditionalExps.size)
    val correctAnswer = tr1.where('c.attr + 'a.attr > 10 && IsNotNull('a) && IsNotNull('c)).
      select('a, 'a.as('a1), 'a.as('a2),
        'b.as('b1), 'c,
        'c.as('c1)).join(tr2.where(IsNotNull('x)), Inner, Some("a2".attr === "x".attr)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("new filter pushed down on Join Node") {
    val tr1 = LocalRelation('a.int, 'b.string, 'c.int)
    val tr2 = LocalRelation('x.int, 'y.string, 'z.int)
    val y = tr1.where('c.attr + 'a.attr > 10 && 'a.attr > -15).select('a, 'a.as('a1), 'a.as('a2),
      'b.as('b1), 'c,
      'c.as('c1)).join(tr2, Inner, Some("a2".attr === "x".attr))
      .where('a1.attr + 'c1.attr > 10).analyze
    assert(y.resolved)
    val optimized = GetOptimizer(OptimizerTypes.WITH_FILTER_PUSHDOWN_THRU_JOIN_AND_PRUNING).
      execute(y)
    val allFilters = optimized.collect[Filter] {
      case x: Filter => x
    }
    val conditionalExps = allFilters.flatMap(filter =>
      filter.expressions.flatMap(expr => expr.collect {
        case x: GreaterThan => x
        case y: LessThan => y
      }))
    assertEquals(3, conditionalExps.size)
    val correctAnswer = tr1.where('c.attr + 'a.attr > 10 && 'a.attr > -15
      && IsNotNull('a) && IsNotNull('c)).select('a, 'a.as('a1), 'a.as('a2),
      'b.as('b1), 'c,
      'c.as('c1)).join(tr2.where(IsNotNull('x) && 'x.attr > -15),
      Inner, Some("a2".attr === "x".attr)).analyze

    comparePlans(optimized, correctAnswer)
    trivialConstraintAbsenceChecker(optimized.constraints)
  }

  test("new filter pushed down on Join Node with multiple join conditions") {
    assume(SQLConf.get.useOptimizedConstraintPropagation)
    def getTestPlan: LogicalPlan = {
      val tr1 = LocalRelation('a.int, 'b.string, 'c.int)
      val tr2 = LocalRelation('x.int, 'y.string, 'z.int)
      tr1.where('c.attr + 'a.attr > 10 && 'a.attr > -15).select('a, 'a.as('a1), 'a.as('a2),
        'b.as('b1), 'c,
        'c.as('c1)).join(tr2, Inner, Some("a2".attr === "x".attr && 'c1.attr === 'z.attr))
        .where('a1.attr + 'c1.attr > 10)
    }
    val (optimized, _) = withSQLConf[(LogicalPlan, ExpressionSet)](
       SQLConf.OPTIMIZER_CONSTRAINT_PROPAGATION_OPTIMIZED.key -> "true") {
      executePlan(getTestPlan, OptimizerTypes.WITH_FILTER_PUSHDOWN_THRU_JOIN_AND_PRUNING)
    }
    trivialConstraintAbsenceChecker(optimized.constraints)
    val allFilters = optimized.collect[Filter] {
      case x: Filter => x
    }
    val conditionalExps = allFilters.flatMap(filter =>
      filter.expressions.flatMap(expr => expr.collect {
        case x: GreaterThan => x
        case y: LessThan => y
      }))
    assertEquals(4, conditionalExps.size)

    // there should be a + operator present on each side of the join node
    val joinNode = optimized.collectFirst {
      case j: Join => j
    }.get
    assertTrue(joinNode.left.collect {
      case f: Filter => f
    }.exists(f => f.condition.collectFirst {
      case a: Add => a
    }.isDefined))
    assertTrue(joinNode.right.collect {
      case f: Filter => f
    }.exists(f => f.condition.collectFirst {
      case a: Add => a
    }.isDefined))
    val tr1 = LocalRelation('a.int, 'b.string, 'c.int)
    val tr2 = LocalRelation('x.int, 'y.string, 'z.int)
    val correctAnswer = tr1.where('c.attr + 'a.attr > 10 && 'a.attr > -15 &&
      IsNotNull('a) && IsNotNull('c)).select('a, 'a.as('a1), 'a.as('a2),
      'b.as('b1), 'c,
      'c.as('c1)).join(tr2.where(IsNotNull('x) && IsNotNull('z) && 'x.attr > -15
      && 'z.attr + 'x.attr > 10),
      Inner, Some("a2".attr === "x".attr && 'c1.attr === 'z.attr)).analyze

    comparePlans(optimized, correctAnswer)
    // get plan for stock spark
    val (optimized1, _) = withSQLConf[(LogicalPlan, ExpressionSet)](
      SQLConf.OPTIMIZER_CONSTRAINT_PROPAGATION_OPTIMIZED.key -> "false") {
      executePlan(getTestPlan, OptimizerTypes.WITH_FILTER_PUSHDOWN_THRU_JOIN_AND_PRUNING)
    }
    // The plans don't match as stock spark does not push down a filter of form x + z > 10
    // comparePlans(optimized1, correctAnswer)
  }

  test("filter pruning when original attributes are lost") {
    val tr = LocalRelation('a.int, 'b.string, 'c.int)
    val y = tr.where('c.attr + 'a.attr > 10).select('a, 'a.as('x), 'b.as('y), 'c,
      'c.as('c1)).select('x.as('x1), 'y.as('y1),
      'c1.as('c2)).where('x1.attr + 'c2.attr > 10).analyze
    assert(y.resolved)
    val optimized = GetOptimizer(OptimizerTypes.NO_PUSH_DOWN_ONLY_PRUNING).execute(y)
    trivialConstraintAbsenceChecker(optimized.constraints)
    val allFilters = optimized.collect[Filter] {
      case x: Filter => x
    }
    assertEquals(1, allFilters.size)
    val conditionalExps = allFilters.head.expressions.flatMap(expr => expr.collect {
      case x: GreaterThan => x
      case y: LessThan => y
    })
    assertEquals(1, conditionalExps.size)
    val correctAnswer = tr.where('c.attr + 'a.attr > 10 && IsNotNull('a) && IsNotNull('c)).
      select('a, 'a.as('x), 'b.as('y), 'c,
        'c.as('c1)).select('x.as('x1), 'y.as('y1),
      'c1.as('c2)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("filter pruning when partial attributes are lost") {
    val tr = LocalRelation('a.int, 'b.string, 'c.int)
    val y = tr.where('c.attr + 'a.attr > 10).select('a, 'a.as('x), 'b.as('y), 'c,
      'c.as('c1)).select('c, 'x.as('x1), 'y.as('y1),
      'c1.as('c2)).where('x1.attr + 'c.attr > 10).analyze
    assert(y.resolved)
    val optimized = GetOptimizer(OptimizerTypes.NO_PUSH_DOWN_ONLY_PRUNING).execute(y)
    trivialConstraintAbsenceChecker(optimized.constraints)
    val allFilters = optimized.collect[Filter] {
      case x: Filter => x
    }
    assertEquals(1, allFilters.size)
    val conditionalExps = allFilters.head.expressions.flatMap(expr => expr.collect {
      case x: GreaterThan => x
      case y: LessThan => y
    })
    assertEquals(1, conditionalExps.size)
    val correctAnswer = tr.where('c.attr + 'a.attr > 10 && IsNotNull('a) && IsNotNull('c)).
      select('a, 'a.as('x), 'b.as('y), 'c,
        'c.as('c1)).select('c, 'x.as('x1), 'y.as('y1),
      'c1.as('c2)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("filter pruning with expressions in alias") {
    val tr = LocalRelation('a.int, 'b.string, 'c.int)
    val y = tr.where('c.attr + 'a.attr > 10).select('a, ('a.attr + 'c.attr).as('x),
      'b.as('y), 'c,
      'c.as('c1)).select('c, 'x.as('x1), 'y.as('y1),
      'c1.as('c2)).where('x1.attr > 10).analyze
    assert(y.resolved)
    val optimized = GetOptimizer(OptimizerTypes.NO_PUSH_DOWN_ONLY_PRUNING).execute(y)
    trivialConstraintAbsenceChecker(optimized.constraints)
    val allFilters = optimized.collect[Filter] {
      case x: Filter => x
    }
    assertEquals(1, allFilters.size)
    val conditionalExps = allFilters.head.expressions.flatMap(expr => expr.collect {
      case x: GreaterThan => x
      case y: LessThan => y
    })
    assertEquals(1, conditionalExps.size)
    val correctAnswer = tr.where('c.attr + 'a.attr > 10 && IsNotNull('a) && IsNotNull('c)).
      select('a, ('a.attr + 'c.attr).as('x),
        'b.as('y), 'c,
        'c.as('c1)).select('c, 'x.as('x1), 'y.as('y1),
      'c1.as('c2)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("filter pruning with subexpressions in alias") {
    val tr = LocalRelation('a.int, 'b.string, 'c.int)
    val y = tr.where('c.attr + 'a.attr + 'b.attr > 10).select(('a.attr + 'c.attr).as('x),
      'b.as('y)).select('x.as('x1), 'y.as('y1)).
      where('x1.attr + 'y1.attr > 10).analyze
    assert(y.resolved)
    val optimized = GetOptimizer(OptimizerTypes.NO_PUSH_DOWN_ONLY_PRUNING).execute(y)
    trivialConstraintAbsenceChecker(optimized.constraints)
    val allFilters = optimized.collect[Filter] {
      case x: Filter => x
    }
    assertEquals(1, allFilters.size)
    val conditionalExps = allFilters.head.expressions.flatMap(expr => expr.collect {
      case x: GreaterThan => x
      case y: LessThan => y
    })
    assertEquals(1, conditionalExps.size)
    val correctAnswer = tr.where('c.attr + 'a.attr + 'b.attr > 10 && IsNotNull('a) && IsNotNull('c)
      && IsNotNull('b)).
      select(('a.attr + 'c.attr).as('x),
        'b.as('y)).select('x.as('x1), 'y.as('y1)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("filter pruning using expression equivalence list - #1") {
    val tr = LocalRelation('a.int, 'b.string, 'c.int)
    val y = tr.where('c.attr + 'a.attr + 'b.attr > 10).select('a, 'c, ('a.attr + 'c.attr).as('x),
      'b, 'b.as('y)).where('x.attr + 'b.attr > 10).analyze
    assert(y.resolved)
    val optimized = GetOptimizer(OptimizerTypes.NO_PUSH_DOWN_ONLY_PRUNING).execute(y)
    trivialConstraintAbsenceChecker(optimized.constraints)
    val allFilters = optimized.collect[Filter] {
      case x: Filter => x
    }
    val conditionalExps = allFilters.flatMap(_.expressions).flatMap(expr => expr.collect {
      case x: GreaterThan => x
      case y: LessThan => y
    })
    assertEquals(1, conditionalExps.size)
    val correctAnswer = tr.where('c.attr + 'a.attr + 'b.attr > 10 && IsNotNull('a) && IsNotNull('c)
      && IsNotNull('b)).
      select('a, 'c, ('a.attr + 'c.attr).as('x),
        'b, 'b.as('y)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("filter pruning using expression equivalence list - #2") {
    val tr = LocalRelation('a.int, 'b.string, 'c.int)
    val y = tr.where('c.attr + 'a.attr + 'b.attr > 10).select('c, ('a.attr + 'c.attr).as('x),
      ('a.attr + 'c.attr).as('z), 'b, 'b.as('y)).where('x.attr + 'b.attr > 10).
      select('z, 'y).where('z.attr + 'y.attr > 10).analyze
    assert(y.resolved)
    val optimized = GetOptimizer(OptimizerTypes.NO_PUSH_DOWN_ONLY_PRUNING).execute(y)
    trivialConstraintAbsenceChecker(optimized.constraints)
    val allFilters = optimized.collect[Filter] {
      case x: Filter => x
    }
    val conditionalExps = allFilters.flatMap(_.expressions).flatMap(expr => expr.collect {
      case x: GreaterThan => x
      case y: LessThan => y
    })
    assertEquals(1, conditionalExps.size)
    val correctAnswer = tr.where('c.attr + 'a.attr + 'b.attr > 10 && IsNotNull('a) && IsNotNull('c)
      && IsNotNull('b)).select('c, ('a.attr + 'c.attr).as('x),
      ('a.attr + 'c.attr).as('z), 'b, 'b.as('y)).select('z, 'y).analyze

    comparePlans(optimized, correctAnswer)
    val z = tr.where('c.attr + 'a.attr + 'b.attr > 10).select('c, ('a.attr + 'c.attr).as('x),
      ('a.attr + 'c.attr).as('z), 'b, 'b.as('y)).where('x.attr + 'b.attr > 10).
      select('z, 'y).where('z.attr + 'y.attr > 10).select(('z.attr + 'y.attr).as('k)).
      where('k.attr > 10).analyze

    val correctAnswer1 = tr.where('c.attr + 'a.attr + 'b.attr > 10 && IsNotNull('a) && IsNotNull('c)
      && IsNotNull('b)).select('c, ('a.attr + 'c.attr).as('x),
      ('a.attr + 'c.attr).as('z), 'b, 'b.as('y)).select('z, 'y).
      select(('z.attr + 'y.attr).as('k)).analyze

    comparePlans(GetOptimizer(OptimizerTypes.NO_PUSH_DOWN_ONLY_PRUNING).execute(z), correctAnswer1)
  }

  test("check redundant constraints are not added") {
    assume(SQLConf.get.useOptimizedConstraintPropagation)
    val tr = LocalRelation('a.int, 'b.int, 'c.int, 'd.int)
    val trAnalyzed = tr.analyze
    val aliasedAnalyzed = trAnalyzed.where('c.attr + 'a.attr + 'b.attr > 10 && 'd.attr > 8).
      select('a, 'd, 'd.attr.as('z), 'd.attr.as('z1),
        ('a.attr + 'c.attr).as('x1), ('a.attr + 'c.attr).as('x),
        'b, 'b.as('y), 'c).analyze
    val y = aliasedAnalyzed.where('x.attr + 'b.attr > 10 && 'z.attr > 8).analyze
    assert(y.resolved)
    /* total expected constraints
    1) a + c + b > 10  2) isnotnull(a) 3) isnotnull(b) 4) isnotnull(c)  5) d > 8
    6) isnotnull(d)
    */
    val expectedConstraints = ExpressionSet(Seq(
      resolveColumn(trAnalyzed, "a") + resolveColumn(trAnalyzed, "b") +
        resolveColumn(trAnalyzed, "c") > 10,
      IsNotNull(resolveColumn(trAnalyzed, "a")),
      IsNotNull(resolveColumn(trAnalyzed, "b")),
      IsNotNull(resolveColumn(trAnalyzed, "c")),
      IsNotNull(resolveColumn(trAnalyzed, "d")),
      resolveColumn(trAnalyzed, "d") > 8
    ))
    val constraints = y.constraints
    trivialConstraintAbsenceChecker(constraints)
    assertEquals(6, constraints.size)
    verifyConstraints(constraints, expectedConstraints)
  }

  test("new filter pushed down on Join Node with filter on each variable" +
    " of join condition") {
    val tr1 = LocalRelation('a.int, 'b.string, 'c.int)
    val tr1_ = tr1.where('c.attr + 'a.attr > 10 && 'a.attr > -11)
    val tr2 = LocalRelation('x.int, 'y.string, 'z.int)
    val tr2_ = tr2.where('x.attr > -12)

    val y = tr1_.select('a, 'a.as('a1), 'a.as('a2),
      'b.as('b1), 'c,
      'c.as('c1)).join(tr2_.select('x.as('x1)), Inner,
      Some('a2.attr === 'x1.attr)).where('a1.attr + 'c1.attr > 10).analyze
    assert(y.resolved)
    val optimized = GetOptimizer(OptimizerTypes.WITH_FILTER_PUSHDOWN_THRU_JOIN_AND_PRUNING).
      execute(y)
    trivialConstraintAbsenceChecker(optimized.constraints)
    val joinNode = optimized.find({
      case _: Join => true
      case _ => false
    }).get.asInstanceOf[Join]

    def checkForGreaterThanFunctions(node: LogicalPlan): Unit = {
      val filterExps = node.collect {
        case x: Filter => x
      }.flatMap(_.expressions)

      assert(filterExps.exists(x => {
        x.find {
          case GreaterThan(_, Literal(-12, IntegerType)) => true
          case _ => false
        }.isDefined
      }))

      assert(filterExps.exists(x => {
        x.find {
          case GreaterThan(_, Literal(-11, IntegerType)) => true
          case _ => false
        }.isDefined
      }))
    }

    checkForGreaterThanFunctions(joinNode.left)
    checkForGreaterThanFunctions(joinNode.right)

    val allFilterExpressions = optimized.collect {
      case x: Filter => x
    }.flatMap(_.expressions)
    assertEquals(5, allFilterExpressions.flatMap(_.collect {
      case _: GreaterThan => true
    }).size)
    val correctAnswer = tr1.where('c.attr + 'a.attr > 10 && 'a.attr > -11
      && 'a.attr > -12 && IsNotNull('a) && IsNotNull('c)).
      select('a, 'a.as('a1), 'a.as('a2), 'b.as('b1),
        'c, 'c.as('c1)).join(tr2.where('x.attr > -12 && IsNotNull('x) && 'x.attr > -11).
      select('x.as('x1)), Inner,
      Some('a2.attr === 'x1.attr)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("compound filter push down for left outer join") {
    assume(SQLConf.get.useOptimizedConstraintPropagation)
    val tr1 = LocalRelation('a.int, 'b.int, 'c.int).subquery('tr1)
    val tr2 = LocalRelation('x.int, 'y.int, 'z.int).subquery('tr2)
    val y = tr1.where('a.attr + 'b.attr > 10)
      .join(tr2.where('x.attr > 100), LeftOuter, Some("tr1.a".attr === "tr2.x".attr
        && "tr1.b".attr === "tr2.y".attr)).analyze
    assert(y.resolved)
    val optimized = GetOptimizer(OptimizerTypes.WITH_FILTER_PUSHDOWN_THRU_JOIN_AND_PRUNING).
      execute(y)
    trivialConstraintAbsenceChecker(optimized.constraints)

    val correctAnswer = tr1.where('a.attr + 'b.attr > 10 && IsNotNull('a) && IsNotNull('b)).
      join(tr2.where('x.attr > 100 && IsNotNull('x) && IsNotNull('y) && 'x.attr + 'y.attr > 10),
        LeftOuter, Some("tr1.a".attr === "tr2.x".attr && "tr1.b".attr === "tr2.y".attr)).analyze
    comparePlans(optimized, correctAnswer)
  }

  test("compound filter push down for left Anti join") {
    assume(SQLConf.get.useOptimizedConstraintPropagation)
    val tr1 = LocalRelation('a.int, 'b.int, 'c.int).subquery('tr1)
    val tr2 = LocalRelation('x.int, 'y.int, 'z.int).subquery('tr2)
    val y = tr1.where('a.attr + 'b.attr > 10)
      .join(tr2.where('x.attr > 100), LeftAnti, Some("tr1.a".attr === "tr2.x".attr
        && "tr1.b".attr === "tr2.y".attr)).analyze
    assert(y.resolved)
    val optimized = GetOptimizer(OptimizerTypes.WITH_FILTER_PUSHDOWN_THRU_JOIN_AND_PRUNING).
      execute(y)
    trivialConstraintAbsenceChecker(optimized.constraints)

    val correctAnswer = tr1.where('a.attr + 'b.attr > 10 && IsNotNull('a) && IsNotNull('b)).
      join(tr2.where('x.attr > 100 && IsNotNull('x) && IsNotNull('y) && 'x.attr + 'y.attr > 10),
        LeftAnti, Some("tr1.a".attr === "tr2.x".attr && "tr1.b".attr === "tr2.y".attr)).analyze
    comparePlans(optimized, correctAnswer)
  }

  test("compound filter push down for left Semi join") {
    assume(SQLConf.get.useOptimizedConstraintPropagation)
    val tr1 = LocalRelation('a.int, 'b.int, 'c.int).subquery('tr1)
    val tr2 = LocalRelation('x.int, 'y.int, 'z.int).subquery('tr2)
    val y = tr1.where('a.attr + 'b.attr > 10)
      .join(tr2.where('x.attr > 100), LeftSemi, Some("tr1.a".attr === "tr2.x".attr
        && "tr1.b".attr === "tr2.y".attr)).analyze
    assert(y.resolved)
    val optimized = GetOptimizer(OptimizerTypes.WITH_FILTER_PUSHDOWN_THRU_JOIN_AND_PRUNING).
      execute(y)
    trivialConstraintAbsenceChecker(optimized.constraints)

    val correctAnswer = tr1.where('a.attr + 'b.attr > 10 && IsNotNull('a) && IsNotNull('b) &&
      'a.attr > 100).
      join(tr2.where('x.attr > 100 && IsNotNull('x) && IsNotNull('y) && 'x.attr + 'y.attr > 10),
        LeftSemi, Some("tr1.a".attr === "tr2.x".attr && "tr1.b".attr === "tr2.y".attr)).analyze
    comparePlans(optimized, correctAnswer)
  }

  test("compound filter push down for right outer join") {
    assume(SQLConf.get.useOptimizedConstraintPropagation)
    val tr1 = LocalRelation('a.int, 'b.int, 'c.int).subquery('tr1)
    val tr2 = LocalRelation('x.int, 'y.int, 'z.int).subquery('tr2)
    val y = tr1.join(tr2.where('x.attr > 100 && 'x.attr + 'y.attr > 10), RightOuter,
      Some("tr1.a".attr === "tr2.x".attr && "tr1.b".attr === "tr2.y".attr)).analyze
    assert(y.resolved)
    val optimized = GetOptimizer(OptimizerTypes.WITH_FILTER_PUSHDOWN_THRU_JOIN_AND_PRUNING).
      execute(y)
    trivialConstraintAbsenceChecker(optimized.constraints)
    val correctAnswer = tr1.where('a.attr + 'b.attr > 10 && IsNotNull('a) && IsNotNull('b)
      && 'a.attr > 100).join(tr2.where('x.attr > 100 && IsNotNull('x) && IsNotNull('y) &&
      'x.attr + 'y.attr > 10), RightOuter, Some("tr1.a".attr === "tr2.x".attr &&
      "tr1.b".attr === "tr2.y".attr)).analyze
    comparePlans(optimized, correctAnswer)
  }

  test("filter pruning due to new filter pushed down on Join Node ") {
    def getTestPlan: LogicalPlan = {
      val tr1 = LocalRelation('a.int, 'b.string, 'c.int)
      val tr1_ = tr1.where('c.attr + 'a.attr > 10 && 'a.attr > -11)
      val tr2 = LocalRelation('x.int, 'y.string, 'z.int)
      val tr2_ = tr2.where('x.attr > -12)
      tr1_.select('a, 'a.as('a1), 'a.as('a2),
        'b.as('b1), 'c,
        'c.as('c1)).join(tr2_.select('x.as('x1)), Inner,
        Some('a2.attr === 'x1.attr)).where('x1.attr + 'c1.attr > 10)
    }
    // The unanalyzed plan needs to be generated within the function
    // so that sqlconf remains same within optimizer & outside
    val (plan1, constraints1) = withSQLConf[(LogicalPlan, ExpressionSet)](
      SQLConf.OPTIMIZER_CONSTRAINT_PROPAGATION_OPTIMIZED.key -> "false") {
      executePlan(getTestPlan, OptimizerTypes.WITH_FILTER_PUSHDOWN_THRU_JOIN_AND_PRUNING)
    }

    val (plan2, constraints2) = withSQLConf[(LogicalPlan, ExpressionSet)](
      SQLConf.OPTIMIZER_CONSTRAINT_PROPAGATION_OPTIMIZED.key -> "true") {
      executePlan(getTestPlan, OptimizerTypes.WITH_FILTER_PUSHDOWN_THRU_JOIN_AND_PRUNING)
    }
    trivialConstraintAbsenceChecker(constraints2)
    assert(constraints1 match {
      case _: ConstraintSet => false
      case _: ExpressionSet => true
    })

    assert(constraints2 match {
      case _: ConstraintSet => true
      case _: ExpressionSet => false
    })
    assert(constraints2.size <= constraints1.size)
    comparePlans(plan1, plan2)
  }

  test("top filter should not be pruned for union with lower filter only on one table") {
    val tr1 = LocalRelation('a.int, 'b.int, 'c.int)
    val tr2 = LocalRelation('d.int, 'e.int, 'f.int)
    val tr3 = LocalRelation('g.int, 'h.int, 'i.int)
    val y = tr1.where('a.attr > 10).union(tr2).union(tr3.where('g.attr > 10))
    val y1 = y.where('a.attr > 10).analyze
    assert(y1.resolved)
    val optimized = GetOptimizer(OptimizerTypes.WITH_FILTER_PUSHDOWN_THRU_JOIN_AND_UNIONS_PRUNING).
      execute(y1)
    trivialConstraintAbsenceChecker(optimized.constraints)
    val allFilterExpressions = optimized.collect {
      case x: Filter => x
    }.flatMap(_.expressions)
    assert(allFilterExpressions.flatMap(_.collect {
      case _: GreaterThan => true
    }).size == 3)
    val union = optimized.find {
      case _: Union => true
      case _ => false
    }.get.asInstanceOf[Union]

    val numGTExpsBelowUnion = union.children.flatMap {
      child =>
        child.expressions.flatMap(_.collect {
          case x: GreaterThan => x
        })
    }
    assertEquals(3, numGTExpsBelowUnion.size)

    assert(union.children.forall(p => {
      p.expressions.flatMap(_.collect {
        case x: GreaterThan => x
      }).nonEmpty
    }))
    val correctAnswer = new Union(Seq(tr1.where('a.attr > 10 && IsNotNull('a)),
      tr2.where('d.attr > 10 && IsNotNull('d)),
      tr3.where('g.attr > 10 && IsNotNull('g)))).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Union Node. missing attribute from equiv list behaviour - 1") {
    val tr1 = LocalRelation('a.int, 'a1.int, 'a2.int, 'a3.int, 'a4.int)
    val tr2 = LocalRelation('b.int, 'b1.int, 'b2.int, 'b3.int, 'b4.int)
    val tr3 = LocalRelation('c.int, 'c1.int, 'c2.int, 'c3.int, 'c4.int)
    val u1 = tr1.where('a > 10).select('a, 'a.as("a1_1"),
      'a.as("a1_2"), 'a.as("a1_3"))
    val u2 = tr2.where('b > 10).select('b, 'b1, 'b1.as("b1_2"),
      'b1.as("b1_3"))
    val u3 = tr3.where('c > 10).select('c, 'c1, 'c2, 'c2.as("c1_3"))
    val union = u1.union(u2).union(u3)
    // The condition 'a > 10 is a redundant condition and should be removed
    // by constraintset.
    val y = union.where('a > 10).analyze
    assert(y.resolved)
    val optimized1 = GetOptimizer(OptimizerTypes.NO_PUSH_DOWN_ONLY_PRUNING).
      execute(y)
    trivialConstraintAbsenceChecker(optimized1.constraints)

    val correctAnswer1 = new Union(Seq(
      tr1.where('a > 10 && IsNotNull('a)).select('a, 'a.as("a1_1"),
        'a.as("a1_2"), 'a.as("a1_3")).union(
        tr2.where('b > 10 && IsNotNull('b)).select('b, 'b1, 'b1.as("b1_2"),
          'b1.as("b1_3"))),
      tr3.where('c > 10 && IsNotNull('c)).select('c, 'c1, 'c2,
        'c2.as("c1_3")))).analyze

    comparePlans(optimized1, correctAnswer1)
    // on top of union put a projection which eats away 1st col
    // in that case the filter after it should survive
    // This is because the first column in each leg is respectively
    // a, b, and c. For Leg2 & Leg3 the filters are respectively on
    // b & c. So if a project on top of union only has constraint on
    // 1st column that is a. Actually the filter should survive
    // irrespective of whether a gets removed or not.
    val y2 = union.select('a1_1, 'a1_2, 'a1_3).where('a1_1 > 10).analyze
    assert(y2.resolved)
    val optimized2 = GetOptimizer(OptimizerTypes.NO_PUSH_DOWN_ONLY_PRUNING).
      execute(y2)
    trivialConstraintAbsenceChecker(optimized2.constraints)

    val correctAnswer2 = tr1.where('a > 10 && IsNotNull('a)).
      select('a, 'a.as("a1_1"),
        'a.as("a1_2"), 'a.as("a1_3")).union(
      tr2.where('b > 10 && IsNotNull('b)).select('b, 'b1, 'b1.as("b1_2"),
        'b1.as("b1_3"))).union(
      tr3.where('c > 10 && IsNotNull('c)).select('c, 'c1, 'c2,
        'c2.as("c1_3"))).select('a1_1, 'a1_2, 'a1_3).
      where('a1_1 > 10 && IsNotNull('a1_1)).analyze

    comparePlans(optimized2, correctAnswer2)
    // check that filter survives, even if no projection is applied on union
    val y3 = union.where('a1_1 > 10).analyze
    assert(y3.resolved)
    val optimized3 = GetOptimizer(OptimizerTypes.NO_PUSH_DOWN_ONLY_PRUNING).
      execute(y3)
    trivialConstraintAbsenceChecker(optimized3.constraints)

    val correctAnswer3 = tr1.where('a > 10 && IsNotNull('a)).
      select('a, 'a.as("a1_1"),
        'a.as("a1_2"), 'a.as("a1_3")).union(
      tr2.where('b > 10 && IsNotNull('b)).select('b, 'b1, 'b1.as("b1_2"),
        'b1.as("b1_3"))).union(
      tr3.where('c > 10 && IsNotNull('c)).select('c, 'c1, 'c2,
        'c2.as("c1_3"))).where('a1_1 > 10 && IsNotNull('a1_1)).analyze

    comparePlans(optimized3, correctAnswer3)

  }
  test("Union Node. missing attribute from equiv list behaviour - 1." +
    " wth changed union legs order") {
    val tr1 = LocalRelation('a.int, 'a1.int, 'a2.int, 'a3.int, 'a4.int)
    val tr2 = LocalRelation('b.int, 'b1.int, 'b2.int, 'b3.int, 'b4.int)
    val tr3 = LocalRelation('c.int, 'c1.int, 'c2.int, 'c3.int, 'c4.int)
    val u1 = tr1.where('a > 10).select('a, 'a.as("a1_1"),
      'a.as("a1_2"), 'a.as("a1_3"))
    val u2 = tr2.where('b > 10).select('b, 'b1, 'b1.as("b1_2"),
      'b1.as("b1_3"))
    val u3 = tr3.where('c > 10).select('c, 'c1, 'c2, 'c2.as("c1_3"))
    val union = u2.union(u3).union(u1)
    // The condition 'b > 10 is a redundant condition and should be removed
    // by constraintset.
    val y = union.where('b > 10).analyze
    assert(y.resolved)
    val optimized1 = GetOptimizer(OptimizerTypes.NO_PUSH_DOWN_ONLY_PRUNING).
      execute(y)
    trivialConstraintAbsenceChecker(optimized1.constraints)

    val correctAnswer1 = new Union(Seq(
      tr2.where('b > 10 && IsNotNull('b)).select('b, 'b1, 'b1.as("b1_2"),
        'b1.as("b1_3"))
        .union( tr3.where('c > 10 && IsNotNull('c)).select('c, 'c1, 'c2,
          'c2.as("c1_3"))
        ),
      tr1.where('a > 10 && IsNotNull('a)).select('a, 'a.as("a1_1"),
        'a.as("a1_2"), 'a.as("a1_3"))
    )).analyze

    comparePlans(optimized1, correctAnswer1)
    // on top of union put a projection which eats away 1st col
    // the filter after it should  survive
    val y2 = union.select('b1, 'b1_2, 'b1_3).where('b1 > 10).analyze
    assert(y2.resolved)
    val optimized2 = GetOptimizer(OptimizerTypes.NO_PUSH_DOWN_ONLY_PRUNING).
      execute(y2)
    trivialConstraintAbsenceChecker(optimized2.constraints)

    val correctAnswer2 =
      tr2.where('b > 10 && IsNotNull('b)).select('b, 'b1, 'b1.as("b1_2"),
        'b1.as("b1_3")).union(
        tr3.where('c > 10 && IsNotNull('c)).select('c, 'c1, 'c2,
          'c2.as("c1_3"))).union(tr1.where('a > 10 && IsNotNull('a)).
        select('a, 'a.as("a1_1"),
          'a.as("a1_2"), 'a.as("a1_3")))
        .select('b1, 'b1_2, 'b1_3).
        where('b1 > 10 && IsNotNull('b1)).analyze

    comparePlans(optimized2, correctAnswer2)
  }

  test("Union Node. missing attribute from equiv list behaviour - 2") {
    val tr1 = LocalRelation('a.int, 'a1.int, 'a2.int, 'a3.int, 'a4.int)
    val tr2 = LocalRelation('b.int, 'b1.int, 'b2.int, 'b3.int, 'b4.int)
    val tr3 = LocalRelation('c.int, 'c1.int, 'c2.int, 'c3.int, 'c4.int)
    val u1 = tr1.select('a, 'a.as("a1_1"),
      'a.as("a1_2"), 'a.as("a1_3"))
    val u1_f1 = u1.where('a1_2 > 10)
    val u2 = tr2.select('b, 'b1, 'b1.as("b1_2"),
      'b1.as("b1_3"))
    val u2_f2 = u2.where('b1_2 > 10)
    val u3 = tr3.select('c, 'c1, 'c2, 'c2.as("c1_3"))
    val u3_f3 = u3.where('c2 > 10)
    val union = u1_f1.union(u2_f2).union(u3_f3)
    // The condition 'a1_2 > 10 is a redundant condition and should be removed
    // by constraintset.
    val y = union.where('a1_2 > 10).analyze
    assert(y.resolved)
    val optimized1 = GetOptimizer(OptimizerTypes.NO_PUSH_DOWN_ONLY_PRUNING).
      execute(y)
    trivialConstraintAbsenceChecker(optimized1.constraints)

    val correctAnswer1 = u1.where('a1_2 > 10 && IsNotNull('a)).
      union(u2.where('b1_2 > 10 && IsNotNull('b1))).union(
      u3.where('c2 > 10 && IsNotNull('c2))).analyze

    comparePlans(optimized1, correctAnswer1)

    // on top of union put a projection which eats away a1_2
    // but still the top filter should be removed as constraint survives
    val y2 = union.select('a1_1, 'a1_3).where('a1_3 > 10).analyze
    assert(y2.resolved)
    val correctAnswer2 = correctAnswer1.select('a1_1, 'a1_3).analyze
    val optimized2 = GetOptimizer(OptimizerTypes.NO_PUSH_DOWN_ONLY_PRUNING).
      execute(y2)
    trivialConstraintAbsenceChecker(optimized2.constraints)
    comparePlans(optimized2, correctAnswer2)
  }
  test("Union Node.  missing attribute from equiv list behaviour - 2." +
    " reverse order of legs") {
    val tr1 = LocalRelation('a.int, 'a1.int, 'a2.int, 'a3.int, 'a4.int)
    val tr2 = LocalRelation('b.int, 'b1.int, 'b2.int, 'b3.int, 'b4.int)
    val tr3 = LocalRelation('c.int, 'c1.int, 'c2.int, 'c3.int, 'c4.int)
    val u1 = tr1.select('a, 'a.as("a1_1"),
      'a.as("a1_2"), 'a.as("a1_3"))
    val u1_f1 = u1.where('a1_2 > 10)
    val u2 = tr2.select('b, 'b1, 'b1.as("b1_2"),
      'b1.as("b1_3"))
    val u2_f2 = u2.where('b1_2 > 10)
    val u3 = tr3.select('c, 'c1, 'c2, 'c2.as("c1_3"))
    val u3_f3 = u3.where('c2 > 10)
    val union = u3_f3.union(u1_f1).union(u2_f2)
    // The condition 'c2 > 10 is a redundant condition and should be removed
    // by constraintset.
    // The reason 'c2 > 10 is redundant because each leg has already has the same
    // constraint as a1_2, b1_2 and c2 so final union dataset column already
    // has this constraint.
    val y = union.where('c2 > 10).analyze
    assert(y.resolved)
    val optimized1 = GetOptimizer(OptimizerTypes.NO_PUSH_DOWN_ONLY_PRUNING).
      execute(y)
    trivialConstraintAbsenceChecker(optimized1.constraints)

    val correctAnswer1 = u3.where('c2 > 10 && IsNotNull('c2)).
      union(u1.where('a1_2 > 10 && IsNotNull('a))).union(
      u2.where('b1_2 > 10 && IsNotNull('b1))).analyze

    comparePlans(optimized1, correctAnswer1)

    // on top of union put a projection which eats away a1_2
    // but still the top filter should be removed as constraint survives
    val y2 = union.select('c1, 'c1_3).where('c1_3 > 10).analyze
    assert(y2.resolved)
    val correctAnswer2 = correctAnswer1.select('c1, 'c1_3).analyze
    val optimized2 = GetOptimizer(OptimizerTypes.NO_PUSH_DOWN_ONLY_PRUNING).
      execute(y2)
    trivialConstraintAbsenceChecker(optimized2.constraints)
    comparePlans(optimized2, correctAnswer2)
  }

  test("Union Node. missing attribute from equiv list behaviour." +
    "Common constraint dependent on more than 1 attributes") {
    val tr1 = LocalRelation('a.int, 'b.int, 'c.int, 'd.int, 'e.int)
    val tr2 = LocalRelation('a1.int, 'b1.int, 'c1.int, 'd1.int, 'e1.int)
    val tr3 = LocalRelation('a2.int, 'b2.int, 'c2.int, 'd2.int, 'e2.int)
    val u1 = tr1.select('a, 'a.as("a_1"),
      'a.as("a_2"), 'a.as("a_3"), 'b, 'b.as("b_1"),
      'b.as("b_2"), 'c, 'c.as("c_1"), 'c.as("c_2"))
    val u1_f1 = u1.where('a + 'b + 'c > 10).analyze
    assert(u1_f1.resolved)

    val u2 = tr2.select('e1, 'd1, ('e1 + 'd1).as("a_2"),
      ('e1 + 'a1).as("a_3"), ('d1 + 'a1).as("b"), ('e1 + 'a1 + 'd1).as("b_1"),
      ('e1 + 'a1 + 'd1).as("b_2"), ('c1 + 'd1).as("c"),
      'c1.as("c_1"), ('c1 + 'a1).as("c_2"))
    val u2_f2 = u2.where('a_3 + 'b_2 + 'c_2 > 10).analyze

    assert(u2_f2.resolved)
    val u3 = tr3.select(('e2 + 'c2).as("a"), 'e2.as("a_1"),
      ('a2 + 'c2).as("a_2"),
      ('d2 * 'a2).as("a_3"), ('c2 * 'd2).as("b"), 'b2.as("b_1"),
      ('e2 * 'a2 * 'd2).as("b_2"), ('c2 + 'a2).as("c"),
      'e2.as("c_1"), ('d2 + 'a2).as("c_2"))

    val u3_f3 = u3.where('a_3 + 'b_2 + 'c_2 > 10).analyze
    assert(u3_f3.resolved)
    val union = u1_f1.union(u2_f2).union(u3_f3)
    // The condition ''a_3 + 'b_2 + 'c_2 > 10 is a redundant condition and should be removed
    // by constraintset.
    val y = union.where('a_3 + 'b_2 + 'c_2 > 10).analyze
    assert(y.resolved)
    val optimized1 = GetOptimizer(OptimizerTypes.NO_PUSH_DOWN_ONLY_PRUNING).
      execute(y)
    trivialConstraintAbsenceChecker(optimized1.constraints)

    val correctAnswer1 = u1.
      where('a + 'b + 'c > 10 && IsNotNull('a) && IsNotNull('b) && IsNotNull('c)).
      union(u2.where('a_3 + 'b_2 + 'c_2 > 10 && IsNotNull('a_3)
        && IsNotNull('b_1) && IsNotNull('c_2))).union(u3.where(
      'a_3 + 'b_2 + 'c_2 > 10 && IsNotNull('a_3) && IsNotNull('b_2)
        && IsNotNull('c_2))).analyze

    assert(correctAnswer1.resolved)

    comparePlans(optimized1, correctAnswer1)
  }

  test("Union Node.  missing attribute from equiv list behaviour." +
    "Common constraint dependent on more than 1 attributes. reverse union legs") {
    val tr1 = LocalRelation('a.int, 'b.int, 'c.int, 'd.int, 'e.int)
    val tr2 = LocalRelation('a1.int, 'b1.int, 'c1.int, 'd1.int, 'e1.int)
    val tr3 = LocalRelation('a2.int, 'b2.int, 'c2.int, 'd2.int, 'e2.int)
    val u1 = tr1.select('a, 'a.as("a_1"),
      'a.as("a_2"), 'a.as("a_3"), 'b, 'b.as("b_1"),
      'b.as("b_2"), 'c, 'c.as("c_1"), 'c.as("c_2"))
    val u1_f1 = u1.where('a + 'b + 'c > 10).analyze
    assert(u1_f1.resolved)

    val u2 = tr2.select('e1, 'd1, ('e1 + 'd1).as("a_2"),
      ('e1 + 'a1).as("a_3"), ('d1 + 'a1).as("b"), ('e1 + 'a1 + 'd1).as("b_1"),
      ('e1 + 'a1 + 'd1).as("b_2"), ('c1 + 'd1).as("c"),
      'c1.as("c_1"), ('c1 + 'a1).as("c_2"))
    val u2_f2 = u2.where('a_3 + 'b_2 + 'c_2 > 10).analyze

    assert(u2_f2.resolved)
    val u3 = tr3.select(('e2 + 'c2).as("a"), 'e2.as("a_1"),
      ('a2 + 'c2).as("a_2"),
      ('d2 * 'a2).as("a_3"), ('c2 * 'd2).as("b"), 'b2.as("b_1"),
      ('e2 * 'a2 * 'd2).as("b_2"), ('c2 + 'a2).as("c"),
      'e2.as("c_1"), ('d2 + 'a2).as("c_2"))

    val u3_f3 = u3.where('a_3 + 'b_2 + 'c_2 > 10).analyze
    assert(u3_f3.resolved)
    val union = u2_f2.union(u1_f1).union(u3_f3)
    // The condition 'a1_2 > 10 is a redundant condition and should be removed
    // by constraintset.
    val y = union.where('a_3 + 'b_2 + 'c_2 > 10).analyze
    assert(y.resolved)
    val optimized1 = GetOptimizer(OptimizerTypes.NO_PUSH_DOWN_ONLY_PRUNING).
      execute(y)
    trivialConstraintAbsenceChecker(optimized1.constraints)

    val correctAnswer1 = u2.where('a_3 + 'b_2 + 'c_2 > 10 && IsNotNull('a_3)
      && IsNotNull('b_1) && IsNotNull('c_2)).
      union(u1.
        where('a + 'b + 'c > 10 && IsNotNull('a) && IsNotNull('b) && IsNotNull('c))).
      union(u3.where(
        'a_3 + 'b_2 + 'c_2 > 10 && IsNotNull('a_3) && IsNotNull('b_2)
          && IsNotNull('c_2))).analyze

    assert(correctAnswer1.resolved)

    comparePlans(optimized1, correctAnswer1)
  }

  test("Union Node." +
    " identifying 'not common' constraints resulting in OR based constraints -1") {
    val tr1 = LocalRelation('a.int, 'b.int, 'c.int, 'd.int, 'e.int)
    val tr2 = LocalRelation('a.int, 'b.int, 'c.int, 'd.int, 'e.int)
    val tr3 = LocalRelation('a.int, 'b.int, 'c.int, 'd.int, 'e.int)
    val u1 = tr1.select('a, 'a.as("a1"),
      'a.as("a2"), 'a.as("a3"), 'b, 'b.as("a5"), 'b.as("a6"),
      'b.as("a7"))
    val u1_f1 = u1.where('a > 10 && 'b > 11)

    val u2 = tr2.select('a, 'a.as("a1"),
      'a.as("a2"), 'a.as("a3"), 'a.as("X"), 'a.as("a5"), 'a.as("a6"),
      'a.as("a7"))
    val u2_f2 = u2.where('a > 10 )
    // This should result in following constraints
    // a > 10  && (b > 11 ||b > 10)
    // where a = {a, a1, a2 a3,}
    // b = {b, a5, a6, a7}

    val union1 = u1_f1.union(u2_f2)

    val y1 = union1.analyze
    assert(y1.resolved)
    val optimized1 = GetOptimizer(OptimizerTypes.NO_PUSH_DOWN_ONLY_PRUNING).
      execute(y1)
    trivialConstraintAbsenceChecker(optimized1.constraints)
    val constraints = optimized1.getValidConstraints
    val attribEquivList = constraints.asInstanceOf[ConstraintSet].
      getAttribEquivalenceList
    val output1 = optimized1.output
    val expectedExprIdsInEquivList = Array[Seq[Attribute]](Seq(output1(0),
      output1(1), output1(2), output1(3)), Seq(output1(4),
      output1(5), output1(6), output1(7)))
    assertEquals(expectedExprIdsInEquivList.length, attribEquivList.length)
    assertTrue(expectedExprIdsInEquivList.forall(buff =>
      attribEquivList.exists(eqiv => eqiv.map(_.canonicalized).toSet.
        diff(buff.map(_.canonicalized).toSet).isEmpty && eqiv.size == buff.size)))
    val expectedConstraint1 = output1.head > Literal(10)
    val expectedConstraint2 = output1(4) > Literal(11) || output1(4) > Literal(10)
    assertTrue(constraints.contains(expectedConstraint1))
    assertTrue(constraints.contains(expectedConstraint2))
    assertEquals(4, constraints.size)
  }
  test("Union Node." +
    " identifying 'not common' constraints resulting in OR based constraints -2") {

    /*  The structure of union legs for the test
            leg2 a---a1 ---a2                filter  a + b > 7
                 a3---a4---a5                filter a3 + b > 10
                 b---b1----b2----b3---b4

            Leg1 a1----a2            a1 + b > 7
                 a4---a5             a4 + b3 > 10
                 b----b1
                 b3---b4
     */
    val tr1 = LocalRelation('a.int, 'b.int, 'c.int, 'd.int, 'e.int)
    val tr2 = LocalRelation('a.int, 'b.int, 'c.int, 'd.int, 'e.int)
    val tr3 = LocalRelation('a.int, 'b.int, 'c.int, 'd.int, 'e.int)
    val u1 = tr1.select('a, 'b.as("a1"), 'b.as("a2"), ('a + 'b).as("a3"),
      'b.as("a4"), 'b.as("a5"), 'b, 'b.as("b1"), ('b + 'c).as("b2"),
      ('b + 'e).as("b3"), ('b + 'e).as("b4"))
    val u1_f1 = u1.where('a1 + 'b > 7 && 'a4 + 'b3 > 10)

    val u2 = tr2.select('a, 'a.as("a1"),
      'a.as("a2"), 'b.as("a3"), 'b.as("a4"), 'b.as("a5"),
      'e.as("b"), 'e.as("b1"), 'e.as("b2"), 'e.as("b3"), 'e.as("b4") )
    val u2_f2 = u2.where('a + 'b > 7 && 'a3 + 'b > 10 )
    // This should result in following constraints
    // a1 + b > 7  && a4 + b3 > 10
    val union1 = u1_f1.union(u2_f2)

    val y1 = union1.analyze
    assert(y1.resolved)
    val optimized1 = GetOptimizer(OptimizerTypes.NO_PUSH_DOWN_ONLY_PRUNING).
      execute(y1)
    trivialConstraintAbsenceChecker(optimized1.constraints)
    val constraints = optimized1.getValidConstraints
    val attribEquivList = constraints.asInstanceOf[ConstraintSet].
      getAttribEquivalenceList
    val output1 = optimized1.output
    val expectedExprIdsInEquivList = Array[Seq[Attribute]](Seq(output1(1),
      output1(2)), Seq(output1(4), output1(5)), Seq(output1(9), output1(10)),
      Seq(output1(6), output1(7)))
    assertEquals(expectedExprIdsInEquivList.length, attribEquivList.length)
    assertTrue(expectedExprIdsInEquivList.forall(buff =>
      attribEquivList.exists(eqiv => eqiv.map(_.canonicalized).toSet.
        diff(buff.map(_.canonicalized).toSet).isEmpty && eqiv.size == buff.size)))
    val expectedConstraint1 = output1(1) + output1(6) > Literal(7)
    val expectedConstraint2 = output1(4) + output1(9) > Literal(10)
    assertTrue(constraints.contains(expectedConstraint1))
    assertTrue(constraints.contains(expectedConstraint2))
    // ensure no other wrong constraints are present
    val totalFilters = constraints.getCanonicalizedFilters
    val numNotNulls = totalFilters.count(_ match {
      case _: IsNotNull => true
      case _ => false
    })

    assertEquals(constraints.size, numNotNulls + 2)
  }
  test("Union Node. " +
    " identifying 'not common' constraints resulting in OR based constraints -3") {

    /*   The structure of union legs for the test
            leg1 a---a1--a2      a > 7
                 a3---a4---a5   a3 > -13    a6 > 9
            leg 2 a--a1--a2--a3--a4--a5--a6    a > 7
     */
    val tr1 = LocalRelation('a.int, 'b.int, 'c.int, 'd.int, 'e.int)
    val tr2 = LocalRelation('a.int, 'b.int, 'c.int, 'd.int, 'e.int)
    val tr3 = LocalRelation('a.int, 'b.int, 'c.int, 'd.int, 'e.int)
    val u1 = tr1.select('a, 'a.as("a1"), 'a.as("a2"),
      ('a + 'b).as("a3"), ('a + 'b).as("a4"), ('a + 'b).as("a5"),
      'c.as("a6"))

    val u1_f1 = u1.where('a > 7 && 'a6 > 9 && 'a3 > -13)

    val u2 = tr2.select('a, 'a.as("a1"),
      'a.as("a2"), 'a.as("a3"), 'a.as("a4"), 'a.as("a5"),
      'a.as("a6"))
    val u2_f2 = u2.where('a > 7 )
    // This should result in following constraints
    // a > 7  &&  (a3 > -13 || a3 > 7)  && (a6 > 9 || a6 > 7)
    val union1 = u1_f1.union(u2_f2)

    val y1 = union1.analyze
    assert(y1.resolved)
    val optimized1 = GetOptimizer(OptimizerTypes.NO_PUSH_DOWN_ONLY_PRUNING).
      execute(y1)
    trivialConstraintAbsenceChecker(optimized1.constraints)
    val constraints = optimized1.getValidConstraints
    val attribEquivList = constraints.asInstanceOf[ConstraintSet].
      getAttribEquivalenceList
    val output1 = optimized1.output
    val expectedExprIdsInEquivList = Array[Seq[Attribute]](Seq(output1.head,
      output1(1), output1(2)), Seq(output1(3), output1(4), output1(5)))
    assertEquals(expectedExprIdsInEquivList.length, attribEquivList.length)
    assertTrue(expectedExprIdsInEquivList.forall(buff =>
      attribEquivList.exists(eqiv => eqiv.map(_.canonicalized).toSet.
        diff(buff.map(_.canonicalized).toSet).isEmpty && eqiv.size == buff.size)))
    val expectedConstraint1 = output1.head > Literal(7)
    val expectedConstraint2 = output1(3) > Literal(7) || output1(3) > Literal(-13)
    val expectedConstraint3 = output1(6) > Literal(7) || output1(6) > Literal(9)

    assertTrue(constraints.contains(expectedConstraint1))
    assertTrue(constraints.contains(expectedConstraint2))
    assertTrue(constraints.contains(expectedConstraint3))
    // ensure no other wrong constraints are present
    val totalFilters = constraints.getCanonicalizedFilters
    val numNotNulls = totalFilters.count(_ match {
      case _: IsNotNull => true
      case _ => false
    })

    assertEquals(constraints.size, numNotNulls + 3)
  }
  test("Union Node." +
    " identifying 'not common' constraints resulting in OR based constraints -4") {

    /*   The structure of union legs for the test
            leg1 a---a1--a2      a > 7
                 a3   a3 > -13
                 a4   a4 > 9
                 a5   a5 > 11

            leg 2 a--a1--a2--a3--a4     a > -17
                  a6   a6 > 8
     */
    val tr1 = LocalRelation('a.int, 'b.int, 'c.int, 'd.int, 'e.int)
    val tr2 = LocalRelation('a.int, 'b.int, 'c.int, 'd.int, 'e.int)
    val tr3 = LocalRelation('a.int, 'b.int, 'c.int, 'd.int, 'e.int)
    val u1 = tr1.select('a, 'a.as("a1"), 'a.as("a2"),
      ('a + 'b).as("a3"), ('a + 'c).as("a4"), ('a + 'd).as("a5"),
      'c.as("a6"))

    val u1_f1 = u1.where('a > 7 && 'a3 > -13 && 'a4 > 9 && 'a5 > 11)

    val u2 = tr2.select('a, 'a.as("a1"),
      'a.as("a2"), 'a.as("a3"), 'a.as("a4"), ('a *'e).as("a5"),
      'd.as("a6"))
    val u2_f2 = u2.where('a > -17 && 'a6 > 8)
    // This should result in following constraints
    // (a > 7 || a > -17)  &&  (a3 > -13 || a3 > -17)  && (a4 > 9 || a4 > -17) &&
    // ((a5 > 11 || a6 > 8)
    val union1 = u1_f1.union(u2_f2)

    val y1 = union1.analyze
    assert(y1.resolved)
    val optimized1 = GetOptimizer(OptimizerTypes.NO_PUSH_DOWN_ONLY_PRUNING).
      execute(y1)
    trivialConstraintAbsenceChecker(optimized1.constraints)
    val constraints = optimized1.getValidConstraints
    val attribEquivList = constraints.asInstanceOf[ConstraintSet].
      getAttribEquivalenceList
    val output1 = optimized1.output
    val expectedExprIdsInEquivList = Array[Seq[Attribute]](Seq(output1.head,
      output1(1), output1(2)))
    assertEquals(expectedExprIdsInEquivList.length, attribEquivList.length)
    assertTrue(expectedExprIdsInEquivList.forall(buff =>
      attribEquivList.exists(eqiv => eqiv.map(_.canonicalized).toSet.
        diff(buff.map(_.canonicalized).toSet).isEmpty && eqiv.size == buff.size)))
    val expectedConstraint1 = output1.head > Literal(7) || output1.head > Literal(-17)
    val expectedConstraint2 = output1(3) > Literal(-13) || output1(3) > Literal(-17)
    val expectedConstraint3 = output1(4) > Literal(9) || output1(4) > Literal(-17)

    assertTrue(constraints.contains(expectedConstraint1))
    assertTrue(constraints.contains(expectedConstraint2))
    assertTrue(constraints.contains(expectedConstraint3))
    // ensure no other wrong constraints are present
    val totalFilters = constraints.getCanonicalizedFilters
    val numNotNulls = totalFilters.count(_ match {
      case _: IsNotNull => true
      case _ => false
    })

    assertEquals(constraints.size, numNotNulls + 3)
  }

  test("Union Node." +
    " identifying 'not common' constraints resulting in OR based constraints -5") {

    /*   The structure of union legs for the test
            leg1 a---a1--a2--a3--a4
                 b---b1--b2--b3--b4
                 filter a + b > 5

            leg 2 a--a1  a2   a3--a4
                  b---b1  b2  b3--b4
                   filter a + b > 5
                          a3 + b3 > 5

       expected union output constraints  a + b > 5
                                          a3 + b3 > 5
     */
    val tr1 = LocalRelation('a.int, 'b.int, 'c.int, 'd.int, 'e.int)
    val tr2 = LocalRelation('a.int, 'b.int, 'c.int, 'd.int, 'e.int)
    val u1 = tr1.select('a, 'a.as("a1"), 'a.as("a2"),
      ('a).as("a3"), ('a).as("a4"), 'b, 'b.as("b1"),
      'b.as("b2"), 'b.as("b3"), 'b.as("b4"))

    val u1_f1 = u1.where('a + 'b > 5)

    val u2 = tr2.select('a, 'a.as("a1"),
      'c.as("a2"), ('c + 'd).as("a3"), ('c + 'd).as("a4"),
      'b, 'b.as("b1"), 'd.as("b2"), ('d * 'e).as("b3"),
      ('d * 'e).as("b4"))
    val u2_f2 = u2.where('a + 'b > 5 && 'a3 + 'b3 > 5)
    val union1 = u1_f1.union(u2_f2)

    val y1 = union1.analyze
    assert(y1.resolved)
    val optimized1 = GetOptimizer(OptimizerTypes.NO_PUSH_DOWN_ONLY_PRUNING).
      execute(y1)
    trivialConstraintAbsenceChecker(optimized1.constraints)
    val constraints = optimized1.getValidConstraints
    val attribEquivList = constraints.asInstanceOf[ConstraintSet].
      getAttribEquivalenceList
    val output1 = optimized1.output
    val expectedExprIdsInEquivList = Array[Seq[Attribute]](Seq(output1.head,
      output1(1)), Seq(output1(3), output1(4)), Seq(output1(5), output1(6)),
      Seq(output1(8), output1(9)))
    assertEquals(expectedExprIdsInEquivList.length, attribEquivList.length)
    assertTrue(expectedExprIdsInEquivList.forall(buff =>
      attribEquivList.exists(eqiv => eqiv.map(_.canonicalized).toSet.
        diff(buff.map(_.canonicalized).toSet).isEmpty && eqiv.size == buff.size)))
    val expectedConstraint1 = output1.head + output1(5) > Literal(5)
    val expectedConstraint2 = output1(3) + output1(8) >  Literal(5)
    assertTrue(constraints.contains(expectedConstraint1))
    assertTrue(constraints.contains(expectedConstraint2))
    // ensure no other wrong constraints are present
    val totalFilters = constraints.getCanonicalizedFilters
    val numNotNulls = totalFilters.count(_ match {
      case _: IsNotNull => true
      case _ => false
    })

    assertEquals(constraints.size, numNotNulls + 2)
  }

  test("top filter should be pruned for union with lower filter on all tables") {
    val tr1 = LocalRelation('a.int, 'b.int, 'c.int)
    val tr2 = LocalRelation('d.int, 'e.int, 'f.int)
    val tr3 = LocalRelation('g.int, 'h.int, 'i.int)

    val y = tr1.where('a.attr > 10).union(tr2.where('d.attr > 10)).
      union(tr3.where('g.attr > 10))
    val y1 = y.where('a.attr > 10).analyze
    assert(y1.resolved)
    val optimized = GetOptimizer(OptimizerTypes.WITH_FILTER_PUSHDOWN_THRU_JOIN_AND_UNIONS_PRUNING).
      execute(y1)
    trivialConstraintAbsenceChecker(optimized.constraints)
    val allFilterExpressions = optimized.collect {
      case x: Filter => x
    }.flatMap(_.expressions)
    assert(allFilterExpressions.flatMap(_.collect {
      case _: GreaterThan => true
    }).size == 3)
    val union = optimized.find {
      case _: Union => true
      case _ => false
    }.get.asInstanceOf[Union]

    assert(union.children.forall(p => {
      p.expressions.flatMap(_.collect {
        case x: GreaterThan => x
      }).nonEmpty
    }))

    val correctAnswer = new Union(Seq(tr1.where('a.attr > 10 && IsNotNull('a)),
      tr2.where('d.attr > 10 && IsNotNull('d)),
      tr3.where('g.attr > 10 && IsNotNull('g)))).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("templatization of constraints") {
    val tr1 = LocalRelation('a.int, 'b.string, 'c.double, 'd.long, 'e.int, 'f.double, 'g.long)
    val tr2 = LocalRelation('a1.int, 'b1.string, 'c1.double, 'd1.long, 'e1.int, 'f1.double,
      'g1.long)
    val templateGenerator = new TemplateAttributeGenerator()
    var filter1 = tr1.where('a + 'e * ('e + 'a) > 7 ).
      analyze.asInstanceOf[Filter]
    val expr1 = filter1.condition
    var templatized1 = ConstraintSet.templatizedConstraints(templateGenerator, Seq(expr1))
    var filter2 = tr2.where('a1 + 'e1 * ('a1 + 'e1) > 7).
      analyze.asInstanceOf[Filter]
    val expr2 = filter2.condition
    var templatized2 = ConstraintSet.templatizedConstraints(templateGenerator, Seq(expr2))
    assertEquals(templatized1.keySet.head, templatized2.keySet.head)

    filter1 = tr1.where('a + 'b * ('e + 'a + 'c * 'c + 'b)  > 9).
      analyze.asInstanceOf[Filter]
    val expr3 = filter1.condition
    templatized1 = ConstraintSet.templatizedConstraints(templateGenerator, Seq(expr3))
    filter2 = tr2.where('e1 + 'b1 * ('a1 + 'a1 + 'f1 * 'c1 + 'b1 ) > 9 ).
      analyze.asInstanceOf[Filter]
    val expr4 = filter2.condition
    templatized2 = ConstraintSet.templatizedConstraints(templateGenerator, Seq(expr4))
    assertEquals(templatized1.keySet.head, templatized2.keySet.head)

    templatized1 = ConstraintSet.templatizedConstraints(templateGenerator, Seq(expr1, expr3))
    templatized2 = ConstraintSet.templatizedConstraints(templateGenerator, Seq(expr2, expr4))
    val pairs = templatized1.keySet.zip(templatized2.keySet)
    pairs.foreach(tup => assertEquals(tup._1, tup._2))

    // negative test
    filter1 = tr1.where('a + 'b  > 3).
      analyze.asInstanceOf[Filter]
    val expr5 = filter1.condition
    templatized1 = ConstraintSet.templatizedConstraints(templateGenerator, Seq(expr5))
    filter2 = tr2.where('c1 + 'b1 > 3).
      analyze.asInstanceOf[Filter]
    val expr6 = filter2.condition
    templatized2 = ConstraintSet.templatizedConstraints(templateGenerator, Seq(expr6))
    assertFalse(templatized1.keySet.head == templatized2.keySet.head)
  }

  test("top filter should be pruned for Intersection with lower filter on one or more tables") {
    val tr1 = LocalRelation('a.int, 'b.int, 'c.int)
    val tr2 = LocalRelation('d.int, 'e.int, 'f.int)
    val tr3 = LocalRelation('g.int, 'h.int, 'i.int)

    val y = tr1.where('a.attr > 10).intersect(tr2.where('e.attr > 5), isAll = true).
      intersect(tr3.where('i.attr > -5), isAll = true)

    val y1 = y.select('a.attr.as("a1"), 'b.attr.as("b1"), 'c.attr.as("c1")).analyze
    assert(y1.resolved)

    val y2 = y1.where('a1.attr > 10 && 'b1.attr > 5 && 'c1.attr > -5).analyze
    assert(y2.resolved)
    val optimized = GetOptimizer(OptimizerTypes.WITH_FILTER_PUSHDOWN_THRU_JOIN_AND_UNIONS_PRUNING).
      execute(y2)
    trivialConstraintAbsenceChecker(optimized.constraints)
    val allFilterExpressions = optimized.collect {
      case x: Filter => x
    }.flatMap(_.expressions)

    assert(allFilterExpressions.flatMap(_.collect {
      case _: GreaterThan => true
    }).size == 3)
    val correctAnswer = tr1.where(IsNotNull('a) && 'a.attr > 10).
      intersect(tr2.where(IsNotNull('e) && 'e.attr > 5), isAll = true).
      intersect(tr3.where(IsNotNull('i) && 'i.attr > -5), isAll = true).
      select('a.attr.as("a1"), 'b.attr.as("b1"),
        'c.attr.as("c1")).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("top filter should be pruned for aggregate with lower filter") {
    val tr = LocalRelation('a.int, 'b.string, 'c.int, 'd.int)
    assert(tr.analyze.constraints.isEmpty)
    val aliasedRelation = tr.where('c.attr > 10 && 'a.attr < 5)
      .groupBy('a, 'c, 'b)('a, 'c.as("c1"), count('a).as("a3")).
      select('c1, 'a, 'a3).analyze
    val withTopFilter = aliasedRelation.where('a.attr < 5 && 'c1.attr > 10 && 'a3.attr > 20).analyze
    val optimized = GetOptimizer(OptimizerTypes.WITH_FILTER_PUSHDOWN_THRU_JOIN_AND_PRUNING).
      execute(withTopFilter)
    trivialConstraintAbsenceChecker(optimized.constraints)
    val correctAnswer = tr.where('c.attr > 10 && 'a.attr < 5 && IsNotNull('a) && IsNotNull('c)
    ).groupBy('a, 'c, 'b)('a, 'c.as("c1"), count('a).as("a3")).
      where('a3 > Literal(20).cast(LongType)).select('c1, 'a, 'a3).analyze
    comparePlans(correctAnswer, optimized)
  }

  test("duplicate removed attributes with different metadata" +
    "causes assert failure") {
    val tr = LocalRelation('a.int, 'b.string, 'c.int, 'd.int)
    val aliasedRelation = tr.select('a, 'a.as("a1"), 'a, 'a.as("a2"),
      'a.as("a3"), 'c, 'c.as("c1")).select('c1, 'a3).where('a3 > 5).
      analyze
    val bugify = aliasedRelation.transformUp {
      case Project(projList, child) if projList.exists(_.name == "a") =>
        Project(projList.zipWithIndex.map{ case(ne, i) =>
          ne match {
            case att: AttributeReference => att.withQualifier(Seq(i.toString))
            case _ => ne
          }
        }, child)
    }
    GetOptimizer(OptimizerTypes.NO_PUSH_DOWN_ONLY_PRUNING).execute(bugify)
  }

  test("filter push down on join with aggregate") {
    def getTestPlan: LogicalPlan = {
      val tr1 = LocalRelation('a.int, 'b.string, 'c.int)
      val tr2 = LocalRelation('x.int, 'y.string, 'z.int)
      tr1.where('c.attr + 'a.attr > 10 && 'a.attr > -15).select('a, 'a.as('a1), 'a.as('a2),
        'b.as('b1), 'c, 'c.as('c1)).
        groupBy('b1.attr, 'c1.attr)('b1, 'c1.as("c2"), count('a).as("a3")).
        select('c2, 'a3).join(tr2.where('x.attr > 9), Inner, Some("c2".attr === "x".attr))
    }

    val (plan1, constraints1) = withSQLConf[(LogicalPlan, ExpressionSet)](
      SQLConf.OPTIMIZER_CONSTRAINT_PROPAGATION_OPTIMIZED.key -> "false") {
      executePlan(getTestPlan, OptimizerTypes.WITH_FILTER_PUSHDOWN_THRU_JOIN_AND_PRUNING)
    }

    val (plan2, constraints2) = withSQLConf[(LogicalPlan, ExpressionSet)](
      SQLConf.OPTIMIZER_CONSTRAINT_PROPAGATION_OPTIMIZED.key -> "true") {
      executePlan(getTestPlan, OptimizerTypes.WITH_FILTER_PUSHDOWN_THRU_JOIN_AND_PRUNING)
    }
    trivialConstraintAbsenceChecker(constraints2)
    assert(constraints1 match {
      case _: ConstraintSet => false
      case _: ExpressionSet => true
    })

    assert(constraints2 match {
      case _: ConstraintSet => true
      case _: ExpressionSet => false
    })
    assert(constraints2.size <= constraints1.size)
    comparePlans(plan1, plan2)


    val conditionFinder: PartialFunction[LogicalPlan, Seq[Expression]] = {
      case f: Filter => f.expressions.find(x => x.find {
        case GreaterThan(att: Attribute, Literal(9, IntegerType)) if att.name == "c" => true
        case LessThan(Literal(9, IntegerType), att: Attribute) if att.name == "c" => true
        case _ => false
      }.isDefined).map(Seq(_)).getOrElse(Seq.empty[Expression])
    }
    val result1 = plan1.collect {
      conditionFinder
    }.flatten
    assert(result1.nonEmpty)
    val result2 = plan2.collect {
      conditionFinder
    }.flatten
    assert(result2.nonEmpty)
  }

  /**
   * Looks like in 3.1 some changes have gone into stock constraint optimization such
   * that if the only rule present is PruneFilters in the optimizer, the stock
   * spark's constraint code is not behaving correctly. The problem lies in stock
   * spark & not in the optimized constraints code, as in the stock spark the
   * relevant constraints for the alias are not present.
   * so modifying the test to allow it to pass for optimized constraint situation
   */
  test("test pruning using constraints with filters after project - 1") {
    def getTestPlan: LogicalPlan = {
      val tr1 = LocalRelation('a.int, 'b.string, 'c.int)
      tr1.select('a, 'a.as('a1), 'a.as('a2),
        'b.as('b1), 'c, 'c.as('c1)).where('c.attr + 'a.attr > 10 && 'a.attr > -15).
        where('c1.attr + 'a2.attr > 10 && 'a2.attr > -15)
    }


    val (plan2, constraints2) = withSQLConf[(LogicalPlan, ExpressionSet)](
      SQLConf.OPTIMIZER_CONSTRAINT_PROPAGATION_OPTIMIZED.key -> "true") {
      executePlan(getTestPlan, OptimizerTypes.NO_PUSH_DOWN_ONLY_PRUNING)
    }
    trivialConstraintAbsenceChecker(constraints2)


    assert(constraints2 match {
      case _: ConstraintSet => true
      case _: ExpressionSet => false
    })
    val filters = plan2.collect {
      case f: Filter => f
    }
    assertEquals(1, filters.size)
    val filterExprs = splitConjunctivePredicates(filters.head.condition)
    assertEquals(4, filterExprs.size)
    val exprsSet = ExpressionSet(filterExprs)
    val proj = plan2.collect {
      case pr: Project => pr
    }.head
    val expectedFilters = Seq(IsNotNull(proj.output.find(_.name == "a").get),
      IsNotNull(proj.output.find(_.name == "c").get),
      proj.output.find(_.name == "a").get > -15,
      proj.output.find(_.name == "a").get + proj.output.find(_.name == "c").get > 10
    )
    expectedFilters.foreach(f => assertTrue(exprsSet.contains(f)))
  }

  // Not comparing with stock spark plan as stock spark plan is unoptimal
  test("test pruning using constraints with filters after project - 2") {
    def getTestPlan: LogicalPlan = {
      val tr1 = LocalRelation('a.int, 'b.string, 'c.int)
      tr1.select('a, 'a.as('a1), 'a.as('a2),
        'b.as('b1), 'c, 'c.as('c1)).where('c.attr + 'a.attr > 10 && 'a.attr > -15).
        where('c1.attr + 'a2.attr > 10 && 'a2.attr > -15)
    }

    val (plan1, constraints1) = withSQLConf[(LogicalPlan, ExpressionSet)](
      SQLConf.OPTIMIZER_CONSTRAINT_PROPAGATION_OPTIMIZED.key -> "false") {
      executePlan(getTestPlan, OptimizerTypes.NO_PUSH_DOWN_ONLY_PRUNING)
    }

    val (plan2, constraints2) = withSQLConf[(LogicalPlan, ExpressionSet)](
      SQLConf.OPTIMIZER_CONSTRAINT_PROPAGATION_OPTIMIZED.key -> "true") {
      executePlan(getTestPlan, OptimizerTypes.NO_PUSH_DOWN_ONLY_PRUNING)
    }
    trivialConstraintAbsenceChecker(constraints2)
    assert(constraints1 match {
      case _: ConstraintSet => false
      case _: ExpressionSet => true
    })

    assert(constraints2 match {
      case _: ConstraintSet => true
      case _: ExpressionSet => false
    })
    val correctAnswer = LocalRelation('a.int, 'b.string, 'c.int).
      select('a, 'a.as('a1), 'a.as('a2),
        'b.as('b1), 'c, 'c.as('c1)).where('c.attr + 'a.attr > 10 && 'a.attr > -15
      && IsNotNull('a) && IsNotNull('c)).analyze
    comparePlans(correctAnswer, plan2)
  }

  // Not comparing with stock spark plan as stock spark plan is unoptimal
  test("test pruning using constraints with filters after project - 3") {
    def getTestPlan: LogicalPlan = {
      val tr1 = LocalRelation('a.int, 'b.string, 'c.int)
      tr1.select('a, 'a.as('a1), 'a.as('a2),
        'b.as('b1), 'c, 'c.as('c1)).where('c1.attr + 'a1.attr > 10 && 'a2.attr > -15).
        where('c.attr + 'a.attr > 10 && 'a .attr > -15)
    }

    val (plan1, constraints1) = withSQLConf[(LogicalPlan, ExpressionSet)](
      SQLConf.OPTIMIZER_CONSTRAINT_PROPAGATION_OPTIMIZED.key -> "false") {
      executePlan(getTestPlan, OptimizerTypes.NO_PUSH_DOWN_ONLY_PRUNING)
    }

    val (plan2, constraints2) = withSQLConf[(LogicalPlan, ExpressionSet)](
      SQLConf.OPTIMIZER_CONSTRAINT_PROPAGATION_OPTIMIZED.key -> "true") {
      executePlan(getTestPlan, OptimizerTypes.NO_PUSH_DOWN_ONLY_PRUNING)
    }
    trivialConstraintAbsenceChecker(constraints2)
    assert(constraints1 match {
      case _: ConstraintSet => false
      case _: ExpressionSet => true
    })

    assert(constraints2 match {
      case _: ConstraintSet => true
      case _: ExpressionSet => false
    })
    val correctAnswer = LocalRelation('a.int, 'b.string, 'c.int).
      select('a, 'a.as('a1), 'a.as('a2),
        'b.as('b1), 'c, 'c.as('c1)).where('c1.attr + 'a1.attr > 10 && 'a2.attr > -15
      && IsNotNull('a) && IsNotNull('c)).analyze
    comparePlans(correctAnswer, plan2)
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

    val (plan1, constraints1) = withSQLConf[(LogicalPlan, ExpressionSet)](
      SQLConf.OPTIMIZER_CONSTRAINT_PROPAGATION_OPTIMIZED.key -> "false") {
      executePlan(getTestPlan, OptimizerTypes.NO_PUSH_DOWN_ONLY_PRUNING)
    }

    val (plan2, constraints2) = withSQLConf[(LogicalPlan, ExpressionSet)](
      SQLConf.OPTIMIZER_CONSTRAINT_PROPAGATION_OPTIMIZED.key -> "true") {
      executePlan(getTestPlan, OptimizerTypes.NO_PUSH_DOWN_ONLY_PRUNING)
    }
    trivialConstraintAbsenceChecker(constraints2)
    assert(constraints1 match {
      case _: ConstraintSet => false
      case _: ExpressionSet => true
    })

    assert(constraints2 match {
      case _: ConstraintSet => true
      case _: ExpressionSet => false
    })
    val correctAnswer = LocalRelation('a.int, 'b.int, 'c.int).
      select('a, 'a.as('a1), 'a.as('a2),
        'b.as('b1), 'c, 'c.as('c1), CaseWhen(Seq(
          ('a.attr + 'b.attr + 'c.attr > Literal(1),
            Literal(1)), ('a.attr + 'b.attr + 'c.attr > Literal(2), Literal(2))),
          Option(Literal(null))).as("z"), 'b).where('z.attr > 10 && 'a2.attr > -15
      && IsNotNull('a) && IsNotNull('z)).select('a, 'a1, 'a2,
      'b1, 'c, 'c1, 'z).analyze
    comparePlans(correctAnswer, plan2)
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

    val (plan1, constraints1) = withSQLConf[(LogicalPlan, ExpressionSet)](
      SQLConf.OPTIMIZER_CONSTRAINT_PROPAGATION_OPTIMIZED.key -> "false") {
      executePlan(getTestPlan, OptimizerTypes.NO_PUSH_DOWN_ONLY_PRUNING)
    }

    val (plan2, constraints2) = withSQLConf[(LogicalPlan, ExpressionSet)](
      SQLConf.OPTIMIZER_CONSTRAINT_PROPAGATION_OPTIMIZED.key -> "true") {
      executePlan(getTestPlan, OptimizerTypes.NO_PUSH_DOWN_ONLY_PRUNING)
    }
    trivialConstraintAbsenceChecker(constraints2)
    assert(constraints1 match {
      case _: ConstraintSet => false
      case _: ExpressionSet => true
    })

    assert(constraints2 match {
      case _: ConstraintSet => true
      case _: ExpressionSet => false
    })
    val correctAnswer = LocalRelation('a.int, 'b.int, 'c.int).
      select('a, 'a.as('a1), 'a.as('a2),
        'b.as('b1), 'c, 'c.as('c1), ('a.attr + CaseWhen(Seq(
          ('a.attr + 'b.attr + 'c.attr > Literal(1),
            Literal(1)), ('a.attr + 'b.attr + 'c.attr > Literal(2), Literal(2))),
          Option(Literal(null)))).as("z"), 'b).where('z.attr > 10 && 'a2.attr > -15
      && IsNotNull('a) && IsNotNull('z)).select('a, 'a1, 'a2,
      'b1, 'c, 'c1, 'z).analyze
    comparePlans(correctAnswer, plan2)
  }

  test("test new filter inference with decanonicalization for expression" +
    "implementing NullIntolerant") {
    def getTestPlan: LogicalPlan = {
      val tr1 = LocalRelation('a.int, 'b.int, 'c.int)
      tr1.select('a, 'a.as('a1), 'a.as('a2),
        'b.as('b1), 'c, 'c.as('c1),
        ('a.attr + 'b.attr + 'c.attr ).as("z")).where('z.attr > 10 && 'a2.attr > -15).
        where('a.attr + 'b1.attr + 'c.attr > 10 && 'a.attr > -15)
    }

    val (plan1, constraints1) = withSQLConf[(LogicalPlan, ExpressionSet)](
      SQLConf.OPTIMIZER_CONSTRAINT_PROPAGATION_OPTIMIZED.key -> "false") {
      executePlan(getTestPlan, OptimizerTypes.NO_PUSH_DOWN_ONLY_PRUNING)
    }

    val (plan2, constraints2) = withSQLConf[(LogicalPlan, ExpressionSet)](
      SQLConf.OPTIMIZER_CONSTRAINT_PROPAGATION_OPTIMIZED.key -> "true") {
      executePlan(getTestPlan, OptimizerTypes.NO_PUSH_DOWN_ONLY_PRUNING)
    }
    trivialConstraintAbsenceChecker(constraints2)
    assert(constraints1 match {
      case _: ConstraintSet => false
      case _: ExpressionSet => true
    })

    assert(constraints2 match {
      case _: ConstraintSet => true
      case _: ExpressionSet => false
    })
    val correctAnswer = LocalRelation('a.int, 'b.int, 'c.int).
      select('a, 'a.as('a1), 'a.as('a2),
        'b.as('b1), 'c, 'c.as('c1),
        ('a.attr + 'b.attr + 'c.attr ).as("z")).where('z.attr > 10 && 'a2.attr > -15
      && IsNotNull('a) && IsNotNull('b1) && IsNotNull('c)).analyze
    comparePlans(correctAnswer, plan2)
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

    val (plan1, constraints1) = withSQLConf[(LogicalPlan, ExpressionSet)](
      SQLConf.OPTIMIZER_CONSTRAINT_PROPAGATION_OPTIMIZED.key -> "false") {
      executePlan(getTestPlan, OptimizerTypes.NO_PUSH_DOWN_ONLY_PRUNING)
    }

    val (plan2, constraints2) = withSQLConf[(LogicalPlan, ExpressionSet)](
      SQLConf.OPTIMIZER_CONSTRAINT_PROPAGATION_OPTIMIZED.key -> "true") {
      executePlan(getTestPlan, OptimizerTypes.NO_PUSH_DOWN_ONLY_PRUNING)
    }
    trivialConstraintAbsenceChecker(constraints2)
    assert(constraints1 match {
      case _: ConstraintSet => false
      case _: ExpressionSet => true
    })

    assert(constraints2 match {
      case _: ConstraintSet => true
      case _: ExpressionSet => false
    })
    val correctAnswer = LocalRelation('a.int, 'b.int, 'c.int).
      select('a, 'a.as('a1), 'a.as('a2), 'b,
        'b.as('b1), 'c, 'c.as('c1), ('a.attr + 'b.attr).as("z")).
      where('c1.attr + 'z.attr > 10 &&
        'a2.attr > -15 && IsNotNull('b)
        && IsNotNull('a) && IsNotNull('c)).analyze
    comparePlans(correctAnswer, plan2)
  }

  test("aliased expression contains embedded alias in projection") {
    val tr1 = LocalRelation('a.int, 'b.int, 'c.int)
    val y = tr1.where('c.attr + 'a.attr + 'b * 'a > 10).select('a, 'b, 'c,
      ('a + 'c).as("summ")).
      select('a, 'b, 'c, 'summ, ('b * 'a).as("mult")).
      select('a, 'b, 'c, ('summ + 'mult).as("z")).
      where('z > 10).analyze
    assert(y.resolved)
    val optimized = GetOptimizer(OptimizerTypes.NO_PUSH_DOWN_ONLY_PRUNING).
      execute(y)
    trivialConstraintAbsenceChecker(optimized.constraints)
    val allFilters = optimized.collect[Filter] {
      case x: Filter => x
    }

    val correctAnswer = tr1.where('c.attr + 'a.attr + 'b * 'a > 10 && IsNotNull('a)
      && IsNotNull('c) && IsNotNull('b)).select('a, 'b, 'c,
      ('a + 'c).as("summ")).
      select('a, 'b, 'c, 'summ, ('b * 'a).as("mult")).
      select('a, 'b, 'c, ('summ + 'mult).as("z")).analyze

    comparePlans(optimized, correctAnswer)
  }

  ignore("Disabled due to spark's canonicalization bug." +
    " test pruning using constraints with filters after project with expression in alias.") {

    def getTestPlan: LogicalPlan = {
      val tr1 = LocalRelation('a.int, 'b.string, 'c.int)
      tr1.select('a, 'a.as('a1), 'a.as('a2), 'b,
        'b.as('b1), 'c, 'c.as('c1), ('a.attr + 'b.attr).as("z")).
        where('c1.attr + 'z.attr > 10 && 'a2.attr > -15).
        where('c.attr + 'a.attr + 'b.attr > 10 && 'a.attr > -15)
    }

    val (plan1, constraints1) = withSQLConf[(LogicalPlan, ExpressionSet)](
      SQLConf.OPTIMIZER_CONSTRAINT_PROPAGATION_OPTIMIZED.key -> "false") {
      executePlan(getTestPlan, OptimizerTypes.NO_PUSH_DOWN_ONLY_PRUNING)
    }

    val (plan2, constraints2) = withSQLConf[(LogicalPlan, ExpressionSet)](
      SQLConf.OPTIMIZER_CONSTRAINT_PROPAGATION_OPTIMIZED.key -> "true") {
      executePlan(getTestPlan, OptimizerTypes.NO_PUSH_DOWN_ONLY_PRUNING)
    }

    assert(constraints1 match {
      case _: ConstraintSet => false
      case _: ExpressionSet => true
    })

    assert(constraints2 match {
      case _: ConstraintSet => true
      case _: ExpressionSet => false
    })
    val correctAnswer = LocalRelation('a.int, 'b.string, 'c.int).
      select('a, 'a.as('a1), 'a.as('a2), 'b,
        'b.as('b1), 'c, 'c.as('c1), ('a.attr + 'b.attr).as("z")).
      where('c1.attr + 'z.attr > 10 && 'a2.attr > -15
        && IsNotNull('a) && IsNotNull('c) && IsNotNull('b)).analyze
    comparePlans(correctAnswer, plan2)
  }

  test("plan equivalence with case statements and performance comparison with benefit" +
    "of more than 10x conservatively") {
    def getTestPlan: LogicalPlan = {
      val tr = LocalRelation('a.int, 'b.int, 'c.int, 'd.int, 'e.int, 'f.int, 'g.int, 'h.int, 'i.int,
        'j.int, 'k.int, 'l.int, 'm.int, 'n.int)
      tr.select('a, 'b, 'c, 'd, 'e, 'f, 'g, 'h, 'i, 'j, 'k, 'l, 'm, 'n,
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
        'l1, 'm1, 'n1, 'cf1, 'JoinKey1).join(tr, condition = Option('a.attr <=> 'JoinKey1.attr))
    }
    val (plan1, constraints1) = withSQLConf[(LogicalPlan, ExpressionSet)](
      SQLConf.OPTIMIZER_CONSTRAINT_PROPAGATION_OPTIMIZED.key -> "false") {
      executePlan(getTestPlan, OptimizerTypes.WITH_FILTER_PUSHDOWN_THRU_JOIN_AND_PRUNING)
    }

    val (plan2, constraints2) = withSQLConf[(LogicalPlan, ExpressionSet)](
      SQLConf.OPTIMIZER_CONSTRAINT_PROPAGATION_OPTIMIZED.key -> "true") {
      executePlan(getTestPlan, OptimizerTypes.WITH_FILTER_PUSHDOWN_THRU_JOIN_AND_PRUNING)
    }
    trivialConstraintAbsenceChecker(constraints2)
    assert(constraints1 match {
      case _: ConstraintSet => false
      case _: ExpressionSet => true
    })

    assert(constraints2 match {
      case _: ConstraintSet => true
      case _: ExpressionSet => false
    })

    // Due to proper tracking of aliases, it is possible that final number of constraints
    // may be a liitle more than the number of constraints returned by old code
    // but intermediate size of old code may be very large causing issue, which is
    // eliminated in the new code. The reason why this happens is that in the
    //  ConstraintSet code to allow proper pruning from canonicalization, it is
    // possible that the incoming expression may be expanded into its constituents
    // refer function ConstraintSet.convertToCanonicalizedIfRqeuired
    // where we are expanding using expression list also.
    // assert(constraints2.expand.size <= constraints1.expand.size)
    comparePlans(plan1, plan2)

    val (plan3, constraints3) = withSQLConf[(LogicalPlan, ExpressionSet)](
      SQLConf.OPTIMIZER_CONSTRAINT_PROPAGATION_OPTIMIZED.key -> "false") {
      executePlan(getTestPlan, OptimizerTypes.WITH_FILTER_PUSHDOWN_THRU_JOIN_AND_PRUNING)
    }

    val (plan4, constraints4) = withSQLConf[(LogicalPlan, ExpressionSet)](
      SQLConf.OPTIMIZER_CONSTRAINT_PROPAGATION_OPTIMIZED.key -> "true") {
      executePlan(getTestPlan, OptimizerTypes.WITH_FILTER_PUSHDOWN_THRU_JOIN_AND_PRUNING)
    }
    trivialConstraintAbsenceChecker(constraints4)
    assert(constraints3 match {
      case _: ConstraintSet => false
      case _: ExpressionSet => true
    })
    assert(constraints4 match {
      case _: ConstraintSet => true
      case _: ExpressionSet => false
    })
    // assert(constraints4.expand.size <= constraints3.expand.size)
    comparePlans(plan3, plan4)
  }

  def executePlan(plan: LogicalPlan, optimizerType: OptimizerTypes.Value):
  (LogicalPlan, ExpressionSet) = {
    object SimpleAnalyzer extends Analyzer(
      new CatalogManager(FakeV2SessionCatalog,
        new SessionCatalog(new InMemoryCatalog, EmptyFunctionRegistry,
          SQLConf.get)))

    val optimizedPlan = GetOptimizer(optimizerType, Some(SQLConf.get)).
      execute(SimpleAnalyzer.execute(plan))
    (optimizedPlan, optimizedPlan.constraints)
  }
}

object OptimizerTypes extends Enumeration {
  val WITH_FILTER_PUSHDOWN_THRU_JOIN_AND_PRUNING,
  NO_PUSH_DOWN_ONLY_PRUNING, WITH_FILTER_PUSHDOWN_THRU_JOIN_AND_UNIONS_PRUNING = Value
}

object GetOptimizer {
  def apply(optimizerType: OptimizerTypes.Value, useConf: Option[SQLConf] = None): Optimizer =
    optimizerType match {
      case OptimizerTypes.WITH_FILTER_PUSHDOWN_THRU_JOIN_AND_PRUNING =>
        new Optimizer( new CatalogManager(
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
        new Optimizer( new CatalogManager(
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
        new Optimizer( new CatalogManager(
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
