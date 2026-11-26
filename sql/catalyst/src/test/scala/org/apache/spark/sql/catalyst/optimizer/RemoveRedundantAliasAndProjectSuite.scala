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

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, MetadataBuilder}

class RemoveRedundantAliasAndProjectSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch(
      "RemoveAliasOnlyProject",
      FixedPoint(50),
      PushProjectionThroughUnion,
      RemoveRedundantAliases,
      RemoveNoopOperators) :: Nil
  }

  test("all expressions in project list are aliased child output") {
    val relation = LocalRelation($"a".int, $"b".int)
    val query = relation.select($"a" as "a", $"b" as "b").analyze
    val optimized = Optimize.execute(query)
    comparePlans(optimized, relation)
  }

  test("all expressions in project list are aliased child output but with different order") {
    val relation = LocalRelation($"a".int, $"b".int)
    val query = relation.select($"b" as "b", $"a" as "a").analyze
    val optimized = Optimize.execute(query)
    val expected = relation.select($"b", $"a").analyze
    comparePlans(optimized, expected)
  }

  test("some expressions in project list are aliased child output") {
    val relation = LocalRelation($"a".int, $"b".int)
    val query = relation.select($"a" as "a", $"b").analyze
    val optimized = Optimize.execute(query)
    comparePlans(optimized, relation)
  }

  test("some expressions in project list are aliased child output but with different order") {
    val relation = LocalRelation($"a".int, $"b".int)
    val query = relation.select($"b" as "b", $"a").analyze
    val optimized = Optimize.execute(query)
    val expected = relation.select($"b", $"a").analyze
    comparePlans(optimized, expected)
  }

  test("some expressions in project list are not Alias or Attribute") {
    val relation = LocalRelation($"a".int, $"b".int)
    val query = relation.select($"a" as "a", $"b" + 1).analyze
    val optimized = Optimize.execute(query)
    val expected = relation.select($"a", $"b" + 1).analyze
    comparePlans(optimized, expected)
  }

  test("some expressions in project list are aliased child output but with metadata") {
    val relation = LocalRelation($"a".int, $"b".int)
    val metadata = new MetadataBuilder().putString("x", "y").build()
    val aliasWithMeta = Alias($"a", "a")(explicitMetadata = Some(metadata))
    val query = relation.select(aliasWithMeta, $"b").analyze
    val optimized = Optimize.execute(query)
    comparePlans(optimized, query)
  }

  test("remove redundant project with self-join") {
    val relation = LocalRelation($"a".int)
    val fragment = relation.select($"a" as "a")
    val query = fragment.select($"a" as "a")
      .join(fragment.select($"a" as "a")).analyze
    val optimized = Optimize.execute(query)
    val expected = relation.join(relation).analyze
    comparePlans(optimized, expected)
  }

  test("alias removal should not break after push project through union") {
    val r1 = LocalRelation($"a".int)
    val r2 = LocalRelation($"b".int)
    val query = r1.select($"a" as "a")
      .union(r2.select($"b" as "b")).select($"a").analyze
    val optimized = Optimize.execute(query)
    val expected = r1.select($"a" as "a").union(r2).analyze
    comparePlans(optimized, expected)
  }

  test("remove redundant alias from aggregate") {
    val relation = LocalRelation($"a".int, $"b".int)
    val query = relation.groupBy($"a" as "a")($"a" as "a", sum($"b")).analyze
    val optimized = Optimize.execute(query)
    val expected = relation.groupBy($"a")($"a", sum($"b")).analyze
    comparePlans(optimized, expected)
  }

  test("remove redundant alias from window") {
    val relation = LocalRelation($"a".int, $"b".int)
    val query = relation.window(Seq($"b" as "b"), Seq($"a" as "a"), Seq()).analyze
    val optimized = Optimize.execute(query)
    val expected = relation.window(Seq($"b"), Seq($"a"), Seq()).analyze
    comparePlans(optimized, expected)
  }

  test("do not remove output attributes from a subquery") {
    val relation = LocalRelation($"a".int, $"b".int)
    val query = Subquery(
      relation.select($"a" as "a", $"b" as "b").where($"b" < 10).select($"a").analyze,
      correlated = false)
    val optimized = Optimize.execute(query)
    val expected = Subquery(
      relation.select($"a" as "a", $"b").where($"b" < 10).select($"a").analyze,
      correlated = false)
    comparePlans(optimized, expected)
  }

  test("SPARK-46640: do not remove outer references from a subquery expression") {
    val a = $"a".int
    val a_alias = Alias(a, "a")()
    val a_alias_attr = a_alias.toAttribute
    val b = $"b".int

    // The original input query
    //  Filter exists [a#1 && (a#1 = b#2)]
    //  :  +- LocalRelation <empty>, [b#2]
    //    +- Project [a#0 AS a#1]
    //    +- LocalRelation <empty>, [a#0]
    val query = Filter(
      Exists(
        LocalRelation(b),
        outerAttrs = Seq(a_alias_attr),
        joinCond = Seq(EqualTo(a_alias_attr, b))
      ),
      Project(Seq(a_alias), LocalRelation(a))
    )

    // The alias would not be removed if excluding subquery references is enabled.
    val expectedWhenExcluded = query

    // The alias would have been removed if excluding subquery references is disabled.
    //  Filter exists [a#0 && (a#0 = b#2)]
    //  :  +- LocalRelation <empty>, [b#2]
    //    +- LocalRelation <empty>, [a#0]
    val expectedWhenNotExcluded = Filter(
      Exists(
        LocalRelation(b),
        outerAttrs = Seq(a),
        joinCond = Seq(EqualTo(a, b))
      ),
      LocalRelation(a)
    )

    withSQLConf(SQLConf.EXCLUDE_SUBQUERY_EXP_REFS_FROM_REMOVE_REDUNDANT_ALIASES.key -> "true") {
      val optimized = Optimize.execute(query)
      comparePlans(optimized, expectedWhenExcluded)
    }

    withSQLConf(SQLConf.EXCLUDE_SUBQUERY_EXP_REFS_FROM_REMOVE_REDUNDANT_ALIASES.key -> "false") {
      val optimized = Optimize.execute(query)
      comparePlans(optimized, expectedWhenNotExcluded)
    }
  }

  test("SPARK-46640: exclude outer references accounts for children of plan expression") {
    val a = $"a".int
    val a_alias = Alias(a, "a")()
    val a_alias_attr = a_alias.toAttribute

    // The original input query
    //  Project [CASE WHEN exists#2 [a#1 && (a#1 = a#0)] THEN 1 ELSE 2 END AS result#3]
    //  :  +- LocalRelation <empty>, [a#0]
    //  +- Project [a#0 AS a#1]
    //    +- LocalRelation <empty>, [a#0]
    // The subquery expression (`exists#2`) is wrapped in a CaseWhen and an Alias.
    // Without the fix on excluding outer references, the rewritten plan would have been:
    //  Project [CASE WHEN exists#2 [a#0 && (a#0 = a#0)] THEN 1 ELSE 2 END AS result#3]
    //  :  +- LocalRelation <empty>, [a#0]
    //  +- LocalRelation <empty>, [a#0]
    // This plan would then fail later with the error -- conflicting a#0 in join condition.

    val query = Project(Seq(
      Alias(
        CaseWhen(Seq((
          Exists(
            LocalRelation(a),
            outerAttrs = Seq(a_alias_attr),
            joinCond = Seq(EqualTo(a_alias_attr, a))
          ), Literal(1))),
          Some(Literal(2))),
        "result"
      )()),
      Project(Seq(a_alias), LocalRelation(a))
    )

    // The alias would not be removed if excluding subquery references is enabled.
    val expectedWhenExcluded = query

    // The alias would be removed and we would have conflicting expression ID(s) in the join cond
    val expectedWhenNotEnabled = Project(Seq(
      Alias(
        CaseWhen(Seq((
          Exists(
            LocalRelation(a),
            outerAttrs = Seq(a),
            joinCond = Seq(EqualTo(a, a))
          ), Literal(1))),
          Some(Literal(2))),
        "result"
      )()),
      LocalRelation(a)
    )

    withSQLConf(SQLConf.EXCLUDE_SUBQUERY_EXP_REFS_FROM_REMOVE_REDUNDANT_ALIASES.key -> "true") {
      val optimized = Optimize.execute(query)
      comparePlans(optimized, expectedWhenExcluded)
    }

    withSQLConf(SQLConf.EXCLUDE_SUBQUERY_EXP_REFS_FROM_REMOVE_REDUNDANT_ALIASES.key -> "false") {
      val optimized = Optimize.execute(query)
      comparePlans(optimized, expectedWhenNotEnabled)
    }
  }

  test("SPARK-53308: Don't remove aliases in RemoveRedundantAliases that would cause duplicates") {
    val exprId = NamedExpression.newExprId
    val attribute = AttributeReference("attr", IntegerType)(exprId = exprId)
    val project = Project(
      Seq(
        Alias(attribute, "attr")(),
        attribute
      ),
      LocalRelation(attribute)
    )
    val projectWithoutAlias =
      Project(
        Seq(
          attribute,
          attribute
        ),
        LocalRelation(attribute)
      )
    val union = Union(Seq(project, project))

    withSQLConf(SQLConf.UNION_IS_RESOLVED_WHEN_DUPLICATES_PER_CHILD_RESOLVED.key -> "true") {
      val optimized = Optimize.execute(union)
      comparePlans(union, optimized)
    }

    withSQLConf(SQLConf.UNION_IS_RESOLVED_WHEN_DUPLICATES_PER_CHILD_RESOLVED.key -> "false") {
      val optimized = Optimize.execute(union)
      comparePlans(optimized, Union(Seq(project, projectWithoutAlias)))
    }
  }

  test("SPARK-62383: RemoveRedundantAliases preserves Union branch uniqueness across " +
    "non-project barriers") {
    val a = AttributeReference("a", IntegerType)()
    val b = AttributeReference("b", IntegerType)()
    val aDup = Alias(a, "a")()
    val bRedundant = Alias(b, "b")()
    val aDupAttr = aDup.toAttribute
    val bRedundantAttr = bRedundant.toAttribute

    val first = Project(
      Seq(Alias(a, "a")(), Alias(a, "a")(), Alias(b, "b")()),
      LocalRelation(a, b))
    // Non-first branch has a Repartition between the top Project and the deduplicating Alias,
    // plus an unrelated redundant alias on `b` that can still be safely removed.
    val second = Project(
      Seq(a, aDupAttr, bRedundantAttr),
      Repartition(2, true, Project(Seq(a, aDup, bRedundant), LocalRelation(a, b))))
    val expectedSecond =
      Repartition(2, true, Project(Seq(a, aDup, b), LocalRelation(a, b)))

    val union = Union(Seq(first, second))
    withSQLConf(SQLConf.UNION_IS_RESOLVED_WHEN_DUPLICATES_PER_CHILD_RESOLVED.key -> "true") {
      val optimized = Optimize.execute(union)
      assert(optimized.resolved)
      comparePlans(optimized, Union(Seq(first, expectedSecond)))
    }

    withSQLConf(SQLConf.UNION_IS_RESOLVED_WHEN_DUPLICATES_PER_CHILD_RESOLVED.key -> "false") {
      val optimized = Optimize.execute(union)
      val strippedSecond = Repartition(2, true, Project(Seq(a, a, b), LocalRelation(a, b)))
      comparePlans(optimized, Union(Seq(first, strippedSecond)))
    }
  }

  test("SPARK-62383: RemoveRedundantAliases preserves double and multi-way aliases in " +
    "Union child") {
    val a = AttributeReference("a", IntegerType)()
    val a1 = Alias(a, "a")()
    val a2 = Alias(a, "a")()
    val a3 = Alias(a, "a")()
    val first = Project(Seq(Alias(a, "a")(), Alias(a, "a")()), LocalRelation(a))
    val second = Project(Seq(a1, a2), LocalRelation(a))
    val union = Union(Seq(first, second))

    // Also cover the reversed order `[Alias(a, "a"), a]` where the unchanged bare attribute
    // appears after the alias, ensuring `unchanged` still protects the alias.
    val reversedSecond = Project(Seq(a1, a), LocalRelation(a))
    val reversedUnion = Union(Seq(first, reversedSecond))

    // 3-way colliding duplicate group: `k - 1` exclusion drops `a1` and keeps `a2` and `a3`.
    val tripleFirst = Project(
      Seq(Alias(a, "a")(), Alias(a, "a")(), Alias(a, "a")()), LocalRelation(a))
    val tripleUnion = Union(Seq(tripleFirst, Project(Seq(a1, a2, a3), LocalRelation(a))))
    val tripleWithMiddleBare = Union(
      Seq(tripleFirst, Project(Seq(a1, a, a2), LocalRelation(a))))

    withSQLConf(SQLConf.UNION_IS_RESOLVED_WHEN_DUPLICATES_PER_CHILD_RESOLVED.key -> "true") {
      val optimized = Optimize.execute(union)
      assert(optimized.resolved)
      comparePlans(optimized, Union(Seq(first, Project(Seq(a, a2), LocalRelation(a)))))

      val optReversed = Optimize.execute(reversedUnion)
      assert(optReversed.resolved)
      comparePlans(optReversed, reversedUnion)

      val optTriple = Optimize.execute(tripleUnion)
      assert(optTriple.resolved)
      comparePlans(optTriple, Union(Seq(tripleFirst, Project(Seq(a, a2, a3), LocalRelation(a)))))

      val optMiddleBare = Optimize.execute(tripleWithMiddleBare)
      assert(optMiddleBare.resolved)
      comparePlans(optMiddleBare, tripleWithMiddleBare)
    }

    withSQLConf(SQLConf.UNION_IS_RESOLVED_WHEN_DUPLICATES_PER_CHILD_RESOLVED.key -> "false") {
      val optimized = Optimize.execute(union)
      comparePlans(optimized, Union(Seq(first, Project(Seq(a, a), LocalRelation(a)))))
    }
  }

  test("SPARK-62383: RemoveRedundantAliases preserves ExprId uniqueness when Union branches " +
    "share ExprIds") {
    val x = AttributeReference("x", IntegerType)()
    val id = AttributeReference("id", IntegerType)()
    val idDup = Alias(id, "id")()
    val idDupAttr = idDup.toAttribute
    val xNull = Alias(Literal(null, IntegerType), "x")(x.exprId)
    val idNull = Alias(Literal(null, IntegerType), "id")(id.exprId)
    val idDupNull = Alias(Literal(null, IntegerType), "id")(idDup.exprId)

    val first = Project(Seq(x, idNull, idDupNull), LocalRelation(x))
    val second = Project(
      Seq(xNull, id, idDupAttr),
      Repartition(2, true, Project(Seq(id, idDup), LocalRelation(id))))
    val union = Union(Seq(first, second))

    withSQLConf(SQLConf.UNION_IS_RESOLVED_WHEN_DUPLICATES_PER_CHILD_RESOLVED.key -> "true") {
      val optimized = Optimize.execute(union)
      assert(optimized.resolved)
      comparePlans(optimized, union)
    }

    withSQLConf(SQLConf.UNION_IS_RESOLVED_WHEN_DUPLICATES_PER_CHILD_RESOLVED.key -> "false") {
      val optimized = Optimize.execute(union)
      val strippedSecond = Project(
        Seq(xNull, id, id),
        Repartition(2, true, Project(Seq(id, id), LocalRelation(id))))
      comparePlans(optimized, Union(Seq(first, strippedSecond)))
    }
  }

  test("SPARK-62383: RemoveRedundantAliases converges when Join sits inside a non-first " +
    "Union child") {
    val x = AttributeReference("x", IntegerType)()
    val leftAlias = Alias(x, "x")()
    val topAlias = Alias(leftAlias.toAttribute, "x")()
    val rightAlias1 = Alias(x, "x")()
    val rightAlias2 = Alias(x, "x")()

    val r = LocalRelation(x)
    val left = Project(Seq(leftAlias), r)
    val right = Project(Seq(rightAlias1, rightAlias2), r)
    val join = Join(left, right, org.apache.spark.sql.catalyst.plans.Inner, None, JoinHint.NONE)
    val second = Project(
      Seq(leftAlias.toAttribute, topAlias, rightAlias1.toAttribute, rightAlias2.toAttribute),
      join)
    val first = Project(
      Seq(Alias(x, "x")(), Alias(x, "x")(), Alias(x, "x")(), Alias(x, "x")()),
      r)
    val union = Union(Seq(first, second))
    val expectedSecond = Project(
      Seq(x, Alias(x, "x")(exprId = topAlias.exprId),
        rightAlias1.toAttribute, rightAlias2.toAttribute),
      Join(r, right, org.apache.spark.sql.catalyst.plans.Inner, None, JoinHint.NONE))
    val barrierUnion = Union(Seq(first, Repartition(2, true, second)))

    // Put `topAlias` first so Pass 1 preserves `leftAlias` (removing `x` from
    // `newLeft.outputSet`), causing Pass 2 to strip `right`'s aliases and Pass 3 to preserve
    // `rightAlias2`.
    val cascadeSecond = Project(
      Seq(topAlias, leftAlias.toAttribute, rightAlias1.toAttribute, rightAlias2.toAttribute),
      join)
    val cascadeUnion = Union(Seq(first, Repartition(2, true, cascadeSecond)))
    val expectedCascadeSecond = Repartition(2, true, Project(
      Seq(topAlias, leftAlias.toAttribute, x, rightAlias2.toAttribute),
      Join(left, Project(Seq(x, rightAlias2), r),
        org.apache.spark.sql.catalyst.plans.Inner, None, JoinHint.NONE)))

    withSQLConf(SQLConf.UNION_IS_RESOLVED_WHEN_DUPLICATES_PER_CHILD_RESOLVED.key -> "true") {
      val optimized = Optimize.execute(union)
      assert(optimized.resolved)
      comparePlans(optimized, Union(Seq(first, expectedSecond)))

      val optBarrier = Optimize.execute(barrierUnion)
      assert(optBarrier.resolved)
      comparePlans(optBarrier, Union(Seq(first, Repartition(2, true, expectedSecond))))

      val optCascade = Optimize.execute(cascadeUnion)
      assert(optCascade.resolved)
      comparePlans(optCascade, Union(Seq(first, expectedCascadeSecond)))
    }
  }

  test("SPARK-62383: RemoveRedundantAliases keeps a deep Join cascade inside a Union child " +
    "resolved") {
    val x = AttributeReference("x", IntegerType)()
    val b = AttributeReference("b", IntegerType)()
    val leftAlias = Alias(x, "x")()
    val topAlias = Alias(leftAlias.toAttribute, "x")()
    val bAlias = Alias(b, "b")()
    val legs = Seq.fill(12)(Alias(x, "x")())
    val r = LocalRelation(x, b)

    // `b` reaches the branch output through the left leg, so `bAlias` is well-formed.
    val join = legs.grouped(2).foldLeft[LogicalPlan](Project(Seq(leftAlias, b), r)) {
      case (left, Seq(first, second)) =>
        Join(left, Project(Seq(first, second), r),
          org.apache.spark.sql.catalyst.plans.Inner, None, JoinHint.NONE)
    }
    val second = Project(
      bAlias +: topAlias +: leftAlias.toAttribute +: legs.map(_.toAttribute), join)
    val first = Project(
      Alias(x, "x")() +: Alias(x, "x")() +: Alias(b, "b")() +: legs.map(_ => Alias(x, "x")()), r)

    withSQLConf(SQLConf.UNION_IS_RESOLVED_WHEN_DUPLICATES_PER_CHILD_RESOLVED.key -> "true") {
      val optimized = Optimize.execute(Union(Seq(first, second)))
      assert(optimized.resolved)
      val branchOutput = optimized.children(1).output
      assert(branchOutput.map(_.exprId).distinct.size == branchOutput.size)
      // `b AS b` collides with nothing, so the branch must have been traversed and the alias
      // stripped; skipping the branch as a whole would keep it.
      assert(!branchOutput.exists(_.exprId == bAlias.exprId))
    }
  }

  test("SPARK-62383: RemoveRedundantAliases preserves Union uniqueness for Aggregate, Window, " +
    "and chains") {
    val a = AttributeReference("a", IntegerType)()
    val a1 = Alias(a, "a")()
    val a2 = Alias(a1.toAttribute, "a")()
    val b = AttributeReference("b", IntegerType)()

    // Unrelated lower alias on bare `a` is still stripped while top deduplicating alias stays.
    val lowerDup = Alias(a, "a")()
    val chainFirst = Project(Seq(Alias(a, "a")(), Alias(a, "a")()), LocalRelation(a))
    val chainSecond = Project(
      Seq(a, a1.toAttribute),
      Repartition(2, true,
        Project(Seq(a, a1.toAttribute, lowerDup),
          Project(Seq(a1, a), LocalRelation(a)))))
    val expectedChainSecond = Project(
      Seq(a, a1.toAttribute),
      Repartition(2, true,
        Project(Seq(a, a1.toAttribute, a),
          Project(Seq(a1, a), LocalRelation(a)))))

    // Chained alias across barriers: inner `a1` can be stripped while outer `a2` stays.
    val twoLevelSecond = Project(
      Seq(a, a2.toAttribute),
      Repartition(2, true,
        Project(Seq(a, a2),
          Repartition(2, true, Project(Seq(a, a1), LocalRelation(a))))))
    val expectedTwoLevelSecond = Repartition(2, true,
      Project(Seq(a, Alias(a, "a")(exprId = a2.exprId)),
        Repartition(2, true, Project(Seq(a, a), LocalRelation(a)))))

    // Aggregate and Window behind a Repartition barrier in non-first Union child.
    val aggSecond = Repartition(2, true,
      Aggregate(Seq(a), Seq(a, Alias(a, "a")()), LocalRelation(a)))
    val winSecond = Repartition(2, true,
      Window(Seq(Alias(a, "a")()), Seq(a), Nil, LocalRelation(a, b)))

    withSQLConf(SQLConf.UNION_IS_RESOLVED_WHEN_DUPLICATES_PER_CHILD_RESOLVED.key -> "true") {
      val optChain = Optimize.execute(Union(Seq(chainFirst, chainSecond)))
      assert(optChain.resolved)
      comparePlans(optChain, Union(Seq(chainFirst, expectedChainSecond)))

      val optTwoLevel = Optimize.execute(Union(Seq(chainFirst, twoLevelSecond)))
      assert(optTwoLevel.resolved)
      comparePlans(optTwoLevel, Union(Seq(chainFirst, expectedTwoLevelSecond)))

      val optAgg = Optimize.execute(Union(Seq(chainFirst, aggSecond)))
      assert(optAgg.resolved)
      comparePlans(optAgg, Union(Seq(chainFirst, aggSecond)))

      val winFirst = Project(
        Seq(Alias(a, "a")(), Alias(b, "b")(), Alias(a, "a")()), LocalRelation(a, b))
      val optWin = Optimize.execute(Union(Seq(winFirst, winSecond)))
      assert(optWin.resolved)
      comparePlans(optWin, Union(Seq(winFirst, winSecond)))

      // Nested Unions with Repartition barriers, inner removable alias, and duplicate aliases.
      val innerFirst = Project(
        Seq(Alias(a, "a")(), Alias(a, "a")(), Alias(b, "b")()), LocalRelation(a, b))
      val innerDup = Alias(a, "a")()
      val innerSecond = Repartition(2, true,
        Project(Seq(a, innerDup, Alias(b, "b")()), LocalRelation(a, b)))
      val innerUnion = Union(Seq(innerFirst, innerSecond))
      val innerOut = innerUnion.output.head
      val outerFirst = Project(
        Seq(Alias(a, "a")(), Alias(a, "a")(), Alias(b, "b")()), LocalRelation(a, b))
      val outerDup = Alias(innerOut, "a")()
      val outerSecond = Project(
        Seq(innerOut, outerDup, innerUnion.output(2)),
        Repartition(2, true, innerUnion))
      val expectedInnerSecond = Repartition(2, true,
        Project(Seq(a, innerDup, b), LocalRelation(a, b)))
      val expectedOuterSecond = Project(
        Seq(innerOut, outerDup, innerUnion.output(2)),
        Repartition(2, true, Union(Seq(innerFirst, expectedInnerSecond))))
      val optNested = Optimize.execute(Union(Seq(outerFirst, outerSecond)))
      assert(optNested.resolved)
      comparePlans(optNested, Union(Seq(outerFirst, expectedOuterSecond)))
    }
  }
}
