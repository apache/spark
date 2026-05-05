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

package org.apache.spark.sql.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, TransformExpression}
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, HashPartitioning, KeyedPartitioning, Partitioning, PartitioningCollection, UnknownPartitioning}
import org.apache.spark.sql.connector.catalog.functions.{BucketFunction, YearsFunction}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StringType}

class ProjectedOrderingAndPartitioningSuite
  extends SharedSparkSession with AdaptiveSparkPlanHelper {
  import testImplicits._

  test("SPARK-42049: Improve AliasAwareOutputExpression - ordering - multi-alias") {
    Seq(0, 1, 2, 5).foreach { limit =>
      withSQLConf(SQLConf.EXPRESSION_PROJECTION_CANDIDATE_LIMIT.key -> limit.toString) {
        val df = spark.range(2).orderBy($"id").selectExpr("id as x", "id as y", "id as z")
        val outputOrdering = df.queryExecution.optimizedPlan.outputOrdering
        limit match {
          case 5 =>
            assert(outputOrdering.size == 1)
            assert(outputOrdering.head.sameOrderExpressions.size == 2)
            assert(outputOrdering.head.sameOrderExpressions.map(_.asInstanceOf[Attribute].name)
              .toSet.subsetOf(Set("x", "y", "z")))
          case 2 =>
            assert(outputOrdering.size == 1)
            assert(outputOrdering.head.sameOrderExpressions.size == 1)
            assert(outputOrdering.head.sameOrderExpressions.map(_.asInstanceOf[Attribute].name)
              .toSet.subsetOf(Set("x", "y", "z")))
          case 1 =>
            assert(outputOrdering.size == 1)
            assert(outputOrdering.head.sameOrderExpressions.size == 0)
          case 0 =>
            assert(outputOrdering.size == 0)
        }
      }
    }
  }

  test("SPARK-42049: Improve AliasAwareOutputExpression - partitioning - multi-alias") {
    Seq(0, 1, 2, 5).foreach { limit =>
      withSQLConf(SQLConf.EXPRESSION_PROJECTION_CANDIDATE_LIMIT.key -> limit.toString) {
        val df = spark.range(2).repartition($"id").selectExpr("id as x", "id as y", "id as z")
        val outputPartitioning = stripAQEPlan(df.queryExecution.executedPlan).outputPartitioning
        limit match {
          case 5 =>
            val p = outputPartitioning.asInstanceOf[PartitioningCollection].partitionings
            assert(p.size == 3)
            assert(p.flatMap(_.asInstanceOf[HashPartitioning].expressions
              .map(_.asInstanceOf[Attribute].name)).toSet == Set("x", "y", "z"))
          case 2 =>
            val p = outputPartitioning.asInstanceOf[PartitioningCollection].partitionings
            assert(p.size == 2)
            p.flatMap(_.asInstanceOf[HashPartitioning].expressions
              .map(_.asInstanceOf[Attribute].name)).toSet.subsetOf(Set("x", "y", "z"))
          case 1 =>
            val p = outputPartitioning.asInstanceOf[HashPartitioning]
            assert(p.expressions.size == 1)
            assert(p.expressions.map(_.asInstanceOf[Attribute].name)
              .toSet.subsetOf(Set("x", "y", "z")))
          case 0 =>
            assert(outputPartitioning.isInstanceOf[UnknownPartitioning])
        }
      }
    }
  }

  test("SPARK-42049: Improve AliasAwareOutputExpression - ordering - multi-references") {
    val df = spark.range(2).selectExpr("id as a", "id as b")
      .orderBy($"a" + $"b").selectExpr("a as x", "b as y")
    val outputOrdering = df.queryExecution.optimizedPlan.outputOrdering
    assert(outputOrdering.size == 1)
    assert(outputOrdering.head.sql == "(x + y) ASC NULLS FIRST")
    assert(outputOrdering.head.sameOrderExpressions.size == 0)
  }

  test("SPARK-42049: Improve AliasAwareOutputExpression - partitioning - multi-references") {
    val df = spark.range(2).selectExpr("id as a", "id as b")
      .repartition($"a" + $"b").selectExpr("a as x", "b as y")
    val outputPartitioning = stripAQEPlan(df.queryExecution.executedPlan).outputPartitioning
    // (a + b), (a + y), (x + b) are pruned since their references are not the subset of output
    outputPartitioning match {
      case p: HashPartitioning => assert(p.sql == "hashpartitioning((x + y))")
      case _ => fail(s"Unexpected $outputPartitioning")
    }
  }

  test("SPARK-46609: Avoid exponential explosion in PartitioningPreservingUnaryExecNode") {
    withSQLConf(SQLConf.EXPRESSION_PROJECTION_CANDIDATE_LIMIT.key -> "2") {
      val output = Seq(AttributeReference("a", StringType)(), AttributeReference("b", StringType)())
      val plan = ProjectExec(
        Seq(
          Alias(output(0), "a1")(),
          Alias(output(0), "a2")(),
          Alias(output(1), "b1")(),
          Alias(output(1), "b2")()
        ),
        DummyLeafPlanExec(output)
      )
      assert(plan.outputPartitioning.asInstanceOf[PartitioningCollection].partitionings.length == 2)
    }
  }

  test("SPARK-42049: Improve AliasAwareOutputExpression - multi-references to complex " +
    "expressions") {
    val df2 = spark.range(2).repartition($"id" + $"id").selectExpr("id + id as a", "id + id as b")
    val outputPartitioning = stripAQEPlan(df2.queryExecution.executedPlan).outputPartitioning
    val partitionings = outputPartitioning.asInstanceOf[PartitioningCollection].partitionings
    assert(partitionings.map {
      case p: HashPartitioning => p.sql
      case _ => fail(s"Unexpected $outputPartitioning")
    } == Seq("hashpartitioning(b)", "hashpartitioning(a)"))

    val df = spark.range(2).orderBy($"id" + $"id").selectExpr("id + id as a", "id + id as b")
    val outputOrdering = df.queryExecution.optimizedPlan.outputOrdering
    assert(outputOrdering.size == 1)
    assert(outputOrdering.head.sql == "b ASC NULLS FIRST")
    assert(outputOrdering.head.sameOrderExpressions.map(_.sql) == Seq("a"))
  }

  test("SPARK-42049: Improve AliasAwareOutputExpression - multi-references to children of " +
    "complex expressions") {
    val df2 = spark.range(2).repartition($"id" + $"id").selectExpr("id as a", "id as b")
    val outputPartitioning = stripAQEPlan(df2.queryExecution.executedPlan).outputPartitioning
    val partitionings = outputPartitioning.asInstanceOf[PartitioningCollection].partitionings
    // (a + b) is the same as (b + a) so expect only one
    assert(partitionings.map {
      case p: HashPartitioning => p.sql
      case _ => fail(s"Unexpected $outputPartitioning")
    } == Seq("hashpartitioning((b + b))", "hashpartitioning((a + b))", "hashpartitioning((a + a))"))

    val df = spark.range(2).orderBy($"id" + $"id").selectExpr("id as a", "id as b")
    val outputOrdering = df.queryExecution.optimizedPlan.outputOrdering
    assert(outputOrdering.size == 1)
    assert(outputOrdering.head.sql == "(b + b) ASC NULLS FIRST")
    // (a + b) is the same as (b + a) so expect only one
    assert(outputOrdering.head.sameOrderExpressions.map(_.sql) == Seq("(a + b)", "(a + a)"))
  }

  test("SPARK-42049: Improve AliasAwareOutputExpression - multi-references to complex " +
    "expressions and to their children") {
    val df2 = spark.range(2).repartition($"id" + $"id")
      .selectExpr("id + id as aa", "id + id as bb", "id as a", "id as b")
    val outputPartitioning = stripAQEPlan(df2.queryExecution.executedPlan).outputPartitioning
    val partitionings = outputPartitioning.asInstanceOf[PartitioningCollection].partitionings
    // (a + b) is the same as (b + a) so expect only one
    assert(partitionings.map {
      case p: HashPartitioning => p.sql
      case _ => fail(s"Unexpected $outputPartitioning")
    } == Seq("hashpartitioning(bb)", "hashpartitioning(aa)", "hashpartitioning((b + b))",
      "hashpartitioning((a + b))", "hashpartitioning((a + a))"))

    val df = spark.range(2).orderBy($"id" + $"id")
      .selectExpr("id + id as aa", "id + id as bb", "id as a", "id as b")
    val outputOrdering = df.queryExecution.optimizedPlan.outputOrdering
    assert(outputOrdering.size == 1)
    assert(outputOrdering.head.sql == "bb ASC NULLS FIRST")
    // (a + b) is the same as (b + a) so expect only one
    assert(outputOrdering.head.sameOrderExpressions.map(_.sql) ==
      Seq("aa", "(b + b)", "(a + b)", "(a + a)"))
  }

  test("SPARK-42049: Improve AliasAwareOutputExpression - ordering partly projected") {
    val df = spark.range(2).orderBy($"id" + 1, $"id" + 2)

    val df1 = df.selectExpr("id + 1 AS a", "id + 2 AS b")
    val outputOrdering1 = df1.queryExecution.optimizedPlan.outputOrdering
    assert(outputOrdering1.size == 2)
    assert(outputOrdering1.map(_.sql) == Seq("a ASC NULLS FIRST", "b ASC NULLS FIRST"))

    val df2 = df.selectExpr("id + 1 AS a")
    val outputOrdering2 = df2.queryExecution.optimizedPlan.outputOrdering
    assert(outputOrdering2.size == 1)
    assert(outputOrdering2.head.sql == "a ASC NULLS FIRST")

    val df3 = df.selectExpr("id + 2 AS b")
    val outputOrdering3 = df3.queryExecution.optimizedPlan.outputOrdering
    assert(outputOrdering3.size == 0)
  }

  test("SPARK-42049: Improve AliasAwareOutputExpression - no alias but still prune expressions") {
    val df = spark.range(2).select($"id" + 1 as "a", $"id" + 2 as "b")

    val df1 = df.repartition($"a", $"b").selectExpr("a")
    val outputPartitioning = stripAQEPlan(df1.queryExecution.executedPlan).outputPartitioning
    assert(outputPartitioning.isInstanceOf[UnknownPartitioning])

    val df2 = df.orderBy("a", "b").select("a")
    val outputOrdering = df2.queryExecution.optimizedPlan.outputOrdering
    assert(outputOrdering.size == 1)
    assert(outputOrdering.head.child.asInstanceOf[Attribute].name == "a")
    assert(outputOrdering.head.sameOrderExpressions.size == 0)
  }

  test("SPARK-46367: KeyedPartitioning expressions are projected through " +
      "PartitioningPreservingUnaryExecNode") {
    val a = AttributeReference("a", IntegerType)()
    val partitionKeys = Seq(InternalRow(1), InternalRow(2), InternalRow(3))
    val child = DummyLeafExecWithPartitioning(
      output = Seq(a),
      partitioning = KeyedPartitioning(Seq(a), partitionKeys))
    val b = Alias(a, "b")()
    val project = ProjectExec(Seq(b), child)

    project.outputPartitioning match {
      case kp: KeyedPartitioning =>
        assert(kp.expressions === Seq(b.toAttribute),
          "expressions must reference the aliased attribute, not the original")
        assert(kp.partitionKeys ===
          child.partitioning.asInstanceOf[KeyedPartitioning].partitionKeys,
          "partition keys must be preserved after projection")
      case other =>
        fail(s"Expected KeyedPartitioning, got $other")
    }
  }

  test("SPARK-46367: narrowing projection on KeyedPartitioning produces projected partition keys") {
    // KP([x, y], [(1,1),(1,2),(2,1),(2,2)]) through Project(x) should produce
    // KP([x], [(1),(1),(2),(2)]) -- granularity narrows from 2 to 1.
    val x = AttributeReference("x", IntegerType)()
    val y = AttributeReference("y", IntegerType)()
    val keys2d = Seq(InternalRow(1, 1), InternalRow(1, 2), InternalRow(2, 1), InternalRow(2, 2))
    val child = DummyLeafExecWithPartitioning(
      output = Seq(x, y),
      partitioning = KeyedPartitioning(Seq(x, y), keys2d))
    val project = ProjectExec(Seq(x), child)

    project.outputPartitioning match {
      case kp: KeyedPartitioning =>
        assert(kp.expressions === Seq(x),
          "narrowed partitioning must keep the projected expression")
        assert(kp.numPartitions === 4,
          "partition count must be preserved")
      case other =>
        fail(s"Expected KeyedPartitioning, got $other")
    }
  }

  test("SPARK-46367: narrowing projection with alias shares partition keys across alternatives") {
    // KP([x, y], ...) through Project(x, x as x_alias) should produce
    // PC(KP([x], keys1d), KP([x_alias], keys1d)) where both KPs reference the same keys1d object.
    val x = AttributeReference("x", IntegerType)()
    val y = AttributeReference("y", IntegerType)()
    val keys2d = Seq(InternalRow(1, 1), InternalRow(1, 2), InternalRow(2, 1), InternalRow(2, 2))
    val child = DummyLeafExecWithPartitioning(
      output = Seq(x, y),
      partitioning = KeyedPartitioning(Seq(x, y), keys2d))
    val xAlias = Alias(x, "x_alias")()
    val project = ProjectExec(Seq(x, xAlias), child)

    project.outputPartitioning match {
      case pc: PartitioningCollection =>
        val kps = pc.partitionings.map(_.asInstanceOf[KeyedPartitioning])
        assert(kps.forall(_.expressions.length == 1),
          "all narrowed KPs must have 1 expression")
        assert(kps.map(_.expressions.head.asInstanceOf[Attribute].name).toSet
          === Set("x", "x_alias"),
          "both the original and aliased attribute must appear")
        // The invariant: all KPs in the collection must share the same partitionKeys object.
        assert(kps.tail.forall(_.partitionKeys eq kps.head.partitionKeys),
          "all KPs must share the same partitionKeys object")
      case other =>
        fail(s"Expected PartitioningCollection, got $other")
    }
  }

  test("SPARK-46367: narrowing projection from 3 to 2 expressions with alias") {
    // KP([x, y, z], keys3d) through Project(x, x as x_alias, y) -- z is dropped.
    // Expected: PC(KP([x, y], keys2d), KP([x_alias, y], keys2d)) where both share keys2d.
    val x = AttributeReference("x", IntegerType)()
    val y = AttributeReference("y", IntegerType)()
    val z = AttributeReference("z", IntegerType)()
    val keys3d = Seq(InternalRow(1, 1, 1), InternalRow(1, 1, 2), InternalRow(1, 2, 1),
      InternalRow(2, 1, 1), InternalRow(2, 2, 2))
    val child = DummyLeafExecWithPartitioning(
      output = Seq(x, y, z),
      partitioning = KeyedPartitioning(Seq(x, y, z), keys3d))
    val xAlias = Alias(x, "x_alias")()
    val project = ProjectExec(Seq(x, xAlias, y), child)

    project.outputPartitioning match {
      case pc: PartitioningCollection =>
        val kps = pc.partitionings.map(_.asInstanceOf[KeyedPartitioning])
        assert(kps.forall(_.expressions.length == 2),
          "narrowed KPs must have 2 expressions (z dropped, x and y kept)")
        assert(kps.map(_.expressions.map(_.asInstanceOf[Attribute].name)).toSet ===
          Set(Seq("x", "y"), Seq("x_alias", "y")))
        assert(kps.tail.forall(_.partitionKeys eq kps.head.partitionKeys),
          "all narrowed KPs must share the same partitionKeys object")
      case other =>
        fail(s"Expected PartitioningCollection, got $other")
    }
  }

  test("SPARK-46367: non-prefix narrowing projection preserves original KP expression order") {
    // KP([x, y, z], keys3d) through Project(z, y) -- x is dropped (non-prefix).
    // The output expression order is [z, y], but the projected KP expressions must follow the
    // original position order [y, z] because the per-position algorithm iterates positions 0..N-1
    // and z is at position 2, y at position 1.
    val x = AttributeReference("x", IntegerType)()
    val y = AttributeReference("y", IntegerType)()
    val z = AttributeReference("z", IntegerType)()
    // Projected to positions [1(y), 2(z)]: (1,1),(1,2),(2,1),(1,1),(2,2) -- (1,1) appears twice.
    val keys3d = Seq(InternalRow(1, 1, 1), InternalRow(1, 1, 2), InternalRow(1, 2, 1),
      InternalRow(2, 1, 1), InternalRow(2, 2, 2))
    val child = DummyLeafExecWithPartitioning(
      output = Seq(x, y, z),
      partitioning = KeyedPartitioning(Seq(x, y, z), keys3d))
    val project = ProjectExec(Seq(z, y), child)

    project.outputPartitioning match {
      case kp: KeyedPartitioning =>
        assert(kp.expressions.map(_.asInstanceOf[Attribute].name) === Seq("y", "z"),
          "expressions must follow original KP position order [y, z], not output order [z, y]")
        assert(kp.isNarrowed, "dropping x must mark the KP as narrowed")
        assert(!kp.isGrouped, "projected keys have duplicate (1,1) entries")
      case other =>
        fail(s"Expected KeyedPartitioning, got $other")
    }
  }

  test("SPARK-46367: non-prefix narrowing projection with alias produces cross-product " +
      "in original KP expression order") {
    // KP([x, y, z], keys3d) through Project(z, z as z_alias, y) -- x is dropped (non-prefix).
    // Projectable positions: y (pos 1) -> [y], z (pos 2) -> [z, z_alias].
    // Cross-product: PC(KP([y, z], keys2d), KP([y, z_alias], keys2d)) -- expressions in
    // original position order [y, z/z_alias], NOT in output expression order [z, z_alias, y].
    val x = AttributeReference("x", IntegerType)()
    val y = AttributeReference("y", IntegerType)()
    val z = AttributeReference("z", IntegerType)()
    val keys3d = Seq(InternalRow(1, 1, 1), InternalRow(1, 1, 2), InternalRow(1, 2, 1),
      InternalRow(2, 1, 1), InternalRow(2, 2, 2))
    val child = DummyLeafExecWithPartitioning(
      output = Seq(x, y, z),
      partitioning = KeyedPartitioning(Seq(x, y, z), keys3d))
    val zAlias = Alias(z, "z_alias")()
    val project = ProjectExec(Seq(z, zAlias, y), child)

    project.outputPartitioning match {
      case pc: PartitioningCollection =>
        val kps = pc.partitionings.map(_.asInstanceOf[KeyedPartitioning])
        assert(kps.forall(_.expressions.length == 2),
          "narrowed KPs must have 2 expressions (x dropped)")
        assert(kps.map(_.expressions.map(_.asInstanceOf[Attribute].name)).toSet ===
          Set(Seq("y", "z"), Seq("y", "z_alias")),
          "expressions must follow original KP position order [y, z/z_alias], not output order")
        assert(kps.tail.forall(_.partitionKeys eq kps.head.partitionKeys),
          "all narrowed KPs must share the same partitionKeys object")
        assert(kps.forall(_.isNarrowed), "all KPs must be marked as narrowed")
        assert(kps.forall(!_.isGrouped), "projected keys have duplicate (1,1) entries")
      case other =>
        fail(s"Expected PartitioningCollection, got $other")
    }
  }

  test("SPARK-46367: PartitioningCollection KPs with mixed projectability produce correct " +
      "per-position cross-product") {
    // PC(KP([x,y], keys2d), KP([x,y_alias], keys2d)) through Project(x, x as x_alias, y_alias):
    // Per-position projection across both KPs:
    //   position 0: ExpressionSet({x, x}) = {x} => projectExpression(x) = [x, x_alias]
    //   position 1: ExpressionSet({y, y_alias})  => projectExpression(y) = [] (y not in output),
    //                                                projectExpression(y_alias) = [y_alias]
    //               => alternatives: [y_alias]
    // Cross-product [x, x_alias] x [y_alias] => KP([x,y_alias], keys2d), KP([x_alias,y_alias],
    // keys2d). Both share the same keys2d object.
    val x = AttributeReference("x", IntegerType)()
    val y = AttributeReference("y", IntegerType)()
    val yAlias = AttributeReference("y_alias", IntegerType)()
    val keys2d = Seq(InternalRow(1, 1), InternalRow(1, 2), InternalRow(2, 1), InternalRow(2, 2))
    val childPartitioning = PartitioningCollection(Seq(
      KeyedPartitioning(Seq(x, y), keys2d),
      KeyedPartitioning(Seq(x, yAlias), keys2d)))
    val child = DummyLeafExecWithPartitioning(
      output = Seq(x, y, yAlias), partitioning = childPartitioning)
    val xAlias = Alias(x, "x_alias")()
    val project = ProjectExec(Seq(x, xAlias, yAlias), child)

    project.outputPartitioning match {
      case pc: PartitioningCollection =>
        val kps = pc.partitionings.map(_.asInstanceOf[KeyedPartitioning])
        assert(kps.forall(_.expressions.length == 2),
          "only full-granularity (2-expr) results must be returned; narrowed ones are subsumed")
        assert(kps.map(_.expressions.map(_.asInstanceOf[Attribute].name)).toSet ===
          Set(Seq("x", "y_alias"), Seq("x_alias", "y_alias")),
          "both x/y_alias and x_alias/y_alias projections must appear")
        // The invariant: all KPs must share the same partitionKeys object.
        assert(kps.tail.forall(_.partitionKeys eq kps.head.partitionKeys),
          "all KPs must share the same partitionKeys object")
      case other =>
        fail(s"Expected PartitioningCollection, got $other")
    }
  }

  test("SPARK-46367: narrowing projection with duplicate keys requires " +
      "allowKeysSubsetOfPartitionKeys to satisfy ClusteredDistribution") {
    val x = AttributeReference("x", IntegerType)()
    val y = AttributeReference("y", IntegerType)()

    // Scenario 1: projected keys have duplicates (x-values: 1, 1, 2) -> isGrouped=false.
    // GroupPartitionsExec would merge the two x=1 partitions, carrying the same skew risk as
    // allowKeysSubsetOfPartitionKeys. EnsureRequirements calls groupedSatisfies() directly.
    val keys2d = Seq(InternalRow(1, 1), InternalRow(1, 2), InternalRow(2, 1))
    val project = ProjectExec(Seq(x),
      DummyLeafExecWithPartitioning(output = Seq(x, y),
        partitioning = KeyedPartitioning(Seq(x, y), keys2d)))

    withSQLConf(SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "false") {
      project.outputPartitioning match {
        case kp: KeyedPartitioning =>
          assert(!kp.isGrouped, "narrowed keys must have duplicates (1 appears twice)")
          assert(kp.isNarrowed, "projection must mark the KP as narrowed")
          assert(!kp.groupedSatisfies(ClusteredDistribution(Seq(x))),
            "narrowed ungrouped KP must not satisfy via groupedSatisfies without config")
        case other => fail(s"Expected KeyedPartitioning, got $other")
      }
    }

    withSQLConf(SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true") {
      project.outputPartitioning match {
        case kp: KeyedPartitioning =>
          assert(kp.groupedSatisfies(ClusteredDistribution(Seq(x))),
            "narrowed ungrouped KP must satisfy via groupedSatisfies when config is enabled")
        case other => fail(s"Expected KeyedPartitioning, got $other")
      }
    }

    // Scenario 2: projected keys are distinct (x-values: 1, 2, 3) -> isGrouped=true.
    // Each projected key maps to exactly one original partition so GroupPartitionsExec does not
    // merge any partitions. No skew risk: must satisfy ClusteredDistribution regardless of config.
    val keys2dDistinct = Seq(InternalRow(1, 1), InternalRow(2, 2), InternalRow(3, 3))
    val projectDistinct = ProjectExec(Seq(x),
      DummyLeafExecWithPartitioning(output = Seq(x, y),
        partitioning = KeyedPartitioning(Seq(x, y), keys2dDistinct)))

    withSQLConf(SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "false") {
      projectDistinct.outputPartitioning match {
        case kp: KeyedPartitioning =>
          assert(kp.isGrouped, "distinct projected keys must be grouped")
          assert(kp.isNarrowed, "projection must mark the KP as narrowed")
          assert(kp.satisfies(ClusteredDistribution(Seq(x))),
            "grouped narrowed KP must satisfy ClusteredDistribution without config (no merging)")
        case other => fail(s"Expected KeyedPartitioning, got $other")
      }
    }
  }

  test("SPARK-46367: isNarrowed is sticky across chained PartitioningPreservingUnaryExecNodes") {
    val x = AttributeReference("x", IntegerType)()
    val y = AttributeReference("y", IntegerType)()

    val keys2d = Seq(InternalRow(1, 1), InternalRow(1, 2), InternalRow(2, 1))
    val innerProject = ProjectExec(Seq(x),
      DummyLeafExecWithPartitioning(output = Seq(x, y),
        partitioning = KeyedPartitioning(Seq(x, y), keys2d)))
    val outerProject = ProjectExec(Seq(x), innerProject)

    withSQLConf(SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "false") {
      outerProject.outputPartitioning match {
        case kp: KeyedPartitioning =>
          assert(!kp.isGrouped, "duplicate keys must survive the second hop")
          assert(kp.isNarrowed,
            "isNarrowed must be sticky: a second hop that keeps all positions must not reset it")
          assert(!kp.groupedSatisfies(ClusteredDistribution(Seq(x))),
            "narrowed ungrouped KP must still not satisfy ClusteredDistribution without config " +
              "after a second PartitioningPreservingUnaryExecNode hop")
        case other => fail(s"Expected KeyedPartitioning, got $other")
      }
    }
  }

  test("SPARK-46367: alias substitution propagates through bucket transform expression") {
    // KP([bucket(32, id)], keys1d) through Project(id as pk) should produce
    // KP([bucket(32, pk)], keys1d): the alias is pushed into the bucket's column argument.
    val id = AttributeReference("id", IntegerType)()
    val bucketExpr = TransformExpression(BucketFunction, Seq(id), Some(32))
    val keys1d = Seq(InternalRow(0), InternalRow(1), InternalRow(2))
    val child = DummyLeafExecWithPartitioning(
      output = Seq(id),
      partitioning = KeyedPartitioning(Seq(bucketExpr), keys1d))
    val pk = Alias(id, "pk")()
    val project = ProjectExec(Seq(pk), child)

    project.outputPartitioning match {
      case kp: KeyedPartitioning =>
        assert(kp.expressions.length === 1)
        kp.expressions.head match {
          case te: TransformExpression =>
            assert(te.isSameFunction(bucketExpr),
              "bucket function and numBuckets must be preserved after alias substitution")
            assert(te.children.head.asInstanceOf[Attribute].name === "pk",
              "bucket's column argument must be rewritten to the aliased attribute")
          case other => fail(s"Expected TransformExpression, got $other")
        }
        assert(kp.partitionKeys eq child.partitioning.asInstanceOf[KeyedPartitioning].partitionKeys,
          "partition keys must be unchanged")
        assert(!kp.isNarrowed, "same number of positions: not narrowed")
      case other => fail(s"Expected KeyedPartitioning, got $other")
    }
  }

  test("SPARK-46367: narrowing projection drops transform when its column is absent") {
    // KP([bucket(32, id), years(ts)], keys2d) through Project(id) -- ts is dropped.
    // bucket(32, id) is projectable (id in output); years(ts) is not (ts absent).
    // Result: KP([bucket(32, id)], keys1d, isNarrowed=true, isGrouped=false).
    val id = AttributeReference("id", IntegerType)()
    val ts = AttributeReference("ts", IntegerType)()
    val bucketExpr = TransformExpression(BucketFunction, Seq(id), Some(32))
    val yearsExpr = TransformExpression(YearsFunction, Seq(ts))
    // Projected to position [0] (bucket): (0),(1),(0) -- bucket value 0 appears twice.
    val keys2d = Seq(InternalRow(0, 2020), InternalRow(1, 2020), InternalRow(0, 2021))
    val child = DummyLeafExecWithPartitioning(
      output = Seq(id, ts),
      partitioning = KeyedPartitioning(Seq(bucketExpr, yearsExpr), keys2d))
    val project = ProjectExec(Seq(id), child)

    project.outputPartitioning match {
      case kp: KeyedPartitioning =>
        assert(kp.expressions.length === 1, "years(ts) must be dropped: ts not in output")
        kp.expressions.head match {
          case te: TransformExpression =>
            assert(te.isSameFunction(bucketExpr), "bucket must be the surviving expression")
            assert(te.children.head.asInstanceOf[Attribute].name === "id")
          case other => fail(s"Expected TransformExpression, got $other")
        }
        assert(kp.isNarrowed, "dropping years(ts) position must mark the KP as narrowed")
        assert(!kp.isGrouped, "projected bucket keys (0,1,0) have duplicates")
      case other => fail(s"Expected KeyedPartitioning, got $other")
    }
  }

  test("SPARK-46367: alias substitution rewrites years transform while preserving bucket") {
    // KP([bucket(32, id), years(ts)], keys2d) through Project(id, ts as ts_alias).
    // bucket(32, id) keeps id (no alias for id); years(ts) is rewritten to years(ts_alias).
    // Result: KP([bucket(32, id), years(ts_alias)], keys2d) -- not narrowed.
    val id = AttributeReference("id", IntegerType)()
    val ts = AttributeReference("ts", IntegerType)()
    val bucketExpr = TransformExpression(BucketFunction, Seq(id), Some(32))
    val yearsExpr = TransformExpression(YearsFunction, Seq(ts))
    val keys2d = Seq(InternalRow(0, 2020), InternalRow(1, 2020), InternalRow(0, 2021))
    val child = DummyLeafExecWithPartitioning(
      output = Seq(id, ts),
      partitioning = KeyedPartitioning(Seq(bucketExpr, yearsExpr), keys2d))
    val tsAlias = Alias(ts, "ts_alias")()
    val project = ProjectExec(Seq(id, tsAlias), child)

    project.outputPartitioning match {
      case kp: KeyedPartitioning =>
        assert(kp.expressions.length === 2, "both positions must survive")
        kp.expressions(0) match {
          case te: TransformExpression =>
            assert(te.isSameFunction(bucketExpr))
            assert(te.children.head.asInstanceOf[Attribute].name === "id",
              "bucket's argument must remain id (no alias for id in this projection)")
          case other => fail(s"Expected TransformExpression at pos 0, got $other")
        }
        kp.expressions(1) match {
          case te: TransformExpression =>
            assert(te.isSameFunction(yearsExpr))
            assert(te.children.head.asInstanceOf[Attribute].name === "ts_alias",
              "years() argument must be rewritten to ts_alias")
          case other => fail(s"Expected TransformExpression at pos 1, got $other")
        }
        assert(kp.partitionKeys eq child.partitioning.asInstanceOf[KeyedPartitioning].partitionKeys,
          "partition keys must be unchanged")
        assert(!kp.isNarrowed, "both positions projected: not narrowed")
      case other => fail(s"Expected KeyedPartitioning, got $other")
    }
  }
}

private case class DummyLeafExecWithPartitioning(
    output: Seq[Attribute],
    partitioning: Partitioning
  ) extends LeafExecNode {
  override protected def doExecute(): RDD[InternalRow] = null
  override def outputPartitioning: Partitioning = partitioning
}

private case class DummyLeafPlanExec(output: Seq[Attribute]) extends LeafExecNode {
  override protected def doExecute(): RDD[InternalRow] = null
  override def outputPartitioning: Partitioning = {
    PartitioningCollection(output.map(attr => HashPartitioning(Seq(attr), 4)))
  }
}
