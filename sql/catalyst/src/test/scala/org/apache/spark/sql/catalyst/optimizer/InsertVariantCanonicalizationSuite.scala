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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{Alias, ArrayTransform, Ascending, Attribute,
  AttributeReference, CreateNamedStruct, Expression, GetStructField, If, IsNull, LambdaFunction,
  Literal, SortOrder, TransformValues}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.variant.{CanonicalizeVariant, VariantGet}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Distinct, LocalRelation,
  LogicalPlan, Project, RepartitionByExpression, Union}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, VariantType}

class InsertVariantCanonicalizationSuite extends PlanTest {
  private val input = LocalRelation(
    AttributeReference("v", VariantType)(),
    Symbol("i").int)
  private val variantAttribute = input.output.head
  private val intAttribute = input.output.last

  private val groupingAliasName = "_groupingvariantcanonicalization"
  private val distinctAliasName = "_distinctvariantcanonicalization"
  private val distinctInputAliasName = "_distinctaggregateexpression"

  private def aliasesNamed(plan: LogicalPlan, name: String): Seq[Alias] = {
    plan.flatMap { node =>
      node.expressions.flatMap(_.collect {
        case alias @ Alias(_, aliasName) if aliasName == name => alias
      })
    }
  }

  private def projects(plan: LogicalPlan): Seq[Project] = plan.collect { case p: Project => p }

  private def distinctArguments(plan: Aggregate): Seq[Expression] = {
    plan.aggregateExpressions.flatMap(_.collect {
      case expression: AggregateExpression if expression.isDistinct =>
        expression.aggregateFunction.children
    }).flatten
  }

  /** Every node must only reference attributes produced by its children. */
  private def assertReferencesResolved(plan: LogicalPlan): Unit = {
    plan.foreach { node =>
      assert(node.missingInput.isEmpty, s"Missing input in node:\n$node\nin plan:\n$plan")
    }
  }

  test("canonicalize a Variant grouping key in a projection below the aggregate") {
    val plan = Aggregate(
      Seq(variantAttribute),
      Seq(variantAttribute, count(intAttribute).as("count")),
      input)
    val rewritten = InsertVariantCanonicalizationInAggregate(plan)

    assert(rewritten.output.map(_.name) == plan.output.map(_.name))
    assertReferencesResolved(rewritten)
    val groupingAliases = aliasesNamed(rewritten, groupingAliasName)
    assert(groupingAliases.size == 1)
    assert(groupingAliases.head.child == CanonicalizeVariant(variantAttribute))
    rewritten match {
      case Aggregate(
          Seq(groupingKey: Attribute),
          Seq(keyOutput: Attribute, _),
          Project(projectList, `input`), _) =>
        assert(groupingKey.semanticEquals(groupingAliases.head.toAttribute))
        assert(keyOutput.exprId == groupingKey.exprId)
        assert(keyOutput.name == "v")
        assert(projectList == input.output :+ groupingAliases.head)
      case other =>
        fail(s"Unexpected plan:\n$other")
    }
  }

  test("rewire references above the aggregate to the canonicalized output") {
    val aggregate = Aggregate(Seq(variantAttribute), Seq(variantAttribute), input)
    val plan = Project(Seq(variantAttribute.as("out")), aggregate)
    val rewritten = InsertVariantCanonicalizationInAggregate(plan)

    assertReferencesResolved(rewritten)
    rewritten match {
      case Project(Seq(Alias(reference: Attribute, "out")), Aggregate(_, Seq(output), _, _)) =>
        assert(reference.exprId == output.exprId)
        assert(reference.exprId != variantAttribute.exprId)
      case other =>
        fail(s"Unexpected plan:\n$other")
    }
  }

  test("canonicalize an extraction grouping expression pulled out of the aggregate") {
    val extraction = VariantGet(
      variantAttribute, Literal("$.a"), VariantType, failOnError = true, Some("UTC"))
    val plan = Aggregate(
      Seq(extraction),
      Seq(extraction.as("a"), count(intAttribute).as("count")),
      input)
    val pulledOut = PullOutGroupingExpressions(plan)
    val rewritten = InsertVariantCanonicalizationInAggregate(pulledOut)

    assert(rewritten.output.map(_.name) == plan.output.map(_.name))
    assertReferencesResolved(rewritten)
    val pulledOutAliases = aliasesNamed(rewritten, "_groupingexpression")
    val groupingAliases = aliasesNamed(rewritten, groupingAliasName)
    assert(pulledOutAliases.size == 1)
    assert(groupingAliases.size == 1)
    assert(groupingAliases.head.child == CanonicalizeVariant(pulledOutAliases.head.toAttribute))

    // The operator optimization batch later drops the pass-through of the pulled-out attribute
    // and merges the two projections into one.
    val collapsed = CollapseProject(ColumnPruning(rewritten))
    assert(projects(collapsed).size == 1)
    assert(aliasesNamed(collapsed, groupingAliasName).head.child == CanonicalizeVariant(extraction))
  }

  test("reuse the grouping canonicalization for an identical distinct argument") {
    val plan = Aggregate(
      Seq(variantAttribute),
      Seq(variantAttribute, countDistinct(variantAttribute).as("count")),
      input)
    val rewritten = InsertVariantCanonicalizationInAggregate(plan)

    assertReferencesResolved(rewritten)
    val groupingAliases = aliasesNamed(rewritten, groupingAliasName)
    assert(groupingAliases.size == 1)
    assert(aliasesNamed(rewritten, distinctAliasName).isEmpty)
    assert(projects(rewritten).size == 1)
    rewritten match {
      case aggregate @ Aggregate(Seq(groupingKey), _, _, _) =>
        assert(groupingKey.semanticEquals(groupingAliases.head.toAttribute))
        assert(distinctArguments(aggregate) == Seq(groupingKey))
      case other =>
        fail(s"Unexpected plan:\n$other")
    }
  }

  test("substitute the canonicalized key inside other aggregate output expressions") {
    val extraction = VariantGet(
      variantAttribute, Literal("$.a"), VariantType, failOnError = true, Some("UTC"))
    val extractionAlias = extraction.as("a")
    val countAlias = count(variantAttribute).as("count")
    val plan = Aggregate(Seq(variantAttribute), Seq(extractionAlias, countAlias), input)
    val rewritten = InsertVariantCanonicalizationInAggregate(plan)

    assertReferencesResolved(rewritten)
    rewritten match {
      case Aggregate(Seq(groupingKey: Attribute), aggregateExpressions, _, _) =>
        // Aliased outputs keep their expression IDs, only the key reference inside changes.
        assert(aggregateExpressions.map(_.exprId) == Seq(extractionAlias.exprId, countAlias.exprId))
        assert(aggregateExpressions.forall(!_.references.contains(variantAttribute)))
        assert(aggregateExpressions.forall(_.references.contains(groupingKey)))
        assert(aggregateExpressions.head.collectFirst { case g: VariantGet => g.child } ==
          Some(groupingKey))
      case other =>
        fail(s"Unexpected plan:\n$other")
    }
  }

  test("share one canonicalization between distinct aggregates over the same argument") {
    val plan = Aggregate(
      Nil,
      Seq(
        countDistinct(variantAttribute).as("c1"),
        countDistinct(variantAttribute, intAttribute).as("c2")),
      input)
    val rewritten = InsertVariantCanonicalizationInAggregate(plan)

    assertReferencesResolved(rewritten)
    val distinctAliases = aliasesNamed(rewritten, distinctAliasName)
    assert(distinctAliases.size == 1)
    assert(projects(rewritten).size == 1)
    rewritten match {
      case aggregate: Aggregate =>
        assert(distinctArguments(aggregate) ==
          Seq(distinctAliases.head.toAttribute, distinctAliases.head.toAttribute, intAttribute))
      case other =>
        fail(s"Unexpected plan:\n$other")
    }
  }

  test("canonicalize a filtered distinct argument and keep its filter") {
    val filter = intAttribute > 0
    val plan = Aggregate(
      Nil,
      Seq(countDistinctWithFilter(filter, variantAttribute).as("count")),
      input)
    val rewritten = InsertVariantCanonicalizationInAggregate(plan)

    assertReferencesResolved(rewritten)
    val distinctAliases = aliasesNamed(rewritten, distinctAliasName)
    assert(distinctAliases.size == 1)
    rewritten match {
      case aggregate @ Aggregate(Nil, Seq(Alias(expression: AggregateExpression, "count")), _, _) =>
        assert(expression.isDistinct)
        assert(expression.filter == Some(filter))
        assert(distinctArguments(aggregate) == Seq(distinctAliases.head.toAttribute))
      case other =>
        fail(s"Unexpected plan:\n$other")
    }
  }

  test("leave scalar extractions from a Variant untouched") {
    val extraction = VariantGet(
      variantAttribute, Literal("$.a"), IntegerType, failOnError = true, Some("UTC"))
    val plan = PullOutGroupingExpressions(Aggregate(
      Seq(extraction),
      Seq(extraction.as("a"), countDistinct(extraction).as("count")),
      input))

    comparePlans(InsertVariantCanonicalizationInAggregate(plan), plan)
  }

  test("canonicalize a distinct argument without grouping keys") {
    val plan = Aggregate(Nil, Seq(countDistinct(variantAttribute).as("count")), input)
    val rewritten = InsertVariantCanonicalizationInAggregate(plan)

    assertReferencesResolved(rewritten)
    val distinctAliases = aliasesNamed(rewritten, distinctAliasName)
    assert(distinctAliases.size == 1)
    assert(distinctAliases.head.child == CanonicalizeVariant(variantAttribute))
    assert(projects(rewritten).size == 1)
    rewritten match {
      case aggregate @ Aggregate(Nil, Seq(Alias(_, "count")), Project(_, `input`), _) =>
        assert(distinctArguments(aggregate) == Seq(distinctAliases.head.toAttribute))
      case other =>
        fail(s"Unexpected plan:\n$other")
    }
  }

  test("project a complex struct distinct argument before canonicalizing it") {
    val struct = namedStruct(Literal("v"), variantAttribute, Literal("i"), intAttribute)
    val plan = Aggregate(Nil, Seq(countDistinct(struct).as("count")), input)
    val rewritten = InsertVariantCanonicalizationInAggregate(plan)

    assertReferencesResolved(rewritten)
    assert(projects(rewritten).size == 2)
    val inputAliases = aliasesNamed(rewritten, distinctInputAliasName)
    val distinctAliases = aliasesNamed(rewritten, distinctAliasName)
    assert(inputAliases.size == 1)
    assert(inputAliases.head.child == struct)
    assert(distinctAliases.size == 1)
    // The struct is rebuilt from the projected attribute: only the Variant field is canonicalized
    // and the original expression is not evaluated again.
    val structInput = inputAliases.head.toAttribute
    assert(distinctAliases.head.child == CreateNamedStruct(Seq(
      Literal("v"), CanonicalizeVariant(GetStructField(structInput, 0, Some("v"))),
      Literal("i"), GetStructField(structInput, 1, Some("i")))))
  }

  test("canonicalize Variants nested in struct, array and map grouping keys") {
    val nestedInput = LocalRelation(
      Symbol("s").struct(StructType(Seq(
        StructField("a", VariantType), StructField("b", IntegerType)))),
      Symbol("arr").array(VariantType),
      Symbol("m").map(StringType, VariantType))
    val Seq(structAttribute, arrayAttribute, mapAttribute) = nestedInput.output
    val plan = Aggregate(nestedInput.output, nestedInput.output, nestedInput)
    val rewritten = InsertVariantCanonicalizationInAggregate(plan)

    assert(rewritten.output.map(_.name) == Seq("s", "arr", "m"))
    assertReferencesResolved(rewritten)
    val groupingAliases = aliasesNamed(rewritten, groupingAliasName)
    assert(groupingAliases.size == 3)
    groupingAliases.map(_.child) match {
      case Seq(
          If(IsNull(`structAttribute`), Literal(null, _), struct: CreateNamedStruct),
          ArrayTransform(`arrayAttribute`, LambdaFunction(arrayBody, _, _)),
          TransformValues(`mapAttribute`, LambdaFunction(mapBody, _, _))) =>
        assert(struct.valExprs == Seq(
          CanonicalizeVariant(GetStructField(structAttribute, 0, Some("a"))),
          GetStructField(structAttribute, 1, Some("b"))))
        assert(arrayBody.isInstanceOf[CanonicalizeVariant])
        assert(mapBody.isInstanceOf[CanonicalizeVariant])
      case other =>
        fail(s"Unexpected canonicalization expressions:\n${other.mkString("\n")}")
    }
  }

  test("canonicalize the aggregate the optimizer creates for DISTINCT") {
    // The point is that the canonicalization batch runs after the DISTINCT has been replaced by
    // an aggregate. The relation needs more than one row, otherwise the aggregate is redundant and
    // removed; local-relation folding is excluded so the projection stays visible, and the
    // test-only plan validation is skipped because it still applies the analyzer check that
    // forbids grouping by Variant until SPARK-60339 lifts it.
    val relation = LocalRelation(input.output, Seq(InternalRow(null, 1), InternalRow(null, 2)))
    val optimized = withSQLConf(
        SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> ConvertToLocalRelation.ruleName,
        SQLConf.PLAN_CHANGE_VALIDATION.key -> "false") {
      SimpleTestOptimizer.execute(Distinct(relation))
    }

    assertReferencesResolved(optimized)
    val groupingAliases = aliasesNamed(optimized, groupingAliasName)
    assert(groupingAliases.size == 1, s"Unexpected plan:\n$optimized")
    assert(groupingAliases.head.child == CanonicalizeVariant(variantAttribute))
    optimized match {
      case Aggregate(groupingKeys, output, _: Project, _) =>
        assert(groupingKeys.exists(_.semanticEquals(groupingAliases.head.toAttribute)))
        assert(output.map(_.name) == Seq("v", "i"))
      case other =>
        fail(s"Unexpected plan:\n$other")
    }
  }

  test("leave aggregates without Variant keys or distinct arguments untouched") {
    val plan = Aggregate(
      Seq(intAttribute),
      Seq(intAttribute, count(variantAttribute).as("count")),
      input)

    comparePlans(InsertVariantCanonicalizationInAggregate(plan), plan)
  }

  test("leave Variant-free aggregates untouched when another aggregate needs rewriting") {
    val scalarInput = LocalRelation(Symbol("j").int)
    val scalarAggregate = Aggregate(
      Nil,
      Seq(count(scalarInput.output.head).as("count")),
      scalarInput)
    val variantAggregate = Aggregate(
      Nil,
      Seq(countDistinct(variantAttribute).as("count")),
      input)

    InsertVariantCanonicalizationInAggregate(Union(Seq(scalarAggregate, variantAggregate))) match {
      case Union(Seq(rewrittenScalarAggregate, rewrittenVariantAggregate), _, _) =>
        comparePlans(rewrittenScalarAggregate, scalarAggregate)
        assert(projects(rewrittenScalarAggregate).isEmpty)
        assert(projects(rewrittenVariantAggregate).size == 1)
      case other =>
        fail(s"Unexpected plan:\n$other")
    }
  }

  test("canonicalize Variant repartition keys in place and keep partition options") {
    val arrayInput = LocalRelation(
      AttributeReference("v", VariantType)(), Symbol("i").int, Symbol("arr").array(VariantType))
    val Seq(v, i, arr) = arrayInput.output
    val plan = RepartitionByExpression(Seq(v, i, arr), arrayInput, optNumPartitions = Some(7))
    val rewritten = InsertVariantCanonicalizationInRepartitionExpressions(plan)

    rewritten match {
      case RepartitionByExpression(
          Seq(CanonicalizeVariant(`v`), `i`, ArrayTransform(`arr`, LambdaFunction(body, _, _))),
          `arrayInput`, Some(7), None) =>
        assert(body.isInstanceOf[CanonicalizeVariant])
      case other =>
        fail(s"Unexpected plan:\n$other")
    }
  }

  test("canonicalize the child of range partitioning sort orders") {
    val plan = RepartitionByExpression(
      Seq(SortOrder(variantAttribute, Ascending), SortOrder(intAttribute, Ascending)),
      input,
      optNumPartitions = None,
      optAdvisoryPartitionSize = Some(64L))
    val rewritten = InsertVariantCanonicalizationInRepartitionExpressions(plan)

    rewritten match {
      case RepartitionByExpression(
          Seq(
            SortOrder(CanonicalizeVariant(`variantAttribute`), Ascending, _, _),
            SortOrder(`intAttribute`, Ascending, _, _)),
          `input`, None, Some(64L)) =>
      case other =>
        fail(s"Unexpected plan:\n$other")
    }
  }

  test("leave repartitioning without Variant keys untouched") {
    val plan = RepartitionByExpression(Seq(intAttribute), input, None)

    comparePlans(InsertVariantCanonicalizationInRepartitionExpressions(plan), plan)
  }
}
