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

package org.apache.spark.sql.catalyst.analysis.resolver

import java.util.{ArrayDeque, HashMap, HashSet}

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  Attribute,
  AttributeReference,
  ExprId,
  NamedExpression
}
import org.apache.spark.sql.catalyst.plans.logical.{CTERelationRef, LeafNode}
import org.apache.spark.sql.catalyst.trees.CurrentOrigin.withOrigin
import org.apache.spark.sql.errors.QueryCompilationErrors

/**
 * [[ExpressionIdAssigner]] is used by the [[ExpressionResolver]] to assign unique expression IDs to
 * [[NamedExpression]]s ([[AttributeReference]]s and [[Alias]]es). This is necessary to ensure
 * that Optimizer performs its work correctly and does not produce correctness issues.
 *
 * The framework works the following way:
 *  - Each leaf operator must have globally unique output IDs (even if it's the same table, view,
 *    or CTE).
 *  - The [[AttributeReference]]s get propagated "upwards" through the operator tree with their IDs
 *    preserved. In case of correlated subqueries [[AttributeReference]]s may propagate downwards
 *    from the outer scope to the point of correlated reference in the subquery. Currently only
 *    one level of correlation is supported.
 *  - Each [[Alias]] gets assigned a new globally unique ID and it sticks with it after it gets
 *    converted to an [[AttributeReference]] when it is outputted from the operator that produced
 *    it.
 *  - Any operator may have [[AttributeReference]]s with the same IDs in its output given it is the
 *    same attribute.
 * Thus, **no multi-child operator may have children with conflicting [[AttributeReference]] IDs**.
 * In other words, two subtrees must not output the [[AttributeReference]]s with the same IDs, since
 * relations, views and CTEs all output unique attributes, and [[Alias]]es get assigned new IDs as
 * well. [[ExpressionIdAssigner.assertOutputsHaveNoConflictingExpressionIds]] is used to assert this
 * invariant.
 *
 * For SQL queries, this framework provides correctness just by reallocating relation outputs and
 * by validating the invariants mentioned above. Reallocation is done in
 * [[Resolver.handleLeafOperator]]. If all the relations (even if it's the same table) have unique
 * output IDs, the expression ID assignment will be correct, because there are no duplicate IDs in
 * a pure unresolved tree. The old ID -> new ID mapping is not needed in this case.
 * For example, consider this query:
 *
 * {{{
 * SELECT * FROM t AS t1 CROSS JOIN t AS t2 ON t1.col1 = t2.col1
 * }}}
 *
 * The analyzed plan should be:
 * {{{
 * Project [col1#0, col2#1, col1#2, col2#3]
 * +- Join Cross, (col1#0 = col1#2)
 *    :- SubqueryAlias t1
 *    :   +- Relation t[col1#0,col2#1] parquet
 *    +- SubqueryAlias t2
 *        +- Relation t[col1#2,col2#3] parquet
 * }}}
 *
 * and not:
 * {{{
 * Project [col1#0, col2#1, col1#0, col2#1]
 * +- Join Cross, (col1#0 = col1#0)
 *    :- SubqueryAlias t1
 *    :   +- Relation t[col1#0,col2#1] parquet
 *    +- SubqueryAlias t2
 *        +- Relation t[col1#0,col2#1] parquet
 * }}}
 *
 * Because in the latter case the join condition is always true.
 *
 * For DataFrame programs we need the full power of [[ExpressionIdAssigner]], and old ID -> new ID
 * mapping comes in handy, because DataFrame programs pass _partially_ resolved plans to the
 * [[Resolver]], which may consist of duplicate subtrees, and thus will have already assigned
 * expression IDs. These already resolved duplicate subtrees with assigned IDs will conflict.
 * Hence, we need to reallocate all the leaf node outputs _and_ remap old IDs to the new ones.
 * Also, DataFrame programs may introduce the same [[Alias]]es in different parts of the query plan,
 * so we just reallocate all the [[Alias]]es.
 *
 * For example, consider this DataFrame program:
 *
 * {{{
 * spark.range(0, 10).select($"id").write.format("parquet").saveAsTable("t")
 * val alias = ($"id" + 1).as("id")
 * spark.table("t").select(alias).select(alias)
 * }}}
 *
 * The analyzed plan should be:
 * {{{
 * Project [(id#6L + cast(1 as bigint)) AS id#13L]
 * +- Project [(id#4L + cast(1 as bigint)) AS id#6L]
 *    +- SubqueryAlias spark_catalog.default.t
 *       +- Relation spark_catalog.default.t[id#4L] parquet
 * }}}
 *
 * and not:
 * {{{
 * Project [(id#6L + cast(1 as bigint)) AS id#6L]
 * +- Project [(id#4L + cast(1 as bigint)) AS id#6L]
 *    +- SubqueryAlias spark_catalog.default.t
 *       +- Relation spark_catalog.default.t[id#4L] parquet
 * }}}
 *
 * Because the latter case will confuse the Optimizer and the top [[Project]] will be eliminated
 * leading to incorrect result.
 *
 * In case of partially resolved DataFrame subtrees with correlated subqueries inside we need to
 * remap [[OuterReference]]s as well:
 *
 * {{{
 * val df = spark.sql("""
 *   SELECT * FROM t1 WHERE EXISTS (
 *     SELECT * FROM t2 WHERE t2.id == t1.id
 *   )
 * """)
 * df.union(df)
 * }}}
 *
 * The analyzed plan should be:
 * {{{
 * Union false, false
 * :- Project [id#1]
 * :  +- Filter exists#9 [id#1]
 * :     :  +- Project [id#16]
 * :     :     +- Filter (id#16 = outer(id#1))
 * :     :        +- SubqueryAlias spark_catalog.default.t2
 * :     :           +- Relation spark_catalog.default.t2[id#16] parquet
 * :     +- SubqueryAlias spark_catalog.default.t1
 * :        +- Relation spark_catalog.default.t1[id#1] parquet
 * +- Project [id#17 AS id#19]
 *    +- Project [id#17]
 *       +- Filter exists#9 [id#17]
 *          :  +- Project [id#18]
 *          :     +- Filter (id#18 = outer(id#17))
 *          :        +- SubqueryAlias spark_catalog.default.t2
 *          :           +- Relation spark_catalog.default.t2[id#18] parquet
 *          +- SubqueryAlias spark_catalog.default.t1
 *             +- Relation spark_catalog.default.t1[id#17] parquet
 * }}}
 *
 * Note how id#17 is the same in outer branch and in a subquery - is was properly remapped, because
 * the right subtree of [[Union]] contained identical expression IDs as the left subtree. That's
 * why we pass main mapping as outer mapping to the correlated subquery branch.
 *
 * There's an important caveat here: those branches of a logical plan tree where outputs do not
 * conflict. We should preserve expression IDs on those branches wherever possible because
 * DataFrames may reference each other using their attributes. This also makes sense for
 * performance reasons.
 *
 * Consider this example:
 *
 * {{{
 * val df1 = spark.range(0, 10).select($"id")
 * val df2 = spark.range(5, 15).select($"id")
 * df1.union(df2).filter(df1("id") === 5)
 * }}}
 *
 * In this example `df("id")` references lower `id` attribute by expression ID, so `union` must not
 * reassign expression IDs in `df1` (left child). Referencing `df2` (right child) is not supported
 * in Spark, because [[Union]] does not output it, but we don't have to regenerate expression IDs
 * in that branch either.
 *
 * However:
 *
 * {{{
 * val df1 = spark.range(0, 10).select($"id")
 * df1.union(df1).filter(df1("id") === 5)
 * }}}
 *
 * Here we need to regenerate expression IDs in the right branch, because those would conflict
 * (both branches are the same plan). Expression IDs in the left branch may be preserved.
 *
 * CTE references are handled in a special way to stay compatible with the fixed-point Analyzer.
 * First [[CTERelationRef]] that we meet in the query plan can preserve its output expression IDs,
 * and the plan will be inlined by the [[InlineCTE]] without any artificial [[Alias]]es that
 * "stitch" expression IDs together. This way we ensure that Optimizer behavior is the same as
 * after the fixed-point Analyzer and that no extra projections are introduced.
 *
 * The [[ExpressionIdAssigner]] covers both SQL and DataFrame scenarios with single approach and is
 * integrated into the single-pass analysis framework.
 *
 * The [[ExpressionIdAssigner]] is used in the following way:
 *  - When the [[Resolver]] traverses the tree downwards prior to starting bottom-up analysis,
 *    we build the [[mappingStack]] by calling [[pushMapping]].
 *    for every child of a multi-child operator, so we have a separate stack entry (separate
 *    mapping) for each branch. This way sibling branches' mappings are isolated from each other and
 *    attribute IDs are reused only within the same branch. Initially we push `None`, because
 *    the mapping needs to be initialized later with the correct output of a resolved operator.
 *  - When the bottom-up analysis starts, we assign IDs to all the [[NamedExpression]]s which are
 *    present in operators starting from the [[LeafNode]]s using [[mapExpression]].
 *    [[createMappingForLeafOperator]] is called right after each [[LeafNode]] is resolved, and
 *    first remapped attributes come from that [[LeafNode]]. This is done if leaf operator output
 *    doesn't conflict with `globalExpressionIds`.
 *  - Once the child branch is resolved, a code block started with [[pushMapping]] ends by calling
 *    [[popMapping]].
 *  - After the multi-child operator is resolved, we call [[createMappingFromChildMappings]] to
 *    initialize the mapping with attributes collected in [[popMapping]] with
 *    `collectChildMapping = true`.
 *  - While traversing the expression tree, we may meet a [[SubqueryExpression]] and resolve its
 *    plan. In this case we call [[pushMapping]] with `isSubqueryRoot = true` to pass the current
 *    mapping as outer mapping to the subquery branches. Any subquery branch may reference
 *    outer attributes, so if `isSubqueryRoot` is `false`, we pass the previous `outerMapping` to
 *    lower branches. Since we only support one level of correlation, for every subquery level
 *    current `mapping` becomes `outerMapping` for the next level.
 *  - Continue remapping expressions until we reach the root of the operator tree.
 */
class ExpressionIdAssigner {
  private val globalExpressionIds = new HashSet[ExprId]
  private val cteRelationRefOutputIds = new HashSet[ExprId]

  private val mappingStack = new ExpressionIdAssigner.Stack
  mappingStack.push(ExpressionIdAssigner.StackEntry())

  /**
   * Push new mapping entry into the `mappingStack` to make sure that each operator branch uses an
   * isolated expression ID mapping.
   *
   * @param isSubqueryRoot whether the new branch is related to a subquery root. In this case we
   *   pass current `mapping` as `outerMapping` to the subquery branches. Otherwise we just
   *   propagate `outerMapping` itself, because any nested subquery operator may reference outer
   *   attributes.
   */
  def pushMapping(isSubqueryRoot: Boolean = false): Unit = {
    val currentStackEntry = mappingStack.peek()

    mappingStack.push(
      ExpressionIdAssigner.StackEntry(
        outerMapping = if (isSubqueryRoot) {
          currentStackEntry.mapping.map(new ExpressionIdAssigner.Mapping(_))
        } else {
          currentStackEntry.outerMapping
        }
      )
    )
  }

  /**
   * Pop a mapping from the `mappingStack`.
   *
   * @param collectChildMapping whether to collect a child mapping into the current stack entry.
   *   This is used in multi-child operators to automatically propagate mapped expression IDs
   *   upwards using [[createMappingFromChildMappings]].
   */
  def popMapping(collectChildMapping: Boolean = false): Unit = {
    val childStackEntry = mappingStack.pop()

    if (collectChildMapping) {
      childStackEntry.mapping match {
        case Some(childMapping) =>
          val currentStackEntry = mappingStack.peek()
          currentStackEntry.childMappings.push(childMapping)
        case None =>
      }
    }
  }

  /**
   * Create mapping for the given `newOperator` that replaces the `oldOperator`. This is used by
   * the [[Resolver]] after a certain [[LeafNode]] is resolved to make sure that leaf node outputs
   * in the query don't have conflicting expression IDs.
   *
   * `oldOperator` is present for already resolved subtrees (e.g. DataFrames), but for SQL queries
   * is will be `None`, because that logical plan is analyzed for the first time.
   */
  def createMappingForLeafOperator(
      newOperator: LeafNode,
      oldOperator: Option[LeafNode] = None): Unit = {
    if (mappingStack.peek().mapping.isDefined) {
      throw SparkException.internalError(
        "Attempt to overwrite existing mapping. " +
        s"New operator: $newOperator, old operator: $oldOperator"
      )
    }

    val newMapping = new ExpressionIdAssigner.Mapping
    oldOperator match {
      case Some(oldOperator) =>
        if (newOperator.output.length != oldOperator.output.length) {
          throw SparkException.internalError(
            "Outputs have different lengths. " +
            s"New operator: $newOperator, old operator: $oldOperator"
          )
        }

        newOperator.output.zip(oldOperator.output).foreach {
          case (newAttribute, oldAttribute) =>
            newMapping.put(oldAttribute.exprId, newAttribute.exprId)
            newMapping.put(newAttribute.exprId, newAttribute.exprId)

            registerLeafOperatorAttribute(newOperator, newAttribute)
        }
      case None =>
        newOperator.output.foreach { newAttribute =>
          newMapping.put(newAttribute.exprId, newAttribute.exprId)

          registerLeafOperatorAttribute(newOperator, newAttribute)
        }
    }

    setCurrentMapping(newMapping)
  }

  /**
   * Create new mapping in current scope based on collected child mappings. The calling code
   * must pass `collectChildMapping = true` to all the [[popMapping]] calls beforehand.
   *
   * In case branches of a multi-child operator that is being resolved contain duplicate IDs, the
   * child mappings will have collisions during this merge operation. We need to decide which of
   * the new IDs get the priority for the old ID. This is done based on the IDs that are actually
   * outputted into the multi-child operator. This information is provided with `newOutputIds`.
   *
   * The principles:
   * 1. If the destination ID is present in `newOutputIds`, we treat it as a higher priority over
   *   the ID that is "hidden" in the logical plan branch.
   * 2. If both destination IDs are present in `newOutputIds`, we prioritize the identity mapping -
   *   the new ID which is equal to the old ID, and not the "remapping". This is valid in SQL
   *   because we are dealing with a fully unresolved plan and the remapping is not needed.
   *   DataFrame queries that contain a self-join or a self-union and are referencing the same
   *   attribute from both branches will fail (which is expected).
   * 3. We iterate over child mappings from right to left, prioritizing IDs from the left, because
   *   that's how multi-child operators like [[Join]] or [[Union]] propagate IDs upwards.
   *
   * Example 1:
   * {{{
   * val df1 = spark.range(0, 10)
   * val df2 = df1.select(($"id" + 1).as("id"))
   *
   * // Both branches originate from the same `df1`, and have duplicate IDs, so right branch IDs
   * // are regenedated. Thus, it's important to prioritize left mapping values for the same keys.
   * val df3 = df2.join(df1, "id")
   *
   * // This succeeds because left branch IDs are preserved.
   * df3.where(df2("id") === 1)
   *
   * // This fails because right branch IDs are regenerated.
   * df3.where(df1("id") === 1)
   * }}}
   *
   * Example 2:
   * {{{
   * val df1 = spark.range(10).withColumn("a", lit(0))
   *
   * // "a" is aliased to "b" and gets a new expression ID.
   * val df2 = df1.withColumnRenamed("a", "b")
   *
   * // Both types of self-join work, despite the fact that they contain duplicate IDs for "a".
   * // This is because ExpressionIdAssigner knows that "a" is outputted from the "df1" branch,
   * // and therefore that mapping is the priority one.
   * df1.join(df2, df1("a") === df2("b"))
   * df2.join(df1, df2("b") === df1("a"))
   * }}}
   *
   * Example 3:
   * {{{
   * -- In this query CTE references a table which is also present in a JOIN. First, CTE definition
   * -- is analyzed with `t1` inside. Let's say it outputs col1#0. Once we get to a left JOIN child,
   * -- which is also `t1`, we know that expression IDs in `t1` have to be regenerated to col#1
   * -- because it's a duplicate relation. After resolving the JOIN, we are left with (#0 -> #0),
   * -- (#1 -> #1) and (#0 -> #1) mappings. Also, JOIN outputs both #0 and #1. This is an example
   * -- of principle 2. when identity (#0 -> #0) and (#1 -> #1) mappings have to be prioritized,
   * -- because (#0 -> #1) is a remapping and not needed in SQL.
   * SELECT * FROM (
   *   WITH cte1 AS (SELECT * FROM t1) SELECT t1.col1 FROM t1 JOIN cte1 USING (col1)
   * );
   * }}}
   *
   * When `mergeIntoExisting` is true, we merge child mappings into an existing mapping entry
   * instead of creating a new one. This setting is used when resolving [[LateralJoin]]s.
   */
  def createMappingFromChildMappings(
      newOutputIds: Set[ExprId],
      mergeIntoExisting: Boolean = false): Unit = {
    if (!mergeIntoExisting && mappingStack.peek().mapping.isDefined) {
      throw SparkException.internalError(
        "Attempt to overwrite existing mapping with child mappings"
      )
    }

    val currentStackEntry = mappingStack.peek()
    if (currentStackEntry.childMappings.isEmpty) {
      throw SparkException.internalError("No child mappings to create new current mapping")
    }

    val newMapping = if (mergeIntoExisting) {
      currentStackEntry.mapping.get
    } else {
      new ExpressionIdAssigner.Mapping
    }

    while (!currentStackEntry.childMappings.isEmpty) {
      val nextMapping = currentStackEntry.childMappings.pop()

      nextMapping.forEach {
        case (oldId, remappedId) =>
          updateNewMapping(
            newMapping = newMapping,
            oldId = oldId,
            remappedId = remappedId,
            newOutputIds = newOutputIds
          )
      }
    }

    setCurrentMapping(newMapping)
  }

  /**
   * Assign a correct ID to the given [[originalExpression]] and return a new instance of that
   * expression, or return a corresponding new instance of the same attribute, that was previously
   * reallocated and is present in the current [[mappingStack]] entry.
   *
   * For [[Alias]]es: Try to preserve it if the alias ID doesn't conflict with
   * `globalExpressionIds`. Conflicting [[Alias]] IDs are never acceptable. In case of such
   * a conflict, or if `alwaysUpdateAlias` is true, we reallocate with a new ID and return
   * that instance.
   *
   * For [[AttributeReference]]s: If the attribute is present in the current [[mappingStack]] entry,
   * return that instance, otherwise reallocate with a new ID and return that instance. The mapping
   * is done both from the original expression ID _and_ from the new expression ID - this way we are
   * able to replace old references to that attribute in the current operator branch, and preserve
   * already reallocated attributes to make this call idempotent.
   * Dangling attribute reference results in an exception, unless `addDanglingAttributeReference`
   * is true.
   *
   * When remapping the provided expressions, we don't replace them with the previously seen
   * attributes, but replace their IDs ([[NamedExpression.withExprId]]). This is done to preserve
   * the properties of attributes at a certain point in the query plan. Examples where it's
   * important:
   *
   * 1) Preserve the name case. In Spark the "requested" name takes precedence over the "original"
   * name:
   *
   * {{{
   * -- The output schema is [col1, COL1]
   * SELECT col1, COL1 FROM VALUES (1);
   * }}}
   *
   * 2) Preserve the metadata:
   *
   * {{{
   * // Metadata "m1" remains, "m2" gets overwritten by the specified schema, "m3" is newly added.
   * val metadata1 = new MetadataBuilder().putString("m1", "1").putString("m2", "2").build()
   * val metadata2 = new MetadataBuilder().putString("m2", "3").putString("m3", "4").build()
   * val schema = new StructType().add("a", IntegerType, nullable = true, metadata = metadata2)
   * val df =
   *   spark.sql("SELECT col1 FROM VALUES (1)").select(col("col1").as("a", metadata1)).to(schema)
   * }}}
   *
   * When resolving partially resolved DataFrame plans, we sometimes may meet a duplicate
   * [[Alias]]. Every [[Alias]] in the logical plan has to have a unique expression ID. If those
   * aliases are not in the same mapping (different logical plan branches or different subqueries),
   * we can reallocate both and create mappings for each of those IDs, because those won't conflict
   * (the mappings are isolated). However, if we meet a duplicate alias in the same logical plan
   * branch, we need to decide which expression ID takes over, because there's a key clash in the
   * mapping (old -> new1, old -> new2). Usually we need the latter one, because, as in the example
   * from the class doc about a duplicate alias in different [[Project]]s, that is the one that
   * is referenced above. However, some parts of the logical plan do not leak any references
   * outside (nothing can be referenced from those parts). One example is grouping expression
   * in [[Aggregate]]. If groping expressions are created from a DataFrame API, they may contain
   * [[Alias]]es with the same IDs as in the related aggregate expressions. Since grouping
   * expressions are resolved after the aggregate expressions, we don't want the latter alias IDs
   * to take over. Hence, we set `prioritizeOldDuplicateAliasId` to `true` when resolving grouping
   * expressions and do not track the new alias, since it cannot be referenced again. And if this
   * alias is referenced for some reason, we will throw a "dangling attribute reference" error.
   *
   * In the following example Spark Connect DataFrames produce a duplicate alias mentioned above:
   * {{{
   * df.groupBy($"a", $"b".as("sum_d")).agg(Map.empty[String, String])
   * }}}
   */
  def mapExpression[NamedExpressionType <: NamedExpression](
      originalExpression: NamedExpressionType,
      alwaysUpdateAlias: Boolean = false,
      addDanglingAttributeReference: Boolean = false,
      prioritizeOldDuplicateAliasId: Boolean = false): NamedExpressionType = {
    if (mappingStack.peek().mapping.isEmpty) {
      throw SparkException.internalError(
        "Expression ID mapping doesn't exist. Please first call " +
        "createMappingForLeafOperator(...) for leaf nodes or createMappingFromChildMappings(...) " +
        s"for multi-child nodes. Original expression: $originalExpression"
      )
    }

    val currentMapping = mappingStack.peek().mapping.get

    val resultExpression = originalExpression match {
      case alias: Alias if (globalExpressionIds.contains(alias.exprId) || alwaysUpdateAlias) =>
        val newAlias = newAliasInstance(alias)

        if (!prioritizeOldDuplicateAliasId || !currentMapping.containsKey(alias.exprId)) {
          currentMapping.put(alias.exprId, newAlias.exprId)
          currentMapping.put(newAlias.exprId, newAlias.exprId)
        }
        globalExpressionIds.add(newAlias.exprId)
        newAlias
      case alias: Alias =>
        currentMapping.put(alias.exprId, alias.exprId)
        globalExpressionIds.add(alias.exprId)

        alias
      case attributeReference: AttributeReference =>
        currentMapping.get(attributeReference.exprId) match {
          case null =>
            if (addDanglingAttributeReference) {
              val newAttribute = attributeReference.newInstance()
              currentMapping.put(attributeReference.exprId, newAttribute.exprId)
              currentMapping.put(newAttribute.exprId, newAttribute.exprId)
              globalExpressionIds.add(newAttribute.exprId)
              newAttribute
            } else {
              throw SparkException.internalError(
                s"Encountered a dangling attribute reference $attributeReference"
              )
            }
          case mappedExpressionId =>
            attributeReference.withExprId(mappedExpressionId)
        }
      case _ =>
        throw QueryCompilationErrors.unsupportedSinglePassAnalyzerFeature(
          s"${originalExpression.getClass} expression ID assignment"
        )
    }

    resultExpression.asInstanceOf[NamedExpressionType]
  }

  /**
   * Map [[AttributeReference]] which is a child of [[OuterReference]]. When [[ExpressionResolver]]
   * meets an attribute under a resolved [[OuterReference]], it remaps it using the outer
   * mapping passed from the parent plan of the [[SubqueryExpression]] that is currently being
   * re-analyzed. This mapping must exist, as well as a mapped expression ID. Otherwise we met a
   * danging outer reference, which is an internal error, unless `ignoreAbsent` is true (in which
   * case we return the input without changes).
   */
  def mapOuterReference(
      attributeReference: AttributeReference,
      ignoreAbsent: Boolean = false): AttributeReference = {
    if (mappingStack.peek().outerMapping.isEmpty) {
      if (ignoreAbsent) {
        attributeReference
      } else {
        throw SparkException.internalError(
          "Outer expression ID mapping doesn't exist while remapping outer reference " +
          s"$attributeReference"
        )
      }
    } else {
      mappingStack.peek().outerMapping.get.get(attributeReference.exprId) match {
        case null =>
          if (!ignoreAbsent) {
            throw SparkException.internalError(
              s"No mapped expression ID for outer reference $attributeReference"
            )
          }
          attributeReference
        case mappedExpressionId =>
          attributeReference.withExprId(mappedExpressionId)
      }
    }
  }

  /**
   * Returns `true` if expression IDs for the current [[LeafNode]] should be preserved. This is
   * important for DataFrames that reference columns by their IDs. See class doc for more details.
   *
   * Expression IDs of outputs of the first CTE reference are not regenerated for compatibility
   * with the fixed-point Analyzer.
   */
  def shouldPreserveLeafOperatorIds(leafOperator: LeafNode): Boolean = {
    leafOperator match {
      case cteRelationRef: CTERelationRef =>
        cteRelationRef.output.forall { attribute =>
          !cteRelationRefOutputIds.contains(attribute.exprId)
        }
      case _ =>
        leafOperator.output.forall { attribute =>
          !globalExpressionIds.contains(attribute.exprId)
        }
    }
  }

  private def setCurrentMapping(mapping: ExpressionIdAssigner.Mapping): Unit = {
    val currentEntry = mappingStack.pop()
    mappingStack.push(currentEntry.copy(mapping = Some(mapping)))
  }

  private def newAliasInstance(alias: Alias): Alias = {
    val newAlias = withOrigin(alias.origin) {
      alias.newInstance().asInstanceOf[Alias]
    }
    newAlias.copyTagsFrom(alias)
    newAlias
  }

  private def registerLeafOperatorAttribute(leafOperator: LeafNode, attribute: Attribute): Unit = {
    globalExpressionIds.add(attribute.exprId)
    if (leafOperator.isInstanceOf[CTERelationRef]) {
      cteRelationRefOutputIds.add(attribute.exprId)
    }
  }

  /**
   * Update `newMapping` with the `oldId -> remappedId` mapping, based on the principles described
   * in [[createMappingFromChildMappings]]:
   * 1. If no mapping from `oldId` exists, we create it
   * 2. If the mapping from `oldId` already exists but is not present in `newOutputIds`, we
   *   deprioritize old mapping in favor of new one
   * 3. If the mapping from `oldId` already exists and is present in `newOutputIds` and the new
   *   mapping is the identity one, we deprioritize old mapping in favor of new one
   * 4. Otherwise we keep the existing mapping
   */
  private def updateNewMapping(
      newMapping: ExpressionIdAssigner.Mapping,
      oldId: ExprId,
      remappedId: ExprId,
      newOutputIds: Set[ExprId]): Unit = {
    newMapping.get(oldId) match {
      case null =>
        newMapping.put(oldId, remappedId)

      case knownRemappedId if !newOutputIds.contains(knownRemappedId) =>
        newMapping.put(oldId, remappedId)

      case knownRemappedId if newOutputIds.contains(remappedId) && remappedId == oldId =>
        newMapping.put(oldId, remappedId)

      case _ =>
    }
  }
}

object ExpressionIdAssigner {
  type Mapping = HashMap[ExprId, ExprId]

  case class StackEntry(
      mapping: Option[Mapping] = None,
      outerMapping: Option[Mapping] = None,
      childMappings: ArrayDeque[Mapping] = new ArrayDeque[Mapping])

  type Stack = ArrayDeque[StackEntry]

  /**
   * Assert that `outputs` don't have conflicting expression IDs.
   */
  def assertOutputsHaveNoConflictingExpressionIds(outputs: Seq[Seq[Attribute]]): Unit = {
    if (doOutputsHaveConflictingExpressionIds(outputs)) {
      throw SparkException.internalError(s"Conflicting expression IDs in child outputs: $outputs")
    }
  }

  /**
   * Check whether `outputs` have conflicting expression IDs. This is only relevant for child
   * outputs of multi-child operators. Conflicting attributes are only checked between different
   * child branches, since one branch may output the same attribute multiple times. Hence, we use
   * only distinct expression IDs from each output.
   *
   * {{{
   * -- This is OK, one operator branch outputs its attribute multiple times
   * SELECT col1, col1 FROM t1;
   * }}}
   *
   * {{{
   * -- If both children of this [[Union]] operator output `col1` with the same expression ID,
   * -- the analyzer is broken.
   * SELECT col1 FROM t1
   * UNION ALL
   * SELECT col1 FROM t1
   * ;
   * }}}
   *
   * One edge case is [[WithCTE]] - we don't have to check conflicts between [[CTERelationDef]]s and
   * the plan itself.
   */
  def doOutputsHaveConflictingExpressionIds(outputs: Seq[Seq[Attribute]]): Boolean = {
    outputs.length > 1 && {
      val expressionIds = new HashSet[ExprId]

      outputs.exists { output =>
        val outputExpressionIds = new HashSet[ExprId]

        val hasConflicting = output.exists { attribute =>
          outputExpressionIds.add(attribute.exprId)
          expressionIds.contains(attribute.exprId)
        }

        if (!hasConflicting) {
          expressionIds.addAll(outputExpressionIds)
        }

        hasConflicting
      }
    }
  }
}
