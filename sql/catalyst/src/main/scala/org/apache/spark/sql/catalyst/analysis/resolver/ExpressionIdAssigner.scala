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
import org.apache.spark.sql.errors.QueryCompilationErrors

/**
 * [[ExpressionIdAssigner]] is used by the [[ExpressionResolver]] to assign unique expression IDs to
 * [[NamedExpression]]s ([[AttributeReference]]s and [[Alias]]es). This is necessary to ensure
 * that Optimizer performs its work correctly and does not produce correctness issues.
 *
 * The framework works the following way:
 *  - Each leaf operator must have unique output IDs (even if it's the same table, view, or CTE).
 *  - The [[AttributeReference]]s get propagated "upwards" through the operator tree with their IDs
 *    preserved.
 *  - Each [[Alias]] gets assigned a new unique ID and it sticks with it after it gets converted to
 *    an [[AttributeReference]] when it is outputted from the operator that produced it.
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
 * There's an important caveat here: the leftmost branch of a logical plan tree. In this branch we
 * need to preserve the expression IDs wherever possible because DataFrames may reference each other
 * using their attributes. This also makes sense for performance reasons.
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
 * in Spark.
 *
 * The [[ExpressionIdAssigner]] covers both SQL and DataFrame scenarios with single approach and is
 * integrated in the single-pass analysis framework.
 *
 * The [[ExpressionIdAssigner]] is used in the following way:
 *  - When the [[Resolver]] traverses the tree downwards prior to starting bottom-up analysis,
 *    we build the [[mappingStack]] by calling [[withNewMapping]] (i.e. [[mappingStack.push]])
 *    for every child of a multi-child operator, so we have a separate stack entry (separate
 *    mapping) for each branch. This way sibling branches' mappings are isolated from each other and
 *    attribute IDs are reused only within the same branch. Initially we push `None`, because
 *    the mapping needs to be initialized later with the correct output of a resolved operator.
 *  - When the bottom-up analysis starts, we assign IDs to all the [[NamedExpression]]s which are
 *    present in operators starting from the [[LeafNode]]s using [[mapExpression]].
 *    [[createMapping]] is called right after each [[LeafNode]] is resolved, and first remapped
 *    attributes come from that [[LeafNode]]. This is done in [[Resolver.handleLeafOperator]] for
 *    each logical plan tree branch except the leftmost.
 *  - Once the child branch is resolved, [[withNewMapping]] ends by calling [[mappingStack.pop]].
 *  - After the multi-child operator is resolved, we call [[createMapping]] to
 *    initialize the mapping with attributes _chosen_ (e.g. [[Union.mergeChildOutputs]]) by that
 *    operator's resolution algorithm and remap _old_ expression IDs to those chosen attributes.
 *  - Continue remapping expressions until we reach the root of the operator tree.
 */
class ExpressionIdAssigner {
  private val mappingStack = new ExpressionIdAssigner.Stack
  mappingStack.push(ExpressionIdAssigner.StackEntry(isLeftmostBranch = true))

  /**
   * Returns `true` if the current logical plan branch is the leftmost branch. This is important
   * in the context of preserving expression IDs in DataFrames. See class doc for more details.
   */
  def isLeftmostBranch: Boolean = mappingStack.peek().isLeftmostBranch

  /**
   * A RAII-wrapper for [[mappingStack.push]] and [[mappingStack.pop]]. [[Resolver]] uses this for
   * every child of a multi-child operator to ensure that each operator branch uses an isolated
   * expression ID mapping.
   *
   * @param isLeftmostChild whether the current child is the leftmost child of the operator that is
   *   being resolved. This is used to determine whether the new stack entry is gonna be in the
   *   leftmost logical plan branch. It's `false` by default, because it's safer to remap attributes
   *   than to leave duplicates (to prevent correctness issues).
   */
  def withNewMapping[R](isLeftmostChild: Boolean = false)(body: => R): R = {
    mappingStack.push(
      ExpressionIdAssigner.StackEntry(
        isLeftmostBranch = isLeftmostChild && isLeftmostBranch
      )
    )
    try {
      body
    } finally {
      mappingStack.pop()
    }
  }

  /**
   * Create mapping with the given `newOutput` that rewrites the `oldOutput`. This
   * is used by the [[Resolver]] after the multi-child operator is resolved to fill the current
   * mapping with the attributes _chosen_ by that operator's resolution algorithm and remap _old_
   * expression IDs to those chosen attributes. It's also used by the [[ExpressionResolver]] right
   * before remapping the attributes of a [[LeafNode]].
   *
   * `oldOutput` is present for already resolved subtrees (e.g. DataFrames), but for SQL queries
   * is will be `None`, because that logical plan is analyzed for the first time.
   */
  def createMapping(
      newOutput: Seq[Attribute] = Seq.empty,
      oldOutput: Option[Seq[Attribute]] = None): Unit = {
    if (mappingStack.peek().mapping.isDefined) {
      throw SparkException.internalError(
        s"Attempt to overwrite existing mapping. New output: $newOutput, old output: $oldOutput"
      )
    }

    val newMapping = new ExpressionIdAssigner.Mapping
    oldOutput match {
      case Some(oldOutput) =>
        if (newOutput.length != oldOutput.length) {
          throw SparkException.internalError(
            s"Outputs have different lengths. New output: $newOutput, old output: $oldOutput"
          )
        }

        newOutput.zip(oldOutput).foreach {
          case (newAttribute, oldAttribute) =>
            newMapping.put(oldAttribute.exprId, newAttribute.exprId)
            newMapping.put(newAttribute.exprId, newAttribute.exprId)
        }
      case None =>
        newOutput.foreach { newAttribute =>
          newMapping.put(newAttribute.exprId, newAttribute.exprId)
        }
    }

    mappingStack.push(mappingStack.pop().copy(mapping = Some(newMapping)))
  }

  /**
   * Assign a correct ID to the given [[originalExpression]] and return a new instance of that
   * expression, or return a corresponding new instance of the same attribute, that was previously
   * reallocated and is present in the current [[mappingStack]] entry.
   *
   * For [[Alias]]es: Try to preserve them if we are in the leftmost logical plan tree branch and
   * unless they conflict. Conflicting [[Alias]] IDs are never acceptable. Otherwise, reallocate
   * with a new ID and return that instance.
   *
   * For [[AttributeReference]]s: If the attribute is present in the current [[mappingStack]] entry,
   * return that instance, otherwise reallocate with a new ID and return that instance. The mapping
   * is done both from the original expression ID _and_ from the new expression ID - this way we are
   * able to replace old references to that attribute in the current operator branch, and preserve
   * already reallocated attributes to make this call idempotent.
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
   */
  def mapExpression(originalExpression: NamedExpression): NamedExpression = {
    if (mappingStack.peek().mapping.isEmpty) {
      throw SparkException.internalError(
        "Expression ID mapping doesn't exist. Please call createMapping(...) first. " +
        s"Original expression: $originalExpression"
      )
    }

    val currentMapping = mappingStack.peek().mapping.get

    val resultExpression = originalExpression match {
      case alias: Alias if isLeftmostBranch =>
        val resultAlias = currentMapping.get(alias.exprId) match {
          case null =>
            alias
          case _ =>
            alias.newInstance()
        }
        currentMapping.put(resultAlias.exprId, resultAlias.exprId)
        resultAlias
      case alias: Alias =>
        reassignExpressionId(alias, currentMapping)
      case attributeReference: AttributeReference =>
        currentMapping.get(attributeReference.exprId) match {
          case null =>
            reassignExpressionId(attributeReference, currentMapping)
          case mappedExpressionId =>
            attributeReference.withExprId(mappedExpressionId)
        }
      case _ =>
        throw QueryCompilationErrors.unsupportedSinglePassAnalyzerFeature(
          s"${originalExpression.getClass} expression ID assignment"
        )
    }

    resultExpression.copyTagsFrom(originalExpression)
    resultExpression
  }

  private def reassignExpressionId(
      originalExpression: NamedExpression,
      currentMapping: ExpressionIdAssigner.Mapping): NamedExpression = {
    val newExpression = originalExpression.newInstance()

    currentMapping.put(originalExpression.exprId, newExpression.exprId)
    currentMapping.put(newExpression.exprId, newExpression.exprId)

    newExpression
  }
}

object ExpressionIdAssigner {
  type Mapping = HashMap[ExprId, ExprId]

  case class StackEntry(mapping: Option[Mapping] = None, isLeftmostBranch: Boolean = false)

  type Stack = ArrayDeque[StackEntry]

  /**
   * Assert that `outputs` don't have conflicting expression IDs. This is only relevant for child
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
   */
  def assertOutputsHaveNoConflictingExpressionIds(outputs: Seq[Seq[Attribute]]): Unit = {
    if (doOutputsHaveConflictingExpressionIds(outputs)) {
      throw SparkException.internalError(s"Conflicting expression IDs in child outputs: $outputs")
    }
  }

  private def doOutputsHaveConflictingExpressionIds(outputs: Seq[Seq[Attribute]]): Boolean = {
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
