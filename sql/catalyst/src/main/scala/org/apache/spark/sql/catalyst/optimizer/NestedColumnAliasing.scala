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

import scala.collection.mutable

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

/**
 * This aims to handle a nested column aliasing pattern inside the [[ColumnPruning]] optimizer rule.
 * If:
 * - A [[Project]] or its child references nested fields
 * - Not all of the fields in a nested attribute are used
 * Then:
 * - Substitute the nested field references with alias attributes
 * - Add grandchild [[Project]]s transforming the nested fields to aliases
 *
 * Example 1: Project
 * ------------------
 * Before:
 * +- Project [concat_ws(s#0.a, s#0.b) AS concat_ws(s.a, s.b)#1]
 *   +- GlobalLimit 5
 *     +- LocalLimit 5
 *       +- LocalRelation <empty>, [s#0]
 * After:
 * +- Project [concat_ws(_extract_a#2, _extract_b#3) AS concat_ws(s.a, s.b)#1]
 *   +- GlobalLimit 5
 *     +- LocalLimit 5
 *       +- Project [s#0.a AS _extract_a#2, s#0.b AS _extract_b#3]
 *         +- LocalRelation <empty>, [s#0]
 *
 * Example 2: Project above Filter
 * -------------------------------
 * Before:
 * +- Project [s#0.a AS s.a#1]
 *   +- Filter (length(s#0.b) > 2)
 *     +- GlobalLimit 5
 *       +- LocalLimit 5
 *         +- LocalRelation <empty>, [s#0]
 * After:
 * +- Project [_extract_a#2 AS s.a#1]
 *   +- Filter (length(_extract_b#3) > 2)
 *     +- GlobalLimit 5
 *       +- LocalLimit 5
 *         +- Project [s#0.a AS _extract_a#2, s#0.b AS _extract_b#3]
 *           +- LocalRelation <empty>, [s#0]
 *
 * Example 3: Nested fields with referenced parents
 * ------------------------------------------------
 * Before:
 * +- Project [s#0.a AS s.a#1, s#0.a.a1 AS s.a.a1#2]
 *   +- GlobalLimit 5
 *     +- LocalLimit 5
 *       +- LocalRelation <empty>, [s#0]
 * After:
 * +- Project [_extract_a#3 AS s.a#1, _extract_a#3.name AS s.a.a1#2]
 *   +- GlobalLimit 5
 *     +- LocalLimit 5
 *       +- Project [s#0.a AS _extract_a#3]
 *         +- LocalRelation <empty>, [s#0]
 *
 * The schema of the datasource relation will be pruned in the [[SchemaPruning]] optimizer rule.
 */
object NestedColumnAliasing {

  def unapply(plan: LogicalPlan): Option[LogicalPlan] = plan match {
    /**
     * This pattern is needed to support [[Filter]] plan cases like
     * [[Project]]->[[Filter]]->listed plan in [[canProjectPushThrough]] (e.g., [[Window]]).
     * The reason why we don't simply add [[Filter]] in [[canProjectPushThrough]] is that
     * the optimizer can hit an infinite loop during the [[PushDownPredicates]] rule.
     */
    case Project(projectList, Filter(condition, child)) if
        SQLConf.get.nestedSchemaPruningEnabled && canProjectPushThrough(child) =>
      rewritePlanIfSubsetFieldsUsed(
        plan, projectList ++ Seq(condition) ++ child.expressions, child.producedAttributes.toSeq)

    case Project(projectList, child) if
        SQLConf.get.nestedSchemaPruningEnabled && canProjectPushThrough(child) =>
      rewritePlanIfSubsetFieldsUsed(
        plan, projectList ++ child.expressions, child.producedAttributes.toSeq)

    case p if SQLConf.get.nestedSchemaPruningEnabled && canPruneOn(p) =>
      rewritePlanIfSubsetFieldsUsed(
        plan, p.expressions, p.producedAttributes.toSeq)

    case _ => None
  }

  /**
   * Rewrites a plan with aliases if only a subset of the nested fields are used.
   */
  def rewritePlanIfSubsetFieldsUsed(
      plan: LogicalPlan,
      exprList: Seq[Expression],
      exclusiveAttrs: Seq[Attribute]): Option[LogicalPlan] = {
    val attrToExtractValues = getAttributeToExtractValues(exprList, exclusiveAttrs)
    if (attrToExtractValues.isEmpty) {
      None
    } else {
      Some(rewritePlanWithAliases(plan, attrToExtractValues))
    }
  }

  /**
   * Replace nested columns to prune unused nested columns later.
   */
  def rewritePlanWithAliases(
      plan: LogicalPlan,
      attributeToExtractValues: Map[Attribute, Seq[ExtractValue]]): LogicalPlan = {
    // Each expression can contain multiple nested fields.
    // Note that we keep the original names to deliver to parquet in a case-sensitive way.
    // A new alias is created for each nested field.
    // Implementation detail: we don't use mapValues, because it creates a mutable view.
    val attributeToExtractValuesAndAliases =
      attributeToExtractValues.map { case (attr, evSeq) =>
        val evAliasSeq = evSeq.map { ev =>
          val fieldName = ev match {
            case g: GetStructField => g.extractFieldName
            case g: GetArrayStructFields => g.field.name
          }
          ev -> Alias(ev, s"_extract_$fieldName")()
        }

        attr -> evAliasSeq
      }

    val nestedFieldToAlias = attributeToExtractValuesAndAliases.values.flatten
      .map { case (field, alias) => field.canonicalized -> alias }.toMap

    // A reference attribute can have multiple aliases for nested fields.
    val attrToAliases =
      AttributeMap(attributeToExtractValuesAndAliases.transform((_, v) => v.map(_._2)))

    plan match {
      case Project(projectList, child) =>
        Project(
          getNewProjectList(projectList, nestedFieldToAlias),
          replaceWithAliases(child, nestedFieldToAlias, attrToAliases))

      // The operators reaching here are already guarded by [[canPruneOn]].
      case other =>
        replaceWithAliases(other, nestedFieldToAlias, attrToAliases)
    }
  }

  /**
   * Replace the [[ExtractValue]]s in a project list with aliased attributes.
   */
  def getNewProjectList(
      projectList: Seq[NamedExpression],
      nestedFieldToAlias: Map[Expression, Alias]): Seq[NamedExpression] = {
    projectList.map(_.transform {
      case f: ExtractValue if nestedFieldToAlias.contains(f.canonicalized) =>
        nestedFieldToAlias(f.canonicalized).toAttribute
    }.asInstanceOf[NamedExpression])
  }

  /**
   * Replace the grandchildren of a plan with [[Project]]s of the nested fields as aliases,
   * and replace the [[ExtractValue]] expressions with aliased attributes.
   */
  def replaceWithAliases(
      plan: LogicalPlan,
      nestedFieldToAlias: Map[Expression, Alias],
      attrToAliases: AttributeMap[Seq[Alias]]): LogicalPlan = {
    plan.withNewChildren(plan.children.map { plan =>
      Project(plan.output.flatMap(a => attrToAliases.getOrElse(a, Seq(a))), plan)
    }).transformExpressions {
      case f: ExtractValue if nestedFieldToAlias.contains(f.canonicalized) =>
        nestedFieldToAlias(f.canonicalized).toAttribute
    }
  }

  /**
   * Returns true for operators on which we can prune nested columns.
   */
  private def canPruneOn(plan: LogicalPlan) = plan match {
    case _: Aggregate => true
    case _: Expand => true
    case _ => false
  }

  /**
   * Returns true for operators through which project can be pushed.
   */
  private def canProjectPushThrough(plan: LogicalPlan) = plan match {
    case _: GlobalLimit => true
    case _: LocalLimit => true
    case _: Repartition => true
    case _: Sample => true
    case _: RepartitionByExpression => true
    case _: RebalancePartitions => true
    case _: Join => true
    case _: Window => true
    case _: Sort => true
    case _ => false
  }

  private def canAlias(ev: Expression): Boolean = {
    // we can not alias the attr from lambda variable whose expr id is not available
    !ev.exists(_.isInstanceOf[NamedLambdaVariable]) && ev.references.size == 1
  }

  /**
   * Returns two types of expressions:
   * - Root references that are individually accessed
   * - [[GetStructField]] or [[GetArrayStructFields]] on top of other [[ExtractValue]]s
   *   or special expressions.
   */
  private def collectRootReferenceAndExtractValue(e: Expression): Seq[Expression] = e match {
    case _: AttributeReference => Seq(e)
    case GetStructField(_: ExtractValue | _: AttributeReference, _, _) if canAlias(e) => Seq(e)
    case GetArrayStructFields(_: MapValues |
                              _: MapKeys |
                              _: ExtractValue |
                              _: AttributeReference, _, _, _, _) if canAlias(e) => Seq(e)
    case es if es.children.nonEmpty => es.children.flatMap(collectRootReferenceAndExtractValue)
    case _ => Seq.empty
  }

  /**
   * Creates a map from root [[Attribute]]s to non-redundant nested [[ExtractValue]]s.
   * Nested field accessors of `exclusiveAttrs` are not considered in nested fields aliasing.
   */
  def getAttributeToExtractValues(
      exprList: Seq[Expression],
      exclusiveAttrs: Seq[Attribute],
      extractor: (Expression) => Seq[Expression] = collectRootReferenceAndExtractValue)
    : Map[Attribute, Seq[ExtractValue]] = {

    val nestedFieldReferences = new mutable.ArrayBuffer[ExtractValue]()
    val otherRootReferences = new mutable.ArrayBuffer[AttributeReference]()
    exprList.foreach { e =>
      extractor(e).foreach {
        case ev: ExtractValue => nestedFieldReferences.append(ev)
        case ar: AttributeReference => otherRootReferences.append(ar)
      }
    }
    val exclusiveAttrSet = AttributeSet(exclusiveAttrs ++ otherRootReferences)

    // Remove cosmetic variations when we group extractors by their references
    nestedFieldReferences
      .filter(!_.references.subsetOf(exclusiveAttrSet))
      .groupBy(_.references.head.canonicalized.asInstanceOf[Attribute])
      .flatMap { case (attr: Attribute, nestedFields: collection.Seq[ExtractValue]) =>

        // Check if `ExtractValue` expressions contain any aggregate functions in their tree. Those
        // that do should not have an alias generated as it can lead to pushing the aggregate down
        // into a projection.
        def containsAggregateFunction(ev: ExtractValue): Boolean =
          ev.exists(_.isInstanceOf[AggregateFunction])

        // Remove redundant [[ExtractValue]]s if they share the same parent nest field.
        // For example, when `a.b` and `a.b.c` are in project list, we only need to alias `a.b`.
        // Because `a.b` requires all of the inner fields of `b`, we cannot prune `a.b.c`.
        val dedupNestedFields = nestedFields.filter {
          // See [[collectExtractValue]]: we only need to deal with [[GetArrayStructFields]] and
          // [[GetStructField]]
          case e @ (_: GetStructField | _: GetArrayStructFields) =>
            val child = e.children.head
            nestedFields.forall(f => !child.exists(_.semanticEquals(f)))
          case _ => true
        }
          .distinct
          // Discard [[ExtractValue]]s that contain aggregate functions.
          .filterNot(containsAggregateFunction)

        // If all nested fields of `attr` are used, we don't need to introduce new aliases.
        // By default, the [[ColumnPruning]] rule uses `attr` already.
        // Note that we need to remove cosmetic variations first, so we only count a
        // nested field once.
        val numUsedNestedFields = dedupNestedFields.map(_.canonicalized).distinct
          .map { nestedField => totalFieldNum(nestedField.dataType) }.sum
        if (dedupNestedFields.nonEmpty && numUsedNestedFields < totalFieldNum(attr.dataType)) {
          Some((attr, dedupNestedFields.toSeq))
        } else {
          None
        }
      }
  }

  /**
   * Return total number of fields of this type. This is used as a threshold to use nested column
   * pruning. It's okay to underestimate. If the number of reference is bigger than this, the parent
   * reference is used instead of nested field references.
   */
  private def totalFieldNum(dataType: DataType): Int = dataType match {
    case _: AtomicType => 1
    case StructType(fields) => fields.map(f => totalFieldNum(f.dataType)).sum
    case ArrayType(elementType, _) => totalFieldNum(elementType)
    case MapType(keyType, valueType, _) => totalFieldNum(keyType) + totalFieldNum(valueType)
    case _ => 1 // UDT and others
  }
}

/**
 * This prunes unnecessary nested columns from [[Generate]], or [[Project]] -> [[Generate]]
 */
object GeneratorNestedColumnAliasing {
  def unapply(plan: LogicalPlan): Option[LogicalPlan] = plan match {
    // Either `nestedPruningOnExpressions` or `nestedSchemaPruningEnabled` is enabled, we
    // need to prune nested columns through Project and under Generate. The difference is
    // when `nestedSchemaPruningEnabled` is on, nested columns will be pruned further at
    // file format readers if it is supported.

    // There are [[ExtractValue]] expressions on or not on the output of the generator. Generator
    // can also have different types:
    // 1. For [[ExtractValue]]s not on the output of the generator, theoretically speaking, there
    //    lots of expressions that we can push down, including non ExtractValues and GetArrayItem
    //    and GetMapValue. But to be safe, we only handle GetStructField and GetArrayStructFields.
    // 2. For [[ExtractValue]]s on the output of the generator, the situation depends on the type
    //    of the generator expression. *For now, we only support Explode*.
    //   2.1 Inline
    //       Inline takes an input of ARRAY<STRUCT<field1, field2>>, and returns an output of
    //       STRUCT<field1, field2>, the output field can be directly accessed by name "field1".
    //       In this case, we should not try to push down the ExtractValue expressions to the
    //       input of the Inline. For example:
    //       Project[field1.x AS x]
    //       - Generate[ARRAY<STRUCT<field1: STRUCT<x: int>, field2:int>>, ..., field1, field2]
    //       It is incorrect to push down the .x to the input of the Inline.
    //       A valid field pruning would be to extract all the fields that are accessed by the
    //       Project, and manually reconstruct an expression using those fields.
    //   2.2 Explode
    //       Explode takes an input of ARRAY<some_type> and returns an output of
    //       STRUCT<col: some_type>. The default field name "col" can be overwritten.
    //       If the input is MAP<key, value>, it returns STRUCT<key: key_type, value: value_type>.
    //       For the array case, it is only valid to push down GetStructField. After push down,
    //       the GetStructField becomes a GetArrayStructFields. Note that we cannot push down
    //       GetArrayStructFields, since the pushed down expression will operate on an array of
    //       array which is invalid.
    //   2.3 Stack
    //       Stack takes a sequence of expressions, and returns an output of
    //       STRUCT<col0: some_type, col1: some_type, ...>
    //       The push down is doable but more complicated in this case as the expression that
    //       operates on the col_i of the output needs to pushed down to every (kn+i)-th input
    //       expression where n is the total number of columns (or struct fields) of the output.
    case Project(projectList, g: Generate) if (SQLConf.get.nestedPruningOnExpressions ||
        SQLConf.get.nestedSchemaPruningEnabled) && canPruneGenerator(g.generator) =>
      // On top on `Generate`, a `Project` that might have nested column accessors.
      // We try to get alias maps for both project list and generator's children expressions.
      val attrToExtractValues = NestedColumnAliasing.getAttributeToExtractValues(
        projectList ++ g.generator.children, Seq.empty)
      if (attrToExtractValues.isEmpty) {
        return None
      }
      val generatorOutputSet = AttributeSet(g.qualifiedGeneratorOutput)
      var (attrToExtractValuesOnGenerator, attrToExtractValuesNotOnGenerator) =
        attrToExtractValues.partition { case (attr, _) =>
          attr.references.subsetOf(generatorOutputSet) }

      val pushedThrough = NestedColumnAliasing.rewritePlanWithAliases(
        plan, attrToExtractValuesNotOnGenerator)

      // We cannot push through if the child of generator is `MapType`.
      g.generator.children.head.dataType match {
        case _: MapType => return Some(pushedThrough)
        case ArrayType(_: ArrayType, _) => return Some(pushedThrough)
        case _ =>
      }

      if (!g.generator.isInstanceOf[ExplodeBase]) {
        return Some(pushedThrough)
      }

      // This function collects all GetStructField*(attribute) from the passed in expression.
      // GetStructField* means arbitrary levels of nesting.
      def collectNestedGetStructFields(e: Expression): Seq[Expression] = {
        // The helper function returns a tuple of
        // (nested GetStructField including the current level, all other nested GetStructField)
        def helper(e: Expression): (Seq[Expression], Seq[Expression]) = e match {
          case _: AttributeReference => (Seq(e), Seq.empty)
          case gsf: GetStructField =>
            val child_res = helper(gsf.child)
            (child_res._1.map(p => gsf.withNewChildren(Seq(p))), child_res._2)
          case other =>
            val child_res = other.children.map(helper)
            val child_res_combined = (child_res.flatMap(_._1), child_res.flatMap(_._2))
            (Seq.empty, child_res_combined._1 ++ child_res_combined._2)
        }

        val res = helper(e)
        (res._1 ++ res._2).filterNot(_.isInstanceOf[Attribute])
      }

      attrToExtractValuesOnGenerator = NestedColumnAliasing.getAttributeToExtractValues(
        attrToExtractValuesOnGenerator.flatMap(_._2).toSeq, Seq.empty,
        collectNestedGetStructFields)

      // Pruning on `Generator`'s output. We only process single field case.
      // For multiple field case, we cannot directly move field extractor into
      // the generator expression. A workaround is to re-construct array of struct
      // from multiple fields. But it will be more complicated and may not worth.
      // TODO(SPARK-34956): support multiple fields.
      val nestedFieldsOnGenerator = attrToExtractValuesOnGenerator.values.flatten.toSet
      if (nestedFieldsOnGenerator.size > 1 || nestedFieldsOnGenerator.isEmpty) {
        Some(pushedThrough)
      } else {
        // Only one nested column accessor.
        // E.g., df.select(explode($"items").as("item")).select($"item.a")
        val nestedFieldOnGenerator = nestedFieldsOnGenerator.head
        pushedThrough match {
          case p @ Project(_, newG: Generate) =>
            // Replace the child expression of `ExplodeBase` generator with
            // nested column accessor.
            // E.g., df.select(explode($"items").as("item")).select($"item.a") =>
            //       df.select(explode($"items.a").as("item.a"))
            val rewrittenG = newG.transformExpressions {
              case e: ExplodeBase =>
                val extractor = replaceGenerator(e, nestedFieldOnGenerator)
                e.withNewChildren(Seq(extractor))
            }

            // As we change the child of the generator, its output data type must be updated.
            val updatedGeneratorOutput = rewrittenG.generatorOutput
              .zip(toAttributes(rewrittenG.generator.elementSchema))
              .map { case (oldAttr, newAttr) =>
                newAttr.withExprId(oldAttr.exprId).withName(oldAttr.name)
              }
            assert(updatedGeneratorOutput.length == rewrittenG.generatorOutput.length,
              "Updated generator output must have the same length " +
                "with original generator output.")
            val updatedGenerate = rewrittenG.copy(generatorOutput = updatedGeneratorOutput)

            // Replace nested column accessor with generator output.
            val attrExprIdsOnGenerator = attrToExtractValuesOnGenerator.keys.map(_.exprId).toSet
            val updatedProject = p.withNewChildren(Seq(updatedGenerate)).transformExpressions {
              case f: GetStructField if nestedFieldsOnGenerator.contains(f) =>
                updatedGenerate.output
                  .find(a => attrExprIdsOnGenerator.contains(a.exprId))
                  .getOrElse(f)
            }
            Some(updatedProject)

          case other =>
            // We should not reach here.
            throw SparkException.internalError(s"Unreasonable plan after optimization: $other")
        }
      }

    case g: Generate if SQLConf.get.nestedSchemaPruningEnabled &&
      canPruneGenerator(g.generator) =>
      // If any child output is required by higher projection, we cannot prune on it even we
      // only use part of nested column of it. A required child output means it is referred
      // as a whole or partially by higher projection, pruning it here will cause unresolved
      // query plan.
      NestedColumnAliasing.rewritePlanIfSubsetFieldsUsed(
        plan, g.generator.children, g.requiredChildOutput)

    case _ =>
      None
  }

  /**
   * Replace the reference attribute of extractor expression with generator input.
   */
  private def replaceGenerator(generator: ExplodeBase, expr: Expression): Expression = {
    expr match {
      case a: Attribute if expr.references.contains(a) =>
        generator.child
      case g: GetStructField =>
        // We cannot simply do a transformUp instead because if we replace the attribute
        // `extractFieldName` could cause `ClassCastException` error. We need to get the
        // field name before replacing down the attribute/other extractor.
        val fieldName = g.extractFieldName
        val newChild = replaceGenerator(generator, g.child)
        ExtractValue(newChild, Literal(fieldName), SQLConf.get.resolver)
      case other =>
        other.mapChildren(replaceGenerator(generator, _))
    }
  }

  /**
   * Types of [[Generator]] on which we can prune nested fields.
   */
  def canPruneGenerator(g: Generator): Boolean = g match {
    case _: Explode => true
    case _: Stack => true
    case _: PosExplode => true
    case _: Inline => true
    case _ => false
  }
}
