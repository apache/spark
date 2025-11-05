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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
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

  /**
   * SPARK-47230: TreeNodeTag to track if generator nested column aliasing has been applied
   * to prevent infinite loops in fixedPoint batch optimization.
   */
  // SPARK-47230: Store iteration count to allow up to 10 transformations per plan node
  // Made private[sql] to allow V2ScanRelationPushDown to clear tags before transformation
  private[sql] val GENERATOR_ALIASING_APPLIED = TreeNodeTag[Int]("GENERATOR_ALIASING_APPLIED")

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

    case Project(projectList, child)
        if SQLConf.get.nestedSchemaPruningEnabled &&
          canProjectPushThrough(child) &&
          !hasExtractAliases(child) =>
      rewritePlanIfSubsetFieldsUsed(
        plan, projectList ++ child.expressions, child.producedAttributes.toSeq)

    case p if SQLConf.get.nestedSchemaPruningEnabled && canPruneOn(p) =>
      rewritePlanIfSubsetFieldsUsed(
        plan, p.expressions, p.producedAttributes.toSeq)

    case _ => None
  }

  def hasExtractAliases(plan: LogicalPlan): Boolean = plan match {
    case p: Project =>
      val tagged = p.getTagValue(GENERATOR_ALIASING_APPLIED).isDefined
      val aliasExists = p.projectList.exists {
        case Alias(child, name) if name.startsWith("_extract_") =>
          child.isInstanceOf[ExtractValue] || child.isInstanceOf[GetArrayStructFields]
        case _ => false
      }
      tagged || aliasExists
    case g: Generate =>
      // SPARK-47230: Check child of Generator for aliases/tag to prevent infinite loop
      hasExtractAliases(g.child)
    case _ => false
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
      AttributeMap(attributeToExtractValuesAndAliases.mapValues(_.map(_._2)))

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
   *
   * SPARK-47230: Added Generate to enable nested column aliasing for generator input columns.
   * This allows NestedColumnAliasing to create _extract_ aliases for fields accessed through
   * generators (EXPLODE, POSEXPLODE), enabling proper schema pruning in V2 datasources.
   *
   * Note: This complements GeneratorNestedColumnAliasing which handles field accesses on
   * generator outputs. This change handles field accesses on generator INPUTS (the array/map
   * being exploded), which is necessary for V2 datasource schema pruning to work correctly.
   *
   * Example:
   *   SELECT item.price FROM table LATERAL VIEW EXPLODE(items) AS item
   *
   *   Before: Generator input 'items' not aliased, full struct read
   *   After:  Creates _extract_ alias for items[*].price, only price field read
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
    case _: Generate => true  // SPARK-47230: Enable nested column aliasing for generator inputs
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
  private def generatorDebug(message: => String): Unit = {}

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
    case p @ Project(projectList, g: Generate) =>
      // DEBUG: Check each pattern match condition
      val configCheck =
        SQLConf.get.nestedPruningOnExpressions || SQLConf.get.nestedSchemaPruningEnabled
      val canPruneCheck = canPruneGenerator(g.generator)
      val hasAliasesCheck = NestedColumnAliasing.hasExtractAliases(g.child)
      // SPARK-47230: Allow up to 10 iterations per plan node
      val currentCount =
        p.getTagValue(NestedColumnAliasing.GENERATOR_ALIASING_APPLIED).getOrElse(0)
      val iterationCheck = currentCount < 10
      val aliasGuard = !hasAliasesCheck || currentCount == 0
      val allConditions =
        configCheck && canPruneCheck && aliasGuard && iterationCheck
      def dbg(msg: String): Unit = generatorDebug(s"[GENERATOR DEBUG] $msg")

      dbg("Pattern match for Project(Generate):")
      dbg(s"  Config enabled: $configCheck")
      dbg(
        s"  Can prune generator: $canPruneCheck (generator: " +
          s"${g.generator.getClass.getSimpleName})")
      dbg(s"  Has extract aliases: $hasAliasesCheck")
      dbg(s"  Iteration count: $currentCount (max 10): $iterationCheck")
      dbg(s"  Alias guard passed: $aliasGuard")
      dbg(s"  ALL CONDITIONS: $allConditions")

      if (!configCheck || !canPruneCheck || !aliasGuard || !iterationCheck) {
        dbg("Skipping transformation - conditions not met")
        return None
      }
      // On top on `Generate`, a `Project` that might have nested column accessors.
      // We try to get alias maps for both project list and generator's children expressions.
      val attrToExtractValues = NestedColumnAliasing.getAttributeToExtractValues(
        projectList ++ g.generator.children, Seq.empty)

      dbg(s"attrToExtractValues: ${attrToExtractValues.size} entries")
      dbg("Extract values details:")
      attrToExtractValues.foreach { case (attr, extractValues) =>
        dbg(s"  - Attr: ${attr.name} (${attr.getClass.getSimpleName})")
        extractValues.foreach(ev => dbg(s"    * ${ev.sql}"))
      }

      if (attrToExtractValues.isEmpty) {
        dbg("Skipping - no extract values found")
        return None
      }

      val generatorOutputSet = AttributeSet(g.qualifiedGeneratorOutput)
      val generatorOutputNames = g.qualifiedGeneratorOutput.map(_.name).mkString(", ")
      dbg(s"Generator output attributes: $generatorOutputNames")

      var (attrToExtractValuesOnGenerator, attrToExtractValuesNotOnGenerator) =
        attrToExtractValues.partition { case (attr, _) =>
          attr.references.subsetOf(generatorOutputSet) }

      dbg("Partition results:")
      dbg(s"  - On generator: ${attrToExtractValuesOnGenerator.size} attrs")
      dbg(s"  - Not on generator: ${attrToExtractValuesNotOnGenerator.size} attrs")

      val pushedThrough = NestedColumnAliasing.rewritePlanWithAliases(
        plan, attrToExtractValuesNotOnGenerator)

      dbg("After rewritePlanWithAliases:")
      dbg(s"  Plan type: ${pushedThrough.getClass.getSimpleName}")

      // We cannot push through if the child of generator is `MapType`.
      val generatorChildType = g.generator.children.head.dataType
      dbg(s"Checking generator child dataType: $generatorChildType")
      g.generator.children.head.dataType match {
        case _: MapType =>
          dbg("MapType - returning pushedThrough")
          return Some(pushedThrough)
        case ArrayType(_: ArrayType, _) =>
          dbg("ArrayType of ArrayType - returning pushedThrough")
          return Some(pushedThrough)
        case other =>
          dbg(s"Other type: ${other.getClass.getSimpleName}, continuing...")
      }

      dbg(
        s"Checking if generator is ExplodeBase: ${g.generator.isInstanceOf[ExplodeBase]}")
      dbg(s"Generator class: ${g.generator.getClass.getSimpleName}")
      if (!g.generator.isInstanceOf[ExplodeBase]) {
        dbg("Not ExplodeBase - returning pushedThrough")
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

      dbg("Before nested field collection:")
      dbg(
        s"  attrToExtractValuesOnGenerator (before): " +
          s"${attrToExtractValuesOnGenerator.size} entries")

      attrToExtractValuesOnGenerator = NestedColumnAliasing.getAttributeToExtractValues(
        attrToExtractValuesOnGenerator.flatMap(_._2).toSeq, Seq.empty,
        collectNestedGetStructFields)

      dbg("After nested field collection:")
      dbg(
        s"  attrToExtractValuesOnGenerator (after): " +
          s"${attrToExtractValuesOnGenerator.size} entries")
      attrToExtractValuesOnGenerator.foreach { case (attr, extractValues) =>
        dbg(s"    - Attr: ${attr.name}")
        extractValues.foreach(ev => dbg(s"      * ${ev.sql}"))
      }

      // SPARK-47230/SPARK-34956: Support multiple nested field pruning on Generator output.
      // For single field, we can push the field access directly into the generator.
      // For multiple fields, we create _extract_* aliases like we do for non-generator fields.
      val nestedFieldsOnGenerator = attrToExtractValuesOnGenerator.values.flatten.toSet
      dbg(s"nestedFieldsOnGenerator size: ${nestedFieldsOnGenerator.size}")
      nestedFieldsOnGenerator.foreach(field => dbg(s"  - Field: ${field.sql}"))

      if (nestedFieldsOnGenerator.isEmpty) {
        dbg("No nested fields on generator - returning pushedThrough")
        Some(pushedThrough)
      } else if (nestedFieldsOnGenerator.size == 1) {
        dbg("SINGLE nested field on generator - applying direct push")
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
            val newCount = currentCount + 1
            updatedProject.setTagValue(NestedColumnAliasing.GENERATOR_ALIASING_APPLIED, newCount)
            Some(updatedProject)

          case other =>
            // We should not reach here.
            throw new IllegalStateException(s"Unreasonable plan after optimization: $other")
        }
      } else {
        dbg("\n" + "=" * 80)
        dbg("=== SPARK-34956/47230: Multi-field Generator Handler ===")
        dbg("=" * 80)
        dbg(s"Number of nested fields on generator: ${nestedFieldsOnGenerator.size}")
        nestedFieldsOnGenerator.zipWithIndex.foreach { case (field, idx) =>
          dbg(s"  [$idx] ${field.getClass.getSimpleName}: $field")
        }
        dbg(s"\nPushed-through plan structure:")
        generatorDebug(pushedThrough.treeString)

        // SPARK-34956/SPARK-47230: Handle multiple nested fields on generator output.
        // For multiple fields, we create GetArrayStructFields on the generator INPUT
        // and add a projection below the generator with aliases.
        pushedThrough match {
          case p @ Project(projectList, newG: Generate) =>
            dbg("\n[OK] Matched Project -> Generate pattern")
            dbg(s"  Generator class: ${newG.generator.getClass.getSimpleName}")
            val newGeneratorOutput = newG.generatorOutput.map(_.name).mkString(", ")
            dbg(s"  Generator output: $newGeneratorOutput")

            newG.generator match {
              case explode: ExplodeBase =>
                dbg("\n[OK] Generator is ExplodeBase")
                val generatorInput = explode.child
                dbg(s"  Input expression: $generatorInput")
                dbg(s"  Input type: ${generatorInput.dataType}")

                generatorInput.dataType match {
                  case ArrayType(elementType: StructType, containsNull) =>
                    dbg("\n[OK] Generator input is Array[Struct]")
                    dbg(s"  Element type: $elementType")
                    val elementFieldSummary =
                      elementType.fields.map(f => s"${f.name}:${f.dataType}").mkString(", ")
                    dbg(s"  Element fields: $elementFieldSummary")
                    dbg(s"  Contains null: $containsNull")

                    // Check for existing _extract_ aliases to prevent infinite loop
                    // SPARK-47230: This check is now redundant with the guard at line 387
                    // but kept for backward compatibility
                    val existingExtractAliases = newG.child match {
                      case Project(list, _) =>
                        list.collect {
                          case Alias(child, name) if child.isInstanceOf[ExtractValue] &&
                                                     name.startsWith("_extract_") => name
                        }
                      case _ => Seq.empty
                    }

                    if (existingExtractAliases.nonEmpty) {
                      dbg("\n[WARN] CYCLE DETECTION: Already processed - found existing aliases:")
                      existingExtractAliases.foreach(name => dbg(s"    - $name"))
                      dbg("  Returning pushedThrough plan to prevent infinite loop")
                      Some(pushedThrough)
                    } else {
                      dbg("\n[OK] No existing _extract_ aliases - proceeding with transformation")

                      // Extract field names from nested field expressions
                      val fieldCount = nestedFieldsOnGenerator.size
                      dbg(
                        s"\nExtracting field names from $fieldCount " +
                          "expressions:")
                      val fieldNamesWithOrdinals = nestedFieldsOnGenerator.flatMap { expr =>
                        expr match {
                          case GetStructField(_, ordinal, Some(name)) =>
                            dbg(s"  [OK] GetStructField(ordinal=$ordinal, name=$name)")
                            Some((name, ordinal))
                          case GetStructField(_, ordinal, None) =>
                            // Try to get name from element type by ordinal
                            if (ordinal >= 0 &&
                                ordinal < elementType.fields.length) {
                              val name = elementType.fields(ordinal).name
                              dbg(
                                s"  [OK] GetStructField(ordinal=$ordinal) -> " +
                                  s"resolved name=$name")
                              Some((name, ordinal))
                            } else {
                              dbg(
                                s"  [ERROR] GetStructField(ordinal=$ordinal) -> " +
                                  "ordinal out of bounds!")
                              None
                            }
                          case other =>
                            dbg(
                              s"  [ERROR] Unexpected expression type: " +
                                s"${other.getClass.getSimpleName}")
                            dbg(s"      Expression: $other")
                            None
                        }
                      }

                      val extractedNames =
                        fieldNamesWithOrdinals.map(_._1).mkString(", ")
                      val extractedSize = fieldNamesWithOrdinals.size
                      dbg(s"\nExtracted $extractedSize field names: $extractedNames")

                      if (fieldNamesWithOrdinals.isEmpty) {
                        dbg("[ERROR] ERROR: No valid field names extracted!")
                        None
                      } else {
                        // Use ordinals directly to get struct fields. This avoids
                        // case sensitivity issues.
                        dbg(s"\nLooking up struct fields by ordinal:")
                        val structFieldsToExtract = fieldNamesWithOrdinals.flatMap {
                          case (fieldName, ordinal) =>
                            if (ordinal >= 0 && ordinal < elementType.fields.length) {
                              val field = elementType.fields(ordinal)
                              dbg(
                                s"  [OK] Ordinal $ordinal -> ${field.name}:${field.dataType} " +
                                  s"(accessed as '$fieldName')")
                              Some((field, ordinal))
                            } else {
                              val maxOrdinal = elementType.fields.length - 1
                              dbg(
                                s"  [ERROR] ERROR: Ordinal $ordinal out of bounds (0-$maxOrdinal)!")
                              dbg(s"      Field name was: $fieldName")
                              None
                            }
                        }

                        dbg(
                          s"\nSuccessfully mapped ${structFieldsToExtract.size}/" +
                            s"${fieldNamesWithOrdinals.size} fields")

                        if (structFieldsToExtract.size != fieldNamesWithOrdinals.size) {
                          dbg(
                            "[ERROR] ERROR: Some fields could not be mapped - " +
                              "aborting transformation")
                          None
                        } else {
                          dbg("\n[OK] All fields mapped successfully")

                          // Create GetArrayStructFields for each field
                          dbg("\nCreating GetArrayStructFields expressions:")
                          val arrayFieldExtractors = structFieldsToExtract.map {
                            case (field, ordinal) =>
                              val extractor = GetArrayStructFields(
                                generatorInput,
                                field,
                                ordinal,
                                field.dataType.defaultSize,
                                containsNull || field.nullable)
                              dbg(s"  [OK] Created for '${field.name}' (ordinal=$ordinal):")
                              dbg(s"      Input: $generatorInput")
                              dbg(s"      Field: ${field.name}:${field.dataType}")
                              dbg(s"      Nullable: ${containsNull || field.nullable}")
                              extractor
                          }

                          // Create aliases with _extract_ prefix
                          dbg("\nCreating alias expressions:")
                          val aliases = arrayFieldExtractors.map { extractor =>
                            val aliasName = s"_extract_${extractor.field.name}"
                            val alias = Alias(extractor, aliasName)()
                            dbg(s"  [OK] Alias: $aliasName")
                            alias
                          }

                          // Insert projection below generator with aliases
                          val projectionBelowGen = Project(
                            newG.child.output ++ aliases,
                            newG.child
                          )
                          projectionBelowGen.setTagValue(
                            NestedColumnAliasing.GENERATOR_ALIASING_APPLIED,
                            currentCount + 1)

                          dbg("\n[OK] Created projection below generator:")
                          val originalOutputs = newG.child.output.map(_.name).mkString(", ")
                          val aliasOutputs = aliases.map(_.name).mkString(", ")
                          val totalOutputs = projectionBelowGen.output.map(_.name).mkString(", ")
                          dbg(s"  Original child outputs: $originalOutputs")
                          dbg(s"  New alias outputs: $aliasOutputs")
                          dbg(s"  Total outputs: $totalOutputs")

                          // Update generator to use new child
                          val updatedGenerate = newG.copy(child = projectionBelowGen)

                          dbg("\n[OK] Updated generator with new child")

                          // Update top project
                          val result = p.withNewChildren(Seq(updatedGenerate))

                          // SPARK-47230: Increment iteration count (allow up to 10 transformations)
                          val newCount = currentCount + 1
                          dbg(
                            s"[OK] Transformation SUCCESSFUL - setting count to $newCount")
                          result.setTagValue(
                            NestedColumnAliasing.GENERATOR_ALIASING_APPLIED,
                            newCount)

                          dbg("\n[OK] Final transformation result:")
                          generatorDebug(result.treeString)
                          dbg(s"  [OK] GENERATOR_ALIASING_APPLIED count = $newCount")
                          dbg("=" * 80 + "\n")

                          Some(result)
                        }
                      }
                    }

                  case other =>
                    generatorDebug(s"\n[ERROR] ERROR: Generator input is not Array[Struct]")
                    generatorDebug(s"  Got: $other")
                    None
                }

              case other =>
                generatorDebug(s"\n[ERROR] ERROR: Generator is not ExplodeBase")
                generatorDebug(s"  Got: ${other.getClass.getSimpleName}")
                None
            }

          case other =>
            generatorDebug(s"\n[ERROR] ERROR: Unexpected plan structure")
            generatorDebug(s"  Expected: Project -> Generate")
            generatorDebug(s"  Got: ${other.getClass.getSimpleName}")
            generatorDebug(other.treeString)
            None
        }
      }

    case g: Generate if SQLConf.get.nestedSchemaPruningEnabled &&
      canPruneGenerator(g.generator) =>
      // SPARK-47230: Enable nested column aliasing for generator children.
      // We pass g.requiredChildOutput to preserve generator outputs while still
      // allowing nested field access aliasing where possible.
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

/**
 * SPARK-47230: Rule wrapper for GeneratorNestedColumnAliasing.
 *
 * This rule is intentionally separated from ColumnPruning and runs in its own batch
 * with Once strategy to avoid idempotence issues in fixedPoint batches where other
 * rules (like CollapseProject, RemoveRedundantAliases) might remove the _extract_
 * aliases created by this transformation, causing infinite loops.
 */
object GeneratorNestedColumnAliasingRule extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case GeneratorNestedColumnAliasing(rewrittenPlan) => rewrittenPlan
  }
}

/**
 * SPARK-47230: Special version for V2 that forces transformation even if aliases exist.
 * This is needed because V2 Phase 16 may see plans with pre-existing aliases
 * but still needs the transformation to create GetArrayStructFields.
 */
object GeneratorNestedColumnAliasingRuleForceApply extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case p @ Project(projectList, g: Generate)
        if (SQLConf.get.nestedPruningOnExpressions || SQLConf.get.nestedSchemaPruningEnabled) &&
           GeneratorNestedColumnAliasing.canPruneGenerator(g.generator) =>
      // Force apply without checking hasExtractAliases
      GeneratorNestedColumnAliasing.unapply(p).getOrElse(p)
  }
}
