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

  /**
   * Analyze the project list above a Generate to find nested array field accesses.
   *
   * For example, if projectList contains: request.servedItems.clicked
   * where request is the output of explode(pv_requests), this returns:
   *   Map("servedItems" -> Set("clicked"))
   *
   * This tells us that when we create the ArrayTransform for pv_requests, we should
   * create a nested ArrayTransform for the servedItems field to prune it to just clicked.
   */
  private def collectNestedArrayFieldAccesses(
      projectList: Seq[Expression],
      generatorOutput: Seq[Attribute]): Map[String, Set[String]] = {
    val accesses = collection.mutable.Map[String, Set[String]]()
    val generatorOutputSet = AttributeSet(generatorOutput)

    // scalastyle:off println
    println(s"[DEBUG] collectNestedArrayFieldAccesses called")
    println(s"[DEBUG] generatorOutput: ${generatorOutput.mkString(", ")}")
    println(s"[DEBUG] projectList size: ${projectList.size}")
    // scalastyle:on println

    projectList.foreach { expr =>
      // scalastyle:off println
      println(s"[DEBUG] Examining expr: ${expr.getClass.getSimpleName} - $expr")
      // scalastyle:on println

      expr.foreach {
        // Pattern: genOutput.arrayField.nestedField
        // Example: request.servedItems.clicked
        // where request is from the generator output
        case gasf @ GetArrayStructFields(
            gsf @ GetStructField(attr: Attribute, _, Some(arrayFieldName)),
            field, _, _, _)
            if generatorOutputSet.contains(attr) =>

          // scalastyle:off println
          println(s"[DEBUG] FOUND GetArrayStructFields!")
          println(s"[DEBUG]   attr: $attr")
          println(s"[DEBUG]   arrayFieldName: $arrayFieldName")
          println(s"[DEBUG]   field: ${field.name}")
          // scalastyle:on println

          val currentFields = accesses.getOrElse(arrayFieldName, Set.empty)
          accesses(arrayFieldName) = currentFields + field.name

        case _ =>
      }
    }

    // scalastyle:off println
    println(s"[DEBUG] Collected accesses: $accesses")
    // scalastyle:on println

    accesses.toMap
  }

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
    case p @ Project(projectList, g: Generate) if (SQLConf.get.nestedPruningOnExpressions ||
        SQLConf.get.nestedSchemaPruningEnabled) && canPruneGenerator(g.generator) =>

      // PHASE 1: Analyze the project list to find nested array field accesses
      // Example: if projectList contains request.servedItems.clicked,
      // this returns Map("servedItems" -> Set("clicked"))
      val nestedArrayFieldAccesses = collectNestedArrayFieldAccesses(
        projectList, g.qualifiedGeneratorOutput)

      // DEBUG: Log what we found
      // scalastyle:off println
      if (nestedArrayFieldAccesses.nonEmpty) {
        println(s"[DEBUG] Found nested array field accesses: $nestedArrayFieldAccesses")
      }
      // scalastyle:on println
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

      // Enhanced pruning on `Generator`'s output supporting multiple fields.
      // For multiple field case, we reconstruct the struct with only the needed fields
      // and update the explode operation to work on this pruned struct.
      val nestedFieldsOnGenerator = attrToExtractValuesOnGenerator.values.flatten.toSet

      // scalastyle:off println
      println(s"[DEBUG] nestedFieldsOnGenerator size: ${nestedFieldsOnGenerator.size}")
      println(s"[DEBUG] nestedFieldsOnGenerator: $nestedFieldsOnGenerator")
      println(s"[DEBUG] nestedArrayFieldAccesses: $nestedArrayFieldAccesses")
      // scalastyle:on println

      if (nestedFieldsOnGenerator.isEmpty) {
        // scalastyle:off println
        println(s"[DEBUG] Taking EMPTY path")
        // scalastyle:on println
        Some(pushedThrough)
      } else if (nestedFieldsOnGenerator.size == 1) {
        // scalastyle:off println
        println(s"[DEBUG] Taking SINGLE FIELD path")
        // scalastyle:on println
        // Single field optimization (existing logic)
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
            throw new IllegalStateException(s"Unreasonable plan after optimization: $other")
        }
      } else {
        // scalastyle:off println
        println(s"[DEBUG] Taking MULTI-FIELD path (size=${nestedFieldsOnGenerator.size})")
        // scalastyle:on println
        // Multi-field case: Use ArrayTransform to create a pruned struct
        pushedThrough match {
          case p @ Project(_, newG: Generate) =>
            val arrayExpr = newG.generator.asInstanceOf[ExplodeBase].child
            arrayExpr.dataType match {
              case ArrayType(st: StructType, containsNull) =>
                // Collect only direct struct field accesses on the generator output
                // (not deeply nested ones like item.level1.level2.field)
                val generatorOutputAttrs = AttributeSet(newG.output)
                val directFieldAccesses = nestedFieldsOnGenerator.collect {
                  case gsf @ GetStructField(attr: Attribute, ordinal, _)
                      if generatorOutputAttrs.contains(attr) && ordinal < st.fields.length =>
                    (gsf.ordinal, st.fields(gsf.ordinal))
                }.toSeq.sortBy(_._1)

                // scalastyle:off println
                println(s"[DEBUG] directFieldAccesses size: ${directFieldAccesses.size}")
                println(s"[DEBUG] directFieldAccesses: " +
                  s"${directFieldAccesses.map(_._2.name).mkString(", ")}")
                println(s"[DEBUG] st.fields.length: ${st.fields.length}")
                // scalastyle:on println

                // Only optimize if we have direct field accesses and not all fields
                // Deep nesting should be handled by applying NestedColumnAliasing on pushedThrough
                // EXCEPTION: If we have nested array field accesses, we should proceed even if
                // all top-level fields are accessed, because we still need to prune nested arrays
                val hasNestedArrayAccesses = nestedArrayFieldAccesses.nonEmpty

                if (directFieldAccesses.isEmpty) {
                  // scalastyle:off println
                  println(s"[DEBUG] EARLY RETURN: directFieldAccesses is empty")
                  // scalastyle:on println
                  // No direct field accesses - have deeply nested access
                  // The pushedThrough plan already has non-generator fields optimized
                  // For generator output with deep nesting, return pushedThrough
                  // to allow it to be handled by subsequent optimizer passes
                  Some(pushedThrough)
                } else if (directFieldAccesses.length == st.fields.length &&
                           !hasNestedArrayAccesses) {
                  // scalastyle:off println
                  println(s"[DEBUG] EARLY RETURN: all fields accessed " +
                    s"(${directFieldAccesses.length}) and no nested array accesses")
                  // scalastyle:on println
                  // All fields accessed - no optimization needed
                  Some(pushedThrough)
                } else {
                  // Build ordinal mapping: original ordinal -> pruned ordinal
                  val ordinalMap = directFieldAccesses.zipWithIndex.map {
                    case ((origOrdinal, _), prunedOrdinal) => origOrdinal -> prunedOrdinal
                  }.toMap

                  // Create ArrayTransform that builds pruned struct
                  // transform(array, x -> named_struct('f1', x.f1, 'f2', x.f2))
                  val elementVar = NamedLambdaVariable("_gen_c", st, nullable = containsNull)

                  // PHASE 2: Use the collected nested array field accesses
                  // nestedArrayFieldAccesses was collected from the projectList in PHASE 1
                  // It maps field names (like "servedItems") to the set of nested fields
                  // accessed (like Set("clicked"))
                  // We use this to create nested ArrayTransforms for array fields

                  // scalastyle:off println
                  val structExprs = directFieldAccesses.flatMap { case (ordinal, field) =>
                    // Check if this field is a nested array with field accesses
                    println(s"[DEBUG] Processing field: ${field.name}, " +
                      s"isArray: ${field.dataType.isInstanceOf[ArrayType]}, " +
                      s"hasNestedAccess: ${nestedArrayFieldAccesses.contains(field.name)}")
                    nestedArrayFieldAccesses.get(field.name) match {
                      case Some(nestedFields) if field.dataType.isInstanceOf[ArrayType] =>
                        println(s"[DEBUG] Creating nested transform for ${field.name} " +
                          s"with fields: $nestedFields")
                        // This field is an array with nested field accesses
                        // Create nested transform
                        field.dataType match {
                          case ArrayType(nestedSt: StructType, nestedContainsNull) =>
                            // Create nested lambda variable
                            val nestedElementVar = NamedLambdaVariable("_nested_elem",
                              nestedSt, nullable = nestedContainsNull)

                            // Create nested struct expressions for accessed fields only
                            val nestedStructExprs = nestedFields.toSeq.flatMap { nestedFieldName =>
                              // scalastyle:off println
                              println(s"[DEBUG] Looking for nested field: $nestedFieldName")
                              // scalastyle:on println
                              nestedSt.fields.zipWithIndex
                                .find(_._1.name == nestedFieldName)
                                .map { case (nestedField, nestedOrdinal) =>
                                  // scalastyle:off println
                                  println(s"[DEBUG] Found nested field at ordinal $nestedOrdinal")
                                  // scalastyle:on println
                                  Seq(
                                    Literal(nestedFieldName),
                                    GetStructField(nestedElementVar, nestedOrdinal,
                                      Some(nestedFieldName))
                                  )
                                }.getOrElse(Seq.empty)
                            }

                            // scalastyle:off println
                            println(s"[DEBUG] nestedStructExprs length: " +
                              s"${nestedStructExprs.length}")
                            // scalastyle:on println

                            val nestedNamedStruct = CreateNamedStruct(nestedStructExprs)
                            val nestedLambda = LambdaFunction(nestedNamedStruct,
                              Seq(nestedElementVar))
                            val nestedTransform = ArrayTransform(
                              GetStructField(elementVar, ordinal, Some(field.name)),
                              nestedLambda)

                            Seq(Literal(field.name), nestedTransform)

                          case _ =>
                            // Not a struct array, use regular field access
                            Seq(Literal(field.name),
                              GetStructField(elementVar, ordinal, Some(field.name)))
                        }

                      case _ =>
                        // No nested accesses or not an array field - regular field access
                        Seq(Literal(field.name),
                          GetStructField(elementVar, ordinal, Some(field.name)))
                    }
                  }
                  // scalastyle:on println

                  val namedStruct = CreateNamedStruct(structExprs)
                  val lambda = LambdaFunction(namedStruct, Seq(elementVar))
                  val transformedArray = ArrayTransform(arrayExpr, lambda)

                  // Update generator with the transformed array directly (no alias)
                  val rewrittenG = newG.transformExpressions {
                    case e: ExplodeBase if e.child == arrayExpr =>
                      e.withNewChildren(Seq(transformedArray))
                  }

                  // Update generator output data type
                  val updatedGeneratorOutput = rewrittenG.generatorOutput
                    .zip(toAttributes(rewrittenG.generator.elementSchema))
                    .map { case (oldAttr, newAttr) =>
                      newAttr.withExprId(oldAttr.exprId).withName(oldAttr.name)
                    }
                  val updatedGenerate = rewrittenG.copy(generatorOutput = updatedGeneratorOutput)

                  // Remap field ordinals in project list
                  val attrExprIdsOnGenerator =
                    attrToExtractValuesOnGenerator.keys.map(_.exprId).toSet
                  val updatedProject =
                    p.withNewChildren(Seq(updatedGenerate)).transformExpressions {
                    case gsf @ GetStructField(attr, ordinal, name)
                        if nestedFieldsOnGenerator.contains(gsf) =>
                      // Replace with generator output for single-field reference
                      // or remap ordinal for multi-field reference
                      ordinalMap.get(ordinal) match {
                        case Some(newOrdinal) =>
                          GetStructField(attr, newOrdinal, name)
                        case None =>
                          // This shouldn't happen if our logic is correct
                          gsf
                      }
                  }
                  Some(updatedProject)
                }
              case _ =>
                Some(pushedThrough)
            }
          case other =>
            Some(pushedThrough)
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
