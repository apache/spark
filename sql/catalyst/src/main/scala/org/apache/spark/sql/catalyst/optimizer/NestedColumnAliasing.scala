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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

/**
 * This aims to handle a nested column aliasing pattern inside the `ColumnPruning` optimizer rule.
 * If a project or its child references to nested fields, and not all the fields
 * in a nested attribute are used, we can substitute them by alias attributes; then a project
 * of the nested fields as aliases on the children of the child will be created.
 */
object NestedColumnAliasing {

  def unapply(plan: LogicalPlan): Option[LogicalPlan] = plan match {
    /**
     * This pattern is needed to support [[Filter]] plan cases like
     * [[Project]]->[[Filter]]->listed plan in `canProjectPushThrough` (e.g., [[Window]]).
     * The reason why we don't simply add [[Filter]] in `canProjectPushThrough` is that
     * the optimizer can hit an infinite loop during the [[PushDownPredicates]] rule.
     */
    case Project(projectList, Filter(condition, child))
        if SQLConf.get.nestedSchemaPruningEnabled && canProjectPushThrough(child) =>
      val exprCandidatesToPrune = projectList ++ Seq(condition) ++ child.expressions
      getAliasSubMap(exprCandidatesToPrune, child.producedAttributes.toSeq).map {
        case (nestedFieldToAlias, attrToAliases) =>
          NestedColumnAliasing.replaceToAliases(plan, nestedFieldToAlias, attrToAliases)
      }

    case Project(projectList, child)
        if SQLConf.get.nestedSchemaPruningEnabled && canProjectPushThrough(child) =>
      val exprCandidatesToPrune = projectList ++ child.expressions
      getAliasSubMap(exprCandidatesToPrune, child.producedAttributes.toSeq).map {
        case (nestedFieldToAlias, attrToAliases) =>
          NestedColumnAliasing.replaceToAliases(plan, nestedFieldToAlias, attrToAliases)
      }

    case p if SQLConf.get.nestedSchemaPruningEnabled && canPruneOn(p) =>
      val exprCandidatesToPrune = p.expressions
      getAliasSubMap(exprCandidatesToPrune, p.producedAttributes.toSeq).map {
        case (nestedFieldToAlias, attrToAliases) =>
          NestedColumnAliasing.replaceToAliases(p, nestedFieldToAlias, attrToAliases)
      }

    case _ => None
  }

  /**
   * Replace nested columns to prune unused nested columns later.
   */
  private def replaceToAliases(
      plan: LogicalPlan,
      nestedFieldToAlias: Map[ExtractValue, Alias],
      attrToAliases: Map[ExprId, Seq[Alias]]): LogicalPlan = plan match {
    case Project(projectList, child) =>
      Project(
        getNewProjectList(projectList, nestedFieldToAlias),
        replaceWithAliases(child, nestedFieldToAlias, attrToAliases))

    // The operators reaching here was already guarded by `canPruneOn`.
    case other =>
      replaceWithAliases(other, nestedFieldToAlias, attrToAliases)
  }

  /**
   * Return a replaced project list.
   */
  def getNewProjectList(
      projectList: Seq[NamedExpression],
      nestedFieldToAlias: Map[ExtractValue, Alias]): Seq[NamedExpression] = {
    projectList.map(_.transform {
      case f: ExtractValue if nestedFieldToAlias.contains(f) =>
        nestedFieldToAlias(f).toAttribute
    }.asInstanceOf[NamedExpression])
  }

  /**
   * Return a plan with new children replaced with aliases, and expressions replaced with
   * aliased attributes.
   */
  def replaceWithAliases(
      plan: LogicalPlan,
      nestedFieldToAlias: Map[ExtractValue, Alias],
      attrToAliases: Map[ExprId, Seq[Alias]]): LogicalPlan = {
    plan.withNewChildren(plan.children.map { plan =>
      Project(plan.output.flatMap(a => attrToAliases.getOrElse(a.exprId, Seq(a))), plan)
    }).transformExpressions {
      case f: ExtractValue if nestedFieldToAlias.contains(f) =>
        nestedFieldToAlias(f).toAttribute
    }
  }

  /**
   * Returns true for those operators that we can prune nested column on it.
   */
  private def canPruneOn(plan: LogicalPlan) = plan match {
    case _: Aggregate => true
    case _: Expand => true
    case _ => false
  }

  /**
   * Returns true for those operators that project can be pushed through.
   */
  private def canProjectPushThrough(plan: LogicalPlan) = plan match {
    case _: GlobalLimit => true
    case _: LocalLimit => true
    case _: Repartition => true
    case _: Sample => true
    case _: RepartitionByExpression => true
    case _: Join => true
    case _: Window => true
    case _: Sort => true
    case _ => false
  }

  /**
   * Return root references that are individually accessed as a whole, and `GetStructField`s
   * or `GetArrayStructField`s which on top of other `ExtractValue`s or special expressions.
   * Check `SelectedField` to see which expressions should be listed here.
   */
  private def collectRootReferenceAndExtractValue(e: Expression): Seq[Expression] = e match {
    case _: AttributeReference => Seq(e)
    case GetStructField(_: ExtractValue | _: AttributeReference, _, _) => Seq(e)
    case GetArrayStructFields(_: MapValues |
                              _: MapKeys |
                              _: ExtractValue |
                              _: AttributeReference, _, _, _, _) => Seq(e)
    case es if es.children.nonEmpty => es.children.flatMap(collectRootReferenceAndExtractValue)
    case _ => Seq.empty
  }

  /**
   * Return two maps in order to replace nested fields to aliases.
   *
   * If `exclusiveAttrs` is given, any nested field accessors of these attributes
   * won't be considered in nested fields aliasing.
   *
   * 1. ExtractValue -> Alias: A new alias is created for each nested field.
   * 2. ExprId -> Seq[Alias]: A reference attribute has multiple aliases pointing it.
   */
  def getAliasSubMap(exprList: Seq[Expression], exclusiveAttrs: Seq[Attribute] = Seq.empty)
    : Option[(Map[ExtractValue, Alias], Map[ExprId, Seq[Alias]])] = {
    val (nestedFieldReferences, otherRootReferences) =
      exprList.flatMap(collectRootReferenceAndExtractValue).partition {
        case _: ExtractValue => true
        case _ => false
      }

    // Note that when we group by extractors with their references, we should remove
    // cosmetic variations.
    val exclusiveAttrSet = AttributeSet(exclusiveAttrs ++ otherRootReferences)
    val aliasSub = nestedFieldReferences.asInstanceOf[Seq[ExtractValue]]
      .filter(!_.references.subsetOf(exclusiveAttrSet))
      .groupBy(_.references.head.canonicalized.asInstanceOf[Attribute])
      .flatMap { case (attr, nestedFields: Seq[ExtractValue]) =>
        // Remove redundant `ExtractValue`s if they share the same parent nest field.
        // For example, when `a.b` and `a.b.c` are in project list, we only need to alias `a.b`.
        // We only need to deal with two `ExtractValue`: `GetArrayStructFields` and
        // `GetStructField`. Please refer to the method `collectRootReferenceAndExtractValue`.
        val dedupNestedFields = nestedFields.filter {
          case e @ (_: GetStructField | _: GetArrayStructFields) =>
            val child = e.children.head
            nestedFields.forall(f => child.find(_.semanticEquals(f)).isEmpty)
          case _ => true
        }

        // Each expression can contain multiple nested fields.
        // Note that we keep the original names to deliver to parquet in a case-sensitive way.
        val nestedFieldToAlias = dedupNestedFields.distinct.map { f =>
          val exprId = NamedExpression.newExprId
          (f, Alias(f, s"_gen_alias_${exprId.id}")(exprId, Seq.empty, None))
        }

        // If all nested fields of `attr` are used, we don't need to introduce new aliases.
        // By default, ColumnPruning rule uses `attr` already.
        // Note that we need to remove cosmetic variations first, so we only count a
        // nested field once.
        if (nestedFieldToAlias.nonEmpty &&
            dedupNestedFields.map(_.canonicalized)
              .distinct
              .map { nestedField => totalFieldNum(nestedField.dataType) }
              .sum < totalFieldNum(attr.dataType)) {
          Some(attr.exprId -> nestedFieldToAlias)
        } else {
          None
        }
      }

    if (aliasSub.isEmpty) {
      None
    } else {
      Some((aliasSub.values.flatten.toMap, aliasSub.map(x => (x._1, x._2.map(_._2)))))
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
 * This prunes unnecessary nested columns from `Generate` and optional `Project` on top
 * of it.
 */
object GeneratorNestedColumnAliasing {
  // Partitions `attrToAliases` based on whether the attribute is in Generator's output.
  private def aliasesOnGeneratorOutput(
      attrToAliases: Map[ExprId, Seq[Alias]],
      generatorOutput: Seq[Attribute]) = {
    val generatorOutputExprId = generatorOutput.map(_.exprId)
    attrToAliases.partition { k =>
      generatorOutputExprId.contains(k._1)
    }
  }

  // Partitions `nestedFieldToAlias` based on whether the attribute of nested field extractor
  // is in Generator's output.
  private def nestedFieldOnGeneratorOutput(
      nestedFieldToAlias: Map[ExtractValue, Alias],
      generatorOutput: Seq[Attribute]) = {
    val generatorOutputSet = AttributeSet(generatorOutput)
    nestedFieldToAlias.partition { pair =>
      pair._1.references.subsetOf(generatorOutputSet)
    }
  }

  def unapply(plan: LogicalPlan): Option[LogicalPlan] = plan match {
    // Either `nestedPruningOnExpressions` or `nestedSchemaPruningEnabled` is enabled, we
    // need to prune nested columns through Project and under Generate. The difference is
    // when `nestedSchemaPruningEnabled` is on, nested columns will be pruned further at
    // file format readers if it is supported.
    case Project(projectList, g: Generate) if (SQLConf.get.nestedPruningOnExpressions ||
        SQLConf.get.nestedSchemaPruningEnabled) && canPruneGenerator(g.generator) =>
      // On top on `Generate`, a `Project` that might have nested column accessors.
      // We try to get alias maps for both project list and generator's children expressions.
      val exprsToPrune = projectList ++ g.generator.children
      NestedColumnAliasing.getAliasSubMap(exprsToPrune).map {
        case (nestedFieldToAlias, attrToAliases) =>
          val (nestedFieldsOnGenerator, nestedFieldsNotOnGenerator) =
            nestedFieldOnGeneratorOutput(nestedFieldToAlias, g.qualifiedGeneratorOutput)
          val (attrToAliasesOnGenerator, attrToAliasesNotOnGenerator) =
            aliasesOnGeneratorOutput(attrToAliases, g.qualifiedGeneratorOutput)

          // Push nested column accessors through `Generator`.
          // Defer updating `Generate.unrequiredChildIndex` to next round of `ColumnPruning`.
          val newChild = NestedColumnAliasing.replaceWithAliases(g,
            nestedFieldsNotOnGenerator, attrToAliasesNotOnGenerator)
          val pushedThrough = Project(NestedColumnAliasing
            .getNewProjectList(projectList, nestedFieldsNotOnGenerator), newChild)

          // If the generator output is `ArrayType`, we cannot push through the extractor.
          // It is because we don't allow field extractor on two-level array,
          // i.e., attr.field when attr is a ArrayType(ArrayType(...)).
          // Similarily, we also cannot push through if the child of generator is `MapType`.
          g.generator.children.head.dataType match {
            case _: MapType => return Some(pushedThrough)
            case ArrayType(_: ArrayType, _) => return Some(pushedThrough)
            case _ =>
          }

          // Pruning on `Generator`'s output. We only process single field case.
          // For multiple field case, we cannot directly move field extractor into
          // the generator expression. A workaround is to re-construct array of struct
          // from multiple fields. But it will be more complicated and may not worth.
          // TODO(SPARK-34956): support multiple fields.
          if (nestedFieldsOnGenerator.size > 1 || nestedFieldsOnGenerator.isEmpty) {
            pushedThrough
          } else {
            // Only one nested column accessor.
            // E.g., df.select(explode($"items").as("item")).select($"item.a")
            pushedThrough match {
              case p @ Project(_, newG: Generate) =>
                // Replace the child expression of `ExplodeBase` generator with
                // nested column accessor.
                // E.g., df.select(explode($"items").as("item")).select($"item.a") =>
                //       df.select(explode($"items.a").as("item.a"))
                val rewrittenG = newG.transformExpressions {
                  case e: ExplodeBase =>
                    val extractor = nestedFieldsOnGenerator.head._1.transformUp {
                      case _: Attribute =>
                        e.child
                      case g: GetStructField =>
                        ExtractValue(g.child, Literal(g.extractFieldName), SQLConf.get.resolver)
                    }
                    e.withNewChildren(Seq(extractor))
                }

                // As we change the child of the generator, its output data type must be updated.
                val updatedGeneratorOutput = rewrittenG.generatorOutput
                  .zip(rewrittenG.generator.elementSchema.toAttributes)
                  .map { case (oldAttr, newAttr) =>
                  newAttr.withExprId(oldAttr.exprId).withName(oldAttr.name)
                }
                assert(updatedGeneratorOutput.length == rewrittenG.generatorOutput.length,
                  "Updated generator output must have the same length " +
                    "with original generator output.")
                val updatedGenerate = rewrittenG.copy(generatorOutput = updatedGeneratorOutput)

                // Replace nested column accessor with generator output.
                p.withNewChildren(Seq(updatedGenerate)).transformExpressions {
                  case f: ExtractValue if nestedFieldsOnGenerator.contains(f) =>
                    updatedGenerate.output
                      .find(a => attrToAliasesOnGenerator.contains(a.exprId))
                      .getOrElse(f)
                }

              case other =>
                // We should not reach here.
                throw new IllegalStateException(s"Unreasonable plan after optimization: $other")
            }
          }
      }

    case g: Generate if SQLConf.get.nestedSchemaPruningEnabled &&
        canPruneGenerator(g.generator) =>
      // If any child output is required by higher projection, we cannot prune on it even we
      // only use part of nested column of it. A required child output means it is referred
      // as a whole or partially by higher projection, pruning it here will cause unresolved
      // query plan.
      NestedColumnAliasing.getAliasSubMap(
        g.generator.children, g.requiredChildOutput).map {
        case (nestedFieldToAlias, attrToAliases) =>
          // Defer updating `Generate.unrequiredChildIndex` to next round of `ColumnPruning`.
          NestedColumnAliasing.replaceWithAliases(g, nestedFieldToAlias, attrToAliases)
      }

    case _ =>
      None
  }

  /**
   * This is a while-list for pruning nested fields at `Generator`.
   */
  def canPruneGenerator(g: Generator): Boolean = g match {
    case _: Explode => true
    case _: Stack => true
    case _: PosExplode => true
    case _: Inline => true
    case _ => false
  }
}
