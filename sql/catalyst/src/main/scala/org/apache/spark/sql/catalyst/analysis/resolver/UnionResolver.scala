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

import java.util.HashSet

import org.apache.spark.sql.catalyst.analysis.{
  withPosition,
  AnsiTypeCoercion,
  TypeCoercion,
  TypeCoercionBase
}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Cast, ExprId}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan, Project, Union}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.{DataType, MetadataBuilder}

/**
 * The [[UnionResolver]] performs [[Union]] operator resolution. This operator has 2+
 * children. Resolution involves checking and normalizing child output attributes
 * (data types and nullability).
 */
class UnionResolver(resolver: Resolver, expressionResolver: ExpressionResolver)
    extends TreeNodeResolver[Union, Union] {
  private val expressionIdAssigner = expressionResolver.getExpressionIdAssigner
  private val scopes = resolver.getNameScopes
  private val typeCoercion: TypeCoercionBase =
    if (conf.ansiEnabled) {
      AnsiTypeCoercion
    } else {
      TypeCoercion
    }

  /**
   * Resolve the [[Union]] operator:
   *  - Retrieve old output and child outputs if the operator is already resolved. This is relevant
   *    for partially resolved subtrees from DataFrame programs.
   *  - Resolve each child in the context of a) New [[NameScope]] b) New [[ExpressionIdAssigner]]
   *    mapping. Collect child outputs to coerce them later.
   *  - Perform projection-based expression ID deduplication if required. This is a hack to stay
   *    compatible with fixed-point [[Analyzer]].
   *  - Perform individual output deduplication to handle the distinct union case described in
   *    [[performIndividualOutputExpressionIdDeduplication]] scaladoc.
   *  - Validate that child outputs have same length or throw "NUM_COLUMNS_MISMATCH" otherwise.
   *  - Compute widened data types for child output attributes using
   *    [[typeCoercion.findWiderTypeForTwo]] or throw "INCOMPATIBLE_COLUMN_TYPE" if coercion fails.
   *  - Add [[Project]] with [[Cast]] on children needing attribute data type widening.
   *  - Assert that coerced outputs don't have conflicting expression IDs.
   *  - Merge transformed outputs: For each column, merge child attributes' types using
   *    [[StructType.unionLikeMerge]]. Mark column as nullable if any child attribute is.
   *  - Store merged output in current [[NameScope]].
   *  - Create a new mapping in [[ExpressionIdAssigner]] using the coerced and validated outputs.
   *  - Return the resolved [[Union]] with new children.
   */
  override def resolve(unresolvedUnion: Union): Union = {
    val (oldOutput, oldChildOutputs) = if (unresolvedUnion.resolved) {
      (Some(unresolvedUnion.output), Some(unresolvedUnion.children.map(_.output)))
    } else {
      (None, None)
    }

    val (resolvedChildren, childOutputs) = unresolvedUnion.children.zipWithIndex.map {
      case (unresolvedChild, childIndex) =>
        scopes.withNewScope {
          expressionIdAssigner.withNewMapping(isLeftmostChild = (childIndex == 0)) {
            val resolvedChild = resolver.resolve(unresolvedChild)
            (resolvedChild, scopes.top.output)
          }
        }
    }.unzip

    val (projectBasedDeduplicatedChildren, projectBasedDeduplicatedChildOutputs) =
      performProjectionBasedExpressionIdDeduplication(
        resolvedChildren,
        childOutputs,
        oldChildOutputs
      )
    val (deduplicatedChildren, deduplicatedChildOutputs) =
      performIndividualOutputExpressionIdDeduplication(
        projectBasedDeduplicatedChildren,
        projectBasedDeduplicatedChildOutputs
      )

    val (newChildren, newChildOutputs) = if (needToCoerceChildOutputs(deduplicatedChildOutputs)) {
      coerceChildOutputs(
        deduplicatedChildren,
        deduplicatedChildOutputs,
        validateAndDeduceTypes(unresolvedUnion, deduplicatedChildOutputs)
      )
    } else {
      (deduplicatedChildren, deduplicatedChildOutputs)
    }

    ExpressionIdAssigner.assertOutputsHaveNoConflictingExpressionIds(newChildOutputs)

    withPosition(unresolvedUnion) {
      scopes.overwriteTop(Union.mergeChildOutputs(newChildOutputs))
    }

    expressionIdAssigner.createMapping(scopes.top.output, oldOutput)

    unresolvedUnion.copy(children = newChildren)
  }

  /**
   * Fixed-point [[Analyzer]] uses [[DeduplicateRelations]] rule to handle duplicate expression IDs
   * in multi-child operator outputs. For [[Union]]s it uses a "projection-based deduplication",
   * i.e. places another [[Project]] operator with new [[Alias]]es on the right child if duplicate
   * expression IDs detected. New [[Alias]] "covers" the original attribute with new expression ID.
   * This is done for all child operators except [[LeafNode]]s.
   *
   * We don't need this operation in single-pass [[Resolver]], since we have
   * [[ExpressionIdAssigner]] for expression ID deduplication, but perform it nevertheless to stay
   * compatible with fixed-point [[Analyzer]]. Since new outputs are already deduplicated by
   * [[ExpressionIdAssigner]], we check the _old_ outputs for duplicates and place a [[Project]]
   * only if old outputs are available (i.e. we are dealing with a resolved subtree from
   * DataFrame program).
   */
  private def performProjectionBasedExpressionIdDeduplication(
      children: Seq[LogicalPlan],
      childOutputs: Seq[Seq[Attribute]],
      oldChildOutputs: Option[Seq[Seq[Attribute]]]
  ): (Seq[LogicalPlan], Seq[Seq[Attribute]]) = {
    oldChildOutputs match {
      case Some(oldChildOutputs) =>
        val oldExpressionIds = new HashSet[ExprId]

        children
          .zip(childOutputs)
          .zip(oldChildOutputs)
          .map {
            case ((child: LeafNode, output), _) =>
              (child, output)

            case ((child, output), oldOutput) =>
              val oldOutputExpressionIds = new HashSet[ExprId]

              val hasConflicting = oldOutput.exists { oldAttribute =>
                oldOutputExpressionIds.add(oldAttribute.exprId)
                oldExpressionIds.contains(oldAttribute.exprId)
              }

              if (hasConflicting) {
                val newExpressions = output.map { attribute =>
                  Alias(attribute, attribute.name)()
                }
                (
                  Project(projectList = newExpressions, child = child),
                  newExpressions.map(_.toAttribute)
                )
              } else {
                oldExpressionIds.addAll(oldOutputExpressionIds)

                (child, output)
              }
          }
          .unzip
      case _ =>
        (children, childOutputs)
    }
  }

  /**
   * Deduplicate expression IDs at the scope of each individual child output. This is necessary to
   * handle the following case:
   *
   * {{{
   * -- The correct answer is (1, 1), (1, 2). Without deduplication it would be (1, 1), because
   * -- aggregation would be done only based on the first column.
   * SELECT
   *   a, a
   * FROM
   *   VALUES (1, 1), (1, 2) AS t1 (a, b)
   * UNION
   * SELECT
   *  a, b
   * FROM
   *   VALUES (1, 1), (1, 2) AS t2 (a, b)
   * }}}
   *
   * Putting [[Alias]] introduces a new expression ID for the attribute duplicates in the output. We
   * also add `__is_duplicate` metadata so that [[AttributeSeq.getCandidatesForResolution]] doesn't
   * produce conflicting candidates when resolving names in the upper [[Project]] - this is
   * technically still the same attribute.
   *
   * Probably there's a better way to do that, but we want to stay compatible with the fixed-point
   * [[Analyzer]].
   *
   * See SPARK-37865 for more details.
   */
  private def performIndividualOutputExpressionIdDeduplication(
      children: Seq[LogicalPlan],
      childOutputs: Seq[Seq[Attribute]]
  ): (Seq[LogicalPlan], Seq[Seq[Attribute]]) = {
    children
      .zip(childOutputs)
      .map {
        case (child, childOutput) =>
          var outputChanged = false

          val expressionIds = new HashSet[ExprId]
          val newOutput = childOutput.map { attribute =>
            if (expressionIds.contains(attribute.exprId)) {
              outputChanged = true

              val newMetadata = new MetadataBuilder()
                .withMetadata(attribute.metadata)
                .putNull("__is_duplicate")
                .build()
              Alias(attribute, attribute.name)(explicitMetadata = Some(newMetadata))
            } else {
              expressionIds.add(attribute.exprId)

              attribute
            }
          }

          if (outputChanged) {
            (Project(projectList = newOutput, child = child), newOutput.map(_.toAttribute))
          } else {
            (child, childOutput)
          }
      }
      .unzip
  }

  /**
   * Check if we need to coerce child output attributes to wider types. We need to do this if:
   * - Output length differs between children. We will throw an appropriate error later during type
   *   coercion with more diagnostics.
   * - Output data types differ between children. We don't care about nullability for type coercion,
   *   it will be correctly assigned later by [[Union.mergeChildOutputs]].
   */
  private def needToCoerceChildOutputs(childOutputs: Seq[Seq[Attribute]]): Boolean = {
    val firstChildOutput = childOutputs.head
    childOutputs.tail.exists { childOutput =>
      childOutput.length != firstChildOutput.length ||
      childOutput.zip(firstChildOutput).exists {
        case (lhsAttribute, rhsAttribute) =>
          !DataType.equalsStructurally(
            lhsAttribute.dataType,
            rhsAttribute.dataType,
            ignoreNullability = true
          )
      }
    }
  }

  /**
   * Returns a sequence of data types representing the widened data types for each column:
   *  - Validates that the number of columns in each child of the `Union` operator are equal.
   *  - Validates that the data types of columns can be widened to a common type.
   *  - Deduces the widened data types for each column.
   */
  private def validateAndDeduceTypes(
      unresolvedUnion: Union,
      childOutputs: Seq[Seq[Attribute]]): Seq[DataType] = {
    val childDataTypes = childOutputs.map(attributes => attributes.map(attr => attr.dataType))

    val expectedNumColumns = childDataTypes.head.length

    childDataTypes.zipWithIndex.tail.foldLeft(childDataTypes.head) {
      case (widenedTypes, (childColumnTypes, childIndex)) =>
        if (childColumnTypes.length != expectedNumColumns) {
          throwNumColumnsMismatch(
            expectedNumColumns,
            childColumnTypes,
            childIndex,
            unresolvedUnion
          )
        }

        widenedTypes.zip(childColumnTypes).zipWithIndex.map {
          case ((widenedColumnType, columnTypeForCurrentRow), columnIndex) =>
            typeCoercion.findWiderTypeForTwo(widenedColumnType, columnTypeForCurrentRow).getOrElse {
              throwIncompatibleColumnTypeError(
                unresolvedUnion,
                columnIndex,
                childIndex + 1,
                widenedColumnType,
                columnTypeForCurrentRow
              )
            }
        }
    }
  }

  /**
   * Coerce `childOutputs` to the previously calculated `widenedTypes`. If the data types for
   * child output has changed, we have to add a [[Project]] operator with a [[Cast]] to the new
   * type.
   */
  private def coerceChildOutputs(
      children: Seq[LogicalPlan],
      childOutputs: Seq[Seq[Attribute]],
      widenedTypes: Seq[DataType]): (Seq[LogicalPlan], Seq[Seq[Attribute]]) = {
    children
      .zip(childOutputs)
      .map {
        case (child, output) =>
          var outputChanged = false
          val newExpressions = output.zip(widenedTypes).map {
            case (attribute, widenedType) =>
              /**
               * Probably more correct way to compare data types here would be to call
               * [[DataType.equalsStructurally]] but fixed-point [[Analyzer]] rule
               * [[WidenSetOperationTypes]] uses `==`, so we do the same to stay compatible.
               */
              if (attribute.dataType == widenedType) {
                attribute
              } else {
                outputChanged = true
                Alias(
                  Cast(attribute, widenedType, Some(conf.sessionLocalTimeZone)),
                  attribute.name
                )()
              }
          }

          if (outputChanged) {
            (Project(newExpressions, child), newExpressions.map(_.toAttribute))
          } else {
            (child, output)
          }
      }
      .unzip
  }

  private def throwNumColumnsMismatch(
      expectedNumColumns: Int,
      childColumnTypes: Seq[DataType],
      columnIndex: Int,
      unresolvedUnion: Union): Unit = {
    throw QueryCompilationErrors.numColumnsMismatch(
      "UNION",
      expectedNumColumns,
      columnIndex + 1,
      childColumnTypes.length,
      unresolvedUnion.origin
    )
  }

  private def throwIncompatibleColumnTypeError(
      unresolvedUnion: Union,
      columnIndex: Int,
      childIndex: Int,
      widenedColumnType: DataType,
      columnTypeForCurrentRow: DataType): Nothing = {
    throw QueryCompilationErrors.incompatibleColumnTypeError(
      "UNION",
      columnIndex,
      childIndex + 1,
      widenedColumnType,
      columnTypeForCurrentRow,
      hint = "",
      origin = unresolvedUnion.origin
    )
  }
}
