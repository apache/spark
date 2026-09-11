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

package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.expressions.{Alias, ArrayTransform, AttributeReference, CreateNamedStruct, Expression, GetStructField, If, IsNull, KnownNotNull, LambdaFunction, Literal, MetadataAttributeWithLogicalName, NamedLambdaVariable, TaggingExpression, TransformKeys, TransformValues, UnresolvedNamedLambdaVariable}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.util.MetadataColumnHelper
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, Metadata, StructType}

/**
 * Rebinds a relation that reads a current table schema to output attributes captured from an
 * earlier compatible schema. The current schema is exposed by the relation so its output remains
 * aligned with the physical scan, while a projection recreates the captured output for the
 * already-analyzed parent plan.
 */
private[sql] object CapturedSchemaProjection extends SQLConfHelper {

  /**
   * Prevents [[CreateNamedStruct]] from inheriting metadata from a field value while leaving the
   * value's type, nullability, evaluation, and code generation unchanged.
   */
  private case class MetadataPropagationBarrier(child: Expression) extends TaggingExpression {
    override protected def withNewChildInternal(
        newChild: Expression): MetadataPropagationBarrier = copy(child = newChild)
  }

  def rebindToCapturedSchema(relation: DataSourceV2Relation): LogicalPlan = {
    // The relation still carries the output captured at analysis time; only its table has been
    // swapped for the current one.
    val capturedOutput = relation.output
    val resolver = conf.resolver
    val current = DataSourceV2Relation.create(
      relation.table,
      relation.catalog,
      relation.identifier,
      relation.options,
      relation.timeTravelSpec)
    val currentMetadataOutput = current.metadataOutput
    val currentMetadata = capturedOutput.filter(_.isMetadataCol).map { captured =>
      val logicalName = metadataLogicalName(captured)
      matchName(currentMetadataOutput, logicalName, resolver)(metadataLogicalName)
        .map(pos => currentMetadataOutput(pos))
        .getOrElse {
          // The connector still reports this metadata column, so it can only be absent here
          // because a data column has taken its name and the connector suppresses rather than
          // renames the conflict (`canRenameConflictingMetadataColumns`). Validation owns
          // rejecting that.
          unexpectedSchemaChange(
            s"captured metadata column $logicalName is missing from the current relation")
        }
    }

    val currentOutput = current.output ++ currentMetadata

    // Refresh may visit an already rebound relation. Preserve its attributes so the projection
    // above it continues to reference valid expression IDs.
    //
    // A further schema change on such a relation adds a second projection instead of replacing
    // the first. Only the cache stores a refreshed plan, so the effect is limited to that entry:
    // it stops matching the single projection a query rebuilds from its own captured output, and
    // is no longer reused. Results stay correct.
    if (sameOutputShape(capturedOutput, currentOutput)) {
      return relation
    }

    val capturedIndex = new AttributeIndex(capturedOutput, resolver)
    val reboundOutput = currentOutput.map { currentAttr =>
      capturedIndex.get(currentAttr).filter(canReuse(_, currentAttr)).getOrElse(currentAttr)
    }
    val reboundRelation = relation.copy(output = reboundOutput)

    val reboundIndex = new AttributeIndex(reboundOutput, resolver)
    val projectList = capturedOutput.map { capturedAttr =>
      val currentAttr = reboundIndex.get(capturedAttr).getOrElse {
        unexpectedSchemaChange(
          s"captured column ${capturedAttr.name} is missing from current table ${relation.name}")
      }
      if (currentAttr.exprId == capturedAttr.exprId &&
        sameAttributeShape(currentAttr, capturedAttr)) {
        currentAttr
      } else {
        if (currentAttr.nullable != capturedAttr.nullable) {
          unexpectedSchemaChange(
            s"nullability changed for captured column ${capturedAttr.name} in ${relation.name}")
        }
        val projected = projectToType(
          currentAttr, currentAttr.dataType, capturedAttr.dataType, resolver)
        if (projected.dataType != capturedAttr.dataType ||
          projected.nullable != capturedAttr.nullable) {
          unexpectedSchemaChange(
            s"failed to recreate captured column ${capturedAttr.name} in ${relation.name}")
        }
        Alias(projected, capturedAttr.name)(
          exprId = capturedAttr.exprId,
          qualifier = capturedAttr.qualifier,
          explicitMetadata = Some(capturedAttr.metadata))
      }
    }

    Project(projectList, reboundRelation)
  }

  private[v2] def projectToType(
      input: Expression,
      from: DataType,
      to: DataType,
      resolver: Resolver): Expression = {
    if (from == to) {
      return input
    }

    val projected = (from, to) match {
      case (fromStruct: StructType, toStruct: StructType) =>
        val structInput = if (input.nullable) KnownNotNull(input) else input
        val fields = toStruct.fields.iterator.flatMap { targetField =>
          val index = matchName(fromStruct, targetField.name, resolver)(_.name).getOrElse {
            unexpectedSchemaChange(
              s"captured struct field ${targetField.name} is missing from $fromStruct")
          }
          val sourceField = fromStruct.fields(index)
          val value = projectToType(
            GetStructField(structInput, index, Some(sourceField.name)),
            sourceField.dataType,
            targetField.dataType,
            resolver)
          val namedValue = if (targetField.metadata == Metadata.empty) {
            // An empty captured value is still an explicit instruction not to inherit metadata
            // from the current GetStructField. CleanupAliases removes an empty-metadata Alias, so
            // use a local passthrough barrier instead of changing shared alias cleanup behavior.
            MetadataPropagationBarrier(value)
          } else {
            Alias(value, targetField.name)(explicitMetadata = Some(targetField.metadata))
          }
          Iterator(Literal(targetField.name), namedValue)
        }.toSeq
        val rebuilt = CreateNamedStruct(fields)
        if (input.nullable) {
          // The null literal takes the rebuilt type rather than `toStruct` so that the type check
          // below still sees any mismatch: `If` merges its branch types and only requires them to
          // match up to `sameType`, which ignores nullability and metadata.
          If(IsNull(input), Literal.create(null, rebuilt.dataType), rebuilt)
        } else {
          rebuilt
        }

      case (ArrayType(fromElement, fromContainsNull), ArrayType(toElement, toContainsNull)) =>
        if (fromContainsNull != toContainsNull) {
          unexpectedSchemaChange(s"array element nullability changed from $from to $to")
        }
        val element = NamedLambdaVariable(
          UnresolvedNamedLambdaVariable.freshVarName("element"),
          fromElement,
          fromContainsNull)
        ArrayTransform(
          input,
          LambdaFunction(
            projectToType(element, fromElement, toElement, resolver), Seq(element)))

      case (
            MapType(fromKey, fromValue, fromValueContainsNull),
            MapType(toKey, toValue, toValueContainsNull)) =>
        if (fromValueContainsNull != toValueContainsNull) {
          unexpectedSchemaChange(s"map value nullability changed from $from to $to")
        }

        val withProjectedKeys = if (fromKey != toKey) {
          val key = NamedLambdaVariable(
            UnresolvedNamedLambdaVariable.freshVarName("key"),
            fromKey,
            nullable = false)
          val value = NamedLambdaVariable(
            UnresolvedNamedLambdaVariable.freshVarName("value"),
            fromValue,
            fromValueContainsNull)
          // A field dropped from a key can collapse keys that are distinct under the current type
          // into one captured key. Rebuilding the map through `TransformKeys` keeps the uniqueness
          // invariant, so such a projection fails with DUPLICATED_MAP_KEY (or keeps the last entry
          // under `spark.sql.mapKeyDedupPolicy=LAST_WIN`) rather than producing a map that holds
          // duplicate keys.
          TransformKeys(
            input,
            LambdaFunction(projectToType(key, fromKey, toKey, resolver), Seq(key, value)))
        } else {
          input
        }

        if (fromValue != toValue) {
          val key = NamedLambdaVariable(
            UnresolvedNamedLambdaVariable.freshVarName("key"),
            toKey,
            nullable = false)
          val value = NamedLambdaVariable(
            UnresolvedNamedLambdaVariable.freshVarName("value"),
            fromValue,
            fromValueContainsNull)
          TransformValues(
            withProjectedKeys,
            LambdaFunction(
              projectToType(value, fromValue, toValue, resolver), Seq(key, value)))
        } else {
          withProjectedKeys
        }

      case _ =>
        unexpectedSchemaChange(s"cannot project incompatible data type $from to $to")
    }

    if (projected.dataType != to) {
      unexpectedSchemaChange(
        s"projected data type ${projected.dataType} does not match captured type $to")
    }
    projected
  }

  /**
   * Returns the position of the entry whose name matches `target`, if any.
   *
   * An exact match wins so that a name binds to itself even when the resolver cannot tell it apart
   * from another name in the same schema. Duplicate names are rejected by folding with
   * `toLowerCase` while resolution compares with `equalsIgnoreCase`, so a schema can legally hold
   * several names the resolver considers equal. Without an exact match the resolver match has to be
   * unique: nothing here can decide which of two indistinguishable names the captured plan read.
   */
  private def matchName[T](
      candidates: Seq[T],
      target: String,
      resolver: Resolver)(name: T => String): Option[Int] = {
    val exact = candidates.indexWhere(candidate => name(candidate) == target)
    if (exact >= 0) {
      return Some(exact)
    }
    val matches = candidates.indices.filter(pos => resolver(name(candidates(pos)), target))
    if (matches.length > 1) {
      unexpectedSchemaChange(
        s"captured name $target matches multiple current names " +
          matches.map(pos => name(candidates(pos))).mkString("[", ", ", "]"))
    }
    matches.headOption
  }

  /**
   * Indexes attributes by name for the rebinding lookups. Data and metadata attributes are indexed
   * separately because a metadata attribute matches on its logical name, which a data column may
   * also carry.
   */
  private class AttributeIndex(attributes: Seq[AttributeReference], resolver: Resolver) {
    private val dataAttrs = attributes.filterNot(_.isMetadataCol)
    private val metadataAttrs = attributes.filter(_.isMetadataCol)

    def get(target: AttributeReference): Option[AttributeReference] = {
      if (target.isMetadataCol) {
        find(metadataAttrs, metadataLogicalName(target))(metadataLogicalName)
      } else {
        find(dataAttrs, target.name)(_.name)
      }
    }

    private def find(attrs: Seq[AttributeReference], targetName: String)(
        name: AttributeReference => String): Option[AttributeReference] = {
      matchName(attrs, targetName, resolver)(name).map(pos => attrs(pos))
    }
  }

  private def metadataLogicalName(attr: AttributeReference): String = attr match {
    case MetadataAttributeWithLogicalName(_, logicalName) => logicalName
    case _ =>
      unexpectedSchemaChange(s"metadata attribute ${attr.name} has no logical name")
  }

  private def canReuse(captured: AttributeReference, current: AttributeReference): Boolean = {
    captured.name == current.name && sameAttributeShape(captured, current)
  }

  private def sameOutputShape(
      left: Seq[AttributeReference],
      right: Seq[AttributeReference]): Boolean = {
    left.length == right.length && left.zip(right).forall { case (l, r) =>
      l.name == r.name && l.isMetadataCol == r.isMetadataCol && sameAttributeShape(l, r)
    }
  }

  private def sameAttributeShape(left: AttributeReference, right: AttributeReference): Boolean = {
    left.dataType == right.dataType &&
    left.nullable == right.nullable &&
    left.metadata == right.metadata
  }

  private def unexpectedSchemaChange(message: String): Nothing = {
    throw SparkException.internalError(
      s"Unexpected incompatible table schema after refresh validation: $message")
  }
}
