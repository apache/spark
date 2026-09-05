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

import java.util.Locale

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.expressions.{Alias, ArrayTransform, AttributeReference, CreateNamedStruct, Expression, GetStructField, If, IsNull, KnownNotNull, LambdaFunction, Literal, MetadataAttributeWithLogicalName, NamedLambdaVariable, TransformKeys, TransformValues, UnresolvedNamedLambdaVariable}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.util.MetadataColumnHelper
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType}

/**
 * Rebinds a relation that reads a current table schema to output attributes captured from an
 * earlier compatible schema. The current schema is exposed by the relation so its output remains
 * aligned with the physical scan, while a projection recreates the captured output for the
 * already-analyzed parent plan.
 */
private[sql] object CapturedSchemaProjection extends SQLConfHelper {

  def rebindToCapturedSchema(relation: DataSourceV2Relation): LogicalPlan = {
    // The relation still carries the output captured at analysis time; only its table has been
    // swapped for the current one.
    val capturedOutput = relation.output
    val caseSensitive = conf.caseSensitiveAnalysis
    val resolver = conf.resolver
    val current = DataSourceV2Relation.create(
      relation.table,
      relation.catalog,
      relation.identifier,
      relation.options,
      relation.timeTravelSpec)
    val currentMetadata = capturedOutput.filter(_.isMetadataCol).map { captured =>
      val logicalName = metadataLogicalName(captured)
      current.metadataOutput.find { attr =>
        resolver(metadataLogicalName(attr), logicalName)
      }.getOrElse {
        // The connector still reports this metadata column, so it can only be absent here because
        // a data column has taken its name and the connector suppresses rather than renames the
        // conflict (`canRenameConflictingMetadataColumns`). Validation owns rejecting that.
        unexpectedSchemaChange(
          s"captured metadata column $logicalName is missing from the current relation")
      }
    }

    val currentOutput = current.output ++ currentMetadata

    // Refresh may visit an already rebound relation. Preserve its attributes so the projection
    // above it continues to reference valid expression IDs.
    if (sameOutputShape(capturedOutput, currentOutput)) {
      return relation
    }

    val capturedIndex = new AttributeIndex(capturedOutput, caseSensitive)
    val reboundOutput = currentOutput.map { currentAttr =>
      capturedIndex.get(currentAttr).filter(canReuse(_, currentAttr)).getOrElse(currentAttr)
    }
    val reboundRelation = relation.copy(output = reboundOutput)

    val reboundIndex = new AttributeIndex(reboundOutput, caseSensitive)
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
          currentAttr, currentAttr.dataType, capturedAttr.dataType, caseSensitive)
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
      caseSensitive: Boolean): Expression = {
    if (from == to) {
      return input
    }

    val projected = (from, to) match {
      case (fromStruct: StructType, toStruct: StructType) =>
        val structInput = if (input.nullable) KnownNotNull(input) else input
        val sourceOrdinals = fieldOrdinals(fromStruct, caseSensitive)
        val fields = toStruct.fields.iterator.flatMap { targetField =>
          val index =
            sourceOrdinals.getOrElse(fold(targetField.name, caseSensitive), -1)
          if (index < 0) {
            unexpectedSchemaChange(
              s"captured struct field ${targetField.name} is missing from $fromStruct")
          }
          val sourceField = fromStruct.fields(index)
          val value = projectToType(
            GetStructField(structInput, index, Some(sourceField.name)),
            sourceField.dataType,
            targetField.dataType,
            caseSensitive)
          val namedValue =
            Alias(value, targetField.name)(explicitMetadata = Some(targetField.metadata))
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
            projectToType(element, fromElement, toElement, caseSensitive), Seq(element)))

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
          TransformKeys(
            input,
            LambdaFunction(projectToType(key, fromKey, toKey, caseSensitive), Seq(key, value)))
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
              projectToType(value, fromValue, toValue, caseSensitive), Seq(key, value)))
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
   * Indexes attributes by name for the rebinding lookups. Data and metadata attributes are indexed
   * separately because a metadata attribute matches on its logical name, which a data column may
   * also carry. Names are folded the way the resolver compares them.
   */
  private class AttributeIndex(attributes: Seq[AttributeReference], caseSensitive: Boolean) {
    private val dataAttrs = index(attributes.filterNot(_.isMetadataCol))(_.name)
    private val metadataAttrs = index(attributes.filter(_.isMetadataCol))(metadataLogicalName)

    def get(target: AttributeReference): Option[AttributeReference] = {
      if (target.isMetadataCol) {
        metadataAttrs.get(fold(metadataLogicalName(target), caseSensitive))
      } else {
        dataAttrs.get(fold(target.name, caseSensitive))
      }
    }

    private def index(attrs: Seq[AttributeReference])(
        name: AttributeReference => String): Map[String, AttributeReference] = {
      attrs.foldLeft(Map.empty[String, AttributeReference]) { (indexed, attr) =>
        val key = fold(name(attr), caseSensitive)
        // Keep the first attribute for a name so the lookup stays deterministic even if a caller
        // reaches this without the validation that rejects duplicate names.
        if (indexed.contains(key)) indexed else indexed.updated(key, attr)
      }
    }
  }

  private def fieldOrdinals(struct: StructType, caseSensitive: Boolean): Map[String, Int] = {
    struct.fields.iterator.zipWithIndex.foldLeft(Map.empty[String, Int]) {
      case (indexed, (field, ordinal)) =>
        val key = fold(field.name, caseSensitive)
        // Keep the first ordinal for a name so resolution stays deterministic even if a caller
        // reaches this without the validation that rejects duplicate field names.
        if (indexed.contains(key)) indexed else indexed.updated(key, ordinal)
    }
  }

  private def fold(name: String, caseSensitive: Boolean): String = {
    if (caseSensitive) name else name.toLowerCase(Locale.ROOT)
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
