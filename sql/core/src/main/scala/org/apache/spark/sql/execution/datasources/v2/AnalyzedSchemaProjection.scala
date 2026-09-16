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

import org.apache.spark.{SparkException, SparkThrowable}
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.expressions.{Alias, ArrayTransform, AttributeReference, AttributeSeq, CreateNamedStruct, Expression, ExtractValue, GetStructField, If, IsNull, KnownNotNull, LambdaFunction, Literal, MetadataAttributeWithLogicalName, NamedLambdaVariable, TaggingExpression, TransformKeys, TransformValues, UnresolvedNamedLambdaVariable}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.util.MetadataColumnHelper
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, Metadata, StructType}
import org.apache.spark.sql.util.SchemaUtils

/**
 * Restores the output a plan was analyzed with on a relation whose table has since changed - the
 * schema `V2TableUtil.validateCapturedColumns` calls the captured one.
 *
 * The relation exposes the table's current schema, so its output stays aligned with the physical
 * scan, and a projection on top recreates the columns, types and expression IDs the parent plan was
 * analyzed against.
 */
private[sql] object AnalyzedSchemaProjection extends SQLConfHelper {

  /**
   * Prevents [[CreateNamedStruct]] from inheriting metadata from a field value while leaving the
   * value's type, nullability, evaluation, and code generation unchanged.
   */
  private case class MetadataPropagationBarrier(child: Expression) extends TaggingExpression {
    override protected def withNewChildInternal(
        newChild: Expression): MetadataPropagationBarrier = copy(child = newChild)
  }

  def rebindToAnalyzedSchema(relation: DataSourceV2Relation): LogicalPlan = {
    // The relation still carries the output captured at analysis time; only its table has been
    // swapped for the current one.
    val capturedOutput = relation.output
    val resolver = conf.resolver
    val caseSensitive = conf.caseSensitiveAnalysis
    val current = DataSourceV2Relation.create(
      relation.table,
      relation.catalog,
      relation.identifier,
      relation.options,
      relation.timeTravelSpec)
    val currentMetadataOutput = current.metadataOutput
    val currentMetadata = capturedOutput.filter(_.isMetadataCol).map { captured =>
      val logicalName = metadataLogicalName(captured)
      matchFoldedName(currentMetadataOutput, logicalName, caseSensitive)(metadataLogicalName)
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

    val capturedIndex = new AttributeIndex(capturedOutput, resolver, caseSensitive)
    val reboundOutput = currentOutput.map { currentAttr =>
      capturedIndex.get(currentAttr).filter(canReuse(_, currentAttr)).getOrElse(currentAttr)
    }
    val reboundRelation = relation.copy(output = reboundOutput)

    val reboundIndex = new AttributeIndex(reboundOutput, resolver, caseSensitive)
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
          val index = matchStructField(structInput, fromStruct, targetField.name, resolver)
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
   * Returns the ordinal in `fromStruct` that the captured field name refers to, by asking the same
   * resolution Spark uses for `st.field` in a query.
   *
   * Deciding this here instead would mean maintaining a second rule, and Spark's two rules are not
   * the same: a top-level column is resolved by folding names with `toLowerCase(ROOT)` to collect
   * candidates and only then filtering them with the resolver, while a struct field is resolved
   * with the resolver alone. A name pair the fold separates but the resolver equates - `s` beside
   * U+017F LONG S, say - is therefore a distinct column at the top level and ambiguous inside a
   * struct. Delegating keeps a refreshed plan readable exactly where a fresh query is readable.
   *
   * An ambiguity is reported as the same user-facing error a fresh query reports. Refresh
   * validation pairs a captured field with a current one the top-level way - candidates by folded
   * name, narrowed with the resolver - so it can see one candidate where this sees two, and accepts
   * a schema this rejects. A missing field, by contrast, cannot happen once validation has passed:
   * the field it paired is always among the candidates the resolver alone collects here, so that
   * branch stays an internal error.
   */
  private def matchStructField(
      structInput: Expression,
      fromStruct: StructType,
      capturedName: String,
      resolver: Resolver): Int = {
    ExtractValue.extractValue(structInput, Literal(capturedName), resolver) match {
      case Left(field: GetStructField) => field.ordinal
      case Left(other) =>
        unexpectedSchemaChange(
          s"captured struct field $capturedName extracted a ${other.getClass.getSimpleName}")
      case Right(ambiguous: SparkThrowable)
          if ambiguous.getCondition == "AMBIGUOUS_REFERENCE_TO_FIELDS" =>
        throw ambiguous.asInstanceOf[Throwable]
      case Right(_) =>
        unexpectedSchemaChange(
          s"captured struct field $capturedName is missing from $fromStruct")
    }
  }

  /**
   * Returns the position of the first entry whose folded name matches `target`, if any.
   *
   * Only the metadata lookups use this. A metadata attribute matches on its logical name rather
   * than on `name`, and `AttributeSeq` indexes by `name`, so that lookup cannot be delegated to
   * Spark's resolution the way the data-column and struct-field lookups are. It keeps the fold and
   * the first-match tie-break it has always had; the weaknesses of that rule - a fold collision can
   * arise under a non-ROOT default locale, and the first match is then arbitrary - are unaddressed
   * here on purpose, because metadata columns are out of this change's scope.
   */
  private def matchFoldedName[T](
      candidates: Seq[T],
      target: String,
      caseSensitive: Boolean)(name: T => String): Option[Int] = {
    val foldedTarget = SchemaUtils.foldName(target, caseSensitive)
    val pos = candidates.indexWhere { candidate =>
      SchemaUtils.foldName(name(candidate), caseSensitive) == foldedTarget
    }
    if (pos >= 0) Some(pos) else None
  }

  /**
   * Indexes attributes by name for the rebinding lookups. Data and metadata attributes are indexed
   * separately because a metadata attribute matches on its logical name, which a data column may
   * also carry.
   *
   * Data attributes are looked up through [[AttributeSeq.resolve]], so a captured column binds to
   * the attribute a fresh query would resolve its name to and an ambiguous name raises the same
   * user-facing error rather than silently binding to one of the candidates. Metadata attributes
   * keep the folded-name lookup they had, for the reason given on [[matchFoldedName]].
   */
  private class AttributeIndex(
      attributes: Seq[AttributeReference],
      resolver: Resolver,
      caseSensitive: Boolean) {
    private val dataAttrs = attributes.filterNot(_.isMetadataCol)
    private val metadataAttrs = attributes.filter(_.isMetadataCol)
    // Both indexes are built once, because `get` runs per column: `AttributeSeq` keeps its name
    // index in per-instance lazy state, and recovering the attribute by expression ID would
    // otherwise scan the attributes again on every lookup.
    private val dataAttrSeq = AttributeSeq(dataAttrs)
    private val dataAttrsById = dataAttrs.map(attr => attr.exprId -> attr).toMap

    def get(target: AttributeReference): Option[AttributeReference] = {
      if (target.isMetadataCol) {
        matchFoldedName(metadataAttrs, metadataLogicalName(target), caseSensitive)(
          metadataLogicalName).map(pos => metadataAttrs(pos))
      } else {
        dataAttrSeq.resolve(Seq(target.name), resolver).flatMap { resolved =>
          // `resolve` renames the attribute it returns to the requested name; recover the attribute
          // itself so the rebound output keeps its own name and expression ID.
          dataAttrsById.get(resolved.references.head.exprId)
        }
      }
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
