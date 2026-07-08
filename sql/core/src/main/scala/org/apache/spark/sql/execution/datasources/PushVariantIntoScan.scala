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

package org.apache.spark.sql.execution.datasources

import scala.collection.mutable.HashMap

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.variant._
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project, Subquery}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.ResolveDefaultColumns
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

// A metadata class of a struct field. All struct fields in a struct must either all have this
// metadata, or all don't have it.
// We define a "variant struct" as: a special struct with its fields annotated with this metadata.
// It indicates that the struct should produce all requested fields of a variant type, and should be
// treated specially by the scan.
case class VariantMetadata(
    // The `path` parameter of VariantGet. It has the same format as a JSON path, except that
    // `[*]` is not supported.
    path: String,
    failOnError: Boolean,
    timeZoneId: String,
    // When set, this struct field is a synthetic cast-error companion paired with the data field
    // of the given NAME in the same variant struct. The companion is populated by the reader
    // with the offending value when the paired data field's strict cast raises
    // INVALID_VARIANT_CAST. We pair by NAME (not struct ordinal) because later pruning or
    // reordering of struct fields preserves names but may shift positions.
    castErrorFor: Option[String] = None) {
  // Produce a metadata contain one key-value pair. The key is the special `METADATA_KEY`.
  // The value contains key-value pairs for `path`, `failOnError`, `timeZoneId`, and -- for
  // companion fields only -- `castErrorFor`.
  def toMetadata: Metadata = {
    val inner = new MetadataBuilder()
      .putString(VariantMetadata.PATH_KEY, path)
      .putBoolean(VariantMetadata.FAIL_ON_ERROR_KEY, failOnError)
      .putString(VariantMetadata.TIME_ZONE_ID_KEY, timeZoneId)
    castErrorFor.foreach { name =>
      inner.putString(VariantMetadata.CAST_ERROR_FOR_KEY, name)
    }
    new MetadataBuilder().putMetadata(VariantMetadata.METADATA_KEY, inner.build()).build()
  }

  def parsedPath(): Array[VariantPathSegment] = {
    VariantPathParser.parse(path).getOrElse {
      val name = if (failOnError) "variant_get" else "try_variant_get"
      throw QueryExecutionErrors.invalidVariantGetPath(path, name)
    }
  }
}

object VariantMetadata {
  val METADATA_KEY = "__VARIANT_METADATA_KEY"
  val PATH_KEY = "path"
  val FAIL_ON_ERROR_KEY = "failOnError"
  val TIME_ZONE_ID_KEY = "timeZoneId"
  // Optional metadata key marking a struct field as a synthetic cast-error companion. When
  // present, the value is the NAME of the paired data field in the same variant struct. We tag
  // in metadata (rather than by a field-name convention or sentinel path) so the marker can't
  // collide with a user-supplied variant path, and so scan-layer schema rewrites that rename
  // fields by ordinal preserve the marker.
  //
  // Example: with two strict-cast requested fields (b::int and obj.b::double) and one
  // non-strict-cast requested field (try_cast(c as long)), the rewritten variant struct looks
  // like:
  // scalastyle:off line.size.limit
  // struct<
  //   "0": int    metadata = { path: "$.b",     failOnError: true,  ... },                  // data slot for b::int
  //   "1": double metadata = { path: "$.obj.b", failOnError: true,  ... },                  // data slot for obj.b::double
  //   "2": long   metadata = { path: "$.c",     failOnError: false, ... },                  // data slot for try_cast(c as long), no companion
  //   "3": string metadata = { path: "$", castErrorFor: "0", ... },                         // companion paired with data field named "0"
  //   "4": string metadata = { path: "$", castErrorFor: "1", ... }                          // companion paired with data field named "1"
  // >
  // scalastyle:on line.size.limit
  val CAST_ERROR_FOR_KEY = "castErrorFor"

  // Build the metadata for a synthetic cast-error companion. `dataFieldName` is the NAME of the
  // paired data field in the same variant struct.
  def castErrorCompanionMetadata(dataFieldName: String): Metadata =
    VariantMetadata("$", failOnError = false, timeZoneId = "UTC",
      castErrorFor = Some(dataFieldName)).toMetadata

  def isVariantStruct(s: StructType): Boolean =
    s.fields.length > 0 && s.fields.forall(_.metadata.contains(METADATA_KEY))

  def isVariantStruct(t: DataType): Boolean = t match {
    case s: StructType => isVariantStruct(s)
    case _ => false
  }

  // Parse the `VariantMetadata` from a metadata produced by `toMetadata`.
  def fromMetadata(metadata: Metadata): VariantMetadata = {
    val value = metadata.getMetadata(METADATA_KEY)
    val castErrorFor =
      if (value.contains(CAST_ERROR_FOR_KEY)) {
        Some(value.getString(CAST_ERROR_FOR_KEY))
      } else {
        None
      }
    VariantMetadata(
      value.getString(PATH_KEY),
      value.getBoolean(FAIL_ON_ERROR_KEY),
      value.getString(TIME_ZONE_ID_KEY),
      castErrorFor
    )
  }
}

// Represent a requested field of a variant that the scan should produce.
// Each `RequestedVariantField` is corresponded to a variant path extraction in the plan.
case class RequestedVariantField(path: VariantMetadata, targetType: DataType)

object RequestedVariantField {
  def fullVariant: RequestedVariantField =
    RequestedVariantField(VariantMetadata("$", failOnError = true, "UTC"), VariantType)

  def apply(v: VariantGet): RequestedVariantField = {
    assert(v.path.foldable)
    RequestedVariantField(
      VariantMetadata(v.path.eval().toString, v.failOnError, v.timeZoneId.get), v.dataType)
  }

  def apply(c: Cast): RequestedVariantField =
    RequestedVariantField(
      VariantMetadata("$", c.evalMode != EvalMode.TRY, c.timeZoneId.get), c.dataType)
}

// Extract a nested struct access path. Return the (root attribute id, a sequence of ordinals to
// access the field). For non-nested attribute access, the sequence is empty.
object StructPath {
  def unapply(expr: Expression): Option[(ExprId, Seq[Int])] = expr match {
    case GetStructField(StructPath(root, path), ordinal, _) => Some((root, path :+ ordinal))
    case a: Attribute => Some(a.exprId, Nil)
    case _ => None
  }
}

// A collection of all eligible variants in a relation, which are in the root of the relation output
// schema, or only nested in struct types.
// The user should:
// 1. Call `addVariantFields` to add all eligible variants in a relation.
// 2. Call `collectRequestedFields` on all expressions depending on the relation. This process will
// add the requested fields of each variant and potentially remove non-eligible variants. See
// `collectRequestedFields` for details.
// 3. Call `rewriteType` to produce a new output schema for the relation.
// 4. Call `rewriteExpr` to rewrite the previously visited expressions by replacing variant
// extractions with struct accessed.
class VariantInRelation {
  // First level key: root attribute id.
  // Second level key: struct access paths to the variant type.
  // Third level key: requested fields of a variant type.
  // Final value: the ordinal of a requested field in the final struct of requested fields.
  val mapping = new HashMap[ExprId, HashMap[Seq[Int], HashMap[RequestedVariantField, Int]]]

  lazy val deferCastErrorEnabled: Boolean =
    SQLConf.get.getConf(SQLConf.PUSH_VARIANT_INTO_SCAN_DEFER_CAST_ERROR)

  // Cast to variant/string never triggers an invalid cast error, so there is no need to wrap.
  def shouldWrapCastError(field: RequestedVariantField): Boolean = field.targetType match {
    case _: VariantType | _: StringType => false
    case _ => field.path.failOnError && deferCastErrorEnabled
  }

  // Extract the SQL-struct path where the leaf is a variant.
  object StructPathToVariant {
    def unapply(expr: Expression): Option[HashMap[RequestedVariantField, Int]] = expr match {
      case StructPath(attrId, path) =>
        mapping.get(attrId).flatMap(_.get(path))
      case _ => None
    }
  }

  // Find eligible variants recursively. `attrId` is the root attribute id.
  // `path` is the current struct access path. `dataType` is the child data type after extracting
  // `path` from the root attribute struct.
  def addVariantFields(
      attrId: ExprId,
      dataType: DataType,
      defaultValue: Any,
      path: Seq[Int]): Unit = {
    dataType match {
      // TODO(SHREDDING): non-null default value is not yet supported.
      case _: VariantType if defaultValue == null =>
        mapping.getOrElseUpdate(attrId, new HashMap).put(path, new HashMap)
      case s: StructType if !VariantMetadata.isVariantStruct(s) =>
        val row = defaultValue.asInstanceOf[InternalRow]
        for ((field, idx) <- s.fields.zipWithIndex) {
          val fieldDefault = if (row == null || row.isNullAt(idx)) {
            null
          } else {
            row.get(idx, field.dataType)
          }
          addVariantFields(attrId, field.dataType, fieldDefault, path :+ idx)
        }
      case _ =>
    }
  }

  def rewriteType(attrId: ExprId, dataType: DataType, path: Seq[Int]): DataType = {
    dataType match {
      case _: VariantType =>
        mapping.get(attrId).flatMap(_.get(path)) match {
          case Some(fields) =>
            val sorted = fields.toArray.sortBy(_._2)
            var dataFields = sorted.map { case (field, ordinal) =>
              StructField(ordinal.toString, field.targetType, metadata = field.path.toMetadata)
            }
            // Avoid producing an empty struct of requested fields. This is intended to simplify the
            // scan implementation, which may not be able to handle empty struct type. This happens
            // if the variant is not used, or only used in `IsNotNull/IsNull` expressions. The value
            // of the placeholder field doesn't matter, even if the scan source accidentally
            // contains such a field.
            if (dataFields.isEmpty) {
              val placeholder = VariantMetadata("$.__placeholder_field__",
                failOnError = false, timeZoneId = "UTC")
              dataFields = Array(StructField("0", BooleanType,
                metadata = placeholder.toMetadata))
            }
            if (deferCastErrorEnabled) {
              // Append a companion field for each strict cast. The reader populates it with the
              // offending value on failure; the rewrite consumes both slots through
              // `UnwrapVariantCastError(error, value)`. The companion's `castErrorFor` metadata
              // stores the data field's NAME so the pairing survives later field renaming.
              val companionDataNames = sorted.collect {
                case (field, ordinal) if shouldWrapCastError(field) => ordinal.toString
              }
              val numData = dataFields.length
              val companionFields = companionDataNames.zipWithIndex.map {
                case (dataFieldName, idx) =>
                  StructField((numData + idx).toString, StringType,
                    metadata = VariantMetadata.castErrorCompanionMetadata(dataFieldName))
              }
              StructType(dataFields ++ companionFields)
            } else {
              StructType(dataFields)
            }
          case _ => dataType
        }
      case s: StructType if !VariantMetadata.isVariantStruct(s) =>
        val newFields = s.fields.zipWithIndex.map { case (field, idx) =>
          field.copy(dataType = rewriteType(attrId, field.dataType, path :+ idx))
        }
        StructType(newFields)
      case _ => dataType
    }
  }

  // Add a requested field to a variant column.
  private def addField(
      map: HashMap[RequestedVariantField, Int],
      field: RequestedVariantField): Unit = {
    val idx = map.size
    map.getOrElseUpdate(field, idx)
  }

  // Update `mapping` with any access to a variant. Add the requested fields of each variant and
  // potentially remove non-eligible variants.
  // If a struct containing a variant is directly used, this variant is not eligible for push down.
  // This is because we need to replace the variant type with a struct producing all requested
  // fields, which also changes the struct type containing it, and it is difficult to reconstruct
  // the original struct value. This is not a big loss, because we need the full variant anyway.
  def collectRequestedFields(expr: Expression): Unit = expr match {
    case v@VariantGet(StructPathToVariant(fields), path, _, _, _) if path.foldable =>
      addField(fields, RequestedVariantField(v))
    case c@Cast(StructPathToVariant(fields), _, _, _) => addField(fields, RequestedVariantField(c))
    case IsNotNull(StructPath(_, _)) | IsNull(StructPath(_, _)) =>
    case StructPath(attrId, path) =>
      mapping.get(attrId) match {
        case Some(variants) =>
          variants.get(path) match {
            case Some(fields) =>
              // Accessing the full variant value
              addField(fields, RequestedVariantField.fullVariant)
            case _ =>
              // Accessing the struct containing a variant.
              // This variant is not eligible for push down.
              // Remove non-eligible variants.
              variants.filterInPlace { case (key, _) => !key.startsWith(path) }
          }
        case _ =>
      }
    case _ => expr.children.foreach(collectRequestedFields)
  }

  // Build the access expression for a requested field. For fields that need cast-error deferral,
  // wrap with `UnwrapVariantCastError` over the paired companion slot; otherwise return the bare
  // `GetStructField`.
  private def accessRequestedField(
      fields: HashMap[RequestedVariantField, Int],
      field: RequestedVariantField,
      v: Expression): Expression = {
    val ordinal = fields(field)
    val value = GetStructField(v, ordinal)
    if (shouldWrapCastError(field)) {
      // Locate the companion: the companion's `castErrorFor` equals the data field's name.
      val variantStruct = v.dataType.asInstanceOf[StructType]
      val dataFieldName = variantStruct.fields(ordinal).name
      val companionOrdinal = variantStruct.fields.indexWhere { f =>
        VariantMetadata.fromMetadata(f.metadata).castErrorFor.contains(dataFieldName)
      }
      assert(companionOrdinal >= 0,
        s"missing cast-error companion for data field $dataFieldName in ${variantStruct.sql}")
      UnwrapVariantCastError(GetStructField(v, companionOrdinal), value)
    } else {
      value
    }
  }

  def rewriteExpr(
      expr: Expression,
      attributeMap: Map[ExprId, AttributeReference]): Expression = {
    def rewriteAttribute(expr: Expression): Expression = expr.transformDown {
      case a: Attribute => attributeMap.getOrElse(a.exprId, a)
    }

    // Rewrite patterns should be consistent with visit patterns in `collectRequestedFields`.
    expr.transformDown {
      case g@VariantGet(v@StructPathToVariant(fields), path, _, _, _) if path.foldable =>
        // Rewrite the attribute in advance, rather than depending on the last branch to rewrite it.
        // Ww need to avoid the `v@StructPathToVariant(fields)` branch to rewrite the child again.
        accessRequestedField(fields, RequestedVariantField(g), rewriteAttribute(v))
      case c@Cast(v@StructPathToVariant(fields), _, _, _) =>
        accessRequestedField(fields, RequestedVariantField(c), rewriteAttribute(v))
      case i@IsNotNull(StructPath(_, _)) => rewriteAttribute(i)
      case i@IsNull(StructPath(_, _)) => rewriteAttribute(i)
      case v@StructPathToVariant(fields) =>
        accessRequestedField(fields, RequestedVariantField.fullVariant, rewriteAttribute(v))
      case a: Attribute => attributeMap.getOrElse(a.exprId, a)
    }
  }
}

// Push variant into scan by rewriting the variant type with a struct type producing all requested
// fields and rewriting the variant extraction expressions by struct accesses.
// For example, for an input plan:
// - Project [v:a::int, v:b::string, v]
//   - Filter [v:a::int = 1]
//     - Relation [v: variant]
// Rewrite it as:
// - Project [v.0, v.1, v.2]
//   - Filter [v.0 = 1]
//     - Relation [v: struct<0: int, 1: string, 2: variant>]
// The struct fields are annotated with `VariantMetadata` to indicate the extraction path.
object PushVariantIntoScan extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan match {
    // A correlated subquery will be rewritten into join later, and will go through this rule
    // eventually.
    case s: Subquery if s.correlated => plan
    case _ if !SQLConf.get.getConf(SQLConf.PUSH_VARIANT_INTO_SCAN) => plan
    case _ => plan.transformDown {
      case p@PhysicalOperation(projectList, filters,
      relation @ LogicalRelationWithTable(
      hadoopFsRelation@HadoopFsRelation(_, _, _, _, _: ParquetFileFormat, _), _)) =>
        rewritePlan(p, projectList, filters, relation, hadoopFsRelation)
    }
  }

  private def rewritePlan(
      originalPlan: LogicalPlan,
      projectList: Seq[NamedExpression],
      filters: Seq[Expression],
      relation: LogicalRelation,
      hadoopFsRelation: HadoopFsRelation): LogicalPlan = {
    val variants = new VariantInRelation

    val schemaAttributes = relation.resolve(hadoopFsRelation.dataSchema,
      hadoopFsRelation.sparkSession.sessionState.analyzer.resolver)
    val defaultValues = ResolveDefaultColumns.existenceDefaultValues(StructType(
      schemaAttributes.map(a => StructField(a.name, a.dataType, a.nullable, a.metadata))))
    for ((a, defaultValue) <- schemaAttributes.zip(defaultValues)) {
      variants.addVariantFields(a.exprId, a.dataType, defaultValue, Nil)
    }
    if (variants.mapping.isEmpty) return originalPlan

    projectList.foreach(variants.collectRequestedFields)
    filters.foreach(variants.collectRequestedFields)
    // `collectRequestedFields` may have removed all variant columns.
    if (variants.mapping.forall(_._2.isEmpty)) return originalPlan

    val attributeMap = schemaAttributes.map { a =>
      if (variants.mapping.get(a.exprId).exists(_.nonEmpty)) {
        val newType = variants.rewriteType(a.exprId, a.dataType, Nil)
        val newAttr = AttributeReference(a.name, newType, a.nullable, a.metadata)(
          qualifier = a.qualifier)
        (a.exprId, newAttr)
      } else {
        // `relation.resolve` actually returns `Seq[AttributeReference]`, although the return type
        // is `Seq[Attribute]`.
        (a.exprId, a.asInstanceOf[AttributeReference])
      }
    }.toMap
    val newFields = schemaAttributes.map { a =>
      val dataType = attributeMap(a.exprId).dataType
      StructField(a.name, dataType, a.nullable, a.metadata)
    }
    val newOutput = relation.output.map(a => attributeMap.getOrElse(a.exprId, a))

    val newHadoopFsRelation = hadoopFsRelation.copy(dataSchema = StructType(newFields))(
      hadoopFsRelation.sparkSession)
    val newRelation = relation.copy(relation = newHadoopFsRelation, output = newOutput.toIndexedSeq)

    val withFilter = if (filters.nonEmpty) {
      Filter(filters.map(variants.rewriteExpr(_, attributeMap)).reduce(And.apply), newRelation)
    } else {
      newRelation
    }

    val newProjectList = projectList.map { e =>
      val rewritten = variants.rewriteExpr(e, attributeMap)
      rewritten match {
        case n: NamedExpression => n
        // This is when the variant column is directly selected. We replace the attribute reference
        // with a struct access, which is not a `NamedExpression` that `Project` requires. We wrap
        // it with an `Alias`.
        case _ => Alias(rewritten, e.name)(e.exprId, e.qualifier)
      }
    }

    Project(newProjectList, withFilter)
  }
}
