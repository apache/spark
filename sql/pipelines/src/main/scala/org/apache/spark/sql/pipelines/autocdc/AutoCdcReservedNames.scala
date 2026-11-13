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

package org.apache.spark.sql.pipelines.autocdc

import org.apache.spark.SparkException
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructField, StructType}

/**
 * Names that AutoCDC reserves for its own use, both for internal columns it inserts during
 * reconciliation (e.g. `${prefix}metadata`, `${prefix}winning_row`) and for internal tables it
 * manages alongside user-defined targets (e.g. the per-target auxiliary state table).
 *
 * A single recognizable prefix gives a single auditable answer to "what does AutoCDC own", and
 * lets user-defined columns and tables be unambiguously distinguished from AutoCDC-managed ones.
 */
private[pipelines] object AutoCdcReservedNames {

  /** Common reserved-name prefix shared by AutoCDC internal columns and internal tables. */
  val prefix: String = "__spark_autocdc_"

  /**
   * Reserved name of the operational metadata column AutoCDC that is projected on every AutoCDC
   * microbatch, auxiliary table, and target table.
   *
   * Shared across all SCD strategies and across the flow resolution, batch-processor, and
   * streaming-write layers.
   *
   * Note that the schema of the CDC metadata column however can and does differ on the SCD-type.
   */
  val cdcMetadataColName: String = s"${prefix}metadata"

  // A field is engine-owned when its name carries the reserved prefix. Matching goes through the
  // caller's `resolver` so a user-declared column is still recognized as reserved under
  // case-insensitive analysis, and the length guard keeps `substring` safe for shorter names.
  private def isReservedFieldName(name: String, resolver: Resolver): Boolean =
    name.length >= prefix.length && resolver(name.substring(0, prefix.length), prefix)

  /** The engine-owned reserved AUTO CDC column(s) present in `schema`, if any. */
  private[pipelines] def reservedFields(schema: StructType, resolver: Resolver): Seq[StructField] =
    schema.fields.toSeq.filter(f => isReservedFieldName(f.name, resolver))

  /** `schema` with the engine-owned reserved AUTO CDC column(s) removed. */
  private[pipelines] def stripReservedFields(schema: StructType, resolver: Resolver): StructType =
    StructType(schema.fields.filterNot(f => isReservedFieldName(f.name, resolver)))

  /**
   * The schema a materialized AUTO CDC target carries when the user declares a schema: the user's
   * columns with the engine-owned type/nullability substituted into any declared reserved column,
   * followed by the engine-owned reserved column(s) the declaration omitted. `flowInferredSchema`
   * must be flow-derived (no user-declared columns merged in), so its reserved fields are exactly
   * the engine-produced ones and never a user column that happens to carry the prefix.
   *
   * A declared reserved field keeps its own spelling, position, and metadata (e.g. a `COMMENT`);
   * only its type and nullability are replaced with the engine's. That matters on the upgrade path:
   * an already-materialized
   * target may carry the reserved column under a different casing (e.g. an older definition
   * declared it upper-cased under case-insensitive analysis), `evolveTable` merges that existing
   * spelling in, and this schema -- which also feeds the analysis-time read path -- must agree with
   * it so a downstream `SELECT *` plans against the column the target actually has.
   *
   * The reserved column itself is made nullable so a target created from a declared schema matches
   * one created from an omitted schema (that path materializes the inferred schema `asNullable`),
   * avoiding a spurious nullability diff when the column declaration is added or removed. That
   * deliberately overrides a top-level `NOT NULL` a declaration might place on the engine-owned
   * reserved column; parity between the declared and omitted paths is what the omit-the-column
   * capability rests on, and the column is engine-written bookkeeping the user does not populate.
   * Its nested nullability is the union of the declaration's and the engine's: forcing
   * a declared nested `NOT NULL` sequencing field nullable would churn an unchanged, already
   * materialized target, and `evolveTable` cannot apply a nullability change beneath nested arrays
   * (`diffSchemas` rejects it as an unsupported nested-complex-type evolution). Omitted (appended)
   * fields stay fully nullable, matching the omitted-schema path.
   */
  private[pipelines] def appendEngineOwnedReservedFields(
      declaredSchema: StructType,
      flowInferredSchema: StructType,
      resolver: Resolver): StructType = {
    // Engine-owned reserved fields in the engine's actual inferred nullability (not blanket
    // `asNullable`). A declared reserved field reconciles against these, so its nested nullability
    // follows what the engine writes and an unchanged declaration does not churn the target.
    // Omitted fields are made nullable where they are appended below.
    val engineReserved = StructType(reservedFields(flowInferredSchema, resolver)).fields

    // A declaration carries at most one field matching each engine-owned reserved column. In both
    // contexts the declaration is validated first -- at materialization by
    // GraphValidations.validateUserSpecifiedSchemas before DatasetManager materializes, and on the
    // same-graph read path by VirtualTableInput.load just before it calls this method -- and
    // `validateDeclaredReservedFields` rejects a case-insensitive duplicate, so this guard is
    // defense-in-depth.
    engineReserved.foreach { ef =>
      val matches = declaredSchema.fields.filter(df => resolver(df.name, ef.name))
      if (matches.length > 1) {
        throw SparkException.internalError(
          s"Ambiguous reserved AUTO CDC field: ${matches.map(_.name).mkString(", ")} all match " +
            s"the engine-owned column ${ef.name}.")
      }
    }

    // Rebuild the declared schema in place: a declared reserved field keeps its spelling and
    // position but takes the engine-owned type/nullability. Both contexts validate the
    // declaration first (see the duplicate guard above), rejecting a reserved-prefixed field with
    // no engine counterpart, so `case None => df` is defense-in-depth.
    val usedEngineNames = scala.collection.mutable.Set.empty[String]
    val rebuilt = declaredSchema.fields.map { df =>
      if (isReservedFieldName(df.name, resolver)) {
        engineReserved.find(ef => resolver(df.name, ef.name)) match {
          case Some(ef) =>
            usedEngineNames += ef.name
            // Force the reserved column itself nullable to match the omitted-declaration path, so
            // adding or removing the declaration is not a top-level nullability change. Nested
            // fields follow the union below (not the omitted path's blanket nullable), so parity
            // holds only at the reserved column itself.
            reconcileReservedField(df, ef.copy(nullable = true), resolver)
          case None => df
        }
      } else {
        df
      }
    }

    // Append engine-owned reserved fields the declaration omitted, in canonical form and fully
    // nullable to match the no-declaration path (DatasetManager materializes the inferred schema
    // `asNullable`).
    val appended =
      StructType(engineReserved.filterNot(ef => usedEngineNames.contains(ef.name)))
        .asNullable.fields
    StructType(rebuilt ++ appended)
  }

  /**
   * Reject a declared AUTO CDC target schema whose reserved metadata column(s) are shaped in a way
   * reconciliation cannot honor, so `appendEngineOwnedReservedFields` and the casing-drift check
   * can assume a well-formed declaration instead of a guard elsewhere. Both callers that
   * reconcile run this first: `GraphValidations.validateUserSpecifiedSchemas` before
   * materialization, and
   * `VirtualTableInput.load` on the same-graph read path, which runs during flow analysis -- before
   * graph validation -- and so must validate the declaration itself rather than rely on that later
   * pass. Recursively through structs, array elements, and map values it throws a user-facing
   * `AnalysisException` when a declared reserved-prefixed field has no engine-owned counterpart (a
   * typo or an extra reserved-namespace column), when two declared fields resolve to one engine
   * field, or when a declared nested field under a reserved column is not one the engine produces.
   */
  private[pipelines] def validateDeclaredReservedFields(
      tableName: String,
      declaredSchema: StructType,
      flowInferredSchema: StructType,
      resolver: Resolver): Unit =
    validateReservedLevel(
      tableName,
      reservedColumn = None,
      reservedFields(declaredSchema, resolver),
      reservedFields(flowInferredSchema, resolver),
      resolver)

  // Validate one level of reserved fields against the engine's. `reservedColumn` is the enclosing
  // reserved column's (possibly dotted) name for nested-field messages, None at the top level. At
  // the top level `declared`/`engine` are the reserved-prefixed columns; within a reserved struct
  // every field is engine-owned, so the recursion passes all of a matched struct's fields.
  private def validateReservedLevel(
      tableName: String,
      reservedColumn: Option[String],
      declared: Seq[StructField],
      engine: Seq[StructField],
      resolver: Resolver): Unit = {
    declared.filterNot(df => engine.exists(ef => resolver(df.name, ef.name))).foreach { df =>
      reservedColumn match {
        case None =>
          throw new AnalysisException(
            "AUTOCDC_INVALID_RESERVED_SCHEMA.UNKNOWN_COLUMN",
            Map("tableName" -> tableName, "columnName" -> df.name,
              "reservedColumnNamePrefix" -> prefix))
        case Some(col) =>
          throw new AnalysisException(
            "AUTOCDC_INVALID_RESERVED_SCHEMA.UNKNOWN_NESTED_FIELD",
            Map("tableName" -> tableName, "reservedColumnName" -> col, "fieldName" -> df.name))
      }
    }
    engine.foreach { ef =>
      val matches = declared.filter(df => resolver(df.name, ef.name))
      if (matches.length > 1) {
        val names = matches.map(f => s"`${f.name}`").mkString(", ")
        reservedColumn match {
          case None =>
            throw new AnalysisException(
              "AUTOCDC_INVALID_RESERVED_SCHEMA.DUPLICATE_COLUMN",
              Map("tableName" -> tableName, "columnNames" -> names, "engineColumnName" -> ef.name))
          case Some(col) =>
            throw new AnalysisException(
              "AUTOCDC_INVALID_RESERVED_SCHEMA.DUPLICATE_NESTED_FIELD",
              Map("tableName" -> tableName, "reservedColumnName" -> col, "fieldNames" -> names,
                "engineFieldName" -> ef.name))
        }
      }
    }
    declared.foreach { df =>
      engine.find(ef => resolver(df.name, ef.name)).foreach { ef =>
        val col = Some(reservedColumn.map(c => s"$c.${df.name}").getOrElse(df.name))
        validateReservedType(tableName, col, df.dataType, ef.dataType, resolver)
      }
    }
  }

  // Descend a matched reserved field's type to validate its nested fields: a struct checks its
  // fields one level down, an array recurses into its element type, and a map recurses into its
  // key and value types, so the struct inside an arbitrarily nested sequencing type (e.g.
  // `ARRAY<ARRAY<STRUCT<...>>>`, or the SCD1 `__VERSION_MAP: MAP<STRING, <sequencing type>>`) is
  // still reached. This mirrors how `reconcileReservedType` descends, so validation rejects
  // exactly the nested shapes reconciliation would otherwise carry into the target. Any other type
  // has no reserved nested field to check.
  private def validateReservedType(
      tableName: String,
      reservedColumn: Option[String],
      declared: DataType,
      engine: DataType,
      resolver: Resolver): Unit =
    (declared, engine) match {
      case (ds: StructType, es: StructType) =>
        validateReservedLevel(tableName, reservedColumn, ds.fields.toSeq, es.fields.toSeq, resolver)
      case (da: ArrayType, ea: ArrayType) =>
        validateReservedType(tableName, reservedColumn, da.elementType, ea.elementType, resolver)
      case (dm: MapType, em: MapType) =>
        validateReservedType(tableName, reservedColumn, dm.keyType, em.keyType, resolver)
        validateReservedType(tableName, reservedColumn, dm.valueType, em.valueType, resolver)
      case _ =>
    }

  /**
   * Reconcile a declared reserved field against its engine-owned counterpart: keep the declared
   * field's spelling and its metadata (e.g. a user `COMMENT`), take the engine field's type and the
   * union of the declared and engine nullability, and reconcile the types recursively (see
   * `reconcileReservedType`) so declared nested spelling survives structs, arrays, and maps.
   * Building from the declared field is what keeps the comment: `SchemaInferenceUtils` merges the
   * declaration in to carry such metadata, and dropping it here would lose it from the created
   * target and the analysis-time read path.
   */
  private def reconcileReservedField(
      declared: StructField,
      engine: StructField,
      resolver: Resolver): StructField =
    declared.copy(
      dataType = reconcileReservedType(declared.dataType, engine.dataType, resolver),
      // Union so a declared `NOT NULL` the engine also produces is preserved (an unchanged target
      // does not churn), while a field the engine may null is widened to nullable.
      nullable = declared.nullable || engine.nullable)

  /**
   * Reconcile a declared data type against its engine-owned counterpart: keep the declared nested
   * spelling and the engine's field ordering and types, and union the declared and engine
   * nullability (`containsNull` for arrays, `valueContainsNull` for maps). Recurse through
   * structs -- the AUTO CDC metadata column is one -- through array element types, since a
   * sequencing column can be `ARRAY<STRUCT<...>>`, and through map key/value types, since the SCD1
   * `__VERSION_MAP` is a `MAP<STRING, <sequencing type>>`; any other shape takes the engine's type
   * as-is.
   */
  private def reconcileReservedType(
      declared: DataType,
      engine: DataType,
      resolver: Resolver): DataType =
    (declared, engine) match {
      case (declaredStruct: StructType, engineStruct: StructType) =>
        reconcileReservedStruct(declaredStruct, engineStruct, resolver)
      case (declaredArray: ArrayType, engineArray: ArrayType) =>
        engineArray.copy(
          elementType =
            reconcileReservedType(declaredArray.elementType, engineArray.elementType, resolver),
          // Union `containsNull` for the same reason as field nullability: keep a declared
          // non-nullable element the engine also produces so an unchanged target does not churn.
          containsNull = declaredArray.containsNull || engineArray.containsNull)
      case (declaredMap: MapType, engineMap: MapType) =>
        engineMap.copy(
          keyType = reconcileReservedType(declaredMap.keyType, engineMap.keyType, resolver),
          valueType = reconcileReservedType(declaredMap.valueType, engineMap.valueType, resolver),
          // Union `valueContainsNull` for the same reason as array `containsNull`.
          valueContainsNull = declaredMap.valueContainsNull || engineMap.valueContainsNull)
      case _ => engine
    }

  /**
   * Rebuild `engine`'s struct in the engine's field order, carrying the declared nested spelling
   * and metadata: each engine nested field takes a matching declared field's name and comment
   * (recursively); an engine nested field the declaration omitted is kept fully nullable
   * (`asNullable`), matching the omitted whole-column path so a later run that drops the
   * declaration does not need to relax nullability beneath a nested array or map (which
   * `diffSchemas` rejects). The order is the engine's, not the declaration's. These structs are
   * engine-owned, and a sequencing
   * struct -- the type of a reserved sequencing field (`${prefix}metadata.upsertSequence` /
   * `deleteSequence` for SCD1, `recordStartAt` for SCD2) when `SEQUENCE BY` yields a struct -- is
   * compared positionally during AUTO CDC processing (struct ordering is name-blind). A partial or
   * reordered declaration must therefore not change the engine's field order: the target stores the
   * sequencing struct in that order, and a mismatch against the engine-ordered microbatch would
   * mis-pair the fields -- letting an older event overwrite a newer one in an SCD1 merge, or
   * mis-ordering the history intervals in SCD2 (SPARK-58118). A declared nested field with no
   * engine counterpart is rejected upstream by `validateReservedLevel`, so the trailing
   * `filterNot` is defense-in-depth, unreachable from a validated declaration.
   */
  private def reconcileReservedStruct(
      declared: StructType,
      engine: StructType,
      resolver: Resolver): StructType = {
    val usedDeclaredNames = scala.collection.mutable.Set.empty[String]
    val rebuilt = engine.fields.map { ef =>
      declared.fields.find(df => resolver(df.name, ef.name)) match {
        case Some(df) =>
          usedDeclaredNames += df.name
          reconcileReservedField(df, ef, resolver)
        case None =>
          // An engine nested field the declaration omitted: append it fully nullable, matching the
          // omitted whole-column path (`appendEngineOwnedReservedFields` uses `.asNullable`). A
          // partial declaration must not leave it at the engine's non-null nesting, or a later run
          // that drops the declaration would need to relax that nullability beneath a nested
          // array/map -- which `diffSchemas` rejects as an unsupported nested-complex-type
          // evolution.
          ef.copy(dataType = ef.dataType.asNullable, nullable = true)
      }
    }
    StructType(rebuilt ++ declared.fields.filterNot(df => usedDeclaredNames.contains(df.name)))
  }

  /**
   * Reserved AUTO CDC fields whose persisted spelling in `existingSchema` differs, case only, from
   * the spelling this run gives them in `desiredSchema`, as `(existingName, desiredName)` pairs.
   *
   * On an incremental run the target's schema merge keeps the existing field spelling, while the
   * analysis-time read path plans a downstream consumer against `desiredName`. A case-only
   * difference between the two leaves the planned and persisted schemas disagreeing on the reserved
   * column, so a case-sensitive downstream `SELECT *` cannot match them. The caller rejects such a
   * run rather than let that mismatch surface as an opaque failure during execution.
   */
  private[pipelines] def reservedFieldCasingDrifts(
      existingSchema: StructType,
      desiredSchema: StructType,
      resolver: Resolver): Seq[(String, String)] =
    reservedFields(existingSchema, resolver).flatMap { existingField =>
      desiredSchema.fields
        .find(desiredField => resolver(desiredField.name, existingField.name))
        .toSeq
        .flatMap(desiredField => fieldCasingDrifts(existingField, desiredField, resolver))
    }

  /**
   * Case-only spelling differences between `existing` and `desired` for a reserved field and,
   * recursively, its nested fields, as `(existingName, desiredName)` pairs. Nested names are
   * qualified by their parent, e.g. `__spark_autocdc_metadata.DELETESEQUENCE`, so the same drift
   * policy covers the metadata column's nested fields -- through structs, arrays, and maps -- as
   * well as the column itself.
   */
  private def fieldCasingDrifts(
      existing: StructField,
      desired: StructField,
      resolver: Resolver): Seq[(String, String)] = {
    val outer =
      if (existing.name != desired.name) Seq((existing.name, desired.name)) else Seq.empty
    val nested = typeCasingDrifts(existing.dataType, desired.dataType, resolver)
      .map { case (e, r) => (s"${existing.name}.$e", s"${desired.name}.$r") }
    outer ++ nested
  }

  /**
   * Case-only spelling drifts among the nested fields of two data types, as `(existingName,
   * desiredName)` pairs qualified by their path. Recurses through structs, through array element
   * types, and through map key/value types, so a sequencing column such as `ARRAY<STRUCT<...>>` or
   * the SCD1 `__VERSION_MAP: MAP<STRING, <sequencing type>>` is covered as well as a nested struct.
   * Any other shape has no nested names to drift.
   */
  private def typeCasingDrifts(
      existing: DataType,
      desired: DataType,
      resolver: Resolver): Seq[(String, String)] =
    (existing, desired) match {
      case (existingStruct: StructType, desiredStruct: StructType) =>
        existingStruct.fields.toSeq.flatMap { existingNested =>
          desiredStruct.fields
            .find(f => resolver(f.name, existingNested.name))
            .toSeq
            .flatMap(desiredNested => fieldCasingDrifts(existingNested, desiredNested, resolver))
        }
      case (existingArray: ArrayType, desiredArray: ArrayType) =>
        // Mark the array hop so a nested drift reads `col.element.field`, matching diffSchemas.
        typeCasingDrifts(existingArray.elementType, desiredArray.elementType, resolver)
          .map { case (e, r) => (s"element.$e", s"element.$r") }
      case (existingMap: MapType, desiredMap: MapType) =>
        // Mark the map hop with `key`/`value` so a nested drift reads `col.value.field`, matching
        // diffSchemas (SchemaInferenceUtils uses `key`/`value` path segments for maps).
        typeCasingDrifts(existingMap.keyType, desiredMap.keyType, resolver)
          .map { case (e, r) => (s"key.$e", s"key.$r") } ++
          typeCasingDrifts(existingMap.valueType, desiredMap.valueType, resolver)
            .map { case (e, r) => (s"value.$e", s"value.$r") }
      case _ => Seq.empty
    }
}
