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

package org.apache.spark.sql.catalyst.analysis

import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark.SparkThrowable
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, Expression, ExtractValue, GetArrayStructFields, GetStructField, HigherOrderFunction, LambdaFunction, Literal, NamedLambdaVariable, PlanExpression}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.COMMAND
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, Identifier, SupportsSchemaEvolution, Table, TableCatalog, TableChange}
import org.apache.spark.sql.connector.catalog.TableChange.ColumnChange
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, ExtractV2CatalogAndIdentifier, ExtractV2Table}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, AtomicType, DataType, MapType, NullType, StructField, StructType}
import org.apache.spark.util.ArrayImplicits.SparkArrayOps


/**
 * A rule that resolves schema evolution for V2 write commands (INSERT, MERGE INTO).
 *
 * This rule will call the DSV2 Catalog to update the schema of the target table.
 */
object ResolveSchemaEvolution extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsWithPruning(
    _.containsPattern(COMMAND)) {
    // This rule should run only if all assignments are resolved, except those
    // that will be satisfied by schema evolution
    case write: WriteWithSchemaEvolution if write.pendingSchemaChanges.nonEmpty =>
      write.table match {
        case relation @ ExtractV2CatalogAndIdentifier(catalog, ident) =>
          evolveSchema(catalog, ident, write.pendingSchemaChanges)
          val newTable = CatalogV2Util.loadTableForV2Write(
            catalog, ident, write.writePrivileges, relation.options)
          val writeWithNewTarget = replaceWriteTarget(write, relation, newTable)

          val remainingChanges = writeWithNewTarget.pendingSchemaChanges
          if (remainingChanges.nonEmpty) {
            throw QueryCompilationErrors.unsupportedAutoSchemaEvolutionChangesError(
              catalog, ident, remainingChanges)
          }

          writeWithNewTarget
        case _ =>
          throw QueryCompilationErrors.unsupportedAutoSchemaEvolutionError(write.table)
      }
  }

  private def evolveSchema(
      catalog: TableCatalog,
      ident: Identifier,
      changes: Seq[TableChange]): Unit = {
    try {
      catalog.alterTable(ident, changes: _*)
    } catch {
      case e: IllegalArgumentException if !e.isInstanceOf[SparkThrowable] =>
        throw QueryExecutionErrors.unsupportedTableChangeError(e)
      case NonFatal(e) =>
        throw QueryCompilationErrors.failedAutoSchemaEvolutionError(catalog, ident, e)
    }
  }

  private def replaceWriteTarget(
      write: WriteWithSchemaEvolution,
      relation: DataSourceV2Relation,
      newTable: Table): WriteWithSchemaEvolution = {
    val oldOutput = write.table.output
    val newOutput = DataTypeUtils.toAttributes(newTable.columns)
    val newRelation = relation.copy(table = newTable, output = newOutput)
    val writeWithNewTargetTable = write.withNewTable(newRelation)
    rebindNestedExtractions(rewriteAttrs(writeWithNewTargetTable, oldOutput, newOutput))
  }

  private def rewriteAttrs[T <: LogicalPlan](
      plan: T,
      oldOutput: Seq[Attribute],
      newOutput: Seq[Attribute]): T = {
    // Match attributes by name rather than position: schema changes may add fields at
    // any position, shifting the ordinal of following columns.
    val newAttrsByName = {
      val byName = newOutput.map(attr => attr.name -> attr).toMap
      if (SQLConf.get.caseSensitiveAnalysis) byName else CaseInsensitiveMap(byName)
    }
    val attrMap = AttributeMap(oldOutput.flatMap { oldAttr =>
      newAttrsByName.get(oldAttr.name).map(oldAttr -> _)
    })
    plan.rewriteAttrs(attrMap).asInstanceOf[T]
  }

  /**
   * Re-resolves field extractions recorded before the target schema changed.
   * Positioned adds shift the ordinals of following siblings, so [[GetStructField]]
   * and [[GetArrayStructFields]] are re-derived by their recorded name; unnamed
   * extractions and fields that no longer resolve are left unchanged.
   */
  private def rebindNestedExtractions[T <: LogicalPlan](plan: T): T = {
    val resolver = conf.resolver

    def rederived(child: Expression, name: String, original: Expression): Expression =
      ExtractValue.extractValue(child, Literal(name), resolver) match {
        case Left(resolved) => resolved
        case Right(_) => original
      }

    def rebind: PartialFunction[Expression, Expression] = {
      case gsf: GetStructField if gsf.name.isDefined =>
        rederived(gsf.child, gsf.name.get, gsf)
      case gasf: GetArrayStructFields =>
        rederived(gasf.child, gasf.field.name, gasf)
      // Lambda variables keep the type they were bound with. Rebinding them from the
      // evolved argument type and re-deriving the body keeps extractions like `x.c`
      // reading the named field after a positioned add inside its struct.
      // bindInternal assumes the same precondition as ResolveLambdaVariables, so
      // ill-typed or unbound functions are left for CheckAnalysis to report.
      case hof: HigherOrderFunction if hof.argumentsResolved &&
          hof.checkArgumentDataTypes().isSuccess &&
          hof.functions.forall {
            case l: LambdaFunction => l.bound
            case _ => false
          } =>
        hof.bind { case (lf: LambdaFunction, argTypes) =>
          val refreshedArgs = (lf.arguments zip argTypes).map {
            case (v: NamedLambdaVariable, (dataType, nullable)) =>
              v.copy(dataType = dataType, nullable = nullable)
            case (arg, _) => arg
          }
          val refreshed = (lf.arguments zip refreshedArgs).collect {
            case (v: NamedLambdaVariable, newV: NamedLambdaVariable)
                if newV.dataType != v.dataType || newV.nullable != v.nullable =>
              v.exprId -> newV
          }.toMap
          if (refreshed.isEmpty) {
            lf
          } else {
            lf.copy(
              function = lf.function.transformUp(rebind orElse {
                case v: NamedLambdaVariable if refreshed.contains(v.exprId) =>
                  refreshed(v.exprId)
              }),
              arguments = refreshedArgs)
          }
        }
      case pe: PlanExpression[LogicalPlan @unchecked] =>
        pe.withNewPlan(pe.plan.transformDown {
          case node => node.transformExpressionsUp(rebind)
        })
    }

    plan.transformExpressionsUp(rebind).asInstanceOf[T]
  }

  /**
   * Computes the set of table changes needed to evolve `targetTable`'s schema
   * to accommodate the `originalSource` schema. Only returns schema changes that are supported by
   * `targetTable`. When `isByName` is true, fields are matched by name. When false, fields are
   * matched by position.
   */
  def computeSupportedSchemaChanges(
      targetTable: LogicalPlan,
      sourceSchema: StructType,
      isByName: Boolean): Seq[TableChange] = {
    val candidateChanges = computeSchemaChanges(
      targetTable.schema,
      sourceSchema,
      isByName)
    val supportedChanges = filterSupportedChanges(targetTable, candidateChanges)
    if (isByName && conf.schemaEvolutionPreserveColumnOrder) {
      assignAddPositions(supportedChanges, targetTable.schema, sourceSchema)
    } else {
      supportedChanges
    }
  }

  /**
   * Computes schema changes between two types, throwing an error with the provided
   * target and source schemas if the types are incompatible.
   */
  private[catalyst] def computeSchemaChanges(
      targetType: StructType,
      sourceType: StructType,
      isByName: Boolean): Seq[TableChange] = {
    computeSchemaChanges(
      targetType,
      sourceType,
      fieldPath = Nil,
      isByName,
      throwError =
        throw QueryExecutionErrors.failedToMergeIncompatibleSchemasError(targetType, sourceType))
  }

  def filterSupportedChanges(
      targetTable: LogicalPlan,
      candidateChanges: Seq[TableChange]): Seq[TableChange] = {
    targetTable match {
      case ExtractV2Table(t: SupportsSchemaEvolution) =>
        candidateChanges.filter {
          case change: ColumnChange => t.supportsColumnChange(change)
          // Reject other table changes.
          case _ => false
        }
      case r: DataSourceV2Relation if r.autoSchemaEvolution =>
        // If a table reports capability [[TableCapability.AUTOMATIC_SCHEMA_EVOLUTION]] but
        // doesn't implement [[SupportsSchemaEvolution]], attempt to apply all changes.
        candidateChanges
      case _ =>
        Seq.empty
    }
  }

  /**
   * Assigns a [[ColumnPosition]] to every [[TableChange.AddColumn]] so that new fields keep
   * the position they occupy in `sourceSchema` instead of being appended at the end. It must
   * run on the final list of changes a table accepts, after filtering and deduplication: an
   * AFTER anchor is valid only when it names a column that exists in `targetSchema` or an
   * AddColumn earlier in the returned list, under the exact spelling that change carries.
   */
  private[catalyst] def assignAddPositions(
      changes: Seq[TableChange],
      targetSchema: StructType,
      sourceSchema: StructType): Seq[TableChange] = {
    val resolver = conf.resolver
    val addedFieldPaths = mutable.ArrayBuffer.empty[Seq[String]]
    changes.map {
      case add: TableChange.AddColumn =>
        val position = newFieldPosition(
          add.fieldNames.toSeq, addedFieldPaths.toSeq, targetSchema, sourceSchema, resolver)
        addedFieldPaths += add.fieldNames.toSeq
        TableChange.addColumn(
          add.fieldNames,
          add.dataType,
          add.isNullable,
          add.comment,
          position,
          add.defaultValue)
      case change => change
    }
  }

  /**
   * Returns the position a new field occupies among its source siblings:
   * [[ColumnPosition.first]] when no preceding sibling exists in the target schema or is
   * itself added earlier, or [[ColumnPosition.after]] the nearest preceding sibling,
   * anchored on the target field's name when the sibling exists there and on the name the
   * earlier AddColumn carries when it does not. Returns null when the field is not found
   * among its source siblings.
   */
  private def newFieldPosition(
      fieldPath: Seq[String],
      addedFieldPaths: Seq[Seq[String]],
      targetSchema: StructType,
      sourceSchema: StructType,
      resolver: Resolver): TableChange.ColumnPosition = {
    val parentPath = fieldPath.init

    def siblingFields(schema: StructType): Seq[StructField] = {
      if (parentPath.isEmpty) {
        schema.fields.toImmutableArraySeq
      } else {
        schema
          .findNestedField(parentPath, includeCollections = true, resolver = resolver)
          .map(_._2.dataType)
          .collect { case struct: StructType => struct.fields.toImmutableArraySeq }
          .getOrElse(Seq.empty)
      }
    }

    val sourceSiblings = siblingFields(sourceSchema)
    val targetSiblings = siblingFields(targetSchema)
    val index = sourceSiblings.indexWhere(f => resolver(f.name, fieldPath.last))
    if (index < 0) {
      null
    } else {
      sourceSiblings.take(index).reverseIterator
        .map { sibling =>
          targetSiblings.find(f => resolver(f.name, sibling.name))
            .map(f => TableChange.ColumnPosition.after(f.name))
            .orElse {
              addedFieldPaths
                .find(added =>
                  added.init.length == parentPath.length &&
                    added.init.lazyZip(parentPath).forall((a, b) => resolver(a, b)) &&
                    resolver(added.last, sibling.name))
                .map(added => TableChange.ColumnPosition.after(added.last))
            }
            .orNull
        }
        .find(_ != null)
        .getOrElse(TableChange.ColumnPosition.first())
    }
  }

  private[catalyst] def computeSchemaChanges(
      currentType: DataType,
      newType: DataType,
      fieldPath: Seq[String],
      isByName: Boolean,
      throwError: => Nothing): Seq[TableChange] = {
    (currentType, newType) match {
      case (StructType(currentFields), StructType(newFields)) =>
        if (isByName) {
          computeSchemaChangesByName(
            currentFields.toImmutableArraySeq,
            newFields.toImmutableArraySeq,
            fieldPath,
            throwError)
        } else {
          computeSchemaChangesByPosition(
            currentFields.toImmutableArraySeq,
            newFields.toImmutableArraySeq,
            fieldPath,
            throwError)
        }

      case (ArrayType(currentElementType, _), ArrayType(newElementType, _)) =>
        computeSchemaChanges(
          currentElementType,
          newElementType,
          fieldPath :+ "element",
          isByName,
          throwError)

      case (MapType(currentKeyType, currentValueType, _),
            MapType(newKeyType, newValueType, _)) =>
        val keyChanges = computeSchemaChanges(
          currentKeyType,
          newKeyType,
          fieldPath :+ "key",
          isByName,
          throwError)
        val valueChanges = computeSchemaChanges(
          currentValueType,
          newValueType,
          fieldPath :+ "value",
          isByName,
          throwError)
        keyChanges ++ valueChanges

      case (currentType: AtomicType, newType: AtomicType) if currentType != newType =>
        Seq(TableChange.updateColumnType(fieldPath.toArray, newType))

      case (currentType, newType) if currentType == newType =>
        // No change needed
        Seq.empty

      case (_, NullType) =>
        // Don't try to change to NullType.
        Seq.empty

      case (_: AtomicType | NullType, newType: AtomicType) =>
        Seq(TableChange.updateColumnType(fieldPath.toArray, newType))

      case _ =>
        // Do not support change between atomic and complex types for now
        throwError
    }
  }

  /**
   * Match fields by name: look up each target field in the source by name to collect schema
   * differences. Nested struct fields are also matched by name.
   */
  private def computeSchemaChangesByName(
      currentFields: Seq[StructField],
      newFields: Seq[StructField],
      fieldPath: Seq[String],
      throwError: => Nothing): Seq[TableChange] = {
    val currentFieldMap = toFieldMap(currentFields)
    val newFieldMap = toFieldMap(newFields)

    // Collect field updates
    val updates = currentFields
      .filter(f => newFieldMap.contains(f.name))
      .flatMap { f =>
        computeSchemaChanges(
          f.dataType,
          newFieldMap(f.name).dataType,
          fieldPath :+ f.name,
          isByName = true,
          throwError)
      }

    // Collect newly added fields
    val adds = newFields
      .collect {
        case f if !currentFieldMap.contains(f.name) =>
          // Make the type nullable, since existing rows in the table will have NULLs
          // for this column.
          TableChange.addColumn(
            (fieldPath :+ f.name).toArray,
            f.dataType.asNullable,
            /* isNullable */ true)
      }

    updates ++ adds
  }

  /**
   * Match fields by position: pair target and source fields in order to collect schema
   * differences. Nested struct fields are also matched by position.
   */
  private def computeSchemaChangesByPosition(
      currentFields: Seq[StructField],
      newFields: Seq[StructField],
      fieldPath: Seq[String],
      throwError: => Nothing): Seq[TableChange] = {
    // Update existing field types by pairing fields at the same position.
    val updates = currentFields.zip(newFields).flatMap { case (currentField, newField) =>
      computeSchemaChanges(
        currentField.dataType,
        newField.dataType,
        fieldPath :+ currentField.name,
        isByName = false,
        throwError)
    }

    // Extra source fields beyond the target's field count are new additions.
    val adds = newFields.drop(currentFields.length)
      .map { f =>
        // Make the type nullable, since existing rows in the table will have NULLs for this column.
        TableChange.addColumn((fieldPath :+ f.name).toArray, f.dataType.asNullable)
      }

    updates ++ adds
  }

  private def toFieldMap(fields: Seq[StructField]): Map[String, StructField] = {
    val fieldMap = fields.map(field => field.name -> field).toMap
    if (SQLConf.get.caseSensitiveAnalysis) {
      fieldMap
    } else {
      CaseInsensitiveMap(fieldMap)
    }
  }
}
