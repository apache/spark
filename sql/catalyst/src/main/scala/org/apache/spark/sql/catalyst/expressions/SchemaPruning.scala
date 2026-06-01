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

package org.apache.spark.sql.catalyst.expressions

import java.util.Locale

import scala.collection.immutable.HashMap

import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.types._

object SchemaPruning extends SQLConfHelper {
  /**
   * Prunes the nested schema by the requested fields. For example, if the schema is:
   * `id int, s struct<a:int, b:int>`, and given requested field "s.a", the inner field "b"
   * is pruned in the returned schema: `id int, s struct<a:int>`.
   * Note that:
   *   1. The schema field ordering at original schema is still preserved in pruned schema.
   *   2. The top-level fields are not pruned here.
   */
  def pruneSchema(
      schema: StructType,
      requestedRootFields: Seq[RootField]): StructType = {
    val resolver = conf.resolver
    // Merge the requested root fields into a single schema. Note the ordering of the fields
    // in the resulting schema may differ from their ordering in the logical relation's
    // original schema
    val mergedSchema = requestedRootFields
      .map { root: RootField => StructType(Array(root.field)) }
      .reduceLeft(_ merge _)
    val mergedDataSchema =
      StructType(schema.map(d => mergedSchema.find(m => resolver(m.name, d.name)).getOrElse(d)))
    // Sort the fields of mergedDataSchema according to their order in dataSchema,
    // recursively. This makes mergedDataSchema a pruned schema of dataSchema
    sortLeftFieldsByRight(mergedDataSchema, schema).asInstanceOf[StructType]
  }

  /**
   * Sorts the fields and descendant fields of structs in left according to their order in
   * right. This function assumes that the fields of left are a subset of the fields of
   * right, recursively. That is, left is a "subschema" of right, ignoring order of
   * fields.
   */
  private def sortLeftFieldsByRight(left: DataType, right: DataType): DataType =
    (left, right) match {
      case _ if left == right => left
      case (ArrayType(leftElementType, containsNull), ArrayType(rightElementType, _)) =>
        ArrayType(
          sortLeftFieldsByRight(leftElementType, rightElementType),
          containsNull)
      case (MapType(leftKeyType, leftValueType, containsNull),
          MapType(rightKeyType, rightValueType, _)) =>
        MapType(
          sortLeftFieldsByRight(leftKeyType, rightKeyType),
          sortLeftFieldsByRight(leftValueType, rightValueType),
          containsNull)
      case (leftStruct: StructType, rightStruct: StructType) =>
        val formatFieldName: String => String =
          if (conf.caseSensitiveAnalysis) identity else _.toLowerCase(Locale.ROOT)

        val leftStructHashMap =
          HashMap(leftStruct.map(f => formatFieldName(f.name)).zip(leftStruct): _*)
        val sortedLeftFields = rightStruct.fieldNames.flatMap { fieldName =>
          val formattedFieldName = formatFieldName(fieldName)
          if (leftStructHashMap.contains(formattedFieldName)) {
            val resolvedLeftStruct = leftStructHashMap(formattedFieldName)
            val leftFieldType = resolvedLeftStruct.dataType
            val rightFieldType = rightStruct(fieldName).dataType
            val sortedLeftFieldType = sortLeftFieldsByRight(leftFieldType, rightFieldType)
            Some(StructField(fieldName, sortedLeftFieldType, nullable = resolvedLeftStruct.nullable,
              metadata = resolvedLeftStruct.metadata))
          } else {
            None
          }
        }
        StructType(sortedLeftFields)
      case _ => left
    }

  /**
   * Returns the set of fields from projection and filtering predicates that the query plan needs.
   */
  def identifyRootFields(
      projects: Seq[NamedExpression],
      filters: Seq[Expression]): Seq[RootField] = {
    val projectionRootFields = projects.flatMap(getRootFields)
    val filterRootFields = filters.flatMap(getRootFields)

    // Kind of expressions don't need to access any fields of a root fields, e.g., `IsNotNull`.
    // For them, if there are any nested fields accessed in the query, we don't need to add root
    // field access of above expressions.
    // For example, for a query `SELECT name.first FROM contacts WHERE name IS NOT NULL`,
    // we don't need to read nested fields of `name` struct other than `first` field.
    val (rootFields, optRootFields) = (projectionRootFields ++ filterRootFields)
      .distinct.partition(!_.prunedIfAnyChildAccessed)

    optRootFields.filter { opt =>
      !rootFields.exists { root =>
        root.field.name == opt.field.name && {
          // Checking if current optional root field can be pruned.
          // For each required root field, we merge it with the optional root field:
          // 1. If this optional root field has nested fields and any nested field of it is used
          //    in the query, the merged field type must equal to the optional root field type.
          //    We can prune this optional root field. For example, for optional root field
          //    `struct<name:struct<middle:string,last:string>>`, if its field
          //    `struct<name:struct<last:string>>` is used, we don't need to add this optional
          //    root field.
          // 2. If this optional root field has no nested fields, the merged field type equals
          //    to the optional root field only if they are the same. If they are, we can prune
          //    this optional root field too.
          val rootFieldType = StructType(Array(root.field))
          val optFieldType = StructType(Array(opt.field))
          val merged = optFieldType.merge(rootFieldType)
          DataTypeUtils.sameType(merged, optFieldType)
        }
      }
    } ++ rootFields
  }

  /**
   * Gets the root (aka top-level, no-parent) [[StructField]]s for the given [[Expression]].
   * When expr is an [[Attribute]], construct a field around it and indicate that that
   * field was derived from an attribute.
   */
  private[catalyst] def getRootFields(expr: Expression): Seq[RootField] = {
    expr match {
      case ArrayTransform(argument, lambda: LambdaFunction) =>
        getArrayHigherOrderFunctionRootFields(expr, argument, lambda)
      case ArrayExists(argument, lambda: LambdaFunction, _) =>
        getArrayHigherOrderFunctionRootFields(expr, argument, lambda)
      case ArrayForAll(argument, lambda: LambdaFunction) =>
        getArrayHigherOrderFunctionRootFields(expr, argument, lambda)
      case att: Attribute =>
        RootField(StructField(att.name, att.dataType, att.nullable, att.metadata),
          derivedFromAtt = true) :: Nil
      case SelectedField(field) =>
        RootField(field, derivedFromAtt = false) +:
          getArrayReturningHigherOrderFunctionRootFields(expr)
      // Root field accesses by `IsNotNull` and `IsNull` are special cases as the expressions
      // don't actually use any nested fields. These root field accesses might be excluded later
      // if there are any nested fields accesses in the query plan.
      case IsNotNull(SelectedField(field)) =>
        RootField(field, derivedFromAtt = false, prunedIfAnyChildAccessed = true) :: Nil
      case IsNull(SelectedField(field)) =>
        RootField(field, derivedFromAtt = false, prunedIfAnyChildAccessed = true) :: Nil
      case IsNotNull(_: Attribute) | IsNull(_: Attribute) =>
        expr.children.flatMap(getRootFields).map(_.copy(prunedIfAnyChildAccessed = true))
      case s: SubqueryExpression =>
        // use subquery references that only include outer attrs and
        // ignore join conditions as those may include attributes from other tables
        s.references.toSeq.flatMap(getRootFields)
      case _ =>
        expr.children.flatMap(getRootFields)
    }
  }

  private def getArrayHigherOrderFunctionRootFields(
      expr: Expression,
      argument: Expression,
      lambda: LambdaFunction): Seq[RootField] = {
    // Field accesses through the lambda variable are not directly rooted at the input
    // attribute. Convert them into a projected type for the array argument so that
    // physical nested column pruning can see them.
    val nestedRootFields = lambda.arguments.headOption.collect {
      case elementVar: NamedLambdaVariable =>
        getArrayHigherOrderFunctionRootField(argument, lambda.function, elementVar)
    }.flatten.toSeq.map(field => RootField(field, derivedFromAtt = false))
    if (nestedRootFields.nonEmpty) {
      nestedRootFields ++ getRootFields(lambda.function) ++
        getArrayReturningHigherOrderFunctionRootFields(argument)
    } else {
      expr.children.flatMap(getRootFields)
    }
  }

  private def getArrayHigherOrderFunctionRootField(
      argument: Expression,
      function: Expression,
      elementVar: NamedLambdaVariable): Option[StructField] = {
    argument.dataType match {
      case ArrayType(_: StructType, containsNull) =>
        val selectedFields = collectLambdaVariableFields(function, elementVar)
        if (selectedFields.exists(_.nonEmpty)) {
          val mergedElementSchema = selectedFields
            .get
            .map(field => StructType(Array(field)))
            .reduceLeft(_ merge _)
          SelectedField.withDataType(
            argument,
            ArrayType(mergedElementSchema, containsNull))
        } else {
          None
        }
      case _ => None
    }
  }

  private def getArrayReturningHigherOrderFunctionRootFields(expr: Expression): Seq[RootField] = {
    expr match {
      case ArrayFilter(argument, lambda: LambdaFunction) =>
        getArrayReturningHigherOrderFunctionRootFields(argument, lambda, numElementVariables = 1)
      case ArraySort(argument, lambda: LambdaFunction, _) =>
        getArrayReturningHigherOrderFunctionRootFields(argument, lambda, numElementVariables = 2)
      case _ =>
        expr.children.flatMap(getArrayReturningHigherOrderFunctionRootFields)
    }
  }

  private def getArrayReturningHigherOrderFunctionRootFields(
      argument: Expression,
      lambda: LambdaFunction,
      numElementVariables: Int): Seq[RootField] = {
    val nestedRootFields = argument.dataType match {
      case ArrayType(_: StructType, containsNull) =>
        val elementVariables = lambda.arguments.take(numElementVariables).collect {
          case elementVar: NamedLambdaVariable => elementVar
        }
        val selectedFields = elementVariables.map(collectLambdaVariableFields(lambda.function, _))
        if (elementVariables.length == numElementVariables && selectedFields.forall(_.isDefined)) {
          val fields = selectedFields.flatten.flatten
          if (fields.nonEmpty) {
            val mergedElementSchema = fields
              .map(field => StructType(Array(field)))
              .reduceLeft(_ merge _)
            SelectedField.withDataType(
              argument,
              ArrayType(mergedElementSchema, containsNull)).toSeq match {
              case Seq() => getRootFields(argument).map(_.field)
              case fields => fields
            }
          } else {
            Seq.empty
          }
        } else {
          getRootFields(argument).map(_.field)
        }
      case _ =>
        Seq.empty
    }
    nestedRootFields.map(field => RootField(field, derivedFromAtt = false)) ++
      getRootFields(lambda.function) ++
      getArrayReturningHigherOrderFunctionRootFields(argument)
  }

  /**
   * Collects statically identifiable nested fields read from `elementVar`.
   *
   * `Some(Seq.empty)` means this subtree does not reference the element variable, and
   * `Some(fields)` means every reference can be satisfied by the listed nested fields. `None`
   * means the full element is required somewhere (for example, `x => struct(x.a, x)`), so it is
   * not safe to prune the element struct.
   *
   * Currently only `GetStructField` chains rooted at `elementVar` are collected; array or map
   * traversal within the lambda conservatively requires the full element. Keep this set of
   * supported paths in sync with `ProjectionOverLambdaVariable` in `ProjectionOverSchema`.
   */
  private def collectLambdaVariableFields(
      expr: Expression,
      elementVar: NamedLambdaVariable): Option[Seq[StructField]] = {
    expr match {
      case LambdaVariableField(field, variable) if variable.semanticEquals(elementVar) =>
        Some(field :: Nil)
      case IsNotNull(variable: NamedLambdaVariable) if variable.semanticEquals(elementVar) =>
        Some(Seq.empty)
      case IsNull(variable: NamedLambdaVariable) if variable.semanticEquals(elementVar) =>
        Some(Seq.empty)
      case variable: NamedLambdaVariable if variable.semanticEquals(elementVar) =>
        None
      case _ =>
        expr.children.foldLeft(Option(Seq.empty[StructField])) {
          case (Some(fields), child) =>
            collectLambdaVariableFields(child, elementVar).map(fields ++ _)
          case (None, _) => None
        }
    }
  }

  /**
   * Converts a field access rooted at the lambda element into the single nested
   * [[StructField]] shape needed by the input array schema. For example,
   * `x.company.address` becomes `company: struct<address: ...>`.
   */
  private object LambdaVariableField {
    def unapply(expr: Expression): Option[(StructField, NamedLambdaVariable)] = {
      def selectField(
          expression: Expression,
          dataTypeOpt: Option[DataType]): Option[(StructField, NamedLambdaVariable)] =
        expression match {
        case variable: NamedLambdaVariable =>
          dataTypeOpt.collect {
            case schema: StructType if schema.length == 1 =>
              schema.head -> variable
          }
        case getStructField: GetStructField =>
          val field = getStructField.childSchema(getStructField.ordinal)
          val newField = field.copy(dataType = dataTypeOpt.getOrElse(field.dataType))
          selectField(getStructField.child, Some(StructType(Array(newField))))
        case _ => None
      }

      selectField(expr, None)
    }
  }

  /**
   * This represents a "root" schema field (aka top-level, no-parent). `field` is the
   * `StructField` for field name and datatype. `derivedFromAtt` indicates whether it
   * was derived from an attribute or had a proper child. `prunedIfAnyChildAccessed` means
   * whether this root field can be pruned if any of child field is used in the query.
   */
  case class RootField(field: StructField, derivedFromAtt: Boolean,
    prunedIfAnyChildAccessed: Boolean = false)
}
