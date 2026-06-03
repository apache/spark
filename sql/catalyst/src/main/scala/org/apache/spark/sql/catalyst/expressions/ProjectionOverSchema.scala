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

import org.apache.spark.SparkException
import org.apache.spark.sql.types._

/**
 * A Scala extractor that projects an expression over a given schema. Data types,
 * field indexes and field counts of complex type extractors and attributes
 * are adjusted to fit the schema. All other expressions are left as-is. This
 * class is motivated by columnar nested schema pruning.
 *
 * @param schema nested column schema
 * @param output output attributes of the data source relation. They are used to filter out
 *               attributes in the schema that do not belong to the current relation.
 */
case class ProjectionOverSchema(schema: StructType, output: AttributeSet) {
  private val fieldNames = schema.fieldNames.toSet

  def unapply(expr: Expression): Option[Expression] = getProjection(expr)

  private def getProjection(expr: Expression): Option[Expression] =
    expr match {
      case a: AttributeReference if fieldNames.contains(a.name) && output.contains(a) =>
        Some(a.copy(dataType = schema(a.name).dataType)(a.exprId, a.qualifier))
      case GetArrayItem(child, arrayItemOrdinal, failOnError) =>
        getProjection(child).map {
          projection => GetArrayItem(projection, arrayItemOrdinal, failOnError)
        }
      case a: GetArrayStructFields =>
        getProjection(a.child).map(p => (p, p.dataType)).map {
          case (projection, ArrayType(projSchema @ StructType(_), _)) =>
            // For case-sensitivity aware field resolution, we should take `ordinal` which
            // points to correct struct field, because `ExtractValue` actually does column
            // name resolving correctly.
            val selectedField = a.child.dataType.asInstanceOf[ArrayType]
              .elementType.asInstanceOf[StructType](a.ordinal)
            val prunedField = projSchema(selectedField.name)
            GetArrayStructFields(projection,
              prunedField.copy(name = a.field.name),
              projSchema.fieldIndex(selectedField.name),
              projSchema.size,
              a.containsNull)
          case (_, projSchema) =>
            throw SparkException.internalError(
              s"unmatched child schema for GetArrayStructFields: ${projSchema.toString}"
            )
        }
      case MapKeys(child) =>
        getProjection(child).map { projection => MapKeys(projection) }
      case MapValues(child) =>
        getProjection(child).map { projection => MapValues(projection) }
      case GetMapValue(child, key) =>
        getProjection(child).map { projection => GetMapValue(projection, key) }
      case transform @ ArrayTransform(argument, lambda: LambdaFunction) =>
        projectArrayHigherOrderFunction(argument, lambda, numElementVariables = 1) {
          (projection, projectedLambda) =>
            transform.copy(argument = projection, function = projectedLambda)
        }
      case exists @ ArrayExists(argument, lambda: LambdaFunction, _) =>
        projectArrayHigherOrderFunction(argument, lambda, numElementVariables = 1) {
          (projection, projectedLambda) =>
            exists.copy(argument = projection, function = projectedLambda)
        }
      case forall @ ArrayForAll(argument, lambda: LambdaFunction) =>
        projectArrayHigherOrderFunction(argument, lambda, numElementVariables = 1) {
          (projection, projectedLambda) =>
            forall.copy(argument = projection, function = projectedLambda)
        }
      case filter @ ArrayFilter(argument, lambda: LambdaFunction) =>
        projectArrayHigherOrderFunction(argument, lambda, numElementVariables = 1) {
          (projection, projectedLambda) =>
            filter.copy(argument = projection, function = projectedLambda)
        }
      case sort @ ArraySort(argument, lambda: LambdaFunction, _) =>
        projectArrayHigherOrderFunction(argument, lambda, numElementVariables = 2) {
          (projection, projectedLambda) =>
            sort.copy(argument = projection, function = projectedLambda)
        }
      case reverse @ Reverse(child) =>
        getProjection(child).map(projection => reverse.copy(child = projection))
      case shuffle @ Shuffle(child, _) =>
        getProjection(child).map(projection => shuffle.copy(child = projection))
      case slice @ Slice(x, _, _) =>
        getProjection(x).map(projection => slice.copy(x = projection))
      case knownNotContainsNull @ KnownNotContainsNull(child) =>
        getProjection(child).map(projection => knownNotContainsNull.copy(child = projection))
      case GetStructFieldObject(child, field: StructField) =>
        getProjection(child).map(p => (p, p.dataType)).map {
          case (projection, projSchema: StructType) =>
            GetStructField(projection, projSchema.fieldIndex(field.name))
          case (_, projSchema) =>
            throw SparkException.internalError(
              s"unmatched child schema for GetStructField: ${projSchema.toString}"
            )
        }
      case ElementAt(left, right, defaultValueOutOfBound, failOnError) if right.foldable =>
        getProjection(left).map(p => ElementAt(p, right, defaultValueOutOfBound, failOnError))
      case _ =>
        None
    }

  private def projectArrayHigherOrderFunction(
      argument: Expression,
      lambda: LambdaFunction,
      numElementVariables: Int)(
      rebuild: (Expression, LambdaFunction) => Expression): Option[Expression] = {
    getProjection(argument).map {
      case projection @ ArrayTypeProjection(projectedElementSchema) =>
        val projectedArguments = lambda.arguments.zipWithIndex.map {
          case (elementVar: NamedLambdaVariable, index) if index < numElementVariables =>
            elementVar.copy(dataType = projectedElementSchema)
          case (argument, _) =>
            argument
        }
        val projectedBody =
          lambda.arguments.zip(projectedArguments).foldLeft(lambda.function) {
            case (body, (elementVar: NamedLambdaVariable, projectedElementVar:
                NamedLambdaVariable)) if elementVar ne projectedElementVar =>
              val lambdaProjection =
                ProjectionOverLambdaVariable(elementVar, projectedElementVar)
              body.transformDown {
                case lambdaProjection(expr) => expr
              }
            case (body, _) =>
              body
          }
        rebuild(
          projection,
          lambda.copy(function = projectedBody, arguments = projectedArguments))
      case projection =>
        rebuild(projection, lambda)
    }
  }

  private object ArrayTypeProjection {
    def unapply(expr: Expression): Option[StructType] = expr.dataType match {
      case ArrayType(projectedElementSchema: StructType, _) => Some(projectedElementSchema)
      case _ => None
    }
  }

  /**
   * Rewrites references rooted at one bound lambda element to use its projected type and
   * recomputes nested field ordinals against each projected struct in the access path.
   * Bound lambda references are matched by exprId because they may be instantiated separately.
   * This must support the same access paths collected by `SchemaPruning` for lambda variables;
   * currently both sides support only `GetStructField` chains.
   */
  private case class ProjectionOverLambdaVariable(
      original: NamedLambdaVariable,
      projected: NamedLambdaVariable) {
    def unapply(expr: Expression): Option[Expression] = project(expr)

    private def project(expr: Expression): Option[Expression] = expr match {
      case variable: NamedLambdaVariable if variable.exprId == original.exprId =>
        Some(projected)
      case GetStructFieldObject(child, field: StructField) =>
        project(child).map { projection =>
          projection.dataType match {
            case projectedSchema: StructType =>
              GetStructField(projection, projectedSchema.fieldIndex(field.name))
            case dataType =>
              throw SparkException.internalError(
                s"unmatched lambda child schema for GetStructField: ${dataType.toString}")
          }
        }
      case _ => None
    }
  }
}
