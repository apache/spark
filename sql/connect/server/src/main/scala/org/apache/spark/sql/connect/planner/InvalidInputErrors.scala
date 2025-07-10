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

package org.apache.spark.sql.connect.planner

import scala.collection.mutable

import com.google.protobuf.Descriptors.Descriptor
import com.google.protobuf.Internal.EnumLite
import com.google.protobuf.ProtocolMessageEnum

import org.apache.spark.connect.proto
import org.apache.spark.sql.connect.common.InvalidPlanInput
import org.apache.spark.sql.errors.DataTypeErrors.{quoteByDefault, toSQLType}
import org.apache.spark.sql.types.DataType

object InvalidInputErrors {

  def noHandlerFoundForExtension(): InvalidPlanInput =
    InvalidPlanInput("No handler found for extension")

  def invalidSQLWithReferences(query: proto.WithRelations): InvalidPlanInput =
    InvalidPlanInput(s"$query is not a valid relation for SQL with references")

  def naFillValuesEmpty(): InvalidPlanInput =
    InvalidPlanInput("values must contains at least 1 item!")

  def naFillValuesLengthMismatch(): InvalidPlanInput =
    InvalidPlanInput(
      "When values contains more than 1 items, values and cols should have the same length!")

  def deduplicateNeedsInput(): InvalidPlanInput =
    InvalidPlanInput("Deduplicate needs a plan input")

  def deduplicateAllColumnsAndSubset(): InvalidPlanInput =
    InvalidPlanInput("Cannot deduplicate on both all columns and a subset of columns")

  def deduplicateRequiresColumnsOrAll(): InvalidPlanInput =
    InvalidPlanInput(
      "Deduplicate requires to either deduplicate on all columns or a subset of columns")

  def invalidDeduplicateColumn(colName: String): InvalidPlanInput =
    InvalidPlanInput(s"Invalid deduplicate column $colName")

  def functionEvalTypeNotSupported(evalType: Int): InvalidPlanInput =
    InvalidPlanInput(s"Function with EvalType: $evalType is not supported")

  def groupingExpressionAbsentForKeyValueGroupedDataset(): InvalidPlanInput =
    InvalidPlanInput("The grouping expression cannot be absent for KeyValueGroupedDataset")

  def expectingScalaUdfButGot(exprType: proto.Expression.ExprTypeCase): InvalidPlanInput =
    InvalidPlanInput(s"Expecting a Scala UDF, but get $exprType")

  def rowNotSupportedForUdf(errorType: String): InvalidPlanInput =
    InvalidPlanInput(s"Row is not a supported $errorType type for this UDF.")

  def notFoundCachedLocalRelation(hash: String, sessionUUID: String): InvalidPlanInput =
    InvalidPlanInput(
      s"Not found any cached local relation with the hash: " +
        s"$hash in the session with sessionUUID $sessionUUID.")

  def withColumnsRequireSingleNamePart(got: String): InvalidPlanInput =
    InvalidPlanInput(s"WithColumns require column name only contains one name part, but got $got")

  def inputDataForLocalRelationNoSchema(): InvalidPlanInput =
    InvalidPlanInput("Input data for LocalRelation does not produce a schema.")

  def schemaRequiredForLocalRelation(): InvalidPlanInput =
    InvalidPlanInput("Schema for LocalRelation is required when the input data is not provided.")

  def invalidSchemaStringNonStructType(schema: String, dataType: DataType): InvalidPlanInput =
    InvalidPlanInput(
      "INVALID_SCHEMA.NON_STRUCT_TYPE",
      Map("inputSchema" -> quoteByDefault(schema), "dataType" -> toSQLType(dataType)))

  def invalidJdbcParams(): InvalidPlanInput =
    InvalidPlanInput("Invalid jdbc params, please specify jdbc url and table.")

  def predicatesNotSupportedForDataSource(format: String): InvalidPlanInput =
    InvalidPlanInput(s"Predicates are not supported for $format data sources.")

  def multiplePathsNotSupportedForStreamingSource(): InvalidPlanInput =
    InvalidPlanInput("Multiple paths are not supported for streaming source")

  def invalidEnum(protoEnum: Enum[_] with ProtocolMessageEnum): InvalidPlanInput =
    InvalidPlanInput(
      s"This enum value of ${protoEnum.getDescriptorForType.getFullName}" +
        s" is invalid: ${protoEnum.name()}(${protoEnum.getNumber})")

  def invalidOneOfField(
      enumCase: Enum[_] with EnumLite,
      descriptor: Descriptor): InvalidPlanInput = {
    // If the oneOf field is not set, the enum number will be 0.
    if (enumCase.getNumber == 0) {
      InvalidPlanInput(
        s"This oneOf field in ${descriptor.getFullName} is not set: ${enumCase.name()}")
    } else {
      InvalidPlanInput(
        s"This oneOf field message in ${descriptor.getFullName} is not supported: " +
          s"${enumCase.name()}(${enumCase.getNumber})")
    }
  }

  def cannotBeEmpty(fieldName: String, descriptor: Descriptor): InvalidPlanInput =
    InvalidPlanInput(s"$fieldName in ${descriptor.getFullName} cannot be empty")

  def invalidSchemaTypeNonStruct(dataType: DataType): InvalidPlanInput =
    InvalidPlanInput("INVALID_SCHEMA_TYPE_NON_STRUCT", Map("dataType" -> toSQLType(dataType)))

  def lambdaFunctionArgumentCountInvalid(got: Int): InvalidPlanInput =
    InvalidPlanInput(s"LambdaFunction requires 1 ~ 3 arguments, but got $got ones!")

  def aliasWithMultipleIdentifiersAndMetadata(): InvalidPlanInput =
    InvalidPlanInput(
      "Alias expressions with more than 1 identifier must not use optional metadata.")

  def unresolvedStarTargetInvalid(target: String): InvalidPlanInput =
    InvalidPlanInput(
      s"UnresolvedStar requires a unparsed target ending with '.*', but got $target.")

  def unresolvedStarWithBothTargetAndPlanId(): InvalidPlanInput =
    InvalidPlanInput("UnresolvedStar with both target and plan id is not supported.")

  def windowFunctionRequired(): InvalidPlanInput =
    InvalidPlanInput("WindowFunction is required in WindowExpression")

  def lowerBoundRequiredInWindowFrame(): InvalidPlanInput =
    InvalidPlanInput("LowerBound is required in WindowFrame")

  def upperBoundRequiredInWindowFrame(): InvalidPlanInput =
    InvalidPlanInput("UpperBound is required in WindowFrame")

  def setOperationMustHaveTwoInputs(): InvalidPlanInput =
    InvalidPlanInput("Set operation must have 2 inputs")

  def exceptDoesNotSupportUnionByName(): InvalidPlanInput =
    InvalidPlanInput("Except does not support union_by_name")

  def intersectDoesNotSupportUnionByName(): InvalidPlanInput =
    InvalidPlanInput("Intersect does not support union_by_name")

  def aggregateNeedsPlanInput(): InvalidPlanInput =
    InvalidPlanInput("Aggregate needs a plan input")

  def aggregateWithPivotRequiresPivot(): InvalidPlanInput =
    InvalidPlanInput("Aggregate with GROUP_TYPE_PIVOT requires a Pivot")

  def invalidWithRelationReference(): InvalidPlanInput =
    InvalidPlanInput("Invalid WithRelation reference")

  def assertionFailure(message: String): InvalidPlanInput =
    InvalidPlanInput(message)

  def unresolvedNamedLambdaVariableRequiresNamePart(): InvalidPlanInput =
    InvalidPlanInput("UnresolvedNamedLambdaVariable requires at least one name part!")

  def usingColumnsOrJoinConditionSetInJoin(): InvalidPlanInput =
    InvalidPlanInput("Using columns or join conditions cannot be set at the same time in Join")

  def sqlCommandExpectsSqlOrWithRelations(other: proto.Relation.RelTypeCase): InvalidPlanInput =
    InvalidPlanInput(s"SQL command expects either a SQL or a WithRelations, but got $other")

  def reduceShouldCarryScalarScalaUdf(got: mutable.Buffer[proto.Expression]): InvalidPlanInput =
    InvalidPlanInput(s"reduce should carry a scalar scala udf, but got $got")

  def unionByNameAllowMissingColRequiresByName(): InvalidPlanInput =
    InvalidPlanInput("UnionByName `allowMissingCol` can be true only if `byName` is true.")

  def unsupportedUserDefinedFunctionImplementation(clazz: Class[_]): InvalidPlanInput =
    InvalidPlanInput(s"Unsupported UserDefinedFunction implementation: ${clazz}")

  def streamingQueryRunIdMismatch(
      id: String,
      runId: String,
      serverRunId: String): InvalidPlanInput =
    InvalidPlanInput(
      s"Run id mismatch for query id $id. Run id in the request $runId " +
        s"does not match one on the server $serverRunId. The query might have restarted.")

  def streamingQueryNotFound(id: String): InvalidPlanInput =
    InvalidPlanInput(s"Streaming query $id is not found")
}
