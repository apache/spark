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

  def noHandlerFoundForExtension(extensionTypeUrl: String): InvalidPlanInput =
    InvalidPlanInput(
      "CONNECT_INVALID_PLAN.NO_HANDLER_FOR_EXTENSION",
      Map("extensionTypeUrl" -> extensionTypeUrl))

  def invalidSQLWithReferences(query: proto.WithRelations): InvalidPlanInput =
    InvalidPlanInput(
      "CONNECT_INVALID_PLAN.INVALID_SQL_WITH_REFERENCES",
      Map("query" -> query.toString))

  def naFillValuesEmpty(): InvalidPlanInput =
    InvalidPlanInput("CONNECT_INVALID_PLAN.NA_FILL_VALUES_EMPTY", Map.empty)

  def naFillValuesLengthMismatch(): InvalidPlanInput =
    InvalidPlanInput("CONNECT_INVALID_PLAN.NA_FILL_VALUES_LENGTH_MISMATCH", Map.empty)

  def deduplicateNeedsInput(): InvalidPlanInput =
    InvalidPlanInput("CONNECT_INVALID_PLAN.DEDUPLICATE_NEEDS_INPUT", Map.empty)

  def deduplicateAllColumnsAndSubset(): InvalidPlanInput =
    InvalidPlanInput("CONNECT_INVALID_PLAN.DEDUPLICATE_ALL_COLUMNS_AND_SUBSET", Map.empty)

  def deduplicateRequiresColumnsOrAll(): InvalidPlanInput =
    InvalidPlanInput("CONNECT_INVALID_PLAN.DEDUPLICATE_REQUIRES_COLUMNS_OR_ALL", Map.empty)

  def invalidDeduplicateColumn(colName: String, fieldNames: String): InvalidPlanInput =
    InvalidPlanInput(
      "CONNECT_INVALID_PLAN.UNRESOLVED_COLUMN_AMONG_FIELD_NAMES",
      Map("colName" -> colName, "fieldNames" -> fieldNames))

  def functionEvalTypeNotSupported(evalType: Int): InvalidPlanInput =
    InvalidPlanInput(
      "CONNECT_INVALID_PLAN.FUNCTION_EVAL_TYPE_NOT_SUPPORTED",
      Map("evalType" -> evalType.toString))

  def groupingExpressionAbsentForKeyValueGroupedDataset(): InvalidPlanInput =
    InvalidPlanInput("CONNECT_INVALID_PLAN.GROUPING_EXPRESSION_ABSENT", Map.empty)

  def expectingScalaUdfButGot(exprType: proto.Expression.ExprTypeCase): InvalidPlanInput =
    InvalidPlanInput(
      "CONNECT_INVALID_PLAN.EXPECTING_SCALA_UDF",
      Map("exprType" -> exprType.toString))

  def rowNotSupportedForUdf(errorType: String): InvalidPlanInput =
    InvalidPlanInput(
      "CONNECT_INVALID_PLAN.ROW_NOT_SUPPORTED_FOR_UDF",
      Map("errorType" -> errorType))

  def notFoundCachedLocalRelation(hash: String, sessionUUID: String): InvalidPlanInput =
    InvalidPlanInput(
      "CONNECT_INVALID_PLAN.NOT_FOUND_CACHED_LOCAL_RELATION",
      Map("hash" -> hash, "sessionUUID" -> sessionUUID))

  def notFoundChunkedCachedLocalRelationBlock(
      hash: String,
      sessionUUID: String): InvalidPlanInput =
    InvalidPlanInput(
      "CONNECT_INVALID_PLAN.NOT_FOUND_CHUNKED_CACHED_LOCAL_RELATION",
      Map("hash" -> hash, "sessionUUID" -> sessionUUID))

  def localRelationSizeLimitExceeded(actualSize: Long, limit: Long): InvalidPlanInput =
    InvalidPlanInput(
      "CONNECT_INVALID_PLAN.LOCAL_RELATION_SIZE_LIMIT_EXCEEDED",
      Map("actualSize" -> actualSize.toString, "limit" -> limit.toString))

  def localRelationChunkSizeLimitExceeded(limit: Long): InvalidPlanInput =
    InvalidPlanInput(
      "CONNECT_INVALID_PLAN.LOCAL_RELATION_CHUNK_SIZE_LIMIT_EXCEEDED",
      Map("limit" -> limit.toString))

  def withColumnsRequireSingleNamePart(got: String): InvalidPlanInput =
    InvalidPlanInput(
      "CONNECT_INVALID_PLAN.WITH_COLUMNS_REQUIRE_SINGLE_NAME_PART",
      Map("got" -> got))

  def inputDataForLocalRelationNoSchema(): InvalidPlanInput =
    InvalidPlanInput("CONNECT_INVALID_PLAN.INPUT_DATA_NO_SCHEMA", Map.empty)

  def chunkedCachedLocalRelationWithoutData(): InvalidPlanInput =
    InvalidPlanInput("CONNECT_INVALID_PLAN.CHUNKED_CACHED_LOCAL_RELATION_WITHOUT_DATA", Map.empty)

  def schemaRequiredForLocalRelation(): InvalidPlanInput =
    InvalidPlanInput("CONNECT_INVALID_PLAN.SCHEMA_REQUIRED_FOR_LOCAL_RELATION", Map.empty)

  def invalidSchemaStringNonStructType(schema: String, dataType: DataType): InvalidPlanInput =
    InvalidPlanInput(
      "CONNECT_INVALID_PLAN.INVALID_SCHEMA_NON_STRUCT_TYPE",
      Map("inputSchema" -> quoteByDefault(schema), "dataType" -> toSQLType(dataType)))

  def invalidJdbcParams(): InvalidPlanInput =
    InvalidPlanInput("CONNECT_INVALID_PLAN.INVALID_JDBC_PARAMS", Map.empty)

  def predicatesNotSupportedForDataSource(format: String): InvalidPlanInput =
    InvalidPlanInput(
      "CONNECT_INVALID_PLAN.PREDICATES_NOT_SUPPORTED_FOR_DATA_SOURCE",
      Map("format" -> format))

  def multiplePathsNotSupportedForStreamingSource(): InvalidPlanInput =
    InvalidPlanInput(
      "CONNECT_INVALID_PLAN.MULTIPLE_PATHS_NOT_SUPPORTED_FOR_STREAMING_SOURCE",
      Map.empty)

  def invalidEnum(protoEnum: Enum[_] with ProtocolMessageEnum): InvalidPlanInput =
    InvalidPlanInput(
      "CONNECT_INVALID_PLAN.INVALID_ENUM",
      Map(
        "fullName" -> protoEnum.getDescriptorForType.getFullName,
        "name" -> protoEnum.name(),
        "number" -> protoEnum.getNumber.toString))

  def invalidOneOfField(
      enumCase: Enum[_] with EnumLite,
      descriptor: Descriptor): InvalidPlanInput = {
    // If the oneOf field is not set, the enum number will be 0.
    if (enumCase.getNumber == 0) {
      InvalidPlanInput(
        "CONNECT_INVALID_PLAN.INVALID_ONE_OF_FIELD_NOT_SET",
        Map("fullName" -> descriptor.getFullName, "name" -> enumCase.name()))
    } else {
      InvalidPlanInput(
        "CONNECT_INVALID_PLAN.INVALID_ONE_OF_FIELD_NOT_SUPPORTED",
        Map(
          "fullName" -> descriptor.getFullName,
          "name" -> enumCase.name(),
          "number" -> enumCase.getNumber.toString))
    }
  }

  def cannotBeEmpty(fieldName: String, descriptor: Descriptor): InvalidPlanInput =
    InvalidPlanInput(
      "CONNECT_INVALID_PLAN.FIELD_CANNOT_BE_EMPTY",
      Map("fieldName" -> fieldName, "fullName" -> descriptor.getFullName))

  def invalidSchemaTypeNonStruct(dataType: DataType): InvalidPlanInput =
    InvalidPlanInput("INVALID_SCHEMA_TYPE_NON_STRUCT", Map("dataType" -> toSQLType(dataType)))

  def lambdaFunctionArgumentCountInvalid(got: Int): InvalidPlanInput =
    InvalidPlanInput(
      "CONNECT_INVALID_PLAN.LAMBDA_FUNCTION_ARGUMENT_COUNT_INVALID",
      Map("got" -> got.toString))

  def aliasWithMultipleIdentifiersAndMetadata(): InvalidPlanInput =
    InvalidPlanInput(
      "CONNECT_INVALID_PLAN.ALIAS_WITH_MULTIPLE_IDENTIFIERS_AND_METADATA",
      Map.empty)

  def unresolvedStarTargetInvalid(target: String): InvalidPlanInput =
    InvalidPlanInput(
      "CONNECT_INVALID_PLAN.UNRESOLVED_STAR_TARGET_INVALID",
      Map("target" -> target))

  def unresolvedStarWithBothTargetAndPlanId(): InvalidPlanInput =
    InvalidPlanInput(
      "CONNECT_INVALID_PLAN.UNRESOLVED_STAR_WITH_BOTH_TARGET_AND_PLAN_ID",
      Map.empty)

  def windowFunctionRequired(): InvalidPlanInput =
    InvalidPlanInput("CONNECT_INVALID_PLAN.WINDOW_FUNCTION_REQUIRED", Map.empty)

  def lowerBoundRequiredInWindowFrame(): InvalidPlanInput =
    InvalidPlanInput("CONNECT_INVALID_PLAN.LOWER_BOUND_REQUIRED_IN_WINDOW_FRAME", Map.empty)

  def upperBoundRequiredInWindowFrame(): InvalidPlanInput =
    InvalidPlanInput("CONNECT_INVALID_PLAN.UPPER_BOUND_REQUIRED_IN_WINDOW_FRAME", Map.empty)

  def setOperationMustHaveTwoInputs(): InvalidPlanInput =
    InvalidPlanInput("CONNECT_INVALID_PLAN.SET_OPERATION_MUST_HAVE_TWO_INPUTS", Map.empty)

  def exceptDoesNotSupportUnionByName(): InvalidPlanInput =
    InvalidPlanInput("CONNECT_INVALID_PLAN.EXCEPT_DOES_NOT_SUPPORT_UNION_BY_NAME", Map.empty)

  def intersectDoesNotSupportUnionByName(): InvalidPlanInput =
    InvalidPlanInput("CONNECT_INVALID_PLAN.INTERSECT_DOES_NOT_SUPPORT_UNION_BY_NAME", Map.empty)

  def aggregateNeedsPlanInput(): InvalidPlanInput =
    InvalidPlanInput("CONNECT_INVALID_PLAN.AGGREGATE_NEEDS_PLAN_INPUT", Map.empty)

  def aggregateWithPivotRequiresPivot(): InvalidPlanInput =
    InvalidPlanInput("CONNECT_INVALID_PLAN.AGGREGATE_WITH_PIVOT_REQUIRES_PIVOT", Map.empty)

  def invalidWithRelationReference(): InvalidPlanInput =
    InvalidPlanInput("CONNECT_INVALID_PLAN.INVALID_WITH_RELATION_REFERENCE", Map.empty)

  def assertionFailure(message: String): InvalidPlanInput =
    InvalidPlanInput("CONNECT_INVALID_PLAN.ASSERTION_FAILURE", Map("message" -> message))

  def unresolvedNamedLambdaVariableRequiresNamePart(): InvalidPlanInput =
    InvalidPlanInput(
      "CONNECT_INVALID_PLAN.UNRESOLVED_NAMED_LAMBDA_VARIABLE_REQUIRES_NAME_PART",
      Map.empty)

  def usingColumnsOrJoinConditionSetInJoin(): InvalidPlanInput =
    InvalidPlanInput(
      "CONNECT_INVALID_PLAN.USING_COLUMNS_OR_JOIN_CONDITION_SET_IN_JOIN",
      Map.empty)

  def sqlCommandExpectsSqlOrWithRelations(other: proto.Relation.RelTypeCase): InvalidPlanInput =
    InvalidPlanInput(
      "CONNECT_INVALID_PLAN.SQL_COMMAND_EXPECTS_SQL_OR_WITH_RELATIONS",
      Map("other" -> other.toString))

  def reduceShouldCarryScalarScalaUdf(got: mutable.Buffer[proto.Expression]): InvalidPlanInput =
    InvalidPlanInput(
      "CONNECT_INVALID_PLAN.REDUCE_SHOULD_CARRY_SCALAR_SCALA_UDF",
      Map("got" -> got.toString))

  def unionByNameAllowMissingColRequiresByName(): InvalidPlanInput =
    InvalidPlanInput(
      "CONNECT_INVALID_PLAN.UNION_BY_NAME_ALLOW_MISSING_COL_REQUIRES_BY_NAME",
      Map.empty)

  def unsupportedUserDefinedFunctionImplementation(clazz: Class[_]): InvalidPlanInput =
    InvalidPlanInput(
      "CONNECT_INVALID_PLAN.UNSUPPORTED_USER_DEFINED_FUNCTION_IMPLEMENTATION",
      Map("clazz" -> clazz.toString))

  def streamingQueryRunIdMismatch(
      id: String,
      runId: String,
      serverRunId: String): InvalidPlanInput =
    InvalidPlanInput(
      "CONNECT_INVALID_PLAN.STREAMING_QUERY_RUN_ID_MISMATCH",
      Map("id" -> id, "runId" -> runId, "serverRunId" -> serverRunId))

  def streamingQueryNotFound(id: String): InvalidPlanInput =
    InvalidPlanInput("CONNECT_INVALID_PLAN.STREAMING_QUERY_NOT_FOUND", Map("id" -> id))

  def cannotFindCachedLocalRelation(hash: String): InvalidPlanInput =
    InvalidPlanInput(
      "CONNECT_INVALID_PLAN.CANNOT_FIND_CACHED_LOCAL_RELATION",
      Map("hash" -> hash))
}
