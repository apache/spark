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

import org.apache.spark.connect.proto
import org.apache.spark.sql.connect.common.{InvalidCommandInput, InvalidPlanInput}
import org.apache.spark.sql.types.DataType

object InvalidInputErrors {

  def unknownRelationNotSupported(rel: proto.Relation): InvalidPlanInput =
    InvalidPlanInput(s"${rel.getUnknown} not supported.")

  def noHandlerFoundForExtension(): InvalidPlanInput =
    InvalidPlanInput("No handler found for extension")

  def catalogTypeNotSupported(catType: proto.Catalog.CatTypeCase): InvalidPlanInput =
    InvalidPlanInput(s"$catType not supported.")

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

  def functionIdNotSupported(functionId: Int): InvalidPlanInput =
    InvalidPlanInput(s"Function with ID: $functionId is not supported")

  def groupingExpressionAbsentForKeyValueGroupedDataset(): InvalidPlanInput =
    InvalidPlanInput("The grouping expression cannot be absent for KeyValueGroupedDataset")

  def expectingScalaUdfButGot(exprType: proto.Expression.ExprTypeCase): InvalidPlanInput =
    InvalidPlanInput(s"Expecting a Scala UDF, but get $exprType")

  def rowNotSupportedForUdf(errorType: String): InvalidPlanInput =
    InvalidPlanInput(s"Row is not a supported $errorType type for this UDF.")

  def invalidUserDefinedOutputSchemaType(actualType: String): InvalidPlanInput =
    InvalidPlanInput(
      s"Invalid user-defined output schema type for TransformWithStateInPandas. " +
        s"Expect a struct type, but got $actualType.")

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

  def invalidSchema(schema: DataType): InvalidPlanInput =
    InvalidPlanInput(s"Invalid schema $schema")

  def invalidJdbcParams(): InvalidPlanInput =
    InvalidPlanInput("Invalid jdbc params, please specify jdbc url and table.")

  def predicatesNotSupportedForDataSource(format: String): InvalidPlanInput =
    InvalidPlanInput(s"Predicates are not supported for $format data sources.")

  def multiplePathsNotSupportedForStreamingSource(): InvalidPlanInput =
    InvalidPlanInput("Multiple paths are not supported for streaming source")

  def doesNotSupport(what: String): InvalidPlanInput =
    InvalidPlanInput(s"Does not support $what")

  def invalidSchemaDataType(dataType: DataType): InvalidPlanInput =
    InvalidPlanInput(s"Invalid schema dataType $dataType")

  def expressionIdNotSupported(exprId: Int): InvalidPlanInput =
    InvalidPlanInput(s"Expression with ID: $exprId is not supported")

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

  def unknownFrameType(
      frameType: proto.Expression.Window.WindowFrame.FrameType): InvalidPlanInput =
    InvalidPlanInput(s"Unknown FrameType $frameType")

  def lowerBoundRequiredInWindowFrame(): InvalidPlanInput =
    InvalidPlanInput("LowerBound is required in WindowFrame")

  def unknownFrameBoundary(
      boundary: proto.Expression.Window.WindowFrame.FrameBoundary.BoundaryCase)
      : InvalidPlanInput =
    InvalidPlanInput(s"Unknown FrameBoundary $boundary")

  def upperBoundRequiredInWindowFrame(): InvalidPlanInput =
    InvalidPlanInput("UpperBound is required in WindowFrame")

  def setOperationMustHaveTwoInputs(): InvalidPlanInput =
    InvalidPlanInput("Set operation must have 2 inputs")

  def exceptDoesNotSupportUnionByName(): InvalidPlanInput =
    InvalidPlanInput("Except does not support union_by_name")

  def intersectDoesNotSupportUnionByName(): InvalidPlanInput =
    InvalidPlanInput("Intersect does not support union_by_name")

  def unsupportedSetOperation(op: Int): InvalidPlanInput =
    InvalidPlanInput(s"Unsupported set operation $op")

  def joinTypeNotSupported(t: proto.Join.JoinType): InvalidPlanInput =
    InvalidPlanInput(s"Join type $t is not supported")

  def aggregateNeedsPlanInput(): InvalidPlanInput =
    InvalidPlanInput("Aggregate needs a plan input")

  def aggregateWithPivotRequiresPivot(): InvalidPlanInput =
    InvalidPlanInput("Aggregate with GROUP_TYPE_PIVOT requires a Pivot")

  def runnerCannotBeEmptyInExecuteExternalCommand(): InvalidPlanInput =
    InvalidPlanInput("runner cannot be empty in executeExternalCommand")

  def commandCannotBeEmptyInExecuteExternalCommand(): InvalidPlanInput =
    InvalidPlanInput("command cannot be empty in executeExternalCommand")

  def unexpectedForeachBatchFunction(): InvalidPlanInput =
    InvalidPlanInput("Unexpected foreachBatch function")

  def invalidWithRelationReference(): InvalidPlanInput =
    InvalidPlanInput("Invalid WithRelation reference")

  def assertionFailure(message: String): InvalidPlanInput =
    InvalidPlanInput(message)

  def unsupportedMergeActionType(actionType: proto.MergeAction.ActionType): InvalidPlanInput =
    InvalidPlanInput(s"Unsupported merge action type $actionType")

  def unresolvedNamedLambdaVariableRequiresNamePart(): InvalidPlanInput =
    InvalidPlanInput("UnresolvedNamedLambdaVariable requires at least one name part!")

  def usingColumnsOrJoinConditionSetInJoin(): InvalidPlanInput =
    InvalidPlanInput("Using columns or join conditions cannot be set at the same time in Join")

  def invalidStateSchemaDataType(dataType: DataType): InvalidPlanInput =
    InvalidPlanInput(s"Invalid state schema dataType $dataType for flatMapGroupsWithState")

  def sqlCommandExpectsSqlOrWithRelations(other: proto.Relation.RelTypeCase): InvalidPlanInput =
    InvalidPlanInput(s"SQL command expects either a SQL or a WithRelations, but got $other")

  def unknownGroupType(groupType: proto.Aggregate.GroupType): InvalidPlanInput =
    InvalidPlanInput(s"Unknown group type $groupType")

  def dataSourceIdNotSupported(dataSourceId: Int): InvalidPlanInput =
    InvalidPlanInput(s"Data source id $dataSourceId is not supported")

  def unknownSubqueryType(subqueryType: proto.SubqueryExpression.SubqueryType): InvalidPlanInput =
    InvalidPlanInput(s"Unknown subquery type $subqueryType")

  def reduceShouldCarryScalarScalaUdf(got: mutable.Buffer[proto.Expression]): InvalidPlanInput =
    InvalidPlanInput(s"reduce should carry a scalar scala udf, but got $got")

  def unionByNameAllowMissingColRequiresByName(): InvalidPlanInput =
    InvalidPlanInput("UnionByName `allowMissingCol` can be true only if `byName` is true.")

  def invalidBucketCount(numBuckets: Int): InvalidCommandInput =
    InvalidCommandInput("BucketBy must specify a bucket count > 0, received $numBuckets instead.")

  def invalidPythonUdtfReturnType(actualType: String): InvalidPlanInput =
    InvalidPlanInput(
      s"Invalid Python user-defined table function return type. " +
        s"Expect a struct type, but got $actualType.")

  def invalidUserDefinedOutputSchemaTypeForTransformWithState(
      actualType: String): InvalidPlanInput =
    InvalidPlanInput(
      s"Invalid user-defined output schema type for TransformWithStateInPandas. " +
        s"Expect a struct type, but got $actualType.")

  def unsupportedUserDefinedFunctionImplementation(clazz: Class[_]): InvalidPlanInput =
    InvalidPlanInput(s"Unsupported UserDefinedFunction implementation: ${clazz}")
}
