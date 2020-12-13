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

package org.apache.spark.sql.errors

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.ResolvedView
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, GroupingID, NamedExpression, SpecifiedWindowFrame, WindowFrame, WindowFunction, WindowSpecDefinition}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.catalyst.util.toPrettySQL
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.connector.catalog.TableChange
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{AbstractDataType, DataType, StructType}

/**
 * Object for grouping all error messages of the query compilation.
 * Currently it includes all AnalysisExcpetions created and thrown directly in
 * org.apache.spark.sql.catalyst.analysis.Analyzer.
 */
object QueryCompilationErrors {

  def groupingIDMismatchError(groupingID: GroupingID, groupByExprs: Seq[Expression]): Throwable = {
    new AnalysisException(
      s"Columns of grouping_id (${groupingID.groupByExprs.mkString(",")}) " +
        s"does not match grouping columns (${groupByExprs.mkString(",")})")
  }

  def groupingColInvalidError(groupingCol: Expression, groupByExprs: Seq[Expression]): Throwable = {
    new AnalysisException(
      s"Column of grouping ($groupingCol) can't be found " +
        s"in grouping columns ${groupByExprs.mkString(",")}")
  }

  def groupingSizeTooLargeError(sizeLimit: Int): Throwable = {
    new AnalysisException(
      s"Grouping sets size cannot be greater than $sizeLimit")
  }

  def unorderablePivotColError(pivotCol: Expression): Throwable = {
    new AnalysisException(
      s"Invalid pivot column '$pivotCol'. Pivot columns must be comparable."
    )
  }

  def nonLiteralPivotValError(pivotVal: Expression): Throwable = {
    new AnalysisException(
      s"Literal expressions required for pivot values, found '$pivotVal'")
  }

  def pivotValDataTypeMismatchError(pivotVal: Expression, pivotCol: Expression): Throwable = {
    new AnalysisException(
      s"Invalid pivot value '$pivotVal': " +
        s"value data type ${pivotVal.dataType.simpleString} does not match " +
        s"pivot column data type ${pivotCol.dataType.catalogString}")
  }

  def unsupportedIfNotExistsError(tableName: String): Throwable = {
    new AnalysisException(
      s"Cannot write, IF NOT EXISTS is not supported for table: $tableName")
  }

  def nonPartitionColError(partitionName: String): Throwable = {
    new AnalysisException(
      s"PARTITION clause cannot contain a non-partition column name: $partitionName")
  }

  def addStaticValToUnknownColError(staticName: String): Throwable = {
    new AnalysisException(
      s"Cannot add static value for unknown column: $staticName")
  }

  def unknownStaticPartitionColError(name: String): Throwable = {
    new AnalysisException(s"Unknown static partition column: $name")
  }

  def nestedGeneratorError(trimmedNestedGenerator: Expression): Throwable = {
    new AnalysisException(
      "Generators are not supported when it's nested in " +
        "expressions, but got: " + toPrettySQL(trimmedNestedGenerator))
  }

  def moreThanOneGeneratorError(generators: Seq[Expression], clause: String): Throwable = {
    new AnalysisException(
      s"Only one generator allowed per $clause clause but found " +
        generators.size + ": " + generators.map(toPrettySQL).mkString(", "))
  }

  def generatorOutsideSelectError(plan: LogicalPlan): Throwable = {
    new AnalysisException(
      "Generators are not supported outside the SELECT clause, but " +
        "got: " + plan.simpleString(SQLConf.get.maxToStringFields))
  }

  def legacyStoreAssignmentPolicyError(): Throwable = {
    val configKey = SQLConf.STORE_ASSIGNMENT_POLICY.key
    new AnalysisException(
      "LEGACY store assignment policy is disallowed in Spark data source V2. " +
        s"Please set the configuration $configKey to other values.")
  }

  def unresolvedUsingColForJoinError(
      colName: String, plan: LogicalPlan, side: String): Throwable = {
    new AnalysisException(
      s"USING column `$colName` cannot be resolved on the $side " +
        s"side of the join. The $side-side columns: [${plan.output.map(_.name).mkString(", ")}]")
  }

  def dataTypeMismatchForDeserializerError(
      dataType: DataType, desiredType: String): Throwable = {
    val quantifier = if (desiredType.equals("array")) "an" else "a"
    new AnalysisException(
      s"need $quantifier $desiredType field but got " + dataType.catalogString)
  }

  def fieldNumberMismatchForDeserializerError(
      schema: StructType, maxOrdinal: Int): Throwable = {
    new AnalysisException(
      s"Try to map ${schema.catalogString} to Tuple${maxOrdinal + 1}, " +
        "but failed as the number of fields does not line up.")
  }

  def upCastFailureError(
      fromStr: String, from: Expression, to: DataType, walkedTypePath: Seq[String]): Throwable = {
    new AnalysisException(
      s"Cannot up cast $fromStr from " +
        s"${from.dataType.catalogString} to ${to.catalogString}.\n" +
        s"The type path of the target object is:\n" + walkedTypePath.mkString("", "\n", "\n") +
        "You can either add an explicit cast to the input data or choose a higher precision " +
        "type of the field in the target object")
  }

  def unsupportedAbstractDataTypeForUpCastError(gotType: AbstractDataType): Throwable = {
    new AnalysisException(
      s"UpCast only support DecimalType as AbstractDataType yet, but got: $gotType")
  }

  def outerScopeFailureForNewInstanceError(className: String): Throwable = {
    new AnalysisException(
      s"Unable to generate an encoder for inner class `$className` without " +
        "access to the scope that this class was defined in.\n" +
        "Try moving this class out of its parent class.")
  }

  def referenceColNotFoundForAlterTableChangesError(
      after: TableChange.After, parentName: String): Throwable = {
    new AnalysisException(
      s"Couldn't find the reference column for $after at $parentName")
  }

  def windowSpecificationNotDefinedError(windowName: String): Throwable = {
    new AnalysisException(s"Window specification $windowName is not defined in the WINDOW clause.")
  }

  def selectExprNotInGroupByError(expr: Expression, groupByAliases: Seq[Alias]): Throwable = {
    new AnalysisException(s"$expr doesn't show up in the GROUP BY list $groupByAliases")
  }

  def groupingMustWithGroupingSetsOrCubeOrRollupError(): Throwable = {
    new AnalysisException("grouping()/grouping_id() can only be used with GroupingSets/Cube/Rollup")
  }

  def pandasUDFAggregateNotSupportedInPivotError(): Throwable = {
    new AnalysisException("Pandas UDF aggregate expressions are currently not supported in pivot.")
  }

  def aggregateExpressionRequiredForPivotError(sql: String): Throwable = {
    new AnalysisException(s"Aggregate expression required for pivot, but '$sql' " +
      "did not appear in any aggregate function.")
  }

  def expectTableNotTempViewError(quoted: String, cmd: String, t: TreeNode[_]): Throwable = {
    new AnalysisException(s"$quoted is a temp view. '$cmd' expects a table",
      t.origin.line, t.origin.startPosition)
  }

  def expectTableOrPermanentViewNotTempViewError(
      quoted: String, cmd: String, t: TreeNode[_]): Throwable = {
    new AnalysisException(s"$quoted is a temp view. '$cmd' expects a table or permanent view.",
      t.origin.line, t.origin.startPosition)
  }

  def viewDepthExceedsMaxResolutionDepthError(
      identifier: TableIdentifier, maxNestedViewDepth: Int, t: TreeNode[_]): Throwable = {
    new AnalysisException(s"The depth of view $identifier exceeds the maximum " +
      s"view resolution depth ($maxNestedViewDepth). Analysis is aborted to " +
      s"avoid errors. Increase the value of ${SQLConf.MAX_NESTED_VIEW_DEPTH.key} to work " +
      "around this.", t.origin.line, t.origin.startPosition)
  }

  def insertIntoViewNotAllowedError(identifier: TableIdentifier, t: TreeNode[_]): Throwable = {
    new AnalysisException(s"Inserting into a view is not allowed. View: $identifier.",
      t.origin.line, t.origin.startPosition)
  }

  def writeIntoViewNotAllowedError(identifier: TableIdentifier, t: TreeNode[_]): Throwable = {
    new AnalysisException(s"Writing into a view is not allowed. View: $identifier.",
      t.origin.line, t.origin.startPosition)
  }

  def writeIntoV1TableNotAllowedError(identifier: TableIdentifier, t: TreeNode[_]): Throwable = {
    new AnalysisException(s"Cannot write into v1 table: $identifier.",
      t.origin.line, t.origin.startPosition)
  }

  def expectTableNotViewError(v: ResolvedView, cmd: String, t: TreeNode[_]): Throwable = {
    val viewStr = if (v.isTemp) "temp view" else "view"
    new AnalysisException(s"${v.identifier.quoted} is a $viewStr. '$cmd' expects a table.",
      t.origin.line, t.origin.startPosition)
  }

  def starNotAllowedWhenGroupByOrdinalPositionUsedError(): Throwable = {
    new AnalysisException(
      "Star (*) is not allowed in select list when GROUP BY ordinal position is used")
  }

  def invalidStarUsageError(prettyName: String): Throwable = {
    new AnalysisException(s"Invalid usage of '*' in $prettyName")
  }

  def orderByPositionRangeError(index: Int, size: Int, t: TreeNode[_]): Throwable = {
    new AnalysisException(s"ORDER BY position $index is not in select list " +
      s"(valid range is [1, $size])", t.origin.line, t.origin.startPosition)
  }

  def groupByPositionRangeError(index: Int, size: Int, t: TreeNode[_]): Throwable = {
    new AnalysisException(s"GROUP BY position $index is not in select list " +
      s"(valid range is [1, $size])", t.origin.line, t.origin.startPosition)
  }

  def generatorNotExpectedError(name: FunctionIdentifier, classCanonicalName: String): Throwable = {
    new AnalysisException(s"$name is expected to be a generator. However, " +
      s"its class is $classCanonicalName, which is not a generator.")
  }

  def distinctOrFilterOnlyWithAggregateFunctionError(prettyName: String): Throwable = {
    new AnalysisException("DISTINCT or FILTER specified, " +
      s"but $prettyName is not an aggregate function")
  }

  def nonDeterministicFilterInAggregateError(): Throwable = {
    new AnalysisException("FILTER expression is non-deterministic, " +
      "it cannot be used in aggregate functions")
  }

  def aliasNumberNotMatchColumnNumberError(
      columnSize: Int, outputSize: Int, t: TreeNode[_]): Throwable = {
    new AnalysisException("Number of column aliases does not match number of columns. " +
      s"Number of column aliases: $columnSize; " +
      s"number of columns: $outputSize.", t.origin.line, t.origin.startPosition)
  }

  def aliasesNumberNotMatchUDTFOutputError(
      aliasesSize: Int, aliasesNames: String): Throwable = {
    new AnalysisException("The number of aliases supplied in the AS clause does not " +
      s"match the number of columns output by the UDTF expected $aliasesSize " +
      s"aliases but got $aliasesNames ")
  }

  def windowAggregateFunctionWithFilterNotSupportedError(): Throwable = {
    new AnalysisException("window aggregate function with filter predicate is not supported yet.")
  }

  def windowFunctionInsideAggregateFunctionNotAllowedError(): Throwable = {
    new AnalysisException("It is not allowed to use a window function inside an aggregate " +
      "function. Please use the inner window function in a sub-query.")
  }

  def expressionWithoutWindowExpressionError(expr: NamedExpression): Throwable = {
    new AnalysisException(s"$expr does not have any WindowExpression.")
  }

  def expressionWithMultiWindowExpressionsError(
      expr: NamedExpression, distinctWindowSpec: Seq[WindowSpecDefinition]): Throwable = {
    new AnalysisException(s"$expr has multiple Window Specifications ($distinctWindowSpec)." +
      "Please file a bug report with this error message, stack trace, and the query.")
  }

  def windowFunctionNotAllowedError(clauseName: String): Throwable = {
    new AnalysisException(s"It is not allowed to use window functions inside $clauseName clause")
  }

  def cannotSpecifyWindowFrameError(prettyName: String): Throwable = {
    new AnalysisException(s"Cannot specify window frame for $prettyName function")
  }

  def windowFrameNotMatchRequiredFrameError(
      f: SpecifiedWindowFrame, required: WindowFrame): Throwable = {
    new AnalysisException(s"Window Frame $f must match the required frame $required")
  }

  def windowFunctionWithWindowFrameNotOrderedError(wf: WindowFunction): Throwable = {
    new AnalysisException(s"Window function $wf requires window to be ordered, please add " +
      s"ORDER BY clause. For example SELECT $wf(value_expr) OVER (PARTITION BY window_partition " +
      "ORDER BY window_ordering) from table")
  }

  def cannotResolveUserSpecifiedColumnsError(col: String, t: TreeNode[_]): Throwable = {
    new AnalysisException(s"Cannot resolve column name $col", t.origin.line, t.origin.startPosition)
  }

  def writeTableWithMismatchedColumnsError(
      columnSize: Int, outputSize: Int, t: TreeNode[_]): Throwable = {
    new AnalysisException("Cannot write to table due to mismatched user specified column " +
      s"size($columnSize) and data column size($outputSize)", t.origin.line, t.origin.startPosition)
  }

  def multiTimeWindowExpressionsNotSupportedError(t: TreeNode[_]): Throwable = {
    new AnalysisException("Multiple time window expressions would result in a cartesian product " +
      "of rows, therefore they are currently not supported.", t.origin.line, t.origin.startPosition)
  }

}
