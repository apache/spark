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
import org.apache.spark.sql.catalyst.expressions.{Expression, GroupingID}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.toPrettySQL
import org.apache.spark.sql.connector.catalog.TableChange
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{AbstractDataType, DataType, StructType}

/**
 * Object for grouping all error messages of the query compilation.
 * Currently it includes all AnalysisExcpetions created and thrown directly in
 * org.apache.spark.sql.catalyst.analysis.Analyzer.
 */
private[spark] object QueryCompilationErrors {

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

}


