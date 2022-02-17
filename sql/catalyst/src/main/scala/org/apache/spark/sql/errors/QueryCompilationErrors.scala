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

import scala.collection.mutable

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{FunctionIdentifier, QualifiedTableName, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{CannotReplaceMissingTableException, NamespaceAlreadyExistsException, NoSuchFunctionException, NoSuchNamespaceException, NoSuchPartitionException, NoSuchTableException, ResolvedTable, ResolvedView, Star, TableAlreadyExistsException, UnresolvedRegex}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, InvalidUDFClassException}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, AttributeSet, CreateMap, Expression, GroupingID, NamedExpression, SpecifiedWindowFrame, WindowFrame, WindowFunction, WindowSpecDefinition}
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoStatement, Join, LogicalPlan, SerdeInfo, Window}
import org.apache.spark.sql.catalyst.trees.{Origin, TreeNode}
import org.apache.spark.sql.catalyst.util.{toPrettySQL, FailFastMode, ParseMode, PermissiveMode}
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.connector.catalog.functions.{BoundFunction, UnboundFunction}
import org.apache.spark.sql.connector.expressions.NamedReference
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.{LEGACY_ALLOW_NEGATIVE_SCALE_OF_DECIMAL_ENABLED, LEGACY_CTE_PRECEDENCE_POLICY}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types._

/**
 * Object for grouping error messages from exceptions thrown during query compilation.
 * As commands are executed eagerly, this also includes errors thrown during the execution of
 * commands, which users can see immediately.
 */
object QueryCompilationErrors {

  def groupingIDMismatchError(groupingID: GroupingID, groupByExprs: Seq[Expression]): Throwable = {
    new AnalysisException(
      errorClass = "GROUPING_ID_COLUMN_MISMATCH",
      messageParameters = Array(groupingID.groupByExprs.mkString(","), groupByExprs.mkString(",")))
  }

  def groupingColInvalidError(groupingCol: Expression, groupByExprs: Seq[Expression]): Throwable = {
    new AnalysisException(
      errorClass = "GROUPING_COLUMN_MISMATCH",
      messageParameters = Array(groupingCol.toString, groupByExprs.mkString(",")))
  }

  def groupingSizeTooLargeError(sizeLimit: Int): Throwable = {
    new AnalysisException(
      errorClass = "GROUPING_SIZE_LIMIT_EXCEEDED",
      messageParameters = Array(sizeLimit.toString))
  }

  def illegalSubstringError(subject: String, illegalContent: String): Throwable = {
    new AnalysisException(
      errorClass = "ILLEGAL_SUBSTRING",
      messageParameters = Array(subject, illegalContent))
  }

  def unorderablePivotColError(pivotCol: Expression): Throwable = {
    new AnalysisException(
      errorClass = "INCOMPARABLE_PIVOT_COLUMN",
      messageParameters = Array(pivotCol.toString))
  }

  def nonLiteralPivotValError(pivotVal: Expression): Throwable = {
    new AnalysisException(
      errorClass = "NON_LITERAL_PIVOT_VALUES",
      messageParameters = Array(pivotVal.toString))
  }

  def pivotValDataTypeMismatchError(pivotVal: Expression, pivotCol: Expression): Throwable = {
    new AnalysisException(
      errorClass = "PIVOT_VALUE_DATA_TYPE_MISMATCH",
      messageParameters = Array(
        pivotVal.toString, pivotVal.dataType.simpleString, pivotCol.dataType.catalogString))
  }

  def unsupportedIfNotExistsError(tableName: String): Throwable = {
    new AnalysisException(
      errorClass = "UNSUPPORTED_FEATURE",
      messageParameters = Array(s"IF NOT EXISTS for the table '$tableName' by INSERT INTO."))
  }

  def nonPartitionColError(partitionName: String): Throwable = {
    new AnalysisException(
      errorClass = "NON_PARTITION_COLUMN",
      messageParameters = Array(partitionName))
  }

  def missingStaticPartitionColumn(staticName: String): Throwable = {
    new AnalysisException(
      errorClass = "MISSING_STATIC_PARTITION_COLUMN",
      messageParameters = Array(staticName))
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
      errorClass = "CANNOT_UP_CAST_DATATYPE",
      messageParameters = Array(
        fromStr,
        from.dataType.catalogString,
        to.catalogString,
        s"The type path of the target object is:\n" + walkedTypePath.mkString("", "\n", "\n") +
          "You can either add an explicit cast to the input data or choose a higher precision " +
          "type of the field in the target object"
      )
    )
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
    new AnalysisException(
      errorClass = "UNSUPPORTED_GROUPING_EXPRESSION",
      messageParameters = Array.empty)
  }

  def pandasUDFAggregateNotSupportedInPivotError(): Throwable = {
    new AnalysisException("Pandas UDF aggregate expressions are currently not supported in pivot.")
  }

  def aggregateExpressionRequiredForPivotError(sql: String): Throwable = {
    new AnalysisException(s"Aggregate expression required for pivot, but '$sql' " +
      "did not appear in any aggregate function.")
  }

  def writeIntoTempViewNotAllowedError(quoted: String): Throwable = {
    new AnalysisException("Cannot write into temp view " +
      s"$quoted as it's not a data source v2 relation.")
  }

  def expectTableOrPermanentViewNotTempViewError(
      quoted: String, cmd: String, t: TreeNode[_]): Throwable = {
    new AnalysisException(s"$quoted is a temp view. '$cmd' expects a table or permanent view.",
      t.origin.line, t.origin.startPosition)
  }

  def readNonStreamingTempViewError(quoted: String): Throwable = {
    new AnalysisException(s"$quoted is not a temp view of streaming " +
      "logical plan, please use batch API such as `DataFrameReader.table` to read it.")
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

  def expectTableNotViewError(
      v: ResolvedView, cmd: String, mismatchHint: Option[String], t: TreeNode[_]): Throwable = {
    val viewStr = if (v.isTemp) "temp view" else "view"
    val hintStr = mismatchHint.map(" " + _).getOrElse("")
    new AnalysisException(s"${v.identifier.quoted} is a $viewStr. '$cmd' expects a table.$hintStr",
      t.origin.line, t.origin.startPosition)
  }

  def expectViewNotTableError(
      v: ResolvedTable, cmd: String, mismatchHint: Option[String], t: TreeNode[_]): Throwable = {
    val hintStr = mismatchHint.map(" " + _).getOrElse("")
    new AnalysisException(s"${v.identifier.quoted} is a table. '$cmd' expects a view.$hintStr",
      t.origin.line, t.origin.startPosition)
  }

  def expectPersistentFuncError(
      name: String, cmd: String, mismatchHint: Option[String], t: TreeNode[_]): Throwable = {
    val hintStr = mismatchHint.map(" " + _).getOrElse("")
    new AnalysisException(
      s"$name is a built-in/temporary function. '$cmd' expects a persistent function.$hintStr",
      t.origin.line, t.origin.startPosition)
  }

  def permanentViewNotSupportedByStreamingReadingAPIError(quoted: String): Throwable = {
    new AnalysisException(s"$quoted is a permanent view, which is not supported by " +
      "streaming reading API such as `DataStreamReader.table` yet.")
  }

  def starNotAllowedWhenGroupByOrdinalPositionUsedError(): Throwable = {
    new AnalysisException(
      "Star (*) is not allowed in select list when GROUP BY ordinal position is used")
  }

  def invalidStarUsageError(prettyName: String, stars: Seq[Star]): Throwable = {
    val regExpr = stars.collect{ case UnresolvedRegex(pattern, _, _) => s"'$pattern'" }
    val resExprMsg = Option(regExpr.distinct).filter(_.nonEmpty).map {
      case Seq(p) => s"regular expression $p"
      case patterns => s"regular expressions ${patterns.mkString(", ")}"
    }
    val starMsg = if (stars.length - regExpr.length > 0) {
      Some("'*'")
    } else {
      None
    }
    val elem = Seq(starMsg, resExprMsg).flatten.mkString(" and ")
    new AnalysisException(s"Invalid usage of $elem in $prettyName")
  }

  def singleTableStarInCountNotAllowedError(targetString: String): Throwable = {
    new AnalysisException(s"count($targetString.*) is not allowed. " +
      "Please use count(*) or expand the columns manually, e.g. count(col1, col2)")
  }

  def orderByPositionRangeError(index: Int, size: Int, t: TreeNode[_]): Throwable = {
    new AnalysisException(s"ORDER BY position $index is not in select list " +
      s"(valid range is [1, $size])", t.origin.line, t.origin.startPosition)
  }

  def groupByPositionRefersToAggregateFunctionError(
      index: Int,
      expr: Expression): Throwable = {
    new AnalysisException(s"GROUP BY $index refers to an expression that is or contains " +
      "an aggregate function. Aggregate functions are not allowed in GROUP BY, " +
      s"but got ${expr.sql}")
  }

  def groupByPositionRangeError(index: Int, size: Int): Throwable = {
    new AnalysisException(s"GROUP BY position $index is not in select list " +
      s"(valid range is [1, $size])")
  }

  def generatorNotExpectedError(name: FunctionIdentifier, classCanonicalName: String): Throwable = {
    new AnalysisException(s"$name is expected to be a generator. However, " +
      s"its class is $classCanonicalName, which is not a generator.")
  }

  def functionWithUnsupportedSyntaxError(prettyName: String, syntax: String): Throwable = {
    new AnalysisException(s"Function $prettyName does not support $syntax")
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
    new AnalysisException("Multiple time/session window expressions would result in a cartesian " +
      "product of rows, therefore they are currently not supported.", t.origin.line,
      t.origin.startPosition)
  }

  def sessionWindowGapDurationDataTypeError(dt: DataType): Throwable = {
    new AnalysisException("Gap duration expression used in session window must be " +
      s"CalendarIntervalType, but got ${dt}")
  }

  def viewOutputNumberMismatchQueryColumnNamesError(
      output: Seq[Attribute], queryColumnNames: Seq[String]): Throwable = {
    new AnalysisException(
      s"The view output ${output.mkString("[", ",", "]")} doesn't have the same" +
        "number of columns with the query column names " +
        s"${queryColumnNames.mkString("[", ",", "]")}")
  }

  def attributeNotFoundError(colName: String, child: LogicalPlan): Throwable = {
    new AnalysisException(
      s"Attribute with name '$colName' is not found in " +
        s"'${child.output.map(_.name).mkString("(", ",", ")")}'")
  }

  def functionUndefinedError(name: FunctionIdentifier): Throwable = {
    new AnalysisException(s"undefined function $name")
  }

  def invalidFunctionArgumentsError(
      name: String, expectedInfo: String, actualNumber: Int): Throwable = {
    new AnalysisException(s"Invalid number of arguments for function $name. " +
      s"Expected: $expectedInfo; Found: $actualNumber")
  }

  def invalidFunctionArgumentNumberError(
      validParametersCount: Seq[Int], name: String, actualNumber: Int): Throwable = {
    if (validParametersCount.length == 0) {
      new AnalysisException(s"Invalid arguments for function $name")
    } else {
      val expectedNumberOfParameters = if (validParametersCount.length == 1) {
        validParametersCount.head.toString
      } else {
        validParametersCount.init.mkString("one of ", ", ", " and ") +
          validParametersCount.last
      }
      invalidFunctionArgumentsError(name, expectedNumberOfParameters, actualNumber)
    }
  }

  def functionAcceptsOnlyOneArgumentError(name: String): Throwable = {
    new AnalysisException(s"Function $name accepts only one argument")
  }

  def alterV2TableSetLocationWithPartitionNotSupportedError(): Throwable = {
    new AnalysisException("ALTER TABLE SET LOCATION does not support partition for v2 tables.")
  }

  def joinStrategyHintParameterNotSupportedError(unsupported: Any): Throwable = {
    new AnalysisException("Join strategy hint parameter " +
      s"should be an identifier or string but was $unsupported (${unsupported.getClass}")
  }

  def invalidHintParameterError(
      hintName: String, invalidParams: Seq[Any]): Throwable = {
    new AnalysisException(s"$hintName Hint parameter should include columns, but " +
      s"${invalidParams.mkString(", ")} found")
  }

  def invalidCoalesceHintParameterError(hintName: String): Throwable = {
    new AnalysisException(s"$hintName Hint expects a partition number as a parameter")
  }

  def attributeNameSyntaxError(name: String): Throwable = {
    new AnalysisException(s"syntax error in attribute name: $name")
  }

  def starExpandDataTypeNotSupportedError(attributes: Seq[String]): Throwable = {
    new AnalysisException(s"Can only star expand struct data types. Attribute: `$attributes`")
  }

  def cannotResolveStarExpandGivenInputColumnsError(
      targetString: String, columns: String): Throwable = {
    new AnalysisException(s"cannot resolve '$targetString.*' given input columns '$columns'")
  }

  def addColumnWithV1TableCannotSpecifyNotNullError(): Throwable = {
    new AnalysisException("ADD COLUMN with v1 tables cannot specify NOT NULL.")
  }

  def operationOnlySupportedWithV2TableError(operation: String): Throwable = {
    new AnalysisException(s"$operation is only supported with v2 tables.")
  }

  def alterColumnWithV1TableCannotSpecifyNotNullError(): Throwable = {
    new AnalysisException("ALTER COLUMN with v1 tables cannot specify NOT NULL.")
  }

  def alterColumnCannotFindColumnInV1TableError(colName: String, v1Table: V1Table): Throwable = {
    new AnalysisException(
      s"ALTER COLUMN cannot find column $colName in v1 table. " +
        s"Available: ${v1Table.schema.fieldNames.mkString(", ")}")
  }

  def invalidDatabaseNameError(quoted: String): Throwable = {
    new AnalysisException(s"The database name is not valid: $quoted")
  }

  def cannotDropViewWithDropTableError(): Throwable = {
    new AnalysisException("Cannot drop a view with DROP TABLE. Please use DROP VIEW instead")
  }

  def showColumnsWithConflictDatabasesError(
      db: Seq[String], v1TableName: TableIdentifier): Throwable = {
    new AnalysisException("SHOW COLUMNS with conflicting databases: " +
        s"'${db.head}' != '${v1TableName.database.get}'")
  }

  def sqlOnlySupportedWithV1TablesError(sql: String): Throwable = {
    new AnalysisException(s"$sql is only supported with v1 tables.")
  }

  def cannotCreateTableWithBothProviderAndSerdeError(
      provider: Option[String], maybeSerdeInfo: Option[SerdeInfo]): Throwable = {
    new AnalysisException(
      s"Cannot create table with both USING $provider and ${maybeSerdeInfo.get.describe}")
  }

  def invalidFileFormatForStoredAsError(serdeInfo: SerdeInfo): Throwable = {
    new AnalysisException(
      s"STORED AS with file format '${serdeInfo.storedAs.get}' is invalid.")
  }

  def commandNotSupportNestedColumnError(command: String, quoted: String): Throwable = {
    new AnalysisException(s"$command does not support nested column: $quoted")
  }

  def columnDoesNotExistError(colName: String): Throwable = {
    new AnalysisException(s"Column $colName does not exist")
  }

  def renameTempViewToExistingViewError(oldName: String, newName: String): Throwable = {
    new AnalysisException(
      s"rename temporary view from '$oldName' to '$newName': destination view already exists")
  }

  def cannotDropNonemptyDatabaseError(db: String): Throwable = {
    new AnalysisException(s"Cannot drop a non-empty database: $db. " +
      "Use CASCADE option to drop a non-empty database.")
  }

  def cannotDropNonemptyNamespaceError(namespace: Seq[String]): Throwable = {
    new AnalysisException(s"Cannot drop a non-empty namespace: ${namespace.quoted}. " +
      "Use CASCADE option to drop a non-empty namespace.")
  }

  def invalidNameForTableOrDatabaseError(name: String): Throwable = {
    new AnalysisException(s"`$name` is not a valid name for tables/databases. " +
      "Valid names only contain alphabet characters, numbers and _.")
  }

  def cannotCreateDatabaseWithSameNameAsPreservedDatabaseError(database: String): Throwable = {
    new AnalysisException(s"$database is a system preserved database, " +
      "you cannot create a database with this name.")
  }

  def cannotDropDefaultDatabaseError(): Throwable = {
    new AnalysisException("Can not drop default database")
  }

  def cannotUsePreservedDatabaseAsCurrentDatabaseError(database: String): Throwable = {
    new AnalysisException(s"$database is a system preserved database, you cannot use it as " +
      "current database. To access global temporary views, you should use qualified name with " +
      s"the GLOBAL_TEMP_DATABASE, e.g. SELECT * FROM $database.viewName.")
  }

  def createExternalTableWithoutLocationError(): Throwable = {
    new AnalysisException("CREATE EXTERNAL TABLE must be accompanied by LOCATION")
  }

  def cannotOperateManagedTableWithExistingLocationError(
      methodName: String, tableIdentifier: TableIdentifier, tableLocation: Path): Throwable = {
    new AnalysisException(s"Can not $methodName the managed table('$tableIdentifier')" +
      s". The associated location('${tableLocation.toString}') already exists.")
  }

  def dropNonExistentColumnsNotSupportedError(
      nonExistentColumnNames: Seq[String]): Throwable = {
    new AnalysisException(
      s"""
         |Some existing schema fields (${nonExistentColumnNames.mkString("[", ",", "]")}) are
         |not present in the new schema. We don't support dropping columns yet.
         """.stripMargin)
  }

  def cannotRetrieveTableOrViewNotInSameDatabaseError(
      qualifiedTableNames: Seq[QualifiedTableName]): Throwable = {
    new AnalysisException("Only the tables/views belong to the same database can be retrieved. " +
      s"Querying tables/views are $qualifiedTableNames")
  }

  def renameTableSourceAndDestinationMismatchError(db: String, newDb: String): Throwable = {
    new AnalysisException(
      s"RENAME TABLE source and destination databases do not match: '$db' != '$newDb'")
  }

  def cannotRenameTempViewWithDatabaseSpecifiedError(
      oldName: TableIdentifier, newName: TableIdentifier): Throwable = {
    new AnalysisException(s"RENAME TEMPORARY VIEW from '$oldName' to '$newName': cannot " +
      s"specify database name '${newName.database.get}' in the destination table")
  }

  def cannotRenameTempViewToExistingTableError(
      oldName: TableIdentifier, newName: TableIdentifier): Throwable = {
    new AnalysisException(s"RENAME TEMPORARY VIEW from '$oldName' to '$newName': " +
      "destination table already exists")
  }

  def invalidPartitionSpecError(details: String): Throwable = {
    new AnalysisException(s"Partition spec is invalid. $details")
  }

  def functionAlreadyExistsError(func: FunctionIdentifier): Throwable = {
    new AnalysisException(s"Function $func already exists")
  }

  def cannotLoadClassWhenRegisteringFunctionError(
      className: String, func: FunctionIdentifier): Throwable = {
    new AnalysisException(s"Can not load class '$className' when registering " +
      s"the function '$func', please make sure it is on the classpath")
  }

  def resourceTypeNotSupportedError(resourceType: String): Throwable = {
    new AnalysisException(s"Resource Type '$resourceType' is not supported.")
  }

  def tableNotSpecifyDatabaseError(identifier: TableIdentifier): Throwable = {
    new AnalysisException(s"table $identifier did not specify database")
  }

  def tableNotSpecifyLocationUriError(identifier: TableIdentifier): Throwable = {
    new AnalysisException(s"table $identifier did not specify locationUri")
  }

  def partitionNotSpecifyLocationUriError(specString: String): Throwable = {
    new AnalysisException(s"Partition [$specString] did not specify locationUri")
  }

  def invalidBucketNumberError(bucketingMaxBuckets: Int, numBuckets: Int): Throwable = {
    new AnalysisException(
      s"Number of buckets should be greater than 0 but less than or equal to " +
        s"bucketing.maxBuckets (`$bucketingMaxBuckets`). Got `$numBuckets`")
  }

  def corruptedTableNameContextInCatalogError(numParts: Int, index: Int): Throwable = {
    new AnalysisException("Corrupted table name context in catalog: " +
      s"$numParts parts expected, but part $index is missing.")
  }

  def corruptedViewSQLConfigsInCatalogError(e: Exception): Throwable = {
    new AnalysisException("Corrupted view SQL configs in catalog", cause = Some(e))
  }

  def corruptedViewQueryOutputColumnsInCatalogError(numCols: String, index: Int): Throwable = {
    new AnalysisException("Corrupted view query output column names in catalog: " +
      s"$numCols parts expected, but part $index is missing.")
  }

  def corruptedViewReferredTempViewInCatalogError(e: Exception): Throwable = {
    new AnalysisException("corrupted view referred temp view names in catalog", cause = Some(e))
  }

  def corruptedViewReferredTempFunctionsInCatalogError(e: Exception): Throwable = {
    new AnalysisException(
      "corrupted view referred temp functions names in catalog", cause = Some(e))
  }

  def columnStatisticsDeserializationNotSupportedError(
      name: String, dataType: DataType): Throwable = {
    new AnalysisException("Column statistics deserialization is not supported for " +
      s"column $name of data type: $dataType.")
  }

  def columnStatisticsSerializationNotSupportedError(
      colName: String, dataType: DataType): Throwable = {
    new AnalysisException("Column statistics serialization is not supported for " +
      s"column $colName of data type: $dataType.")
  }

  def cannotReadCorruptedTablePropertyError(key: String, details: String = ""): Throwable = {
    new AnalysisException(s"Cannot read table property '$key' as it's corrupted.$details")
  }

  def invalidSchemaStringError(exp: Expression): Throwable = {
    new AnalysisException(s"The expression '${exp.sql}' is not a valid schema string.")
  }

  def schemaNotFoldableError(exp: Expression): Throwable = {
    new AnalysisException(
      "Schema should be specified in DDL format as a string literal or output of " +
        s"the schema_of_json/schema_of_csv functions instead of ${exp.sql}")
  }

  def schemaIsNotStructTypeError(dataType: DataType): Throwable = {
    new AnalysisException(s"Schema should be struct type but got ${dataType.sql}.")
  }

  def keyValueInMapNotStringError(m: CreateMap): Throwable = {
    new AnalysisException(
      s"A type of keys and values in map() must be string, but got ${m.dataType.catalogString}")
  }

  def nonMapFunctionNotAllowedError(): Throwable = {
    new AnalysisException("Must use a map() function for options")
  }

  def invalidFieldTypeForCorruptRecordError(): Throwable = {
    new AnalysisException("The field for corrupt records must be string type and nullable")
  }

  def dataTypeUnsupportedByClassError(x: DataType, className: String): Throwable = {
    new AnalysisException(s"DataType '$x' is not supported by $className.")
  }

  def parseModeUnsupportedError(funcName: String, mode: ParseMode): Throwable = {
    new AnalysisException(s"$funcName() doesn't support the ${mode.name} mode. " +
      s"Acceptable modes are ${PermissiveMode.name} and ${FailFastMode.name}.")
  }

  def unfoldableFieldUnsupportedError(): Throwable = {
    new AnalysisException("The field parameter needs to be a foldable string value.")
  }

  def literalTypeUnsupportedForSourceTypeError(field: String, source: Expression): Throwable = {
    new AnalysisException(s"Literals of type '$field' are currently not supported " +
      s"for the ${source.dataType.catalogString} type.")
  }

  def arrayComponentTypeUnsupportedError(clz: Class[_]): Throwable = {
    new AnalysisException(s"Unsupported component type $clz in arrays")
  }

  def secondArgumentNotDoubleLiteralError(): Throwable = {
    new AnalysisException("The second argument should be a double literal.")
  }

  def dataTypeUnsupportedByExtractValueError(
      dataType: DataType, extraction: Expression, child: Expression): Throwable = {
    val errorMsg = dataType match {
      case StructType(_) =>
        s"Field name should be String Literal, but it's $extraction"
      case other =>
        s"Can't extract value from $child: need struct type but got ${other.catalogString}"
    }
    new AnalysisException(errorMsg)
  }

  def noHandlerForUDAFError(name: String): Throwable = {
    new InvalidUDFClassException(s"No handler for UDAF '$name'. " +
      "Use sparkSession.udf.register(...) instead.")
  }

  def batchWriteCapabilityError(
      table: Table, v2WriteClassName: String, v1WriteClassName: String): Throwable = {
    new AnalysisException(
      s"Table ${table.name} declares ${TableCapability.V1_BATCH_WRITE} capability but " +
        s"$v2WriteClassName is not an instance of $v1WriteClassName")
  }

  def unsupportedDeleteByConditionWithSubqueryError(condition: Option[Expression]): Throwable = {
    new AnalysisException(
      s"Delete by condition with subquery is not supported: $condition")
  }

  def cannotTranslateExpressionToSourceFilterError(f: Expression): Throwable = {
    new AnalysisException("Exec update failed:" +
      s" cannot translate expression to source filter: $f")
  }

  def cannotDeleteTableWhereFiltersError(table: Table, filters: Array[Filter]): Throwable = {
    new AnalysisException(
      s"Cannot delete from table ${table.name} where ${filters.mkString("[", ", ", "]")}")
  }

  def deleteOnlySupportedWithV2TablesError(): Throwable = {
    new AnalysisException("DELETE is only supported with v2 tables.")
  }

  def describeDoesNotSupportPartitionForV2TablesError(): Throwable = {
    new AnalysisException("DESCRIBE does not support partition for v2 tables.")
  }

  def cannotReplaceMissingTableError(
      tableIdentifier: Identifier): Throwable = {
    new CannotReplaceMissingTableException(tableIdentifier)
  }

  def cannotReplaceMissingTableError(
      tableIdentifier: Identifier, cause: Option[Throwable]): Throwable = {
    new CannotReplaceMissingTableException(tableIdentifier, cause)
  }

  def unsupportedTableOperationError(table: Table, cmd: String): Throwable = {
    new AnalysisException(s"Table ${table.name} does not support $cmd.")
  }

  def unsupportedBatchReadError(table: Table): Throwable = {
    unsupportedTableOperationError(table, "batch scan")
  }

  def unsupportedMicroBatchOrContinuousScanError(table: Table): Throwable = {
    unsupportedTableOperationError(table, "either micro-batch or continuous scan")
  }

  def unsupportedAppendInBatchModeError(table: Table): Throwable = {
    unsupportedTableOperationError(table, "append in batch mode")
  }

  def unsupportedDynamicOverwriteInBatchModeError(table: Table): Throwable = {
    unsupportedTableOperationError(table, "dynamic overwrite in batch mode")
  }

  def unsupportedTruncateInBatchModeError(table: Table): Throwable = {
    unsupportedTableOperationError(table, "truncate in batch mode")
  }

  def unsupportedOverwriteByFilterInBatchModeError(table: Table): Throwable = {
    unsupportedTableOperationError(table, "overwrite by filter in batch mode")
  }

  def streamingSourcesDoNotSupportCommonExecutionModeError(
      microBatchSources: Seq[String],
      continuousSources: Seq[String]): Throwable = {
    new AnalysisException(
      "The streaming sources in a query do not have a common supported execution mode.\n" +
        "Sources support micro-batch: " + microBatchSources.mkString(", ") + "\n" +
        "Sources support continuous: " + continuousSources.mkString(", "))
  }

  def noSuchTableError(ident: Identifier): Throwable = {
    new NoSuchTableException(ident)
  }

  def noSuchNamespaceError(namespace: Array[String]): Throwable = {
    new NoSuchNamespaceException(namespace)
  }

  def tableAlreadyExistsError(ident: Identifier): Throwable = {
    new TableAlreadyExistsException(ident)
  }

  def requiresSinglePartNamespaceError(ns: Seq[String]): Throwable = {
    new AnalysisException(CatalogManager.SESSION_CATALOG_NAME +
      " requires a single-part namespace, but got " + ns.mkString("[", ", ", "]"))
  }

  def namespaceAlreadyExistsError(namespace: Array[String]): Throwable = {
    new NamespaceAlreadyExistsException(namespace)
  }

  private def notSupportedInJDBCCatalog(cmd: String): Throwable = {
    new AnalysisException(s"$cmd is not supported in JDBC catalog.")
  }

  def cannotCreateJDBCTableUsingProviderError(): Throwable = {
    notSupportedInJDBCCatalog("CREATE TABLE ... USING ...")
  }

  def cannotCreateJDBCTableUsingLocationError(): Throwable = {
    notSupportedInJDBCCatalog("CREATE TABLE ... LOCATION ...")
  }

  def cannotCreateJDBCNamespaceUsingProviderError(): Throwable = {
    notSupportedInJDBCCatalog("CREATE NAMESPACE ... LOCATION ...")
  }

  def cannotCreateJDBCNamespaceWithPropertyError(k: String): Throwable = {
    notSupportedInJDBCCatalog(s"CREATE NAMESPACE with property $k")
  }

  def cannotSetJDBCNamespaceWithPropertyError(k: String): Throwable = {
    notSupportedInJDBCCatalog(s"SET NAMESPACE with property $k")
  }

  def cannotUnsetJDBCNamespaceWithPropertyError(k: String): Throwable = {
    notSupportedInJDBCCatalog(s"Remove NAMESPACE property $k")
  }

  def unsupportedJDBCNamespaceChangeInCatalogError(changes: Seq[NamespaceChange]): Throwable = {
    new AnalysisException(s"Unsupported NamespaceChange $changes in JDBC catalog.")
  }

  private def tableDoesNotSupportError(cmd: String, table: Table): Throwable = {
    new AnalysisException(s"Table does not support $cmd: ${table.name}")
  }

  def tableDoesNotSupportReadsError(table: Table): Throwable = {
    tableDoesNotSupportError("reads", table)
  }

  def tableDoesNotSupportWritesError(table: Table): Throwable = {
    tableDoesNotSupportError("writes", table)
  }

  def tableDoesNotSupportDeletesError(table: Table): Throwable = {
    tableDoesNotSupportError("deletes", table)
  }

  def tableDoesNotSupportTruncatesError(table: Table): Throwable = {
    tableDoesNotSupportError("truncates", table)
  }

  def tableDoesNotSupportPartitionManagementError(table: Table): Throwable = {
    tableDoesNotSupportError("partition management", table)
  }

  def tableDoesNotSupportAtomicPartitionManagementError(table: Table): Throwable = {
    tableDoesNotSupportError("atomic partition management", table)
  }

  def cannotRenameTableWithAlterViewError(): Throwable = {
    new AnalysisException(
      "Cannot rename a table with ALTER VIEW. Please use ALTER TABLE instead.")
  }

  private def notSupportedForV2TablesError(cmd: String): Throwable = {
    new AnalysisException(s"$cmd is not supported for v2 tables.")
  }

  def analyzeTableNotSupportedForV2TablesError(): Throwable = {
    notSupportedForV2TablesError("ANALYZE TABLE")
  }

  def alterTableRecoverPartitionsNotSupportedForV2TablesError(): Throwable = {
    notSupportedForV2TablesError("ALTER TABLE ... RECOVER PARTITIONS")
  }

  def alterTableSerDePropertiesNotSupportedForV2TablesError(): Throwable = {
    notSupportedForV2TablesError("ALTER TABLE ... SET [SERDE|SERDEPROPERTIES]")
  }

  def loadDataNotSupportedForV2TablesError(): Throwable = {
    notSupportedForV2TablesError("LOAD DATA")
  }

  def showCreateTableAsSerdeNotSupportedForV2TablesError(): Throwable = {
    notSupportedForV2TablesError("SHOW CREATE TABLE AS SERDE")
  }

  def showColumnsNotSupportedForV2TablesError(): Throwable = {
    notSupportedForV2TablesError("SHOW COLUMNS")
  }

  def repairTableNotSupportedForV2TablesError(): Throwable = {
    notSupportedForV2TablesError("MSCK REPAIR TABLE")
  }

  def databaseFromV1SessionCatalogNotSpecifiedError(): Throwable = {
    new AnalysisException("Database from v1 session catalog is not specified")
  }

  def nestedDatabaseUnsupportedByV1SessionCatalogError(catalog: String): Throwable = {
    new AnalysisException(s"Nested databases are not supported by v1 session catalog: $catalog")
  }

  def invalidRepartitionExpressionsError(sortOrders: Seq[Any]): Throwable = {
    new AnalysisException(s"Invalid partitionExprs specified: $sortOrders For range " +
      "partitioning use REPARTITION_BY_RANGE instead.")
  }

  def partitionColumnNotSpecifiedError(format: String, partitionColumn: String): Throwable = {
    new AnalysisException(s"Failed to resolve the schema for $format for " +
      s"the partition column: $partitionColumn. It must be specified manually.")
  }

  def dataSchemaNotSpecifiedError(format: String): Throwable = {
    new AnalysisException(s"Unable to infer schema for $format. It must be specified manually.")
  }

  def dataPathNotExistError(path: String): Throwable = {
    new AnalysisException(s"Path does not exist: $path")
  }

  def dataSourceOutputModeUnsupportedError(
      className: String, outputMode: OutputMode): Throwable = {
    new AnalysisException(s"Data source $className does not support $outputMode output mode")
  }

  def schemaNotSpecifiedForSchemaRelationProviderError(className: String): Throwable = {
    new AnalysisException(s"A schema needs to be specified when using $className.")
  }

  def userSpecifiedSchemaMismatchActualSchemaError(
      schema: StructType, actualSchema: StructType): Throwable = {
    new AnalysisException(
      s"""
         |The user-specified schema doesn't match the actual schema:
         |user-specified: ${schema.toDDL}, actual: ${actualSchema.toDDL}. If you're using
         |DataFrameReader.schema API or creating a table, please do not specify the schema.
         |Or if you're scanning an existed table, please drop it and re-create it.
       """.stripMargin)
  }

  def dataSchemaNotSpecifiedError(format: String, fileCatalog: String): Throwable = {
    new AnalysisException(
      s"Unable to infer schema for $format at $fileCatalog. It must be specified manually")
  }

  def invalidDataSourceError(className: String): Throwable = {
    new AnalysisException(s"$className is not a valid Spark SQL Data Source.")
  }

  def cannotSaveIntervalIntoExternalStorageError(): Throwable = {
    new AnalysisException("Cannot save interval data type into external storage.")
  }

  def cannotResolveAttributeError(name: String, outputStr: String): Throwable = {
    new AnalysisException(
      s"Unable to resolve $name given [$outputStr]")
  }

  def orcNotUsedWithHiveEnabledError(): Throwable = {
    new AnalysisException(
      s"""
         |Hive built-in ORC data source must be used with Hive support enabled.
         |Please use the native ORC data source by setting 'spark.sql.orc.impl' to 'native'
       """.stripMargin)
  }

  def failedToFindAvroDataSourceError(provider: String): Throwable = {
    new AnalysisException(
      s"""
         |Failed to find data source: $provider. Avro is built-in but external data
         |source module since Spark 2.4. Please deploy the application as per
         |the deployment section of "Apache Avro Data Source Guide".
       """.stripMargin.replaceAll("\n", " "))
  }

  def failedToFindKafkaDataSourceError(provider: String): Throwable = {
    new AnalysisException(
      s"""
         |Failed to find data source: $provider. Please deploy the application as
         |per the deployment section of "Structured Streaming + Kafka Integration Guide".
       """.stripMargin.replaceAll("\n", " "))
  }

  def findMultipleDataSourceError(provider: String, sourceNames: Seq[String]): Throwable = {
    new AnalysisException(
      s"""
         |Multiple sources found for $provider (${sourceNames.mkString(", ")}),
         | please specify the fully qualified class name.
       """.stripMargin)
  }

  def writeEmptySchemasUnsupportedByDataSourceError(): Throwable = {
    new AnalysisException(
      s"""
         |Datasource does not support writing empty or nested empty schemas.
         |Please make sure the data schema has at least one or more column(s).
       """.stripMargin)
  }

  def insertMismatchedColumnNumberError(
      targetAttributes: Seq[Attribute],
      sourceAttributes: Seq[Attribute],
      staticPartitionsSize: Int): Throwable = {
    new AnalysisException(
      s"""
         |The data to be inserted needs to have the same number of columns as the
         |target table: target table has ${targetAttributes.size} column(s) but the
         |inserted data has ${sourceAttributes.size + staticPartitionsSize} column(s),
         |which contain $staticPartitionsSize partition column(s) having assigned
         |constant values.
       """.stripMargin)
  }

  def insertMismatchedPartitionNumberError(
      targetPartitionSchema: StructType,
      providedPartitionsSize: Int): Throwable = {
    new AnalysisException(
      s"""
         |The data to be inserted needs to have the same number of partition columns
         |as the target table: target table has ${targetPartitionSchema.fields.size}
         |partition column(s) but the inserted data has $providedPartitionsSize
         |partition columns specified.
       """.stripMargin.replaceAll("\n", " "))
  }

  def invalidPartitionColumnError(
      partKey: String, targetPartitionSchema: StructType): Throwable = {
    new AnalysisException(
      s"""
         |$partKey is not a partition column. Partition columns are
         |${targetPartitionSchema.fields.map(_.name).mkString("[", ",", "]")}
       """.stripMargin)
  }

  def multiplePartitionColumnValuesSpecifiedError(
      field: StructField, potentialSpecs: Map[String, String]): Throwable = {
    new AnalysisException(
      s"""
         |Partition column ${field.name} have multiple values specified,
         |${potentialSpecs.mkString("[", ", ", "]")}. Please only specify a single value.
       """.stripMargin)
  }

  def invalidOrderingForConstantValuePartitionColumnError(
      targetPartitionSchema: StructType): Throwable = {
    new AnalysisException(
      s"""
         |The ordering of partition columns is
         |${targetPartitionSchema.fields.map(_.name).mkString("[", ",", "]")}
         |All partition columns having constant values need to appear before other
         |partition columns that do not have an assigned constant value.
       """.stripMargin)
  }

  def cannotWriteDataToRelationsWithMultiplePathsError(): Throwable = {
    new AnalysisException("Can only write data to relations with a single path.")
  }

  def failedToRebuildExpressionError(filter: Filter): Throwable = {
    new AnalysisException(
      s"Fail to rebuild expression: missing key $filter in `translatedFilterToExpr`")
  }

  def dataTypeUnsupportedByDataSourceError(format: String, field: StructField): Throwable = {
    new AnalysisException(
      s"$format data source does not support ${field.dataType.catalogString} data type.")
  }

  def failToResolveDataSourceForTableError(table: CatalogTable, key: String): Throwable = {
    new AnalysisException(
      s"""
         |Fail to resolve data source for the table ${table.identifier} since the table
         |serde property has the duplicated key $key with extra options specified for this
         |scan operation. To fix this, you can rollback to the legacy behavior of ignoring
         |the extra options by setting the config
         |${SQLConf.LEGACY_EXTRA_OPTIONS_BEHAVIOR.key} to `false`, or address the
         |conflicts of the same config.
       """.stripMargin)
  }

  def outputPathAlreadyExistsError(outputPath: Path): Throwable = {
    new AnalysisException(s"path $outputPath already exists.")
  }

  def cannotUseDataTypeForPartitionColumnError(field: StructField): Throwable = {
    new AnalysisException(s"Cannot use ${field.dataType} for partition column")
  }

  def cannotUseAllColumnsForPartitionColumnsError(): Throwable = {
    new AnalysisException(s"Cannot use all columns for partition columns")
  }

  def partitionColumnNotFoundInSchemaError(col: String, schemaCatalog: String): Throwable = {
    new AnalysisException(s"Partition column `$col` not found in schema $schemaCatalog")
  }

  def columnNotFoundInSchemaError(
      col: StructField, tableSchema: Option[StructType]): Throwable = {
    new AnalysisException(s"""Column "${col.name}" not found in schema $tableSchema""")
  }

  def unsupportedDataSourceTypeForDirectQueryOnFilesError(className: String): Throwable = {
    new AnalysisException(s"Unsupported data source type for direct query on files: $className")
  }

  def saveDataIntoViewNotAllowedError(): Throwable = {
    new AnalysisException("Saving data into a view is not allowed.")
  }

  def mismatchedTableFormatError(
      tableName: String, existingProvider: Class[_], specifiedProvider: Class[_]): Throwable = {
    new AnalysisException(
      s"""
         |The format of the existing table $tableName is `${existingProvider.getSimpleName}`.
         |It doesn't match the specified format `${specifiedProvider.getSimpleName}`.
       """.stripMargin)
  }

  def mismatchedTableLocationError(
      identifier: TableIdentifier,
      existingTable: CatalogTable,
      tableDesc: CatalogTable): Throwable = {
    new AnalysisException(
      s"""
         |The location of the existing table ${identifier.quotedString} is
         |`${existingTable.location}`. It doesn't match the specified location
         |`${tableDesc.location}`.
       """.stripMargin)
  }

  def mismatchedTableColumnNumberError(
      tableName: String,
      existingTable: CatalogTable,
      query: LogicalPlan): Throwable = {
    new AnalysisException(
      s"""
         |The column number of the existing table $tableName
         |(${existingTable.schema.catalogString}) doesn't match the data schema
         |(${query.schema.catalogString})
       """.stripMargin)
  }

  def cannotResolveColumnGivenInputColumnsError(col: String, inputColumns: String): Throwable = {
    new AnalysisException(s"cannot resolve '$col' given input columns: [$inputColumns]")
  }

  def mismatchedTablePartitionColumnError(
      tableName: String,
      specifiedPartCols: Seq[String],
      existingPartCols: String): Throwable = {
    new AnalysisException(
      s"""
         |Specified partitioning does not match that of the existing table $tableName.
         |Specified partition columns: [${specifiedPartCols.mkString(", ")}]
         |Existing partition columns: [$existingPartCols]
       """.stripMargin)
  }

  def mismatchedTableBucketingError(
      tableName: String,
      specifiedBucketString: String,
      existingBucketString: String): Throwable = {
    new AnalysisException(
      s"""
         |Specified bucketing does not match that of the existing table $tableName.
         |Specified bucketing: $specifiedBucketString
         |Existing bucketing: $existingBucketString
       """.stripMargin)
  }

  def specifyPartitionNotAllowedWhenTableSchemaNotDefinedError(): Throwable = {
    new AnalysisException("It is not allowed to specify partitioning when the " +
      "table schema is not defined.")
  }

  def bucketingColumnCannotBePartOfPartitionColumnsError(
      bucketCol: String, normalizedPartCols: Seq[String]): Throwable = {
    new AnalysisException(s"bucketing column '$bucketCol' should not be part of " +
      s"partition columns '${normalizedPartCols.mkString(", ")}'")
  }

  def bucketSortingColumnCannotBePartOfPartitionColumnsError(
    sortCol: String, normalizedPartCols: Seq[String]): Throwable = {
    new AnalysisException(s"bucket sorting column '$sortCol' should not be part of " +
      s"partition columns '${normalizedPartCols.mkString(", ")}'")
  }

  def mismatchedInsertedDataColumnNumberError(
      tableName: String, insert: InsertIntoStatement, staticPartCols: Set[String]): Throwable = {
    new AnalysisException(
      s"$tableName requires that the data to be inserted have the same number of columns as " +
        s"the target table: target table has ${insert.table.output.size} column(s) but the " +
        s"inserted data has ${insert.query.output.length + staticPartCols.size} column(s), " +
        s"including ${staticPartCols.size} partition column(s) having constant value(s).")
  }

  def requestedPartitionsMismatchTablePartitionsError(
      tableName: String,
      normalizedPartSpec: Map[String, Option[String]],
      partColNames: StructType): Throwable = {
    new AnalysisException(
      s"""
         |Requested partitioning does not match the table $tableName:
         |Requested partitions: ${normalizedPartSpec.keys.mkString(",")}
         |Table partitions: ${partColNames.mkString(",")}
       """.stripMargin)
  }

  def ddlWithoutHiveSupportEnabledError(detail: String): Throwable = {
    new AnalysisException(s"Hive support is required to $detail")
  }

  def createTableColumnTypesOptionColumnNotFoundInSchemaError(
      col: String, schema: StructType): Throwable = {
    new AnalysisException(
      s"createTableColumnTypes option column $col not found in schema ${schema.catalogString}")
  }

  def parquetTypeUnsupportedYetError(parquetType: String): Throwable = {
    new AnalysisException(s"Parquet type not yet supported: $parquetType")
  }

  def illegalParquetTypeError(parquetType: String): Throwable = {
    new AnalysisException(s"Illegal Parquet type: $parquetType")
  }

  def unrecognizedParquetTypeError(field: String): Throwable = {
    new AnalysisException(s"Unrecognized Parquet type: $field")
  }

  def cannotConvertDataTypeToParquetTypeError(field: StructField): Throwable = {
    new AnalysisException(s"Unsupported data type ${field.dataType.catalogString}")
  }

  def incompatibleViewSchemaChange(
      viewName: String,
      colName: String,
      expectedNum: Int,
      actualCols: Seq[Attribute],
      viewDDL: Option[String]): Throwable = {
    new AnalysisException(s"The SQL query of view $viewName has an incompatible schema change " +
      s"and column $colName cannot be resolved. Expected $expectedNum columns named $colName but " +
      s"got ${actualCols.map(_.name).mkString("[", ",", "]")}" +
      viewDDL.map(s => s"\nPlease try to re-create the view by running: $s").getOrElse(""))
  }

  def numberOfPartitionsNotAllowedWithUnspecifiedDistributionError(): Throwable = {
    throw new AnalysisException("The number of partitions can't be specified with unspecified" +
      " distribution. Invalid writer requirements detected.")
  }

  def cannotApplyTableValuedFunctionError(
      name: String, arguments: String, usage: String, details: String = ""): Throwable = {
    new AnalysisException(s"Table-valued function $name with alternatives: $usage\n" +
      s"cannot be applied to ($arguments): $details")
  }

  def incompatibleRangeInputDataTypeError(
      expression: Expression, dataType: DataType): Throwable = {
    new AnalysisException(s"Incompatible input data type. " +
      s"Expected: ${dataType.typeName}; Found: ${expression.dataType.typeName}")
  }

  def groupAggPandasUDFUnsupportedByStreamingAggError(): Throwable = {
    new AnalysisException("Streaming aggregation doesn't support group aggregate pandas UDF")
  }

  def streamJoinStreamWithoutEqualityPredicateUnsupportedError(plan: LogicalPlan): Throwable = {
    new AnalysisException(
      "Stream-stream join without equality predicate is not supported", plan = Some(plan))
  }

  def cannotUseMixtureOfAggFunctionAndGroupAggPandasUDFError(): Throwable = {
    new AnalysisException(
      "Cannot use a mixture of aggregate function and group aggregate pandas UDF")
  }

  def ambiguousAttributesInSelfJoinError(
      ambiguousAttrs: Seq[AttributeReference]): Throwable = {
    new AnalysisException(
      s"""
         |Column ${ambiguousAttrs.mkString(", ")} are ambiguous. It's probably because
         |you joined several Datasets together, and some of these Datasets are the same.
         |This column points to one of the Datasets but Spark is unable to figure out
         |which one. Please alias the Datasets with different names via `Dataset.as`
         |before joining them, and specify the column using qualified name, e.g.
         |`df.as("a").join(df.as("b"), $$"a.id" > $$"b.id")`. You can also set
         |${SQLConf.FAIL_AMBIGUOUS_SELF_JOIN_ENABLED.key} to false to disable this check.
       """.stripMargin.replaceAll("\n", " "))
  }

  def unexpectedEvalTypesForUDFsError(evalTypes: Set[Int]): Throwable = {
    new AnalysisException(
      s"Expected udfs have the same evalType but got different evalTypes: " +
        s"${evalTypes.mkString(",")}")
  }

  def ambiguousFieldNameError(
      fieldName: Seq[String], numMatches: Int, context: Origin): Throwable = {
    new AnalysisException(
      errorClass = "AMBIGUOUS_FIELD_NAME",
      messageParameters = Array(fieldName.quoted, numMatches.toString),
      origin = context)
  }

  def cannotUseIntervalTypeInTableSchemaError(): Throwable = {
    new AnalysisException("Cannot use interval type in the table schema.")
  }

  def cannotPartitionByNestedColumnError(reference: NamedReference): Throwable = {
    new AnalysisException(s"Cannot partition by nested column: $reference")
  }

  def missingCatalogAbilityError(plugin: CatalogPlugin, ability: String): Throwable = {
    new AnalysisException(s"Catalog ${plugin.name} does not support $ability")
  }

  def identifierHavingMoreThanTwoNamePartsError(
      quoted: String, identifier: String): Throwable = {
    new AnalysisException(s"$quoted is not a valid $identifier as it has more than 2 name parts.")
  }

  def emptyMultipartIdentifierError(): Throwable = {
    new AnalysisException("multi-part identifier cannot be empty.")
  }

  def cannotOperateOnHiveDataSourceFilesError(operation: String): Throwable = {
    new AnalysisException("Hive data source can only be used with tables, you can not " +
      s"$operation files of Hive data source directly.")
  }

  def setPathOptionAndCallWithPathParameterError(method: String): Throwable = {
    new AnalysisException(
      s"""
         |There is a 'path' option set and $method() is called with a path
         |parameter. Either remove the path option, or call $method() without the parameter.
         |To ignore this check, set '${SQLConf.LEGACY_PATH_OPTION_BEHAVIOR.key}' to 'true'.
       """.stripMargin.replaceAll("\n", " "))
  }

  def userSpecifiedSchemaUnsupportedError(operation: String): Throwable = {
    new AnalysisException(s"User specified schema not supported with `$operation`")
  }

  def tempViewNotSupportStreamingWriteError(viewName: String): Throwable = {
    new AnalysisException(s"Temporary view $viewName doesn't support streaming write")
  }

  def streamingIntoViewNotSupportedError(viewName: String): Throwable = {
    new AnalysisException(s"Streaming into views $viewName is not supported.")
  }

  def inputSourceDiffersFromDataSourceProviderError(
      source: String, tableName: String, table: CatalogTable): Throwable = {
    new AnalysisException(s"The input source($source) is different from the table " +
      s"$tableName's data source provider(${table.provider.get}).")
  }

  def tableNotSupportStreamingWriteError(tableName: String, t: Table): Throwable = {
    new AnalysisException(s"Table $tableName doesn't support streaming write - $t")
  }

  def queryNameNotSpecifiedForMemorySinkError(): Throwable = {
    new AnalysisException("queryName must be specified for memory sink")
  }

  def sourceNotSupportedWithContinuousTriggerError(source: String): Throwable = {
    new AnalysisException(s"'$source' is not supported with continuous trigger")
  }

  def columnNotFoundInExistingColumnsError(
      columnType: String, columnName: String, validColumnNames: Seq[String]): Throwable = {
    new AnalysisException(s"$columnType column $columnName not found in " +
      s"existing columns (${validColumnNames.mkString(", ")})")
  }

  def operationNotSupportPartitioningError(operation: String): Throwable = {
    new AnalysisException(s"'$operation' does not support partitioning")
  }

  def mixedRefsInAggFunc(funcStr: String): Throwable = {
    val msg = "Found an aggregate function in a correlated predicate that has both " +
      "outer and local references, which is not supported: " + funcStr
    new AnalysisException(msg)
  }

  def lookupFunctionInNonFunctionCatalogError(
      ident: Identifier, catalog: CatalogPlugin): Throwable = {
    new AnalysisException(s"Trying to lookup function '$ident' in " +
      s"catalog '${catalog.name()}', but it is not a FunctionCatalog.")
  }

  def functionCannotProcessInputError(
      unbound: UnboundFunction,
      arguments: Seq[Expression],
      unsupported: UnsupportedOperationException): Throwable = {
    new AnalysisException(s"Function '${unbound.name}' cannot process " +
      s"input: (${arguments.map(_.dataType.simpleString).mkString(", ")}): " +
      unsupported.getMessage, cause = Some(unsupported))
  }

  def v2FunctionInvalidInputTypeLengthError(
      bound: BoundFunction,
      args: Seq[Expression]): Throwable = {
    new AnalysisException(s"Invalid bound function '${bound.name()}: there are ${args.length} " +
        s"arguments but ${bound.inputTypes().length} parameters returned from 'inputTypes()'")
  }

  def ambiguousRelationAliasNameInNestedCTEError(name: String): Throwable = {
    new AnalysisException(s"Name $name is ambiguous in nested CTE. " +
      s"Please set ${LEGACY_CTE_PRECEDENCE_POLICY.key} to CORRECTED so that name " +
      "defined in inner CTE takes precedence. If set it to LEGACY, outer CTE " +
      "definitions will take precedence. See more details in SPARK-28228.")
  }

  def commandUnsupportedInV2TableError(name: String): Throwable = {
    new AnalysisException(s"$name is not supported for v2 tables.")
  }

  def cannotResolveColumnNameAmongAttributesError(
      colName: String, fieldNames: String): Throwable = {
    new AnalysisException(s"""Cannot resolve column name "$colName" among ($fieldNames)""")
  }

  def cannotWriteTooManyColumnsToTableError(
      tableName: String, expected: Seq[Attribute], query: LogicalPlan): Throwable = {
    new AnalysisException(
      s"""
         |Cannot write to '$tableName', too many data columns:
         |Table columns: ${expected.map(c => s"'${c.name}'").mkString(", ")}
         |Data columns: ${query.output.map(c => s"'${c.name}'").mkString(", ")}
       """.stripMargin)
  }

  def cannotWriteNotEnoughColumnsToTableError(
      tableName: String, expected: Seq[Attribute], query: LogicalPlan): Throwable = {
    new AnalysisException(
      s"""Cannot write to '$tableName', not enough data columns:
         |Table columns: ${expected.map(c => s"'${c.name}'").mkString(", ")}
         |Data columns: ${query.output.map(c => s"'${c.name}'").mkString(", ")}"""
        .stripMargin)
  }

  def cannotWriteIncompatibleDataToTableError(tableName: String, errors: Seq[String]): Throwable = {
    new AnalysisException(
      s"Cannot write incompatible data to table '$tableName':\n- ${errors.mkString("\n- ")}")
  }

  def secondArgumentOfFunctionIsNotIntegerError(
      function: String, e: NumberFormatException): Throwable = {
    // The second argument of '{function}' function needs to be an integer
    new AnalysisException(
      errorClass = "SECOND_FUNCTION_ARGUMENT_NOT_INTEGER",
      messageParameters = Array(function),
      cause = Some(e))
  }

  def nonPartitionPruningPredicatesNotExpectedError(
      nonPartitionPruningPredicates: Seq[Expression]): Throwable = {
    new AnalysisException(
      s"Expected only partition pruning predicates: $nonPartitionPruningPredicates")
  }

  def columnNotDefinedInTableError(
      colType: String, colName: String, tableName: String, tableCols: Seq[String]): Throwable = {
    new AnalysisException(s"$colType column $colName is not defined in table $tableName, " +
      s"defined table columns are: ${tableCols.mkString(", ")}")
  }

  def invalidLiteralForWindowDurationError(): Throwable = {
    new AnalysisException("The duration and time inputs to window must be " +
      "an integer, long or string literal.")
  }

  def noSuchStructFieldInGivenFieldsError(
      fieldName: String, fields: Array[StructField]): Throwable = {
    new AnalysisException(
      s"No such struct field $fieldName in ${fields.map(_.name).mkString(", ")}")
  }

  def ambiguousReferenceToFieldsError(fields: String): Throwable = {
    new AnalysisException(s"Ambiguous reference to fields $fields")
  }

  def secondArgumentInFunctionIsNotBooleanLiteralError(funcName: String): Throwable = {
    new AnalysisException(s"The second argument in $funcName should be a boolean literal.")
  }

  def joinConditionMissingOrTrivialError(
      join: Join, left: LogicalPlan, right: LogicalPlan): Throwable = {
    new AnalysisException(
      s"""Detected implicit cartesian product for ${join.joinType.sql} join between logical plans
         |${left.treeString(false).trim}
         |and
         |${right.treeString(false).trim}
         |Join condition is missing or trivial.
         |Either: use the CROSS JOIN syntax to allow cartesian products between these
         |relations, or: enable implicit cartesian products by setting the configuration
         |variable spark.sql.crossJoin.enabled=true"""
        .stripMargin)
  }

  def usePythonUDFInJoinConditionUnsupportedError(joinType: JoinType): Throwable = {
    new AnalysisException("Using PythonUDF in join condition of join type" +
      s" $joinType is not supported.")
  }

  def conflictingAttributesInJoinConditionError(
      conflictingAttrs: AttributeSet, outerPlan: LogicalPlan, subplan: LogicalPlan): Throwable = {
    new AnalysisException("Found conflicting attributes " +
      s"${conflictingAttrs.mkString(",")} in the condition joining outer plan:\n  " +
      s"$outerPlan\nand subplan:\n  $subplan")
  }

  def emptyWindowExpressionError(expr: Window): Throwable = {
    new AnalysisException(s"Window expression is empty in $expr")
  }

  def foundDifferentWindowFunctionTypeError(windowExpressions: Seq[NamedExpression]): Throwable = {
    new AnalysisException(
      s"Found different window function type in $windowExpressions")
  }

  def charOrVarcharTypeAsStringUnsupportedError(): Throwable = {
    new AnalysisException("char/varchar type can only be used in the table schema. " +
      s"You can set ${SQLConf.LEGACY_CHAR_VARCHAR_AS_STRING.key} to true, so that Spark" +
      s" treat them as string type as same as Spark 3.0 and earlier")
  }

  def invalidPatternError(pattern: String, message: String): Throwable = {
    new AnalysisException(
      s"the pattern '$pattern' is invalid, $message")
  }

  def tableIdentifierExistsError(tableIdentifier: TableIdentifier): Throwable = {
    new AnalysisException(s"$tableIdentifier already exists.")
  }

  def tableIdentifierNotConvertedToHadoopFsRelationError(
      tableIdentifier: TableIdentifier): Throwable = {
    new AnalysisException(s"$tableIdentifier should be converted to HadoopFsRelation.")
  }

  def alterDatabaseLocationUnsupportedError(version: String): Throwable = {
    new AnalysisException(s"Hive $version does not support altering database location")
  }

  def hiveTableTypeUnsupportedError(tableType: String): Throwable = {
    new AnalysisException(s"Hive $tableType is not supported.")
  }

  def hiveCreatePermanentFunctionsUnsupportedError(): Throwable = {
    new AnalysisException("Hive 0.12 doesn't support creating permanent functions. " +
      "Please use Hive 0.13 or higher.")
  }

  def unknownHiveResourceTypeError(resourceType: String): Throwable = {
    new AnalysisException(s"Unknown resource type: $resourceType")
  }

  def invalidDayTimeField(field: Byte): Throwable = {
    val supportedIds = DayTimeIntervalType.dayTimeFields
      .map(i => s"$i (${DayTimeIntervalType.fieldToString(i)})")
    new AnalysisException(s"Invalid field id '$field' in day-time interval. " +
      s"Supported interval fields: ${supportedIds.mkString(", ")}.")
  }

  def invalidDayTimeIntervalType(startFieldName: String, endFieldName: String): Throwable = {
    new AnalysisException(s"'interval $startFieldName to $endFieldName' is invalid.")
  }

  def invalidYearMonthField(field: Byte): Throwable = {
    val supportedIds = YearMonthIntervalType.yearMonthFields
      .map(i => s"$i (${YearMonthIntervalType.fieldToString(i)})")
    new AnalysisException(s"Invalid field id '$field' in year-month interval. " +
      s"Supported interval fields: ${supportedIds.mkString(", ")}.")
  }

  def invalidYearMonthIntervalType(startFieldName: String, endFieldName: String): Throwable = {
    new AnalysisException(s"'interval $startFieldName to $endFieldName' is invalid.")
  }

  def configRemovedInVersionError(
      configName: String,
      version: String,
      comment: String): Throwable = {
    new AnalysisException(
      s"The SQL config '$configName' was removed in the version $version. $comment")
  }

  def failedFallbackParsingError(msg: String, e1: Throwable, e2: Throwable): Throwable = {
    new AnalysisException(s"$msg${e1.getMessage}\nFailed fallback parsing: ${e2.getMessage}",
      cause = Some(e1.getCause))
  }

  def decimalCannotGreaterThanPrecisionError(scale: Int, precision: Int): Throwable = {
    new AnalysisException(s"Decimal scale ($scale) cannot be greater than precision ($precision).")
  }

  def decimalOnlySupportPrecisionUptoError(decimalType: String, precision: Int): Throwable = {
    new AnalysisException(s"$decimalType can only support precision up to $precision")
  }

  def negativeScaleNotAllowedError(scale: Int): Throwable = {
    new AnalysisException(
      s"""|Negative scale is not allowed: $scale.
         |You can use ${LEGACY_ALLOW_NEGATIVE_SCALE_OF_DECIMAL_ENABLED.key}=true
         |to enable legacy mode to allow it.""".stripMargin.replaceAll("\n", " "))
  }

  def invalidPartitionColumnKeyInTableError(key: String, tblName: String): Throwable = {
    new AnalysisException(s"$key is not a valid partition column in table $tblName.")
  }

  def invalidPartitionSpecError(
      specKeys: String,
      partitionColumnNames: Seq[String],
      tableName: String): Throwable = {
    new AnalysisException(
      s"""|Partition spec is invalid. The spec ($specKeys) must match
        |the partition spec (${partitionColumnNames.mkString(", ")}) defined in
        |table '$tableName'""".stripMargin.replaceAll("\n", " "))
  }

  def foundDuplicateColumnError(colType: String, duplicateCol: Seq[String]): Throwable = {
    new AnalysisException(
      s"Found duplicate column(s) $colType: ${duplicateCol.sorted.mkString(", ")}")
  }

  def noSuchTableError(db: String, table: String): Throwable = {
    new NoSuchTableException(db = db, table = table)
  }

  def tempViewNotCachedForAnalyzingColumnsError(tableIdent: TableIdentifier): Throwable = {
    new AnalysisException(s"Temporary view $tableIdent is not cached for analyzing columns.")
  }

  def columnTypeNotSupportStatisticsCollectionError(
      name: String,
      tableIdent: TableIdentifier,
      dataType: DataType): Throwable = {
    new AnalysisException(s"Column $name in table $tableIdent is of type $dataType, " +
      "and Spark does not support statistics collection on this column type.")
  }

  def analyzeTableNotSupportedOnViewsError(): Throwable = {
    new AnalysisException("ANALYZE TABLE is not supported on views.")
  }

  def unexpectedPartitionColumnPrefixError(
      table: String,
      database: String,
      schemaColumns: String,
      specColumns: String): Throwable = {
    new AnalysisException(
      s"""
         |The list of partition columns with values
         |in partition specification for table '${table}'
         |in database '${database}' is not a prefix of the list of
         |partition columns defined in the table schema.
         |Expected a prefix of [${schemaColumns}], but got [${specColumns}].
       """.stripMargin.replaceAll("\n", " "))
  }

  def noSuchPartitionError(
      db: String,
      table: String,
      partition: TablePartitionSpec): Throwable = {
    new NoSuchPartitionException(db, table, partition)
  }

  def analyzingColumnStatisticsNotSupportedForColumnTypeError(
      name: String,
      dataType: DataType): Throwable = {
    new AnalysisException("Analyzing column statistics is not supported for column " +
      s"$name of data type: $dataType.")
  }

  def tableAlreadyExistsError(table: String, guide: String = ""): Throwable = {
    new AnalysisException(s"Table $table already exists." + guide)
  }

  def createTableAsSelectWithNonEmptyDirectoryError(tablePath: String): Throwable = {
    new AnalysisException(
      s"CREATE-TABLE-AS-SELECT cannot create table with location to a non-empty directory " +
        s"${tablePath} . To allow overwriting the existing non-empty directory, " +
        s"set '${SQLConf.ALLOW_NON_EMPTY_LOCATION_IN_CTAS.key}' to true.")
  }

  def tableOrViewNotFoundError(table: String): Throwable = {
    new AnalysisException(s"Table or view not found: $table")
  }

  def noSuchFunctionError(
      rawName: Seq[String],
      t: TreeNode[_],
      fullName: Option[Seq[String]] = None): Throwable = {
    if (rawName.length == 1 && fullName.isDefined) {
      new AnalysisException(s"Undefined function: ${rawName.head}. " +
        "This function is neither a built-in/temporary function, nor a persistent " +
        s"function that is qualified as ${fullName.get.quoted}.",
        t.origin.line, t.origin.startPosition)
    } else {
      new AnalysisException(s"Undefined function: ${rawName.quoted}",
        t.origin.line, t.origin.startPosition)
    }
  }

  def unsetNonExistentPropertyError(property: String, table: TableIdentifier): Throwable = {
    new AnalysisException(s"Attempted to unset non-existent property '$property' in table '$table'")
  }

  def alterTableChangeColumnNotSupportedForColumnTypeError(
      originColumn: StructField,
      newColumn: StructField): Throwable = {
    new AnalysisException("ALTER TABLE CHANGE COLUMN is not supported for changing column " +
      s"'${originColumn.name}' with type '${originColumn.dataType}' to " +
      s"'${newColumn.name}' with type '${newColumn.dataType}'")
  }

  def cannotFindColumnError(name: String, fieldNames: Array[String]): Throwable = {
    new AnalysisException(s"Can't find column `$name` given table data columns " +
      s"${fieldNames.mkString("[`", "`, `", "`]")}")
  }

  def alterTableSetSerdeForSpecificPartitionNotSupportedError(): Throwable = {
    new AnalysisException("Operation not allowed: ALTER TABLE SET " +
      "[SERDE | SERDEPROPERTIES] for a specific partition is not supported " +
      "for tables created with the datasource API")
  }

  def alterTableSetSerdeNotSupportedError(): Throwable = {
    new AnalysisException("Operation not allowed: ALTER TABLE SET SERDE is " +
      "not supported for tables created with the datasource API")
  }

  def cmdOnlyWorksOnPartitionedTablesError(cmd: String, tableIdentWithDB: String): Throwable = {
    new AnalysisException(
      s"Operation not allowed: $cmd only works on partitioned tables: $tableIdentWithDB")
  }

  def cmdOnlyWorksOnTableWithLocationError(cmd: String, tableIdentWithDB: String): Throwable = {
    new AnalysisException(s"Operation not allowed: $cmd only works on table with " +
      s"location provided: $tableIdentWithDB")
  }

  def actionNotAllowedOnTableWithFilesourcePartitionManagementDisabledError(
      action: String,
      tableName: String): Throwable = {
    new AnalysisException(
      s"$action is not allowed on $tableName since filesource partition management is " +
        "disabled (spark.sql.hive.manageFilesourcePartitions = false).")
  }

  def actionNotAllowedOnTableSincePartitionMetadataNotStoredError(
     action: String,
     tableName: String): Throwable = {
    new AnalysisException(
      s"$action is not allowed on $tableName since its partition metadata is not stored in " +
        "the Hive metastore. To import this information into the metastore, run " +
        s"`msck repair table $tableName`")
  }

  def cannotAlterViewWithAlterTableError(): Throwable = {
    new AnalysisException(
      "Cannot alter a view with ALTER TABLE. Please use ALTER VIEW instead")
  }

  def cannotAlterTableWithAlterViewError(): Throwable = {
    new AnalysisException(
      "Cannot alter a table with ALTER VIEW. Please use ALTER TABLE instead")
  }

  def cannotOverwritePathBeingReadFromError(): Throwable = {
    new AnalysisException("Cannot overwrite a path that is also being read from.")
  }

  def cannotDropBuiltinFuncError(functionName: String): Throwable = {
    new AnalysisException(s"Cannot drop built-in function '$functionName'")
  }

  def cannotRefreshBuiltInFuncError(functionName: String): Throwable = {
    new AnalysisException(s"Cannot refresh built-in function $functionName")
  }

  def cannotRefreshTempFuncError(functionName: String): Throwable = {
    new AnalysisException(s"Cannot refresh temporary function $functionName")
  }

  def noSuchFunctionError(identifier: FunctionIdentifier): Throwable = {
    new NoSuchFunctionException(identifier.database.get, identifier.funcName)
  }

  def alterAddColNotSupportViewError(table: TableIdentifier): Throwable = {
    new AnalysisException(
      s"""
         |ALTER ADD COLUMNS does not support views.
         |You must drop and re-create the views for adding the new columns. Views: $table
       """.stripMargin)
  }

  def alterAddColNotSupportDatasourceTableError(
      tableType: Any,
      table: TableIdentifier): Throwable = {
    new AnalysisException(
      s"""
         |ALTER ADD COLUMNS does not support datasource table with type $tableType.
         |You must drop and re-create the table for adding the new columns. Tables: $table
       """.stripMargin)
  }

  def loadDataNotSupportedForDatasourceTablesError(tableIdentWithDB: String): Throwable = {
    new AnalysisException(s"LOAD DATA is not supported for datasource tables: $tableIdentWithDB")
  }

  def loadDataWithoutPartitionSpecProvidedError(tableIdentWithDB: String): Throwable = {
    new AnalysisException(s"LOAD DATA target table $tableIdentWithDB is partitioned, " +
      s"but no partition spec is provided")
  }

  def loadDataPartitionSizeNotMatchNumPartitionColumnsError(
      tableIdentWithDB: String,
      partitionSize: Int,
      targetTableSize: Int): Throwable = {
    new AnalysisException(
      s"""
         |LOAD DATA target table $tableIdentWithDB is partitioned,
         |but number of columns in provided partition spec ($partitionSize)
         |do not match number of partitioned columns in table ($targetTableSize)
       """.stripMargin.replaceAll("\n", " "))
  }

  def loadDataTargetTableNotPartitionedButPartitionSpecWasProvidedError(
      tableIdentWithDB: String): Throwable = {
    new AnalysisException(s"LOAD DATA target table $tableIdentWithDB is not " +
      s"partitioned, but a partition spec was provided.")
  }

  def loadDataInputPathNotExistError(path: String): Throwable = {
    new AnalysisException(s"LOAD DATA input path does not exist: $path")
  }

  def truncateTableOnExternalTablesError(tableIdentWithDB: String): Throwable = {
    new AnalysisException(
      s"Operation not allowed: TRUNCATE TABLE on external tables: $tableIdentWithDB")
  }

  def truncateTablePartitionNotSupportedForNotPartitionedTablesError(
      tableIdentWithDB: String): Throwable = {
    new AnalysisException(s"Operation not allowed: TRUNCATE TABLE ... PARTITION is not supported" +
      s" for tables that are not partitioned: $tableIdentWithDB")
  }

  def failToTruncateTableWhenRemovingDataError(
      tableIdentWithDB: String,
      path: Path,
      e: Throwable): Throwable = {
    new AnalysisException(s"Failed to truncate table $tableIdentWithDB when " +
        s"removing data of the path: $path because of ${e.toString}")
  }

  def descPartitionNotAllowedOnTempView(table: String): Throwable = {
    new AnalysisException(s"DESC PARTITION is not allowed on a temporary view: $table")
  }

  def descPartitionNotAllowedOnView(table: String): Throwable = {
    new AnalysisException(s"DESC PARTITION is not allowed on a view: $table")
  }

  def showPartitionNotAllowedOnTableNotPartitionedError(tableIdentWithDB: String): Throwable = {
    new AnalysisException(
      s"SHOW PARTITIONS is not allowed on a table that is not partitioned: $tableIdentWithDB")
  }

  def showCreateTableNotSupportedOnTempView(table: String): Throwable = {
    new AnalysisException(s"SHOW CREATE TABLE is not supported on a temporary view: $table")
  }

  def showCreateTableFailToExecuteUnsupportedFeatureError(table: CatalogTable): Throwable = {
    new AnalysisException("Failed to execute SHOW CREATE TABLE against table " +
      s"${table.identifier}, which is created by Hive and uses the " +
      s"following unsupported feature(s)\n" +
      table.unsupportedFeatures.map(" - " + _).mkString("\n") + ". " +
      s"Please use `SHOW CREATE TABLE ${table.identifier} AS SERDE` to show Hive DDL instead.")
  }

  def showCreateTableNotSupportTransactionalHiveTableError(table: CatalogTable): Throwable = {
    new AnalysisException("SHOW CREATE TABLE doesn't support transactional Hive table. " +
      s"Please use `SHOW CREATE TABLE ${table.identifier} AS SERDE` " +
      "to show Hive DDL instead.")
  }

  def showCreateTableFailToExecuteUnsupportedConfError(
      table: TableIdentifier,
      builder: mutable.StringBuilder): Throwable = {
    new AnalysisException("Failed to execute SHOW CREATE TABLE against table " +
        s"${table.identifier}, which is created by Hive and uses the " +
        "following unsupported serde configuration\n" +
        builder.toString()
    )
  }

  def descPartitionNotAllowedOnViewError(table: String): Throwable = {
    new AnalysisException(s"DESC PARTITION is not allowed on a view: $table")
  }

  def showCreateTableAsSerdeNotAllowedOnSparkDataSourceTableError(
      table: TableIdentifier): Throwable = {
    new AnalysisException(
      s"$table is a Spark data source table. Use `SHOW CREATE TABLE` without `AS SERDE` instead.")
  }

  def showCreateTableOrViewFailToExecuteUnsupportedFeatureError(
      table: CatalogTable,
      features: Seq[String]): Throwable = {
    new AnalysisException(
      s"Failed to execute SHOW CREATE TABLE against table/view ${table.identifier}, " +
        "which is created by Hive and uses the following unsupported feature(s)\n" +
        features.map(" - " + _).mkString("\n"))
  }

  def logicalPlanForViewNotAnalyzedError(): Throwable = {
    new AnalysisException("The logical plan that represents the view is not analyzed.")
  }

  def createViewNumColumnsMismatchUserSpecifiedColumnLengthError(
      analyzedPlanLength: Int,
      userSpecifiedColumnsLength: Int): Throwable = {
    new AnalysisException(s"The number of columns produced by the SELECT clause " +
      s"(num: `$analyzedPlanLength`) does not match the number of column names " +
      s"specified by CREATE VIEW (num: `$userSpecifiedColumnsLength`).")
  }

  def tableIsNotViewError(name: TableIdentifier): Throwable = {
    new AnalysisException(s"$name is not a view")
  }

  def viewAlreadyExistsError(name: TableIdentifier): Throwable = {
    new AnalysisException(
      s"View $name already exists. If you want to update the view definition, " +
        "please use ALTER VIEW AS or CREATE OR REPLACE VIEW AS")
  }

  def createPersistedViewFromDatasetAPINotAllowedError(): Throwable = {
    new AnalysisException("It is not allowed to create a persisted view from the Dataset API")
  }

  def recursiveViewDetectedError(
      viewIdent: TableIdentifier,
      newPath: Seq[TableIdentifier]): Throwable = {
    new AnalysisException(s"Recursive view $viewIdent detected " +
      s"(cycle: ${newPath.mkString(" -> ")})")
  }

  def notAllowedToCreatePermanentViewWithoutAssigningAliasForExpressionError(
      name: TableIdentifier,
      attrName: String): Throwable = {
    new AnalysisException(s"Not allowed to create a permanent view $name without " +
      s"explicitly assigning an alias for expression $attrName")
  }

  def notAllowedToCreatePermanentViewByReferencingTempViewError(
      name: TableIdentifier,
      nameParts: String): Throwable = {
    new AnalysisException(s"Not allowed to create a permanent view $name by " +
      s"referencing a temporary view $nameParts. " +
      "Please create a temp view instead by CREATE TEMP VIEW")
  }

  def notAllowedToCreatePermanentViewByReferencingTempFuncError(
      name: TableIdentifier,
      funcName: String): Throwable = {
    new AnalysisException(s"Not allowed to create a permanent view $name by " +
      s"referencing a temporary function `$funcName`")
  }

  def queryFromRawFilesIncludeCorruptRecordColumnError(): Throwable = {
    new AnalysisException(
      """
        |Since Spark 2.3, the queries from raw JSON/CSV files are disallowed when the
        |referenced columns only include the internal corrupt record column
        |(named _corrupt_record by default). For example:
        |spark.read.schema(schema).csv(file).filter($"_corrupt_record".isNotNull).count()
        |and spark.read.schema(schema).csv(file).select("_corrupt_record").show().
        |Instead, you can cache or save the parsed results and then send the same query.
        |For example, val df = spark.read.schema(schema).csv(file).cache() and then
        |df.filter($"_corrupt_record".isNotNull).count().
      """.stripMargin)
  }

  def userDefinedPartitionNotFoundInJDBCRelationError(
      columnName: String, schema: String): Throwable = {
    new AnalysisException(s"User-defined partition column $columnName not " +
      s"found in the JDBC relation: $schema")
  }

  def invalidPartitionColumnTypeError(column: StructField): Throwable = {
    new AnalysisException(
      s"""
         |Partition column type should be ${NumericType.simpleString},
         |${DateType.catalogString}, or ${TimestampType.catalogString}, but
         |${column.dataType.catalogString} found.
       """.stripMargin.replaceAll("\n", " "))
  }

  def tableOrViewAlreadyExistsError(name: String): Throwable = {
    new AnalysisException(
      s"Table or view '$name' already exists. SaveMode: ErrorIfExists.")
  }

  def columnNameContainsInvalidCharactersError(name: String): Throwable = {
    new AnalysisException(
      s"""
         |Column name "$name" contains invalid character(s).
         |Please use alias to rename it.
       """.stripMargin.replaceAll("\n", " "))
  }

  def textDataSourceWithMultiColumnsError(schema: StructType): Throwable = {
    new AnalysisException(
      s"Text data source supports only a single column, and you have ${schema.size} columns.")
  }

  def cannotFindPartitionColumnInPartitionSchemaError(
      readField: StructField, partitionSchema: StructType): Throwable = {
    new AnalysisException(s"Can't find required partition column ${readField.name} " +
      s"in partition schema $partitionSchema")
  }

  def cannotSpecifyDatabaseForTempViewError(tableIdent: TableIdentifier): Throwable = {
    new AnalysisException(
      s"Temporary view '$tableIdent' should not have specified a database")
  }

  def cannotCreateTempViewUsingHiveDataSourceError(): Throwable = {
    new AnalysisException("Hive data source can only be used with tables, " +
      "you can't use it with CREATE TEMP VIEW USING")
  }

  def invalidTimestampProvidedForStrategyError(
      strategy: String, timeString: String): Throwable = {
    new AnalysisException(
      s"The timestamp provided for the '$strategy' option is invalid. The expected format " +
        s"is 'YYYY-MM-DDTHH:mm:ss', but the provided timestamp: $timeString")
  }

  def hostOptionNotSetError(): Throwable = {
    new AnalysisException("Set a host to read from with option(\"host\", ...).")
  }

  def portOptionNotSetError(): Throwable = {
    new AnalysisException("Set a port to read from with option(\"port\", ...).")
  }

  def invalidIncludeTimestampValueError(): Throwable = {
    new AnalysisException("includeTimestamp must be set to either \"true\" or \"false\"")
  }

  def checkpointLocationNotSpecifiedError(): Throwable = {
    new AnalysisException(
      s"""
         |checkpointLocation must be specified either
         |through option("checkpointLocation", ...) or
         |SparkSession.conf.set("${SQLConf.CHECKPOINT_LOCATION.key}", ...)
       """.stripMargin.replaceAll("\n", " "))
  }

  def recoverQueryFromCheckpointUnsupportedError(checkpointPath: Path): Throwable = {
    new AnalysisException("This query does not support recovering from checkpoint location. " +
      s"Delete $checkpointPath to start over.")
  }

  def cannotFindColumnInRelationOutputError(
      colName: String, relation: LogicalPlan): Throwable = {
    new AnalysisException(s"Unable to find the column `$colName` " +
      s"given [${relation.output.map(_.name).mkString(", ")}]")
  }

  def invalidBoundaryStartError(start: Long): Throwable = {
    new AnalysisException(s"Boundary start is not a valid integer: $start")
  }

  def invalidBoundaryEndError(end: Long): Throwable = {
    new AnalysisException(s"Boundary end is not a valid integer: $end")
  }

  def databaseDoesNotExistError(dbName: String): Throwable = {
    new AnalysisException(s"Database '$dbName' does not exist.")
  }

  def tableDoesNotExistInDatabaseError(tableName: String, dbName: String): Throwable = {
    new AnalysisException(s"Table '$tableName' does not exist in database '$dbName'.")
  }

  def tableOrViewNotFoundInDatabaseError(tableName: String, dbName: String): Throwable = {
    new AnalysisException(s"Table or view '$tableName' not found in database '$dbName'")
  }

  def unexpectedTypeOfRelationError(relation: LogicalPlan, tableName: String): Throwable = {
    new AnalysisException(
      s"Unexpected type ${relation.getClass.getCanonicalName} of the relation $tableName")
  }

  def unsupportedTableChangeInJDBCCatalogError(change: TableChange): Throwable = {
    new AnalysisException(s"Unsupported TableChange $change in JDBC catalog.")
  }

  def pathOptionNotSetCorrectlyWhenReadingError(): Throwable = {
    new AnalysisException(
      s"""
         |There is a 'path' or 'paths' option set and load() is called
         |with path parameters. Either remove the path option if it's the same as the path
         |parameter, or add it to the load() parameter if you do want to read multiple paths.
         |To ignore this check, set '${SQLConf.LEGACY_PATH_OPTION_BEHAVIOR.key}' to 'true'.
       """.stripMargin.replaceAll("\n", " "))
  }

  def pathOptionNotSetCorrectlyWhenWritingError(): Throwable = {
    new AnalysisException(
      s"""
         |There is a 'path' option set and save() is called with a path
         |parameter. Either remove the path option, or call save() without the parameter.
         |To ignore this check, set '${SQLConf.LEGACY_PATH_OPTION_BEHAVIOR.key}' to 'true'.
       """.stripMargin.replaceAll("\n", " "))
  }

  def writeWithSaveModeUnsupportedBySourceError(source: String, createMode: String): Throwable = {
    new AnalysisException(s"TableProvider implementation $source cannot be " +
      s"written with $createMode mode, please use Append or Overwrite modes instead.")
  }

  def partitionByDoesNotAllowedWhenUsingInsertIntoError(): Throwable = {
    new AnalysisException(
      """
        |insertInto() can't be used together with partitionBy().
        |Partition columns have already been defined for the table.
        |It is not necessary to use partitionBy().
      """.stripMargin.replaceAll("\n", " "))
  }

  def cannotFindCatalogToHandleIdentifierError(quote: String): Throwable = {
    new AnalysisException(s"Couldn't find a catalog to handle the identifier $quote.")
  }

  def sortByNotUsedWithBucketByError(): Throwable = {
    new AnalysisException("sortBy must be used together with bucketBy")
  }

  def bucketByUnsupportedByOperationError(operation: String): Throwable = {
    new AnalysisException(s"'$operation' does not support bucketBy right now")
  }

  def bucketByAndSortByUnsupportedByOperationError(operation: String): Throwable = {
    new AnalysisException(s"'$operation' does not support bucketBy and sortBy right now")
  }

  def tableAlreadyExistsError(tableIdent: TableIdentifier): Throwable = {
    new AnalysisException(s"Table $tableIdent already exists.")
  }

  def cannotOverwriteTableThatIsBeingReadFromError(tableName: String): Throwable = {
    new AnalysisException(s"Cannot overwrite table $tableName that is also being read from")
  }

  def invalidPartitionTransformationError(expr: Expression): Throwable = {
    new AnalysisException(s"Invalid partition transformation: ${expr.sql}")
  }

  def cannotResolveColumnNameAmongFieldsError(
      colName: String, fieldsStr: String, extraMsg: String): AnalysisException = {
    new AnalysisException(
      s"""Cannot resolve column name "$colName" among (${fieldsStr})${extraMsg}""")
  }

  def cannotParseIntervalError(delayThreshold: String, e: Throwable): Throwable = {
    new AnalysisException(s"Unable to parse '$delayThreshold'", cause = Some(e))
  }

  def invalidJoinTypeInJoinWithError(joinType: JoinType): Throwable = {
    new AnalysisException(s"Invalid join type in joinWith: ${joinType.sql}")
  }

  def cannotPassTypedColumnInUntypedSelectError(typedCol: String): Throwable = {
    new AnalysisException(s"Typed column $typedCol that needs input type and schema " +
      "cannot be passed in untyped `select` API. Use the typed `Dataset.select` API instead.")
  }

  def invalidViewNameError(viewName: String): Throwable = {
    new AnalysisException(s"Invalid view name: $viewName")
  }

  def invalidBucketsNumberError(numBuckets: String, e: String): Throwable = {
    new AnalysisException(s"Invalid number of buckets: bucket($numBuckets, $e)")
  }

  def usingUntypedScalaUDFError(): Throwable = {
    new AnalysisException("You're using untyped Scala UDF, which does not have the input type " +
      "information. Spark may blindly pass null to the Scala closure with primitive-type " +
      "argument, and the closure will see the default value of the Java type for the null " +
      "argument, e.g. `udf((x: Int) => x, IntegerType)`, the result is 0 for null input. " +
      "To get rid of this error, you could:\n" +
      "1. use typed Scala UDF APIs(without return type parameter), e.g. `udf((x: Int) => x)`\n" +
      "2. use Java UDF APIs, e.g. `udf(new UDF1[String, Integer] { " +
      "override def call(s: String): Integer = s.length() }, IntegerType)`, " +
      "if input types are all non primitive\n" +
      s"3. set ${SQLConf.LEGACY_ALLOW_UNTYPED_SCALA_UDF.key} to true and " +
      s"use this API with caution")
  }

  def aggregationFunctionAppliedOnNonNumericColumnError(colName: String): Throwable = {
    new AnalysisException(s""""$colName" is not a numeric column. """ +
      "Aggregation function can only be applied on a numeric column.")
  }

  def aggregationFunctionAppliedOnNonNumericColumnError(
      pivotColumn: String, maxValues: Int): Throwable = {
    new AnalysisException(
      s"""
         |The pivot column $pivotColumn has more than $maxValues distinct values,
         |this could indicate an error.
         |If this was intended, set ${SQLConf.DATAFRAME_PIVOT_MAX_VALUES.key}
         |to at least the number of distinct values of the pivot column.
       """.stripMargin.replaceAll("\n", " "))
  }

  def cannotModifyValueOfStaticConfigError(key: String): Throwable = {
    new AnalysisException(s"Cannot modify the value of a static config: $key")
  }

  def cannotModifyValueOfSparkConfigError(key: String): Throwable = {
    new AnalysisException(
      s"""
         |Cannot modify the value of a Spark config: $key.
         |See also 'https://spark.apache.org/docs/latest/sql-migration-guide.html#ddl-statements'
       """.stripMargin.replaceAll("\n", " "))
  }

  def commandExecutionInRunnerUnsupportedError(runner: String): Throwable = {
    new AnalysisException(s"Command execution is not supported in runner $runner")
  }

  def udfClassDoesNotImplementAnyUDFInterfaceError(className: String): Throwable = {
    new AnalysisException(s"UDF class $className doesn't implement any UDF interface")
  }

  def udfClassNotAllowedToImplementMultiUDFInterfacesError(className: String): Throwable = {
    new AnalysisException(
      s"It is invalid to implement multiple UDF interfaces, UDF class $className")
  }

  def udfClassWithTooManyTypeArgumentsError(n: Int): Throwable = {
    new AnalysisException(s"UDF class with $n type arguments is not supported.")
  }

  def classWithoutPublicNonArgumentConstructorError(className: String): Throwable = {
    new AnalysisException(s"Can not instantiate class $className, please make sure" +
      " it has public non argument constructor")
  }

  def cannotLoadClassNotOnClassPathError(className: String): Throwable = {
    new AnalysisException(s"Can not load class $className, please make sure it is on the classpath")
  }

  def classDoesNotImplementUserDefinedAggregateFunctionError(className: String): Throwable = {
    new AnalysisException(
      s"class $className doesn't implement interface UserDefinedAggregateFunction")
  }

  def missingFieldError(
      fieldName: Seq[String], table: ResolvedTable, context: Origin): Throwable = {
    throw new AnalysisException(
      s"Missing field ${fieldName.quoted} in table ${table.name} with schema:\n" +
        table.schema.treeString,
      context.line,
      context.startPosition)
  }

  def invalidFieldName(fieldName: Seq[String], path: Seq[String], context: Origin): Throwable = {
    new AnalysisException(
      errorClass = "INVALID_FIELD_NAME",
      messageParameters = Array(fieldName.quoted, path.quoted),
      origin = context)
  }

  def invalidJsonSchema(schema: DataType): Throwable = {
    new AnalysisException(
      errorClass = "INVALID_JSON_SCHEMA_MAPTYPE",
      messageParameters = Array(schema.toString))
  }

  def tableIndexNotSupportedError(errorMessage: String): Throwable = {
    new AnalysisException(errorMessage)
  }

  def invalidViewText(viewText: String, tableName: String): Throwable = {
    new AnalysisException(
      s"Invalid view text: $viewText. The view $tableName may have been tampered with")
  }

  def invalidTimeTravelSpecError(): Throwable = {
    new AnalysisException(
      "Cannot specify both version and timestamp when time travelling the table.")
  }

  def invalidTimestampExprForTimeTravel(expr: Expression): Throwable = {
    new AnalysisException(s"${expr.sql} is not a valid timestamp expression for time travel.")
  }

  def timeTravelUnsupportedError(target: String): Throwable = {
    new AnalysisException(s"Cannot time travel $target.")
  }

  def tableNotSupportTimeTravelError(tableName: Identifier): UnsupportedOperationException = {
    new UnsupportedOperationException(s"Table $tableName does not support time travel.")
  }

  def writeDistributionAndOrderingNotSupportedInContinuousExecution(): Throwable = {
    new AnalysisException(
      "Sinks cannot request distribution and ordering in continuous execution mode")
  }
}
