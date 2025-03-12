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

package org.apache.spark.sql.execution.command

import org.apache.spark.SparkException
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.{Analyzer, SQLFunctionNode, UnresolvedAlias, UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation}
import org.apache.spark.sql.catalyst.catalog.{SessionCatalog, SQLFunction, UserDefinedFunctionErrors}
import org.apache.spark.sql.catalyst.expressions.{Alias, Cast, Generator, LateralSubquery, Literal, ScalarSubquery, SubqueryExpression, WindowExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{LateralJoin, LogicalPlan, OneRowRelation, Project, UnresolvedWith}
import org.apache.spark.sql.catalyst.trees.TreePattern.UNRESOLVED_ATTRIBUTE
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.command.CreateUserDefinedFunctionCommand._
import org.apache.spark.sql.types.{DataType, StructField, StructType}

/**
 * The DDL command that creates a SQL function.
 * For example:
 * {{{
 *    CREATE [OR REPLACE] [TEMPORARY] FUNCTION [IF NOT EXISTS] [db_name.]function_name
 *    ([param_name param_type [COMMENT param_comment], ...])
 *    RETURNS {ret_type | TABLE (ret_name ret_type [COMMENT ret_comment], ...])}
 *    [function_properties] function_body;
 *
 *    function_properties:
 *      [NOT] DETERMINISTIC | COMMENT function_comment | [ CONTAINS SQL | READS SQL DATA ]
 *
 *    function_body:
 *      RETURN {expression | TABLE ( query )}
 * }}}
 */
case class CreateSQLFunctionCommand(
    name: FunctionIdentifier,
    inputParamText: Option[String],
    returnTypeText: String,
    exprText: Option[String],
    queryText: Option[String],
    comment: Option[String],
    isDeterministic: Option[Boolean],
    containsSQL: Option[Boolean],
    isTableFunc: Boolean,
    isTemp: Boolean,
    ignoreIfExists: Boolean,
    replace: Boolean)
    extends CreateUserDefinedFunctionCommand {

  import SQLFunction._

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val parser = sparkSession.sessionState.sqlParser
    val analyzer = sparkSession.sessionState.analyzer
    val catalog = sparkSession.sessionState.catalog
    val conf = sparkSession.sessionState.conf

    val inputParam = inputParamText.map(parser.parseTableSchema)
    val returnType = parseReturnTypeText(returnTypeText, isTableFunc, parser)

    val function = SQLFunction(
      name,
      inputParam,
      returnType.getOrElse(if (isTableFunc) Right(null) else Left(null)),
      exprText,
      queryText,
      comment,
      isDeterministic,
      containsSQL,
      isTableFunc,
      Map.empty)

    val newFunction = {
      val (expression, query) = function.getExpressionAndQuery(parser, isTableFunc)
      assert(query.nonEmpty || expression.nonEmpty)

      // Check if the function can be replaced.
      if (replace && catalog.functionExists(name)) {
        checkFunctionSignatures(catalog, name)
      }

      // Build function input.
      val inputPlan = if (inputParam.isDefined) {
        val param = inputParam.get
        checkParameterNotNull(param, inputParamText.get)
        checkParameterNameDuplication(param, conf, name)
        checkDefaultsTrailing(param, name)

        // Qualify the input parameters with the function name so that attributes referencing
        // the function input parameters can be resolved correctly.
        val qualifier = Seq(name.funcName)
        val input = param.map(p => Alias(
          {
            val defaultExpr = p.getDefault()
            if (defaultExpr.isEmpty) {
              Literal.create(null, p.dataType)
            } else {
              val defaultPlan = parseDefault(defaultExpr.get, parser)
              if (SubqueryExpression.hasSubquery(defaultPlan)) {
                throw new AnalysisException(
                  errorClass = "USER_DEFINED_FUNCTIONS.NOT_A_VALID_DEFAULT_EXPRESSION",
                  messageParameters =
                    Map("functionName" -> name.funcName, "parameterName" -> p.name))
              } else if (defaultPlan.containsPattern(UNRESOLVED_ATTRIBUTE)) {
                // TODO(SPARK-50698): use parsed expression instead of expression string.
                defaultPlan.collect {
                  case a: UnresolvedAttribute =>
                    throw QueryCompilationErrors.unresolvedAttributeError(
                      "UNRESOLVED_COLUMN", a.sql, Seq.empty, a.origin)
                }
              }
              Cast(defaultPlan, p.dataType)
            }
          }, p.name)(qualifier = qualifier))
        Project(input, OneRowRelation())
      } else {
        OneRowRelation()
      }

      // Build the function body and check if the function body can be analyzed successfully.
      val (unresolvedPlan, analyzedPlan, inferredReturnType) = if (!isTableFunc) {
        // Build SQL scalar function plan.
        val outputExpr = if (query.isDefined) ScalarSubquery(query.get) else expression.get
        val plan: LogicalPlan = returnType.map { t =>
          val retType: DataType = t match {
            case Left(t) => t
            case _ => throw SparkException.internalError(
              "Unexpected return type for a scalar SQL UDF.")
          }
          val outputCast = Seq(Alias(Cast(outputExpr, retType), name.funcName)())
          Project(outputCast, inputPlan)
        }.getOrElse {
          // If no explicit RETURNS clause is present, infer the result type from the function body.
          val outputAlias = Seq(Alias(outputExpr, name.funcName)())
          Project(outputAlias, inputPlan)
        }

        // Check the function body can be analyzed correctly.
        val analyzed = analyzer.execute(plan)
        val (resolved, resolvedReturnType) = analyzed match {
          case p @ Project(expr :: Nil, _) if expr.resolved =>
            (p, Left(expr.dataType))
          case other =>
            (other, function.returnType)
        }

        // Check if the SQL function body contains aggregate/window functions.
        // This check needs to be performed before checkAnalysis to provide better error messages.
        checkAggOrWindowOrGeneratorExpr(resolved)

        // Check if the SQL function body can be analyzed.
        checkFunctionBodyAnalysis(analyzer, function, resolved)

        (plan, resolved, resolvedReturnType)
      } else {
        // Build SQL table function plan.
        if (query.isEmpty) {
          throw UserDefinedFunctionErrors.bodyIsNotAQueryForSqlTableUdf(name.funcName)
        }

        // Construct a lateral join to analyze the function body.
        val plan = LateralJoin(inputPlan, LateralSubquery(query.get), Inner, None)
        val analyzed = analyzer.execute(plan)
        val newPlan = analyzed match {
          case Project(_, j: LateralJoin) => j
          case j: LateralJoin => j
          case _ => throw SparkException.internalError("Unexpected plan returned when " +
            s"creating a SQL TVF: ${analyzed.getClass.getSimpleName}.")
        }
        val maybeResolved = newPlan.asInstanceOf[LateralJoin].right.plan

        // Check if the function body can be analyzed.
        checkFunctionBodyAnalysis(analyzer, function, maybeResolved)

        // Get the function's return schema.
        val returnParam: StructType = returnType.map {
          case Right(t) => t
          case Left(_) => throw SparkException.internalError(
            "Unexpected return schema for a SQL table function.")
        }.getOrElse {
          // If no explicit RETURNS clause is present, infer the result type from the function body.
          // To detect this, we search for instances of the UnresolvedAlias expression. Examples:
          // CREATE TABLE t USING PARQUET AS VALUES (0, 1), (1, 2) AS tab(c1, c2);
          // SELECT c1 FROM t           -->  UnresolvedAttribute: 'c1
          // SELECT c1 + 1 FROM t       -->  UnresolvedAlias: unresolvedalias(('c1 + 1), None)
          // SELECT c1 + 1 AS a FROM t  -->  Alias: ('c1 + 1) AS a#2
          query.get match {
            case Project(projectList, _) if projectList.exists(_.isInstanceOf[UnresolvedAlias]) =>
              throw UserDefinedFunctionErrors.missingColumnNamesForSqlTableUdf(name.funcName)
            case _ =>
              StructType(analyzed.asInstanceOf[LateralJoin].right.plan.output.map { col =>
                StructField(col.name, col.dataType)
              })
          }
        }

        // Check the return columns cannot have NOT NULL specified.
        checkParameterNotNull(returnParam, returnTypeText)

        // Check duplicated return column names.
        checkReturnsColumnDuplication(returnParam, conf, name)

        // Check if the actual output size equals to the number of return parameters.
        val outputSize = maybeResolved.output.size
        if (outputSize != returnParam.size) {
          throw new AnalysisException(
            errorClass = "USER_DEFINED_FUNCTIONS.RETURN_COLUMN_COUNT_MISMATCH",
            messageParameters = Map(
              "outputSize" -> s"$outputSize",
              "returnParamSize" -> s"${returnParam.size}",
              "name" -> s"$name"
            )
          )
        }

        (plan, analyzed, Right(returnParam))
      }

      // A permanent function is not allowed to reference temporary objects.
      // This should be called after `qe.assertAnalyzed()` (i.e., `plan` can be resolved)
      verifyTemporaryObjectsNotExists(catalog, isTemp, name, unresolvedPlan, analyzedPlan)

      // Generate function properties.
      val properties = generateFunctionProperties(sparkSession, unresolvedPlan, analyzedPlan)

      // Derive determinism of the SQL function.
      val deterministic = analyzedPlan.deterministic

      function.copy(
        // Assign the return type, inferring from the function body if needed.
        returnType = inferredReturnType,
        deterministic = Some(function.deterministic.getOrElse(deterministic)),
        properties = properties
      )
    }

    if (isTemp) {
      if (isTableFunc) {
        catalog.registerSQLTableFunction(newFunction, overrideIfExists = replace)
      } else {
        catalog.registerSQLScalarFunction(newFunction, overrideIfExists = replace)
      }
    } else {
      if (replace && catalog.functionExists(name)) {
        // Hive metastore alter function method does not alter function resources
        // so the existing function must be dropped first when replacing a SQL function.
        assert(!ignoreIfExists)
        catalog.dropFunction(name, ignoreIfExists)
      }
      // For a persistent function, we will store the metadata into underlying external catalog.
      // This function will be loaded into the FunctionRegistry when a query uses it.
      // We do not load it into FunctionRegistry right now, to avoid loading the resource
      // immediately, as the Spark application to create the function may not have
      // access to the function.
      catalog.createUserDefinedFunction(newFunction, ignoreIfExists)
    }

    Seq.empty
  }

  /**
   * Check if the function body can be analyzed.
   */
  private def checkFunctionBodyAnalysis(
      analyzer: Analyzer,
      function: SQLFunction,
      body: LogicalPlan): Unit = {
    analyzer.checkAnalysis(SQLFunctionNode(function, body))
  }

  /** Check whether the new function is replacing an existing SQL function. */
  private def checkFunctionSignatures(catalog: SessionCatalog, name: FunctionIdentifier): Unit = {
    val info = catalog.lookupFunctionInfo(name)
    if (!isSQLFunction(info.getClassName)) {
      throw new AnalysisException(
        errorClass = "USER_DEFINED_FUNCTIONS.CANNOT_REPLACE_NON_SQL_UDF_WITH_SQL_UDF",
        messageParameters = Map("name" -> s"$name")
      )
    }
  }

  /**
   * Collect all temporary views and functions and return the identifiers separately
   * This func traverses the unresolved plan `child`. Below are the reasons:
   * 1) Analyzer replaces unresolved temporary views by a SubqueryAlias with the corresponding
   * logical plan. After replacement, it is impossible to detect whether the SubqueryAlias is
   * added/generated from a temporary view.
   * 2) The temp functions are represented by multiple classes. Most are inaccessible from this
   * package (e.g., HiveGenericUDF).
   * 3) Temporary SQL functions, once resolved, cannot be identified as temp functions.
   */
  private def collectTemporaryObjectsInUnresolvedPlan(
      catalog: SessionCatalog,
      child: LogicalPlan): (Seq[Seq[String]], Seq[String]) = {
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
    def collectTempViews(child: LogicalPlan): Seq[Seq[String]] = {
      child.flatMap {
        case UnresolvedRelation(nameParts, _, _) if catalog.isTempView(nameParts) =>
          Seq(nameParts)
        case w: UnresolvedWith if !w.resolved => w.innerChildren.flatMap(collectTempViews)
        case plan if !plan.resolved => plan.expressions.flatMap(_.flatMap {
          case e: SubqueryExpression => collectTempViews(e.plan)
          case _ => Seq.empty
        })
        case _ => Seq.empty
      }.distinct
    }

    def collectTempFunctions(child: LogicalPlan): Seq[String] = {
      child.flatMap {
        case w: UnresolvedWith if !w.resolved => w.innerChildren.flatMap(collectTempFunctions)
        case plan if !plan.resolved =>
          plan.expressions.flatMap(_.flatMap {
            case e: SubqueryExpression => collectTempFunctions(e.plan)
            case e: UnresolvedFunction
              if catalog.isTemporaryFunction(e.nameParts.asFunctionIdentifier) =>
              Seq(e.nameParts.asFunctionIdentifier.funcName)
            case _ => Seq.empty
          })
        case _ => Seq.empty
      }.distinct
    }
    (collectTempViews(child), collectTempFunctions(child))
  }

  /**
   * Permanent functions are not allowed to reference temp objects, including temp functions
   * and temp views.
   */
  private def verifyTemporaryObjectsNotExists(
      catalog: SessionCatalog,
      isTemporary: Boolean,
      name: FunctionIdentifier,
      child: LogicalPlan,
      analyzed: LogicalPlan): Unit = {
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
    if (!isTemporary) {
      val (tempViews, tempFunctions) = collectTemporaryObjectsInUnresolvedPlan(catalog, child)
      tempViews.foreach { nameParts =>
        throw UserDefinedFunctionErrors.invalidTempViewReference(
          routineName = name.asMultipart, tempViewName = nameParts)
      }
      tempFunctions.foreach { funcName =>
        throw UserDefinedFunctionErrors.invalidTempFuncReference(
          routineName = name.asMultipart, tempFuncName = funcName)
      }
      val tempVars = ViewHelper.collectTemporaryVariables(analyzed)
      tempVars.foreach { varName =>
        throw UserDefinedFunctionErrors.invalidTempVarReference(
          routineName = name.asMultipart, varName = varName)
      }
    }
  }

  /**
   * Check if the SQL function body contains aggregate/window/generate functions.
   * Note subqueries inside the SQL function body can contain aggregate/window/generate functions.
   */
  private def checkAggOrWindowOrGeneratorExpr(plan: LogicalPlan): Unit = {
    if (plan.resolved) {
      plan.transformAllExpressions {
        case e if e.isInstanceOf[WindowExpression] || e.isInstanceOf[Generator] ||
          e.isInstanceOf[AggregateExpression] =>
          throw new AnalysisException(
            errorClass = "USER_DEFINED_FUNCTIONS.CANNOT_CONTAIN_COMPLEX_FUNCTIONS",
            messageParameters = Map("queryText" -> s"${exprText.orElse(queryText).get}")
          )
      }
    }
  }

  /**
   * Generate the function properties, including:
   * 1. the SQL configs when creating the function.
   * 2. the catalog and database name when creating the function. This will be used to provide
   *    context during nested function resolution.
   * 3. referred temporary object names if the function is a temp function.
   */
  private def generateFunctionProperties(
      session: SparkSession,
      plan: LogicalPlan,
      analyzed: LogicalPlan): Map[String, String] = {
    val catalog = session.sessionState.catalog
    val conf = session.sessionState.conf
    val manager = session.sessionState.catalogManager

    // Only collect temporary object names when the function is a temp function.
    val (tempViews, tempFunctions) = if (isTemp) {
      collectTemporaryObjectsInUnresolvedPlan(catalog, plan)
    } else {
      (Nil, Nil)
    }
    val tempVars = ViewHelper.collectTemporaryVariables(analyzed)

    sqlConfigsToProps(conf) ++
      catalogAndNamespaceToProps(
        manager.currentCatalog.name,
        manager.currentNamespace.toIndexedSeq) ++
      referredTempNamesToProps(tempViews, tempFunctions, tempVars)
  }
}
