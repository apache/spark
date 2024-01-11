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

package org.apache.spark.sql

import java.io.File

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.util.stringToFile

class SubquerySQLGeneratorSuite
  extends SparkFunSuite with QueryGeneratorHelper {

  /**
   * Function to generate a subquery given the following parameters:
   * @param innerTable The relation within the subquery.
   * @param correlationConditions Conditions referencing both inner and outer tables.
   * @param isDistinct Whether the result of the subquery is to be de-duplicated.
   * @param operatorInSubquery The operator to be included in this subquery.
   * @param isScalarSubquery Whether the subquery is a scalar subquery or not.
   */
  private def generateSubquery(
      innerTable: Relation,
      correlationConditions: Seq[Predicate],
      isDistinct: Boolean,
      operatorInSubquery: Operator,
      isScalarSubquery: Boolean): QueryOrganization = {

    // Generating the From clause of the subquery:
    val fromClause = FromClause(Seq(innerTable))

    // Generating the Select clause of the subquery: consider Aggregation result expressions, if the
    // operator to be included is an Aggregate.
    val projections = operatorInSubquery match {
      case Aggregate(resultExpressions, _) => resultExpressions
      case _ => Seq(innerTable.output.head)
    }
    val selectClause = SelectClause(projections, isDistinct = isDistinct)

    // Generating the Where clause of the subquery: add correlation conditions, if any.
    val whereClause = if (correlationConditions.nonEmpty) {
      Some(WhereClause(correlationConditions))
    } else {
      None
    }

    // Generating the GroupBy clause of the subquery: add GroupBy if the operator to be included is
    // an Aggregate.
    val groupByClause = operatorInSubquery match {
      case a: Aggregate if a.groupingExpressions.nonEmpty =>
        Some(GroupByClause(a.groupingExpressions))
      case _ => None
    }

    // For the OrderBy, consider whether or not the result of the subquery is required to be sorted.
    // This is to maintain test determinism. This is affected by whether the subquery has a limit
    // clause.
    val requiresLimitOne = isScalarSubquery && (operatorInSubquery match {
      case a: Aggregate => a.groupingExpressions.nonEmpty
      case l: Limit => l.limitValue > 1
      case _ => true
    })

    val orderByClause = if (requiresLimitOne || operatorInSubquery.isInstanceOf[Limit]) {
      Some(OrderByClause(projections))
    } else {
      None
    }

    // For the Limit clause, consider whether the subquery needs to return 1 row, or whether the
    // operator to be included is a Limit.
    val limitClause = if (requiresLimitOne) {
      Some(Limit(1))
    } else operatorInSubquery match {
      case limit: Limit =>
        Some(limit)
      case _ =>
        None
    }

    QueryOrganization(selectClause, fromClause, whereClause, groupByClause,
      orderByClause, limitClause)()
  }

  /**
   * Generate a query (that has a subquery) with the given parameters.
   * @param innerTable Table within the subquery.
   * @param outerTable Table outside of the subquery, in the main query.
   * @param subqueryAlias
   * @param subqueryClause The clause of the main query where the subquery is located.
   * @param subqueryType The type of subquery, such as SCALAR, PREDICATE (EQUALS, EXISTS, etc.)
   * @param isCorrelated Whether the subquery is to be correlated.
   * @param isDistinct Whether subquery results is to be de-duplicated, i.e. have a DISTINCT clause.
   * @param operatorInSubquery The operator to be included in the subquery.
   */
  private def generateQuery(
      innerTable: Relation,
      outerTable: Relation,
      subqueryAlias: String,
      subqueryClause: SubqueryClause.Value,
      subqueryType: SubqueryType.Value,
      isCorrelated: Boolean,
      isDistinct: Boolean,
      operatorInSubquery: Operator): QueryOrganization = {

    // Correlation conditions, this is hardcoded for now.
    val correlationConditions = if (isCorrelated) {
      Seq(Equals(innerTable.output.head, outerTable.output.head))
    } else {
      Seq()
    }
    val isScalarSubquery = Seq(SubqueryType.SCALAR, SubqueryType.SCALAR_PREDICATE_EQUALS,
      SubqueryType.SCALAR_PREDICATE_LESS_THAN).contains(subqueryType)
    val subqueryOrganization = generateSubquery(
      innerTable, correlationConditions, isDistinct, operatorInSubquery, isScalarSubquery)

    val (queryProjection, selectClause, fromClause, whereClause) = subqueryClause match {
      case SubqueryClause.SELECT =>
        // If the subquery is in the FROM clause, then it is treated as an Attribute.
        val queryProjection = outerTable.output ++
          Seq(Alias(Subquery(subqueryOrganization), subqueryAlias))
        val fromClause = FromClause(Seq(outerTable))
        val selectClause = SelectClause(queryProjection)
        (queryProjection, selectClause, fromClause, None)
      case SubqueryClause.FROM =>
        // If the subquery is in the FROM clause, then it is treated as a Relation.
        val subqueryProjection = subqueryOrganization.selectClause.projection
        // Transform the subquery projection as Attributes from a Relation.
        val subqueryOutput = subqueryProjection.map {
          case a: Attribute => Attribute(name = a.name, qualifier = Some(subqueryAlias))
          case a: Alias => Attribute(name = a.name, qualifier = Some(subqueryAlias))
        }
        val selectClause = SelectClause(subqueryOutput)
        val subqueryRelation = SubqueryRelation(subqueryAlias, subqueryOutput,
          subqueryOrganization)
        val fromClause = FromClause(Seq(subqueryRelation))
        (subqueryOutput, selectClause, fromClause, None)
      case SubqueryClause.WHERE =>
        // If the subquery is in the FROM clause, then it is treated as a Predicate.
        val queryProjection = outerTable.output
        val selectClause = SelectClause(queryProjection)
        val fromClause = FromClause(Seq(outerTable))
        // Hardcoded expression for "=", "<", "IN" and "NOT IN"
        val expr = outerTable.output.last
        val whereClausePredicate = subqueryType match {
          case SubqueryType.SCALAR_PREDICATE_EQUALS =>
            Equals(expr, ScalarSubquery(subqueryOrganization))
          case SubqueryType.SCALAR_PREDICATE_LESS_THAN =>
            LessThan(expr, ScalarSubquery(subqueryOrganization))
          case SubqueryType.EXISTS => Exists(subqueryOrganization)
          case SubqueryType.NOT_EXISTS => NotExists(subqueryOrganization)
          case SubqueryType.IN => In(expr, subqueryOrganization)
          case SubqueryType.NOT_IN => NotIn(expr, subqueryOrganization)
        }
        val whereClause = Some(WhereClause(Seq(whereClausePredicate)))
        (queryProjection, selectClause, fromClause, whereClause)
    }
    val orderByClause = Some(OrderByClause(queryProjection))

    val comment = f"-- inner_table=$innerTable, outer_table=$outerTable," +
      f" subqueryClause=$subqueryClause, subquery_type=$subqueryType," +
      f" is_correlated=$isCorrelated, distinct=$isDistinct, subquery_operator=$operatorInSubquery"
    QueryOrganization(selectClause, fromClause, whereClause, groupByClause = None,
      orderByClause, limitClause = None)(comment)
  }

  test("Generate subquery SQL") {
    val FIRST_COLUMN = "a"
    val SECOND_COLUMN = "b"

    // Table definitions
    val INNER_TABLE_NAME = "inner_table"
    val INNER_TABLE_SCHEMA = Seq(
      Attribute(FIRST_COLUMN, Some(INNER_TABLE_NAME)),
      Attribute(SECOND_COLUMN, Some(INNER_TABLE_NAME)))
    val INNER_TABLE = TableRelation(INNER_TABLE_NAME, INNER_TABLE_SCHEMA)

    val OUTER_TABLE_NAME = "outer_table"
    val OUTER_TABLE_SCHEMA = Seq(
      Attribute(FIRST_COLUMN, Some(OUTER_TABLE_NAME)),
      Attribute(SECOND_COLUMN, Some(OUTER_TABLE_NAME)))
    val OUTER_TABLE = TableRelation(OUTER_TABLE_NAME, OUTER_TABLE_SCHEMA)

    val NO_MATCH_TABLE_NAME = "no_match_table"
    val NO_MATCH_TABLE_SCHEMA = Seq(
      Attribute(FIRST_COLUMN, Some(NO_MATCH_TABLE_NAME)),
      Attribute(SECOND_COLUMN, Some(NO_MATCH_TABLE_NAME)))
    val NO_MATCH_TABLE = TableRelation(NO_MATCH_TABLE_NAME, NO_MATCH_TABLE_SCHEMA)

    val JOIN_TABLE_NAME = "join_table"
    val JOIN_TABLE_SCHEMA = Seq(
      Attribute(FIRST_COLUMN, Some(JOIN_TABLE_NAME)),
      Attribute(SECOND_COLUMN, Some(JOIN_TABLE_NAME)))
    val JOIN_TABLE = TableRelation(JOIN_TABLE_NAME, JOIN_TABLE_SCHEMA)

    val NULL_TABLE_NAME = "null_table"
    val NULL_TABLE_SCHEMA = Seq(
      Attribute(FIRST_COLUMN, Some(NULL_TABLE_NAME)),
      Attribute(SECOND_COLUMN, Some(NULL_TABLE_NAME)))
    val NULL_TABLE = TableRelation(NULL_TABLE_NAME, NULL_TABLE_SCHEMA)

    val TABLE_COMBINATIONS = Seq(
      (INNER_TABLE, OUTER_TABLE),
      (INNER_TABLE, NULL_TABLE),
      (NULL_TABLE, OUTER_TABLE),
      (NO_MATCH_TABLE, OUTER_TABLE),
      (INNER_TABLE, NO_MATCH_TABLE)
    )

    val INNER_SUBQUERY_ALIAS = "innerSubqueryAlias"
    val SUBQUERY_ALIAS = "subqueryAlias"
    val AGGREGATE_FUNCTION_ALIAS = "aggFunctionAlias"
    val JOIN_TYPES = Seq(JoinType.INNER, JoinType.LEFT_OUTER, JoinType.RIGHT_OUTER)
    val SET_OPERATIONS = Seq(
      SetOperationType.UNION, SetOperationType.EXCEPT, SetOperationType.INTERSECT)

    // Generate combinations of the inner table to have joins and set operations (with the
    // JOIN_TABLE).
    val ALL_COMBINATIONS = TABLE_COMBINATIONS.flatMap {
      case (innerTable, outerTable) =>
        val joins = JOIN_TYPES.map(joinType => JoinedRelation(
          leftRelation = innerTable,
          rightRelation = JOIN_TABLE,
          // Hardcoded join condition.
          condition = Equals(innerTable.output.head, JOIN_TABLE.output.head),
          joinType = joinType))
        val setOps = SET_OPERATIONS.map(setOp => SetOperation(innerTable, JOIN_TABLE, setOp))
          .map(plan => {
            val output = innerTable.output.map(a => a.copy(qualifier = Some(INNER_SUBQUERY_ALIAS)))
            SubqueryRelation(name = INNER_SUBQUERY_ALIAS, output = output, inner = plan)
          })
        (Seq(innerTable) ++ joins ++ setOps).map(inner => (inner, outerTable))
    }

    // If the subquery is in the SELECT clause of the main query, it can only be a scalar subquery.
    def subqueryTypeChoices (subqueryClause: SubqueryClause.Value): Seq[SubqueryType.Value] = {
      if (subqueryClause == SubqueryClause.SELECT) {
        Seq(SubqueryType.SCALAR)
      } else {
        Seq(
          SubqueryType.SCALAR_PREDICATE_LESS_THAN,
          SubqueryType.SCALAR_PREDICATE_EQUALS,
          SubqueryType.IN,
          SubqueryType.NOT_IN,
          SubqueryType.EXISTS,
          SubqueryType.NOT_EXISTS)
      }
    }
    // If the subquery is in the FROM clause of the main query, it cannot be correlated.
    def correlationChoices(subqueryClause: SubqueryClause.Value): Seq[Boolean] = {
      if (subqueryClause== SubqueryClause.FROM) {
        Seq(false)
      } else {
        Seq(true, false)
      }
    }

    val generatedQueries = scala.collection.mutable.ListBuffer[QueryOrganization]()

    // Generate queries across the different axis.
    for {
      (innerTable, outerTable) <- ALL_COMBINATIONS
      subqueryClause <- Seq(SubqueryClause.WHERE, SubqueryClause.SELECT, SubqueryClause.FROM)
      subqueryType <- subqueryTypeChoices(subqueryClause)
      isCorrelated <- correlationChoices(subqueryClause)
      isDistinct <- Seq(true, false)
    } {
      // Hardcoded aggregation column and group by column.
      val (aggColumn, groupByColumn) = innerTable.output.head -> innerTable.output(1)
      val aggFunctions = Seq(Sum(aggColumn), Count(aggColumn))
        .map(af => Alias(af, AGGREGATE_FUNCTION_ALIAS))
      val groupByOptions = Seq(true, false)
      // Generate all combinations of (aggFunction = sum/count, groupBy = true/false).
      val combinations = aggFunctions.flatMap(agg => groupByOptions.map(groupBy => (agg, groupBy)))
      val aggregates = combinations.map {
        case (af, groupBy) => Aggregate(Seq(af), if (groupBy) Seq(groupByColumn) else Seq())
      }
      val SUBQUERY_OPERATORS = Seq(Limit(1), Limit(10)) ++ aggregates

      for {
        subqueryOperator <- SUBQUERY_OPERATORS
        // Generate queries.
        generatedQuery = generateQuery(innerTable, outerTable, SUBQUERY_ALIAS, subqueryClause,
          subqueryType, isCorrelated, isDistinct, subqueryOperator)
        if !generatedQueries.contains(generatedQuery)
      } {
        generatedQueries += generatedQuery
      }
    }

    // Number of queries we want per file. This is to reduce test duration / improve test
    // efficiency with sharding.
    val sizeOfSubarrays: Int = 50
    val arrayOfGeneratedQueries = generatedQueries.grouped(sizeOfSubarrays)
      .map(queries => queries.mkString(";\n"))
    val fileNames = arrayOfGeneratedQueries.zipWithIndex.map {
      case (_, index) => f"generated_subquery_$index.sql"
    }

    val baseResourcePath =
      getWorkspaceFilePath("sql", "core", "src", "test", "resources", "sql-tests").toFile
    val inputFilePath = new File(baseResourcePath, "inputs").getAbsolutePath
    val generatedSubqueryDirPath = new File(inputFilePath, "subquery/generated")

    fileNames.zip(arrayOfGeneratedQueries).foreach {
      case (fileName, queryString) =>
        val file = new File(generatedSubqueryDirPath, fileName)
        val parent = file.getParentFile
        if (!parent.exists()) {
          assert(parent.mkdirs(), "Could not create directory: " + parent)
        }
        stringToFile(file, queryString)
    }
  }
}
