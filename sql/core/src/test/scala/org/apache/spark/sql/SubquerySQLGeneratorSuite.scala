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
      isScalarSubquery: Boolean): Query = {

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

    Query(selectClause, fromClause, whereClause, groupByClause,
      orderByClause, limitClause)()
  }

  /**
   * Generate a query (that has a subquery) with the given parameters.
   * @param innerTable Table within the subquery.
   * @param outerTable Table outside of the subquery, in the main query.
   * @param subqueryAlias
   * @param subqueryLocation The clause of the main query where the subquery is located.
   * @param subqueryType The type of subquery, such as SCALAR, PREDICATE (EQUALS, EXISTS, etc.)
   * @param isCorrelated Whether the subquery is to be correlated.
   * @param isDistinct Whether subquery results is to be de-duplicated, i.e. have a DISTINCT clause.
   * @param operatorInSubquery The operator to be included in the subquery.
   */
  private def generateQuery(
      innerTable: Relation,
      outerTable: Relation,
      subqueryAlias: String,
      subqueryLocation: SubqueryLocation.Value,
      subqueryType: SubqueryType.Value,
      isCorrelated: Boolean,
      isDistinct: Boolean,
      operatorInSubquery: Operator): Query = {

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

    val (queryProjection, selectClause, fromClause, whereClause) = subqueryLocation match {
      case SubqueryLocation.SELECT =>
        // If the subquery is in the FROM clause, then it is treated as an Attribute.
        val queryProjection = outerTable.output ++
          Seq(Alias(Subquery(subqueryOrganization), subqueryAlias))
        val fromClause = FromClause(Seq(outerTable))
        val selectClause = SelectClause(queryProjection)
        (queryProjection, selectClause, fromClause, None)
      case SubqueryLocation.FROM =>
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
      case SubqueryLocation.WHERE =>
        // If the subquery is in the FROM clause, then it is treated as a Predicate.
        val queryProjection = outerTable.output
        val selectClause = SelectClause(queryProjection)
        val fromClause = FromClause(Seq(outerTable))
        // Hardcoded expression for "=", "<", "IN" and "NOT IN"
        val expr = outerTable.output.last
        val whereClausePredicate = subqueryType match {
          case SubqueryType.SCALAR_PREDICATE_EQUALS =>
            Equals(expr, Subquery(subqueryOrganization))
          case SubqueryType.SCALAR_PREDICATE_LESS_THAN =>
            LessThan(expr, Subquery(subqueryOrganization))
          case SubqueryType.EXISTS => Exists(subqueryOrganization)
          case SubqueryType.NOT_EXISTS => Not(Exists(subqueryOrganization))
          case SubqueryType.IN => In(expr, subqueryOrganization)
          case SubqueryType.NOT_IN => Not(In(expr, subqueryOrganization))
        }
        val whereClause = Some(WhereClause(Seq(whereClausePredicate)))
        (queryProjection, selectClause, fromClause, whereClause)
    }
    val orderByClause = Some(OrderByClause(queryProjection))

    val comment = f"-- innerTable=$innerTable, outerTable=$outerTable," +
      f" subqueryLocation=$subqueryLocation, subqueryType=$subqueryType," +
      f" isCorrelated=$isCorrelated, subqueryDistinct=$isDistinct," +
      f" subqueryOperator=$operatorInSubquery"
    Query(selectClause, fromClause, whereClause, groupByClause = None,
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

    // scalastyle:off line.size.limit
    val createTableSql = f"""
        |CREATE TEMPORARY VIEW ${INNER_TABLE.name}(${INNER_TABLE.output.map(_.name).mkString(", ")}) AS VALUES
        |    (1, 1),
        |    (2, 2),
        |    (3, 3),
        |    (4, 4),
        |    (5, 5),
        |    (8, 8),
        |    (9, 9);
        |
        |CREATE TEMPORARY VIEW ${OUTER_TABLE.name}(${OUTER_TABLE.output.map(_.name).mkString(", ")}) AS VALUES
        |    (1, 1),
        |    (2, 1),
        |    (3, 3),
        |    (6, 6),
        |    (7, 7),
        |    (9, 9);
        |
        |CREATE TEMPORARY VIEW ${NO_MATCH_TABLE.name}(${NO_MATCH_TABLE.output.map(_.name).mkString(", ")}) AS VALUES
        |    (1000, 1000);
        |
        |CREATE TEMPORARY VIEW ${JOIN_TABLE.name}(${JOIN_TABLE.output.map(_.name).mkString(", ")}) AS VALUES
        |    (1, 1),
        |    (2, 1),
        |    (3, 3),
        |    (7, 8),
        |    (5, 6);
        |
        |CREATE TEMPORARY VIEW ${NULL_TABLE.name}(${NULL_TABLE.output.map(_.name).mkString(", ")}) AS SELECT CAST(null AS int), CAST(null as int);
        |""".stripMargin.strip()
    // scalastyle:off line.size.limit

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
          // Hardcoded keys for join condition.
          condition = Equals(innerTable.output.head, JOIN_TABLE.output.head),
          joinType = joinType))
        val leftTableQuery = Query(SelectClause(innerTable.output), FromClause(Seq(innerTable)))()
        val rightTableQuery = Query(SelectClause(JOIN_TABLE.output), FromClause(Seq(JOIN_TABLE)))()
        val setOps = SET_OPERATIONS.map(setOp =>
          SetOperation(leftTableQuery, rightTableQuery, setOp))
          .map(plan => {
            val output = innerTable.output.map(a => a.copy(qualifier = Some(INNER_SUBQUERY_ALIAS)))
            SubqueryRelation(name = INNER_SUBQUERY_ALIAS, output = output, inner = plan)
          })
        (Seq(innerTable) ++ joins ++ setOps).map(inner => (inner, outerTable))
    }

    // If the subquery is in the SELECT clause of the main query, it can only be a scalar subquery.
    def subqueryTypeChoices(subqueryLocation: SubqueryLocation.Value): Seq[SubqueryType.Value] = {
      if (subqueryLocation == SubqueryLocation.SELECT) {
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
    def correlationChoices(subqueryLocation: SubqueryLocation.Value): Seq[Boolean] = {
      if (subqueryLocation == SubqueryLocation.FROM) {
        Seq(false)
      } else {
        Seq(true, false)
      }
    }

    case class QuerySpec(query: Query, isCorrelated: Boolean,
        subqueryLocation: SubqueryLocation.Value, subqueryType: SubqueryType.Value)

    val generatedQueries = scala.collection.mutable.Set[QuerySpec]()

    // Generate queries across the different axis.
    for {
      (innerTable, outerTable) <- ALL_COMBINATIONS
      subqueryLocation <-
        Seq(SubqueryLocation.WHERE, SubqueryLocation.SELECT, SubqueryLocation.FROM)
      subqueryType <- subqueryTypeChoices(subqueryLocation)
      isCorrelated <- correlationChoices(subqueryLocation)
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

      for (subqueryOperator <- SUBQUERY_OPERATORS) {
        val generatedQuery = generateQuery(innerTable, outerTable, SUBQUERY_ALIAS, subqueryLocation,
          subqueryType, isCorrelated, isDistinct, subqueryOperator)
        generatedQueries += QuerySpec(generatedQuery, isCorrelated, subqueryLocation, subqueryType)
      }
    }

    // Partition the queries by (isCorrelated, subqueryLocation, SubqueryType).
    val partitionedQueries = generatedQueries.groupBy(query =>
      (query.isCorrelated, query.subqueryLocation, query.subqueryType))

    val baseResourcePath =
      getWorkspaceFilePath("sql", "core", "src", "test", "resources", "sql-tests").toFile
    val inputFilePath = new File(baseResourcePath, "inputs").getAbsolutePath
    val generatedSubqueryDirPath = new File(inputFilePath, "subquery/generated")

    partitionedQueries.foreach { case ((isCorrelated, subqueryLocation, subqueryType), querySpec) =>
      val correlationDirName = if (isCorrelated) "correlated" else "uncorrelated"
      val subqueryLocationDirName = subqueryLocation match {
        case SubqueryLocation.SELECT => "select"
        case SubqueryLocation.FROM => "from"
        case SubqueryLocation.WHERE => "where"
      }
      val subqueryTypeName = subqueryType match {
        case SubqueryType.IN | SubqueryType.NOT_IN => "in"
        case SubqueryType.EXISTS | SubqueryType.NOT_EXISTS => "exists"
        case _ => "scalar"
      }
      val resultFile = new File(generatedSubqueryDirPath,
        f"$correlationDirName/$subqueryLocationDirName/$subqueryTypeName.sql")
      val parent = resultFile.getParentFile
      if (!parent.exists()) {
        assert(parent.mkdirs(), "Could not create directory: " + parent)
      }
      val queryString = createTableSql + "\n\n" + querySpec.map(_.query).mkString(";\n\n") + "\n"
      stringToFile(resultFile, queryString)
    }
  }
}
