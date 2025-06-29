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
package org.apache.spark.sql.{jdbc, SparkSession}

import java.sql.{Connection, ResultSet, Statement}
import java.util.Locale

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import org.apache.spark.sql.{QueryGeneratorHelper, QueryTest}
import org.apache.spark.sql.Row
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.tags.DockerTest

/**
 * This suite is used to generate subqueries, and test Spark against Postgres.
 * To run this test suite for a specific version (e.g., postgres:17.2-alpine):
 * {{{
 *   ENABLE_DOCKER_INTEGRATION_TESTS=1 POSTGRES_DOCKER_IMAGE_NAME=postgres:17.2-alpine
 *     ./build/sbt -Pdocker-integration-tests
 *     "docker-integration-tests/testOnly org.apache.spark.sql.jdbc.GeneratedSubquerySuite"
 * }}}
 */
@DockerTest
class GeneratedSubquerySuite extends DockerJDBCIntegrationSuite with QueryGeneratorHelper {
  // Turn on testNestedCorrelations to test nested correlated subqueries only.
  private val testNestedCorrelationsFlag: Boolean =
    sys.props.getOrElse("testNestedCorrelations", "false").map(_.toBoolean)

  // If the user has set the subqueryType property, test that subquery type only.
  private val customizedSubqueryTypeFlag: String =
    sys.props.getOrElse("subqueryType", "").map(_.toLowerCase(Locale.ROOT))

  override val db = new PostgresDatabaseOnDocker

  private val FIRST_COLUMN = "a"
  private val SECOND_COLUMN = "b"

  // Table definitions
  private val INNER_TABLE_NAME = "inner_table"
  private val INNER_TABLE_SCHEMA = Seq(
  Attribute(FIRST_COLUMN, Some(INNER_TABLE_NAME)),
  Attribute(SECOND_COLUMN, Some(INNER_TABLE_NAME)))
  private val INNER_TABLE = TableRelation(INNER_TABLE_NAME, INNER_TABLE_SCHEMA)

  private val MIDDLE_TABLE_NAME = "middle_table"
  private val MIDDLE_TABLE_SCHEMA = Seq(
    Attribute(FIRST_COLUMN, Some(MIDDLE_TABLE_NAME)),
    Attribute(SECOND_COLUMN, Some(MIDDLE_TABLE_NAME)))
  )
  private val MIDDLE_TABLE = TableRelation(MIDDLE_TABLE_NAME, MIDDLE_TABLE_SCHEMA)

  private val OUTER_TABLE_NAME = "outer_table"
  private val OUTER_TABLE_SCHEMA = Seq(
    Attribute(FIRST_COLUMN, Some(OUTER_TABLE_NAME)),
    Attribute(SECOND_COLUMN, Some(OUTER_TABLE_NAME)))
  private val OUTER_TABLE = TableRelation(OUTER_TABLE_NAME, OUTER_TABLE_SCHEMA)

  private val NO_MATCH_TABLE_NAME = "no_match_table"
  private val NO_MATCH_TABLE_SCHEMA = Seq(
    Attribute(FIRST_COLUMN, Some(NO_MATCH_TABLE_NAME)),
    Attribute(SECOND_COLUMN, Some(NO_MATCH_TABLE_NAME)))
  private val NO_MATCH_TABLE = TableRelation(NO_MATCH_TABLE_NAME, NO_MATCH_TABLE_SCHEMA)

  private val JOIN_TABLE_NAME = "join_table"
  private val JOIN_TABLE_SCHEMA = Seq(
    Attribute(FIRST_COLUMN, Some(JOIN_TABLE_NAME)),
    Attribute(SECOND_COLUMN, Some(JOIN_TABLE_NAME)))
  private val JOIN_TABLE = TableRelation(JOIN_TABLE_NAME, JOIN_TABLE_SCHEMA)

  private val NULL_TABLE_NAME = "null_table"
  private val NULL_TABLE_SCHEMA = Seq(
    Attribute(FIRST_COLUMN, Some(NULL_TABLE_NAME)),
    Attribute(SECOND_COLUMN, Some(NULL_TABLE_NAME)))
  private val NULL_TABLE = TableRelation(NULL_TABLE_NAME, NULL_TABLE_SCHEMA)

  override def dataPreparation(conn: Connection): Unit = {}

  private def generateRandomDataSQL(
      tableName: String, schemaStr: String, dataStr: Option[String] = None): String = {

    def toSqlValues(data: Seq[(Int, Int)]): String = {
      data.map { case (a, b) => s"($a, $b)" }.mkString(",\n ")
    }

    val finalDataStr = if (dataStr.isEmpty) {
      val rnd = new Random()

      val baseData = (1 to 10).map(i => (i, i))

      // Remove 2 random entries
      val toRemove = rnd.shuffle(baseData).take(2)
      val afterRemoval = baseData.filterNot(toRemove.toSet)

      // Duplicate 2 random entries from remaining
      val toDuplicate = rnd.shuffle(afterRemoval).take(2)
      val finalData = afterRemoval ++ toDuplicate
      toSqlValues(finalData)
    } else {
      dataStr.get
    }

    val statement =
      f"""
         |CREATE TEMPORARY VIEW $tableName
         |($schemaStr) AS VALUES
         |    ${finalDataStr};
         |""".stripMargin

    statement
  }

  private def createTables(conn: Connection, localSparkSession: SparkSession): Unit = {
    val innerTableCreationSql = generateRandomDataSQL(
      INNER_TABLE_NAME, INNER_TABLE.output.map(_.name).mkString(", "))
    val outerTableCreationSql = generateRandomDataSQL(
      OUTER_TABLE_NAME, OUTER_TABLE.output.map(_.name).mkString(", "))
    val noMatchTableCreationSql = generateRandomDataSQL(
      NO_MATCH_TABLE_NAME, NO_MATCH_TABLE.output.map(_.name).mkString(", "), "(1000, 1000)")
    val joinTableCreationSql = generateRandomDataSQL(
      JOIN_TABLE_NAME, JOIN_TABLE.output.map(_.name).mkString(", "), "(1, 1), (2, 1), (3, 3), (7, 8), (5, 6)")
    val nullTableCreationSql = f"""
                                  |CREATE TEMPORARY VIEW ${NULL_TABLE.name}
                                  |(${NULL_TABLE.output.map(_.name).mkString(", ")}) AS
                                  | SELECT CAST(null AS int), CAST(null as int);
                                  |""".stripMargin
    conn.prepareStatement(innerTableCreationSql).executeUpdate()
    conn.prepareStatement(outerTableCreationSql).executeUpdate()
    conn.prepareStatement(nullTableCreationSql).executeUpdate()
    conn.prepareStatement(joinTableCreationSql).executeUpdate()
    conn.prepareStatement(noMatchTableCreationSql).executeUpdate()
    localSparkSession.sql(innerTableCreationSql)
    localSparkSession.sql(outerTableCreationSql)
    localSparkSession.sql(nullTableCreationSql)
    localSparkSession.sql(joinTableCreationSql)
    localSparkSession.sql(noMatchTableCreationSql)
  }

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

    // For some situation needs exactly one row as output, we force the
    // subquery to have a limit of 1 and no offset value (in case it outputs
    // empty result set).
    val requiresExactlyOneRowOutput = isScalarSubquery && (operatorInSubquery match {
      case a: Aggregate => a.groupingExpressions.nonEmpty
      case _ => true
    })

    // For the OrderBy, consider whether or not the result of the subquery is required to be sorted.
    // This is to maintain test determinism. This is affected by whether the subquery has a limit
    // clause or an offset clause.
    val orderByClause = if (
      requiresExactlyOneRowOutput || operatorInSubquery.isInstanceOf[LimitAndOffset]
    ) {
      Some(OrderByClause(projections))
    } else {
      None
    }

    // For the Limit clause, consider whether the subquery needs to return 1 row, or whether the
    // operator to be included is a Limit.
    val limitAndOffsetClause = if (requiresExactlyOneRowOutput) {
      Some(LimitAndOffset(1, 0))
    } else {
      operatorInSubquery match {
        case lo: LimitAndOffset =>
          if (lo.offsetValue == 0 && lo.limitValue == 0) {
            None
          } else {
            Some(lo)
          }
        case _ => None
      }
    }

    Query(
      selectClause, fromClause, whereClause, groupByClause, orderByClause, limitAndOffsetClause
    )
  }

  /**
   * Generate a query (that has a subquery) with the given parameters.
   * @param tables The tables to be included in the main query. The first table is the inner most table,
   *               the last table is the outer most table.
   * @param subqueryAlias
   * @param subqueryLocation The clause of the main query where the subquery is located.
   * @param subqueryType The type of subquery, such as SCALAR, RELATION, PREDICATE
   * @param correlationConditions The correlated conditions of subquery.
   * @param isDistinct Whether subquery results is to be de-duplicated, i.e. have a DISTINCT clause.
   * @param operatorInSubquery The operator to be included in the subquery.
   */
  private def generateQuery(
      tables: Seq[Relation],
      subqueryAlias: String,
      subqueryLocation: SubqueryLocation.Value,
      subqueryType: SubqueryType.Value,
      correlationConditions: Seq[Predicate],
      isDistinct: Boolean,
      operatorInSubquery: Operator): Query = {

    val scalarSubqueryTypes = Seq(SubqueryType.SCALAR_PREDICATE_EQUALS,
      SubqueryType.SCALAR_PREDICATE_NOT_EQUALS, SubqueryType.SCALAR_PREDICATE_LESS_THAN,
      SubqueryType.SCALAR_PREDICATE_LESS_THAN_OR_EQUALS, SubqueryType.SCALAR_PREDICATE_GREATER_THAN,
      SubqueryType.SCALAR_PREDICATE_GREATER_THAN_OR_EQUALS)
    val isScalarSubquery = scalarSubqueryTypes.contains(subqueryType)
    val innerTable = tables.head
    val outerTable = tables.tail.head
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
        // If the subquery is in the WHERE clause, then it is treated as a Predicate.
        val queryProjection = outerTable.output
        val selectClause = SelectClause(queryProjection)
        val fromClause = FromClause(Seq(outerTable))
        // Hardcoded expression for "=", "<", "IN" and "NOT IN"
        var currentSubquery = subqueryOrganization
        var whereClausePredicates: Seq[Predicate] = Seq.empty[Predicate]
        for (table <- tables.tail) {
          val expr = table.output.last
          val whereClausePredicate = subqueryType match {
            case SubqueryType.SCALAR_PREDICATE_EQUALS =>
              Equals(expr, Subquery(currentSubquery))
            case SubqueryType.SCALAR_PREDICATE_NOT_EQUALS =>
              NotEquals(expr, Subquery(currentSubquery))
            case SubqueryType.SCALAR_PREDICATE_LESS_THAN =>
              LessThan(expr, Subquery(currentSubquery))
            case SubqueryType.SCALAR_PREDICATE_LESS_THAN_OR_EQUALS =>
              LessThanOrEquals(expr, Subquery(currentSubquery))
            case SubqueryType.SCALAR_PREDICATE_GREATER_THAN =>
              GreaterThan(expr, Subquery(currentSubquery))
            case SubqueryType.SCALAR_PREDICATE_GREATER_THAN_OR_EQUALS =>
              GreaterThanOrEquals(expr, Subquery(currentSubquery))
            case SubqueryType.EXISTS => Exists(currentSubquery)
            case SubqueryType.NOT_EXISTS => Not(Exists(currentSubquery))
            case SubqueryType.IN => In(expr, currentSubquery)
            case SubqueryType.NOT_IN => Not(In(expr, currentSubquery))
          }
          val aggColumn = table.output.head
          // Use no groupBy, aggFunc = Count to test count bug scenario.
          val agg = Aggregate(Seq(Count(aggColumn)), Seq())
          // TODO: right now we always generate same subquery types for the middle subqueries.
          // Testing for more combined subqueries can be supported later.
          currentSubquery = generateSubquery(
            table, Seq(whereClausePredicate), false, agg, scalarSubqueryTypes.contains(subqueryType))
          whereClausePredicates = Seq(whereClausePredicate)
        }
        val whereClause = Some(WhereClause(whereClausePredicates))
        (queryProjection, selectClause, fromClause, whereClause)
    }
    val orderByClause = Some(OrderByClause(queryProjection))

    Query(selectClause, fromClause, whereClause, groupByClause = None,
      orderByClause, limitAndOffsetClause = None)
  }

  private def getPostgresResult(stmt: Statement, sql: String): Array[Row] = {
    val isResultSet = stmt.execute(sql)
    val rows = ArrayBuffer[Row]()
    if (isResultSet) {
      val rs = stmt.getResultSet
      val metadata = rs.getMetaData
      while (rs.next()) {
        val row = Row.fromSeq((1 to metadata.getColumnCount).map(i => rs.getObject(i)))
        rows.append(row)
      }
      rows.toArray
    } else {
      Array.empty
    }
  }

  private def generateBasicTableCombinations(): Seq[Seq[Relation]] = {
    if (testNestedCorrelationsFlag) {
        Seq(
          (INNER_TABLE, MIDDLE_TABLE, OUTER_TABLE),
          (INNER_TABLE, MIDDLE_TABLE, NULL_TABLE),
          (INNER_TABLE, MIDDLE_TABLE, NO_MATCH_TABLE),
          (INNER_TABLE, NULL_TABLE, OUTER_TABLE),
          (INNER_TABLE, NULL_TABLE, NO_MATCH_TABLE),
          (INNER_TABLE, NO_MATCH_TABLE, OUTER_TABLE),
          (INNER_TABLE, NO_MATCH_TABLE, NULL_TABLE),
          (NULL_TABLE, MIDDLE_TABLE, OUTER_TABLE),
          (NULL_TABLE, MIDDLE_TABLE, NO_MATCH_TABLE),
          (NO_MATCH_TABLE, MIDDLE_TABLE, OUTER_TABLE),
          (NO_MATCH_TABLE, MIDDLE_TABLE, NULL_TABLE)
        )
    } else {
      Seq(
        (INNER_TABLE, OUTER_TABLE),
        (INNER_TABLE, NULL_TABLE),
        (NULL_TABLE, OUTER_TABLE),
        (NO_MATCH_TABLE, OUTER_TABLE),
        (INNER_TABLE, NO_MATCH_TABLE)
      )
    }
  }

  private def generateAllRelationCombinations(tableCombinations: Seq[Seq[Relation]]): Seq[Seq[Relation]] = {
    // Generate combinations of the inner table to have joins and set operations (with the
    // JOIN_TABLE).
    tableCombinations.flatMap {
      case tables =>
        val (innerTable, remainedTables) = (tables.head, tables.tail)
        val joinTypes = Seq(JoinType.INNER, JoinType.LEFT_OUTER, JoinType.RIGHT_OUTER)
        val setOperations = Seq(
          SetOperationType.UNION, SetOperationType.EXCEPT, SetOperationType.INTERSECT)
        val joins = joinTypes.map(joinType => JoinedRelation(
          leftRelation = innerTable,
          rightRelation = JOIN_TABLE,
          // Hardcoded keys for join condition.
          condition = Equals(innerTable.output.head, JOIN_TABLE.output.head),
          joinType = joinType))
        // Hardcoded select all for set operation.
        val leftTableQuery = Query(SelectClause(innerTable.output), FromClause(Seq(innerTable)))
        val rightTableQuery = Query(SelectClause(JOIN_TABLE.output), FromClause(Seq(JOIN_TABLE)))
        val setOps = setOperations.map(setOp =>
            SetOperation(leftTableQuery, rightTableQuery, setOp))
          .map(plan => {
            val output = innerTable.output.map(a => a.copy(qualifier = Some(innerSubqueryAlias)))
            SubqueryRelation(name = innerSubqueryAlias, output = output, inner = plan)
          })
        (Seq(innerTable) ++ joins ++ setOps).map(inner => Seq(inner) ++ remainedTables)
    }
  }

  private def generateCorrelationConditions(
      tables: Seq[Relation],
      isCorrelated: Boolean,
      subqueryType: SubqueryType.Value): Seq[Seq[Predicate]] = {
    if (isCorrelated) {
      if (tables.size > 2 && subqueryType != SubqueryType.ATTRIBUTE &&
          subqueryType != SubqueryType.RELATION) {
        // We don't test nested correlations for Attribute or Relation subquery types.
        val innerTable = tables(0)
        val middleTable = tables(1)
        val outerTable = tables.last
        Seq(
          Seq(Equals(innerTable.output.head, outerTable.output.head)),
          Seq(NotEquals(innerTable.output.head, outerTable.output.head)),
          Seq(LessThan(innerTable.output.head, outerTable.output.head)),
          Seq(LessThanOrEquals(innerTable.output.head, outerTable.output.head)),
          Seq(GreaterThan(innerTable.output.head, outerTable.output.head)),
          Seq(GreaterThanOrEquals(innerTable.output.head, outerTable.output.head)),
          Seq(Equals(innerTable.output.head, outerTable.output.head),
            Equals(innerTable.output.head, middleTable.output.head)),
          Seq(NotEquals(innerTable.output.head, outerTable.output.head),
            NotEquals(innerTable.output.head, middleTable.output.head)),
          Seq(LessThan(innerTable.output.head, outerTable.output.head),
            LessThan(innerTable.output.head, middleTable.output.head)),
          Seq(LessThanOrEquals(innerTable.output.head, outerTable.output.head),
            LessThanOrEquals(innerTable.output.head, middleTable.output.head)),
          Seq(GreaterThan(innerTable.output.head, outerTable.output.head),
            GreaterThan(innerTable.output.head, middleTable.output.head)),
          Seq(GreaterThanOrEquals(innerTable.output.head, outerTable.output.head),
            GreaterThanOrEquals(innerTable.output.head, middleTable.output.head))
        )
      } else {
        val innerTable = tables(0)
        val outerTable = tables.last
        Seq(
          Seq(Equals(innerTable.output.head, outerTable.output.head)),
          Seq(NotEquals(innerTable.output.head, outerTable.output.head)),
          Seq(LessThan(innerTable.output.head, outerTable.output.head)),
          Seq(LessThanOrEquals(innerTable.output.head, outerTable.output.head)),
          Seq(GreaterThan(innerTable.output.head, outerTable.output.head)),
          Seq(GreaterThanOrEquals(innerTable.output.head, outerTable.output.head))
        )
      }
    } else {
      Seq(Seq())
    }
  }

  private def subqueryTypeChoices(subqueryLocation: SubqueryLocation.Value): Seq[SubqueryType.Value] = {
    customizedSubqueryTypeFlag match {
      case "scalarsubquery" if subqueryLocation == SubqueryLocation.WHERE => Seq(
        SubqueryType.SCALAR_PREDICATE_LESS_THAN,
        SubqueryType.SCALAR_PREDICATE_LESS_THAN_OR_EQUALS,
        SubqueryType.SCALAR_PREDICATE_GREATER_THAN,
        SubqueryType.SCALAR_PREDICATE_GREATER_THAN_OR_EQUALS,
        SubqueryType.SCALAR_PREDICATE_EQUALS,
        SubqueryType.SCALAR_PREDICATE_NOT_EQUALS
      )
      case "in" if subqueryLocation == SubqueryLocation.WHERE => Seq(
        SubqueryType.IN,
        SubqueryType.NOT_IN
      )
      case "exists" if subqueryLocation == SubqueryLocation.WHERE => Seq(
        SubqueryType.EXISTS,
        SubqueryType.NOT_EXISTS
      )
      case _ =>
        subqueryLocation match {
          case SubqueryLocation.SELECT => Seq(SubqueryType.ATTRIBUTE)
          case SubqueryLocation.FROM => Seq(SubqueryType.RELATION)
          case SubqueryLocation.WHERE => Seq(
            SubqueryType.SCALAR_PREDICATE_LESS_THAN,
            SubqueryType.SCALAR_PREDICATE_LESS_THAN_OR_EQUALS,
            SubqueryType.SCALAR_PREDICATE_GREATER_THAN,
            SubqueryType.SCALAR_PREDICATE_GREATER_THAN_OR_EQUALS,
            SubqueryType.SCALAR_PREDICATE_EQUALS,
            SubqueryType.SCALAR_PREDICATE_NOT_EQUALS,
            SubqueryType.IN,
            SubqueryType.NOT_IN,
            SubqueryType.EXISTS,
            SubqueryType.NOT_EXISTS)
        }
    }
  }

  private def correlationChoices(subqueryLocation: SubqueryLocation.Value): Seq[Boolean] =
    // If the subquery is in the FROM clause of the main query, it cannot be correlated.
    subqueryLocation match {
      case SubqueryLocation.FROM => Seq(false)
      case _ => Seq(true, false)
    }

  private def distinctChoices(subqueryOperator: Operator): Seq[Boolean] = {
    subqueryOperator match {
      // Don't do DISTINCT if there is group by because it is redundant.
      case Aggregate(_, groupingExpressions) if groupingExpressions.isEmpty => Seq(false)
      case _ => Seq(true, false)
    }
  }

  private def limitAndOffsetChoices(): Seq[LimitAndOffset] = {
    val limitValues = Seq(0, 1, 10)
    val offsetValues = Seq(0, 1, 10)
    limitValues.flatMap(
      limit => offsetValues.map(
        offset => LimitAndOffset(limit, offset)
      )
    ).filter(lo => !(lo.limitValue == 0 && lo.offsetValue == 0))
  }

  def generateQueriesAndRunTestCases(): Unit = {
    val tableCombinations = generateBasicTableCombinations()

    val innerSubqueryAlias = "innerSubqueryAlias"
    val subqueryAlias = "subqueryAlias"
    val aggregationFunctionAlias = "aggFunctionAlias"

    val allRelationCombinations = generateAllRelationCombinations(tableCombinations)

    case class SubquerySpec(query: String, isCorrelated: Boolean,
                            hasNestedCorrelation: Boolean, subqueryType: SubqueryType.Value)

    val generatedQuerySpecs = scala.collection.mutable.Set[SubquerySpec]()

    // Generate queries across the different axis.
    for {
      tables <- allRelationCombinations
      subqueryLocation <-
        Seq(SubqueryLocation.WHERE, SubqueryLocation.SELECT, SubqueryLocation.FROM)
      subqueryType <- subqueryTypeChoices(subqueryLocation)
      isCorrelated <- correlationChoices(subqueryLocation)
      correlationCondition <- generateCorrelationConditions(tables, isCorrelated, subqueryType)
    } {
      // Hardcoded aggregation column and group by column.
      val (aggColumn, groupByColumn) = innerTable.output.head -> innerTable.output(1)
      val aggFunctions = Seq(Sum(aggColumn), Count(aggColumn))
        .map(af => Alias(af, aggregationFunctionAlias))
      val groupByOptions = Seq(true, false)
      // Generate all combinations of (aggFunction = sum/count, groupBy = true/false).
      val combinations = aggFunctions.flatMap(agg => groupByOptions.map(groupBy => (agg, groupBy)))
      val aggregates = combinations.map {
        case (af, groupBy) => Aggregate(Seq(af), if (groupBy) Seq(groupByColumn) else Seq())
      }

      val subqueryOperators = limitAndOffsetChoices() ++ aggregates

      for {
        subqueryOperator <- subqueryOperators
        isDistinct <- distinctChoices(subqueryOperator)
      } {
        generatedQuerySpecs += SubquerySpec(generateQuery(tables,
          subqueryAlias, subqueryLocation, subqueryType, correlationCondition, isDistinct,
          subqueryOperator).toString + ";", isCorrelated,
          tables.size > 2 && subqueryType != SubqueryType.ATTRIBUTE && subqueryType != SubqueryType.RELATION, subqueryType)
      }
    }

    // Partition the queries by (isCorrelated, subqueryLocation, SubqueryType).
    val partitionedQueries = generatedQuerySpecs.groupBy(query =>
      (query.isCorrelated, query.hasNestedCorrelation, query.subqueryType))

    // Create separate test case for each partition.
    partitionedQueries.foreach { case ((isCorrelated, hasNestedCorrelation, subqueryType), querySpec) =>
      val testName = (isCorrelated, hasNestedCorrelation) match {
      case (true, true) =>
        f"nested-correlated-${subqueryType.toString.toLowerCase(Locale.ROOT)}"
      case (true, false) =>
        f"correlated-${subqueryType.toString.toLowerCase(Locale.ROOT)}"
      case (false, _) =>
        f"uncorrelated-${subqueryType.toString.toLowerCase(Locale.ROOT)}"
      }

      test(testName) {
        val conn = getConnection()
        val localSparkSession = spark.newSession()
        createTables(conn, localSparkSession)
        // Enable ANSI so that { NULL IN { <empty> } } behavior is correct in Spark.
        localSparkSession.conf.set(SQLConf.ANSI_ENABLED.key, true)

        val generatedQueries = querySpec.map(_.query).toSeq
        // Randomize query order because we are taking a subset of queries.
        val shuffledQueries = scala.util.Random.shuffle(generatedQueries)

        // Run generated queries on both Spark and Postgres, and test against each other.
        shuffledQueries.take(GeneratedSubquerySuite.NUM_QUERIES_PER_TEST).foreach { sqlStr =>
          if (!GeneratedSubquerySuite.KNOWN_QUERIES_WITH_DIFFERENT_RESULTS.contains(sqlStr)) {
            val stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
              ResultSet.CONCUR_READ_ONLY)
            val sparkDf = localSparkSession.sql(sqlStr)
            val postgresResult = getPostgresResult(stmt, sqlStr)
            QueryTest.checkAnswer(sparkDf, postgresResult.toSeq)
          }
        }
        conn.close()
      }
    }
  }

  generateQueriesAndRunTestCases()
}

object GeneratedSubquerySuite {

  // Limit number of generated queries per test so that tests will not take too long.
  private val NUM_QUERIES_PER_TEST = 1000

  private val KNOWN_QUERIES_WITH_DIFFERENT_RESULTS = Seq()
}
