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

import java.util.UUID

import org.apache.spark.sql.catalyst.analysis.UnresolvedInlineTable
import org.apache.spark.sql.catalyst.expressions.{Expression, ScalarSubquery}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, Project}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class InlineTableParsingImprovementsSuite extends QueryTest with SharedSparkSession {

  /**
   * SQL parser.
   */
  private lazy val parser = spark.sessionState.sqlParser

  /**
   * Generate a random table name.
   */
  private def getRandomTableName(): String =
    s"test_${UUID.randomUUID()}".replaceAll("-", "_")

  /**
   * Create a table using a randomly generated name and return that name.
   */
  private def createTable: String = {
    val tableName = getRandomTableName()
    spark.sql(s"""
      CREATE TABLE $tableName (
        id INT,
        first_name VARCHAR(50) DEFAULT 'John',
        last_name VARCHAR(50) DEFAULT 'Doe',
        age INT DEFAULT 25,
        gender CHAR(1) DEFAULT 'M',
        email VARCHAR(100) DEFAULT 'john.doe@databricks.com',
        phone_number VARCHAR(20) DEFAULT '555-555-5555',
        address VARCHAR(200) DEFAULT '123 John Doe St',
        city VARCHAR(50) DEFAULT 'John Doe City',
        state VARCHAR(50) DEFAULT 'CA',
        zip_code VARCHAR(10) DEFAULT '12345',
        country VARCHAR(50) DEFAULT 'USA',
        registration_date String DEFAULT '2021-01-01')
    """)
    tableName
  }

  /**
   * Generate an INSERT INTO VALUES statement with basic literals with the given number of rows.
   */
  private def generateInsertStatementWithLiterals(tableName: String, numRows: Int): String = {
    val baseQuery = s"INSERT INTO $tableName (id, first_name, last_name, age, gender," +
      s" email, phone_number, address, city, state, zip_code, country, registration_date) "
    baseQuery + generateValuesWithLiterals(numRows) + ";"
  }

  /**
   * Generate a VALUES clause with the given number of rows using basic literals.
   */
  private def generateValuesWithLiterals(numRows: Int = 10): String = {
    val rows = (1 to numRows).map { i =>
      val id = i
      val firstName = s"'FirstName_$id'"
      val lastName = s"'LastName_$id'"
      val age = (20 + i % 50) // Just a simple pattern for age
      val gender = if (i % 2 == 0) "'M'" else "'F'"
      val email = s"'user_$id@example.com'"
      val phoneNumber = s"'555-${1000 + i}'"
      val address = s"'$id Fake St'"
      val city = "'Anytown'"
      val state = "'CA'"
      val zipCode = "'12345'"
      val country = "'USA'"
      val registrationDate = s"'2021-${1 + i % 12}-01'" // Varying the month part of the date

      s"($id, $firstName, $lastName, $age, $gender, $email, $phoneNumber," +
        s" $address, $city, $state, $zipCode, $country, $registrationDate)"
    }.mkString(",\n")

    s" VALUES $rows"
  }

  /**
   * Traverse the plan and check for the presence of the given node type.
   */
  private def traversePlanAndCheckForNodeType[T <: LogicalPlan](
      plan: LogicalPlan, nodeType: Class[T]): Boolean = plan match {
    case node if nodeType.isInstance(node) => true
    case n: Project =>
      // If the plan node is a Project, we need to check the expressions in the project list
      // and the child nodes.
      n.projectList.exists(traverseExpressionAndCheckForNodeType(_, nodeType)) ||
      n.children.exists(traversePlanAndCheckForNodeType(_, nodeType))
    case node if node.children.isEmpty => false
    case _ => plan.children.exists(traversePlanAndCheckForNodeType(_, nodeType))
  }

  /**
   * Traverse the expression and check for the presence of the given node type.
   */
  private def traverseExpressionAndCheckForNodeType[T <: LogicalPlan](
        expression: Expression, nodeType: Class[T]): Boolean = expression match {
    case scalarSubquery: ScalarSubquery => scalarSubquery.plan.exists(
      traversePlanAndCheckForNodeType(_, nodeType))
    case _ =>
      expression.children.exists(traverseExpressionAndCheckForNodeType(_, nodeType))
  }

  /**
   * Generate an INSERT INTO VALUES statement with both literals and expressions.
   */
  private def generateInsertStatementsWithComplexExpressions(
      tableName: String): String = {
    s"""
      INSERT INTO $tableName (id, first_name, last_name, age, gender,
        email, phone_number, address, city, state, zip_code, country, registration_date) VALUES
      (1, base64('FirstName_1'), base64('LastName_1'), 10+10, 'M', 'usr' || '@gmail.com',
        concat('555','-1234'), hex('123 Fake St'), 'Anytown', 'CA', '12345', 'USA',
        '2021-01-01'),

      (2, 'FirstName_2', string(5), abs(-8), 'F', 'usr@gmail.com', '555-1234', '123 Fake St',
        concat('Anytown', 'sada'), 'CA', '12345', 'USA', '2021-01-01'),

      (3, 'FirstName_3', 'LastName_3', 34::int, 'M', 'usr@gmail.com', '555-1234',
        '123 Fake St', 'Anytown', 'CA', '12345', 'USA', '2021-01-01'),

      (4, left('FirstName_4', 5), upper('LastName_4'), acos(1), 'F', 'user@gmail.com',
        '555-1234', '123 Fake St', 'Anytown', 'CA', '12345', 'USA', '2021-01-01');
    """
  }
  test("Insert Into Values optimization - Basic literals.") {
    // Set the number of inserted rows to 10.
    val rowCount = 10
    var firstTableName: Option[String] = None
    Seq(true, false).foreach { eagerEvalOfUnresolvedInlineTableEnabled =>

      // Create a table with a randomly generated name.
      val tableName = createTable

      // Set the feature flag for the InsertIntoValues improvement.
      withSQLConf(SQLConf.EAGER_EVAL_OF_UNRESOLVED_INLINE_TABLE_ENABLED.key ->
        eagerEvalOfUnresolvedInlineTableEnabled.toString) {

        // Generate an INSERT INTO VALUES statement.
        val sqlStatement = generateInsertStatementWithLiterals(tableName, rowCount)

        // Parse the SQL statement.
        val plan = parser.parsePlan(sqlStatement)

        // Traverse the plan and check for the presence of appropriate nodes depending on the
        // feature flag.
        if (eagerEvalOfUnresolvedInlineTableEnabled) {
          assert(traversePlanAndCheckForNodeType(plan, classOf[LocalRelation]))
        } else {
          assert(traversePlanAndCheckForNodeType(plan, classOf[UnresolvedInlineTable]))
        }

        spark.sql(sqlStatement)

        // Double check that the insertion was successful.
        val countStar = spark.sql(s"SELECT count(*) FROM $tableName").collect()
        assert(countStar.head.getLong(0) == rowCount,
          "The number of rows in the table should match the number of rows inserted.")

        // Check that both insertions will produce equivalent tables.
        if (firstTableName.isEmpty) {
          firstTableName = Some(tableName)
        } else {
          val df1 = spark.table(firstTableName.get)
          val df2 = spark.table(tableName)
          checkAnswer(df1, df2)
        }
      }
    }
  }

  test("Insert Into Values optimization - Basic literals & expressions.") {
    var firstTableName: Option[String] = None
    Seq(true, false).foreach { eagerEvalOfUnresolvedInlineTableEnabled =>
      // Create a table with a randomly generated name.
      val tableName = createTable

      // Set the feature flag for the InsertIntoValues improvement.
      withSQLConf(SQLConf.EAGER_EVAL_OF_UNRESOLVED_INLINE_TABLE_ENABLED.key ->
        eagerEvalOfUnresolvedInlineTableEnabled.toString) {

        // Generate an INSERT INTO VALUES statement.
        val sqlStatement = generateInsertStatementsWithComplexExpressions(tableName)

        // Parse the SQL statement.
        val plan = parser.parsePlan(sqlStatement)

        // Traverse the plan and check for the presence of appropriate nodes.
        // In this case, the plan should always contain a UnresolvedInlineTable node
        // because the expressions are not eagerly resolved, therefore
        // `plan.expressionsResolved` in `EvaluateUnresolvedInlineTable.evaluate` will
        // always be false.
        assert(traversePlanAndCheckForNodeType(plan, classOf[UnresolvedInlineTable]))

        spark.sql(sqlStatement)

        // Check that both insertions will produce equivalent tables.
        if (firstTableName.isEmpty) {
          firstTableName = Some(tableName)
        } else {
            val df1 = spark.table(firstTableName.get)
            val df2 = spark.table(tableName)
            checkAnswer(df1, df2)
        }
      }
    }
  }

  test("Insert Into Values with defaults.") {
    var firstTableName: Option[String] = None
    Seq(true, false).foreach { eagerEvalOfUnresolvedInlineTableEnabled =>
      // Create a table with default values specified.
      val tableName = createTable

      // Set the feature flag for the InsertIntoValues improvement.
      withSQLConf(SQLConf.EAGER_EVAL_OF_UNRESOLVED_INLINE_TABLE_ENABLED.key ->
        eagerEvalOfUnresolvedInlineTableEnabled.toString) {

        // Generate an INSERT INTO VALUES statement that omits all columns
        // containing a DEFAULT value.
        val sqlStatement = s"INSERT INTO $tableName (id) VALUES (1);"

        // Parse the SQL statement.
        val plan = parser.parsePlan(sqlStatement)

        // Traverse the plan and check for the presence of appropriate nodes depending on the
        // feature flag.
        if (eagerEvalOfUnresolvedInlineTableEnabled) {
          assert(traversePlanAndCheckForNodeType(plan, classOf[LocalRelation]))
        } else {
          assert(traversePlanAndCheckForNodeType(plan, classOf[UnresolvedInlineTable]))
        }

        spark.sql(sqlStatement)

        // Verify that the default values are applied correctly.
        val resultRow = spark.sql(
          s"""
        SELECT
          first_name,
          last_name,
          gender,
          email,
          phone_number,
          address,
          city,
          state,
          zip_code,
          country,
          registration_date
        FROM $tableName WHERE id = 1""").collect()

        // Checking that the default values are applied correctly.
        assert(resultRow.head.getString(0) == "John", "Default name should be 'John'")
        assert(resultRow.head.getString(1) == "Doe", "Default last name should be 'Doe'")
        assert(resultRow.head.getString(2) == "M", "Default gender should be 'M'")
        assert(resultRow.head.getString(3) == "john.doe@databricks.com",
          "Default email should be 'john.doe@databricks.com'")
        assert(resultRow.head.getString(4) == "555-555-5555",
          "Default phone number should be '555-555-5555'")
        assert(resultRow.head.getString(5) == "123 John Doe St",
          "Default address should be '123 John Doe St'")
        assert(resultRow.head.getString(6) == "John Doe City",
          "Default city should be 'John Doe City'")
        assert(resultRow.head.getString(7) == "CA", "Default state should be 'CA'")
        assert(resultRow.head.getString(8) == "12345", "Default zip code should be '12345'")
        assert(resultRow.head.getString(9) == "USA", "Default country should be 'USA'")
        assert(resultRow.head.getString(10) == "2021-01-01",
          "Default registration date should be '2021-01-01'")

        // Check that both insertions will produce equivalent tables.
        if (firstTableName.isEmpty) {
          firstTableName = Some(tableName)
        } else {
          val df1 = spark.table(firstTableName.get)
          val df2 = spark.table(tableName)
          checkAnswer(df1, df2)
        }
      }
    }
  }

  test("SPARK-49269: Value list in subquery") {
    var firstDF: Option[DataFrame] = None
    val flagVals = Seq(true, false)
    flagVals.foreach { eagerEvalOfUnresolvedInlineTableEnabled =>
      // Set the feature flag for the InsertIntoValues improvement.
      withSQLConf(SQLConf.EAGER_EVAL_OF_UNRESOLVED_INLINE_TABLE_ENABLED.key ->
        eagerEvalOfUnresolvedInlineTableEnabled.toString) {

        // Generate a subquery with a VALUES clause.
        val sqlStatement = s"SELECT * FROM (${generateValuesWithLiterals()});"

        // Parse the SQL statement.
        val plan = parser.parsePlan(sqlStatement)

        // Traverse the plan and check for the presence of appropriate nodes depending on the
        // feature flag.
        if (eagerEvalOfUnresolvedInlineTableEnabled) {
          assert(traversePlanAndCheckForNodeType(plan, classOf[LocalRelation]))
        } else {
          assert(traversePlanAndCheckForNodeType(plan, classOf[UnresolvedInlineTable]))
        }

        val res = spark.sql(sqlStatement)

        // Check that both insertions will produce equivalent tables.
        if (flagVals.head == eagerEvalOfUnresolvedInlineTableEnabled) {
          firstDF = Some(res)
        } else {
          checkAnswer(res, firstDF.get)
        }
      }
    }
  }

  test("SPARK-49269: Value list in projection list subquery") {
    var firstDF: Option[DataFrame] = None
    val flagVals = Seq(true, false)
    flagVals.foreach { eagerEvalOfUnresolvedInlineTableEnabled =>
      // Set the feature flag for the InsertIntoValues improvement.
      withSQLConf(SQLConf.EAGER_EVAL_OF_UNRESOLVED_INLINE_TABLE_ENABLED.key ->
        eagerEvalOfUnresolvedInlineTableEnabled.toString) {

        // Generate a subquery with a VALUES clause in the projection list.
        val sqlStatement = s"SELECT (SELECT COUNT(*) FROM ${generateValuesWithLiterals()});"

        // Parse the SQL statement.
        val plan = parser.parsePlan(sqlStatement)

        // Traverse the plan and check for the presence of appropriate nodes depending on the
        // feature flag.
        if (eagerEvalOfUnresolvedInlineTableEnabled) {
          assert(traversePlanAndCheckForNodeType(plan, classOf[LocalRelation]))
        } else {
          assert(traversePlanAndCheckForNodeType(plan, classOf[UnresolvedInlineTable]))
        }

        val res = spark.sql(sqlStatement)

        // Check that both insertions will produce equivalent tables.
        if (flagVals.head == eagerEvalOfUnresolvedInlineTableEnabled) {
          firstDF = Some(res)
        } else {
          checkAnswer(res, firstDF.get)
        }
      }
    }
  }
}
