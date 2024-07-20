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

import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class ParsingImprovementsSuite extends QueryTest with SharedSparkSession {

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
   * Create a table with the given data and format. Return the randomly generated table name.
   */
  private def createTable(format: String = "delta"): String = {
    val tableName = getRandomTableName()
    spark.sql(s"""
      CREATE TABLE $tableName (
        id INT,
        first_name VARCHAR(50),
        last_name VARCHAR(50),
        age INT,
        gender CHAR(1),
        email VARCHAR(100),
        phone_number VARCHAR(20),
        address VARCHAR(200),
        city VARCHAR(50),
        state VARCHAR(50),
        zip_code VARCHAR(10),
        country VARCHAR(50),
        registration_date String
      ) using $format;
    """)
    tableName
  }

  /**
   * Generate an INSERT INTO VALUES statement with basic literals with the given number of rows.
   */
  private def generateInsertStatementWithLiterals(tableName: String, numRows: Int): String = {
    val baseQuery = s"INSERT INTO $tableName (id, first_name, last_name, age, gender," +
      s" email, phone_number, address, city, state, zip_code, country, registration_date) VALUES "
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

    baseQuery + rows + ";"
  }

  /**
   * Checks if two tables are equal.
   */
  private def areTablesEqual(df1: DataFrame, df2: DataFrame): Boolean = {
    val df1ExceptDf2 = df1.except(df2).cache()
    val df2ExceptDf1 = df2.except(df1).cache()
    df1ExceptDf2.count() == 0 && df2ExceptDf1.count() == 0
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
    // Set the number of inserted rows to 10000.
    val rowCount = 10000
    var firstTableName: Option[String] = None
    Seq(true, false).foreach { insertIntoValueImprovementEnabled =>

      // Create a table with a randomly generated name.
      val tableName = createTable()

      // Set the feature flag for the InsertIntoValues improvement.
      withSQLConf(SQLConf.OPTIMIZE_INSERT_INTO_VALUES_PARSER.key ->
        insertIntoValueImprovementEnabled.toString) {

        // Generate an INSERT INTO VALUES statement.
        val sqlStatement = generateInsertStatementWithLiterals(tableName, rowCount)
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
            assert(areTablesEqual(df1, df2), "The tables should be equal.")
        }
      }
    }
  }

  test("Insert Into Values optimization - Basic literals & expressions.") {
    var firstTableName: Option[String] = None
    Seq(true, false).foreach { insertIntoValueImprovementEnabled =>
      // Create a table with a randomly generated name.
      val tableName = createTable()

      // Set the feature flag for the InsertIntoValues improvement.
      withSQLConf(SQLConf.OPTIMIZE_INSERT_INTO_VALUES_PARSER.key ->
        insertIntoValueImprovementEnabled.toString) {

        // Generate an INSERT INTO VALUES statement.
        val sqlStatement = generateInsertStatementsWithComplexExpressions(tableName)
        spark.sql(sqlStatement)

        // Check that both insertions will produce equivalent tables.
        if (firstTableName.isEmpty) {
          firstTableName = Some(tableName)
        } else {
            val df1 = spark.table(firstTableName.get)
            val df2 = spark.table(tableName)
            assert(areTablesEqual(df1, df2), "The tables should be equal.")
        }
      }
    }
  }
}
