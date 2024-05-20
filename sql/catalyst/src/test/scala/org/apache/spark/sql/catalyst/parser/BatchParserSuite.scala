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

package org.apache.spark.sql.catalyst.parser

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.exceptions.SqlBatchLangException

class BatchParserSuite extends SparkFunSuite with SQLHelper {
  import CatalystSqlParser._

  test("single select") {
    val batch = "SELECT 1;"
    val tree = parseBatch(batch)
    assert(tree.collection.length == 1)
    assert(tree.collection.head.isInstanceOf[SparkStatementWithPlan])
    val sparkStatement = tree.collection.head.asInstanceOf[SparkStatementWithPlan]
    assert(sparkStatement.getText(batch) == "SELECT 1;")
  }

  test("single select without ;") {
    val batch = "SELECT 1"
    val tree = parseBatch(batch)
    assert(tree.collection.length == 1)
    assert(tree.collection.head.isInstanceOf[SparkStatementWithPlan])
    val sparkStatement = tree.collection.head.asInstanceOf[SparkStatementWithPlan]
    assert(sparkStatement.getText(batch) == "SELECT 1")
  }

  test("multi select without ; - should fail") {
    val batch = "SELECT 1 SELECT 1"
    intercept[ParseException] {
      parseBatch(batch)
    }
  }

  test("multi select") {
    val batch = "BEGIN SELECT 1;SELECT 2; END"
    val tree = parseBatch(batch)
    assert(tree.collection.length == 2)
    assert(tree.collection.forall(_.isInstanceOf[SparkStatementWithPlan]))

    batch.split(";")
      .map(_.replace("\n", "").replace("BEGIN", "").replace("END", "").trim)
      .zip(tree.collection)
      .foreach { case (expected, statement) =>
        val sparkStatement = statement.asInstanceOf[SparkStatementWithPlan]
        val statementText = sparkStatement.getText(batch)
        assert(statementText == expected)
      }
  }

  test("multi statement") {
    val batch =
      """
        |BEGIN
        |  SELECT 1;
        |  SELECT 2;
        |  INSERT INTO A VALUES (a, b, 3);
        |  SELECT a, b, c FROM T;
        |  SELECT * FROM T;
        |END""".stripMargin
    val tree = parseBatch(batch)
    assert(tree.collection.length == 5)
    assert(tree.collection.forall(_.isInstanceOf[SparkStatementWithPlan]))
    batch.split(";")
      .map(_.replace("\n", "").replace("BEGIN", "").replace("END", "").trim)
      .zip(tree.collection)
      .foreach { case (expected, statement) =>
        val sparkStatement = statement.asInstanceOf[SparkStatementWithPlan]
        val statementText = sparkStatement.getText(batch)
        assert(statementText == expected)
      }
  }

  test("multi statement without ; at the end") {
    val batch =
      """
        |BEGIN
        |SELECT 1;
        |SELECT 2;
        |INSERT INTO A VALUES (a, b, 3);
        |SELECT a, b, c FROM T;
        |SELECT * FROM T
        |END""".stripMargin
    val tree = parseBatch(batch)
    assert(tree.collection.length == 5)
    assert(tree.collection.forall(_.isInstanceOf[SparkStatementWithPlan]))
    batch.split(";")
      .map(_.replace("\n", "").replace("BEGIN", "").replace("END", "").trim)
      .zip(tree.collection)
      .foreach { case (expected, statement) =>
        val sparkStatement = statement.asInstanceOf[SparkStatementWithPlan]
        val statementText = sparkStatement.getText(batch)
        assert(statementText == expected)
      }
  }

  test("nested begin end") {
    val batch =
      """
        |BEGIN
        |  BEGIN
        |  SELECT 1;
        |  END;
        |  BEGIN
        |    BEGIN
        |      SELECT 2;
        |      SELECT 3;
        |    END;
        |  END;
        |END""".stripMargin
    val tree = parseBatch(batch)
    assert(tree.collection.length == 2)
    assert(tree.collection.head.isInstanceOf[BatchBody])
    val body1 = tree.collection.head.asInstanceOf[BatchBody]
    assert(body1.collection.length == 1)
    assert(body1.collection.head.asInstanceOf[SparkStatementWithPlan].getText(batch) == "SELECT 1")

    val body2 = tree.collection(1).asInstanceOf[BatchBody]
    assert(body2.collection.length == 1)
    assert(body2.collection.head.isInstanceOf[BatchBody])
    val nestedBody = body2.collection.head.asInstanceOf[BatchBody]
    assert(
      nestedBody.collection.head.asInstanceOf[SparkStatementWithPlan].getText(batch) == "SELECT 2")
    assert(
      nestedBody.collection(1).asInstanceOf[SparkStatementWithPlan].getText(batch) == "SELECT 3")
  }

  test("variable declare and set") {
    val batch =
      """
        |BEGIN
        |DECLARE totalInsertCount = 0;
        |SET VAR totalInsertCount = totalInsertCount + 1;
        |END""".stripMargin
    val tree = parseBatch(batch)

    assert(tree.collection.length == 2)
    assert(tree.collection.head.isInstanceOf[SparkStatementWithPlan])
    assert(tree.collection(1).isInstanceOf[SparkStatementWithPlan])
  }

  test ("declare in compound top") {
    val batch =
      """
        |BEGIN
        |DECLARE totalInsertCount = 0;
        |SET VAR totalInsertCount = totalInsertCount + 1;
        |BEGIN
        |  DECLARE totalInsertCount2 = 0;
        |  SET VAR totalInsertCount2 = totalInsertCount2 + 1;
        |END
        |END""".stripMargin
    val _ = parseBatch(batch)
  }

  test("declare after compound top") {
    val batch =
      """
        |BEGIN
        |DECLARE totalInsertCount = 0;
        |SET VAR totalInsertCount = totalInsertCount + 1;
        |DECLARE totalInsertCount2 = 0;
        |END""".stripMargin
    val e = intercept[SqlBatchLangException] {
      parseBatch(batch)
    }
    assert(e.getErrorClass === "SQL_BATCH_LANG_INVALID_VARIABLE_DECLARATION.ONLY_AT_BEGINNING")
    assert(e.getMessage.contains("DECLARE totalInsertCount2 = 0;"))
  }
}
