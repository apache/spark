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

package org.apache.spark.sql.scripting

import org.apache.spark.SparkConf
import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.catalyst.plans.logical.CompoundBody
import org.apache.spark.sql.catalyst.util.QuotingUtils.toSQLConf
import org.apache.spark.sql.connector.catalog.{Aborted, Committed, Identifier, InMemoryRowLevelOperationTableCatalog, Txn, TxnTable, TxnTableCatalog}
import org.apache.spark.sql.exceptions.SqlScriptingException
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}


/**
 * End-to-end tests for SQL Scripting.
 * This suite is not intended to heavily test the SQL scripting (parser & interpreter) logic.
 * It is rather focused on testing the sql() API - whether it can handle SQL scripts correctly,
 *  results are returned in expected manner, config flags are applied properly, etc.
 * For full functionality tests, see SqlScriptingParserSuite and SqlScriptingInterpreterSuite.
 */
class SqlScriptingE2eSuite extends SharedSparkSession {

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    conf.setConf(SQLConf.SQL_SCRIPTING_CONTINUE_HANDLER_ENABLED, true)
  }

  protected override def afterAll(): Unit = {
    conf.unsetConf(SQLConf.SQL_SCRIPTING_CONTINUE_HANDLER_ENABLED.key)
    super.afterAll()
  }

  // Helpers
  private def withCatalog(
      name: String)(
      f: InMemoryRowLevelOperationTableCatalog => Unit): Unit = {
    withSQLConf(s"spark.sql.catalog.$name" ->
        classOf[InMemoryRowLevelOperationTableCatalog].getName) {
      val catalog = spark.sessionState.catalogManager
        .catalog(name)
        .asInstanceOf[InMemoryRowLevelOperationTableCatalog]
      try f(catalog) finally spark.sessionState.catalogManager.reset()
    }
  }

  private def loadTxnTable(
      txn: Txn,
      tableName: String,
      namespace: Array[String] = Array("ns1")): TxnTable =
    txn.catalog
      .asInstanceOf[TxnTableCatalog]
      .loadTable(Identifier.of(namespace, tableName))
      .asInstanceOf[TxnTable]

  private def verifySqlScriptResult(
      sqlText: String,
      expected: Seq[Row],
      expectedSchema: Option[StructType] = None): Unit = {
    val df = spark.sql(sqlText)
    checkAnswer(df, expected)

    assert(expectedSchema.forall(_ === df.schema))
  }

  private def verifySqlScriptResultWithNamedParams(
      sqlText: String,
      expected: Seq[Row],
      args: Map[String, Any]): Unit = {
    val df = spark.sql(sqlText, args)
    checkAnswer(df, expected)
  }

  // Tests setup
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(SQLConf.ANSI_ENABLED.key, "true")
  }

  // Tests
  test("SQL Scripting not enabled") {
    withSQLConf(SQLConf.SQL_SCRIPTING_ENABLED.key -> "false") {
      val sqlScriptText =
        """
          |BEGIN
          |  SELECT 1;
          |END""".stripMargin
      checkError(
        exception = intercept[SqlScriptingException] {
          spark.sql(sqlScriptText).asInstanceOf[CompoundBody]
        },
        condition = "UNSUPPORTED_FEATURE.SQL_SCRIPTING",
        parameters = Map("sqlScriptingEnabled" -> toSQLConf(SQLConf.SQL_SCRIPTING_ENABLED.key)))
    }
  }

  test("Scripting with exit exception handlers") {
    val sqlScript =
      """
        |BEGIN
        |  DECLARE flag INT = -1;
        |  DECLARE EXIT HANDLER FOR DIVIDE_BY_ZERO
        |  BEGIN
        |    SELECT flag;
        |    SET flag = 1;
        |  END;
        |  BEGIN
        |    DECLARE EXIT HANDLER FOR SQLSTATE '22012'
        |    BEGIN
        |      SELECT flag;
        |      SET flag = 2;
        |    END;
        |    SELECT 5;
        |    SELECT 1/0;
        |    SELECT 6;
        |  END;
        |  SELECT 7;
        |  SELECT flag;
        |END
        |""".stripMargin
    verifySqlScriptResult(sqlScript, Seq(Row(2)))
  }

  test("Scripting with continue exception handlers") {
    val sqlScript =
      """
        |BEGIN
        |  DECLARE flag1 INT = -1;
        |  DECLARE flag2 INT = -1;
        |  DECLARE CONTINUE HANDLER FOR DIVIDE_BY_ZERO
        |  BEGIN
        |    SELECT flag1;
        |    SET flag1 = 1;
        |  END;
        |  BEGIN
        |    DECLARE CONTINUE HANDLER FOR SQLSTATE '22012'
        |    BEGIN
        |      SELECT flag1;
        |      SET flag1 = 2;
        |    END;
        |    SELECT 5;
        |    SET flag2 = 1;
        |    SELECT 1/0;
        |    SELECT 6;
        |    SET flag2 = 2;
        |  END;
        |  SELECT 7;
        |  SELECT flag1, flag2;
        |END
        |""".stripMargin
    verifySqlScriptResult(sqlScript, Seq(Row(2, 2)))
  }

  test("single select") {
    val sqlText = "SELECT 1;"
    verifySqlScriptResult(sqlText, Seq(Row(1)))
  }

  test("multiple selects") {
    val sqlText =
      """
        |BEGIN
        |  SELECT 1;
        |  SELECT 2;
        |END""".stripMargin
    verifySqlScriptResult(sqlText, Seq(Row(2)))
  }

  test("multi statement - simple") {
    withTable("t") {
      val sqlScript =
        """
          |BEGIN
          |  CREATE TABLE t (a INT, b STRING, c DOUBLE) USING parquet;
          |  INSERT INTO t VALUES (1, 'a', 1.0);
          |  SELECT a FROM t;
          |END
          |""".stripMargin
      verifySqlScriptResult(sqlScript, Seq(Row(1)))
    }
  }

  test("multi statement with transactional checks - insert then delete") {
    withCatalog("cat") { catalog =>
      withTable("cat.ns1.t") {
        val sqlScript =
          """
            |BEGIN
            |  CREATE TABLE cat.ns1.t (pk INT NOT NULL, salary INT, dep STRING)
            |    PARTITIONED BY (dep);
            |  INSERT INTO cat.ns1.t VALUES (1, 100, 'hr'), (2, 200, 'software');
            |  DELETE FROM cat.ns1.t
            |    WHERE pk IN (SELECT pk FROM cat.ns1.t WHERE dep = 'hr');
            |  SELECT * FROM cat.ns1.t;
            |END
            |""".stripMargin

        verifySqlScriptResult(sqlScript, Seq(Row(2, 200, "software")))

        // Each DML statement in a script runs in its own independent QE and transaction.
        assert(catalog.observedTransactions.size === 2)
        assert(catalog.observedTransactions.forall(t =>
          t.currentState === Committed && t.isClosed))

        // The DELETE subquery scans the table with a dep='hr' predicate; verify it was tracked.
        val deleteTxnTable = loadTxnTable(catalog.observedTransactions(1), "t")
        assert(deleteTxnTable.scanEvents.flatten.exists {
          case sources.EqualTo("dep", "hr") => true
          case _ => false
        })
      }
    }
  }

  test("multi statement with transactional checks - second statement fails") {
    withCatalog("cat") { catalog =>
      withTable("cat.ns1.t") {
        val sqlScript =
          """
            |BEGIN
            |  CREATE TABLE cat.ns1.t (pk INT NOT NULL, salary INT, dep STRING)
            |    PARTITIONED BY (dep);
            |  INSERT INTO cat.ns1.t VALUES (1, 100, 'hr'), (2, 200, 'software');
            |  DELETE FROM cat.ns1.t WHERE nonexistent_column = 1;
            |END
            |""".stripMargin

        checkError(
          exception = intercept[AnalysisException] {
            spark.sql(sqlScript).collect()
          },
          condition = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
          parameters = Map(
            "objectName" -> "`nonexistent_column`",
            "proposal" -> ".*"),
          matchPVals = true,
          queryContext = Array(ExpectedContext("nonexistent_column")))

        // INSERT committed; DELETE was aborted because analysis failed on the bad column.
        assert(catalog.observedTransactions.size === 2)
        assert(catalog.observedTransactions(0).currentState === Committed)
        assert(catalog.observedTransactions(0).isClosed)
        assert(catalog.observedTransactions(1).currentState === Aborted)
        assert(catalog.observedTransactions(1).isClosed)
        assert(catalog.lastTransaction.currentState === Aborted)
      }
    }
  }

  test("multi statement with transactional checks - insert, merge, update") {
    withCatalog("cat") { catalog =>
      withTable("cat.ns1.t", "cat.ns1.src") {
        val sqlScript =
          """
            |BEGIN
            |  CREATE TABLE cat.ns1.t (pk INT NOT NULL, salary INT, dep STRING)
            |    PARTITIONED BY (dep);
            |  CREATE TABLE cat.ns1.src (pk INT NOT NULL, salary INT, dep STRING)
            |    PARTITIONED BY (dep);
            |  INSERT INTO cat.ns1.t VALUES (1, 100, 'hr'), (2, 200, 'software'), (3, 300, 'hr');
            |  INSERT INTO cat.ns1.src VALUES (1, 150, 'hr'), (4, 400, 'finance');
            |  MERGE INTO cat.ns1.t AS t
            |    USING cat.ns1.src AS s
            |    ON t.pk = s.pk
            |    WHEN MATCHED THEN UPDATE SET salary = s.salary
            |    WHEN NOT MATCHED THEN INSERT (pk, salary, dep)
            |      VALUES (s.pk, s.salary, s.dep);
            |  UPDATE cat.ns1.t SET salary = salary + 50 WHERE dep = 'software';
            |  SELECT * FROM cat.ns1.t ORDER BY pk;
            |END
            |""".stripMargin

        verifySqlScriptResult(
          sqlScript,
          Seq(
            Row(1, 150, "hr"),
            Row(2, 250, "software"),
            Row(3, 300, "hr"),
            Row(4, 400, "finance")))

        // INSERT (x2), MERGE, and UPDATE each run in their own independent QE and transaction.
        assert(catalog.observedTransactions.size === 4)
        assert(catalog.observedTransactions.forall(t => t.currentState === Committed && t.isClosed))

        def txnTable(txnIdx: Int): TxnTable =
          loadTxnTable(catalog.observedTransactions(txnIdx), "t")

        // Both inserts are pure writes - no scan.
        assert(txnTable(0).scanEvents.isEmpty)
        assert(txnTable(1).scanEvents.isEmpty)

        // MERGE scans the full target table. The join is on pk (not the partition column).
        assert(txnTable(2).scanEvents.nonEmpty)
        assert(txnTable(2).scanEvents.flatten.isEmpty)

        // UPDATE with WHERE dep='software' pushes an equality predicate on the partition column.
        assert(txnTable(3).scanEvents.flatten.exists {
          case sources.EqualTo("dep", "software") => true
          case _ => false
        })
      }
    }
  }

  test("loop with transactional checks - each iteration runs in its own transaction") {
    withCatalog("cat") { catalog =>
      withTable("cat.ns1.t") {
        val sqlScript =
          """
            |BEGIN
            |  DECLARE i INT = 1;
            |  CREATE TABLE
            |    cat.ns1.t (pk INT NOT NULL, salary INT, dep STRING)
            |    PARTITIONED BY (dep);
            |  WHILE i <= 3 DO
            |    INSERT INTO cat.ns1.t VALUES (i, i * 100, 'hr');
            |    SET i = i + 1;
            |  END WHILE;
            |  SELECT * FROM cat.ns1.t ORDER BY pk;
            |END
            |""".stripMargin

        verifySqlScriptResult(
          sqlScript,
          Seq(Row(1, 100, "hr"), Row(2, 200, "hr"), Row(3, 300, "hr")))

        // Each loop iteration's INSERT runs in its own independent transaction.
        assert(catalog.observedTransactions.size === 3)
        assert(catalog.observedTransactions.forall(t => t.currentState === Committed && t.isClosed))
      }
    }
  }

  test("continue handler with transactional checks - handler DML runs in its own transaction") {
    withCatalog("cat") { catalog =>
      withTable("cat.ns1.t") {
        val sqlScript =
          """
            |BEGIN
            |  DECLARE CONTINUE HANDLER FOR DIVIDE_BY_ZERO
            |  BEGIN
            |    INSERT INTO cat.ns1.t VALUES (-1, -1, 'error');
            |  END;
            |  CREATE TABLE
            |    cat.ns1.t (pk INT NOT NULL, salary INT, dep STRING)
            |    PARTITIONED BY (dep);
            |  INSERT INTO cat.ns1.t VALUES (1, 100, 'hr');
            |  SELECT 1/0;
            |  INSERT INTO cat.ns1.t VALUES (2, 200, 'software');
            |  SELECT * FROM cat.ns1.t ORDER BY pk;
            |END
            |""".stripMargin

        verifySqlScriptResult(
          sqlScript,
          Seq(Row(-1, -1, "error"), Row(1, 100, "hr"), Row(2, 200, "software")))

        // INSERT(1), handler INSERT(-1), INSERT(2) - each in its own transaction.
        assert(catalog.observedTransactions.size === 3)
        assert(catalog.observedTransactions.forall(t => t.currentState === Committed && t.isClosed))
      }
    }
  }

  test("script without result statement") {
    val sqlScript =
      """
        |BEGIN
        |  DECLARE x INT;
        |  SET x = 1;
        |END
        |""".stripMargin
    verifySqlScriptResult(sqlScript, Seq.empty)
  }

  test("SPARK-51284: script with empty result") {
    withTable("scripting_test_table") {
      val sqlScript =
        """
          |BEGIN
          |  CREATE TABLE scripting_test_table (id INT);
          |  SELECT * FROM scripting_test_table;
          |  DROP TABLE scripting_test_table;
          |END
          |""".stripMargin
      verifySqlScriptResult(
        sqlScript,
        Seq.empty,
        Some(StructType(Seq(StructField("id", IntegerType)).toArray))
      )
    }
  }

  test("empty script") {
    val sqlScript =
      """
        |BEGIN
        |END
        |""".stripMargin
    verifySqlScriptResult(sqlScript, Seq.empty)
  }

  test("named params") {
    val sqlScriptText =
      """
        |BEGIN
        |  SELECT 1;
        |  IF :param_1 > 10 THEN
        |    SELECT :param_2;
        |  ELSE
        |    SELECT :param_3;
        |  END IF;
        |END""".stripMargin
    // Define a map with SQL parameters
    val args: Map[String, Any] = Map(
      "param_1" -> 5,
      "param_2" -> "greater",
      "param_3" -> "smaller"
    )
    verifySqlScriptResultWithNamedParams(sqlScriptText, Seq(Row("smaller")), args)
  }

  test("positional params") {
    val sqlScriptText =
      """
        |BEGIN
        |  SELECT 1;
        |  IF ? > 10 THEN
        |    SELECT ?;
        |  ELSE
        |    SELECT ?;
        |  END IF;
        |END""".stripMargin
    // Define an array with SQL parameters in the correct order.
    val args: Array[Any] = Array(5, "greater", "smaller")
    checkError(
      exception = intercept[SqlScriptingException] {
        spark.sql(sqlScriptText, args).asInstanceOf[CompoundBody]
      },
      condition = "UNSUPPORTED_FEATURE.SQL_SCRIPTING_WITH_POSITIONAL_PARAMETERS",
      parameters = Map.empty)
  }

  test("named params with positional params - should fail") {
    val sqlScriptText =
      """
        |BEGIN
        |  SELECT ?;
        |  IF :param > 10 THEN
        |    SELECT 1;
        |  ELSE
        |    SELECT 2;
        |  END IF;
        |END""".stripMargin
    // Define a map with SQL parameters.
    val args: Map[String, Any] = Map("param" -> 5)
    checkError(
      exception = intercept[AnalysisException] {
        spark.sql(sqlScriptText, args).asInstanceOf[CompoundBody]
      },
      condition = "INVALID_QUERY_MIXED_QUERY_PARAMETERS",
      parameters = Map())
  }

  test("SQL Script labels with identifier") {
    val sqlScript =
      """
        |BEGIN
        |  IDENTIFIER('loop_label'): LOOP
        |    SELECT 1;
        |    LEAVE IDENTIFIER('loop_label');
        |  END LOOP IDENTIFIER('loop_label');
        |END""".stripMargin
    verifySqlScriptResult(sqlScript, Seq(Row(1)))
  }

  test("SQL Script with labeled BEGIN/END block using identifier") {
    val sqlScript =
      """
        |BEGIN
        |  IDENTIFIER('block_label'): BEGIN
        |    DECLARE IDENTIFIER('x') INT DEFAULT 1;
        |    SELECT x;
        |  END IDENTIFIER('block_label');
        |END""".stripMargin
    verifySqlScriptResult(sqlScript, Seq(Row(1)))
  }

  test("WHILE loop with identifier label") {
    val sqlScript =
      """
        |BEGIN
        |  DECLARE counter INT DEFAULT 0;
        |  IDENTIFIER('while_label'): WHILE counter < 3 DO
        |    SET IDENTIFIER('counter') = counter + 1;
        |  END WHILE IDENTIFIER('while_label');
        |  SELECT counter;
        |END""".stripMargin
    verifySqlScriptResult(sqlScript, Seq(Row(3)))
  }

  test("REPEAT loop with identifier label") {
    val sqlScript =
      """
        |BEGIN
        |  DECLARE cnt INT DEFAULT 0;
        |  repeat_label: REPEAT
        |    SET cnt = cnt + 1;
        |  UNTIL cnt >= 2
        |  END REPEAT IDENTIFIER('repeat_label');
        |  SELECT cnt;
        |END""".stripMargin
    verifySqlScriptResult(sqlScript, Seq(Row(2)))
  }

  test("FOR loop with identifier") {
    val sqlScript =
      """
        |BEGIN
        |  IDENTIFIER('for_label'): FOR IDENTIFIER('row') AS SELECT 1 AS c1 DO
        |    SELECT row.c1;
        |  END FOR for_label;
        |END""".stripMargin
    verifySqlScriptResult(sqlScript, Seq(Row(1)))
  }
}
