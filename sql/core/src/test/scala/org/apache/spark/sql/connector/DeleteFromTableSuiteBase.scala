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

package org.apache.spark.sql.connector

import java.util.Collections

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.{AnalysisException, DataFrame, Encoders, QueryTest, Row}
import org.apache.spark.sql.connector.catalog.{Identifier, InMemoryRowLevelOperationTable, InMemoryRowLevelOperationTableCatalog}
import org.apache.spark.sql.connector.expressions.LogicalExpressions._
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.v2.{DeleteFromTableExec, ReplaceDataExec}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.QueryExecutionListener

abstract class DeleteFromTableSuiteBase
  extends QueryTest with SharedSparkSession with BeforeAndAfter with AdaptiveSparkPlanHelper {

  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
  import testImplicits._

  before {
    spark.conf.set("spark.sql.catalog.cat", classOf[InMemoryRowLevelOperationTableCatalog].getName)
  }

  after {
    spark.sessionState.catalogManager.reset()
    spark.sessionState.conf.unsetConf("spark.sql.catalog.cat")
  }

  protected val namespace: Array[String] = Array("ns1")
  protected val ident: Identifier = Identifier.of(namespace, "test_table")
  protected val tableNameAsString: String = "cat." + ident.toString

  protected def catalog: InMemoryRowLevelOperationTableCatalog = {
    val catalog = spark.sessionState.catalogManager.catalog("cat")
    catalog.asTableCatalog.asInstanceOf[InMemoryRowLevelOperationTableCatalog]
  }

  protected def table: InMemoryRowLevelOperationTable = {
    catalog.loadTable(ident).asInstanceOf[InMemoryRowLevelOperationTable]
  }

  test("EXPLAIN only delete") {
    createAndInitTable("id INT, dep STRING", """{ "id": 1, "dep": "hr" }""")

    sql(s"EXPLAIN DELETE FROM $tableNameAsString WHERE id <= 10")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(1, "hr") :: Nil)
  }

  test("delete from empty tables") {
    createTable("id INT, dep STRING")

    sql(s"DELETE FROM $tableNameAsString WHERE id <= 1")

    checkAnswer(sql(s"SELECT * FROM $tableNameAsString"), Nil)
  }

  test("delete with basic filters") {
    createAndInitTable("id INT, dep STRING",
      """{ "id": 1, "dep": "hr" }
        |{ "id": 2, "dep": "software" }
        |{ "id": 3, "dep": "hr" }
        |""".stripMargin)

    sql(s"DELETE FROM $tableNameAsString WHERE id <= 1")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(2, "software") :: Row(3, "hr") :: Nil)
  }

  test("delete with aliases") {
    createAndInitTable("id INT, dep STRING",
      """{ "id": 1, "dep": "hr" }
        |{ "id": 2, "dep": "software" }
        |{ "id": 3, "dep": "hr" }
        |""".stripMargin)

    sql(s"DELETE FROM $tableNameAsString AS t WHERE t.id <= 1 OR t.dep = 'hr'")

    checkAnswer(sql(s"SELECT * FROM $tableNameAsString"), Row(2, "software") :: Nil)
  }

  test("delete with IN predicates") {
    createAndInitTable("id INT, dep STRING",
      """{ "id": 1, "dep": "hr" }
        |{ "id": 2, "dep": "software" }
        |{ "id": null, "dep": "hr" }
        |""".stripMargin)

    sql(s"DELETE FROM $tableNameAsString WHERE id IN (1, null)")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(2, "software") :: Row(null, "hr") :: Nil)
  }

  test("delete with NOT IN predicates") {
    createAndInitTable("id INT, dep STRING",
      """{ "id": 1, "dep": "hr" }
        |{ "id": 2, "dep": "software" }
        |{ "id": null, "dep": "hr" }
        |""".stripMargin)

    sql(s"DELETE FROM $tableNameAsString WHERE id NOT IN (null, 1)")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(1, "hr") :: Row(2, "software") :: Row(null, "hr") :: Nil)

    sql(s"DELETE FROM $tableNameAsString WHERE id NOT IN (1, 10)")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(1, "hr") :: Row(null, "hr") :: Nil)
  }

  test("delete with conditions on nested columns") {
    createAndInitTable("id INT, complex STRUCT<c1:INT,c2:STRING>, dep STRING",
      """{ "id": 1, "complex": { "c1": 3, "c2": "v1" }, "dep": "hr" }
        |{ "id": 2, "complex": { "c1": 2, "c2": "v2" }, "dep": "software" }
        |""".stripMargin)

    sql(s"DELETE FROM $tableNameAsString WHERE complex.c1 = id + 2")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(2, Row(2, "v2"), "software") :: Nil)

    sql(s"DELETE FROM $tableNameAsString t WHERE t.complex.c1 = id")

    checkAnswer(sql(s"SELECT * FROM $tableNameAsString"), Nil)
  }

  test("delete with IN subqueries") {
    withTempView("deleted_id", "deleted_dep") {
      createAndInitTable("id INT, dep STRING",
        """{ "id": 1, "dep": "hr" }
          |{ "id": 2, "dep": "hardware" }
          |{ "id": null, "dep": "hr" }
          |""".stripMargin)

      val deletedIdDF = Seq(Some(0), Some(1), None).toDF()
      deletedIdDF.createOrReplaceTempView("deleted_id")

      val deletedDepDF = Seq("software", "hr").toDF()
      deletedDepDF.createOrReplaceTempView("deleted_dep")

      sql(
        s"""DELETE FROM $tableNameAsString
           |WHERE
           | id IN (SELECT * FROM deleted_id)
           | AND
           | dep IN (SELECT * FROM deleted_dep)
           |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Row(2, "hardware") :: Row(null, "hr") :: Nil)

      append("id INT, dep STRING",
        """{ "id": 1, "dep": "hr" }
          |{ "id": -1, "dep": "hr" }
          |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Row(-1, "hr") :: Row(1, "hr") :: Row(2, "hardware") :: Row(null, "hr") :: Nil)

      sql(
        s"""DELETE FROM $tableNameAsString
           |WHERE
           | id IS NULL
           | OR
           | id IN (SELECT value + 2 FROM deleted_id)
           |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Row(-1, "hr") :: Row(1, "hr") :: Nil)

      append("id INT, dep STRING",
        """{ "id": null, "dep": "hr" }
          |{ "id": 2, "dep": "hr" }
          |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Row(-1, "hr") :: Row(1, "hr") :: Row(2, "hr") :: Row(null, "hr") :: Nil)

      sql(
        s"""DELETE FROM $tableNameAsString
           |WHERE
           | id IN (SELECT value + 2 FROM deleted_id)
           | AND
           | dep = 'hr'
           |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Row(-1, "hr") :: Row(1, "hr") :: Row(null, "hr") :: Nil)
    }
  }

  test("delete with multi-column IN subqueries") {
    withTempView("deleted_employee") {
      createAndInitTable("id INT, dep STRING",
        """{ "id": 1, "dep": "hr" }
          |{ "id": 2, "dep": "hardware" }
          |{ "id": null, "dep": "hr" }
          |""".stripMargin)

      val deletedEmployeeDF = Seq((None, "hr"), (Some(1), "hr")).toDF()
      deletedEmployeeDF.createOrReplaceTempView("deleted_employee")

      sql(
        s"""DELETE FROM $tableNameAsString
           |WHERE
           | (id, dep) IN (SELECT * FROM deleted_employee)
           |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Row(2, "hardware") :: Row(null, "hr") :: Nil)
    }
  }

  test("delete with NOT IN subqueries") {
    withTempView("deleted_id", "deleted_dep") {
      createAndInitTable("id INT, dep STRING",
        """{ "id": 1, "dep": "hr" }
          |{ "id": 2, "dep": "hardware" }
          |{ "id": null, "dep": "hr" }
          |""".stripMargin)

      val deletedIdDF = Seq(Some(-1), Some(-2), None).toDF()
      deletedIdDF.createOrReplaceTempView("deleted_id")

      val deletedDepDF = Seq("software", "hr").toDF()
      deletedDepDF.createOrReplaceTempView("deleted_dep")

      sql(
        s"""DELETE FROM $tableNameAsString
           |WHERE
           | id NOT IN (SELECT * FROM deleted_id)
           |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Row(1, "hr") :: Row(2, "hardware") :: Row(null, "hr") :: Nil)

      sql(
        s"""DELETE FROM $tableNameAsString
           |WHERE
           | id NOT IN (SELECT * FROM deleted_id WHERE value IS NOT NULL)
           |""".stripMargin)

      checkAnswer(sql(s"SELECT * FROM $tableNameAsString"), Row(null, "hr") :: Nil)

      append("id INT, dep STRING",
        """{ "id": 1, "dep": "hr" }
          |{ "id": 2, "dep": "hardware" }
          |{ "id": null, "dep": "hr" }
          |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Row(1, "hr") :: Row(2, "hardware") :: Row(null, "hr") :: Row(null, "hr") :: Nil)

      sql(
        s"""DELETE FROM $tableNameAsString
           |WHERE
           | id NOT IN (SELECT * FROM deleted_id)
           | OR
           | dep IN ('software', 'hr')
           |""".stripMargin)

      checkAnswer(sql(s"SELECT * FROM $tableNameAsString"), Row(2, "hardware") :: Nil)

      sql(
        s"""DELETE FROM $tableNameAsString
           |WHERE
           | id NOT IN (SELECT * FROM deleted_id WHERE value IS NOT NULL)
           | AND
           | EXISTS (SELECT 1 FROM FROM deleted_dep WHERE dep = deleted_dep.value)
           |""".stripMargin)

      checkAnswer(sql(s"SELECT * FROM $tableNameAsString"), Row(2, "hardware") :: Nil)

      sql(
        s"""DELETE FROM $tableNameAsString t
           |WHERE
           | t.id NOT IN (SELECT * FROM deleted_id WHERE value IS NOT NULL)
           | OR
           | EXISTS (SELECT 1 FROM FROM deleted_dep WHERE t.dep = deleted_dep.value)
           |""".stripMargin)

      checkAnswer(sql(s"SELECT * FROM $tableNameAsString"), Nil)
    }
  }

  test("delete with EXISTS subquery") {
    withTempView("deleted_id", "deleted_dep") {
      createAndInitTable("id INT, dep STRING",
        """{ "id": 1, "dep": "hr" }
          |{ "id": 2, "dep": "hardware" }
          |{ "id": null, "dep": "hr" }
          |""".stripMargin)

      val deletedIdDF = Seq(Some(-1), Some(-2), None).toDF()
      deletedIdDF.createOrReplaceTempView("deleted_id")

      val deletedDepDF = Seq("software", "hr").toDF()
      deletedDepDF.createOrReplaceTempView("deleted_dep")

      sql(
        s"""DELETE FROM $tableNameAsString t
           |WHERE
           | EXISTS (SELECT 1 FROM deleted_id d WHERE t.id = d.value)
           |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Row(1, "hr") :: Row(2, "hardware") :: Row(null, "hr") :: Nil)

      sql(
        s"""DELETE FROM $tableNameAsString t
           |WHERE
           | EXISTS (SELECT 1 FROM deleted_id d WHERE t.id = d.value + 2)
           |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Row(2, "hardware") :: Row(null, "hr") :: Nil)

      sql(
        s"""DELETE FROM $tableNameAsString t
           |WHERE
           | EXISTS (SELECT 1 FROM deleted_id d WHERE t.id = d.value) OR t.id IS NULL
           |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Row(2, "hardware") :: Nil)

      sql(
        s"""DELETE FROM $tableNameAsString t
           |WHERE
           | EXISTS (SELECT 1 FROM deleted_id di WHERE t.id = di.value)
           | AND
           | EXISTS (SELECT 1 FROM deleted_dep dd WHERE t.dep = dd.value)
           |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Row(2, "hardware") :: Nil)
    }
  }

  test("delete with NOT EXISTS subquery") {
    withTempView("deleted_id", "deleted_dep") {
      createAndInitTable("id INT, dep STRING",
        """{ "id": 1, "dep": "hr" }
          |{ "id": 2, "dep": "hardware" }
          |{ "id": null, "dep": "hr" }
          |""".stripMargin)

      val deletedIdDF = Seq(Some(-1), Some(-2), None).toDF()
      deletedIdDF.createOrReplaceTempView("deleted_id")

      val deletedDepDF = Seq("software", "hr").toDF()
      deletedDepDF.createOrReplaceTempView("deleted_dep")

      sql(
        s"""DELETE FROM $tableNameAsString t
           |WHERE
           | NOT EXISTS (SELECT 1 FROM deleted_id di WHERE t.id = di.value + 2)
           | AND
           | NOT EXISTS (SELECT 1 FROM deleted_dep dd WHERE t.dep = dd.value)
           |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Row(1, "hr") :: Row(null, "hr") :: Nil)

      sql(
        s"""DELETE FROM $tableNameAsString t
           |WHERE
           | NOT EXISTS (SELECT 1 FROM deleted_id d WHERE t.id = d.value + 2)
           |""".stripMargin)

      checkAnswer(sql(s"SELECT * FROM $tableNameAsString"), Row(1, "hr") :: Nil)

      sql(
        s"""DELETE FROM $tableNameAsString t
           |WHERE
           | NOT EXISTS (SELECT 1 FROM deleted_id d WHERE t.id = d.value + 2)
           | OR
           | t.id = 1
           |""".stripMargin)

      checkAnswer(sql(s"SELECT * FROM $tableNameAsString"), Nil)
    }
  }

  test("delete with a scalar subquery") {
    withTempView("deleted_id") {
      createAndInitTable("id INT, dep STRING",
        """{ "id": 1, "dep": "hr" }
          |{ "id": 2, "dep": "hardware" }
          |{ "id": null, "dep": "hr" }
          |""".stripMargin)

      val deletedIdDF = Seq(Some(1), Some(100), None).toDF()
      deletedIdDF.createOrReplaceTempView("deleted_id")

      sql(
        s"""DELETE FROM $tableNameAsString t
           |WHERE
           | id <= (SELECT min(value) FROM deleted_id)
           |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Row(2, "hardware") :: Row(null, "hr") :: Nil)
    }
  }

  test("delete refreshes relation cache") {
    withTempView("temp") {
      withCache("temp") {
        createAndInitTable("id INT, dep STRING",
          """{ "id": 1, "dep": "hr" }
            |{ "id": 1, "dep": "hardware" }
            |{ "id": 2, "dep": "hardware" }
            |{ "id": 3, "dep": "hr" }
            |""".stripMargin)

        // define a view on top of the table
        val query = sql(s"SELECT * FROM $tableNameAsString WHERE id = 1")
        query.createOrReplaceTempView("temp")

        // cache the view
        sql("CACHE TABLE temp")

        // verify the view returns expected results
        checkAnswer(
          sql("SELECT * FROM temp"),
          Row(1, "hr") :: Row(1, "hardware") :: Nil)

        // delete some records from the table
        sql(s"DELETE FROM $tableNameAsString WHERE id <= 1")

        // verify the delete was successful
        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Row(2, "hardware") :: Row(3, "hr") :: Nil)

        // verify the view reflects the changes in the table
        checkAnswer(sql("SELECT * FROM temp"), Nil)
      }
    }
  }

  test("delete with nondeterministic conditions") {
    createAndInitTable("id INT, dep STRING",
      """{ "id": 1, "dep": "hr" }
        |{ "id": 2, "dep": "software" }
        |{ "id": 3, "dep": "hr" }
        |""".stripMargin)

    val e = intercept[AnalysisException] {
      sql(s"DELETE FROM $tableNameAsString WHERE id <= 1 AND rand() > 0.5")
    }
    assert(e.message.contains("nondeterministic expressions are only allowed"))
  }

  test("delete without condition executed as delete with filters") {
    createAndInitTable("id INT, dep INT",
      """{ "id": 1, "dep": 100 }
        |{ "id": 2, "dep": 200 }
        |{ "id": 3, "dep": 100 }
        |""".stripMargin)

    executeDeleteWithFilters(s"DELETE FROM $tableNameAsString")

    checkAnswer(sql(s"SELECT * FROM $tableNameAsString"), Nil)
  }

  test("delete with supported predicates gets converted into delete with filters") {
    createAndInitTable("id INT, dep INT",
      """{ "id": 1, "dep": 100 }
        |{ "id": 2, "dep": 200 }
        |{ "id": 3, "dep": 100 }
        |""".stripMargin)

    executeDeleteWithFilters(s"DELETE FROM $tableNameAsString WHERE dep = 100")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(2, 200) :: Nil)
  }

  test("delete with unsupported predicates cannot be converted into delete with filters") {
    createAndInitTable("id INT, dep INT",
      """{ "id": 1, "dep": 100 }
        |{ "id": 2, "dep": 200 }
        |{ "id": 3, "dep": 100 }
        |""".stripMargin)

    executeDeleteWithRewrite(s"DELETE FROM $tableNameAsString WHERE dep = 100 OR dep < 200")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(2, 200) :: Nil)
  }

  test("delete with subquery cannot be converted into delete with filters") {
    withTempView("deleted_id") {
      createAndInitTable("id INT, dep INT",
        """{ "id": 1, "dep": 100 }
          |{ "id": 2, "dep": 200 }
          |{ "id": 3, "dep": 100 }
          |""".stripMargin)

      val deletedIdDF = Seq(Some(1), Some(100), None).toDF()
      deletedIdDF.createOrReplaceTempView("deleted_id")

      val q = s"DELETE FROM $tableNameAsString WHERE dep = 100 AND id IN (SELECT * FROM deleted_id)"
      executeDeleteWithRewrite(q)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Row(2, 200) :: Row(3, 100) :: Nil)
    }
  }

  protected def createTable(schemaString: String): Unit = {
    val schema = StructType.fromDDL(schemaString)
    val tableProps = Collections.emptyMap[String, String]
    catalog.createTable(ident, schema, Array(identity(reference(Seq("dep")))), tableProps)
  }

  protected def createAndInitTable(schemaString: String, jsonData: String): Unit = {
    createTable(schemaString)
    append(schemaString, jsonData)
  }

  private def append(schemaString: String, jsonData: String): Unit = {
    val df = toDF(jsonData, schemaString)
    df.coalesce(1).writeTo(tableNameAsString).append()
  }

  private def toDF(jsonData: String, schemaString: String = null): DataFrame = {
    val jsonRows = jsonData.split("\\n").filter(str => str.trim.nonEmpty)
    val jsonDS = spark.createDataset(jsonRows)(Encoders.STRING)
    if (schemaString == null) {
      spark.read.json(jsonDS)
    } else {
      spark.read.schema(schemaString).json(jsonDS)
    }
  }

  private def executeDeleteWithFilters(query: String): Unit = {
    val executedPlan = executeAndKeepPlan {
      sql(query)
    }

    executedPlan match {
      case _: DeleteFromTableExec =>
        // OK
      case other =>
        fail("unexpected executed plan: " + other)
    }
  }

  private def executeDeleteWithRewrite(query: String): Unit = {
    val executedPlan = executeAndKeepPlan {
      sql(query)
    }

    executedPlan match {
      case _: ReplaceDataExec =>
        // OK
      case other =>
        fail("unexpected executed plan: " + other)
    }
  }

  // executes an operation and keeps the executed plan
  protected def executeAndKeepPlan(func: => Unit): SparkPlan = {
    var executedPlan: SparkPlan = null

    val listener = new QueryExecutionListener {
      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
        executedPlan = qe.executedPlan
      }
      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
      }
    }
    spark.listenerManager.register(listener)

    func

    sparkContext.listenerBus.waitUntilEmpty()

    stripAQEPlan(executedPlan)
  }
}
