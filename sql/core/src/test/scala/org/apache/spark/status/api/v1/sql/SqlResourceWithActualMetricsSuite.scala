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

package org.apache.spark.status.api.v1.sql

import java.net.{URI, URL}
import java.text.SimpleDateFormat
import java.util.Locale

import jakarta.servlet.http.HttpServletResponse
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods
import org.scalatest.time.SpanSugar._

import org.apache.spark.SparkConf
import org.apache.spark.deploy.history.HistoryServerSuite.getContentAndCode
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.execution.metric.SQLMetricsTestUtils
import org.apache.spark.sql.execution.ui.SQLExecutionUIData
import org.apache.spark.sql.internal.SQLConf.ADAPTIVE_EXECUTION_ENABLED
import org.apache.spark.sql.test.SharedSparkSession

case class Person(id: Int, name: String, age: Int)
case class Salary(personId: Int, salary: Double)

/**
 * Sql Resource Public API Unit Tests running query and extracting the metrics.
 */
class SqlResourceWithActualMetricsSuite
  extends SharedSparkSession with SQLMetricsTestUtils {

  import testImplicits._

  // Exclude nodes which may not have the metrics
  val excludedNodes = List("WholeStageCodegen", "Project", "SerializeFromObject")

  implicit val formats: DefaultFormats = new DefaultFormats {
    override def dateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
  }

  override def sparkConf: SparkConf = {
    super.sparkConf.set("spark.ui.enabled", "true")
  }

  test("Check Sql Rest Api Endpoints") {
    // Materalize result DataFrame
    withSQLConf(ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val count = getDF().count()
      assert(count == 2, s"Expected Query Count is 2 but received: $count")
    }

    // Spark apps launched by local-mode seems not having `attemptId` as default
    // so UT is just added for existing endpoints.
    val executionId = callSqlRestEndpointAndVerifyResult()
    callSqlRestEndpointByExecutionIdAndVerifyResult(executionId)
  }

  private def callSqlRestEndpointAndVerifyResult(): Long = {
    val url = new URI(spark.sparkContext.ui.get.webUrl
      + s"/api/v1/applications/${spark.sparkContext.applicationId}/sql").toURL
    val jsonResult = verifyAndGetSqlRestResult(url)
    val executionDatas = JsonMethods.parse(jsonResult).extract[Seq[ExecutionData]]
    assert(executionDatas.size > 0,
      s"Expected Query Result Size is higher than 0 but received: ${executionDatas.size}")
    val executionData = executionDatas.head
    verifySqlRestContent(executionData)
    executionData.id
  }

  private def callSqlRestEndpointByExecutionIdAndVerifyResult(executionId: Long): Unit = {
    val url = new URI(spark.sparkContext.ui.get.webUrl
      + s"/api/v1/applications/${spark.sparkContext.applicationId}/sql/${executionId}").toURL
    val jsonResult = verifyAndGetSqlRestResult(url)
    val executionData = JsonMethods.parse(jsonResult).extract[ExecutionData]
    verifySqlRestContent(executionData)
  }

  private def verifySqlRestContent(executionData: ExecutionData): Unit = {
    assert(executionData.status == "COMPLETED",
      s"Expected status is COMPLETED but actual: ${executionData.status}")
    assert(executionData.successJobIds.nonEmpty,
      s"Expected successJobIds should not be empty")
    assert(executionData.runningJobIds.isEmpty,
      s"Expected runningJobIds should be empty but actual: ${executionData.runningJobIds}")
    assert(executionData.failedJobIds.isEmpty,
      s"Expected failedJobIds should be empty but actual: ${executionData.failedJobIds}")
    assert(executionData.nodes.nonEmpty, "Expected nodes should not be empty}")
    executionData.nodes.filterNot(node => excludedNodes.contains(node.nodeName)).foreach { node =>
      assert(node.metrics.nonEmpty, "Expected metrics of nodes should not be empty")
    }
  }

  private def verifyAndGetSqlRestResult(url: URL): String = {
    val (code, resultOpt, error) = getContentAndCode(url)
    assert(code == 200, s"Expected Http Response Code is 200 but received: $code for url: $url")
    assert(resultOpt.nonEmpty, s"Rest result should not be empty for url: $url")
    assert(error.isEmpty, s"Error message should be empty for url: $url")
    resultOpt.get
  }

  private def getDF(): DataFrame = {
    val person: DataFrame =
      spark.sparkContext.parallelize(
        Person(0, "mike", 30) ::
          Person(1, "jim", 20) :: Nil).toDF()

    val salary: DataFrame =
      spark.sparkContext.parallelize(
        Salary(0, 2000.0) ::
          Salary(1, 1000.0) :: Nil).toDF()

    val salaryDF = salary.withColumnRenamed("personId", "id")
    val ds = person.join(salaryDF, "id")
      .groupBy("name", "age", "salary").avg("age", "salary")
      .filter(_.getAs[Int]("age") <= 30)
      .sort()

    ds.toDF()
  }

  test("SPARK-44334: Status of a failed DDL/DML with no jobs should be FAILED") {
    withTable("SPARK_44334") {
      val sqlStr = "CREATE TABLE SPARK_44334 USING parquet AS SELECT 1 AS a"
      sql(sqlStr)
      intercept[TableAlreadyExistsException](sql(sqlStr))

      val url = new URI(spark.sparkContext.ui.get.webUrl +
        s"/api/v1/applications/${spark.sparkContext.applicationId}/sql").toURL
      eventually(timeout(20.seconds), interval(50.milliseconds)) {
        val result = verifyAndGetSqlRestResult(url)
        val executionDataList = JsonMethods.parse(result)
          .extract[Seq[ExecutionData]]
          .filter(_.planDescription.contains("SPARK_44334"))
        assert(executionDataList.size == 2)
        assert(executionDataList.head.status == "COMPLETED")
        assert(executionDataList.last.status == "FAILED")
      }
    }
  }

  test("SPARK-45291: Use unknown query execution id instead of no such app when id is invalid") {
    val url = new URI(spark.sparkContext.ui.get.webUrl +
      s"/api/v1/applications/${spark.sparkContext.applicationId}/sql/${Long.MaxValue}").toURL
    val (code, resultOpt, error) = getContentAndCode(url)
    assert(code === HttpServletResponse.SC_NOT_FOUND)
    assert(resultOpt.isEmpty)
    assert(error.get === s"unknown query execution id: ${Long.MaxValue}")
  }

  test("SPARK-56140: sqlTable server-side pagination endpoint") {
    // Run a query to generate SQL executions
    spark.sql("SELECT 1 + 1").collect()
    spark.sql("SELECT 2 + 2").collect()

    eventually(timeout(10.seconds), interval(1.second)) {
      val baseUrl = spark.sparkContext.ui.get.webUrl +
        s"/api/v1/applications/${spark.sparkContext.applicationId}/sql/sqlTable"

      // Test basic pagination
      val url = new URI(s"$baseUrl?start=0&length=5&draw=1").toURL
      val (code, resultOpt, _) = getContentAndCode(url)
      assert(code === HttpServletResponse.SC_OK)
      val json = JsonMethods.parse(resultOpt.get)
      val draw = (json \ "draw").extract[Int]
      val recordsTotal = (json \ "recordsTotal").extract[Long]
      val recordsFiltered = (json \ "recordsFiltered").extract[Long]
      val aaData = (json \ "aaData").children
      assert(draw === 1, "draw should be echoed back")
      assert(recordsTotal > 0, "should have some executions")
      assert(recordsFiltered === recordsTotal, "no filter applied")
      assert(aaData.size <= 5, "should respect page length")

      // Verify row data fields
      val firstRow = aaData.head
      assert((firstRow \ "id").extract[Long] >= 0)
      assert((firstRow \ "status").extract[String].nonEmpty)
      assert((firstRow \ "description").extract[String] != null)
      assert((firstRow \ "duration").extract[Long] >= 0)

      // Test search filter
      val searchUrl = new URI(
        s"$baseUrl?start=0&length=100&draw=2&search%5Bvalue%5D=nonexistent_xyz").toURL
      val (searchCode, searchResultOpt, _) = getContentAndCode(searchUrl)
      assert(searchCode === HttpServletResponse.SC_OK)
      val searchJson = JsonMethods.parse(searchResultOpt.get)
      val searchFiltered = (searchJson \ "recordsFiltered").extract[Long]
      val searchData = (searchJson \ "aaData").children
      assert(searchFiltered === 0, "search for nonexistent should return 0")
      assert(searchData.isEmpty)

      // Test status filter
      val statusUrl = new URI(
        s"$baseUrl?start=0&length=100&draw=3&status=COMPLETED").toURL
      val (statusCode, statusResultOpt, _) = getContentAndCode(statusUrl)
      assert(statusCode === HttpServletResponse.SC_OK)
      val statusJson = JsonMethods.parse(statusResultOpt.get)
      val statusData = (statusJson \ "aaData").children
      statusData.foreach { row =>
        assert((row \ "status").extract[String] === "COMPLETED")
      }
    }
  }

  test("SPARK-56811: sqlTable groups sub-executions under their root execution") {
    // CACHE TABLE produces a root execution plus an inner sub-execution that
    // shares its rootExecutionId. This is the canonical case where the SQL
    // listing should fold the sub row under the root rather than flattening it.
    spark.sql("CREATE OR REPLACE TEMP VIEW spark_56811 AS SELECT id FROM RANGE(10)")
      .collect()
    spark.sql("CACHE TABLE spark_56811_cached AS SELECT * FROM spark_56811").collect()
    try {
      eventually(timeout(10.seconds), interval(1.second)) {
        val baseUrl = spark.sparkContext.ui.get.webUrl +
          s"/api/v1/applications/${spark.sparkContext.applicationId}/sql/sqlTable"

        // Grouping ON: roots only, with subExecutions embedded on the root that
        // owns a sub-execution.
        val groupedUrl = new URI(
          s"$baseUrl?start=0&length=100&draw=1&groupSubExecution=true").toURL
        val (groupedCode, groupedOpt, _) = getContentAndCode(groupedUrl)
        assert(groupedCode === HttpServletResponse.SC_OK)
        val groupedJson = JsonMethods.parse(groupedOpt.get)
        val groupedRecordsTotal = (groupedJson \ "recordsTotal").extract[Long]
        val groupedRecordsFiltered = (groupedJson \ "recordsFiltered").extract[Long]
        val groupedRows = (groupedJson \ "aaData").children
        assert(groupedRecordsTotal === groupedRows.size,
          "with no filter, recordsTotal should match returned root count")
        assert(groupedRecordsFiltered === groupedRows.size,
          "with no filter, recordsFiltered should match returned root count")
        // Every row in grouped mode is either a true root (id == rootExecutionId)
        // or an orphan sub whose real parent is absent from the result set.
        val visibleIds = groupedRows.map(r => (r \ "id").extract[Long]).toSet
        groupedRows.foreach { row =>
          val id = (row \ "id").extract[Long]
          val rootId = (row \ "rootExecutionId").extract[Long]
          assert(id == rootId || !visibleIds.contains(rootId),
            s"grouped row $id (rootId=$rootId) is neither a root nor an orphan")
        }
        val rootsWithSubs = groupedRows.filter { row =>
          (row \ "subExecutions").children.nonEmpty
        }
        assert(rootsWithSubs.nonEmpty,
          "CACHE TABLE should produce at least one root with sub-executions")
        rootsWithSubs.foreach { row =>
          val rootId = (row \ "id").extract[Long]
          (row \ "subExecutions").children.foreach { sub =>
            assert((sub \ "rootExecutionId").extract[Long] === rootId,
              "sub-execution should reference its parent root")
            assert((sub \ "id").extract[Long] !== rootId,
              "sub-execution must not have the same id as its root")
          }
        }

        // Grouping OFF: flat list of every execution, with no embedded subs.
        val flatUrl = new URI(
          s"$baseUrl?start=0&length=100&draw=2&groupSubExecution=false").toURL
        val (flatCode, flatOpt, _) = getContentAndCode(flatUrl)
        assert(flatCode === HttpServletResponse.SC_OK)
        val flatJson = JsonMethods.parse(flatOpt.get)
        val flatRows = (flatJson \ "aaData").children
        assert(flatRows.size > groupedRows.size,
          "flat listing should contain at least one extra sub-execution row")
        val embeddedSubs = groupedRows.map(r => (r \ "subExecutions").children.size).sum
        assert(flatRows.size === groupedRows.size + embeddedSubs,
          "flat size should equal grouped roots plus embedded sub rows")
        flatRows.foreach { row =>
          assert((row \ "subExecutions").children.isEmpty,
            "flat listing should not embed subExecutions")
        }
      }
    } finally {
      spark.sql("UNCACHE TABLE IF EXISTS spark_56811_cached")
    }
  }

  test("SPARK-56811: partitionRoots surfaces orphan sub-executions as root rows") {
    def mkExec(id: Long, rootId: Long): SQLExecutionUIData = new SQLExecutionUIData(
      executionId = id,
      rootExecutionId = rootId,
      description = s"exec $id",
      details = "",
      physicalPlanDescription = "",
      modifiedConfigs = Map.empty,
      metrics = Seq.empty,
      submissionTime = id,
      completionTime = None,
      errorMessage = None,
      jobs = Map.empty,
      stages = Set.empty,
      metricValues = null,
      queryId = null)

    // Tree:
    //   1 (root) -> 2, 3 (subs)
    //   4 (root, no subs)
    //   6 (sub of 5, but 5 is missing -> orphan)
    val root1 = mkExec(1, 1)
    val sub2 = mkExec(2, 1)
    val sub3 = mkExec(3, 1)
    val root4 = mkExec(4, 4)
    val orphan6 = mkExec(6, 5)

    val (roots, subsByRoot) =
      SqlResource.partitionRoots(Seq(root1, sub2, sub3, root4, orphan6))

    assert(roots.map(_.executionId).toSet === Set(1L, 4L, 6L),
      "true roots and orphan subs should both be promoted to root rows")
    assert(subsByRoot.keySet === Set(1L),
      "only execs with a parent present in the input should appear in subsByRoot")
    assert(subsByRoot(1L).map(_.executionId).toSet === Set(2L, 3L),
      "subs should be grouped under their parent root id")
    val orphanRow = roots.find(_.executionId == 6L).get
    assert(orphanRow.rootExecutionId === 5L,
      "orphan promoted to a root row preserves its original rootExecutionId")
  }

  test("SPARK-56137: sqlList returns ISO date format in submissionTime") {
    withSQLConf(ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      spark.sql("SELECT 'date_format_test'").collect()
    }

    val url = new URI(spark.sparkContext.ui.get.webUrl +
      s"/api/v1/applications/${spark.sparkContext.applicationId}" +
      "/sql").toURL
    eventually(timeout(10.seconds), interval(50.milliseconds)) {
      val result = verifyAndGetSqlRestResult(url)
      // yyyy-MM-dd'T'HH:mm:ss.SSS'GMT'
      val datePattern =
        """"submissionTime"\s*:\s*"\d{4}-\d{2}-\d{2}T""" +
        """\d{2}:\d{2}:\d{2}\.\d{3}GMT"""
      assert(datePattern.r.findFirstIn(result).isDefined,
        "submissionTime should match ISO date format with GMT")
    }
  }

  test("SPARK-56137: sqlList returns all executions without limit") {
    val url = new URI(spark.sparkContext.ui.get.webUrl +
      s"/api/v1/applications/${spark.sparkContext.applicationId}" +
      "/sql").toURL

    val startCount = {
      val result = verifyAndGetSqlRestResult(url)
      JsonMethods.parse(result).extract[Seq[ExecutionData]].size
    }

    withSQLConf(ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      for (i <- 1 to 5) {
        spark.sql(s"SELECT $i AS limit_test").collect()
      }
    }

    eventually(timeout(10.seconds), interval(50.milliseconds)) {
      val result = verifyAndGetSqlRestResult(url)
      val executions =
        JsonMethods.parse(result).extract[Seq[ExecutionData]]
      assert(executions.size >= startCount + 5,
        s"Expected at least ${startCount + 5} executions " +
          s"but got: ${executions.size}")
    }
  }

  test("SPARK-56137: execution detail returns planDescription") {
    withSQLConf(ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      spark.sql("SELECT 'plan_desc_test'").collect()
    }

    val baseUrl = spark.sparkContext.ui.get.webUrl
    val appId = spark.sparkContext.applicationId
    val listUrl = new URI(
      s"$baseUrl/api/v1/applications/$appId/sql").toURL

    eventually(timeout(10.seconds), interval(50.milliseconds)) {
      val listResult = verifyAndGetSqlRestResult(listUrl)
      val executions = JsonMethods.parse(listResult)
        .extract[Seq[ExecutionData]]
      assert(executions.nonEmpty,
        "Should have at least one execution")

      val execId = executions.head.id
      val detailUrl = new URI(
        s"$baseUrl/api/v1/applications/$appId/sql/$execId"
      ).toURL
      val detailResult = verifyAndGetSqlRestResult(detailUrl)
      val execution = JsonMethods.parse(detailResult)
        .extract[ExecutionData]
      assert(execution.planDescription != null &&
        execution.planDescription.nonEmpty,
        "planDescription should not be empty")
      assert(execution.nodes.nonEmpty,
        "nodes should not be empty in detail response")
    }
  }

  test("SPARK-56137: multiple query types appear in listing") {
    withSQLConf(ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withTable("spark_56137_multi") {
        // DDL
        spark.sql(
          "CREATE TABLE spark_56137_multi " +
            "(id INT, name STRING) USING parquet")
        // DML
        spark.sql(
          "INSERT INTO spark_56137_multi VALUES (1, 'test')")
        // SELECT
        spark.sql(
          "SELECT * FROM spark_56137_multi").collect()

        val url = new URI(spark.sparkContext.ui.get.webUrl +
          s"/api/v1/applications/" +
          s"${spark.sparkContext.applicationId}/sql").toURL
        eventually(
            timeout(20.seconds), interval(50.milliseconds)) {
          val result = verifyAndGetSqlRestResult(url)
          val testExecs = JsonMethods.parse(result)
            .extract[Seq[ExecutionData]]
            .filter { e =>
              val desc =
                Option(e.description).getOrElse("")
              val plan =
                Option(e.planDescription).getOrElse("")
              val text = (desc + " " + plan).toLowerCase(Locale.ROOT)
              text.contains("spark_56137_multi")
            }
          assert(testExecs.size >= 3,
            s"Expected at least 3 executions " +
              s"(DDL, DML, SELECT), got: ${testExecs.size}")
          testExecs.foreach { e =>
            assert(e.status == "COMPLETED",
              s"Execution ${e.id} should be COMPLETED " +
                s"but was ${e.status}")
          }
        }
      }
    }
  }
}
