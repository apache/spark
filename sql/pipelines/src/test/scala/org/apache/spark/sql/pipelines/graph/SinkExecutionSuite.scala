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

package org.apache.spark.sql.pipelines.graph

import org.apache.spark.sql.classic.DataFrame
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.pipelines.utils.{ExecutionTest, TestGraphRegistrationContext}
import org.apache.spark.sql.test.SharedSparkSession

class SinkExecutionSuite extends ExecutionTest with SharedSparkSession {
  override def defaultExecutionMode: ExecutionMode = ExecutionMode.MICROBATCH

  def withTempDir(f: String => Unit): Unit = {
    if (isHighFidelityMode) {
      // HiFi test writes to a S3 fake. So we have to use S3 file path for all path
      // operation, instead of using Java file interface.
      withHiFiTempDir(spark) { uri =>
        val locationName =
          "test_location_" + UUID.randomUUID().getMostSignificantBits.formatted("%016x_")
        createExternalLocation(spark, locationName, uri)
        f(uri.toString)
      }
    } else {
      val tempDir = createTempDir()
      f(tempDir)
    }
  }

  val session = spark
  import session.implicits._

  def createDataflowGraph(
      rootDirectory: String,
      inputs: DataFrame,
      sinkName: String,
      flowName: String,
      format: String,
      sinkOptions: Map[String, String] = Map.empty
  ): DataflowGraph = {
    val registrationContext = new TestGraphRegistrationContext(spark) {
      registerTemporaryView("a", query = dfFlowFunc(inputs))
      registerSink(sinkName, format, sinkOptions)
      registerFlow(sinkName, flowName, query = readStreamFlowFunc("a"))
    }
  }



  test("update to external sink - memory sink") {
    SparkSessionUtils.withSQLConf(
      spark,
      ("pipelines.externalSink.enabled", true.toString)
    ) {
      val ints = MemoryStream[Int]
      val rootDirectory = pipelineStorageRoot
      class P extends Pipeline {
        createExternalSink("mem_sink", "memory")
        createSinkForTopLevelFlow("mem_sink").query(
          name = "flow_to_sink",
          // APPEND output mode doesn't support aggregation without watermark
          ints.toDS().toDF("id").groupBy("id").count(),
          outputMode = Some("update")
        )
      }
      val graphExecution = createContinuousGraphExecution(
        DataflowGraph(new P)
      )._2

      graphExecution.start()
      ints.addData(1, 2, 2, 3)
      graphExecution.processAllAvailable("flow_to_sink")

      verifyCheckpointLocation(
        rootDirectory,
        graphExecution,
        "mem_sink",
        "flow_to_sink"
      )
      checkAnswer(spark.read.format("memory").table("mem_sink"), Seq((1, 1), (2, 2), (3, 1)).toDF())
      graphExecution.stop()
    }
  }

  test("writing to external sink - memory sink") {
    SparkSessionUtils.withSQLConf(
      spark,
      ("pipelines.externalSink.enabled", true.toString)
    ) {
      val ints = MemoryStream[Int]
      val rootDirectory = pipelineStorageRoot

      val pipeline =
        new ExternalSinkPipeline(rootDirectory, ints.toDF(), "sink_a", "flow_to_sink_a", "memory")
      val graphExecution = createContinuousGraphExecution(
        DataflowGraph(pipeline)
      )._2

      graphExecution.start()
      ints.addData(1, 2, 3, 4)
      graphExecution.processAllAvailable("flow_to_sink_a")
      verifyCheckpointLocation(rootDirectory, graphExecution, "sink_a", "flow_to_sink_a")

      checkAnswer(spark.sql("SELECT * FROM sink_a"), Seq(1, 2, 3, 4).toDF())
      graphExecution.stop()
    }
  }

  test("writing to external sink - delta sink with path") {
    SparkSessionUtils.withSQLConf(
      spark,
      ("pipelines.externalSink.enabled", true.toString)
    ) {
      withTempDir { externalDeltaPath =>
        val ints = MemoryStream[Int]
        val rootDirectory = pipelineStorageRoot
        val pipeline = new ExternalSinkPipeline(
          rootDirectory,
          ints.toDF(),
          "delta_sink",
          "flow_to_delta_sink",
          "delta",
          Map(
            "path" -> externalDeltaPath
          )
        )
        val graphExecution = createContinuousGraphExecution(
          DataflowGraph(pipeline)
        )._2

        graphExecution.start()
        ints.addData(1, 2, 3, 4)
        graphExecution.processAllAvailable("flow_to_delta_sink")

        verifyCheckpointLocation(
          rootDirectory,
          graphExecution,
          "delta_sink",
          "flow_to_delta_sink"
        )

        checkAnswer(spark.read.format("delta").load(externalDeltaPath), Seq(1, 2, 3, 4).toDF())
        graphExecution.stop()
      }
    }
  }

  test("writing to external sink - sink name has upper case characters") {
    SparkSessionUtils.withSQLConf(
      spark,
      ("pipelines.externalSink.enabled", true.toString)
    ) {
      val ints = MemoryStream[Int]
      val rootDirectory = pipelineStorageRoot

      val pipeline =
        new ExternalSinkPipeline(rootDirectory, ints.toDF(), "mySink", "flow_to_sink_a", "memory")
      val graphExecution = createContinuousGraphExecution(
        pipeline.toDataflowGraph(shouldLowerCaseNames = updateContext.isUCPipeline)
      )._2

      graphExecution.start()
      ints.addData(1, 2, 3, 4)
      graphExecution.processAllAvailable("flow_to_sink_a")

      val sinkName = if (updateContext.isUCPipeline) {
        "mysink"
      } else {
        "mySink"
      }

      verifyCheckpointLocation(rootDirectory, graphExecution, sinkName, "flow_to_sink_a")

      checkAnswer(spark.sql(s"SELECT * FROM $sinkName"), Seq(1, 2, 3, 4).toDF())
      graphExecution.stop()
    }
  }





  def testOnlyAllowFullyQualifiedTableId(tableId: String, isFullyQualified: Boolean) = {
    test(
      s"writing to external sink - delta sink with table name -" +
      s" only fully qualified table name is allowed -" +
      s" tableId=$tableId,isFullyQualified=$isFullyQualified"
    ) {
      SparkSessionUtils.withSQLConf(
        spark,
        ("pipelines.externalSink.enabled", true.toString),
        ("pipelines.ucManagedDeltaSink.enabled", true.toString)
      ) {
        val errorMsg = if (updateContext.isUCPipeline) {
          "UC pipeline expect a fully qualified table name <catalog>.<schema>.<table>."
        } else {
          "HMS pipeline expect a two-part-name <schema>.<table>."
        }
        runDeltaTableTest(tableId, Map("tableName" -> tableId), isFullyQualified == true, errorMsg)
      }
    }
  }

  def runDeltaTableTest(
      tableName: String,
      sinkOptions: Map[String, String],
      expectSuccess: Boolean,
      errMsgIfFail: String): Unit = {
    val ints = MemoryStream[Int]
    val rootDirectory = pipelineStorageRoot

    val pipeline = new ExternalSinkPipeline(
      rootDirectory,
      ints.toDF(),
      "delta_sink",
      "flow_to_delta_sink",
      "delta",
      sinkOptions
    )
    val graphExecution = createContinuousGraphExecution(
      DataflowGraph(pipeline)
    )._2

    if (expectSuccess) {
      graphExecution.start()
      ints.addData(1, 2, 3, 4)
      graphExecution.processAllAvailable("flow_to_delta_sink")

      verifyCheckpointLocation(
        rootDirectory,
        graphExecution,
        "delta_sink",
        "flow_to_delta_sink"
      )

      checkAnswer(spark.sql(s"SELECT * FROM $tableName"), Seq(1, 2, 3, 4).toDF())
    } else {
      val ex = intercept[IllegalArgumentException] {
        graphExecution.start()
        graphExecution.startStreamingFlow("flow_to_delta_sink")
      }
      assert(ex.getMessage.contains(errMsgIfFail))
    }

    graphExecution.stop()
  }

  def verifyCheckpointLocation(
      rootDirectory: String,
      graphExecution: ContinuousGraphExecution,
      sinkName: String,
      flowName: String): Unit = {
    val checkpointLocation = if (updateContext.isUCPipeline) {
      new Path(
        graphExecution.graphForExecution
          .sinkByName(sinkName)
          .path + s"/_dlt_metadata/checkpoints/$flowName/0"
      )
    } else {
      new Path("file://" + rootDirectory + s"/checkpoints/$flowName/0")
    }
    assert(
      new Path(
        getCheckpointPath(
          spark.streams.get(graphExecution.physicalFlows(flowName).getOrigin.getFlowId)
        )
      ) == checkpointLocation
    )
  }

  private def getCheckpointPath(q: StreamingQuery): String =
    q.asInstanceOf[StreamingQueryWrapper].streamingQuery.resolvedCheckpointRoot
}

class ExternalSinkExecutionSuite extends BaseExternalSinkExecutionSuite with CoreExecutionTest {
  Seq("test_table", "default.test_table", "main.default.test_table")
    .zip(Seq(false, true, false))
    .foreach {
      case (tableId, isFullyQualified) =>
        testOnlyAllowFullyQualifiedTableId(tableId, isFullyQualified)
    }
}

class ExternalSinkExecutionWithUCSuite
    extends BaseExternalSinkExecutionSuite
    with CoreExecutionWithUCTest {

  Seq(true, false).foreach { allowUCManagedTable =>
    test(
      s"writing to external sink - delta sink with table in a different catalog -" +
      s"allowUCManagedTable: $allowUCManagedTable"
    ) {
      val time = System.currentTimeMillis()
      val testTableName = s"test_table_$time"

      // Create a table under "my_catalog.my_schema"
      val catalog = "my_catalog"
      val schema = "my_schema"
      spark.sql(s"CREATE CATALOG IF NOT EXISTS $catalog")
      spark.sql(s"DROP SCHEMA IF EXISTS $catalog.$schema CASCADE")
      spark.sql(s"CREATE SCHEMA $catalog.$schema")
      spark.sql(s"USE CATALOG $catalog")
      spark.sql(s"USE SCHEMA $schema")
      spark.sql(s"CREATE TABLE $testTableName (value int) USING DELTA")

      // Describe table under "my_catalog.my_schema" should succeed.
      spark.sql(s"DESCRIBE TABLE $testTableName")

      // Switch to a different catalog
      spark.sql(s"USE CATALOG ${catalogInPipelineSpec.get}")

      // Describe table under "catalogInPipelineSpec.get" should fail.
      intercept[AnalysisException] {
        spark.sql(s"DESC TABLE $testTableName")
      }

      SparkSessionUtils.withSQLConf(
        spark,
        ("pipelines.externalSink.enabled", true.toString),
        ("pipelines.ucManagedDeltaSink.enabled", allowUCManagedTable.toString)
      ) {
        runDeltaTableTest(
          s"$catalog.$schema.$testTableName",
          Map("tableName" -> s"$catalog.$schema.$testTableName"),
          allowUCManagedTable == true,
          errMsgIfFail = "Only UC External Table is supported for UC pipeline."
        )
        spark.sql(s"DROP CATALOG $catalog CASCADE")
      }
    }
  }

  Seq(true, false).foreach { tableNameExists =>
    test(
      s"writing to external sink - " +
      s"UC external delta sink with table name is allowed - tableNameExists: $tableNameExists",
      UCHiFiOnly
    ) {
      SparkSessionUtils.withSQLConf(
        spark,
        ("pipelines.externalSink.enabled", true.toString)
      ) {
        withTempDir { externalLocation =>
          val catalog =
            spark.sql("select current_catalog()").collect().head.getString(0)
          val schema =
            spark.sql("select current_schema()").collect().head.getString(0)
          val time = System.currentTimeMillis()
          val testTableName = s"${catalog}.${schema}.test_table_$time"
          if (tableNameExists) {
            spark.sql(
              s"CREATE TABLE $testTableName (value int) USING delta LOCATION '$externalLocation'"
            )
          }

          runDeltaTableTest(
            testTableName,
            Map(
              "tableName" -> testTableName,
              "path" -> externalLocation
            ),
            expectSuccess = true,
            errMsgIfFail = ""
          )
        }
      }
    }
  }

  test(
    s"writing to external sink - table name with special characters"
  ) {
    SparkSessionUtils.withSQLConf(
      spark,
      ("pipelines.externalSink.enabled", true.toString),
      ("pipelines.ucManagedDeltaSink.enabled", true.toString)
    ) {
      val time = System.currentTimeMillis()
      val testTableName = s"main.default.`中文_test_table_$time`"
      spark.sql(
        s"CREATE TABLE $testTableName (value int) USING delta"
      )

      runDeltaTableTest(
        testTableName,
        Map(
          "tableName" -> testTableName
        ),
        expectSuccess = true,
        errMsgIfFail = ""
      )
    }
  }

  Seq("test_table", "default.test_table", "main.default.test_table")
    .zip(Seq(false, false, true))
    .foreach {
      case (tableId, isFullyQualified) =>
        testOnlyAllowFullyQualifiedTableId(tableId, isFullyQualified)
    }

}
