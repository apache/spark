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
package org.apache.spark.sql.execution.python.streaming

import java.io.File
import java.util.concurrent.{CountDownLatch, TimeUnit}

import scala.concurrent.duration._

import org.apache.spark.SparkException
import org.apache.spark.sql.{AnalysisException, DataFrame, Row}
import org.apache.spark.sql.IntegratedUDFTestUtils.{createUserDefinedPythonDataSource, shouldTestPandasUDFs}
import org.apache.spark.sql.connector.read.streaming.ReadLimit
import org.apache.spark.sql.execution.datasources.v2.python.{PythonDataSourceV2, PythonMicroBatchStream, PythonMicroBatchStreamWithAdmissionControl, PythonStreamingSourceOffset, PythonStreamingSourceReadLimit}
import org.apache.spark.sql.execution.python.PythonDataSourceSuiteBase
import org.apache.spark.sql.execution.streaming.ProcessingTimeTrigger
import org.apache.spark.sql.execution.streaming.checkpointing.{CommitLog, OffsetSeqLog}
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.streaming.{StreamingQueryException, Trigger}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class PythonStreamingDataSourceSimpleSuite extends PythonDataSourceSuiteBase {
  val waitTimeout = 15.seconds

  protected def simpleDataStreamReaderScript: String =
    """
      |from pyspark.sql.datasource import SimpleDataSourceStreamReader
      |
      |class SimpleDataStreamReader(SimpleDataSourceStreamReader):
      |    def initialOffset(self):
      |        return {"partition-1": 0}
      |    def read(self, start: dict):
      |        start_idx = start["partition-1"]
      |        it = iter([(i, ) for i in range(start_idx, start_idx + 2)])
      |        return (it, {"partition-1": start_idx + 2})
      |    def readBetweenOffsets(self, start: dict, end: dict):
      |        start_idx = start["partition-1"]
      |        end_idx = end["partition-1"]
      |        return iter([(i, ) for i in range(start_idx, end_idx)])
      |""".stripMargin

  protected def simpleDataStreamReaderWithEmptyBatchScript: String =
    """
      |from pyspark.sql.datasource import SimpleDataSourceStreamReader
      |
      |class SimpleDataStreamReader(SimpleDataSourceStreamReader):
      |    def initialOffset(self):
      |        return {"partition-1": 0}
      |    def read(self, start: dict):
      |        start_idx = start["partition-1"]
      |        if start_idx % 4 == 0:
      |            it = iter([(i, ) for i in range(start_idx, start_idx + 2)])
      |        else:
      |            it = iter([])
      |        return (it, {"partition-1": start_idx + 2})
      |    def readBetweenOffsets(self, start: dict, end: dict):
      |        start_idx = start["partition-1"]
      |        end_idx = end["partition-1"]
      |        return iter([(i, ) for i in range(start_idx, end_idx)])
      |""".stripMargin

  private val errorDataSourceName = "ErrorDataSource"

  test("SimpleDataSourceStreamReader run query and restart") {
    assume(shouldTestPandasUDFs)
    val dataSourceScript =
      s"""
         |from pyspark.sql.datasource import DataSource
         |$simpleDataStreamReaderScript
         |
         |class $dataSourceName(DataSource):
         |    def schema(self) -> str:
         |        return "id INT"
         |    def simpleStreamReader(self, schema):
         |        return SimpleDataStreamReader()
         |""".stripMargin
    val dataSource = createUserDefinedPythonDataSource(dataSourceName, dataSourceScript)
    spark.dataSource.registerPython(dataSourceName, dataSource)
    assert(spark.sessionState.dataSourceManager.dataSourceExists(dataSourceName))
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      val checkpointDir = new File(path, "checkpoint")
      val df = spark.readStream.format(dataSourceName).load()

      val stopSignal1 = new CountDownLatch(1)

      val q1 = df.writeStream
        .option("checkpointLocation", checkpointDir.getAbsolutePath)
        .foreachBatch((df: DataFrame, batchId: Long) => {
          df.cache()
          checkAnswer(df, Seq(Row(batchId * 2), Row(batchId * 2 + 1)))
          if (batchId == 10) stopSignal1.countDown()
        })
        .start()
      stopSignal1.await()
      assert(q1.recentProgress.forall(_.numInputRows == 2))
      q1.stop()
      q1.awaitTermination()

      val stopSignal2 = new CountDownLatch(1)
      val q2 = df.writeStream
        .option("checkpointLocation", checkpointDir.getAbsolutePath)
        .foreachBatch((df: DataFrame, batchId: Long) => {
          df.cache()
          checkAnswer(df, Seq(Row(batchId * 2), Row(batchId * 2 + 1)))
          if (batchId == 20) stopSignal2.countDown()
        })
        .start()
      stopSignal2.await()
      assert(q2.recentProgress.forall(_.numInputRows == 2))
      q2.stop()
      q2.awaitTermination()
    }
  }

  // Verify prefetch and cache pattern of SimpleDataSourceStreamReader handle empty
  // data batch correctly.
  test("SimpleDataSourceStreamReader read empty batch") {
    assume(shouldTestPandasUDFs)
    val dataSourceScript =
      s"""
         |from pyspark.sql.datasource import DataSource
         |$simpleDataStreamReaderWithEmptyBatchScript
         |
         |class $dataSourceName(DataSource):
         |    def schema(self) -> str:
         |        return "id INT"
         |    def simpleStreamReader(self, schema):
         |        return SimpleDataStreamReader()
         |""".stripMargin
    val dataSource = createUserDefinedPythonDataSource(dataSourceName, dataSourceScript)
    spark.dataSource.registerPython(dataSourceName, dataSource)
    assert(spark.sessionState.dataSourceManager.dataSourceExists(dataSourceName))
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      val checkpointDir = new File(path, "checkpoint")
      val df = spark.readStream.format(dataSourceName).load()

      val stopSignal = new CountDownLatch(1)

      val q = df.writeStream
        .option("checkpointLocation", checkpointDir.getAbsolutePath)
        .foreachBatch((df: DataFrame, batchId: Long) => {
          df.cache()
          if (batchId % 2 == 0) {
            checkAnswer(df, Seq(Row(batchId * 2), Row(batchId * 2 + 1)))
          } else {
            assert(df.isEmpty)
          }
          if (batchId == 10) stopSignal.countDown()
        })
        .start()
      stopSignal.await()
      q.stop()
      q.awaitTermination()
    }
  }

  test("SimpleDataSourceStreamReader read exactly once") {
    assume(shouldTestPandasUDFs)
    val dataSourceScript =
      s"""
         |from pyspark.sql.datasource import DataSource
         |$simpleDataStreamReaderScript
         |
         |class $dataSourceName(DataSource):
         |    def schema(self) -> str:
         |        return "id INT"
         |    def simpleStreamReader(self, schema):
         |        return SimpleDataStreamReader()
         |""".stripMargin
    val dataSource = createUserDefinedPythonDataSource(dataSourceName, dataSourceScript)
    spark.dataSource.registerPython(dataSourceName, dataSource)
    assert(spark.sessionState.dataSourceManager.dataSourceExists(dataSourceName))
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      val checkpointDir = new File(path, "checkpoint")
      val outputDir = new File(path, "output")
      val df = spark.readStream.format(dataSourceName).load()
      var lastBatchId = 0
      // Restart streaming query multiple times to verify exactly once guarantee.
      for (i <- 1 to 5) {

        if (i % 2 == 0) {
          // Remove the last entry of commit log to test replaying microbatch during restart.
          val offsetLog =
            new OffsetSeqLog(spark, new File(checkpointDir, "offsets").getCanonicalPath)
          val commitLog = new CommitLog(spark, new File(checkpointDir, "commits").getCanonicalPath)
          commitLog.purgeAfter(offsetLog.getLatest().get._1 - 1)
        }

        val q = df.writeStream
          .option("checkpointLocation", checkpointDir.getAbsolutePath)
          .format("json")
          .start(outputDir.getAbsolutePath)

        eventually(timeout(30.seconds)) {
          assert(q.recentProgress.length >= 5,
            s"Expected at least 5 progress updates but got ${q.recentProgress.length}. " +
            s"Query exception: ${q.exception}. " +
            s"Recent progress: ${q.recentProgress.mkString(", ")}")
        }
        q.stop()
        q.awaitTermination()
        lastBatchId = q.lastProgress.batchId.toInt
      }
      assert(lastBatchId > 20)
      val rowCount = spark.read.format("json").load(outputDir.getAbsolutePath).count()
      // There may be one uncommitted batch that is not recorded in query progress.
      // The number of batch can be lastBatchId + 1 or lastBatchId + 2.
      assert(rowCount == 2 * (lastBatchId + 1) || rowCount == 2 * (lastBatchId + 2))
      checkAnswer(
        spark.read.format("json").load(outputDir.getAbsolutePath),
        (0 until rowCount.toInt).map(Row(_))
      )
    }
  }

  test("initialOffset() method not implemented in SimpleDataSourceStreamReader") {
    assume(shouldTestPandasUDFs)
    val initialOffsetNotImplementedScript =
      s"""
         |from pyspark.sql.datasource import DataSource
         |from pyspark.sql.datasource import SimpleDataSourceStreamReader
         |class ErrorDataStreamReader(SimpleDataSourceStreamReader):
         |    ...
         |
         |class $errorDataSourceName(DataSource):
         |    def simpleStreamReader(self, schema):
         |        return ErrorDataStreamReader()
         |""".stripMargin
    val inputSchema = StructType.fromDDL("input BINARY")

    val dataSource =
      createUserDefinedPythonDataSource(errorDataSourceName, initialOffsetNotImplementedScript)
    spark.dataSource.registerPython(errorDataSourceName, dataSource)
    val pythonDs = new PythonDataSourceV2
    pythonDs.setShortName("ErrorDataSource")

    def testMicroBatchStreamError(action: String, msg: String)(
        func: PythonMicroBatchStreamWithAdmissionControl => Unit): Unit = {
      val options = CaseInsensitiveStringMap.empty()
      val runner = PythonMicroBatchStream.createPythonStreamingSourceRunner(
        pythonDs, errorDataSourceName, inputSchema, options)
      runner.init()

      val stream = new PythonMicroBatchStreamWithAdmissionControl(
        pythonDs,
        errorDataSourceName,
        inputSchema,
        options,
        runner
      )
      val err = intercept[SparkException] {
        func(stream)
      }
      checkErrorMatchPVals(
        err,
        condition = "PYTHON_STREAMING_DATA_SOURCE_RUNTIME_ERROR",
        parameters = Map(
          "action" -> action,
          "msg" -> "(.|\\n)*"
        )
      )
      assert(err.getMessage.contains(msg))
      stream.stop()
    }

    testMicroBatchStreamError(
      "initialOffset",
      "[NOT_IMPLEMENTED] initialOffset is not implemented") {
      stream =>
        stream.initialOffset()
    }
  }

  test("read() method throw error in SimpleDataSourceStreamReader") {
    assume(shouldTestPandasUDFs)
    val initialOffsetNotImplementedScript =
      s"""
         |from pyspark.sql.datasource import DataSource
         |from pyspark.sql.datasource import SimpleDataSourceStreamReader
         |class ErrorDataStreamReader(SimpleDataSourceStreamReader):
         |    def initialOffset(self):
         |        return {"partition": 1}
         |    def read(self, start):
         |        raise Exception("error reading available data")
         |
         |class $errorDataSourceName(DataSource):
         |    def simpleStreamReader(self, schema):
         |        return ErrorDataStreamReader()
         |""".stripMargin
    val inputSchema = StructType.fromDDL("input BINARY")

    val dataSource =
      createUserDefinedPythonDataSource(errorDataSourceName, initialOffsetNotImplementedScript)
    spark.dataSource.registerPython(errorDataSourceName, dataSource)
    val pythonDs = new PythonDataSourceV2
    pythonDs.setShortName("ErrorDataSource")

    def testMicroBatchStreamError(action: String, msg: String)(
        func: PythonMicroBatchStreamWithAdmissionControl => Unit): Unit = {
      val options = CaseInsensitiveStringMap.empty()
      val runner = PythonMicroBatchStream.createPythonStreamingSourceRunner(
        pythonDs, errorDataSourceName, inputSchema, options)
      runner.init()

      val stream = new PythonMicroBatchStreamWithAdmissionControl(
        pythonDs,
        errorDataSourceName,
        inputSchema,
        options,
        runner
      )
      val err = intercept[SparkException] {
        func(stream)
      }
      checkErrorMatchPVals(
        err,
        condition = "PYTHON_STREAMING_DATA_SOURCE_RUNTIME_ERROR",
        parameters = Map(
          "action" -> action,
          "msg" -> "(.|\\n)*"
        )
      )
      assert(err.getMessage.contains(msg))
      stream.stop()
    }

    testMicroBatchStreamError("latestOffset", "Exception: error reading available data") { stream =>
      stream.latestOffset(PythonStreamingSourceOffset("""{"partition": 0}"""),
        ReadLimit.allAvailable())
    }
  }

  test("SimpleDataSourceStreamReader with Trigger.AvailableNow") {
    assume(shouldTestPandasUDFs)
    val dataSourceScript =
      s"""
         |from pyspark.sql.datasource import DataSource
         |from pyspark.sql.datasource import SimpleDataSourceStreamReader
         |from pyspark.sql.streaming.datasource import SupportsTriggerAvailableNow
         |
         |class SimpleDataStreamReader(SimpleDataSourceStreamReader, SupportsTriggerAvailableNow):
         |    def initialOffset(self):
         |        return {"partition-1": 0}
         |    def read(self, start: dict):
         |        start_idx = start["partition-1"]
         |        end_offset = min(start_idx + 2, self.desired_end_offset)
         |        it = iter([(i, ) for i in range(start_idx, end_offset)])
         |        return (it, {"partition-1": end_offset})
         |    def readBetweenOffsets(self, start: dict, end: dict):
         |        start_idx = start["partition-1"]
         |        end_idx = end["partition-1"]
         |        return iter([(i, ) for i in range(start_idx, end_idx)])
         |    def prepareForTriggerAvailableNow(self):
         |        self.desired_end_offset = 10
         |
         |class $dataSourceName(DataSource):
         |    def schema(self) -> str:
         |        return "id INT"
         |    def simpleStreamReader(self, schema):
         |        return SimpleDataStreamReader()
         |""".stripMargin
    val dataSource = createUserDefinedPythonDataSource(dataSourceName, dataSourceScript)
    spark.dataSource.registerPython(dataSourceName, dataSource)
    assert(spark.sessionState.dataSourceManager.dataSourceExists(dataSourceName))
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      val checkpointDir = new File(path, "checkpoint")
      val outputDir = new File(path, "output")
      val df = spark.readStream.format(dataSourceName).load()
      val q = df.writeStream
        .option("checkpointLocation", checkpointDir.getAbsolutePath)
        .format("json")
        .trigger(Trigger.AvailableNow())
        .start(outputDir.getAbsolutePath)
      q.awaitTermination(waitTimeout.toMillis)
      val rowCount = spark.read.format("json").load(outputDir.getAbsolutePath).count()
      assert(rowCount === 10)
      checkAnswer(
        spark.read.format("json").load(outputDir.getAbsolutePath),
        (0 until rowCount.toInt).map(Row(_))
      )
    }
  }

  test("SPARK-54768: SimpleDataSourceStreamReader schema mismatch - prefetched batches") {
    assume(shouldTestPandasUDFs)
    val dataSourceScript =
      s"""
         |from pyspark.sql.datasource import DataSource, SimpleDataSourceStreamReader
         |import pyarrow as pa
         |
         |class SchemaMismatchSimpleReader(SimpleDataSourceStreamReader):
         |    def initialOffset(self):
         |        return {"offset": 0}
         |    def read(self, start: dict):
         |        # Return PyArrow RecordBatch with STRING when INT is expected.
         |        schema = pa.schema([pa.field("id", pa.string(), nullable=True)])
         |        batch = pa.RecordBatch.from_arrays(
         |            [pa.array(["1"], type=pa.string())], schema=schema)
         |        return iter([batch]), {"offset": 1}
         |    def readBetweenOffsets(self, start: dict, end: dict):
         |        return iter([])
         |    def commit(self, end: dict):
         |        pass
         |
         |class $errorDataSourceName(DataSource):
         |    def schema(self) -> str:
         |        return "id INT NOT NULL"
         |    def simpleStreamReader(self, schema):
         |        return SchemaMismatchSimpleReader()
         |""".stripMargin

    val dataSource = createUserDefinedPythonDataSource(errorDataSourceName, dataSourceScript)
    spark.dataSource.registerPython(errorDataSourceName, dataSource)

    val df = spark.readStream.format(errorDataSourceName).load()
    val err = intercept[StreamingQueryException] {
      val q = df.writeStream
        .trigger(Trigger.Once())
        .foreachBatch((df: DataFrame, _: Long) => {
          df.count()
          ()
        })
        .start()
      q.awaitTermination()
    }
    assert(err.getCause.isInstanceOf[SparkException])
    val cause = err.getCause.asInstanceOf[SparkException]
    checkErrorMatchPVals(
      cause,
      condition = "ARROW_TYPE_MISMATCH",
      parameters = Map(
        "operation" -> "Python streaming data source read",
        "outputTypes" -> "StructType\\(StructField\\(id,IntegerType,false\\)\\)",
        "actualDataTypes" -> "StructType\\(StructField\\(id,StringType,true\\)\\)"
      )
    )
  }
}

class PythonStreamingDataSourceSuite extends PythonDataSourceSuiteBase {
  val waitTimeout = 15.seconds

  protected def testDataStreamReaderScript: String =
    """
      |from pyspark.sql.datasource import DataSourceStreamReader, InputPartition
      |
      |class TestDataStreamReader(DataSourceStreamReader):
      |    current = 0
      |    def initialOffset(self):
      |        return {"offset": {"partition-1": 0}}
      |    def latestOffset(self):
      |        self.current += 2
      |        return {"offset": {"partition-1": self.current}}
      |    def partitions(self, start: dict, end: dict):
      |        start_index = start["offset"]["partition-1"]
      |        end_index = end["offset"]["partition-1"]
      |        return [InputPartition(i) for i in range(start_index, end_index)]
      |    def commit(self, end: dict):
      |        1 + 2
      |    def read(self, partition):
      |        yield (partition.value,)
      |""".stripMargin

  protected def errorDataStreamReaderScript: String =
    """
      |from pyspark.sql.datasource import DataSourceStreamReader, InputPartition
      |
      |class ErrorDataStreamReader(DataSourceStreamReader):
      |    def initialOffset(self):
      |        raise Exception("error reading initial offset")
      |    def latestOffset(self):
      |        raise Exception("error reading latest offset")
      |    def partitions(self, start: dict, end: dict):
      |        raise Exception("error planning partitions")
      |    def commit(self, end: dict):
      |        raise Exception("error committing offset")
      |    def read(self, partition):
      |        yield (0, partition.value)
      |        yield (1, partition.value)
      |        yield (2, partition.value)
      |""".stripMargin

  private val errorDataSourceName = "ErrorDataSource"

  test("Test PythonMicroBatchStream") {
    assume(shouldTestPandasUDFs)
    val dataSourceScript =
      s"""
         |from pyspark.sql.datasource import DataSource
         |$testDataStreamReaderScript
         |
         |class $dataSourceName(DataSource):
         |    def streamReader(self, schema):
         |        return TestDataStreamReader()
         |""".stripMargin
    val inputSchema = StructType.fromDDL("input BINARY")

    val dataSource = createUserDefinedPythonDataSource(dataSourceName, dataSourceScript)
    spark.dataSource.registerPython(dataSourceName, dataSource)
    val pythonDs = new PythonDataSourceV2
    pythonDs.setShortName("SimpleDataSource")

    val options = CaseInsensitiveStringMap.empty()
    val runner = PythonMicroBatchStream.createPythonStreamingSourceRunner(
      pythonDs, dataSourceName, inputSchema, options)
    runner.init()

    val stream = new PythonMicroBatchStream(
      pythonDs,
      dataSourceName,
      inputSchema,
      options,
      runner
    )

    var startOffset = stream.initialOffset()
    assert(startOffset.json == "{\"offset\": {\"partition-1\": 0}}")
    for (i <- 1 to 50) {
      val endOffset = stream.latestOffset()
      assert(endOffset.json == s"""{"offset": {"partition-1": ${2 * i}}}""")
      assert(stream.planInputPartitions(startOffset, endOffset).size == 2)
      stream.commit(endOffset)
      startOffset = endOffset
    }
    stream.stop()
  }

  test("Read from test data stream source") {
    assume(shouldTestPandasUDFs)
    val dataSourceScript =
      s"""
         |from pyspark.sql.datasource import DataSource
         |$testDataStreamReaderScript
         |
         |class $dataSourceName(DataSource):
         |    def schema(self) -> str:
         |        return "id INT"
         |    def streamReader(self, schema):
         |        return TestDataStreamReader()
         |""".stripMargin

    val dataSource = createUserDefinedPythonDataSource(dataSourceName, dataSourceScript)
    spark.dataSource.registerPython(dataSourceName, dataSource)
    assert(spark.sessionState.dataSourceManager.dataSourceExists(dataSourceName))
    val df = spark.readStream.format(dataSourceName).load()

    val stopSignal = new CountDownLatch(1)

    val q = df.writeStream
      .foreachBatch((df: DataFrame, batchId: Long) => {
        // checkAnswer may materialize the dataframe more than once
        // Cache here to make sure the numInputRows metrics is consistent.
        df.cache()
        checkAnswer(df, Seq(Row(batchId * 2), Row(batchId * 2 + 1)))
        if (batchId > 30) stopSignal.countDown()
      })
      .trigger(ProcessingTimeTrigger(0))
      .start()
    stopSignal.await()
    assert(q.recentProgress.forall(_.numInputRows == 2))
    q.stop()
    q.awaitTermination()
  }

  // Verify that socket between python runner and JVM doesn't timeout with large trigger interval.
  test("Read from test data stream source, trigger interval=20 seconds") {
    assume(shouldTestPandasUDFs)
    val dataSourceScript =
      s"""
         |from pyspark.sql.datasource import DataSource
         |$testDataStreamReaderScript
         |
         |class $dataSourceName(DataSource):
         |    def schema(self) -> str:
         |        return "id INT"
         |    def streamReader(self, schema):
         |        return TestDataStreamReader()
         |""".stripMargin

    val dataSource = createUserDefinedPythonDataSource(dataSourceName, dataSourceScript)
    spark.dataSource.registerPython(dataSourceName, dataSource)
    assert(spark.sessionState.dataSourceManager.dataSourceExists(dataSourceName))
    val df = spark.readStream.format(dataSourceName).load()

    val stopSignal = new CountDownLatch(1)

    val q = df.writeStream
      .foreachBatch((df: DataFrame, batchId: Long) => {
        // checkAnswer may materialize the dataframe more than once
        // Cache here to make sure the numInputRows metrics is consistent.
        df.cache()
        checkAnswer(df, Seq(Row(batchId * 2), Row(batchId * 2 + 1)))
        if (batchId >= 2) stopSignal.countDown()
      })
      .trigger(ProcessingTimeTrigger(20 * 1000))
      .start()
    stopSignal.await()
    assert(q.recentProgress.forall(_.numInputRows == 2))
    q.stop()
    q.awaitTermination()
    assert(q.exception.isEmpty)
  }

  test("Streaming data source read with custom partitions") {
    assume(shouldTestPandasUDFs)
    val dataSourceScript =
      s"""
         |from pyspark.sql.datasource import DataSource, DataSourceStreamReader, InputPartition
         |class RangePartition(InputPartition):
         |    def __init__(self, start, end):
         |        self.start = start
         |        self.end = end
         |
         |class TestDataStreamReader(DataSourceStreamReader):
         |    current = 0
         |    def initialOffset(self):
         |        return {"offset": 0}
         |    def latestOffset(self):
         |        self.current += 2
         |        return {"offset": self.current}
         |    def partitions(self, start: dict, end: dict):
         |        return [RangePartition(start["offset"], end["offset"])]
         |    def commit(self, end: dict):
         |        1 + 2
         |    def read(self, partition: RangePartition):
         |        start, end = partition.start, partition.end
         |        for i in range(start, end):
         |            yield (i, )
         |
         |
         |class $dataSourceName(DataSource):
         |    def schema(self) -> str:
         |        return "id INT"
         |
         |    def streamReader(self, schema):
         |        return TestDataStreamReader()
         |""".stripMargin
    val dataSource = createUserDefinedPythonDataSource(dataSourceName, dataSourceScript)
    spark.dataSource.registerPython(dataSourceName, dataSource)
    assert(spark.sessionState.dataSourceManager.dataSourceExists(dataSourceName))
    val df = spark.readStream.format(dataSourceName).load()

    val stopSignal = new CountDownLatch(1)

    val q = df.writeStream
      .foreachBatch((df: DataFrame, batchId: Long) => {
        // checkAnswer may materialize the dataframe more than once
        // Cache here to make sure the numInputRows metrics is consistent.
        df.cache()
        checkAnswer(df, Seq(Row(batchId * 2), Row(batchId * 2 + 1)))
        if (batchId > 30) stopSignal.countDown()
      })
      .trigger(ProcessingTimeTrigger(0))
      .start()
    stopSignal.await()
    assert(q.recentProgress.forall(_.numInputRows == 2))
    q.stop()
    q.awaitTermination()
  }

  private val testAdmissionControlScript =
    s"""
       |from pyspark.sql.datasource import DataSource
       |from pyspark.sql.datasource import (
       |    DataSourceStreamReader,
       |    InputPartition,
       |)
       |from pyspark.sql.streaming.datasource import (
       |    ReadAllAvailable,
       |    ReadLimit,
       |    ReadMaxRows,
       |)
       |
       |class TestDataStreamReader(
       |    DataSourceStreamReader,
       |):
       |    def initialOffset(self):
       |        return {"partition-1": 0}
       |    def getDefaultReadLimit(self):
       |        return ReadMaxRows(2)
       |    def latestOffset(self, start: dict, limit: ReadLimit):
       |        start_idx = start["partition-1"]
       |        if isinstance(limit, ReadAllAvailable):
       |            end_offset = start_idx + 10
       |        else:
       |            assert isinstance(limit, ReadMaxRows), ("Expected ReadMaxRows read "
       |                                                    "limit but got "
       |                                                    + str(type(limit)))
       |            end_offset = start_idx + limit.max_rows
       |        return {"partition-1": end_offset}
       |    def reportLatestOffset(self):
       |        return {"partition-1": 1000000}
       |    def partitions(self, start: dict, end: dict):
       |        start_index = start["partition-1"]
       |        end_index = end["partition-1"]
       |        return [InputPartition(i) for i in range(start_index, end_index)]
       |    def read(self, partition):
       |        yield (partition.value,)
       |
       |class $dataSourceName(DataSource):
       |    def schema(self) -> str:
       |        return "id INT"
       |    def streamReader(self, schema):
       |        return TestDataStreamReader()
       |""".stripMargin

  private val testAvailableNowScript =
    s"""
       |from pyspark.sql.datasource import DataSource
       |from pyspark.sql.datasource import (
       |    DataSourceStreamReader,
       |    InputPartition,
       |)
       |from pyspark.sql.streaming.datasource import (
       |    ReadAllAvailable,
       |    ReadLimit,
       |    ReadMaxRows,
       |    SupportsTriggerAvailableNow
       |)
       |
       |class TestDataStreamReader(
       |    DataSourceStreamReader,
       |    SupportsTriggerAvailableNow
       |):
       |    def initialOffset(self):
       |        return {"partition-1": 0}
       |    def getDefaultReadLimit(self):
       |        return ReadMaxRows(2)
       |    def latestOffset(self, start: dict, limit: ReadLimit):
       |        start_idx = start["partition-1"]
       |        if isinstance(limit, ReadAllAvailable):
       |            end_offset = start_idx + 5
       |        else:
       |            assert isinstance(limit, ReadMaxRows), ("Expected ReadMaxRows read "
       |                                                    "limit but got "
       |                                                    + str(type(limit)))
       |            end_offset = start_idx + limit.max_rows
       |        end_offset = min(end_offset, self.desired_end_offset)
       |        return {"partition-1": end_offset}
       |    def reportLatestOffset(self):
       |        return {"partition-1": 1000000}
       |    def prepareForTriggerAvailableNow(self):
       |        self.desired_end_offset = 10
       |    def partitions(self, start: dict, end: dict):
       |        start_index = start["partition-1"]
       |        end_index = end["partition-1"]
       |        return [InputPartition(i) for i in range(start_index, end_index)]
       |    def read(self, partition):
       |        yield (partition.value,)
       |
       |class $dataSourceName(DataSource):
       |    def schema(self) -> str:
       |        return "id INT"
       |    def streamReader(self, schema):
       |        return TestDataStreamReader()
       |""".stripMargin

  test("DataSourceStreamReader with Admission Control, Trigger.Once") {
    assume(shouldTestPandasUDFs)
    val dataSource = createUserDefinedPythonDataSource(dataSourceName, testAdmissionControlScript)
    spark.dataSource.registerPython(dataSourceName, dataSource)
    assert(spark.sessionState.dataSourceManager.dataSourceExists(dataSourceName))
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      val checkpointDir = new File(path, "checkpoint")
      val outputDir = new File(path, "output")
      val df = spark.readStream.format(dataSourceName).load()
      val q = df.writeStream
        .option("checkpointLocation", checkpointDir.getAbsolutePath)
        .format("json")
        // Use Trigger.Once here by intention to test read with admission control.
        .trigger(Trigger.Once())
        .start(outputDir.getAbsolutePath)
      q.awaitTermination(waitTimeout.toMillis)

      assert(q.recentProgress.length === 1)
      assert(q.lastProgress.numInputRows === 10)
      assert(q.lastProgress.sources(0).numInputRows === 10)
      assert(q.lastProgress.sources(0).latestOffset === """{"partition-1": 1000000}""")

      val rowCount = spark.read.format("json").load(outputDir.getAbsolutePath).count()
      assert(rowCount === 10)
      checkAnswer(
        spark.read.format("json").load(outputDir.getAbsolutePath),
        (0 until rowCount.toInt).map(Row(_))
      )
    }
  }

  test("DataSourceStreamReader with Admission Control, processing time trigger") {
    assume(shouldTestPandasUDFs)
    val dataSource = createUserDefinedPythonDataSource(dataSourceName, testAdmissionControlScript)
    spark.dataSource.registerPython(dataSourceName, dataSource)
    assert(spark.sessionState.dataSourceManager.dataSourceExists(dataSourceName))
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      val checkpointDir = new File(path, "checkpoint")
      val df = spark.readStream.format(dataSourceName).load()

      val stopSignal = new CountDownLatch(1)

      val q = df.writeStream
        .option("checkpointLocation", checkpointDir.getAbsolutePath)
        .foreachBatch((df: DataFrame, batchId: Long) => {
          df.cache()
          checkAnswer(df, Seq(Row(batchId * 2), Row(batchId * 2 + 1)))
          if (batchId == 10) stopSignal.countDown()
        })
        .trigger(Trigger.ProcessingTime(0))
        .start()
      stopSignal.await()
      q.stop()
      q.awaitTermination()

      assert(q.recentProgress.length >= 10)
      q.recentProgress.foreach { progress =>
        assert(progress.numInputRows === 2)
        assert(progress.sources(0).numInputRows === 2)
        assert(progress.sources(0).latestOffset === """{"partition-1": 1000000}""")
      }
    }
  }

  test("DataSourceStreamReader with Trigger.AvailableNow") {
    assume(shouldTestPandasUDFs)
    val dataSource = createUserDefinedPythonDataSource(dataSourceName, testAvailableNowScript)
    spark.dataSource.registerPython(dataSourceName, dataSource)
    assert(spark.sessionState.dataSourceManager.dataSourceExists(dataSourceName))
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      val checkpointDir = new File(path, "checkpoint")
      val outputDir = new File(path, "output")
      val df = spark.readStream.format(dataSourceName).load()
      val q = df.writeStream
        .option("checkpointLocation", checkpointDir.getAbsolutePath)
        .format("json")
        .trigger(Trigger.AvailableNow())
        .start(outputDir.getAbsolutePath)
      q.awaitTermination(waitTimeout.toMillis)

      // 2 rows * 5 batches = 10 rows
      assert(q.recentProgress.length === 5)
      q.recentProgress.foreach { progress =>
        assert(progress.numInputRows === 2)
        assert(progress.sources(0).numInputRows === 2)
        assert(progress.sources(0).latestOffset === """{"partition-1": 1000000}""")
      }

      val rowCount = spark.read.format("json").load(outputDir.getAbsolutePath).count()
      assert(rowCount === 10)
      checkAnswer(
        spark.read.format("json").load(outputDir.getAbsolutePath),
        (0 until rowCount.toInt).map(Row(_))
      )
    }
  }

  test("Error creating stream reader") {
    assume(shouldTestPandasUDFs)
    val dataSourceScript =
      s"""
         |from pyspark.sql.datasource import DataSource
         |class $dataSourceName(DataSource):
         |    def schema(self) -> str:
         |        return "id INT"
         |    def streamReader(self, schema):
         |        raise Exception("error creating stream reader")
         |""".stripMargin
    val dataSource =
      createUserDefinedPythonDataSource(name = dataSourceName, pythonScript = dataSourceScript)
    spark.dataSource.registerPython(dataSourceName, dataSource)

    val err = intercept[StreamingQueryException] {
      val q = spark.readStream.format(dataSourceName).load().writeStream.format("console").start()
      q.awaitTermination()
    }
    assert(err.getCondition == "STREAM_FAILED")
    assert(err.getMessage.contains("error creating stream reader"))
  }

  test("Streaming data source read error") {
    assume(shouldTestPandasUDFs)
    val dataSourceScript =
      s"""
         |from pyspark.sql.datasource import DataSource, DataSourceStreamReader, InputPartition
         |class RangePartition(InputPartition):
         |    def __init__(self, start, end):
         |        self.start = start
         |        self.end = end
         |
         |class SimpleDataStreamReader(DataSourceStreamReader):
         |    current = 0
         |    def initialOffset(self):
         |        return {"offset": "0"}
         |    def latestOffset(self):
         |        self.current += 2
         |        return {"offset": str(self.current)}
         |    def partitions(self, start: dict, end: dict):
         |        return [RangePartition(int(start["offset"]), int(end["offset"]))]
         |    def commit(self, end: dict):
         |        1 + 2
         |    def read(self, partition: RangePartition):
         |        raise Exception("error reading data")
         |
         |
         |class $dataSourceName(DataSource):
         |    def schema(self) -> str:
         |        return "id INT"
         |
         |    def streamReader(self, schema):
         |        return SimpleDataStreamReader()
         |""".stripMargin
    val dataSource = createUserDefinedPythonDataSource(dataSourceName, dataSourceScript)
    spark.dataSource.registerPython(dataSourceName, dataSource)
    assert(spark.sessionState.dataSourceManager.dataSourceExists(dataSourceName))
    val df = spark.readStream.format(dataSourceName).load()

    val err = intercept[StreamingQueryException] {
      val q = df.writeStream
        .foreachBatch((df: DataFrame, _: Long) => {
          df.count()
          ()
        })
        .start()
      q.awaitTermination()
    }
    assert(err.getMessage.contains("error reading data"))
  }

  test("Method not implemented in stream reader") {
    assume(shouldTestPandasUDFs)
    val dataSourceScript =
      s"""
         |from pyspark.sql.datasource import DataSource
         |from pyspark.sql.datasource import DataSourceStreamReader
         |class ErrorDataStreamReader(DataSourceStreamReader):
         |    def read(self, partition):
         |        yield (0, partition.value)
         |
         |class $errorDataSourceName(DataSource):
         |    def streamReader(self, schema):
         |        return ErrorDataStreamReader()
         |""".stripMargin
    val inputSchema = StructType.fromDDL("input BINARY")

    val dataSource = createUserDefinedPythonDataSource(errorDataSourceName, dataSourceScript)
    spark.dataSource.registerPython(errorDataSourceName, dataSource)
    val pythonDs = new PythonDataSourceV2
    pythonDs.setShortName("ErrorDataSource")

    def testMicroBatchStreamError(action: String, msg: String)(
        func: PythonMicroBatchStreamWithAdmissionControl => Unit): Unit = {
      val options = CaseInsensitiveStringMap.empty()
      val runner = PythonMicroBatchStream.createPythonStreamingSourceRunner(
        pythonDs, errorDataSourceName, inputSchema, options)
      runner.init()

      // New default for python stream reader is with Admission Control
      val stream = new PythonMicroBatchStreamWithAdmissionControl(
        pythonDs,
        errorDataSourceName,
        inputSchema,
        options,
        runner
      )
      val err = intercept[SparkException] {
        func(stream)
      }
      checkErrorMatchPVals(
        err,
        condition = "PYTHON_STREAMING_DATA_SOURCE_RUNTIME_ERROR",
        parameters = Map(
          "action" -> action,
          "msg" -> "(.|\\n)*"
        )
      )
      assert(err.getMessage.contains(msg))
      stream.stop()
    }

    testMicroBatchStreamError(
      "initialOffset",
      "[NOT_IMPLEMENTED] initialOffset is not implemented") {
      stream =>
        stream.initialOffset()
    }

    val offset = PythonStreamingSourceOffset("{\"offset\": \"2\"}")
    testMicroBatchStreamError("latestOffset", "[NOT_IMPLEMENTED] latestOffset is not implemented") {
      stream =>
        val readLimit = PythonStreamingSourceReadLimit(
          PythonStreamingSourceRunner.READ_ALL_AVAILABLE_JSON)
        stream.latestOffset(offset, readLimit)
    }

    testMicroBatchStreamError("planPartitions", "[NOT_IMPLEMENTED] partitions is not implemented") {
      stream =>
        stream.planInputPartitions(offset, offset)
    }
  }

  test("Error in stream reader") {
    assume(shouldTestPandasUDFs)
    val dataSourceScript =
      s"""
         |from pyspark.sql.datasource import DataSource
         |$errorDataStreamReaderScript
         |
         |class $errorDataSourceName(DataSource):
         |    def streamReader(self, schema):
         |        return ErrorDataStreamReader()
         |""".stripMargin
    val inputSchema = StructType.fromDDL("input BINARY")

    val dataSource = createUserDefinedPythonDataSource(errorDataSourceName, dataSourceScript)
    spark.dataSource.registerPython(errorDataSourceName, dataSource)
    val pythonDs = new PythonDataSourceV2
    pythonDs.setShortName("ErrorDataSource")
    val offset = PythonStreamingSourceOffset("{\"offset\": 2}")

    def testMicroBatchStreamError(action: String, msg: String)(
        func: PythonMicroBatchStream => Unit): Unit = {
      val options = CaseInsensitiveStringMap.empty()
      val runner = PythonMicroBatchStream.createPythonStreamingSourceRunner(
        pythonDs, errorDataSourceName, inputSchema, options)
      runner.init()

      val stream = new PythonMicroBatchStream(
        pythonDs,
        errorDataSourceName,
        inputSchema,
        options,
        runner
      )
      val err = intercept[SparkException] {
        func(stream)
      }
      checkErrorMatchPVals(
        err,
        condition = "PYTHON_STREAMING_DATA_SOURCE_RUNTIME_ERROR",
        parameters = Map(
          "action" -> action,
          "msg" -> "(.|\\n)*"
        )
      )
      assert(err.getMessage.contains(msg))
      stream.stop()
    }

    testMicroBatchStreamError("initialOffset", "error reading initial offset") { stream =>
      stream.initialOffset()
    }

    testMicroBatchStreamError("latestOffset", "error reading latest offset") { stream =>
      stream.latestOffset()
    }

    testMicroBatchStreamError("planPartitions", "error planning partitions") { stream =>
      stream.planInputPartitions(offset, offset)
    }

    testMicroBatchStreamError("commitSource", "error committing offset") { stream =>
      stream.commit(offset)
    }
  }

  test("empty batch for stream when latestOffset produces the same offset with start") {
    assume(shouldTestPandasUDFs)
    val dataSourceScript =
      s"""
         |from pyspark.sql.datasource import DataSource, DataSourceStreamReader, InputPartition
         |
         |class ConditionalEmptyBatchReader(DataSourceStreamReader):
         |    call_count = 0
         |
         |    def initialOffset(self):
         |        return {"offset": 0}
         |
         |    def latestOffset(self, start, limit):
         |        self.call_count += 1
         |        # For odd batches (call count - 1 is odd), return the same offset
         |        # (simulating no new data)
         |        # For even batches, advance the offset by 2
         |        if (self.call_count - 1) % 2 == 1:
         |            # Return current offset without advancing
         |            return start
         |        else:
         |            # Advance offset by 2
         |            return {"offset": start["offset"] + 2}
         |
         |    def partitions(self, start: dict, end: dict):
         |        start_offset = start["offset"]
         |        end_offset = end["offset"]
         |        # Create partitions for the range [start, end)
         |        return [InputPartition(i) for i in range(start_offset, end_offset)]
         |
         |    def commit(self, end: dict):
         |        pass
         |
         |    def read(self, partition):
         |        # Yield a value with a marker to identify this is from Python source
         |        yield (partition.value, 1000)
         |
         |class $dataSourceName(DataSource):
         |    def schema(self) -> str:
         |        return "id INT, source INT"
         |
         |    def streamReader(self, schema):
         |        return ConditionalEmptyBatchReader()
         |""".stripMargin

    val dataSource =
      createUserDefinedPythonDataSource(dataSourceName, dataSourceScript)
    spark.dataSource.registerPython(dataSourceName, dataSource)

    val pythonDF = spark.readStream.format(dataSourceName).load()

    // Create a rate source that produces data every microbatch
    val rateDF = spark.readStream
      .format("rate-micro-batch")
      .option("rowsPerBatch", "2")
      .load()
      .selectExpr("CAST(value AS INT) as id", "2000 as source")

    // Union the two sources
    val unionDF = pythonDF.union(rateDF)

    val stopSignal = new CountDownLatch(1)
    var batchesWithoutPythonData = 0
    var batchesWithPythonData = 0

    val q = unionDF.writeStream
      .foreachBatch((df: DataFrame, batchId: Long) => {
        df.cache()
        val pythonRows = df.filter("source = 1000").count()
        val rateRows = df.filter("source = 2000").count()

        // Rate source should always produce 2 rows per batch
        assert(
          rateRows == 2,
          s"Batch $batchId: Expected 2 rows from rate source, got $rateRows"
        )

        // Python source should produce 0 rows for odd batches (empty batches)
        // and 2 rows for even batches
        if (batchId % 2 == 1) {
          // Odd batch - Python source should return same offset, producing empty batch
          assert(
            pythonRows == 0,
            s"Batch $batchId: Expected 0 rows from Python source (empty batch), got $pythonRows"
          )
          batchesWithoutPythonData += 1
        } else {
          // Even batch - Python source should advance offset and produce data
          assert(
            pythonRows == 2,
            s"Batch $batchId: Expected 2 rows from Python source, got $pythonRows"
          )
          batchesWithPythonData += 1
        }

        if (batchId >= 7) stopSignal.countDown()
      })
      .trigger(ProcessingTimeTrigger(0))
      .start()

    eventually(timeout(waitTimeout)) {
      assert(
        stopSignal.await(1, TimeUnit.SECONDS),
        s"""
           |Streaming query did not reach specific microbatch in time,
           |# of batches with data from python stream source: $batchesWithPythonData,
           |# of batches without data from python stream source: $batchesWithoutPythonData,
           |recentProgress: ${q.recentProgress.mkString("[", ", ", "]")},
           |exception (if any): ${q.exception}
           |""".stripMargin
      )
    }

    q.stop()
    q.awaitTermination()

    // Verify that we observed both types of batches
    assert(
      batchesWithoutPythonData >= 4,
      s"Expected at least 4 batches without Python data, got $batchesWithoutPythonData"
    )
    assert(
      batchesWithPythonData >= 4,
      s"Expected at least 4 batches with Python data, got $batchesWithPythonData"
    )
  }
}

class PythonStreamingDataSourceWriteSuite extends PythonDataSourceSuiteBase {

  import testImplicits._

  val waitTimeout = 15.seconds

  protected def simpleDataStreamWriterScript: String =
    s"""
       |import json
       |import uuid
       |import os
       |from pyspark import TaskContext
       |from pyspark.sql.datasource import DataSource, DataSourceStreamWriter
       |from pyspark.sql.datasource import WriterCommitMessage
       |
       |class SimpleDataSourceStreamWriter(DataSourceStreamWriter):
       |    def __init__(self, options, overwrite):
       |        self.options = options
       |        self.overwrite = overwrite
       |
       |    def write(self, iterator):
       |        context = TaskContext.get()
       |        partition_id = context.partitionId()
       |        path = self.options.get("path")
       |        assert path is not None
       |        output_path = os.path.join(path, f"{partition_id}.json")
       |        cnt = 0
       |        mode = "w" if self.overwrite else "a"
       |        with open(output_path, mode) as file:
       |            for row in iterator:
       |                file.write(json.dumps(row.asDict()) + "\\n")
       |        return WriterCommitMessage()
       |
       |class SimpleDataSource(DataSource):
       |    def schema(self) -> str:
       |        return "id INT"
       |    def streamWriter(self, schema, overwrite):
       |        return SimpleDataSourceStreamWriter(self.options, overwrite)
       |""".stripMargin

  Seq("append", "complete").foreach { mode =>
    test(s"data source stream write - $mode mode") {
      assume(shouldTestPandasUDFs)
      val dataSource =
        createUserDefinedPythonDataSource(dataSourceName, simpleDataStreamWriterScript)
      spark.dataSource.registerPython(dataSourceName, dataSource)
      val inputData = MemoryStream[Int]
      withTempDir { dir =>
        val path = dir.getAbsolutePath
        val checkpointDir = new File(path, "checkpoint")
        checkpointDir.mkdir()
        val outputDir = new File(path, "output")
        outputDir.mkdir()
        val streamDF = if (mode == "append") {
          inputData.toDF()
        } else {
          // Complete mode only supports stateful aggregation
          inputData.toDF()
            .groupBy("value").count()
        }
        def resultDf: DataFrame = spark.read.format("json")
          .load(outputDir.getAbsolutePath)
        val q = streamDF
          .writeStream
          .format(dataSourceName)
          .outputMode(mode)
          .option("checkpointLocation", checkpointDir.getAbsolutePath)
          .start(outputDir.getAbsolutePath)

        inputData.addData(1, 2, 3)
        eventually(timeout(waitTimeout)) {
          if (mode == "append") {
            checkAnswer(
              resultDf,
              Seq(Row(1), Row(2), Row(3)))
          } else {
            checkAnswer(
              resultDf.select("value", "count"),
              Seq(Row(1, 1), Row(2, 1), Row(3, 1)))
          }
        }

        inputData.addData(1, 4)
        eventually(timeout(waitTimeout)) {
          if (mode == "append") {
            checkAnswer(
              resultDf,
              Seq(Row(1), Row(2), Row(3), Row(4), Row(1)))
          } else {
            checkAnswer(
              resultDf.select("value", "count"),
              Seq(Row(1, 2), Row(2, 1), Row(3, 1), Row(4, 1)))
          }
        }

        q.stop()
        q.awaitTermination()
        assert(q.exception.isEmpty)
      }
    }
  }

  // Verify that commit runner work correctly with large timeout interval.
  test(s"data source stream write, trigger interval=20 seconds") {
    assume(shouldTestPandasUDFs)
    val dataSource =
      createUserDefinedPythonDataSource(dataSourceName, simpleDataStreamWriterScript)
    spark.dataSource.registerPython(dataSourceName, dataSource)
    val inputData = MemoryStream[Int](numPartitions = 3)
    val df = inputData.toDF()
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      val checkpointDir = new File(path, "checkpoint")
      checkpointDir.mkdir()
      val outputDir = new File(path, "output")
      outputDir.mkdir()
      val q = df
        .writeStream
        .format(dataSourceName)
        .option("checkpointLocation", checkpointDir.getAbsolutePath)
        .trigger(ProcessingTimeTrigger(20 * 1000))
        .start(outputDir.getAbsolutePath)
      def resultDf: DataFrame = spark.read.format("json")
        .load(outputDir.getAbsolutePath)

      inputData.addData(1 to 3)
      eventually(timeout(waitTimeout * 5)) {
        assert(q.lastProgress.batchId >= 1)
      }
      checkAnswer(resultDf, (1 to 3).map(Row(_)))

      inputData.addData(4 to 6)
      eventually(timeout(waitTimeout * 5)) {
        assert(q.lastProgress.batchId >= 2)
      }
      checkAnswer(resultDf, (1 to 6).map(Row(_)))
      q.stop()
      q.awaitTermination()
      assert(q.exception.isEmpty)
    }
  }

  test("streaming sink write commit and abort") {
    assume(shouldTestPandasUDFs)
    // The data source write the number of rows and partitions into batchId.json in
    // the output directory in commit() function. If aborting a microbatch, it writes
    // batchId.txt into output directory.
    val dataSourceScript =
      s"""
         |import json
         |import os
         |from dataclasses import dataclass
         |from pyspark import TaskContext
         |from pyspark.sql.datasource import DataSource, DataSourceStreamWriter, WriterCommitMessage
         |
         |@dataclass
         |class SimpleCommitMessage(WriterCommitMessage):
         |    partition_id: int
         |    count: int
         |
         |class SimpleDataSourceStreamWriter(DataSourceStreamWriter):
         |    def __init__(self, options):
         |        self.options = options
         |        self.path = self.options.get("path")
         |        assert self.path is not None
         |
         |    def write(self, iterator):
         |        context = TaskContext.get()
         |        partition_id = context.partitionId()
         |        cnt = 0
         |        for row in iterator:
         |            if row.value > 50:
         |                raise Exception("invalid value")
         |            cnt += 1
         |        return SimpleCommitMessage(partition_id=partition_id, count=cnt)
         |
         |    def commit(self, messages, batchId) -> None:
         |        status = dict(num_partitions=len(messages), rows=sum(m.count for m in messages))
         |
         |        with open(os.path.join(self.path, f"{batchId}.json"), "a") as file:
         |            file.write(json.dumps(status) + "\\n")
         |
         |    def abort(self, messages, batchId) -> None:
         |        with open(os.path.join(self.path, f"{batchId}.txt"), "w") as file:
         |            file.write(f"failed in batch {batchId}")
         |
         |class SimpleDataSource(DataSource):
         |    def streamWriter(self, schema, overwrite):
         |        return SimpleDataSourceStreamWriter(self.options)
         |""".stripMargin
    val dataSource = createUserDefinedPythonDataSource(dataSourceName, dataSourceScript)
    spark.dataSource.registerPython(dataSourceName, dataSource)
    val inputData = MemoryStream[Int](numPartitions = 3)
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      val checkpointDir = new File(path, "checkpoint")
      checkpointDir.mkdir()
      val outputDir = new File(path, "output")
      outputDir.mkdir()
      val q = inputData.toDF()
        .writeStream
        .format(dataSourceName)
        .outputMode("append")
        .option("checkpointLocation", checkpointDir.getAbsolutePath)
        .start(outputDir.getAbsolutePath)

      def metadataDf: DataFrame = spark.read.format("json")
        .load(outputDir.getAbsolutePath)

      // Batch 0-2 should succeed and json commit files are written.
      inputData.addData(1 to 30)
      eventually(timeout(waitTimeout)) {
        checkAnswer(metadataDf, Seq(Row(3, 30)))
      }

      inputData.addData(31 to 50)
      eventually(timeout(waitTimeout)) {
        checkAnswer(metadataDf, Seq(Row(3, 30), Row(3, 20)))
      }

      // Write and commit an empty batch.
      inputData.addData(Seq.empty)
      eventually(timeout(waitTimeout)) {
        checkAnswer(metadataDf, Seq(Row(3, 30), Row(3, 20), Row(3, 0)))
      }

      // The sink throws exception when encountering value > 50 in batch 3.
      // The streamWriter will write error message in 3.txt during abort().
      inputData.addData(51 to 100)
      eventually(timeout(waitTimeout)) {
        checkAnswer(
          spark.read.text(outputDir.getAbsolutePath + "/3.txt"),
          Seq(Row("failed in batch 3")))
      }

      q.stop()
      assert(q.exception.get.message.contains("invalid value"))
    }
  }

  test("python streaming sink: invalid write mode") {
    assume(shouldTestPandasUDFs)
    // The data source write the number of rows and partitions into batchId.json in
    // the output directory in commit() function. If aborting a microbatch, it writes
    // batchId.txt into output directory.

    val dataSource = createUserDefinedPythonDataSource(dataSourceName, simpleDataStreamWriterScript)
    spark.dataSource.registerPython(dataSourceName, dataSource)

    withTempDir { dir =>
      val path = dir.getAbsolutePath
      val checkpointDir = new File(path, "checkpoint")
      checkpointDir.mkdir()
      val outputDir = new File(path, "output")
      outputDir.mkdir()

      def runQuery(mode: String): Unit = {
        val inputData = MemoryStream[Int]
        withTempDir { dir =>
          val path = dir.getAbsolutePath
          val checkpointDir = new File(path, "checkpoint")
          checkpointDir.mkdir()
          val outputDir = new File(path, "output")
          outputDir.mkdir()
          val q = inputData.toDF()
            .writeStream
            .format(dataSourceName)
            .outputMode(mode)
            .option("checkpointLocation", checkpointDir.getAbsolutePath)
            .start(outputDir.getAbsolutePath)
          q.stop()
          q.awaitTermination()
        }
      }

      runQuery("append")
      runQuery("update")

      // Complete mode is not supported for stateless query.
      checkError(
        exception = intercept[AnalysisException] {
          runQuery("complete")
        },
        condition = "STREAMING_OUTPUT_MODE.UNSUPPORTED_OPERATION",
        sqlState = "42KDE",
        parameters = Map(
          "outputMode" -> "complete",
          "operation" -> "no streaming aggregations"))

      // Query should fail in planning with "invalid" mode.
      val error2 = intercept[IllegalArgumentException] {
        runQuery("invalid")
      }
      assert(error2.getMessage.contains("invalid"))
    }
  }
}
