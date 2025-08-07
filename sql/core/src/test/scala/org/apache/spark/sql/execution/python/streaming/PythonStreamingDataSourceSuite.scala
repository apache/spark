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
import java.util.concurrent.CountDownLatch

import scala.concurrent.duration._

import org.apache.spark.SparkException
import org.apache.spark.sql.{AnalysisException, DataFrame, Row}
import org.apache.spark.sql.IntegratedUDFTestUtils.{createUserDefinedPythonDataSource, shouldTestPandasUDFs}
import org.apache.spark.sql.execution.datasources.v2.python.{PythonDataSourceV2, PythonMicroBatchStream, PythonStreamingSourceOffset}
import org.apache.spark.sql.execution.python.PythonDataSourceSuiteBase
import org.apache.spark.sql.execution.streaming.{CommitLog, MemoryStream, OffsetSeqLog, ProcessingTimeTrigger}
import org.apache.spark.sql.streaming.StreamingQueryException
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

        while (q.recentProgress.length < 5) {
          Thread.sleep(200)
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
        func: PythonMicroBatchStream => Unit): Unit = {
      val stream = new PythonMicroBatchStream(
        pythonDs,
        errorDataSourceName,
        inputSchema,
        CaseInsensitiveStringMap.empty()
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

    // User don't need to implement latestOffset for SimpleDataSourceStreamReader.
    // The latestOffset method of simple stream reader invokes initialOffset() and read()
    // So the not implemented method is initialOffset.
    testMicroBatchStreamError(
      "latestOffset",
      "[NOT_IMPLEMENTED] initialOffset is not implemented") {
      stream =>
        stream.latestOffset()
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
        func: PythonMicroBatchStream => Unit): Unit = {
      val stream = new PythonMicroBatchStream(
        pythonDs,
        errorDataSourceName,
        inputSchema,
        CaseInsensitiveStringMap.empty()
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
      stream.latestOffset()
    }
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
    val stream = new PythonMicroBatchStream(
      pythonDs,
      dataSourceName,
      inputSchema,
      CaseInsensitiveStringMap.empty()
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
        func: PythonMicroBatchStream => Unit): Unit = {
      val stream = new PythonMicroBatchStream(
        pythonDs,
        errorDataSourceName,
        inputSchema,
        CaseInsensitiveStringMap.empty()
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

    testMicroBatchStreamError("latestOffset", "[NOT_IMPLEMENTED] latestOffset is not implemented") {
      stream =>
        stream.latestOffset()
    }

    val offset = PythonStreamingSourceOffset("{\"offset\": \"2\"}")
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
      val stream = new PythonMicroBatchStream(
        pythonDs,
        errorDataSourceName,
        inputSchema,
        CaseInsensitiveStringMap.empty()
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
