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
package org.apache.spark.sql.execution.python

import org.apache.spark.SparkException
import org.apache.spark.sql.{AnalysisException, DataFrame, Row}
import org.apache.spark.sql.IntegratedUDFTestUtils.{createUserDefinedPythonDataSource, shouldTestPandasUDFs}
import org.apache.spark.sql.execution.datasources.v2.python.{PythonDataSourceV2, PythonMicroBatchStream, PythonStreamingSourceOffset}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class PythonStreamingDataSourceSuite extends PythonDataSourceSuiteBase {

  protected def simpleDataStreamReaderScript: String =
    """
      |from pyspark.sql.datasource import DataSourceStreamReader, InputPartition
      |
      |class SimpleDataStreamReader(DataSourceStreamReader):
      |    current = 0
      |    def initialOffset(self):
      |        return {"offset": "0"}
      |    def latestOffset(self):
      |        self.current += 2
      |        return {"offset": str(self.current)}
      |    def partitions(self, start: dict, end: dict):
      |        return [InputPartition(i) for i in range(int(start["offset"]), int(end["offset"]))]
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

  test("simple data stream source") {
    assume(shouldTestPandasUDFs)
    val dataSourceScript =
      s"""
         |from pyspark.sql.datasource import DataSource
         |$simpleDataStreamReaderScript
         |
         |class $dataSourceName(DataSource):
         |    def schema(self) -> str:
         |        return "id INT"
         |    def streamReader(self, schema):
         |        return SimpleDataStreamReader()
         |""".stripMargin
    val inputSchema = StructType.fromDDL("input BINARY")

    val dataSource = createUserDefinedPythonDataSource(dataSourceName, dataSourceScript)
    spark.dataSource.registerPython(dataSourceName, dataSource)
    val pythonDs = new PythonDataSourceV2
    pythonDs.setShortName("SimpleDataSource")
    val stream = new PythonMicroBatchStream(
      pythonDs, dataSourceName, inputSchema, CaseInsensitiveStringMap.empty())

    val initialOffset = stream.initialOffset()
    assert(initialOffset.json == s"""{"offset": "0"}""")
    var prevOffset = initialOffset
    for (i <- 1 to 50) {
      val offset = stream.latestOffset()
      assert(offset.json == s"""{"offset": "${2*i}"}""")
      assert(stream.planInputPartitions(prevOffset, offset).size == 2)
      stream.commit(offset)
      prevOffset = offset
    }
    stream.stop()
  }

  test("Read from simple data stream source") {
    assume(shouldTestPandasUDFs)
    val dataSourceScript =
      s"""
         |from pyspark.sql.datasource import DataSource
         |$simpleDataStreamReaderScript
         |
         |class $dataSourceName(DataSource):
         |    def schema(self) -> str:
         |        return "id INT"
         |    def streamReader(self, schema):
         |        return SimpleDataStreamReader()
         |""".stripMargin

    val dataSource = createUserDefinedPythonDataSource(dataSourceName, dataSourceScript)
    spark.dataSource.registerPython(dataSourceName, dataSource)
    assert(spark.sessionState.dataSourceManager.dataSourceExists(dataSourceName))
    val df = spark.readStream.format(dataSourceName).load()

    val q = df.writeStream.foreachBatch((df: DataFrame, batchId: Long) => {
      checkAnswer(df, Seq(Row(batchId * 2), Row(batchId * 2 + 1)))
    }).start()
    Thread.sleep(3000)
    q.stop()
    q.awaitTermination()
  }

  test("Error creating stream reader") {
    assume(shouldTestPandasUDFs)
    val dataSourceScript =
      s"""
         |from pyspark.sql.datasource import DataSource
         |class $dataSourceName(DataSource):
         |    def streamReader(self, schema):
         |        raise Exception("error creating stream reader")
         |""".stripMargin
    val dataSource = createUserDefinedPythonDataSource(
      name = dataSourceName, pythonScript = dataSourceScript)
    spark.dataSource.registerPython(dataSourceName, dataSource)
    val pythonDs = new PythonDataSourceV2
    pythonDs.setShortName("SimpleDataSource")
    val inputSchema = StructType.fromDDL("input BINARY")
    val err = intercept[AnalysisException] {
      new PythonMicroBatchStream(
        pythonDs, dataSourceName, inputSchema, CaseInsensitiveStringMap.empty())
    }
    assert(err.getErrorClass == "PYTHON_DATA_SOURCE_ERROR")
    assert(err.getMessage.contains("error creating stream reader"))
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
    val offset = PythonStreamingSourceOffset("{\"0\": \"2\"}")

    def testMicroBatchStreamError(msg: String)
                                 (func: PythonMicroBatchStream => Unit): Unit = {
      val stream = new PythonMicroBatchStream(
        pythonDs, errorDataSourceName, inputSchema, CaseInsensitiveStringMap.empty())
      val err = intercept[SparkException] {
        func(stream)
      }
      assert(err.getMessage.contains(msg))
      stream.stop()
    }

    testMicroBatchStreamError("error reading initial offset") {
      stream => stream.initialOffset()
    }

    testMicroBatchStreamError("error reading latest offset") {
      stream => stream.latestOffset()
    }

    testMicroBatchStreamError("error planning partitions") {
      stream => stream.planInputPartitions(offset, offset)
    }

    testMicroBatchStreamError("error committing offset") {
      stream => stream.commit(offset)
    }
  }
}
