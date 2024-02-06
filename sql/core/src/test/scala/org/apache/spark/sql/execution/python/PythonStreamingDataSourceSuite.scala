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

import org.apache.spark.sql.AnalysisException
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
      |    def latestOffset(self):
      |        return {"0": "2"}
      |    def partitions(self, start: dict, end: dict):
      |        return [InputPartition(i) for i in range(int(start["0"]))]
      |    def read(self, partition):
      |        yield (0, partition.value)
      |        yield (1, partition.value)
      |        yield (2, partition.value)
      |""".stripMargin

  protected def errorDataStreamReaderScript: String =
    """
      |from pyspark.sql.datasource import DataSourceStreamReader, InputPartition
      |
      |class ErrorDataStreamReader(DataSourceStreamReader):
      |    def latestOffset(self):
      |        raise Exception("error reading latest offset")
      |    def partitions(self, start: dict, end: dict):
      |        raise Exception("error planning partitions")
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

    for (_ <- 1 to 50) {
      val offset = stream.latestOffset()
      assert(offset.json == "{\"0\": \"2\"}")
      assert(stream.planInputPartitions(offset, offset).size == 2)
    }
    stream.stop()
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
    val stream1 = new PythonMicroBatchStream(
      pythonDs, errorDataSourceName, inputSchema, CaseInsensitiveStringMap.empty())
    val latestOffsetErr = intercept[AnalysisException] {
      stream1.latestOffset()
    }
    assert(latestOffsetErr.getErrorClass == "PYTHON_DATA_SOURCE_ERROR")
    assert(latestOffsetErr.getMessage.contains("error reading latest offset"))
    stream1.stop()

    val stream2 = new PythonMicroBatchStream(
      pythonDs, errorDataSourceName, inputSchema, CaseInsensitiveStringMap.empty())
    val offset = PythonStreamingSourceOffset("{\"0\": \"2\"}")
    val planInputPartitionErr = intercept[AnalysisException] {
      stream2.planInputPartitions(offset, offset)
    }
    assert(planInputPartitionErr.getErrorClass == "PYTHON_DATA_SOURCE_ERROR")
    assert(planInputPartitionErr.getMessage.contains("error planning partitions"))
    stream2.stop()
  }
}
