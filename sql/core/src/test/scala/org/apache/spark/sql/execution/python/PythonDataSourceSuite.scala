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

import org.apache.spark.sql.{AnalysisException, IntegratedUDFTestUtils, QueryTest, Row}
import org.apache.spark.sql.catalyst.plans.logical.{BatchEvalPythonUDTF, PythonDataSourcePartitions}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType

class PythonDataSourceSuite extends QueryTest with SharedSparkSession {
  import IntegratedUDFTestUtils._

  test("simple data source") {
    val dataSourceScript =
      """
        |from pyspark.sql.datasource import DataSource, DataSourceReader
        |class MyDataSourceReader(DataSourceReader):
        |    def partitions(self):
        |        return range(0, 2)
        |    def read(self, partition):
        |        yield (0, partition)
        |        yield (1, partition)
        |        yield (2, partition)
        |
        |class MyDataSource(DataSource):
        |    def reader(self, schema):
        |        return MyDataSourceReader()
        |""".stripMargin
    val schema = StructType.fromDDL("id INT, partition INT")
    val dataSource = createUserDefinedPythonDataSource(
      name = "MyDataSource", pythonScript = dataSourceScript, schema = schema)
    val df = dataSource(spark)
    assert(df.rdd.getNumPartitions == 2)
    val plan = df.queryExecution.optimizedPlan
    plan match {
      case BatchEvalPythonUDTF(pythonUDTF, _, _, _: PythonDataSourcePartitions)
        if pythonUDTF.name == "python_data_source_read" =>
      case _ => fail(s"Plan did not match the expected pattern. Actual plan:\n$plan")
    }
    checkAnswer(df, Seq(Row(0, 0), Row(0, 1), Row(1, 0), Row(1, 1), Row(2, 0), Row(2, 1)))
  }

  test("reader not implemented") {
    val dataSourceScript =
      """
        |from pyspark.sql.datasource import DataSource, DataSourceReader
        |class MyDataSource(DataSource):
        |    pass
        |""".stripMargin
    val schema = StructType.fromDDL("id INT, partition INT")
    val dataSource = createUserDefinedPythonDataSource(
      name = "MyDataSource", pythonScript = dataSourceScript, schema = schema)
    val err = intercept[AnalysisException] {
      dataSource(spark).collect()
    }
    assert(err.getErrorClass == "PYTHON_DATA_SOURCE_FAILED_TO_PLAN_IN_PYTHON")
    assert(err.getMessage.contains("PYTHON_DATA_SOURCE_METHOD_NOT_IMPLEMENTED"))
  }

  test("error creating reader") {
    val dataSourceScript =
      """
        |from pyspark.sql.datasource import DataSource
        |class MyDataSource(DataSource):
        |    def reader(self, schema):
        |        raise Exception("error creating reader")
        |""".stripMargin
    val schema = StructType.fromDDL("id INT, partition INT")
    val dataSource = createUserDefinedPythonDataSource(
      name = "MyDataSource", pythonScript = dataSourceScript, schema = schema)
    val err = intercept[AnalysisException] {
      dataSource(spark).collect()
    }
    assert(err.getErrorClass == "PYTHON_DATA_SOURCE_FAILED_TO_PLAN_IN_PYTHON")
    assert(err.getMessage.contains("PYTHON_DATA_SOURCE_CREATE_ERROR"))
    assert(err.getMessage.contains("error creating reader"))
  }

  test("data source assertion error") {
    val dataSourceScript =
      """
        |class MyDataSource:
        |   def __init__(self, options):
        |       ...
        |""".stripMargin
    val schema = StructType.fromDDL("id INT, partition INT")
    val dataSource = createUserDefinedPythonDataSource(
      name = "MyDataSource", pythonScript = dataSourceScript, schema = schema)
    val err = intercept[AnalysisException] {
      dataSource(spark).collect()
    }
    assert(err.getErrorClass == "PYTHON_DATA_SOURCE_FAILED_TO_PLAN_IN_PYTHON")
    assert(err.getMessage.contains("PYTHON_DATA_SOURCE_TYPE_MISMATCH"))
    assert(err.getMessage.contains("PySparkAssertionError"))
  }
}
