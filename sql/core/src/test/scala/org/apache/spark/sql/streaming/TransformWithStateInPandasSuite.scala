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

package org.apache.spark.sql.streaming

import org.apache.spark.sql.streaming.StreamTest
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.IntegratedUDFTestUtils.TestGroupedMapPandasUDFWithState
import org.apache.spark.sql.catalyst.expressions.PythonUDF
import org.apache.spark.sql.catalyst.streaming.InternalOutputModes.Update
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.tags.SlowSQLTest

@SlowSQLTest
class TransformWithStateInPandasSuite extends StreamTest {

  test("transformWithStateInPandas - streaming") {

    val pythonScript =
      """
        |import pandas as pd
        |from pyspark.sql.streaming import StatefulProcessor, StatefulProcessorHandle
        |from pyspark.sql.types import StructType, StructField, LongType, StringType
        |from typing import Iterator
        |
        |state_schema = StructType([
        |    StructField("value", StringType(), True)
        |])
        |
        |class SimpleStatefulProcessor(StatefulProcessor):
        |  def init(self, handle: StatefulProcessorHandle) -> None:
        |    self.value_state = handle.getValueState("testValueState", state_schema)
        |  def handleInputRows(self, key, rows) -> Iterator[pd.DataFrame]:
        |    self.value_state.update("test_value")
        |    exists = self.value_state.exists()
        |    value = self.value_state.get()
        |    self.value_state.clear()
        |    return rows
        |  def close(self) -> None:
        |    pass
        |
      |""".stripMargin

    val pythonFunc = TestGroupedMapPandasUDFWithState(
      name = "pandas_grouped_map_with_state", pythonScript = pythonScript)

    val inputData = MemoryStream[String]
    val outputStructType = StructType(
      Seq(
        StructField("key", StringType),
        StructField("countAsString", StringType)))
    val stateStructType = StructType(Seq(StructField("count", LongType)))
    val inputDataDS = inputData.toDS()
    val result =
      inputDataDS
        .groupBy("value")
        .applyInPandasWithState(
          pythonFunc(inputDataDS("value")).expr.asInstanceOf[PythonUDF],
          outputStructType,
          stateStructType,
          "Update",
          "NoTimeout")
  }
}
