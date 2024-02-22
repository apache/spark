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

import java.io.File

import scala.concurrent.duration._

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.IntegratedUDFTestUtils.{createUserDefinedPythonDataSource, shouldTestPandasUDFs}
import org.apache.spark.sql.execution.streaming.MemoryStream

class PythonStreamingDataSourceSuite extends PythonDataSourceSuiteBase {

  import testImplicits._

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

  Seq("append", "complete").foreach { mode =>
    test(s"data source stream write - $mode mode") {
      assume(shouldTestPandasUDFs)
      val dataSourceScript =
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
      val dataSource = createUserDefinedPythonDataSource(dataSourceName, dataSourceScript)
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
        eventually(timeout(3.seconds)) {
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
        eventually(timeout(3.seconds)) {
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
      inputData.addData(1 to 30)

      def metadataDf: DataFrame = spark.read.format("json")
        .load(outputDir.getAbsolutePath)

      eventually(timeout(3.seconds)) {
        checkAnswer(metadataDf, Seq(Row(3, 30)))
      }

      inputData.addData(31 to 50)

      eventually(timeout(3.seconds)) {
        checkAnswer(metadataDf, Seq(Row(3, 30), Row(3, 20)))
      }

      // The sink throws exception when encountering value > 50.
      inputData.addData(51 to 100)

      eventually(timeout(3.seconds)) {
        checkAnswer(
          spark.read.text(outputDir.getAbsolutePath + "/2.txt"),
          Seq(Row("failed in batch 2")))
      }

      q.stop()
      assert(q.exception.get.message.contains("invalid value"))
    }
  }
}
