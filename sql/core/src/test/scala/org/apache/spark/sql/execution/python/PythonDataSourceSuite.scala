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

import java.io.{File, FileWriter}

import org.apache.spark.SparkException
import org.apache.spark.api.python.PythonUtils
import org.apache.spark.sql.{AnalysisException, IntegratedUDFTestUtils, QueryTest, Row}
import org.apache.spark.sql.execution.FilterExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.DataSourceManager
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, DataSourceV2ScanRelation}
import org.apache.spark.sql.execution.datasources.v2.python.PythonScan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils

abstract class PythonDataSourceSuiteBase
    extends QueryTest
    with SharedSparkSession
    with AdaptiveSparkPlanHelper {

  protected val simpleDataSourceReaderScript: String =
    """
      |from pyspark.sql.datasource import DataSourceReader, InputPartition
      |class SimpleDataSourceReader(DataSourceReader):
      |    def partitions(self):
      |        return [InputPartition(i) for i in range(2)]
      |    def read(self, partition):
      |        yield (0, partition.value)
      |        yield (1, partition.value)
      |        yield (2, partition.value)
      |""".stripMargin
  protected val staticSourceName = "custom_source"
  protected var tempDir: File = _

  override def beforeAll(): Unit = {
    // Create a Python Data Source package before starting up the Spark Session
    // that triggers automatic registration of the Python Data Source.
    val dataSourceScript =
    s"""
       |from pyspark.sql.datasource import DataSource, DataSourceReader
       |$simpleDataSourceReaderScript
       |
       |class DefaultSource(DataSource):
       |    def schema(self) -> str:
       |        return "id INT, partition INT"
       |
       |    def reader(self, schema):
       |        return SimpleDataSourceReader()
       |
       |    @classmethod
       |    def name(cls):
       |        return "$staticSourceName"
       |""".stripMargin
    tempDir = Utils.createTempDir()
    // Write a temporary package to test.
    // tmp/my_source
    // tmp/my_source/__init__.py
    val packageDir = new File(tempDir, "pyspark_mysource")
    assert(packageDir.mkdir())
    Utils.tryWithResource(
      new FileWriter(new File(packageDir, "__init__.py")))(_.write(dataSourceScript))
    // So Spark Session initialization can lookup this temporary directory.
    DataSourceManager.dataSourceBuilders = None
    PythonUtils.additionalTestingPath = Some(tempDir.toString)
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    try {
      Utils.deleteRecursively(tempDir)
      PythonUtils.additionalTestingPath = None
    } finally {
      super.afterAll()
    }
  }

  setupTestData()

  protected def dataSourceName = "SimpleDataSource"
}

class PythonDataSourceSuite extends PythonDataSourceSuiteBase {
  import IntegratedUDFTestUtils._

  test("SPARK-50426: should not trigger static Python data source lookup") {
    assume(shouldTestPandasUDFs)
    val testAppender = new LogAppender("Python data source lookup")
    // Using builtin and Java data sources should not trigger a static
    // Python data source lookup
    withLogAppender(testAppender) {
      spark.read.format("org.apache.spark.sql.test").load()
      spark.range(3).write.mode("overwrite").format("noop").save()
    }
    assert(!testAppender.loggingEvents
      .exists(msg => msg.getMessage.getFormattedMessage.contains(
        "Loading static Python Data Sources.")))
    // Now trigger a Python data source lookup
    withLogAppender(testAppender) {
      spark.read.format(staticSourceName).load()
    }
    assert(testAppender.loggingEvents
      .exists(msg => msg.getMessage.getFormattedMessage.contains(
        "Loading static Python Data Sources.")))
  }

  test("SPARK-45917: automatic registration of Python Data Source") {
    assume(shouldTestPandasUDFs)
    val df = spark.read.format(staticSourceName).load()
    checkAnswer(df, Seq(Row(0, 0), Row(0, 1), Row(1, 0), Row(1, 1), Row(2, 0), Row(2, 1)))
  }

  test("simple data source") {
    assume(shouldTestPandasUDFs)
    val dataSourceScript =
      s"""
        |from pyspark.sql.datasource import DataSource
        |$simpleDataSourceReaderScript
        |
        |class $dataSourceName(DataSource):
        |    def reader(self, schema):
        |        return SimpleDataSourceReader()
        |""".stripMargin
    val schema = StructType.fromDDL("id INT, partition INT")
    val dataSource = createUserDefinedPythonDataSource(
      name = dataSourceName, pythonScript = dataSourceScript)
    spark.dataSource.registerPython(dataSourceName, dataSource)
    val df = spark.read.format(dataSourceName).schema(schema).load()
    assert(df.rdd.getNumPartitions == 2)
    val plan = df.queryExecution.optimizedPlan
    plan match {
      case s: DataSourceV2ScanRelation
        if s.relation.table.getClass.toString.contains("PythonTable") =>
      case _ => fail(s"Plan did not match the expected pattern. Actual plan:\n$plan")
    }
    checkAnswer(df, Seq(Row(0, 0), Row(0, 1), Row(1, 0), Row(1, 1), Row(2, 0), Row(2, 1)))
  }

  test("simple data source with string schema") {
    assume(shouldTestPandasUDFs)
    val dataSourceScript =
      s"""
         |from pyspark.sql.datasource import DataSource, DataSourceReader
         |$simpleDataSourceReaderScript
         |
         |class $dataSourceName(DataSource):
         |    def schema(self) -> str:
         |        return "id INT, partition INT"
         |
         |    def reader(self, schema):
         |        return SimpleDataSourceReader()
         |""".stripMargin
    val dataSource = createUserDefinedPythonDataSource(dataSourceName, dataSourceScript)
    spark.dataSource.registerPython(dataSourceName, dataSource)
    val df = spark.read.format(dataSourceName).load()
    checkAnswer(df, Seq(Row(0, 0), Row(0, 1), Row(1, 0), Row(1, 1), Row(2, 0), Row(2, 1)))
  }

  test("simple data source with StructType schema") {
    assume(shouldTestPandasUDFs)
    val dataSourceScript =
      s"""
         |from pyspark.sql.datasource import DataSource, DataSourceReader
         |from pyspark.sql.types import IntegerType, StructType, StructField
         |$simpleDataSourceReaderScript
         |
         |class $dataSourceName(DataSource):
         |    def schema(self) -> str:
         |        return StructType([
         |            StructField("id", IntegerType()),
         |            StructField("partition", IntegerType())
         |        ])
         |
         |    def reader(self, schema):
         |        return SimpleDataSourceReader()
         |""".stripMargin
    val dataSource = createUserDefinedPythonDataSource(dataSourceName, dataSourceScript)
    spark.dataSource.registerPython(dataSourceName, dataSource)
    val df = spark.read.format(dataSourceName).load()
    checkAnswer(df, Seq(Row(0, 0), Row(0, 1), Row(1, 0), Row(1, 1), Row(2, 0), Row(2, 1)))
  }

  test("data source with invalid schema") {
    assume(shouldTestPandasUDFs)
    val dataSourceScript =
      s"""
         |from pyspark.sql.datasource import DataSource, DataSourceReader
         |$simpleDataSourceReaderScript
         |
         |class $dataSourceName(DataSource):
         |    def schema(self) -> str:
         |        return "INT"
         |
         |    def reader(self, schema):
         |        return SimpleDataSourceReader()
         |""".stripMargin
    val dataSource = createUserDefinedPythonDataSource(dataSourceName, dataSourceScript)
    spark.dataSource.registerPython(dataSourceName, dataSource)
    checkError(
      exception = intercept[AnalysisException](spark.read.format(dataSourceName).load()),
      condition = "INVALID_SCHEMA.NON_STRUCT_TYPE",
      parameters = Map("inputSchema" -> "INT", "dataType" -> "\"INT\""))
  }

  test("data source reader with filter pushdown") {
    assume(shouldTestPandasUDFs)
    val dataSourceScript =
      s"""
         |from pyspark.sql.datasource import (
         |    DataSource,
         |    DataSourceReader,
         |    EqualTo,
         |    InputPartition,
         |)
         |
         |class SimpleDataSourceReader(DataSourceReader):
         |    def partitions(self):
         |        return [InputPartition(i) for i in range(2)]
         |
         |    def pushFilters(self, filters):
         |        for filter in filters:
         |            if filter != EqualTo(("partition",), 0):
         |                yield filter
         |
         |    def read(self, partition):
         |        yield (0, partition.value)
         |        yield (1, partition.value)
         |        yield (2, partition.value)
         |
         |class SimpleDataSource(DataSource):
         |    def schema(self):
         |        return "id int, partition int"
         |
         |    def reader(self, schema):
         |        return SimpleDataSourceReader()
         |""".stripMargin
    val schema = StructType.fromDDL("id INT, partition INT")
    val dataSource =
      createUserDefinedPythonDataSource(name = dataSourceName, pythonScript = dataSourceScript)
    withSQLConf(SQLConf.PYTHON_FILTER_PUSHDOWN_ENABLED.key -> "true") {
      spark.dataSource.registerPython(dataSourceName, dataSource)
      val df =
        spark.read.format(dataSourceName).schema(schema).load().filter("id = 1 and partition = 0")
      val plan = df.queryExecution.executedPlan

      /**
       * == Physical Plan ==
       * *(1) Project [id#261, partition#262]
       * +- *(1) Filter ((isnotnull(id#261) AND isnotnull(partition#262)) AND (id#261 = 1))
       *    +- BatchScan SimpleDataSource[id#261, partition#262] (Python)
       *       PushedFilters: [EqualTo(partition,0)],
       *       ReadSchema: struct<id:int,partition:int> RuntimeFilters: []
       */
      val filter = collectFirst(df.queryExecution.executedPlan) {
        case s: FilterExec =>
          val condition = s.condition.toString
          assert(!condition.contains("= 0")) // pushed filter is not in FilterExec
          assert(condition.contains("= 1")) // unsupported filter is in FilterExec
          s
      }.getOrElse(
        fail(s"Filter not found in the plan. Actual plan:\n$plan")
      )

      collectFirst(filter) {
        case s: BatchScanExec if s.scan.isInstanceOf[PythonScan] =>
          val p = s.scan.asInstanceOf[PythonScan]
          assert(p.getMetaData().get("PushedFilters").contains("[EqualTo(partition,0)]"))
      }.getOrElse(
        fail(s"PythonScan not found in the plan. Actual plan:\n$plan")
      )

      checkAnswer(df, Seq(Row(1, 0), Row(1, 1)))
    }
  }

  test("register data source") {
    assume(shouldTestPandasUDFs)
    val dataSourceScript =
      s"""
         |from pyspark.sql.datasource import DataSource, DataSourceReader
         |$simpleDataSourceReaderScript
         |
         |class $dataSourceName(DataSource):
         |    def schema(self) -> str:
         |        return "id INT, partition INT"
         |
         |    def reader(self, schema):
         |        return SimpleDataSourceReader()
         |""".stripMargin

    val dataSource = createUserDefinedPythonDataSource(dataSourceName, dataSourceScript)
    spark.dataSource.registerPython(dataSourceName, dataSource)
    assert(spark.sessionState.dataSourceManager.dataSourceExists(dataSourceName))
    checkAnswer(
      spark.read.format(dataSourceName).load(),
      Seq(Row(0, 0), Row(0, 1), Row(1, 0), Row(1, 1), Row(2, 0), Row(2, 1)))

    // Should be able to override an already registered data source.
    val newScript =
      s"""
         |from pyspark.sql.datasource import DataSource, DataSourceReader
         |class SimpleDataSourceReader(DataSourceReader):
         |    def read(self, partition):
         |        yield (0, )
         |
         |class $dataSourceName(DataSource):
         |    def schema(self) -> str:
         |        return "id INT"
         |
         |    def reader(self, schema):
         |        return SimpleDataSourceReader()
         |""".stripMargin
    val newDataSource = createUserDefinedPythonDataSource(dataSourceName, newScript)
    spark.dataSource.registerPython(dataSourceName, newDataSource)
    assert(spark.sessionState.dataSourceManager.dataSourceExists(dataSourceName))
    checkAnswer(
      spark.read.format(dataSourceName).load(),
      Seq(Row(0)))
  }

  test("load data source") {
    assume(shouldTestPandasUDFs)
    val dataSourceScript =
      s"""
         |from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
         |import json
         |
         |class SimpleDataSourceReader(DataSourceReader):
         |    def __init__(self, options):
         |        self.options = options
         |
         |    def partitions(self):
         |        if "paths" in self.options:
         |            paths = json.loads(self.options["paths"])
         |        elif "path" in self.options:
         |            paths = [self.options["path"]]
         |        else:
         |            paths = []
         |        return [InputPartition(p) for p in paths]
         |
         |    def read(self, part):
         |        if part is not None:
         |            assert isinstance(part, InputPartition)
         |            yield (part.value, 1)
         |        else:
         |            yield (part, 1)
         |
         |class $dataSourceName(DataSource):
         |    @classmethod
         |    def name(cls) -> str:
         |        return "test"
         |
         |    def schema(self) -> str:
         |        return "id STRING, value INT"
         |
         |    def reader(self, schema):
         |        return SimpleDataSourceReader(self.options)
         |""".stripMargin
    val dataSource = createUserDefinedPythonDataSource(dataSourceName, dataSourceScript)
    spark.dataSource.registerPython("test", dataSource)
    checkAnswer(spark.read.format("test").load(), Seq(Row(null, 1)))
    checkAnswer(spark.read.format("test").load("1"), Seq(Row("1", 1)))
    checkAnswer(spark.read.format("test").load("1", "2"), Seq(Row("1", 1), Row("2", 1)))

    withTable("tblA") {
      sql("CREATE TABLE tblA USING test")
      // The path will be the actual temp path.
      checkAnswer(spark.table("tblA").selectExpr("value"), Seq(Row(1)))
    }
  }

  test("SPARK-46522: data source name conflicts") {
    assume(shouldTestPandasUDFs)
    val dataSourceScript =
      s"""
         |from pyspark.sql.datasource import DataSource
         |
         |class $dataSourceName(DataSource):
         |    ...
         |""".stripMargin
    val dataSource = createUserDefinedPythonDataSource(dataSourceName, dataSourceScript)
    Seq(
      "text", "json", "csv", "avro", "orc", "parquet", "jdbc",
      "binaryFile", "xml", "kafka", "noop",
      "org.apache.spark.sql.test",
      "org.apache.spark.sql.hive.orc"
    ).foreach { provider =>
      withClue(s"Data source: $provider") {
        checkError(
          exception = intercept[AnalysisException] {
            spark.dataSource.registerPython(provider, dataSource)
          },
          condition = "DATA_SOURCE_ALREADY_EXISTS",
          parameters = Map("provider" -> provider))
      }
    }
  }

  test("reader not implemented") {
    assume(shouldTestPandasUDFs)
    val dataSourceScript =
       s"""
        |from pyspark.sql.datasource import DataSource, DataSourceReader
        |class $dataSourceName(DataSource):
        |    pass
        |""".stripMargin
    val schema = StructType.fromDDL("id INT, partition INT")
    val dataSource = createUserDefinedPythonDataSource(
      name = dataSourceName, pythonScript = dataSourceScript)
    spark.dataSource.registerPython(dataSourceName, dataSource)
    val err = intercept[AnalysisException] {
      spark.read.format(dataSourceName).schema(schema).load().collect()
    }
    assert(err.getCondition == "PYTHON_DATA_SOURCE_ERROR")
    assert(err.getMessage.contains("PySparkNotImplementedError"))
  }

  test("error creating reader") {
    assume(shouldTestPandasUDFs)
    val dataSourceScript =
      s"""
        |from pyspark.sql.datasource import DataSource
        |class $dataSourceName(DataSource):
        |    def reader(self, schema):
        |        raise Exception("error creating reader")
        |""".stripMargin
    val schema = StructType.fromDDL("id INT, partition INT")
    val dataSource = createUserDefinedPythonDataSource(
      name = dataSourceName, pythonScript = dataSourceScript)
    spark.dataSource.registerPython(dataSourceName, dataSource)
    val err = intercept[AnalysisException] {
      spark.read.format(dataSourceName).schema(schema).load().collect()
    }
    assert(err.getCondition == "PYTHON_DATA_SOURCE_ERROR")
    assert(err.getMessage.contains("error creating reader"))
  }

  test("data source assertion error") {
    assume(shouldTestPandasUDFs)
    val dataSourceScript =
      s"""
        |class $dataSourceName:
        |   def __init__(self, options):
        |       ...
        |""".stripMargin
    val schema = StructType.fromDDL("id INT, partition INT")
    val dataSource = createUserDefinedPythonDataSource(
      name = dataSourceName, pythonScript = dataSourceScript)
    spark.dataSource.registerPython(dataSourceName, dataSource)
    val err = intercept[AnalysisException] {
      spark.read.format(dataSourceName).schema(schema).load().collect()
    }
    assert(err.getCondition == "PYTHON_DATA_SOURCE_ERROR")
    assert(err.getMessage.contains("DATA_SOURCE_TYPE_MISMATCH"))
    assert(err.getMessage.contains("PySparkAssertionError"))
  }

  test("data source read with custom partitions") {
    assume(shouldTestPandasUDFs)
    val dataSourceScript =
      s"""
         |from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
         |class RangePartition(InputPartition):
         |    def __init__(self, start, end):
         |        self.start = start
         |        self.end = end
         |
         |class SimpleDataSourceReader(DataSourceReader):
         |    def partitions(self):
         |        return [RangePartition(1, 2), RangePartition(3, 4)]
         |
         |    def read(self, partition: RangePartition):
         |        start, end = partition.start, partition.end
         |        for i in range(start, end):
         |            yield (i, )
         |
         |class $dataSourceName(DataSource):
         |    def schema(self) -> str:
         |        return "id INT"
         |
         |    def reader(self, schema):
         |        return SimpleDataSourceReader()
         |""".stripMargin
    val dataSource = createUserDefinedPythonDataSource(dataSourceName, dataSourceScript)
    spark.dataSource.registerPython(dataSourceName, dataSource)
    val df = spark.read.format(dataSourceName).load()
    checkAnswer(df, Seq(Row(1), Row(3)))
  }

  test("data source read with empty partitions") {
    assume(shouldTestPandasUDFs)
    val dataSourceScript =
      s"""
         |from pyspark.sql.datasource import DataSource, DataSourceReader
         |class SimpleDataSourceReader(DataSourceReader):
         |    def partitions(self):
         |        return []
         |
         |    def read(self, partition):
         |        if partition is None:
         |            yield ("success", )
         |        else:
         |            yield ("failed", )
         |
         |class $dataSourceName(DataSource):
         |    def schema(self) -> str:
         |        return "status STRING"
         |
         |    def reader(self, schema):
         |        return SimpleDataSourceReader()
         |""".stripMargin
    val dataSource = createUserDefinedPythonDataSource(dataSourceName, dataSourceScript)
    spark.dataSource.registerPython(dataSourceName, dataSource)
    val df = spark.read.format(dataSourceName).load()
    checkAnswer(df, Row("success"))
  }

  test("data source read with invalid partitions") {
    assume(shouldTestPandasUDFs)
    val reader1 =
      s"""
         |class SimpleDataSourceReader(DataSourceReader):
         |    def partitions(self):
         |        return 1
         |    def read(self, partition):
         |        ...
         |""".stripMargin

    val reader2 =
      s"""
         |class SimpleDataSourceReader(DataSourceReader):
         |    def partitions(self):
         |        return [1, 2]
         |    def read(self, partition):
         |        ...
         |""".stripMargin

    val reader3 =
      s"""
         |class SimpleDataSourceReader(DataSourceReader):
         |    def partitions(self):
         |        raise Exception("error")
         |    def read(self, partition):
         |        ...
         |""".stripMargin

    Seq(reader1, reader2, reader3).foreach { readerScript =>
      val dataSourceScript =
        s"""
           |from pyspark.sql.datasource import DataSource, DataSourceReader
           |$readerScript
           |
           |class $dataSourceName(DataSource):
           |    def schema(self) -> str:
           |        return "id INT"
           |
           |    def reader(self, schema):
           |        return SimpleDataSourceReader()
           |""".stripMargin
      val dataSource = createUserDefinedPythonDataSource(dataSourceName, dataSourceScript)
      spark.dataSource.registerPython(dataSourceName, dataSource)
      val err = intercept[AnalysisException](
        spark.read.format(dataSourceName).load().collect())
      assert(err.getCondition == "PYTHON_DATA_SOURCE_ERROR")
      assert(err.getMessage.contains("partitions"))
    }
  }

  test("SPARK-46540: data source read output named rows") {
    assume(shouldTestPandasUDFs)
    val dataSourceScript =
      s"""
         |from pyspark.sql.datasource import DataSource, DataSourceReader
         |class SimpleDataSourceReader(DataSourceReader):
         |    def read(self, partition):
         |        from pyspark.sql import Row
         |        yield Row(x = 0, y = 1)
         |        yield Row(y = 2, x = 1)
         |        yield Row(2, 3)
         |        yield (3, 4)
         |
         |class $dataSourceName(DataSource):
         |    def schema(self) -> str:
         |        return "x int, y int"
         |
         |    def reader(self, schema):
         |        return SimpleDataSourceReader()
         |""".stripMargin
    val dataSource = createUserDefinedPythonDataSource(dataSourceName, dataSourceScript)
    spark.dataSource.registerPython(dataSourceName, dataSource)
    val df = spark.read.format(dataSourceName).load()
    checkAnswer(df, Seq(Row(0, 1), Row(1, 2), Row(2, 3), Row(3, 4)))
  }

  test("SPARK-46424: Support Python metrics") {
    assume(shouldTestPandasUDFs)
    val dataSourceScript =
      s"""
         |from pyspark.sql.datasource import DataSource, DataSourceReader
         |class SimpleDataSourceReader(DataSourceReader):
         |    def partitions(self):
         |        return []
         |
         |    def read(self, partition):
         |        if partition is None:
         |            yield ("success", )
         |        else:
         |            yield ("failed", )
         |
         |class $dataSourceName(DataSource):
         |    def schema(self) -> str:
         |        return "status STRING"
         |
         |    def reader(self, schema):
         |        return SimpleDataSourceReader()
         |""".stripMargin
    val dataSource = createUserDefinedPythonDataSource(dataSourceName, dataSourceScript)
    spark.dataSource.registerPython(dataSourceName, dataSource)
    val df = spark.read.format(dataSourceName).load()

    val statusStore = spark.sharedState.statusStore
    val oldCount = statusStore.executionsList().size

    df.collect()

    // Wait until the new execution is started and being tracked.
    while (statusStore.executionsCount() < oldCount) {
      Thread.sleep(100)
    }

    // Wait for listener to finish computing the metrics for the execution.
    while (statusStore.executionsList().isEmpty ||
      statusStore.executionsList().last.metricValues == null) {
      Thread.sleep(100)
    }

    val executedPlan = df.queryExecution.executedPlan.collectFirst {
      case p: BatchScanExec => p
    }
    assert(executedPlan.isDefined)

    val execId = statusStore.executionsList().last.executionId
    val metrics = statusStore.executionMetrics(execId)
    val pythonDataSent = executedPlan.get.metrics("pythonDataSent")
    val pythonDataReceived = executedPlan.get.metrics("pythonDataReceived")
    assert(metrics.contains(pythonDataSent.id))
    assert(metrics(pythonDataSent.id).asInstanceOf[String].endsWith("B"))
    assert(metrics.contains(pythonDataReceived.id))
    assert(metrics(pythonDataReceived.id).asInstanceOf[String].endsWith("B"))
  }

  test("simple data source write") {
    assume(shouldTestPandasUDFs)
    val dataSourceScript =
      s"""
         |import json
         |import os
         |from pyspark import TaskContext
         |from pyspark.sql.datasource import DataSource, DataSourceWriter, WriterCommitMessage
         |
         |class SimpleDataSourceWriter(DataSourceWriter):
         |    def __init__(self, options):
         |        self.options = options
         |
         |    def write(self, iterator):
         |        context = TaskContext.get()
         |        partition_id = context.partitionId()
         |        path = self.options.get("path")
         |        assert path is not None
         |        output_path = os.path.join(path, f"{partition_id}.json")
         |        cnt = 0
         |        with open(output_path, "w") as file:
         |            for row in iterator:
         |                file.write(json.dumps(row.asDict()) + "\\n")
         |                cnt += 1
         |        return WriterCommitMessage()
         |
         |class SimpleDataSource(DataSource):
         |    def writer(self, schema, overwrite):
         |        return SimpleDataSourceWriter(self.options)
         |""".stripMargin
    val dataSource = createUserDefinedPythonDataSource(dataSourceName, dataSourceScript)
    spark.dataSource.registerPython(dataSourceName, dataSource)
    Seq(
      "SELECT * FROM range(0, 5, 1, 3)",
      "SELECT * FROM testData LIMIT 5",
      "SELECT * FROM testData3",
      "SELECT * FROM arrayData"
    ).foreach { query =>
      withTempDir { dir =>
        val df = sql(query)
        val path = dir.getAbsolutePath
        df.write.format(dataSourceName).mode("append").save(path)
        val df2 = spark.read.json(path)
        checkAnswer(df, df2)
      }
    }
  }

  test("data source write - error cases") {
    assume(shouldTestPandasUDFs)
    val dataSourceScript =
      s"""
         |from pyspark.sql.datasource import DataSource, DataSourceWriter
         |
         |class SimpleDataSourceWriter(DataSourceWriter):
         |    def write(self, iterator):
         |        num_rows = 0
         |        for row in iterator:
         |            num_rows += 1
         |            if num_rows > 2:
         |                raise Exception("something is wrong")
         |
         |class SimpleDataSource(DataSource):
         |    def writer(self, schema, saveMode):
         |        return SimpleDataSourceWriter()
         |""".stripMargin
    spark.dataSource.registerPython(dataSourceName,
      createUserDefinedPythonDataSource(dataSourceName, dataSourceScript))

    withClue("user error") {
      val error = intercept[SparkException] {
        spark.range(10).write.format(dataSourceName).mode("append").save()
      }
      assert(error.getMessage.contains("something is wrong"))
    }

    withClue("no commit message") {
      val error = intercept[SparkException] {
        spark.range(1).write.format(dataSourceName).mode("append").save()
      }
      assert(error.getMessage.contains("DATA_SOURCE_TYPE_MISMATCH"))
      assert(error.getMessage.contains("WriterCommitMessage"))
    }

    withClue("without mode") {
      checkError(
        exception = intercept[AnalysisException] {
          spark.range(1).write.format(dataSourceName).save()
        },
        condition = "UNSUPPORTED_DATA_SOURCE_SAVE_MODE",
        parameters = Map("source" -> "SimpleDataSource", "createMode" -> "\"ErrorIfExists\""))
    }

    withClue("with unsupported mode") {
      checkError(
        exception = intercept[AnalysisException] {
          spark.range(1).write.format(dataSourceName).mode("ignore").save()
        },
        condition = "UNSUPPORTED_DATA_SOURCE_SAVE_MODE",
        parameters = Map("source" -> "SimpleDataSource", "createMode" -> "\"Ignore\""))
    }

    withClue("invalid mode") {
      checkError(
        exception = intercept[AnalysisException] {
          spark.range(1).write.format(dataSourceName).mode("foo").save()
        },
        condition = "INVALID_SAVE_MODE",
        parameters = Map("mode" -> "\"foo\""))
    }
  }

  test("data source write - overwrite mode") {
    assume(shouldTestPandasUDFs)
    val dataSourceScript =
      s"""
         |import json
         |import os
         |from pyspark import TaskContext
         |from pyspark.sql.datasource import DataSource, DataSourceWriter, WriterCommitMessage
         |
         |class SimpleDataSourceWriter(DataSourceWriter):
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
         |                cnt += 1
         |        return WriterCommitMessage()
         |
         |class SimpleDataSource(DataSource):
         |    def writer(self, schema, overwrite):
         |        return SimpleDataSourceWriter(self.options, overwrite)
         |""".stripMargin
    val dataSource = createUserDefinedPythonDataSource(dataSourceName, dataSourceScript)
    spark.dataSource.registerPython(dataSourceName, dataSource)
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      spark.range(1).write.format(dataSourceName).mode("append").save(path)
      checkAnswer(
        spark.read.json(path),
        Seq(Row(0)))
      spark.range(1).write.format(dataSourceName).mode("append").save(path)
      checkAnswer(
        spark.read.json(path),
        Seq(Row(0), Row(0)))
      spark.range(2, 3).write.format(dataSourceName).mode("overwrite").save(path)
      checkAnswer(
        spark.read.json(path),
        Seq(Row(2)))
    }
  }

  test("data source write commit and abort") {
    assume(shouldTestPandasUDFs)
    val dataSourceScript =
      s"""
         |import json
         |import os
         |from dataclasses import dataclass
         |from pyspark import TaskContext
         |from pyspark.sql.datasource import DataSource, DataSourceWriter, WriterCommitMessage
         |
         |@dataclass
         |class SimpleCommitMessage(WriterCommitMessage):
         |    partition_id: int
         |    count: int
         |
         |class SimpleDataSourceWriter(DataSourceWriter):
         |    def __init__(self, options):
         |        self.options = options
         |        self.path = self.options.get("path")
         |        assert self.path is not None
         |
         |    def write(self, iterator):
         |        context = TaskContext.get()
         |        partition_id = context.partitionId()
         |        output_path = os.path.join(self.path, f"{partition_id}.json")
         |        cnt = 0
         |        with open(output_path, "w") as file:
         |            for row in iterator:
         |                if row.id >= 10:
         |                    raise Exception("invalid value")
         |                file.write(json.dumps(row.asDict()) + "\\n")
         |                cnt += 1
         |        return SimpleCommitMessage(partition_id=partition_id, count=cnt)
         |
         |    def commit(self, messages) -> None:
         |        status = dict(num_files=len(messages), count=sum(m.count for m in messages))
         |
         |        with open(os.path.join(self.path, "success.json"), "a") as file:
         |            file.write(json.dumps(status) + "\\n")
         |
         |    def abort(self, messages) -> None:
         |        with open(os.path.join(self.path, "failed.txt"), "a") as file:
         |            file.write("failed")
         |
         |class SimpleDataSource(DataSource):
         |    def writer(self, schema, saveMode):
         |        return SimpleDataSourceWriter(self.options)
         |""".stripMargin
    val dataSource = createUserDefinedPythonDataSource(dataSourceName, dataSourceScript)
    spark.dataSource.registerPython(dataSourceName, dataSource)
    withTempDir { dir =>
      val path = dir.getAbsolutePath

      withClue("commit") {
        sql("SELECT * FROM range(0, 5, 1, 3)")
          .write.format(dataSourceName)
          .mode("append")
          .save(path)
        checkAnswer(
          spark.read.format("json")
            .schema("num_files bigint, count bigint")
            .load(path + "/success.json"),
          Seq(Row(3, 5)))
      }

      withClue("commit again") {
        sql("SELECT * FROM range(5, 7, 1, 1)")
          .write.format(dataSourceName)
          .mode("append")
          .save(path)
        checkAnswer(
          spark.read.format("json")
            .schema("num_files bigint, count bigint")
            .load(path + "/success.json"),
          Seq(Row(3, 5), Row(1, 2)))
      }

      withClue("abort") {
        intercept[SparkException] {
          sql("SELECT * FROM range(8, 12, 1, 4)")
            .write.format(dataSourceName)
            .mode("append")
            .save(path)
        }
        checkAnswer(
          spark.read.text(path + "/failed.txt"),
          Seq(Row("failed")))
      }
    }
  }

  test("SPARK-46568: case insensitive options") {
    assume(shouldTestPandasUDFs)
    val dataSourceScript =
      s"""
         |from pyspark.sql.datasource import (
         |    DataSource, DataSourceReader, DataSourceWriter, WriterCommitMessage)
         |class SimpleDataSourceReader(DataSourceReader):
         |    def __init__(self, options):
         |        self.options = options
         |
         |    def read(self, partition):
         |        foo = self.options.get("Foo")
         |        bar = self.options.get("BAR")
         |        baz = "BaZ" in self.options
         |        yield (foo, bar, baz)
         |
         |class SimpleDataSourceWriter(DataSourceWriter):
         |    def __init__(self, options):
         |        self.options = options
         |
         |    def write(self, row):
         |        if "FOO" not in self.options or "BAR" not in self.options:
         |            raise Exception("FOO or BAR not found")
         |        return WriterCommitMessage()
         |
         |class $dataSourceName(DataSource):
         |    def schema(self) -> str:
         |        return "a string, b string, c string"
         |
         |    def reader(self, schema):
         |        return SimpleDataSourceReader(self.options)
         |
         |    def writer(self, schema, overwrite):
         |        return SimpleDataSourceWriter(self.options)
         |""".stripMargin
    val dataSource = createUserDefinedPythonDataSource(dataSourceName, dataSourceScript)
    spark.dataSource.registerPython(dataSourceName, dataSource)
    val df = spark.read.option("foo", 1).option("bar", 2).option("BAZ", 3)
      .format(dataSourceName).load()
    checkAnswer(df, Row("1", "2", "true"))
    df.write.option("foo", 1).option("bar", 2).format(dataSourceName).mode("append").save()
  }
}
