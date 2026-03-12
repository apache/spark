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
import java.io.File
import java.nio.file.Files
import org.apache.spark.sql.pipelines.utils.ExecutionTest
import org.apache.spark.sql.test.SharedSparkSession
class DirectoryWriteSuite extends ExecutionTest with SharedSparkSession {
  private var tempDir: File = _
  override def beforeEach(): Unit = {
    super.beforeEach()
    tempDir = Files.createTempDirectory("directory-write-test").toFile
  }
  override def afterEach(): Unit = {
    if (tempDir != null && tempDir.exists()) {
      tempDir.listFiles().foreach(_.delete())
      tempDir.delete()
    }
    super.afterEach()
  }
  private def getTestOutputPath(name: String): String = {
    new File(tempDir, name).getAbsolutePath
  }
  test("basic INSERT OVERWRITE DIRECTORY with parquet") {
    val session = spark
    import session.implicits._
    val sourceTable = fullyQualifiedIdentifier("source")
    spark.sql(s"CREATE TABLE $sourceTable AS SELECT id, id * 2 as value FROM RANGE(10)")
    val outputPath = getTestOutputPath("parquet-output")
    val unresolvedDataflowGraph = unresolvedDataflowGraphFromSql(
      sqlText = s"""
        |INSERT OVERWRITE DIRECTORY '$outputPath'
        |USING parquet
        |SELECT * FROM $sourceTable;
      """.stripMargin
    )
    val updateContext = TestPipelineUpdateContext(spark, unresolvedDataflowGraph, storageRoot)
    updateContext.pipelineExecution.startPipeline()
    updateContext.pipelineExecution.awaitCompletion()
    // Verify files were written
    val outputDir = new File(outputPath)
    assert(outputDir.exists(), s"Output directory $outputPath should exist")
    assert(outputDir.listFiles().exists(_.getName.endsWith(".parquet")),
      "Should contain parquet files")
    assert(outputDir.listFiles().exists(_.getName == "_SUCCESS"),
      "Should contain _SUCCESS file")
    // Verify data can be read back
    val result = spark.read.parquet(outputPath)
    checkAnswer(result, Seq.range(0, 10).map(i => (i, i * 2)).toDF("id", "value"))
    spark.sql(s"DROP TABLE IF EXISTS $sourceTable")
  }
  test("INSERT OVERWRITE DIRECTORY with CSV format and options") {
    val session = spark
    val sourceTable = fullyQualifiedIdentifier("source")
    spark.sql(s"""
      CREATE TABLE $sourceTable (id INT, name STRING, value DOUBLE)
      USING parquet
    """)
    spark.sql(s"""
      INSERT INTO $sourceTable VALUES
        (1, 'Alice', 100.5),
        (2, 'Bob', 200.75),
        (3, 'Charlie', 300.25)
    """)
    val outputPath = getTestOutputPath("csv-output")
    val unresolvedDataflowGraph = unresolvedDataflowGraphFromSql(
      sqlText = s"""
        |INSERT OVERWRITE DIRECTORY '$outputPath'
        |USING csv
        |OPTIONS (
        |  'header' = 'true',
        |  'delimiter' = '|',
        |  'compression' = 'gzip'
        |)
        |SELECT * FROM $sourceTable;
      """.stripMargin
    )
    val updateContext = TestPipelineUpdateContext(spark, unresolvedDataflowGraph, storageRoot)
    updateContext.pipelineExecution.startPipeline()
    updateContext.pipelineExecution.awaitCompletion()
    // Verify files were written
    val outputDir = new File(outputPath)
    assert(outputDir.exists(), s"Output directory $outputPath should exist")
    assert(outputDir.listFiles().exists(_.getName.endsWith(".csv.gz")),
      "Should contain compressed CSV files")
    // Verify data can be read back with correct options
    val result = spark.read
      .option("header", "true")
      .option("delimiter", "|")
      .option("compression", "gzip")
      .csv(outputPath)
    assert(result.count() == 3, "Should have 3 rows")
    assert(result.columns.toSet == Set("id", "name", "value"),
      "Should have correct column names")
    spark.sql(s"DROP TABLE IF EXISTS $sourceTable")
  }
  test("INSERT OVERWRITE DIRECTORY with JSON format") {
    val session = spark
    val sourceTable = fullyQualifiedIdentifier("source")
    spark.sql(s"""
      CREATE TABLE $sourceTable (id INT, data STRING, timestamp TIMESTAMP)
      USING parquet
    """)
    spark.sql(s"""
      INSERT INTO $sourceTable VALUES
        (1, 'event1', TIMESTAMP '2024-03-12 10:00:00'),
        (2, 'event2', TIMESTAMP '2024-03-12 11:00:00')
    """)
    val outputPath = getTestOutputPath("json-output")
    val unresolvedDataflowGraph = unresolvedDataflowGraphFromSql(
      sqlText = s"""
        |INSERT OVERWRITE DIRECTORY '$outputPath'
        |USING json
        |SELECT * FROM $sourceTable;
      """.stripMargin
    )
    val updateContext = TestPipelineUpdateContext(spark, unresolvedDataflowGraph, storageRoot)
    updateContext.pipelineExecution.startPipeline()
    updateContext.pipelineExecution.awaitCompletion()
    // Verify files were written
    val outputDir = new File(outputPath)
    assert(outputDir.exists(), s"Output directory $outputPath should exist")
    assert(outputDir.listFiles().exists(_.getName.endsWith(".json")),
      "Should contain JSON files")
    // Verify data can be read back
    val result = spark.read.json(outputPath)
    assert(result.count() == 2, "Should have 2 rows")
    spark.sql(s"DROP TABLE IF EXISTS $sourceTable")
  }
  test("INSERT OVERWRITE DIRECTORY with partitioning") {
    val session = spark
    val sourceTable = fullyQualifiedIdentifier("source")
    spark.sql(s"""
      CREATE TABLE $sourceTable (id INT, category STRING, value DOUBLE)
      USING parquet
    """)
    spark.sql(s"""
      INSERT INTO $sourceTable VALUES
        (1, 'A', 100.0),
        (2, 'A', 200.0),
        (3, 'B', 300.0),
        (4, 'B', 400.0),
        (5, 'C', 500.0)
    """)
    val outputPath = getTestOutputPath("partitioned-output")
    val unresolvedDataflowGraph = unresolvedDataflowGraphFromSql(
      sqlText = s"""
        |INSERT OVERWRITE DIRECTORY '$outputPath'
        |USING parquet
        |OPTIONS ('partitionBy' = 'category')
        |SELECT * FROM $sourceTable;
      """.stripMargin
    )
    val updateContext = TestPipelineUpdateContext(spark, unresolvedDataflowGraph, storageRoot)
    updateContext.pipelineExecution.startPipeline()
    updateContext.pipelineExecution.awaitCompletion()
    // Verify partitioned directories were created
    val outputDir = new File(outputPath)
    assert(outputDir.exists(), s"Output directory $outputPath should exist")
    val partitionDirs = outputDir.listFiles().filter(_.isDirectory)
    val partitionNames = partitionDirs.map(_.getName).toSet
    assert(partitionNames.contains("category=A"), "Should have partition for category A")
    assert(partitionNames.contains("category=B"), "Should have partition for category B")
    assert(partitionNames.contains("category=C"), "Should have partition for category C")
    // Verify data can be read back
    val result = spark.read.parquet(outputPath)
    assert(result.count() == 5, "Should have 5 rows")
    assert(result.filter("category = 'A'").count() == 2, "Category A should have 2 rows")
    assert(result.filter("category = 'B'").count() == 2, "Category B should have 2 rows")
    assert(result.filter("category = 'C'").count() == 1, "Category C should have 1 row")
    spark.sql(s"DROP TABLE IF EXISTS $sourceTable")
  }
  test("INSERT OVERWRITE DIRECTORY overwrites existing data") {
    val session = spark
    val sourceTable = fullyQualifiedIdentifier("source")
    spark.sql(s"CREATE TABLE $sourceTable (id INT, value STRING) USING parquet")
    val outputPath = getTestOutputPath("overwrite-test")
    // First write
    spark.sql(s"INSERT INTO $sourceTable VALUES (1, 'first'), (2, 'first')")
    val firstGraph = unresolvedDataflowGraphFromSql(
      sqlText = s"""
        |INSERT OVERWRITE DIRECTORY '$outputPath'
        |USING parquet
        |SELECT * FROM $sourceTable;
      """.stripMargin
    )
    val firstContext = TestPipelineUpdateContext(spark, firstGraph, storageRoot)
    firstContext.pipelineExecution.startPipeline()
    firstContext.pipelineExecution.awaitCompletion()
    // Verify first write
    val firstResult = spark.read.parquet(outputPath)
    assert(firstResult.count() == 2, "First write should have 2 rows")
    assert(firstResult.filter("value = 'first'").count() == 2,
      "All rows should have value 'first'")
    // Second write with different data
    spark.sql(s"INSERT OVERWRITE TABLE $sourceTable VALUES (3, 'second'), (4, 'second'), (5, 'second')")
    val secondGraph = unresolvedDataflowGraphFromSql(
      sqlText = s"""
        |INSERT OVERWRITE DIRECTORY '$outputPath'
        |USING parquet
        |SELECT * FROM $sourceTable;
      """.stripMargin
    )
    val secondContext = TestPipelineUpdateContext(spark, secondGraph, storageRoot)
    secondContext.pipelineExecution.startPipeline()
    secondContext.pipelineExecution.awaitCompletion()
    // Verify second write overwrote first
    val secondResult = spark.read.parquet(outputPath)
    assert(secondResult.count() == 3, "Second write should have 3 rows")
    assert(secondResult.filter("value = 'second'").count() == 3,
      "All rows should have value 'second'")
    assert(secondResult.filter("value = 'first'").count() == 0,
      "First write should be completely overwritten")
    spark.sql(s"DROP TABLE IF EXISTS $sourceTable")
  }
  test("INSERT OVERWRITE DIRECTORY with complex query") {
    val session = spark
    val sourceTable = fullyQualifiedIdentifier("source")
    spark.sql(s"""
      CREATE TABLE $sourceTable (
        id INT,
        category STRING,
        amount DOUBLE,
        date DATE
      ) USING parquet
    """)
    spark.sql(s"""
      INSERT INTO $sourceTable VALUES
        (1, 'A', 100.0, DATE '2024-03-10'),
        (2, 'A', 200.0, DATE '2024-03-11'),
        (3, 'B', 150.0, DATE '2024-03-10'),
        (4, 'B', 250.0, DATE '2024-03-11'),
        (5, 'C', 300.0, DATE '2024-03-12')
    """)
    val outputPath = getTestOutputPath("complex-query-output")
    val unresolvedDataflowGraph = unresolvedDataflowGraphFromSql(
      sqlText = s"""
        |INSERT OVERWRITE DIRECTORY '$outputPath'
        |USING parquet
        |OPTIONS ('compression' = 'snappy')
        |SELECT
        |  category,
        |  COUNT(*) as count,
        |  SUM(amount) as total_amount,
        |  AVG(amount) as avg_amount,
        |  MIN(date) as min_date,
        |  MAX(date) as max_date
        |FROM $sourceTable
        |WHERE date >= DATE '2024-03-10'
        |GROUP BY category
        |HAVING SUM(amount) > 100;
      """.stripMargin
    )
    val updateContext = TestPipelineUpdateContext(spark, unresolvedDataflowGraph, storageRoot)
    updateContext.pipelineExecution.startPipeline()
    updateContext.pipelineExecution.awaitCompletion()
    // Verify files were written
    val outputDir = new File(outputPath)
    assert(outputDir.exists(), s"Output directory $outputPath should exist")
    // Verify aggregated data
    val result = spark.read.parquet(outputPath)
    assert(result.count() == 3, "Should have 3 categories")
    val categoryA = result.filter("category = 'A'").head()
    assert(categoryA.getLong(1) == 2, "Category A should have 2 rows")
    assert(categoryA.getDouble(2) == 300.0, "Category A total should be 300.0")
    spark.sql(s"DROP TABLE IF EXISTS $sourceTable")
  }
  test("INSERT OVERWRITE DIRECTORY from materialized view") {
    val session = spark
    val mvName = fullyQualifiedIdentifier("mymv")
    val outputPath = getTestOutputPath("mv-export")
    val unresolvedDataflowGraph = unresolvedDataflowGraphFromSql(
      sqlText = s"""
        |CREATE MATERIALIZED VIEW mymv AS
        |SELECT id, id * 10 as value FROM RANGE(5);
        |
        |INSERT OVERWRITE DIRECTORY '$outputPath'
        |USING parquet
        |SELECT * FROM $mvName;
      """.stripMargin
    )
    val updateContext = TestPipelineUpdateContext(spark, unresolvedDataflowGraph, storageRoot)
    updateContext.pipelineExecution.startPipeline()
    updateContext.pipelineExecution.awaitCompletion()
    // Verify export
    val result = spark.read.parquet(outputPath)
    assert(result.count() == 5, "Should have 5 rows")
    val values = result.orderBy("id").select("value").collect().map(_.getLong(0))
    assert(values.toSeq == Seq(0, 10, 20, 30, 40), "Values should be correct")
  }
  test("multiple INSERT OVERWRITE DIRECTORY statements") {
    val session = spark
    val sourceTable = fullyQualifiedIdentifier("source")
    spark.sql(s"CREATE TABLE $sourceTable AS SELECT id, id as value FROM RANGE(10)")
    val output1 = getTestOutputPath("output1")
    val output2 = getTestOutputPath("output2")
    val output3 = getTestOutputPath("output3")
    val unresolvedDataflowGraph = unresolvedDataflowGraphFromSql(
      sqlText = s"""
        |INSERT OVERWRITE DIRECTORY '$output1'
        |USING parquet
        |SELECT * FROM $sourceTable WHERE id < 5;
        |
        |INSERT OVERWRITE DIRECTORY '$output2'
        |USING json
        |SELECT * FROM $sourceTable WHERE id >= 5;
        |
        |INSERT OVERWRITE DIRECTORY '$output3'
        |USING csv
        |OPTIONS ('header' = 'true')
        |SELECT * FROM $sourceTable;
      """.stripMargin
    )
    val updateContext = TestPipelineUpdateContext(spark, unresolvedDataflowGraph, storageRoot)
    updateContext.pipelineExecution.startPipeline()
    updateContext.pipelineExecution.awaitCompletion()
    // Verify all three outputs
    assert(new File(output1).exists(), "Output1 should exist")
    assert(new File(output2).exists(), "Output2 should exist")
    assert(new File(output3).exists(), "Output3 should exist")
    val result1 = spark.read.parquet(output1)
    assert(result1.count() == 5, "Output1 should have 5 rows")
    val result2 = spark.read.json(output2)
    assert(result2.count() == 5, "Output2 should have 5 rows")
    val result3 = spark.read.option("header", "true").csv(output3)
    assert(result3.count() == 10, "Output3 should have 10 rows")
    spark.sql(s"DROP TABLE IF EXISTS $sourceTable")
  }
  test("INSERT OVERWRITE DIRECTORY with ORC format") {
    val session = spark
    val sourceTable = fullyQualifiedIdentifier("source")
    spark.sql(s"""
      CREATE TABLE $sourceTable (id INT, name STRING, value DOUBLE)
      USING parquet
    """)
    spark.sql(s"INSERT INTO $sourceTable VALUES (1, 'test1', 100.0), (2, 'test2', 200.0)")
    val outputPath = getTestOutputPath("orc-output")
    val unresolvedDataflowGraph = unresolvedDataflowGraphFromSql(
      sqlText = s"""
        |INSERT OVERWRITE DIRECTORY '$outputPath'
        |USING orc
        |OPTIONS ('compression' = 'snappy')
        |SELECT * FROM $sourceTable;
      """.stripMargin
    )
    val updateContext = TestPipelineUpdateContext(spark, unresolvedDataflowGraph, storageRoot)
    updateContext.pipelineExecution.startPipeline()
    updateContext.pipelineExecution.awaitCompletion()
    // Verify ORC files were written
    val outputDir = new File(outputPath)
    assert(outputDir.exists(), s"Output directory $outputPath should exist")
    assert(outputDir.listFiles().exists(_.getName.endsWith(".orc")),
      "Should contain ORC files")
    // Verify data can be read back
    val result = spark.read.orc(outputPath)
    assert(result.count() == 2, "Should have 2 rows")
    spark.sql(s"DROP TABLE IF EXISTS $sourceTable")
  }
  test("INSERT OVERWRITE DIRECTORY with empty result set") {
    val session = spark
    val sourceTable = fullyQualifiedIdentifier("source")
    spark.sql(s"CREATE TABLE $sourceTable AS SELECT id FROM RANGE(10)")
    val outputPath = getTestOutputPath("empty-output")
    val unresolvedDataflowGraph = unresolvedDataflowGraphFromSql(
      sqlText = s"""
        |INSERT OVERWRITE DIRECTORY '$outputPath'
        |USING parquet
        |SELECT * FROM $sourceTable WHERE id > 100;
      """.stripMargin
    )
    val updateContext = TestPipelineUpdateContext(spark, unresolvedDataflowGraph, storageRoot)
    updateContext.pipelineExecution.startPipeline()
    updateContext.pipelineExecution.awaitCompletion()
    // Verify directory exists but has no data files (only _SUCCESS)
    val outputDir = new File(outputPath)
    assert(outputDir.exists(), s"Output directory $outputPath should exist")
    assert(outputDir.listFiles().exists(_.getName == "_SUCCESS"),
      "Should contain _SUCCESS file")
    // Verify empty result
    val result = spark.read.parquet(outputPath)
    assert(result.count() == 0, "Should have 0 rows")
    spark.sql(s"DROP TABLE IF EXISTS $sourceTable")
  }
}