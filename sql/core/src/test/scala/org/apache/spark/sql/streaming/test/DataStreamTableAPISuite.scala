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

package org.apache.spark.sql.streaming.test

import java.io.File
import java.util

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.{AnalysisException, Row, SaveMode}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2
import org.apache.spark.sql.connector.{FakeV2Provider, FakeV2ProviderWithCustomSchema, InMemoryTableSessionCatalog}
import org.apache.spark.sql.connector.catalog.{Column, Identifier, InMemoryTableCatalog, MetadataColumn, SupportsMetadataColumns, SupportsRead, Table, TableCapability, V2TableWithV1Fallback}
import org.apache.spark.sql.connector.expressions.{ClusterByTransform, FieldReference, Transform}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.execution.streaming.{MemoryStream, MemoryStreamScanBuilder, StreamingQueryWrapper}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.StreamTest
import org.apache.spark.sql.streaming.sources.FakeScanBuilder
import org.apache.spark.sql.types.{DataType, IntegerType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.tags.SlowSQLTest
import org.apache.spark.util.Utils

@SlowSQLTest
class DataStreamTableAPISuite extends StreamTest with BeforeAndAfter {
  import testImplicits._
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  before {
    spark.conf.set("spark.sql.catalog.testcat", classOf[InMemoryTableCatalog].getName)
    spark.conf.set("spark.sql.catalog.teststream", classOf[InMemoryStreamTableCatalog].getName)
  }

  after {
    spark.sessionState.catalogManager.reset()
    spark.sessionState.conf.clear()
    sqlContext.streams.active.foreach(_.stop())
  }

  test("read: table API with file source") {
    Seq("parquet", "").foreach { source =>
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> source) {
        withTempDir { tempDir =>
          val tblName = "my_table"
          val dir = tempDir.getAbsolutePath
          withTable(tblName) {
            spark.range(3).write.format("parquet").option("path", dir).saveAsTable(tblName)

            testStream(spark.readStream.table(tblName))(
              ProcessAllAvailable(),
              CheckAnswer(Row(0), Row(1), Row(2))
            )
          }
        }
      }
    }
  }

  test("read: read non-exist table") {
    val e = intercept[AnalysisException] {
      spark.readStream.table("non_exist_table")
    }
    checkErrorTableNotFound(e, "`non_exist_table`")
  }

  test("read: stream table API with temp view") {
    val tblName = "my_table"
    val stream = MemoryStream[Int]
    withTable(tblName) {
      stream.toDF().createOrReplaceTempView(tblName)

      testStream(spark.readStream.table(tblName)) (
        AddData(stream, 1, 2, 3),
        CheckLastBatch(1, 2, 3),
        AddData(stream, 4, 5),
        CheckLastBatch(4, 5)
      )
    }
  }

  test("read: stream table API with non-streaming temp view") {
    val tblName = "my_table"
    withTable(tblName) {
      spark.range(3).createOrReplaceTempView(tblName)
      intercept[AnalysisException] {
        spark.readStream.table(tblName)
      }.message.contains("is not a temp view of streaming logical plan")
    }
  }

  test("read: read table without streaming capability support") {
    val tableIdentifier = "testcat.table_name"

    spark.sql(s"CREATE TABLE $tableIdentifier (id bigint, data string) USING foo")

    checkError(
      exception = intercept[AnalysisException] {
        spark.readStream.table(tableIdentifier)
      },
      errorClass = "UNSUPPORTED_FEATURE.TABLE_OPERATION",
      parameters = Map(
        "tableName" -> "`testcat`.`table_name`",
        "operation" -> "either micro-batch or continuous scan"
      )
    )
  }

  test("read: read table with custom catalog") {
    val tblName = "teststream.table_name"
    withTable(tblName) {
      spark.sql(s"CREATE TABLE $tblName (data int) USING foo")
      val stream = MemoryStream[Int]
      val testCatalog = spark.sessionState.catalogManager.catalog("teststream").asTableCatalog
      val table = testCatalog.loadTable(Identifier.of(Array(), "table_name"))
      table.asInstanceOf[InMemoryStreamTable].setStream(stream)

      testStream(spark.readStream.table(tblName)) (
        AddData(stream, 1, 2, 3),
        CheckLastBatch(1, 2, 3),
        AddData(stream, 4, 5),
        CheckLastBatch(4, 5)
      )
    }
  }

  test("read: read table with custom catalog & namespace") {
    spark.sql("CREATE NAMESPACE teststream.ns")

    val tblName = "teststream.ns.table_name"
    withTable(tblName) {
      spark.sql(s"CREATE TABLE $tblName (data int) USING foo")
      val stream = MemoryStream[Int]
      val testCatalog = spark.sessionState.catalogManager.catalog("teststream").asTableCatalog
      val table = testCatalog.loadTable(Identifier.of(Array("ns"), "table_name"))
      table.asInstanceOf[InMemoryStreamTable].setStream(stream)

      testStream(spark.readStream.table(tblName)) (
        AddData(stream, 1, 2, 3),
        CheckLastBatch(1, 2, 3),
        AddData(stream, 4, 5),
        CheckLastBatch(4, 5)
      )
    }
  }

  test("read: fallback to V1 relation") {
    val tblName = DataStreamTableAPISuite.V1FallbackTestTableName
    spark.conf.set(SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION.key,
      classOf[InMemoryStreamTableCatalog].getName)
    val v2Source = classOf[FakeV2Provider].getName
    withTempDir { tempDir =>
      withTable(tblName) {
        spark.sql(s"CREATE TABLE $tblName (data int) USING $v2Source")

        // Check the StreamingRelationV2 has been replaced by StreamingRelation
        val exists = spark.readStream.option("path", tempDir.getCanonicalPath).table(tblName)
          .queryExecution.analyzed.exists(_.isInstanceOf[StreamingRelationV2])
        assert(!exists)
      }
    }
  }

  test("write: write to table with custom catalog & no namespace") {
    val tableIdentifier = "testcat.table_name"

    withTable(tableIdentifier) {
      spark.sql(s"CREATE TABLE $tableIdentifier (id bigint, data string) USING foo")
      checkAnswer(spark.table(tableIdentifier), Seq.empty)

      runTestWithStreamAppend(tableIdentifier)
    }
  }

  test("write: write to table with custom catalog & namespace") {
    spark.sql("CREATE NAMESPACE testcat.ns")
    val tableIdentifier = "testcat.ns.table_name"

    withTable(tableIdentifier) {
      spark.sql(s"CREATE TABLE $tableIdentifier (id bigint, data string) USING foo")
      checkAnswer(spark.table(tableIdentifier), Seq.empty)

      runTestWithStreamAppend(tableIdentifier)
    }
  }

  test("write: write to table with default session catalog") {
    val v2Source = classOf[FakeV2ProviderWithCustomSchema].getName
    spark.conf.set(SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION.key,
      classOf[InMemoryTableSessionCatalog].getName)

    spark.sql("CREATE NAMESPACE ns")

    val tableIdentifier = "ns.table_name"
    withTable(tableIdentifier) {
      spark.sql(s"CREATE TABLE $tableIdentifier (id bigint, data string) USING $v2Source")
      checkAnswer(spark.table(tableIdentifier), Seq.empty)

      runTestWithStreamAppend(tableIdentifier)
    }
  }

  test("write: write to non-exist table with custom catalog") {
    val tableIdentifier = "testcat.nonexistenttable"

    withTable(tableIdentifier) {
      runTestWithStreamAppend(tableIdentifier)
    }
  }

  test("write: write to temporary view isn't allowed yet") {
    val tableIdentifier = "testcat.table_name"
    val tempViewIdentifier = "temp_view"

    spark.sql(s"CREATE TABLE $tableIdentifier (id bigint, data string) USING foo")
    checkAnswer(spark.table(tableIdentifier), Seq.empty)

    spark.table(tableIdentifier).createOrReplaceTempView(tempViewIdentifier)

    withTempDir { checkpointDir =>
      val exc = intercept[AnalysisException] {
        runStreamQueryAppendMode(tempViewIdentifier, checkpointDir, Seq.empty, Seq.empty)
      }
      assert(exc.getMessage.contains("doesn't support streaming write"))
    }
  }

  test("write: write to view shouldn't be allowed") {
    val tableIdentifier = "testcat.table_name"
    val viewIdentifier = "table_view"

    spark.sql(s"CREATE TABLE $tableIdentifier (id bigint, data string) USING foo")
    checkAnswer(spark.table(tableIdentifier), Seq.empty)

    spark.sql(s"CREATE VIEW $viewIdentifier AS SELECT id, data FROM $tableIdentifier")

    withTempDir { checkpointDir =>
      val exc = intercept[AnalysisException] {
        runStreamQueryAppendMode(viewIdentifier, checkpointDir, Seq.empty, Seq.empty)
      }
      assert(exc.getMessage.contains(s"Streaming into views $viewIdentifier is not supported"))
    }
  }

  test("write: write to an external table") {
    withTempDir { dir =>
      val tableName = "stream_test"
      withTable(tableName) {
        checkForStreamTable(Some(dir), tableName)
      }
    }
  }

  test("write: write to a managed table") {
    val tableName = "stream_test"
    withTable(tableName) {
      checkForStreamTable(None, tableName)
    }
  }

  test("write: write to an external table with existing path") {
    withTempDir { dir =>
      val tableName = "stream_test"
      withTable(tableName) {
        // The file written by batch will not be seen after the table was written by a streaming
        // query. This is because we load files from the metadata log instead of listing them
        // using HDFS API.
        Seq(4, 5, 6).toDF("value").write.format("parquet")
          .option("path", dir.getCanonicalPath).saveAsTable(tableName)

        checkForStreamTable(Some(dir), tableName)
      }
    }
  }

  test("write: write to a managed table with existing path") {
    val tableName = "stream_test"
    withTable(tableName) {
      // The file written by batch will not be seen after the table was written by a streaming
      // query. This is because we load files from the metadata log instead of listing them
      // using HDFS API.
      Seq(4, 5, 6).toDF("value").write.format("parquet").saveAsTable(tableName)

      checkForStreamTable(None, tableName)
    }
  }

  test("write: write to an external path and create table") {
    withTempDir { dir =>
      val tableName = "stream_test"
      withTable(tableName) {
        // The file written by batch will not be seen after the table was written by a streaming
        // query. This is because we load files from the metadata log instead of listing them
        // using HDFS API.
        Seq(4, 5, 6).toDF("value").write
          .mode("append").format("parquet").save(dir.getCanonicalPath)

        checkForStreamTable(Some(dir), tableName)
      }
    }
  }

  test("write: write to table with different format shouldn't be allowed") {
    val tableName = "stream_test"

    spark.sql(s"CREATE TABLE $tableName (id bigint, data string) USING json")
    checkAnswer(spark.table(tableName), Seq.empty)

    withTempDir { checkpointDir =>
      val exc = intercept[AnalysisException] {
        runStreamQueryAppendMode(tableName, checkpointDir, Seq.empty, Seq.empty)
      }
      assert(exc.getMessage.contains("The input source(parquet) is different from the table " +
        s"$tableName's data source provider(json)"))
    }
  }

  test("write: write to new table with clusterBy") {
    val tableIdentifier = "testcat.cluster_test"

    withTable(tableIdentifier) {
      runStreamAppendWithClusterBy(tableIdentifier)

      val table = spark.sessionState.catalogManager.catalog("testcat").asTableCatalog
        .loadTable(Identifier.of(Array(), "cluster_test"))
      assert(table.partitioning === Seq(ClusterByTransform(Seq(FieldReference("id")))))
    }
  }

  test("write: write to existing table with matching clustering column using clusterBy") {
    val tableIdentifier = "testcat.cluster_test"

    withTable(tableIdentifier) {
      sql(s"CREATE TABLE $tableIdentifier (id BIGINT, data STRING) CLUSTER BY (id)")
      runStreamAppendWithClusterBy(tableIdentifier)

      val table = spark.sessionState.catalogManager.catalog("testcat").asTableCatalog
        .loadTable(Identifier.of(Array(), "cluster_test"))
      assert(table.partitioning === Seq(ClusterByTransform(Seq(FieldReference("id")))))
    }
  }

  test("explain with table on DSv1 data source") {
    val tblSourceName = "tbl_src"
    val tblTargetName = "tbl_target"
    val tblSourceQualified = s"default.$tblSourceName"
    val tblTargetQualified = s"`default`.`$tblTargetName`"

    withTable(tblSourceQualified, tblTargetQualified) {
      withTempDir { dir =>
        sql(s"CREATE TABLE $tblSourceQualified (col1 string, col2 integer) USING parquet")
        sql(s"CREATE TABLE $tblTargetQualified (col1 string, col2 integer) USING parquet")

        sql(s"INSERT INTO $tblSourceQualified VALUES ('a', 1)")
        sql(s"INSERT INTO $tblSourceQualified VALUES ('b', 2)")
        sql(s"INSERT INTO $tblSourceQualified VALUES ('c', 3)")

        val df = spark.readStream.table(tblSourceQualified)
        val sq = df.writeStream
          .format("parquet")
          .option("checkpointLocation", dir.getCanonicalPath)
          .toTable(tblTargetQualified)
          .asInstanceOf[StreamingQueryWrapper].streamingQuery

        try {
          sq.processAllAvailable()

          val explainWithoutExtended = sq.explainInternal(false)
          // `extended = false` only displays the physical plan.
          assert("FileScan".r
            .findAllMatchIn(explainWithoutExtended).size === 1)
          assert(tblSourceName.r
            .findAllMatchIn(explainWithoutExtended).size === 1)

          // We have marker node for DSv1 sink only in logical node. In physical plan, there is no
          // information for DSv1 sink.

          val explainWithExtended = sq.explainInternal(true)
          // `extended = true` displays 3 logical plans (Parsed/Analyzed/Optimized) and 1 physical
          // plan.
          assert("Relation".r
            .findAllMatchIn(explainWithExtended).size === 3)
          assert("FileScan".r
            .findAllMatchIn(explainWithExtended).size === 1)
          // we don't compare with exact number since the number is also affected by SubqueryAlias
          assert(tblSourceQualified.r
            .findAllMatchIn(explainWithExtended).size >= 4)

          assert("WriteToMicroBatchDataSourceV1".r
            .findAllMatchIn(explainWithExtended).size === 2)
          assert(tblTargetQualified.r
            .findAllMatchIn(explainWithExtended).size >= 2)
        } finally {
          sq.stop()
        }
      }
    }
  }

  test("explain with table on DSv2 data source") {
    val tblSourceName = "tbl_src"
    val tblTargetName = "tbl_target"
    val tblSourceQualified = s"teststream.ns.$tblSourceName"
    val tblTargetQualified = s"testcat.ns.$tblTargetName"

    spark.sql("CREATE NAMESPACE teststream.ns")
    spark.sql("CREATE NAMESPACE testcat.ns")

    withTable(tblSourceQualified, tblTargetQualified) {
      withTempDir { dir =>
        sql(s"CREATE TABLE $tblSourceQualified (value int) USING foo")
        sql(s"CREATE TABLE $tblTargetQualified (col1 string, col2 integer) USING foo")

        val stream = MemoryStream[Int]
        val testCatalog = spark.sessionState.catalogManager.catalog("teststream").asTableCatalog
        val table = testCatalog.loadTable(Identifier.of(Array("ns"), tblSourceName))
        table.asInstanceOf[InMemoryStreamTable].setStream(stream)

        val df = spark.readStream.table(tblSourceQualified)
          .select(lit('a'), $"value")
        val sq = df.writeStream
          .option("checkpointLocation", dir.getCanonicalPath)
          .toTable(tblTargetQualified)
          .asInstanceOf[StreamingQueryWrapper].streamingQuery

        try {
          stream.addData(1, 2, 3)

          sq.processAllAvailable()

          val explainWithoutExtended = sq.explainInternal(false)
          // `extended = false` only displays the physical plan.
          // we don't guarantee the table information is available in physical plan.
          assert("MicroBatchScan".r
            .findAllMatchIn(explainWithoutExtended).size === 1)
          assert("WriteToDataSourceV2".r
            .findAllMatchIn(explainWithoutExtended).size === 1)

          val explainWithExtended = sq.explainInternal(true)
          // `extended = true` displays 3 logical plans (Parsed/Analyzed/Optimized) and 1 physical
          // plan.
          assert("StreamingDataSourceV2ScanRelation".r
            .findAllMatchIn(explainWithExtended).size === 3)
          // WriteToMicroBatchDataSource is used for both parsed and analyzed logical plan
          assert("WriteToMicroBatchDataSource".r
            .findAllMatchIn(explainWithExtended).size === 2)
          // optimizer replaces WriteToMicroBatchDataSource to WriteToDataSourceV2
          assert("WriteToDataSourceV2".r
            .findAllMatchIn(explainWithExtended).size === 2)
          assert("MicroBatchScan".r
            .findAllMatchIn(explainWithExtended).size === 1)

          assert(tblSourceQualified.r
            .findAllMatchIn(explainWithExtended).size >= 3)
          assert(tblTargetQualified.r
            .findAllMatchIn(explainWithExtended).size >= 3)
        } finally {
          sq.stop()
        }
      }
    }
  }

  test("SPARK-39940: refresh table when streaming query writes to the catalog table via DSv1") {
    withTable("tbl1", "tbl2") {
      withTempDir { dir =>
        val baseTbls = new File(dir, "tables")
        val tbl1File = new File(baseTbls, "tbl1")
        val tbl2File = new File(baseTbls, "tbl2")
        val checkpointLocation = new File(dir, "checkpoint")

        val format = "parquet"
        Seq((1, 2)).toDF("i", "d")
          .write.format(format).option("path", tbl1File.getCanonicalPath).saveAsTable("tbl1")

        val query = spark.readStream.format(format).table("tbl1")
          .writeStream.format(format)
          .option("checkpointLocation", checkpointLocation.getCanonicalPath)
          .option("path", tbl2File.getCanonicalPath)
          .toTable("tbl2")

        try {
          query.processAllAvailable()
          checkAnswer(spark.table("tbl2").sort($"i"), Seq(Row(1, 2)))

          Seq((3, 4)).toDF("i", "d")
            .write.format(format).option("path", tbl1File.getCanonicalPath)
            .mode(SaveMode.Append).saveAsTable("tbl1")

          query.processAllAvailable()
          checkAnswer(spark.table("tbl2").sort($"i"), Seq(Row(1, 2), Row(3, 4)))

          assert(query.exception.isEmpty, "No exception should happen in streaming query: " +
            s"exception - ${query.exception}")
        } finally {
          query.stop()
        }
      }
    }
  }

  test("SPARK-41040: self-union using readStream.table should not fail") {
    withTable("self_union_table") {
      spark.range(10).write.format("parquet").saveAsTable("self_union_table")
      val df = spark.readStream.format("parquet").table("self_union_table")
      val q = df.union(df).writeStream.format("noop").start()
      try {
        q.processAllAvailable()
      } finally {
        q.stop()
      }
    }
  }

  test("SPARK-44865: Test StreamingRelationV2 with metadata column") {
    val tblName = "teststream.table_name"
    withTable(tblName) {
      spark.sql(s"CREATE TABLE $tblName (data int) USING foo")
      val stream = MemoryStream[Int]
      val testCatalog = spark.sessionState.catalogManager.catalog("teststream").asTableCatalog
      val table = testCatalog.loadTable(Identifier.of(Array(), "table_name"))
      table.asInstanceOf[InMemoryStreamTable].setStream(stream)
      // It will not throw UNRESOLVED_COLUMN exception because
      // we add metadata column to StreamingRelationV2
      spark.readStream.table(tblName).select("value", "_seq")
    }
  }

  private def checkForStreamTable(dir: Option[File], tableName: String): Unit = {
    val memory = MemoryStream[Int]
    val dsw = memory.toDS().writeStream.format("parquet")
    dir.foreach { output =>
      dsw.option("path", output.getCanonicalPath)
    }
    val sq = dsw
      .option("checkpointLocation", Utils.createTempDir().getCanonicalPath)
      .toTable(tableName)
    memory.addData(1, 2, 3)
    sq.processAllAvailable()

    checkDataset(
      spark.table(tableName).as[Int],
      1, 2, 3)
    val catalogTable = spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName))
    val path = if (dir.nonEmpty) {
      dir.get
    } else {
      new File(catalogTable.location)
    }
    checkDataset(
      spark.read.format("parquet").load(path.getCanonicalPath).as[Int],
      1, 2, 3)
  }

  private def runTestWithStreamAppend(tableIdentifier: String) = {
    withTempDir { checkpointDir =>
      val input1 = Seq((1L, "a"), (2L, "b"), (3L, "c"))
      verifyStreamAppend(tableIdentifier, checkpointDir, Seq.empty, input1, input1)

      val input2 = Seq((4L, "d"), (5L, "e"), (6L, "f"))
      verifyStreamAppend(tableIdentifier, checkpointDir, Seq(input1), input2, input1 ++ input2)
    }
  }

  private def runStreamQueryAppendMode(
      tableIdentifier: String,
      checkpointDir: File,
      prevInputs: Seq[Seq[(Long, String)]],
      newInputs: Seq[(Long, String)]): Unit = {
    val inputData = MemoryStream[(Long, String)]
    val inputDF = inputData.toDF().toDF("id", "data")

    prevInputs.foreach { inputsPerBatch =>
      inputData.addData(inputsPerBatch: _*)
    }

    val query = inputDF
      .writeStream
      .option("checkpointLocation", checkpointDir.getAbsolutePath)
      .toTable(tableIdentifier)

    inputData.addData(newInputs: _*)

    query.processAllAvailable()
    query.stop()
  }

  private def verifyStreamAppend(
      tableIdentifier: String,
      checkpointDir: File,
      prevInputs: Seq[Seq[(Long, String)]],
      newInputs: Seq[(Long, String)],
      expectedOutputs: Seq[(Long, String)]): Unit = {
    runStreamQueryAppendMode(tableIdentifier, checkpointDir, prevInputs, newInputs)
    checkAnswer(
      spark.table(tableIdentifier),
      expectedOutputs.map { case (id, data) => Row(id, data) }
    )
  }

  private def runStreamAppendWithClusterBy(tableIdentifier: String): Unit = {
    withTempDir { ckptDir =>
      val inputData = MemoryStream[(Long, String)]
      val inputDF = inputData.toDF().toDF("id", "data")

      val query = inputDF
        .writeStream
        .clusterBy("id")
        .option("checkpointLocation", ckptDir.getAbsolutePath)
        .toTable(tableIdentifier)

      inputData.addData(Seq((1L, "a"), (2L, "b"), (3L, "c")))

      query.processAllAvailable()
      query.stop()
    }
  }
}

object DataStreamTableAPISuite {
  val V1FallbackTestTableName = "fallbackV1Test"
}

class InMemoryStreamTable(override val name: String)
  extends Table
  with SupportsRead
  with SupportsMetadataColumns {
  var stream: MemoryStream[Int] = _

  def setStream(inputData: MemoryStream[Int]): Unit = stream = inputData

  override def schema(): StructType = stream.fullSchema()

  override def capabilities(): util.Set[TableCapability] = {
    util.EnumSet.of(TableCapability.MICRO_BATCH_READ, TableCapability.CONTINUOUS_READ)
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new MemoryStreamScanBuilder(stream)
  }

  private object SeqColumn extends MetadataColumn {
    override def name: String = "_seq"
    override def dataType: DataType = IntegerType
    override def comment: String = "Seq"
  }

  override val metadataColumns: Array[MetadataColumn] = Array(SeqColumn)
}

class NonStreamV2Table(override val name: String)
    extends Table with SupportsRead with V2TableWithV1Fallback {
  override def schema(): StructType = StructType(Nil)
  override def capabilities(): util.Set[TableCapability] =
    util.EnumSet.of(TableCapability.BATCH_READ)
  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = new FakeScanBuilder

  override def v1Table: CatalogTable = {
    CatalogTable(
      identifier =
        TableIdentifier(DataStreamTableAPISuite.V1FallbackTestTableName, Some("default")),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat.empty,
      owner = null,
      schema = schema(),
      provider = Some("parquet"))
  }
}


class InMemoryStreamTableCatalog extends InMemoryTableCatalog {
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  override def createTable(
      ident: Identifier,
      columns: Array[Column],
      partitions: Array[Transform],
      properties: util.Map[String, String]): Table = {
    if (tables.containsKey(ident)) {
      throw new TableAlreadyExistsException(ident.asMultipartIdentifier)
    }

    val table = if (ident.name() == DataStreamTableAPISuite.V1FallbackTestTableName) {
      new NonStreamV2Table(s"$name.${ident.quoted}")
    } else {
      new InMemoryStreamTable(s"$name.${ident.quoted}")
    }
    tables.put(ident, table)
    namespaces.putIfAbsent(ident.namespace.toList, Map())
    table
  }
}
