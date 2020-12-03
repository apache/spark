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

import scala.collection.JavaConverters._

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{NoSuchTableException, TableAlreadyExistsException}
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2
import org.apache.spark.sql.connector.{FakeV2Provider, InMemoryTableCatalog, InMemoryTableSessionCatalog}
import org.apache.spark.sql.connector.catalog.{Identifier, SupportsRead, Table, TableCapability, V2TableWithV1Fallback}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.execution.streaming.{MemoryStream, MemoryStreamScanBuilder}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.StreamTest
import org.apache.spark.sql.streaming.sources.FakeScanBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

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
    intercept[AnalysisException] {
      spark.readStream.table("non_exist_table")
    }.message.contains("Table not found")
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
    val tableIdentifer = "testcat.table_name"

    spark.sql(s"CREATE TABLE $tableIdentifer (id bigint, data string) USING foo")

    intercept[AnalysisException] {
      spark.readStream.table(tableIdentifer)
    }.message.contains("does not support either micro-batch or continuous scan")
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
        val plan = spark.readStream.option("path", tempDir.getCanonicalPath).table(tblName)
          .queryExecution.analyzed.collectFirst {
            case d: StreamingRelationV2 => d
          }
        assert(plan.isEmpty)
      }
    }
  }

  test("write: write to table with custom catalog & no namespace") {
    val tableIdentifier = "testcat.table_name"

    spark.sql(s"CREATE TABLE $tableIdentifier (id bigint, data string) USING foo")
    checkAnswer(spark.table(tableIdentifier), Seq.empty)

    runTestWithStreamAppend(tableIdentifier)
  }

  test("write: write to table with custom catalog & namespace") {
    spark.sql("CREATE NAMESPACE testcat.ns")

    val tableIdentifier = "testcat.ns.table_name"

    spark.sql(s"CREATE TABLE $tableIdentifier (id bigint, data string) USING foo")
    checkAnswer(spark.table(tableIdentifier), Seq.empty)

    runTestWithStreamAppend(tableIdentifier)
  }

  test("write: write to table with default session catalog") {
    val v2Source = classOf[FakeV2Provider].getName
    spark.conf.set(SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION.key,
      classOf[InMemoryTableSessionCatalog].getName)

    spark.sql("CREATE NAMESPACE ns")

    val tableIdentifier = "ns.table_name"
    spark.sql(s"CREATE TABLE $tableIdentifier (id bigint, data string) USING $v2Source")
    checkAnswer(spark.table(tableIdentifier), Seq.empty)

    runTestWithStreamAppend(tableIdentifier)
  }

  test("write: write to non-exist table with custom catalog") {
    val tableIdentifier = "testcat.nonexisttable"
    spark.sql("CREATE NAMESPACE testcat.ns")

    withTempDir { checkpointDir =>
      val exc = intercept[NoSuchTableException] {
        runStreamQueryAppendMode(tableIdentifier, checkpointDir, Seq.empty, Seq.empty)
      }
      assert(exc.getMessage.contains("nonexisttable"))
    }
  }

  test("write: write to file provider based table isn't allowed yet") {
    val tableIdentifier = "table_name"

    spark.sql(s"CREATE TABLE $tableIdentifier (id bigint, data string) USING parquet")
    checkAnswer(spark.table(tableIdentifier), Seq.empty)

    withTempDir { checkpointDir =>
      val exc = intercept[AnalysisException] {
        runStreamQueryAppendMode(tableIdentifier, checkpointDir, Seq.empty, Seq.empty)
      }
      assert(exc.getMessage.contains("doesn't support streaming write"))
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
      assert(exc.getMessage.contains("doesn't support streaming write"))
    }
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
}

object DataStreamTableAPISuite {
  val V1FallbackTestTableName = "fallbackV1Test"
}

class InMemoryStreamTable(override val name: String) extends Table with SupportsRead {
  var stream: MemoryStream[Int] = _

  def setStream(inputData: MemoryStream[Int]): Unit = stream = inputData

  override def schema(): StructType = stream.fullSchema()

  override def capabilities(): util.Set[TableCapability] = {
    Set(TableCapability.MICRO_BATCH_READ, TableCapability.CONTINUOUS_READ).asJava
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new MemoryStreamScanBuilder(stream)
  }
}

class NonStreamV2Table(override val name: String)
    extends Table with SupportsRead with V2TableWithV1Fallback {
  override def schema(): StructType = StructType(Nil)
  override def capabilities(): util.Set[TableCapability] = Set(TableCapability.BATCH_READ).asJava
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
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): Table = {
    if (tables.containsKey(ident)) {
      throw new TableAlreadyExistsException(ident)
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
