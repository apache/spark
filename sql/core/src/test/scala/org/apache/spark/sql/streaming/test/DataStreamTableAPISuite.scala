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

import java.util

import scala.collection.JavaConverters._

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2
import org.apache.spark.sql.connector.{FakeV2Provider, InMemoryTableCatalog}
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
  }

  test("table API with file source") {
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

  test("read non-exist table") {
    intercept[AnalysisException] {
      spark.readStream.table("non_exist_table")
    }.message.contains("Table not found")
  }

  test("stream table API with temp view") {
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

  test("stream table API with non-streaming temp view") {
    val tblName = "my_table"
    withTable(tblName) {
      spark.range(3).createOrReplaceTempView(tblName)
      intercept[AnalysisException] {
        spark.readStream.table(tblName)
      }.message.contains("is not a temp view of streaming logical plan")
    }
  }

  test("read table without streaming capability support") {
    val tableIdentifer = "testcat.table_name"

    spark.sql(s"CREATE TABLE $tableIdentifer (id bigint, data string) USING foo")

    intercept[AnalysisException] {
      spark.readStream.table(tableIdentifer)
    }.message.contains("does not support either micro-batch or continuous scan")
  }

  test("read table with custom catalog") {
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

  test("read table with custom catalog & namespace") {
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

  test("fallback to V1 relation") {
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
