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

package org.apache.spark.sql.connector

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{BasicInMemoryTableCatalog, Identifier, SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.{LocalScan, Scan, ScanBuilder}
import org.apache.spark.sql.execution.LocalTableScanExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class LocalScanSuite extends QueryTest with SharedSparkSession {
  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(SQLConf.DEFAULT_CATALOG.key, "testcat")
    spark.conf.set("spark.sql.catalog.testcat", classOf[TestLocalScanCatalog].getName)
    sql("CREATE TABLE testcat.tbl(i int)")
  }

  override def afterAll(): Unit = {
    spark.conf.unset(SQLConf.DEFAULT_CATALOG.key)
    spark.conf.unset("spark.sql.catalog.testcat")
    super.afterAll()
  }

  test("full scan") {
    val df = spark.table("testcat.tbl")
    assert(df.schema == TestLocalScanTable.schema)

    val localScan = df.queryExecution.executedPlan.collect {
      case s: LocalTableScanExec => s
    }
    assert(localScan.length == 1)
    checkAnswer(df, TestLocalScanTable.data.map(Row(_)))
  }
}

class TestLocalScanCatalog extends BasicInMemoryTableCatalog {
  override def createTable(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: java.util.Map[String, String]): Table = {
    val table = new TestLocalScanTable(ident.toString)
    tables.put(ident, table)
    table
  }
}

object TestLocalScanTable {
  val schema = new StructType().add("i", "int")
  val data = Seq(1, 2, 3)
}

class TestLocalScanTable(override val name: String) extends Table with SupportsRead {
  override def schema(): StructType = TestLocalScanTable.schema

  override def capabilities(): java.util.Set[TableCapability] =
    java.util.EnumSet.of(TableCapability.BATCH_READ)

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    new TestLocalScanBuilder

  private class TestLocalScanBuilder extends ScanBuilder {
    override def build(): Scan = new TestLocalScan
  }

  private class TestLocalScan extends LocalScan {
    override def rows(): Array[InternalRow] = TestLocalScanTable.data.map(InternalRow(_)).toArray

    override def readSchema(): StructType = TestLocalScanTable.schema
  }
}
