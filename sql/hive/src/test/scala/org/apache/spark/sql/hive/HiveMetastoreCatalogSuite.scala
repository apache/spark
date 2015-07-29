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

package org.apache.spark.sql.hive

import java.io.File

import org.scalatest.BeforeAndAfterAll

import org.apache.spark.Logging
import org.apache.spark.util.Utils
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.hive.client.{ExternalTable, HiveColumn, ManagedTable}
import org.apache.spark.sql.{AnalysisException, SQLConf}
import org.apache.spark.sql.sources.DataSourceTest
import org.apache.spark.sql.test.ExamplePointUDT
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.hive.test.TestHive
import org.apache.spark.sql.hive.test.TestHive._
import org.apache.spark.sql.hive.test.TestHive.implicits._


class HiveMetastoreCatalogSuite extends SparkFunSuite with Logging {

  test("struct field should accept underscore in sub-column name") {
    val metastr = "struct<a: int, b_1: string, c: string>"

    val datatype = HiveMetastoreTypes.toDataType(metastr)
    assert(datatype.isInstanceOf[StructType])
  }

  test("udt to metastore type conversion") {
    val udt = new ExamplePointUDT
    assert(HiveMetastoreTypes.toMetastoreType(udt) ===
      HiveMetastoreTypes.toMetastoreType(udt.sqlType))
  }

  test("duplicated metastore relations") {
    import TestHive.implicits._
    val df = TestHive.sql("SELECT * FROM src")
    logInfo(df.queryExecution.toString)
    df.as('a).join(df.as('b), $"a.key" === $"b.key")
  }
}

class DataSourceWithHiveMetastoreCatalogSuite extends DataSourceTest with BeforeAndAfterAll {
  var path: File = null

  override def beforeAll(): Unit = {
    path = Utils.createTempDir()
    Utils.deleteRecursively(path)
  }

  override def afterAll(): Unit = {}

  after {
    Utils.deleteRecursively(path)
  }

  test("SPARK-7550 / SPARK-6923 Support setting the right schema & serde to Hive metastore #1") {
    import org.apache.spark.sql.SaveMode
    val df = Seq((1, "1", 2), (2, "2", 2)).toDF("d1", "d2", "p")
    df.write
      .mode(SaveMode.Overwrite)
      .partitionBy("p")
      .format("parquet")
      .saveAsTable("datasource_7550_1")

    val hiveTable = catalog.client.getTable("default", "datasource_7550_1")
    val columns = hiveTable.schema
    assert(columns.length === 2)
    assert(columns(0).name === "d1")
    assert(columns(0).hiveType === "int")

    assert(columns(1).name === "d2")
    assert(columns(1).hiveType === "string")

    assert(hiveTable.inputFormat ===
      Some("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"))
    assert(hiveTable.outputFormat ===
      Some("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"))
    assert(hiveTable.serde ===
      Some("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"))

    assert(hiveTable.isPartitioned === true)
    assert(hiveTable.tableType === ManagedTable)
    assert(hiveTable.partitionColumns.length === 1)
    assert(hiveTable.partitionColumns.head === HiveColumn("p", "int", ""))
  }

  test("SPARK-7550 / SPARK-6923 Support setting the right schema & serde to Hive metastore #2") {
    import org.apache.spark.sql.SaveMode
    val df = Seq((1, "1", 2), (2, "2", 2)).toDF("d1", "d2", "p")

    df.write
      .mode(SaveMode.ErrorIfExists)
      .partitionBy("p")
      .format("parquet")
      .option("path", path.toString)
      .saveAsTable("datasource_7550_2")

    val hiveTable = catalog.client.getTable("default", "datasource_7550_2")

    assert(hiveTable.isPartitioned === true)
    assert(hiveTable.tableType === ExternalTable)
    // hiveTable.location is "file:///tmp/..." while path.toString just be "/tmp/.."
    assert(hiveTable.location.get.contains(path.toString))
  }

  test("SPARK-7550 / SPARK-6923 Support setting the right schema & serde to Hive metastore #3") {
    // Ignore to write the meta into Hive Metastore, as we don't support the json for Hive
    import org.apache.spark.sql.SaveMode
    val df = Seq((1, "1", 2), (2, "2", 2)).toDF("d1", "d2", "p")
    df.write
      .mode(SaveMode.ErrorIfExists)
      .format("json")
      .saveAsTable("datasource_7550_3")
    assert(2 === sql("SELECT * from datasource_7550_3").count())
  }

  test("SPARK-7550 / SPARK-6923 Support setting the right schema & serde to Hive metastore #4") {
    // Ignore to write the meta into Hive Metastore, as we don't support the DataSource for Hive
    sql(
      """CREATE TABLE datasource_7550_4
        | USING org.apache.spark.sql.sources.DDLScanSource
        | OPTIONS (
        |   From '1',
        |   To '10',
        |   Table 'test1')
      """.stripMargin)
    assert(10 === sql("SELECT * from datasource_7550_4").count())
  }

  test("SPARK-7550 / SPARK-6923 Support setting the right schema & serde to Hive metastore #5") {
    sql(s"""
       |CREATE TABLE datasource_7550_5
       |USING orc
       |OPTIONS (
       |  path '${path.getCanonicalPath}'
       |) AS
       |SELECT 1 as d1, "aa" as d2
    """.stripMargin)

    val hiveTable = catalog.client.getTable("default", "datasource_7550_5")
    val columns = hiveTable.schema
    assert(columns.length === 2)
    assert(columns(0).name === "d1")
    assert(columns(0).hiveType === "int")

    assert(columns(1).name === "d2")
    assert(columns(1).hiveType === "string")

    assert(hiveTable.inputFormat ===
      Some("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat"))
    assert(hiveTable.outputFormat ===
      Some("org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat"))
    assert(hiveTable.serde ===
      Some("org.apache.hadoop.hive.ql.io.orc.OrcSerde"))

    assert(hiveTable.isPartitioned === false)
    assert(hiveTable.tableType === ExternalTable)
    assert(hiveTable.partitionColumns.length === 0)
  }
}
