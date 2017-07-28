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

import org.apache.hadoop.conf.Configuration

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.types.StructType

/**
 * Test suite for the [[HiveExternalCatalog]].
 */
class HiveExternalCatalogSuite extends ExternalCatalogSuite {

  private val externalCatalog: HiveExternalCatalog = {
    val catalog = new HiveExternalCatalog(new SparkConf, new Configuration)
    catalog.client.reset()
    catalog
  }

  protected override val utils: CatalogTestUtils = new CatalogTestUtils {
    override val tableInputFormat: String = "org.apache.hadoop.mapred.SequenceFileInputFormat"
    override val tableOutputFormat: String = "org.apache.hadoop.mapred.SequenceFileOutputFormat"
    override def newEmptyCatalog(): ExternalCatalog = externalCatalog
    override val defaultProvider: String = "hive"
  }

  protected override def resetState(): Unit = {
    externalCatalog.client.reset()
  }

  import utils._

  test("SPARK-18647: do not put provider in table properties for Hive serde table") {
    val catalog = newBasicCatalog()
    val hiveTable = CatalogTable(
      identifier = TableIdentifier("hive_tbl", Some("db1")),
      tableType = CatalogTableType.MANAGED,
      storage = storageFormat,
      schema = new StructType().add("col1", "int").add("col2", "string"),
      provider = Some("hive"))
    catalog.createTable(hiveTable, ignoreIfExists = false)

    val rawTable = externalCatalog.client.getTable("db1", "hive_tbl")
    assert(!rawTable.properties.contains(HiveExternalCatalog.DATASOURCE_PROVIDER))
    assert(DDLUtils.isHiveTable(externalCatalog.getTable("db1", "hive_tbl")))
  }

  Seq("parquet", "hive").foreach { format =>
    test(s"Partition columns should be put at the end of table schema for the format $format") {
      val catalog = newBasicCatalog()
      val newSchema = new StructType()
        .add("col1", "int")
        .add("col2", "string")
        .add("partCol1", "int")
        .add("partCol2", "string")
      val table = CatalogTable(
        identifier = TableIdentifier("tbl", Some("db1")),
        tableType = CatalogTableType.MANAGED,
        storage = CatalogStorageFormat.empty,
        schema = new StructType()
          .add("col1", "int")
          .add("partCol1", "int")
          .add("partCol2", "string")
          .add("col2", "string"),
        provider = Some(format),
        partitionColumnNames = Seq("partCol1", "partCol2"))
      catalog.createTable(table, ignoreIfExists = false)

      val restoredTable = externalCatalog.getTable("db1", "tbl")
      assert(restoredTable.schema == newSchema)
    }
  }
}
