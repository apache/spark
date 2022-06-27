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

package org.apache.spark.sql.hive.execution.command

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType, SessionCatalog}
import org.apache.spark.sql.execution.command.v1
import org.apache.spark.sql.internal.HiveSerDe
import org.apache.spark.sql.types.{IntegerType, MetadataBuilder, StructField, StructType}

/**
 * The class contains tests for the `ALTER TABLE .. SET [SERDE|SERDEPROPERTIES]` command to check
 * V1 Hive external table catalog.
 */
class AlterTableSetSerdeSuite extends v1.AlterTableSetSerdeSuiteBase with CommandSuiteBase {

  override def afterEach(): Unit = {
    try {
      // drop all databases, tables and functions after each test
      spark.sessionState.catalog.reset()
    } finally {
      super.afterEach()
    }
  }

  protected override def generateTable(
    catalog: SessionCatalog,
    name: TableIdentifier,
    isDataSource: Boolean,
    partitionCols: Seq[String] = Seq("a", "b")): CatalogTable = {
    val storage =
      if (isDataSource) {
        val serde = HiveSerDe.sourceToSerDe("parquet")
        assert(serde.isDefined, "The default format is not Hive compatible")
        CatalogStorageFormat(
          locationUri = Some(catalog.defaultTablePath(name)),
          inputFormat = serde.get.inputFormat,
          outputFormat = serde.get.outputFormat,
          serde = serde.get.serde,
          compressed = false,
          properties = Map.empty)
      } else {
        CatalogStorageFormat(
          locationUri = Some(catalog.defaultTablePath(name)),
          inputFormat = Some("org.apache.hadoop.mapred.SequenceFileInputFormat"),
          outputFormat = Some("org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat"),
          serde = Some("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"),
          compressed = false,
          properties = Map("serialization.format" -> "1"))
      }
    val metadata = new MetadataBuilder()
      .putString("key", "value")
      .build()
    val schema = new StructType()
      .add("col1", "int", nullable = true, metadata = metadata)
      .add("col2", "string")
    CatalogTable(
      identifier = name,
      tableType = CatalogTableType.EXTERNAL,
      storage = storage,
      schema = schema.copy(
        fields = schema.fields ++ partitionCols.map(StructField(_, IntegerType))),
      provider = if (isDataSource) Some("parquet") else Some("hive"),
      partitionColumnNames = partitionCols,
      createTime = 0L,
      createVersion = org.apache.spark.SPARK_VERSION,
      tracksPartitionsInCatalog = true)
  }

  test("alter table: set serde") {
    testSetSerde(isDatasourceTable = false)
  }

  test("alter table: set serde partition") {
    testSetSerdePartition(isDatasourceTable = false)
  }
}
