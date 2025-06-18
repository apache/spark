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

package org.apache.spark.sql.hive.pipelines

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.connector.catalog.{CatalogV2Util, Identifier, TableCatalog, TableChange, TableInfo}
import org.apache.spark.sql.hive.test.{TestHive, TestHiveContext}
import org.apache.spark.sql.pipelines.graph.MaterializeTablesSuite
import org.apache.spark.sql.test.TestSparkSession
import org.apache.spark.sql.types.{IntegerType, StructType}

class HiveMaterializeTablesSuite extends MaterializeTablesSuite {
  test("super basic") {
    val catalogManager = spark.sessionState.catalogManager
    val catalog = catalogManager.currentCatalog.asInstanceOf[TableCatalog]
    val identifier = Identifier.of(Array("default"), "test_table")
    val outputSchema =
      new StructType().add("a", IntegerType, true, "comment1")
    catalog.createTable(
      identifier,
      new TableInfo.Builder()
        .withProperties(Map.empty.asJava)
        .withColumns(CatalogV2Util.structTypeToV2Columns(outputSchema))
        .withPartitions(Array.empty)
        .build()
    )

    catalog.alterTable(identifier, TableChange.updateColumnComment(Array("a"), "comment2"))
    val table = catalog.loadTable(identifier)
    assert(table.schema() == new StructType().add("a", IntegerType, true, "comment2"))
  }

  protected val hiveContext: TestHiveContext = TestHive

  override def createSparkSession: TestSparkSession = TestHive.sparkSession

  override def afterAll(): Unit = {
    try {
      hiveContext.reset()
    } finally {
      super.afterAll()
    }
  }

  override def afterEach(): Unit = {
    try {
      spark.artifactManager.cleanUpResourcesForTesting()
    } finally {
      super.afterEach()
    }
  }
}
