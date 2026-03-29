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

package org.apache.spark.sql.internal

import java.io.File

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType

class DeprecatedCreateExternalTableSuite extends SharedSparkSession {
  test("createExternalTable with explicit path") {
    withTable("t") {
      withTempDir { dir =>
        val path = new File(dir, "test")
        spark.range(100).write.parquet(path.getAbsolutePath)
        spark.catalog.createExternalTable(
          tableName = "t",
          path = path.getAbsolutePath
        )
        assert(spark.sessionState.catalog.tableExists(TableIdentifier("t")))
        val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier("t"))
        assert(table.tableType === CatalogTableType.EXTERNAL)
        assert(table.provider === Some("parquet"))
        assert(table.schema === new StructType().add("id", "long"))
        assert(table.storage.locationUri.get == makeQualifiedPath(path.getAbsolutePath))
      }
    }
  }

  test("createExternalTable with 'path' options") {
    withTable("t") {
      withTempDir { dir =>
        val path = new File(dir, "test")
        spark.range(100).write.parquet(path.getAbsolutePath)
        spark.catalog.createExternalTable(
          tableName = "t",
          source = "parquet",
          options = Map("path" -> path.getAbsolutePath))
        assert(spark.sessionState.catalog.tableExists(TableIdentifier("t")))
        val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier("t"))
        assert(table.tableType === CatalogTableType.EXTERNAL)
        assert(table.provider === Some("parquet"))
        assert(table.schema === new StructType().add("id", "long"))
        assert(table.storage.locationUri.get == makeQualifiedPath(path.getAbsolutePath))
      }
    }
  }

  test("createExternalTable with explicit schema") {
    withTable("t") {
      withTempDir { dir =>
        val path = new File(dir, "test")
        spark.range(100).write.parquet(path.getAbsolutePath)
        spark.catalog.createExternalTable(
          tableName = "t",
          source = "parquet",
          schema = new StructType().add("i", "int"),
          options = Map("path" -> path.getAbsolutePath))
        assert(spark.sessionState.catalog.tableExists(TableIdentifier("t")))
        val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier("t"))
        assert(table.tableType === CatalogTableType.EXTERNAL)
        assert(table.provider === Some("parquet"))
        assert(table.schema === new StructType().add("i", "int"))
        assert(table.storage.locationUri.get == makeQualifiedPath(path.getAbsolutePath))
      }
    }
  }
}
