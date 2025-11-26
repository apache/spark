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

import java.util

import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.{InputPartition, ScanBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/** Used as a V2 DataSource for V2SessionCatalog DDL */
class FakeV2Provider extends TableProvider {

  // Supports external metadata so that users can specify schema and partitions
  // when creating the table.
  override def supportsExternalMetadata(): Boolean = true

  class MyScanBuilder extends SimpleScanBuilder {
    override def planInputPartitions(): Array[InputPartition] = {
      Array(RangeInputPartition(0, 5), RangeInputPartition(5, 10))
    }
  }

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    FakeV2Provider.schema
  }

  def getTable(options: CaseInsensitiveStringMap): Table = {
    new SimpleBatchTable {
      override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
        new MyScanBuilder()
      }
    }
  }

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]): Table = {
    getTable(new CaseInsensitiveStringMap(properties))
  }
}

object FakeV2Provider {
  val schema: StructType = new StructType().add("i", "int").add("j", "int")
}

class FakeV2ProviderWithCustomSchema extends FakeV2Provider {
  class FakeTable(
      schema: StructType,
      partitioning: Array[Transform],
      options: CaseInsensitiveStringMap) extends SimpleBatchTable {
    override def schema(): StructType = schema

    override def partitioning(): Array[Transform] = partitioning

    override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
      new MyScanBuilder()
    }
  }

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]): Table = {
    new FakeTable(schema, partitioning, new CaseInsensitiveStringMap(properties))
  }
}
