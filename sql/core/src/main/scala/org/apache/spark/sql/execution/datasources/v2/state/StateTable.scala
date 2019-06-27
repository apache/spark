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
package org.apache.spark.sql.execution.datasources.v2.state

import java.util

import scala.collection.JavaConverters._

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class StateTable(
    session: SparkSession,
    override val schema: StructType,
    checkpointLocation: String,
    version: Int,
    operatorId: Int,
    storeName: String)
  extends Table with SupportsRead {

  import StateTable._

  if (!isValidSchema(schema)) {
    throw new AnalysisException("The fields of schema should be 'key' and 'value', " +
      "and each field should have corresponding fields (they should be a StructType)")
  }

  override def name(): String =
    s"state-table-cp-$checkpointLocation-ver-$version-operator-$operatorId-store-$storeName"

  override def capabilities(): util.Set[TableCapability] = CAPABILITY

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    new StateScanBuilder(session, schema, checkpointLocation, version, operatorId, storeName)

  override def properties(): util.Map[String, String] = Map(
      "checkpointLocation" -> checkpointLocation,
      "version" -> version.toString,
      "operatorId" -> operatorId.toString,
      "storeName" -> storeName).asJava

  private def isValidSchema(schema: StructType): Boolean = {
    if (schema.fieldNames.toSeq != Seq("key", "value")) {
      false
    } else if (!SchemaUtil.getSchemaAsDataType(schema, "key").isInstanceOf[StructType]) {
      false
    } else if (!SchemaUtil.getSchemaAsDataType(schema, "value").isInstanceOf[StructType]) {
      false
    } else {
      true
    }
  }
}

object StateTable {
  private val CAPABILITY = Set(TableCapability.BATCH_READ).asJava
}
