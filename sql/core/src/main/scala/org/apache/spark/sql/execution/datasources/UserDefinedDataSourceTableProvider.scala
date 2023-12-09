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

package org.apache.spark.sql.execution.datasources

import java.util

import org.apache.spark.SparkException
import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class UserDefinedDataSourceTableProvider(
    name: String,
    builder: UserDefinedDataSourceBuilder) extends TableProvider {
  private var planBuilder: UserDefinedDataSourcePlanBuilder = _

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    assert(planBuilder == null)
    // When we reach here, it means there is no user-specified schema
    planBuilder = builder.build(name, userSpecifiedSchema = None, options = options)
    planBuilder.schema
  }

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]): Table = {
    assert(partitioning.isEmpty)
    if (planBuilder == null) {
      // When we reach here, it means there is user-specified schema
      planBuilder = builder.build(
        name,
        userSpecifiedSchema = Some(schema),
        options = new CaseInsensitiveStringMap(properties))
    } else {
      assert(schema == planBuilder.schema)
    }
    UserDefinedDataSourceTable(name, planBuilder)
  }
}

case class UserDefinedDataSourceTable(
    name: String,
    builder: UserDefinedDataSourcePlanBuilder) extends Table
  with SupportsRead with SupportsWrite {
  override def schema(): StructType = builder.schema
  override def capabilities(): util.Set[TableCapability] = {
    util.EnumSet.of(TableCapability.BATCH_READ, TableCapability.BATCH_WRITE)
  }
  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    throw SparkException.internalError(
      "UserDefinedDataSourceTable.newScanBuilder should not be called.")
  }
  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    throw SparkException.internalError(
      "UserDefinedDataSourceTable.newWriteBuilder should not be called.")
  }
}
