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
package org.apache.spark.sql.rabbitmq.v2

import java.util

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, Column, SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.rabbitmq.common.RmqPropsHolder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap


class RmqTable(structType: StructType, rmqPropsHolder: RmqPropsHolder)
  extends Table with SupportsRead with Logging {

  override def name(): String = s"rmq-stream:${rmqPropsHolder.getQueueName}"

  override def columns: Array[Column] = CatalogV2Util.structTypeToV2Columns(structType)

  override def capabilities(): util.HashSet[TableCapability] =
    new java.util.HashSet[TableCapability](java.util.Arrays.asList(
      TableCapability.MICRO_BATCH_READ,
    ))

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    logInfo(s"[RMQ V2] newScanBuilder: queue=${rmqPropsHolder.getQueueName}")
    new RmqScanBuilder(options, structType)
  }
}
