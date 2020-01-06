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

package org.apache.spark.sql.execution.datasources.v2

import scala.collection.JavaConverters._

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.connector.catalog.{SupportsDelete, SupportsRead, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

object DataSourceV2Implicits {
  implicit class TableHelper(table: Table) {
    def asReadable: SupportsRead = {
      table match {
        case support: SupportsRead =>
          support
        case _ =>
          throw new AnalysisException(s"Table does not support reads: ${table.name}")
      }
    }

    def asWritable: SupportsWrite = {
      table match {
        case support: SupportsWrite =>
          support
        case _ =>
          throw new AnalysisException(s"Table does not support writes: ${table.name}")
      }
    }

    def asDeletable: SupportsDelete = {
      table match {
        case support: SupportsDelete =>
          support
        case _ =>
          throw new AnalysisException(s"Table does not support deletes: ${table.name}")
      }
    }

    def supports(capability: TableCapability): Boolean = table.capabilities.contains(capability)

    def supportsAny(capabilities: TableCapability*): Boolean = capabilities.exists(supports)
  }

  implicit class OptionsHelper(options: Map[String, String]) {
    def asOptions: CaseInsensitiveStringMap = {
      new CaseInsensitiveStringMap(options.asJava)
    }
  }
}
