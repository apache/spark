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

package org.apache.spark.sql.execution.streaming.sources

import java.util

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}
import org.apache.spark.sql.connector.read.streaming.{ContinuousStream, MicroBatchStream}
import org.apache.spark.sql.internal.connector.SimpleTableProvider
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.{MapType, StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class PythonProfileSourceProvider extends SimpleTableProvider with DataSourceRegister with Logging {
  override def getTable(options: CaseInsensitiveStringMap): Table = new PythonProfileTable()

  override def shortName(): String = "socket"
}

class PythonProfileTable()
  extends Table with SupportsRead {

  override def name(): String = s"PythonProfile"

  override def schema(): StructType = {
    StructType(StructField("key", MapType(keyType = StringType, valueType = StringType)) :: Nil)
  }

  override def capabilities(): util.Set[TableCapability] = {
    util.EnumSet.of(TableCapability.MICRO_BATCH_READ, TableCapability.CONTINUOUS_READ)
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = () => new Scan {
    override def readSchema(): StructType = {
      import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
      columns.asSchema
    }

    override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream = {
      new PythonProfileMicroBatchStream()
    }

    override def toContinuousStream(checkpointLocation: String): ContinuousStream = {
      throw new UnsupportedOperationException()
    }

    override def columnarSupportMode(): Scan.ColumnarSupportMode =
      Scan.ColumnarSupportMode.UNSUPPORTED
  }
}
