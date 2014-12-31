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

package org.apache.spark.sql.hive.parquet

import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category
import org.apache.hadoop.hive.serde2.{SerDeStats, SerDe}
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.io.Writable

/**
 * A placeholder that allows Spark SQL users to create metastore tables that are stored as
 * parquet files.  It is only intended to pass the checks that the serde is valid and exists
 * when a CREATE TABLE is run.  The actual work of decoding will be done by ParquetTableScan
 * when "spark.sql.hive.convertMetastoreParquet" is set to true.
 */
@deprecated("No code should depend on FakeParquetHiveSerDe as it is only intended as a " +
            "placeholder in the Hive MetaStore", "1.2.0")
class FakeParquetSerDe extends SerDe {
  override def getObjectInspector: ObjectInspector = new ObjectInspector {
    override def getCategory: Category = Category.PRIMITIVE

    override def getTypeName: String = "string"
  }

  override def deserialize(p1: Writable): AnyRef = throwError

  override def initialize(p1: Configuration, p2: Properties): Unit = {}

  override def getSerializedClass: Class[_ <: Writable] = throwError

  override def getSerDeStats: SerDeStats = throwError

  override def serialize(p1: scala.Any, p2: ObjectInspector): Writable = throwError

  private def throwError =
    sys.error(
      "spark.sql.hive.convertMetastoreParquet must be set to true to use FakeParquetSerDe")
}
