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

package org.apache.spark.sql.execution.datasources.json

import org.apache.spark.sql.FileDataSourceSuiteBase

class JsonDataSourceSuite extends FileDataSourceSuiteBase {

  override protected def format: String = "json"

  override def excluded: Seq[String] =
    Seq("SPARK-15474: Write and read back non-empty schema with empty dataframe",
      "Spark native readers should respect spark.sql.caseSensitive",
      "SPARK-24204: error handling for unsupported Null data types",
      "SPARK-31116: Select nested schema with case insensitive mode",
      "test casts pushdown on orc/parquet for integral types")
}
