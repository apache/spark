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
package org.apache.spark.sql.hive

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.SQLTestUtils

class StreamTableDDLCommandSuite extends SQLTestUtils with TestHiveSingleton {
  private val catalog = spark.sessionState.catalog

  test("CTAS: create data source stream table") {
    withTempPath { dir =>
      withTable("t") {
        sql(
          s"""CREATE TABLE t USING PARQUET
             |OPTIONS (
             |PATH = '${dir.toURI}',
        |location = '${dir.toURI}',
        |isStreaming = 'true')
        |AS SELECT 1 AS a, 2 AS b, 3 AS c
        """.stripMargin
        )
        val streamTable = catalog.getTableMetadata(TableIdentifier("t"))
        assert(streamTable.isStreaming)
      }
    }
  }
}
