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
package org.apache.spark.sql.hive.execution

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedStreamRelation
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoTable, WithStreamDefinition}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.internal.SQLConf

class ValidSQLStreamingSuite extends ResolveStreamRelationSuite {

  import org.apache.spark.sql.catalyst.dsl.expressions._
  import org.apache.spark.sql.catalyst.dsl.plans._

  private val consoleSinkOptions = Map(
    "numRows" -> sqlConf.sqlStreamConsoleOutputRows,
    "source" -> "console"
  )

  test("select stream * from csvTable with enable=false") {
    sqlConf.setConf(SQLConf.SQLSTREAM_QUERY_ENABLE, false)
    assertAnalysisError(
      WithStreamDefinition(getStreamRelation("csv", csvOptions).select(star())),
      "Disable SQLStreaming for default." :: Nil
    )
  }

  test("select stream * from csvTable with enable=true") {
    sqlConf.setConf(SQLConf.SQLSTREAM_QUERY_ENABLE, true)
    assertAnalysisSuccess(
      WithStreamDefinition(
        UnresolvedStreamRelation(TableIdentifier("csvTable"))
      )
    )
  }

  test("insert into csvTable select stream * from parquetTable") {
    sqlConf.setConf(SQLConf.SQLSTREAM_QUERY_ENABLE, true)
    assertAnalysisSuccess(
      InsertIntoTable(
        LogicalRelation(
          null,
          Seq(),
          Some(getCatalogTable("csvTable", "csv", csvOptions)),
          isStreaming = false
        ),
        Map(),
        WithStreamDefinition(
          UnresolvedStreamRelation(TableIdentifier("parquetTable"))
        ),
        overwrite = false,
        ifPartitionNotExists = false
      )
    )
  }

  test("insert into csvTable select stream * from parquetTable with enable=true") {
    sqlConf.setConf(SQLConf.SQLSTREAM_QUERY_ENABLE, true)
    assertAnalysisError(
      InsertIntoTable(
        LogicalRelation(
          null,
          Seq(),
          Some(getCatalogTable("csvTable", "csv", csvOptions.filterNot(_._1 == "isStreaming"))),
          isStreaming = false
        ),
        Map(),
        WithStreamDefinition(
          UnresolvedStreamRelation(TableIdentifier("parquetTable"))
        ),
        overwrite = false,
        ifPartitionNotExists = false
      ),
      "Not supported to insert write into none stream table" :: Nil
    )
  }
}
