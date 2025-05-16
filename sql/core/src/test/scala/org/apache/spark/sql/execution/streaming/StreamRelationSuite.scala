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

package org.apache.spark.sql.execution.streaming

import org.apache.spark.sql.test.SharedSparkSession

class StreamRelationSuite extends SharedSparkSession {
  test("STREAM with options is correctly propagated to datasource in V1") {
    sql("CREATE TABLE t AS SELECT 1")

    val analyzedPlan = sql("SELECT * FROM STREAM t WITH ('readOptionKey'='readOptionValue')")
      .queryExecution.analyzed
    assert(analyzedPlan.isStreaming)

    val areOptionsPropagatedToDatasource = analyzedPlan.exists {
      case StreamingRelation(datasource, _, _)
        if datasource.catalogTable.map(_.identifier.table).contains("t") =>
          datasource.options.get("readOptionKey").contains("readOptionValue")
      case _ =>
        false
    }
    assert(areOptionsPropagatedToDatasource)
  }
}
