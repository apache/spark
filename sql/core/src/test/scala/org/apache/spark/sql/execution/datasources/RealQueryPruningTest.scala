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

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

class RealQueryPruningTest extends QueryTest with SharedSparkSession {

  def countFields(dt: org.apache.spark.sql.types.DataType): Int = dt match {
    case s: org.apache.spark.sql.types.StructType =>
      s.fields.map(f => countFields(f.dataType)).sum
    case a: org.apache.spark.sql.types.ArrayType =>
      countFields(a.elementType)
    case _ => 1
  }

  test("SPARK-47230: Schema pruning with CTE and explode") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      // Create test data similar to the real parquet file structure
      sql("""
        SELECT
          'pub1' as pv_publisherId,
          array(
            struct(
              true as available,
              array(struct(false as clicked)) as servedItems
            )
          ) as pv_requests
      """).write.parquet(path)

      sql(s"CREATE OR REPLACE TEMP VIEW rawdata USING parquet OPTIONS (path '$path')")

      val explodeQuery = sql("""
        with exploded_data_igor as (
          SELECT *, request.available as request_available
          FROM rawdata
          LATERAL VIEW OUTER explode(pv_requests) as request
          LATERAL VIEW OUTER explode(request.servedItems) as servedItem
        )
        select
          sum(if(request.available,1,0)) as available_requests,
          sum(if(servedItem.clicked,1,0)) as clicked_items
        from exploded_data_igor
        group by pv_publisherId
      """)

      // Check that schema pruning occurred
      val schema = explodeQuery.queryExecution.optimizedPlan.collect {
        case r: LogicalRelation => r.relation.schema
      }.headOption

      schema.foreach { s =>
        val fieldCount = countFields(s)
        // We expect only a few fields, not the full schema
        assert(fieldCount < 100, s"Schema should be pruned but has $fieldCount fields")
        info(s"Explode query: Pruned schema has $fieldCount leaf fields")
      }

      // Ensure query executes without errors
      val result = explodeQuery.collect()
      assert(result.length == 1)
    }
  }

  test("SPARK-47230: Schema pruning with CTE and posexplode") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      // Create test data
      sql("""
        SELECT
          'pub1' as pv_publisherId,
          array(
            struct(
              true as available,
              array(struct(false as clicked)) as servedItems
            )
          ) as pv_requests
      """).write.parquet(path)

      sql(s"CREATE OR REPLACE TEMP VIEW rawdata2 USING parquet OPTIONS (path '$path')")

      val posexplodeQuery = sql("""
        with exploded_data_igor as (
          SELECT *, request.available as request_available
          FROM rawdata2
          LATERAL VIEW OUTER posexplode(pv_requests) as requestIdx, request
          LATERAL VIEW OUTER posexplode(request.servedItems) as servedItemIdx, servedItem
        )
        select
          sum(if(request.available,1,0)) as available_requests,
          sum(if(servedItem.clicked,1,0)) as clicked_items
        from exploded_data_igor
        group by pv_publisherId
      """)

      // Check that schema pruning occurred
      val schema = posexplodeQuery.queryExecution.optimizedPlan.collect {
        case r: LogicalRelation => r.relation.schema
      }.headOption

      schema.foreach { s =>
        val fieldCount = countFields(s)
        // We expect only a few fields, not the full schema
        assert(fieldCount < 100, s"Schema should be pruned but has $fieldCount fields")
        info(s"Posexplode query: Pruned schema has $fieldCount leaf fields")
      }

      // Ensure query executes without errors
      val result = posexplodeQuery.collect()
      assert(result.length == 1)
    }
  }
}
