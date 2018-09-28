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

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.execution.SparkSqlParserSuite
/**
 * Used to check stream query
 */
class StreamingQuerySuite extends SparkSqlParserSuite {

  import org.apache.spark.sql.catalyst.dsl.expressions._
  import org.apache.spark.sql.catalyst.dsl.plans._

  private lazy val parser = new SparkSqlParser(newConf)

  // change table to UnresolvedStreamRelation
  def streamTable(ref: String): LogicalPlan = UnresolvedStreamRelation(TableIdentifier(ref))

  private def assertEqual(sqlCommand: String, plan: LogicalPlan): Unit = {
    val normalized1 = normalizePlan(parser.parsePlan(sqlCommand))
    val normalized2 = normalizePlan(plan)
    comparePlans(normalized1, normalized2)
  }

  test("simple stream query") {
    assertEqual("select stream * from b",
      WithStreamDefinition(streamTable("b").select(star())))
  }

  test("simple stream join batch") {
    // stream join batch
    assertEqual("select stream * from b join s",
      WithStreamDefinition(
        streamTable("b").join(streamTable("s"), Inner, None).select(star())
      )
    )
  }

  test("simple subquery(stream) join batch") {
    // subquery(stream) join batch
    assertEqual("select stream * from (select * from b) as c join s",
      WithStreamDefinition(
        WithStreamDefinition(streamTable("b").select(star())).as("c")
          .join(streamTable("s"), Inner, None).select(star())
      )
    )
  }

  test("simple stream join subquery(batch)") {
    // stream join subquery(batch)
    assertEqual("select stream * from s join (select * from b) as c",
      WithStreamDefinition(
        streamTable("s").join(
          WithStreamDefinition(streamTable("b").select(star())).as("c"),
          Inner,
          None).select(star())
      )
    )
  }

  test("simple subquery(stream) join subquery(batch)") {
    // subquery(stream) join subquery(batch)
    assertEqual("select stream * from (select * from s) as t join (select * from b) as c",
      WithStreamDefinition(
        WithStreamDefinition(streamTable("s").select(star())).as("t")
          .join(WithStreamDefinition(streamTable("b").select(star())).as("c"), Inner, None)
          .select(star())
      )
    )
  }
}
