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

package org.apache.spark.sql.execution.subquery

import org.apache.spark.sql.{Dataset, QueryTest}
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext

class SubqueryDedupSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  test("Create common subquery") {
    withSQLConf(SQLConf.CROSS_JOINS_ENABLED.key -> "true") {
      val subqueryDedup = new SubqueryDedup()

      spark.range(10).createOrReplaceTempView("t1")
      val df = sql("SELECT 1 FROM t1")
      val project = df.queryExecution.analyzed.collect {
        case p: Project => p
      }.head

      val commonSubqueryAlias = subqueryDedup.createCommonSubquery(spark, project)
      assert(commonSubqueryAlias.output === project.output)
      assert(Dataset.ofRows(spark, commonSubqueryAlias).collect() === df.collect())

      spark.range(10).createOrReplaceTempView("t2")

      val df2 = sql("SELECT * FROM t1 join t2")
      val project2 = df2.queryExecution.analyzed.collect {
        case p: Project => p
      }.head

      val commonSubqueryAlias2 = subqueryDedup.createCommonSubquery(spark, project2)
      assert(commonSubqueryAlias2.output === project2.output)
      assert(Dataset.ofRows(spark, commonSubqueryAlias2).collect() === df2.collect())
    }
  }
}
