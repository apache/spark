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

import org.apache.spark.sql.catalyst.plans.logical.OneRowRelation
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SQLTestUtils

class LogicalPlanSQLGenerationSuite extends SQLGenerationTest with SQLTestUtils {
  import hiveContext.implicits._

  protected override def beforeAll(): Unit = {
    super.beforeAll()

    sqlContext.range(10).select('id alias "a").registerTempTable("t0")
    sqlContext.range(10).select('id alias "b").registerTempTable("t1")
  }

  protected override def afterAll(): Unit = {
    sqlContext.dropTempTable("t0")
    sqlContext.dropTempTable("t1")

    super.afterAll()
  }

  test("single row project") {
    checkSQL(OneRowRelation.select(lit(1)), "SELECT 1 AS `1`")
    checkSQL(OneRowRelation.select(lit(1) as 'a), "SELECT 1 AS `a`")
  }

  test("project with limit") {
    checkSQL(OneRowRelation.select(lit(1)).limit(1), "SELECT 1 AS `1` LIMIT 1")
    checkSQL(OneRowRelation.select(lit(1) as 'a).limit(1), "SELECT 1 AS `a` LIMIT 1")
  }

  test("table lookup") {
    checkSQL(sqlContext.table("t0"), "SELECT `t0`.`a` FROM `t0`")
    checkSQL(sqlContext.table("t1").select('b alias "c"), "SELECT `t1`.`id` AS `c` FROM `t1`")
  }
}
