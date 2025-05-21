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

package org.apache.spark.sql.execution

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions.countDistinct
import org.apache.spark.sql.test.SharedSparkSession

class RecursiveCTESuite extends QueryTest with SharedSparkSession {

  test("Random rCTEs produce different results in different iterations - RAND") {
    val df = sql("""WITH RECURSIVE randoms(val) AS (
                   |    SELECT CAST(floor(rand() * 50 + 1) AS INT)
                   |    UNION ALL
                   |    SELECT CAST(floor(rand() * 50 + 1) AS INT)
                   |    FROM randoms
                   |)
                   |SELECT val FROM randoms LIMIT 10;""".stripMargin)

    val distinctCount = df.select(countDistinct("val")).collect()(0).getLong(0)
    assert(distinctCount > 2)
  }

  test("Random rCTEs produce different results in different iterations - UNIFORM") {
    val df = sql("""WITH RECURSIVE randoms(val) AS (
                   |    SELECT CAST(UNIFORM(1,51) AS INT)
                   |    UNION ALL
                   |    SELECT CAST(UNIFORM(1,51) AS INT)
                   |    FROM randoms
                   |)
                   |SELECT val FROM randoms LIMIT 10;""".stripMargin)

    val distinctCount = df.select(countDistinct("val")).collect()(0).getLong(0)
    assert(distinctCount > 2)
  }

  test("Random rCTEs produce different results in different iterations - RANDN") {
    val df = sql("""WITH RECURSIVE randoms(val) AS (
                   |    SELECT CAST(floor(randn() * 50) AS INT)
                   |    UNION ALL
                   |    SELECT CAST(floor(randn() * 50) AS INT)
                   |    FROM randoms
                   |)
                   |SELECT val FROM randoms LIMIT 10;""".stripMargin)

    val distinctCount = df.select(countDistinct("val")).collect()(0).getLong(0)
    assert(distinctCount > 2)
  }

  test("Random rCTEs produce different results in different iterations - RANDSTR") {
    val df = sql("""WITH RECURSIVE randoms(val) AS (
                   |    SELECT randstr(10)
                   |    UNION ALL
                   |    SELECT randstr(10)
                   |    FROM randoms
                   |)
                   |SELECT val FROM randoms LIMIT 10;""".stripMargin)

    val distinctCount = df.select(countDistinct("val")).collect()(0).getLong(0)
    assert(distinctCount > 2)
  }

  test("Random rCTEs produce different results in different iterations - UUID") {
    val df = sql("""WITH RECURSIVE randoms(val) AS (
                   |    SELECT UUID()
                   |    UNION ALL
                   |    SELECT UUID()
                   |    FROM randoms
                   |)
                   |SELECT val FROM randoms LIMIT 10;""".stripMargin)

    val distinctCount = df.select(countDistinct("val")).collect()(0).getLong(0)
    assert(distinctCount > 2)
  }

  test("Random rCTEs produce different results in different iterations - SHUFFLE") {
    val df = sql("""WITH RECURSIVE randoms(val) AS (
                   |    SELECT ARRAY(1,2,3,4,5)
                   |    UNION ALL
                   |    SELECT SHUFFLE(ARRAY(1,2,3,4,5))
                   |    FROM randoms
                   |  )
                   |SELECT val FROM randoms LIMIT 10;""".stripMargin)

    val distinctCount = df.select(countDistinct("val")).collect()(0).getLong(0)
    assert(distinctCount > 2)
  }
}
