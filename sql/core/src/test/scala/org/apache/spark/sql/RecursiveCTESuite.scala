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

package org.apache.spark.sql

import org.apache.spark.sql.test.SharedSparkSession

import scala.util.Random

import java.time.LocalDateTime

class RecursiveCTESuite extends QueryTest with SharedSparkSession {


  test("Recursive CTE generate first n integers") {
    spark.sql("""WITH RECURSIVE fibonacci AS (
                |  VALUES (0, 1) AS t(a, b)
                |  UNION ALL
                |  SELECT b, a + b FROM fibonacci WHERE a < 14
                |)
                |SELECT a FROM fibonacci ORDER BY a;""".stripMargin).show()
  }

  test("Recursive CTE generate first n in cycle") {
    val n = 110
    Random.setSeed(12052002)

    spark.sql("""CREATE TABLE IF NOT EXISTS caci (
                 id int,
                 bagafana int
                 );""")

    val id: Seq[Int] = Seq.range(1, n+1)
    val s1 = Random.shuffle(id)
    val s2 = Random.shuffle(id)
    for (i <- 0 until n) {
      val r1 = i
      val r2 = i+1
      Console.println(s"""INSERT INTO caci VALUES ($r1, $r2);""")
      spark.sql(s"""INSERT INTO caci VALUES ($r1, $r2)""")
    }

    Console.println(LocalDateTime.now())
    spark.sql("""WITH RECURSIVE t1(n) AS (
                |  SELECT 1
                |  UNION ALL
                |  SELECT bagafana FROM t1 JOIN caci ON id = n)
                |SELECT * FROM t1""".stripMargin).count()
    Console.println(LocalDateTime.now())

  }


}
