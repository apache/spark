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

import org.scalatest.FunSuite
import org.scalatest.Matchers._

import org.apache.spark.sql.test.TestSQLContext
import org.apache.spark.sql.test.TestSQLContext.implicits._

class DataFrameStatSuite extends FunSuite  {

  val sqlCtx = TestSQLContext

  test("Frequent Items") {
    def toLetter(i: Int): String = (i + 96).toChar.toString
    val rows = Array.tabulate(1000) { i =>
      if (i % 3 == 0) (1, toLetter(1), -1.0) else (i, toLetter(i), i * -1.0)
    }
    val df = sqlCtx.sparkContext.parallelize(rows).toDF("numbers", "letters", "negDoubles")

    val results = df.stat.freqItems(Array("numbers", "letters"), 0.1)
    val items = results.collect().head
    items.getSeq[Int](0) should contain (1)
    items.getSeq[String](1) should contain (toLetter(1))

    val singleColResults = df.stat.freqItems(Array("negDoubles"), 0.1)
    val items2 = singleColResults.collect().head
    items2.getSeq[Double](0) should contain (-1.0)

  }
}
