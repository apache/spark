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

package org.apache.spark.sql.connect.client

import java.util.Random

import org.apache.spark.sql.connect.client.util.RemoteSparkSession

class DataFrameStatSuite extends RemoteSparkSession {

  test("approximate quantile") {
    val session = spark
    import session.implicits._

    val n = 1000
    val df = Seq.tabulate(n + 1)(i => (i, 2.0 * i)).toDF("singles", "doubles")

    val q1 = 0.5
    val q2 = 0.8
    val epsilons = List(0.1, 0.05, 0.001)

    for (epsilon <- epsilons) {
      val Array(single1) = df.stat.approxQuantile("singles", Array(q1), epsilon)
      val Array(double2) = df.stat.approxQuantile("doubles", Array(q2), epsilon)
      // Also make sure there is no regression by computing multiple quantiles at once.
      val Array(d1, d2) = df.stat.approxQuantile("doubles", Array(q1, q2), epsilon)
      val Array(s1, s2) = df.stat.approxQuantile("singles", Array(q1, q2), epsilon)

      val errorSingle = 1000 * epsilon
      val errorDouble = 2.0 * errorSingle

      assert(math.abs(single1 - q1 * n) <= errorSingle)
      assert(math.abs(double2 - 2 * q2 * n) <= errorDouble)
      assert(math.abs(s1 - q1 * n) <= errorSingle)
      assert(math.abs(s2 - q2 * n) <= errorSingle)
      assert(math.abs(d1 - 2 * q1 * n) <= errorDouble)
      assert(math.abs(d2 - 2 * q2 * n) <= errorDouble)

      // Multiple columns
      val Array(Array(ms1, ms2), Array(md1, md2)) =
        df.stat.approxQuantile(Array("singles", "doubles"), Array(q1, q2), epsilon)

      assert(math.abs(ms1 - q1 * n) <= errorSingle)
      assert(math.abs(ms2 - q2 * n) <= errorSingle)
      assert(math.abs(md1 - 2 * q1 * n) <= errorDouble)
      assert(math.abs(md2 - 2 * q2 * n) <= errorDouble)
    }

    // quantile should be in the range [0.0, 1.0]
    val e = intercept[IllegalArgumentException] {
      df.stat.approxQuantile(Array("singles", "doubles"), Array(q1, q2, -0.1), epsilons.head)
    }
    assert(e.getMessage.contains("percentile should be in the range [0.0, 1.0]"))

    // relativeError should be non-negative
    val e2 = intercept[IllegalArgumentException] {
      df.stat.approxQuantile(Array("singles", "doubles"), Array(q1, q2), -1.0)
    }
    assert(e2.getMessage.contains("Relative Error must be non-negative"))
  }

  test("crosstab") {
    val session = spark
    import session.implicits._
    val rng = new Random()
    val data = Seq.tabulate(25)(_ => (rng.nextInt(5), rng.nextInt(10)))
    val df = data.toDF("a", "b")
    val crosstab = df.stat.crosstab("a", "b")
    val columnNames = crosstab.schema.fieldNames
    assert(columnNames(0) === "a_b")
    // reduce by key
    val expected = data.map(t => (t, 1)).groupBy(_._1).mapValues(_.length)
    val rows = crosstab.collect()
    rows.foreach { row =>
      val i = row.getString(0).toInt
      for (col <- 1 until columnNames.length) {
        val j = columnNames(col).toInt
        assert(row.getLong(col) === expected.getOrElse((i, j), 0).toLong)
      }
    }
  }

}
