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

class TimeTypeAnalyticsSuite extends QueryTest with SharedSparkSession {

  test("histogram_numeric with TIME type") {
    withTable("t") {
      sql("CREATE TABLE t (time_col TIME) USING parquet")
      sql("""INSERT INTO t VALUES
        (TIME '01:00:00'), (TIME '02:00:00'), (TIME '03:00:00'), (TIME '04:00:00')""")
      val result = sql("SELECT histogram_numeric(time_col, 2) FROM t").first().getSeq[Row](0)
      assert(result.length == 2)
    }
  }

  test("percentile_approx with TIME type") {
    withTable("t") {
      sql("CREATE TABLE t (time_col TIME) USING parquet")
      sql("""INSERT INTO t VALUES
        (TIME '01:00:00'), (TIME '02:00:00'), (TIME '03:00:00'), (TIME '04:00:00')""")
      val result = sql("SELECT percentile_approx(time_col, 0.5) FROM t").first().get(0)
      // The median should be around 02:30:00 (9000 seconds)
      val expected = java.time.LocalTime.of(2, 30, 0)
      val actual = result.asInstanceOf[java.time.LocalTime]
      // Allow for some approximation error (with 4 values, it might pick one of the values)
      assert(java.time.Duration.between(expected, actual).getSeconds.abs <= 1800)
    }
  }

  test("histogram_numeric with TIME type - negative tests") {
    withTable("t") {
      sql("CREATE TABLE t (time_col TIME) USING parquet")
      sql("INSERT INTO t VALUES (TIME '01:00:00')")
      // Negative number of bins
      val e1 = intercept[AnalysisException] {
        sql("SELECT histogram_numeric(time_col, -1) FROM t")
      }
      assert(e1.getMessage.contains("The nb must be between [2, 2147483647]"))

      // Non-integer number of bins
      val e2 = intercept[AnalysisException] {
        sql("SELECT histogram_numeric(time_col, 2.5) FROM t")
      }
      assert(e2.getMessage.contains(
        "requires the \"INT\" type, however \"2.5\" has the type \"DECIMAL(2,1)\""))
    }
  }

  test("percentile_approx with TIME type - negative tests") {
    withTable("t") {
      sql("CREATE TABLE t (time_col TIME) USING parquet")
      sql("INSERT INTO t VALUES (TIME '01:00:00')")
      // Percentile not in [0, 1]
      val e1 = intercept[AnalysisException] {
        sql("SELECT percentile_approx(time_col, 1.1) FROM t")
      }
      assert(e1.getMessage.contains("The percentage must be between [0.0, 1.0]"))

      val e2 = intercept[AnalysisException] {
        sql("SELECT percentile_approx(time_col, -0.1) FROM t")
      }
      assert(e2.getMessage.contains("The percentage must be between [0.0, 1.0]"))
    }
  }

  test("histogram_numeric and percentile_approx on empty table") {
    withTable("t") {
      sql("CREATE TABLE t (time_col TIME) USING parquet")
      checkAnswer(sql("SELECT histogram_numeric(time_col, 2) FROM t"), Row(null))
      checkAnswer(sql("SELECT percentile_approx(time_col, 0.5) FROM t"), Row(null))
    }
  }
}
