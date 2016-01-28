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

import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.sql.SQLContext
import org.apache.spark.util.Benchmark

/**
  * Benchmark to measure whole stage codegen performance.
  * To run this:
  *  build/sbt "sql/test-only *BenchmarkWholeStageCodegen"
  */
class BenchmarkWholeStageCodegen extends SparkFunSuite {
  lazy val conf = new SparkConf().setMaster("local[1]").setAppName("benchmark")
  lazy val sc = SparkContext.getOrCreate(conf)
  lazy val sqlContext = SQLContext.getOrCreate(sc)

  def testWholeStage(values: Int): Unit = {

    val benchmark = new Benchmark("Single Int Column Scan", values)

    benchmark.addCase("Without whole stage codegen") { iter =>
      sqlContext.setConf("spark.sql.codegen.wholeStage", "false")
      sqlContext.range(values).filter("(id & 1) = 1").count()
    }

    benchmark.addCase("With whole stage codegen") { iter =>
      sqlContext.setConf("spark.sql.codegen.wholeStage", "true")
      sqlContext.range(values).filter("(id & 1) = 1").count()
    }

    /*
      Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
      Single Int Column Scan:            Avg Time(ms)    Avg Rate(M/s)  Relative Rate
      -------------------------------------------------------------------------------
      Without whole stage codegen             7775.53            26.97         1.00 X
      With whole stage codegen                 342.15           612.94        22.73 X
    */
    benchmark.run()
  }

  def testStddev(values: Int): Unit = {

    val benchmark = new Benchmark("stddev", values)

    benchmark.addCase("stddev w/o codegen") { iter =>
      sqlContext.setConf("spark.sql.codegen.wholeStage", "false")
      sqlContext.range(values).groupBy().agg("id" -> "stddev").collect()
    }

    benchmark.addCase("stddev w codegen") { iter =>
      sqlContext.setConf("spark.sql.codegen.wholeStage", "true")
      sqlContext.range(values).groupBy().agg("id" -> "stddev").collect()
    }

    benchmark.addCase("kurtosis w/o codegen") { iter =>
      sqlContext.setConf("spark.sql.codegen.wholeStage", "false")
      sqlContext.range(values).groupBy().agg("id" -> "kurtosis").collect()
    }

    benchmark.addCase("kurtosis w codegen") { iter =>
      sqlContext.setConf("spark.sql.codegen.wholeStage", "true")
      sqlContext.range(values).groupBy().agg("id" -> "kurtosis").collect()
    }


    /**
    Using ImperativeAggregate:

      Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
      stddev:                            Avg Time(ms)    Avg Rate(M/s)  Relative Rate
      -------------------------------------------------------------------------------
      stddev w/o codegen                      2019.04            10.39         1.00 X
      stddev w codegen                        2097.29            10.00         0.96 X
      kurtosis w/o codegen                    2108.99             9.94         0.96 X
      kurtosis w codegen                      2090.69            10.03         0.97 X

      Using DeclarativeAggregate:

      Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
      stddev:                            Avg Time(ms)    Avg Rate(M/s)  Relative Rate
      -------------------------------------------------------------------------------
      stddev w/o codegen                      1499.08            13.99         1.00 X
      stddev w codegen                         360.40            58.19         4.16 X
      kurtosis w/o codegen                    2941.94             7.13         0.51 X
      kurtosis w codegen                       432.04            48.54         3.47 X
      */
    benchmark.run()
  }

  test("benchmark") {
    // testWholeStage(200 << 20)
    // testStddev(20 << 20)
  }
}
