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

package org.apache.spark.sql.execution.benchmark

import scala.concurrent.duration._

import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFPercentileApprox

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogFunction
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.aggregate.ApproximatePercentile
import org.apache.spark.sql.hive.HiveSessionCatalog
import org.apache.spark.sql.hive.execution.TestingTypedCount
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.LongType
import org.apache.spark.util.Benchmark

class ObjectHashAggregateExecBenchmark extends BenchmarkBase with TestHiveSingleton {
  ignore("Hive UDAF vs Spark AF") {
    val N = 2 << 15

    val benchmark = new Benchmark(
      name = "hive udaf vs spark af",
      valuesPerIteration = N,
      minNumIters = 5,
      warmupTime = 5.seconds,
      minTime = 10.seconds,
      outputPerIteration = true
    )

    registerHiveFunction("hive_percentile_approx", classOf[GenericUDAFPercentileApprox])

    sparkSession.range(N).createOrReplaceTempView("t")

    benchmark.addCase("hive udaf w/o group by") { _ =>
      sparkSession.conf.set(SQLConf.USE_OBJECT_HASH_AGG.key, "false")
      sparkSession.sql("SELECT hive_percentile_approx(id, 0.5) FROM t").collect()
    }

    benchmark.addCase("spark af w/o group by") { _ =>
      sparkSession.conf.set(SQLConf.USE_OBJECT_HASH_AGG.key, "true")
      sparkSession.sql("SELECT percentile_approx(id, 0.5) FROM t").collect()
    }

    benchmark.addCase("hive udaf w/ group by") { _ =>
      sparkSession.conf.set(SQLConf.USE_OBJECT_HASH_AGG.key, "false")
      sparkSession.sql(
        s"SELECT hive_percentile_approx(id, 0.5) FROM t GROUP BY CAST(id / ${N / 4} AS BIGINT)"
      ).collect()
    }

    benchmark.addCase("spark af w/ group by w/o fallback") { _ =>
      sparkSession.conf.set(SQLConf.USE_OBJECT_HASH_AGG.key, "true")
      sparkSession.sql(
        s"SELECT percentile_approx(id, 0.5) FROM t GROUP BY CAST(id / ${N / 4} AS BIGINT)"
      ).collect()
    }

    benchmark.addCase("spark af w/ group by w/ fallback") { _ =>
      sparkSession.conf.set(SQLConf.USE_OBJECT_HASH_AGG.key, "true")
      sparkSession.conf.set(SQLConf.OBJECT_AGG_SORT_BASED_FALLBACK_THRESHOLD.key, "2")
      sparkSession.sql(
        s"SELECT percentile_approx(id, 0.5) FROM t GROUP BY CAST(id / ${N / 4} AS BIGINT)"
      ).collect()
    }

    benchmark.run()

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_92-b14 on Mac OS X 10.10.5
    Intel(R) Core(TM) i7-4960HQ CPU @ 2.60GHz

    hive udaf vs spark af:                   Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    hive udaf w/o group by                        5326 / 5408          0.0       81264.2       1.0X
    spark af w/o group by                           93 /  111          0.7        1415.6      57.4X
    hive udaf w/ group by                         3804 / 3946          0.0       58050.1       1.4X
    spark af w/ group by w/o fallback               71 /   90          0.9        1085.7      74.8X
    spark af w/ group by w/ fallback                98 /  111          0.7        1501.6      54.1X
     */
  }

  ignore("ObjectHashAggregateExec vs SortAggregateExec - typed_count") {
    val N: Long = 1024 * 1024 * 100

    val benchmark = new Benchmark(
      name = "object agg v.s. sort agg",
      valuesPerIteration = N,
      minNumIters = 1,
      warmupTime = 10.seconds,
      minTime = 45.seconds,
      outputPerIteration = true
    )

    import sparkSession.implicits._

    def typed_count(column: Column): Column =
      Column(TestingTypedCount(column.expr).toAggregateExpression())

    val df = sparkSession.range(N)

    benchmark.addCase("sort agg w/ group by") { _ =>
      sparkSession.conf.set(SQLConf.USE_OBJECT_HASH_AGG.key, "false")
      df.groupBy($"id" < (N / 2)).agg(typed_count($"id")).collect()
    }

    benchmark.addCase("object agg w/ group by w/o fallback") { _ =>
      sparkSession.conf.set(SQLConf.USE_OBJECT_HASH_AGG.key, "true")
      df.groupBy($"id" < (N / 2)).agg(typed_count($"id")).collect()
    }

    benchmark.addCase("object agg w/ group by w/ fallback") { _ =>
      sparkSession.conf.set(SQLConf.USE_OBJECT_HASH_AGG.key, "true")
      sparkSession.conf.set(SQLConf.OBJECT_AGG_SORT_BASED_FALLBACK_THRESHOLD.key, "2")
      df.groupBy($"id" < (N / 2)).agg(typed_count($"id")).collect()
    }

    benchmark.addCase("sort agg w/o group by") { _ =>
      sparkSession.conf.set(SQLConf.USE_OBJECT_HASH_AGG.key, "false")
      df.select(typed_count($"id")).collect()
    }

    benchmark.addCase("object agg w/o group by w/o fallback") { _ =>
      sparkSession.conf.set(SQLConf.USE_OBJECT_HASH_AGG.key, "true")
      df.select(typed_count($"id")).collect()
    }

    benchmark.run()

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_92-b14 on Mac OS X 10.10.5
    Intel(R) Core(TM) i7-4960HQ CPU @ 2.60GHz

    object agg v.s. sort agg:                Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    sort agg w/ group by                        31251 / 31908          3.4         298.0       1.0X
    object agg w/ group by w/o fallback           6903 / 7141         15.2          65.8       4.5X
    object agg w/ group by w/ fallback          20945 / 21613          5.0         199.7       1.5X
    sort agg w/o group by                         4734 / 5463         22.1          45.2       6.6X
    object agg w/o group by w/o fallback          4310 / 4529         24.3          41.1       7.3X
     */
  }

  ignore("ObjectHashAggregateExec vs SortAggregateExec - percentile_approx") {
    val N = 2 << 20

    val benchmark = new Benchmark(
      name = "object agg v.s. sort agg",
      valuesPerIteration = N,
      minNumIters = 5,
      warmupTime = 15.seconds,
      minTime = 45.seconds,
      outputPerIteration = true
    )

    import sparkSession.implicits._

    val df = sparkSession.range(N).coalesce(1)

    benchmark.addCase("sort agg w/ group by") { _ =>
      sparkSession.conf.set(SQLConf.USE_OBJECT_HASH_AGG.key, "false")
      df.groupBy($"id" / (N / 4) cast LongType).agg(percentile_approx($"id", 0.5)).collect()
    }

    benchmark.addCase("object agg w/ group by w/o fallback") { _ =>
      sparkSession.conf.set(SQLConf.USE_OBJECT_HASH_AGG.key, "true")
      df.groupBy($"id" / (N / 4) cast LongType).agg(percentile_approx($"id", 0.5)).collect()
    }

    benchmark.addCase("object agg w/ group by w/ fallback") { _ =>
      sparkSession.conf.set(SQLConf.USE_OBJECT_HASH_AGG.key, "true")
      sparkSession.conf.set(SQLConf.OBJECT_AGG_SORT_BASED_FALLBACK_THRESHOLD.key, "2")
      df.groupBy($"id" / (N / 4) cast LongType).agg(percentile_approx($"id", 0.5)).collect()
    }

    benchmark.addCase("sort agg w/o group by") { _ =>
      sparkSession.conf.set(SQLConf.USE_OBJECT_HASH_AGG.key, "false")
      df.select(percentile_approx($"id", 0.5)).collect()
    }

    benchmark.addCase("object agg w/o group by w/o fallback") { _ =>
      sparkSession.conf.set(SQLConf.USE_OBJECT_HASH_AGG.key, "true")
      df.select(percentile_approx($"id", 0.5)).collect()
    }

    benchmark.run()

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_92-b14 on Mac OS X 10.10.5
    Intel(R) Core(TM) i7-4960HQ CPU @ 2.60GHz

    object agg v.s. sort agg:                Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    sort agg w/ group by                          3418 / 3530          0.6        1630.0       1.0X
    object agg w/ group by w/o fallback           3210 / 3314          0.7        1530.7       1.1X
    object agg w/ group by w/ fallback            3419 / 3511          0.6        1630.1       1.0X
    sort agg w/o group by                         4336 / 4499          0.5        2067.3       0.8X
    object agg w/o group by w/o fallback          4271 / 4372          0.5        2036.7       0.8X
     */
  }

  private def registerHiveFunction(functionName: String, clazz: Class[_]): Unit = {
    val sessionCatalog = sparkSession.sessionState.catalog.asInstanceOf[HiveSessionCatalog]
    val functionIdentifier = FunctionIdentifier(functionName, database = None)
    val func = CatalogFunction(functionIdentifier, clazz.getName, resources = Nil)
    sessionCatalog.registerFunction(func, ignoreIfExists = false)
  }

  private def percentile_approx(
      column: Column, percentage: Double, isDistinct: Boolean = false): Column = {
    val approxPercentile = new ApproximatePercentile(column.expr, Literal(percentage))
    Column(approxPercentile.toAggregateExpression(isDistinct))
  }
}
