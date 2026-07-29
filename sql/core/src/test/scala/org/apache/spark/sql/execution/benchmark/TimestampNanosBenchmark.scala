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

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils.{withDefaultTimeZone, LA}
import org.apache.spark.sql.internal.SQLConf

/**
 * Synthetic benchmark comparing the microsecond timestamp path against the nanosecond-precision
 * timestamp types `TIMESTAMP_NTZ(p)` / `TIMESTAMP_LTZ(p)`, `p in [7, 9]` (umbrella SPARK-56822).
 *
 * Most groups pair the same operation over a microsecond timestamp and the nanosecond type, so the
 * per-row delta of the nanos path (a 16-byte `TimestampNanosVal` = epochMicros:Long +
 * nanosWithinMicro:Short carrier, vs a plain 8-byte Long) is read directly off adjacent rows. A few
 * groups pair related operations instead: Cast includes the cross-casts (micros <-> nanos(9)) and a
 * nanos-only narrowing (nanos(9) -> nanos(7)), and Datetime functions pairs `unix_micros` with the
 * nanos-only `unix_nanos`.
 * The areas where nanos is expected to diverge from micros:
 *   - hashing: two hash ops (hashInt(nanos), hashLong(micros)) vs one;
 *   - ordering/sort: nanos has NO radix sort prefix, so it falls back to a full `compareTo` per
 *     comparison (micros sort on the primitive Long prefix);
 *   - Parquet read: nanos read is row-based only (vectorized read is disabled for the 16-byte
 *     two-field carrier), whereas micros read is vectorized;
 *   - ORC read: nanos reads vectorized over the full year range, like micros.
 *
 * Parsing and cast-to-string are additionally spot-checked at p=7 (alongside p=9): the 16-byte
 * carrier is identical for all p, so only fractional-digit truncation/formatting differs.
 *
 * The nanosecond timestamp types are behind the preview flag
 * `spark.sql.timestampNanosTypes.enabled` (default = `isTesting`). `BenchmarkBase.main` sets
 * `IS_TESTING=true`, so the flag is on during a benchmark run; this suite also sets it explicitly.
 *
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class>
 *        --jars <spark core test jar>,<spark catalyst test jar> <sql core test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/TimestampNanosBenchmark-results.txt".
 * }}}
 */
object TimestampNanosBenchmark extends SqlBasedBenchmark {

  private val N = 10000000

  // ---------------------------------------------------------------------------
  // Value generators (SQL expressions over `id` from spark.range).
  //
  // Nanos values are built as `id` seconds + (id % 1000) nanoseconds-of-microsecond, so both the
  // micros field and the sub-microsecond field vary across rows. The whole range stays in ~1970,
  // inside the Parquet INT64-NANOS window (1677-09-21 .. 2262-04-11) so Parquet writes never throw
  // DATETIME_OVERFLOW.
  // ---------------------------------------------------------------------------
  private val microLtz = "timestamp_seconds(id)"
  private val microNtz = "cast(timestamp_seconds(id) as timestamp_ntz)"
  private val nanosArg = "id * 1000000000 + id % 1000"
  private val nanosLtz = s"timestamp_nanos($nanosArg)"
  private val nanosNtz = s"cast(timestamp_nanos($nanosArg) as timestamp_ntz(9))"
  private val nanosNtz7 = s"cast(timestamp_nanos($nanosArg) as timestamp_ntz(7))"

  // 9-, 7- and 6-digit fractional-second strings for the parsing group.
  private val nanosStr =
    "concat('2019-01-27 11:02:01.', lpad(cast(id % 1000000000 as string), 9, '0'))"
  private val nanosStr7 =
    "concat('2019-01-27 11:02:01.', lpad(cast(id % 10000000 as string), 7, '0'))"
  private val microStr =
    "concat('2019-01-27 11:02:01.', lpad(cast(id % 1000000 as string), 6, '0'))"

  private def doBenchmark(cardinality: Int, exprs: String*): Unit = {
    spark.range(cardinality).selectExpr(exprs: _*).noop()
  }

  /** A wholestage on/off codegen benchmark that just projects `exprs` over `cardinality` rows. */
  private def runProject(cardinality: Int, name: String, exprs: String*): Unit = {
    codegenBenchmark(name, cardinality) {
      doBenchmark(cardinality, exprs: _*)
    }
  }

  /** A codegen benchmark that builds a single `ts` column then runs `op(ds)`. */
  private def runOnColumn(
      cardinality: Int, name: String, tsExpr: String)(op: org.apache.spark.sql.DataFrame => Unit)
    : Unit = {
    codegenBenchmark(name, cardinality) {
      op(spark.range(cardinality).selectExpr(s"$tsExpr as ts"))
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    withDefaultTimeZone(LA) {
      withSQLConf(
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> LA.getId,
        SQLConf.TIMESTAMP_NANOS_TYPES_ENABLED.key -> "true") {

        runBenchmark("Cast") {
          runProject(N, "micros -> string", s"cast($microNtz as string)")
          runProject(N, "nanos(9) -> string", s"cast($nanosNtz as string)")
          runProject(N, "nanos(7) -> string", s"cast($nanosNtz7 as string)")
          runProject(N, "micros -> nanos(9)", s"cast($microNtz as timestamp_ntz(9))")
          runProject(N, "nanos(9) -> micros", s"cast($nanosNtz as timestamp_ntz)")
          runProject(N, "nanos(9) -> nanos(7)", s"cast($nanosNtz as timestamp_ntz(7))")
          runProject(N, "micros -> date", s"cast($microNtz as date)")
          runProject(N, "nanos(9) -> date", s"cast($nanosNtz as date)")
        }

        runBenchmark("Parse from string") {
          val n = 1000000
          runProject(n, "string(6 digits) -> micros", s"cast($microStr as timestamp_ntz)")
          runProject(n, "string(9 digits) -> nanos(9)", s"cast($nanosStr as timestamp_ntz(9))")
          runProject(n, "string(7 digits) -> nanos(7)", s"cast($nanosStr7 as timestamp_ntz(7))")
        }

        runBenchmark("Hashing") {
          runProject(N, "murmur3 hash(micros)", s"hash($microNtz)")
          runProject(N, "murmur3 hash(nanos(9))", s"hash($nanosNtz)")
          runProject(N, "xxhash64(micros)", s"xxhash64($microNtz)")
          runProject(N, "xxhash64(nanos(9))", s"xxhash64($nanosNtz)")
        }

        runBenchmark("Sort (global ORDER BY)") {
          runOnColumn(N, "order by micros", microNtz)(_.orderBy("ts").noop())
          runOnColumn(N, "order by nanos(9)", nanosNtz)(_.orderBy("ts").noop())
        }

        runBenchmark("Group-by aggregation (~1M distinct keys)") {
          val microKey = "timestamp_seconds(id % 1000000)"
          val nanosKey = "timestamp_nanos((id % 1000000) * 1000000000 + (id % 1000000) % 1000)"
          runOnColumn(N, "group by micros key", microKey)(_.groupBy("ts").count().noop())
          runOnColumn(N, "group by nanos(9) key", nanosKey)(_.groupBy("ts").count().noop())
        }

        runBenchmark("MIN/MAX aggregation") {
          runProject(N, "min/max micros", s"min($microNtz) as mn", s"max($microNtz) as mx")
          runProject(N, "min/max nanos(9)", s"min($nanosNtz) as mn", s"max($nanosNtz) as mx")
        }

        runBenchmark("Datetime functions") {
          runProject(N, "hour(micros)", s"hour($microLtz)")
          runProject(N, "hour(nanos(9))", s"hour($nanosLtz)")
          runProject(N, "second(micros)", s"second($microLtz)")
          runProject(N, "second(nanos(9))", s"second($nanosLtz)")
          runProject(N, "extract(SECOND, micros)", s"extract(SECOND from $microLtz)")
          runProject(N, "extract(SECOND, nanos(9))", s"extract(SECOND from $nanosLtz)")
          // unix_nanos is nanos-only (requires p in [7,9]); the micro analog is unix_micros.
          runProject(N, "unix_micros(micros)", s"unix_micros($microLtz)")
          runProject(N, "unix_nanos(nanos(9))", s"unix_nanos($nanosLtz)")
          runProject(N, "micros + interval 1 hour", s"$microLtz + interval 1 hour")
          runProject(N, "nanos(9) + interval 1 hour", s"$nanosLtz + interval 1 hour")
        }

        runBenchmark("Parquet write/read") {
          withTempPath { path =>
            val microPath = s"${path.getAbsolutePath}/micros"
            val nanosPath = s"${path.getAbsolutePath}/nanos"

            val write = new Benchmark("Save to Parquet", N, output = output)
            write.addCase("micros (TIMESTAMP_NTZ)", 1) { _ =>
              spark.range(N).selectExpr(s"$microNtz as ts")
                .write.mode("overwrite").format("parquet").save(microPath)
            }
            write.addCase("nanos(9) (TIMESTAMP_NTZ(9))", 1) { _ =>
              spark.range(N).selectExpr(s"$nanosNtz as ts")
                .write.mode("overwrite").format("parquet").save(nanosPath)
            }
            write.run()

            val read = new Benchmark("Load from Parquet", N, output = output)
            Seq(true, false).foreach { vec =>
              read.addCase(s"micros, vectorized ${if (vec) "on" else "off"}", 3) { _ =>
                withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> vec.toString) {
                  spark.read.format("parquet").load(microPath).noop()
                }
              }
            }
            // For nanos, vectorized read is disabled internally regardless of the flag - running
            // both documents that there is no vectorized fast path yet.
            Seq(true, false).foreach { vec =>
              val name =
                s"nanos(9), vectorized ${if (vec) "on" else "off"} (row-based)"
              read.addCase(name, 3) { _ =>
                withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> vec.toString) {
                  spark.read.format("parquet").load(nanosPath).noop()
                }
              }
            }
            read.run()
          }
        }

        runBenchmark("ORC write/read") {
          withTempPath { path =>
            val microPath = s"${path.getAbsolutePath}/micros"
            val nanosPath = s"${path.getAbsolutePath}/nanos"

            val write = new Benchmark("Save to ORC", N, output = output)
            write.addCase("micros (TIMESTAMP_NTZ)", 1) { _ =>
              spark.range(N).selectExpr(s"$microNtz as ts")
                .write.mode("overwrite").format("orc").save(microPath)
            }
            write.addCase("nanos(9) (TIMESTAMP_NTZ(9))", 1) { _ =>
              spark.range(N).selectExpr(s"$nanosNtz as ts")
                .write.mode("overwrite").format("orc").save(nanosPath)
            }
            write.run()

            val read = new Benchmark("Load from ORC", N, output = output)
            Seq(true, false).foreach { vec =>
              read.addCase(s"micros, vectorized ${if (vec) "on" else "off"}", 3) { _ =>
                withSQLConf(SQLConf.ORC_VECTORIZED_READER_ENABLED.key -> vec.toString) {
                  spark.read.format("orc").load(microPath).noop()
                }
              }
            }
            Seq(true, false).foreach { vec =>
              read.addCase(s"nanos(9), vectorized ${if (vec) "on" else "off"}", 3) { _ =>
                withSQLConf(SQLConf.ORC_VECTORIZED_READER_ENABLED.key -> vec.toString) {
                  spark.read.format("orc").load(nanosPath).noop()
                }
              }
            }
            read.run()
          }
        }
      }
    }
  }
}
