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

import java.time.{Instant, LocalDateTime}

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

/**
 * End-to-end window-function correctness tests over the nanosecond-precision timestamp types
 * `TIMESTAMP_NTZ(p)` / `TIMESTAMP_LTZ(p)` (`p` in `[7, 9]`). Window functions are type-agnostic
 * and ride entirely on orderability and the `UnsafeRow` / window-buffer primitives, so no
 * production change is required -- this suite locks the behaviour in.
 *
 * The headline assertion is sub-microsecond ordering: input values share their `epochMicros` and
 * differ only in `nanosWithinMicro`, so the micro path cannot distinguish them and
 * `row_number()` / `rank()` / `dense_rank()` are the real proof of nanos ordering.
 * `lag` / `lead` additionally round-trip the nanos value through the window buffer / `UnsafeRow`
 * append, so collecting the neighbour back as `LocalDateTime` / `Instant` proves the carrier
 * (`epochMicros` + `nanosWithinMicro`) survives. Each ordering body runs on both the whole-stage
 * codegen comparison arm and the interpreted `Ordering[TimestampNanosVal]` arm, NTZ and LTZ.
 *
 * All sub-microsecond remainders are multiples of 100ns (100 / 200 / ... / 900) so they are exact
 * at every precision p in [7, 9] (p=7 has 100ns resolution, p=8 has 10ns); a non-100ns-multiple
 * remainder would be floored away at p=7/p=8 and collapse the intended distinct values into ties.
 *
 * The preview flag is enabled by default under tests (`Utils.isTesting`), so it is not set. The
 * session time zone is fixed so `TIMESTAMP_LTZ` values render deterministically. The two
 * subclasses run every test with ANSI mode on and off.
 *
 * NOTE: every test here projects a deterministic, distinct ordinal column (`id`, or the window
 * output `rn`/`rk`) alongside the nanos column, so `checkAnswer` (order-insensitive) suffices --
 * the row-number / rank value IS the ordering proof, so no collect-strict assertion is needed.
 */
abstract class TimestampNanosWindowSuiteBase extends SharedSparkSession {

  import testImplicits._

  override def sparkConf: SparkConf = super.sparkConf
    .set(SQLConf.SESSION_LOCAL_TIMEZONE.key, "America/Los_Angeles")

  protected val codegenModes: Seq[Seq[(String, String)]] = Seq(
    Seq(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true",
      SQLConf.CODEGEN_FACTORY_MODE.key -> "CODEGEN_ONLY"),
    Seq(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
      SQLConf.CODEGEN_FACTORY_MODE.key -> "NO_CODEGEN"))

  private def ntzSchema(p: Int): StructType =
    new StructType().add("id", IntegerType).add("ts", TimestampNTZNanosType(p))

  private def ltzSchema(p: Int): StructType =
    new StructType().add("id", IntegerType).add("ts", TimestampLTZNanosType(p))

  // ==========================================================================================
  // row_number() OVER (ORDER BY <nanos col>) -- sub-microsecond ordering, NTZ + LTZ.
  // ==========================================================================================
  // All three values share epochMicros 2020-01-01T00:00:00.000000 and differ only inside the
  // microsecond (100ns / 500ns / 900ns), so the row numbers are produced purely by nanos ordering.
  test("row_number over a nanosecond TIMESTAMP_NTZ orders by the sub-microsecond part") {
    codegenModes.foreach { conf =>
      withSQLConf(conf: _*) {
        Seq(7, 8, 9).foreach { p =>
          val data = Seq(
            Row(10, LocalDateTime.parse("2020-01-01T00:00:00.000000900")),
            Row(20, LocalDateTime.parse("2020-01-01T00:00:00.000000100")),
            Row(30, LocalDateTime.parse("2020-01-01T00:00:00.000000500")))
          val df = spark.createDataFrame(spark.sparkContext.parallelize(data), ntzSchema(p))
          // ASC: 100ns -> 500ns -> 900ns -> ids 20, 30, 10.
          checkAnswer(
            df.select($"id", row_number().over(Window.orderBy($"ts")).as("rn")),
            Seq(Row(20, 1), Row(30, 2), Row(10, 3)))
          // DESC: reversed.
          checkAnswer(
            df.select($"id", row_number().over(Window.orderBy($"ts".desc)).as("rn")),
            Seq(Row(10, 1), Row(30, 2), Row(20, 3)))
        }
      }
    }
  }

  test("row_number over a nanosecond TIMESTAMP_LTZ orders by the sub-microsecond part") {
    codegenModes.foreach { conf =>
      withSQLConf(conf: _*) {
        Seq(7, 8, 9).foreach { p =>
          val data = Seq(
            Row(10, Instant.parse("2020-01-01T00:00:00.000000900Z")),
            Row(20, Instant.parse("2020-01-01T00:00:00.000000100Z")),
            Row(30, Instant.parse("2020-01-01T00:00:00.000000500Z")))
          val df = spark.createDataFrame(spark.sparkContext.parallelize(data), ltzSchema(p))
          // ASC: 100ns -> 500ns -> 900ns -> ids 20, 30, 10.
          checkAnswer(
            df.select($"id", row_number().over(Window.orderBy($"ts")).as("rn")),
            Seq(Row(20, 1), Row(30, 2), Row(10, 3)))
          // DESC: reversed.
          checkAnswer(
            df.select($"id", row_number().over(Window.orderBy($"ts".desc)).as("rn")),
            Seq(Row(10, 1), Row(30, 2), Row(20, 3)))
        }
      }
    }
  }

  // ==========================================================================================
  // rank()/dense_rank() OVER (PARTITION BY g ORDER BY <nanos col>) -- ties at the nanos level.
  // ==========================================================================================
  // Two partitions. Within g=1 two rows share .000000500 (a sub-microsecond tie), so rank() skips
  // and dense_rank() does not; the tie can only be detected by the full nanos comparison (all rows
  // in g=1 share epochMicros).
  test("rank/dense_rank partition by key, order by a nanosecond NTZ column") {
    codegenModes.foreach { conf =>
      withSQLConf(conf: _*) {
        Seq(7, 8, 9).foreach { p =>
          val schema = new StructType()
            .add("g", IntegerType).add("id", IntegerType).add("ts", TimestampNTZNanosType(p))
          val data = Seq(
            Row(1, 101, LocalDateTime.parse("2020-01-01T00:00:00.000000500")),
            Row(1, 102, LocalDateTime.parse("2020-01-01T00:00:00.000000500")),
            Row(1, 103, LocalDateTime.parse("2020-01-01T00:00:00.000000900")),
            Row(1, 104, LocalDateTime.parse("2020-01-01T00:00:00.000000100")),
            Row(2, 201, LocalDateTime.parse("2020-01-01T00:00:00.000000900")),
            Row(2, 202, LocalDateTime.parse("2020-01-01T00:00:00.000000500")))
          val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
          val w = Window.partitionBy($"g").orderBy($"ts")
          checkAnswer(
            df.select($"g", $"id", rank().over(w).as("rk"), dense_rank().over(w).as("drk")),
            Seq(
              Row(1, 104, 1, 1),  // 100ns
              Row(1, 101, 2, 2),  // 500ns
              Row(1, 102, 2, 2),  // 500ns tie: same rank/dense_rank as 101
              Row(1, 103, 4, 3),  // 900ns: rank skips to 4, dense_rank advances to 3
              Row(2, 202, 1, 1),  // 500ns
              Row(2, 201, 2, 2))) // 900ns
        }
      }
    }
  }

  test("rank/dense_rank partition by key, order by a nanosecond LTZ column (SQL path)") {
    codegenModes.foreach { conf =>
      withSQLConf(conf: _*) {
        Seq(7, 8, 9).foreach { p =>
          val schema = new StructType()
            .add("g", IntegerType).add("id", IntegerType).add("ts", TimestampLTZNanosType(p))
          val data = Seq(
            Row(1, 101, Instant.parse("2020-01-01T00:00:00.000000500Z")),
            Row(1, 102, Instant.parse("2020-01-01T00:00:00.000000500Z")),
            Row(1, 103, Instant.parse("2020-01-01T00:00:00.000000900Z")),
            Row(2, 201, Instant.parse("2020-01-01T00:00:00.000000900Z")),
            Row(2, 202, Instant.parse("2020-01-01T00:00:00.000000500Z")))
          val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
          withTempView("nanos_ltz") {
            df.createOrReplaceTempView("nanos_ltz")
            checkAnswer(
              spark.sql(
                """select g, id,
                  |  rank() over (partition by g order by ts) as rk,
                  |  dense_rank() over (partition by g order by ts) as drk
                  |from nanos_ltz""".stripMargin),
              Seq(
                Row(1, 101, 1, 1), Row(1, 102, 1, 1), Row(1, 103, 3, 2),
                Row(2, 202, 1, 1), Row(2, 201, 2, 2)))
          }
        }
      }
    }
  }

  // ==========================================================================================
  // lag()/lead() return the neighbouring nanos VALUE -- round-trips epochMicros+nanosWithinMicro
  // through the window buffer. NTZ + LTZ. Window ordered by an unambiguous Int key so the order is
  // independent of ts; the asserted values differ only inside the microsecond.
  // ==========================================================================================
  test("lag/lead return the neighbouring nanosecond NTZ value down to the sub-microsecond") {
    codegenModes.foreach { conf =>
      withSQLConf(conf: _*) {
        Seq(7, 8, 9).foreach { p =>
          val v1 = LocalDateTime.parse("2020-01-01T00:00:00.000000100")
          val v2 = LocalDateTime.parse("2020-01-01T00:00:00.000000500")
          val v3 = LocalDateTime.parse("2020-01-01T00:00:00.000000900")
          val df = spark.createDataFrame(
            spark.sparkContext.parallelize(Seq(Row(1, v1), Row(2, v2), Row(3, v3))), ntzSchema(p))
          val w = Window.orderBy($"id")
          val res = df.select(
            $"id", lag($"ts", 1).over(w).as("prev_ts"), lead($"ts", 1).over(w).as("next_ts"))
          checkAnswer(res, Seq(
            Row(1, null, v2),   // first row: no previous
            Row(2, v1, v3),     // prev=100ns, next=900ns round-trip exactly
            Row(3, v2, null)))  // last row: no next
          assert(res.schema("prev_ts").dataType === TimestampNTZNanosType(p))
          assert(res.schema("next_ts").dataType === TimestampNTZNanosType(p))
        }
      }
    }
  }

  test("lag/lead return the neighbouring nanosecond LTZ value down to the sub-microsecond") {
    codegenModes.foreach { conf =>
      withSQLConf(conf: _*) {
        Seq(7, 8, 9).foreach { p =>
          val v1 = Instant.parse("2020-01-01T00:00:00.000000100Z")
          val v2 = Instant.parse("2020-01-01T00:00:00.000000500Z")
          val v3 = Instant.parse("2020-01-01T00:00:00.000000900Z")
          val df = spark.createDataFrame(
            spark.sparkContext.parallelize(Seq(Row(1, v1), Row(2, v2), Row(3, v3))), ltzSchema(p))
          val w = Window.orderBy($"id")
          val res = df.select(
            $"id", lag($"ts", 1).over(w).as("prev_ts"), lead($"ts", 1).over(w).as("next_ts"))
          checkAnswer(res, Seq(Row(1, null, v2), Row(2, v1, v3), Row(3, v2, null)))
          assert(res.schema("prev_ts").dataType === TimestampLTZNanosType(p))
          assert(res.schema("next_ts").dataType === TimestampLTZNanosType(p))
        }
      }
    }
  }

  // ==========================================================================================
  // lead() over a window ORDERED BY the nanos column itself -- combines both nanos paths: ordering
  // is by the sub-microsecond key AND the returned neighbour is also a nanos value.
  // ==========================================================================================
  test("lead over a window ordered by the nanosecond NTZ column itself") {
    codegenModes.foreach { conf =>
      withSQLConf(conf: _*) {
        Seq(7, 8, 9).foreach { p =>
          val a = LocalDateTime.parse("2020-01-01T00:00:00.000000100")
          val b = LocalDateTime.parse("2020-01-01T00:00:00.000000200")
          val c = LocalDateTime.parse("2020-01-01T00:00:00.000000300")
          val df = spark.createDataFrame(
            spark.sparkContext.parallelize(Seq(Row(30, c), Row(10, a), Row(20, b))), ntzSchema(p))
          val w = Window.orderBy($"ts")
          checkAnswer(
            df.select($"id", lead($"ts", 1).over(w).as("next_ts")),
            Seq(Row(10, b), Row(20, c), Row(30, null)))
        }
      }
    }
  }

  test("lead over a window ordered by the nanosecond LTZ column itself") {
    codegenModes.foreach { conf =>
      withSQLConf(conf: _*) {
        Seq(7, 8, 9).foreach { p =>
          val a = Instant.parse("2020-01-01T00:00:00.000000100Z")
          val b = Instant.parse("2020-01-01T00:00:00.000000200Z")
          val c = Instant.parse("2020-01-01T00:00:00.000000300Z")
          val df = spark.createDataFrame(
            spark.sparkContext.parallelize(Seq(Row(30, c), Row(10, a), Row(20, b))), ltzSchema(p))
          val w = Window.orderBy($"ts")
          checkAnswer(
            df.select($"id", lead($"ts", 1).over(w).as("next_ts")),
            Seq(Row(10, b), Row(20, c), Row(30, null)))
        }
      }
    }
  }

  // ==========================================================================================
  // NULLS ordering inside a window (NULLS FIRST/LAST x ASC/DESC), NTZ + LTZ.
  // ==========================================================================================
  test("row_number honours NULLS FIRST/LAST over a nanosecond NTZ window") {
    codegenModes.foreach { conf =>
      withSQLConf(conf: _*) {
        Seq(7, 8, 9).foreach { p =>
          val lo = LocalDateTime.parse("2020-01-01T00:00:00.000000100")
          val hi = LocalDateTime.parse("2020-01-01T00:00:00.000000900")
          val df = spark.createDataFrame(
            spark.sparkContext.parallelize(Seq(Row(1, lo), Row(2, hi), Row(3, null))), ntzSchema(p))
          // ASC default => NULLS FIRST: null, 100ns, 900ns.
          checkAnswer(
            df.select($"id", row_number().over(Window.orderBy($"ts")).as("rn")),
            Seq(Row(3, 1), Row(1, 2), Row(2, 3)))
          // ASC NULLS LAST: 100ns, 900ns, null.
          checkAnswer(
            df.select($"id", row_number().over(Window.orderBy($"ts".asc_nulls_last)).as("rn")),
            Seq(Row(1, 1), Row(2, 2), Row(3, 3)))
          // DESC default => NULLS LAST: 900ns, 100ns, null.
          checkAnswer(
            df.select($"id", row_number().over(Window.orderBy($"ts".desc)).as("rn")),
            Seq(Row(2, 1), Row(1, 2), Row(3, 3)))
          // DESC NULLS FIRST: null, 900ns, 100ns.
          checkAnswer(
            df.select($"id", row_number().over(Window.orderBy($"ts".desc_nulls_first)).as("rn")),
            Seq(Row(3, 1), Row(2, 2), Row(1, 3)))
        }
      }
    }
  }

  test("row_number honours NULLS FIRST/LAST over a nanosecond LTZ window") {
    codegenModes.foreach { conf =>
      withSQLConf(conf: _*) {
        Seq(7, 8, 9).foreach { p =>
          val lo = Instant.parse("2020-01-01T00:00:00.000000100Z")
          val hi = Instant.parse("2020-01-01T00:00:00.000000900Z")
          val df = spark.createDataFrame(
            spark.sparkContext.parallelize(Seq(Row(1, lo), Row(2, hi), Row(3, null))), ltzSchema(p))
          // ASC default => NULLS FIRST: null, 100ns, 900ns.
          checkAnswer(
            df.select($"id", row_number().over(Window.orderBy($"ts")).as("rn")),
            Seq(Row(3, 1), Row(1, 2), Row(2, 3)))
          // ASC NULLS LAST: 100ns, 900ns, null.
          checkAnswer(
            df.select($"id", row_number().over(Window.orderBy($"ts".asc_nulls_last)).as("rn")),
            Seq(Row(1, 1), Row(2, 2), Row(3, 3)))
          // DESC default => NULLS LAST: 900ns, 100ns, null.
          checkAnswer(
            df.select($"id", row_number().over(Window.orderBy($"ts".desc)).as("rn")),
            Seq(Row(2, 1), Row(1, 2), Row(3, 3)))
          // DESC NULLS FIRST: null, 900ns, 100ns.
          checkAnswer(
            df.select($"id", row_number().over(Window.orderBy($"ts".desc_nulls_first)).as("rn")),
            Seq(Row(3, 1), Row(2, 2), Row(1, 3)))
        }
      }
    }
  }
}

// Runs the nanosecond timestamp window tests with ANSI mode enabled explicitly.
class TimestampNanosWindowAnsiOnSuite extends TimestampNanosWindowSuiteBase {
  override def sparkConf: SparkConf = super.sparkConf.set(SQLConf.ANSI_ENABLED.key, "true")
}

// Runs the nanosecond timestamp window tests with ANSI mode disabled explicitly.
class TimestampNanosWindowAnsiOffSuite extends TimestampNanosWindowSuiteBase {
  override def sparkConf: SparkConf = super.sparkConf.set(SQLConf.ANSI_ENABLED.key, "false")
}
