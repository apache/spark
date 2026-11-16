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

import java.nio.charset.StandardCharsets
import java.util.Locale

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.columnar.{ColumnarScalarFunction, ColumnarUserDefinedFunction}
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, IntegerType, LongType, StringType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.unsafe.Platform

/** Adds one to a bigint. */
class BenchAddOne extends ColumnarScalarFunction {
  override def eval(args: ColumnarBatch): ColumnVector = {
    val n = args.numRows()
    val in = args.column(0)
    val out = new OnHeapColumnVector(n, LongType)
    var i = 0
    while (i < n) {
      if (in.isNullAt(i)) out.putNull(i) else out.putLong(i, in.getLong(i) + 1)
      i += 1
    }
    out
  }
}

object BenchMix {
  /** Eight rounds of a 64-bit mixer. */
  def mix(value: Long): Long = {
    var x = value
    var round = 0
    while (round < 8) {
      x = (x ^ (x >>> 33)) * 0xff51afd7ed558ccdL
      x = (x ^ (x >>> 29)) * 0xc4ceb9fe1a85ec53L
      round += 1
    }
    x ^ (x >>> 32)
  }
}

/** Mixes the bits of a bigint, which is heavier arithmetic than adding one. */
class BenchMix extends ColumnarScalarFunction {
  override def eval(args: ColumnarBatch): ColumnVector = {
    val n = args.numRows()
    val in = args.column(0)
    val out = new OnHeapColumnVector(n, LongType)
    var i = 0
    while (i < n) {
      if (in.isNullAt(i)) out.putNull(i) else out.putLong(i, BenchMix.mix(in.getLong(i)))
      i += 1
    }
    out
  }
}

/** The upper case of a string, followed by an exclamation mark. */
class BenchShout extends ColumnarScalarFunction {
  @transient private var buffer: Array[Byte] = _

  private def ensureCapacity(numBytes: Int): Unit = {
    if (buffer == null || buffer.length < numBytes) buffer = new Array[Byte](2 * numBytes)
  }

  override def eval(args: ColumnarBatch): ColumnVector = {
    val n = args.numRows()
    val in = args.column(0)
    val out = new OnHeapColumnVector(n, StringType)
    var i = 0
    while (i < n) {
      if (in.isNullAt(i)) {
        out.putNull(i)
      } else {
        val s = in.getUTF8String(i)
        var numBytes = s.numBytes()
        ensureCapacity(numBytes + 1)
        Platform.copyMemory(
          s.getBaseObject, s.getBaseOffset, buffer, Platform.BYTE_ARRAY_OFFSET, numBytes)
        // Converts in place while the string is ASCII, as the built-in `upper` does.
        var ascii = true
        var j = 0
        while (j < numBytes) {
          val b = buffer(j)
          if (b < 0) {
            ascii = false
          } else if (b >= 'a' && b <= 'z') {
            buffer(j) = (b - 32).toByte
          }
          j += 1
        }
        if (!ascii) {
          // scalastyle:off caselocale
          val upper = s.toUpperCase.getBytes
          // scalastyle:on caselocale
          numBytes = upper.length
          ensureCapacity(numBytes + 1)
          System.arraycopy(upper, 0, buffer, 0, numBytes)
        }
        buffer(numBytes) = '!'
        out.putByteArray(i, buffer, 0, numBytes + 1)
      }
      i += 1
    }
    out
  }
}

object BenchFnv {
  val OffsetBasis = 0xcbf29ce484222325L
  val Prime = 0x100000001b3L
}

/** The FNV-1a hash of the bytes of a string. */
class BenchFnv extends ColumnarScalarFunction {
  override def eval(args: ColumnarBatch): ColumnVector = {
    val n = args.numRows()
    val in = args.column(0)
    val out = new OnHeapColumnVector(n, LongType)
    var i = 0
    while (i < n) {
      if (in.isNullAt(i)) {
        out.putNull(i)
      } else {
        val s = in.getUTF8String(i)
        val base = s.getBaseObject
        val offset = s.getBaseOffset
        val numBytes = s.numBytes()
        var hash = BenchFnv.OffsetBasis
        var j = 0
        while (j < numBytes) {
          hash = (hash ^ (Platform.getByte(base, offset + j) & 0xffL)) * BenchFnv.Prime
          j += 1
        }
        out.putLong(i, hash)
      }
      i += 1
    }
    out
  }
}

/**
 * Compares columnar user-defined functions with Scala UDFs and, where there is one, the
 * equivalent built-in expression, over a columnar scan of Parquet files.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class>
 *        --jars <spark core test jar>,<spark catalyst test jar> <spark sql test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/ColumnarUDFBenchmark-results.txt".
 * }}}
 */
object ColumnarUDFBenchmark extends SqlBasedBenchmark {

  private def columnar(name: String, in: DataType, out: DataType, f: ColumnarScalarFunction) =
    ColumnarUserDefinedFunction(name, Seq(in), out, Some(f))

  private val addOneColumnar = columnar("add_one", LongType, LongType, new BenchAddOne)
  private val mixColumnar = columnar("mix", LongType, LongType, new BenchMix)
  private val shoutColumnar = columnar("shout", StringType, StringType, new BenchShout)
  private val fnvColumnar = columnar("fnv", StringType, LongType, new BenchFnv)

  private val addOneScala = udf((x: Long) => x + 1).asNonNullable()
  private val mixScala = udf((x: Long) => BenchMix.mix(x)).asNonNullable()
  private val shoutScala = udf((s: String) => s.toUpperCase(Locale.ROOT) + "!")
  private val fnvScala = udf { (s: String) =>
    val bytes = s.getBytes(StandardCharsets.UTF_8)
    var hash = BenchFnv.OffsetBasis
    var j = 0
    while (j < bytes.length) {
      hash = (hash ^ (bytes(j) & 0xffL)) * BenchFnv.Prime
      j += 1
    }
    hash
  }.asNonNullable()

  private def run(name: String, rows: Long)(cases: (String, DataFrame)*): Unit = {
    val benchmark = new Benchmark(name, rows, output = output)
    cases.foreach { case (caseName, df) =>
      benchmark.addCase(caseName, 5) { _ => df.noop() }
    }
    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val rows = 20L * 1000 * 1000
    runBenchmark("Columnar UDF") {
      withTempPath { path =>
        spark.range(rows)
          .select(
            col("id"),
            col("id").cast(IntegerType).as("i"),
            concat(lit("user_"), col("id"), lit("_lorem ipsum dolor")).as("s"))
          .write.parquet(path.getCanonicalPath)
        def in: DataFrame = spark.read.parquet(path.getCanonicalPath)
        val id = col("id")
        val s = col("s")
        // The Scala UDF comes first, so that the relative column is the speedup over it. The last
        // case is the same scan and aggregate without a function, which is the part of the time
        // that no implementation of the function can save.
        run("add_one(bigint): bigint", rows)(
          "Scala UDF" -> in.select(max(addOneScala(id))),
          "columnar UDF" -> in.select(max(addOneColumnar(id))),
          "built-in expression" -> in.select(max(id + 1)),
          "scan and aggregate only" -> in.select(max(id)))
        // The argument is cast to the type of the parameter. The cast is computed by a row-based
        // projection below the columnar function, whose result is converted back to columns.
        val i = col("i")
        run("add_one(bigint): bigint, over an int column that is cast", rows)(
          "Scala UDF" -> in.select(max(addOneScala(i))),
          "columnar UDF" -> in.select(max(addOneColumnar(i))),
          "built-in expression" -> in.select(max(i.cast(LongType) + 1)),
          "scan and aggregate only" -> in.select(max(i)))
        run("mix(bigint): bigint", rows)(
          "Scala UDF" -> in.select(max(mixScala(id))),
          "columnar UDF" -> in.select(max(mixColumnar(id))),
          "scan and aggregate only" -> in.select(max(id)))
        run("shout(string): string", rows)(
          "Scala UDF" -> in.select(max(octet_length(shoutScala(s)))),
          "columnar UDF" -> in.select(max(octet_length(shoutColumnar(s)))),
          "built-in expression" -> in.select(max(octet_length(concat(upper(s), lit("!"))))),
          "scan and aggregate only" -> in.select(max(octet_length(s))))
        run("fnv(string): bigint", rows)(
          "Scala UDF" -> in.select(max(fnvScala(s))),
          "columnar UDF" -> in.select(max(fnvColumnar(s))),
          "scan and aggregate only" -> in.select(max(octet_length(s))))
      }
    }
  }
}
