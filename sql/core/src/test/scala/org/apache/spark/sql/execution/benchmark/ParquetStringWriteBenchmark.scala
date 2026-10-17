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

import java.lang.management.ManagementFactory
import java.util.concurrent.atomic.AtomicLong

import com.sun.management.ThreadMXBean
import org.apache.hadoop.fs.Path
import org.apache.parquet.column.ParquetProperties

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.col

/**
 * Measures Parquet string writes from heap-backed row slices after a shuffle.
 * Compare unmodified and patched writers in fresh JVMs with the same settings.
 * Allocation counts cover the whole JVM, including driver work; CPU counts cover executor tasks.
 *
 * To run:
 * {{{
 * build/sbt "sql/Test/runMain org.apache.spark.sql.execution.benchmark.ParquetStringWriteBenchmark"
 * }}}
 * An optional argument selects one workload: short, wide, long, or low-cardinality.
 */
object ParquetStringWriteBenchmark extends SqlBasedBenchmark {

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val workloads = Seq(
      Workload("short", 120000, 8, 32),
      Workload("wide", 40000, 32, 32),
      Workload("long", 12000, 8, 1024),
      Workload("low-cardinality", 120000, 8, 32, seed = "id % 16"))
    val names = workloads.map(_.name)
    require(mainArgs.forall(names.contains), s"Unknown workload, expected one of $names")
    val allocationBean = ManagementFactory.getThreadMXBean.asInstanceOf[ThreadMXBean]
    require(allocationBean.isThreadAllocatedMemorySupported)
    allocationBean.setThreadAllocatedMemoryEnabled(true)

    workloads.filter(w => mainArgs.isEmpty || mainArgs.contains(w.name)).foreach { workload =>
      withTempPath { root =>
        val columns = (0 until workload.columns).map { index =>
          val value = s"sha2(concat(cast(${workload.seed} as string), '-$index'), 256)"
          val repeated = s"substring(repeat($value, ${(workload.length + 63) / 64}), " +
            s"1, ${workload.length})"
          s"if(id % 10 = 0, null, $repeated) as s$index"
        }
        val source = new Path(root.toURI.toString, "source").toString
        spark.range(workload.rows).selectExpr((Seq("id") ++ columns): _*)
          .write.parquet(source)
        // Cache before the shuffle so that the measured region pays for the shuffle and the write
        // but not for the Parquet scan. Caching after the shuffle would instead feed rows from the
        // columnar cache, bypassing the UnsafeRow slices this benchmark exists to measure.
        val cached = spark.read.parquet(source).cache()
        cached.count()
        try {
          val input = cached.repartition(4, col("id"))
          val expected = checksum(input)
          val destination = new Path(root.toURI.toString, "output")
          val fs = destination.getFileSystem(spark.sessionState.newHadoopConf())
          ParquetProperties.WriterVersion.values().foreach { version =>
            val name = s"${workload.name} strings, $version"
            runBenchmark(name) {
              val cpu = new AtomicLong()
              val listener = new SparkListener {
                override def onTaskEnd(event: SparkListenerTaskEnd): Unit = {
                  if (event.taskMetrics != null) {
                    cpu.addAndGet(event.taskMetrics.executorCpuTime)
                  }
                }
              }
              spark.sparkContext.addSparkListener(listener)
              try {
                val benchmark = new Benchmark(name, workload.rows, output = output)
                benchmark.addTimerCase("write strings", numIters = 5) { timer =>
                  spark.sparkContext.listenerBus.waitUntilEmpty(30000)
                  val beforeCpu = cpu.get()
                  val beforeAllocation = allocationBean.getTotalThreadAllocatedBytes
                  timer.startTiming()
                  input.write.mode("overwrite")
                    .option("compression", "snappy")
                    .option("parquet.writer.version", version.toString)
                    .option("parquet.enable.dictionary", "true")
                    .option("parquet.dictionary.page.size", 1024 * 1024)
                    .parquet(destination.toString)
                  timer.stopTiming()
                  val allocated = allocationBean.getTotalThreadAllocatedBytes - beforeAllocation
                  spark.sparkContext.listenerBus.waitUntilEmpty(30000)
                  // Warm-up runs get iteration -1; report only the timed ones.
                  if (timer.iteration >= 0) {
                    val files = fs.listStatus(destination)
                      .filter(_.getPath.getName.endsWith(".parquet"))
                    // scalastyle:off println
                    benchmark.out.println(s"${workload.name},$version,${timer.iteration}," +
                      s"allocatedBytes=$allocated,executorCpuNs=${cpu.get() - beforeCpu}," +
                      s"outputBytes=${files.map(_.getLen).sum},files=${files.length}")
                    // scalastyle:on println
                  }
                }
                benchmark.run()
                assert(checksum(spark.read.parquet(destination.toString)) == expected)
              } finally {
                spark.sparkContext.removeSparkListener(listener)
              }
            }
          }
        } finally {
          cached.unpersist()
        }
      }
    }
  }

  private def checksum(data: DataFrame): Row = {
    data.selectExpr("count(*)", "sum(cast(xxhash64(*) as decimal(38, 0)))").head()
  }

  private case class Workload(
      name: String,
      rows: Int,
      columns: Int,
      length: Int,
      seed: String = "id")
}
