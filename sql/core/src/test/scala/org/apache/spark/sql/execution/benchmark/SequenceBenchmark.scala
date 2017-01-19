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

import org.apache.spark.util.Benchmark

/**
 * Benchmark [[Seq]], [[List]] and [[scala.collection.mutable.Queue]] serialization
 * performance.
 * To run this:
 *  1. replace ignore(...) with test(...)
 *  2. build/sbt "sql/test-only *benchmark.SequenceBenchmark"
 *
 * Benchmarks in this file are skipped in normal builds.
 */
class SequenceBenchmark extends BenchmarkBase {
  val partitions = 20
  val rows = 1000
  val size = 100

  def generate[T <: Seq[Int]](generator: Int => ( => Int) => T): Seq[T] = {
    Seq.fill(rows)(generator(size)(1))
  }

  ignore("Collect sequence types") {
    import sparkSession.implicits._

    val sc = sparkSession.sparkContext

    val benchmark = new Benchmark(s"collect", rows)

    val seq = generate(Seq.fill(_))
    benchmark.addCase("Seq") { _ =>
      sc.parallelize(seq, partitions).toDS().map(identity).queryExecution.toRdd.collect().length
    }

    val list = generate(List.fill(_))
    benchmark.addCase("List") { _ =>
      sc.parallelize(list, partitions).toDS().map(identity).queryExecution.toRdd.collect().length
    }

    val queue = generate(scala.collection.mutable.Queue.fill(_))
    benchmark.addCase("mutable.Queue") { _ =>
      sc.parallelize(queue, partitions).toDS().map(identity).queryExecution.toRdd.collect().length
    }

    benchmark.run()

    /*
    OpenJDK 64-Bit Server VM 1.8.0_112-b15 on Linux 4.8.13-1-ARCH
    AMD A10-4600M APU with Radeon(tm) HD Graphics
    collect:                                 Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Seq                                            255 /  316          0.0      254697.3       1.0X
    List                                           152 /  177          0.0      152410.0       1.7X
    mutable.Queue                                  213 /  235          0.0      213470.0       1.2X
    */
  }
}
