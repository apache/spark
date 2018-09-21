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

/**
 * Benchmark to measure Avro data sources write performance.
 * Usage:
 * 1. with spark-submit: bin/spark-submit --class <this class> <spark sql test jar>
 * 2. with sbt: build/sbt "avro/test:runMain <this class>"
 */
object AvroWriteBenchmark extends DataSourceWriteBenchmark {
  def main(args: Array[String]): Unit = {
    /*
    Intel(R) Core(TM) i7-6920HQ CPU @ 2.90GHz
    Avro writer benchmark:                   Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Output Single Int Column                      2481 / 2499          6.3         157.8       1.0X
    Output Single Double Column                   2705 / 2710          5.8         172.0       0.9X
    Output Int and String Column                  5539 / 5639          2.8         352.2       0.4X
    Output Partitions                             4613 / 5004          3.4         293.3       0.5X
    Output Buckets                                5554 / 5561          2.8         353.1       0.4X
    */
    runBenchmark("Avro")
  }
}
