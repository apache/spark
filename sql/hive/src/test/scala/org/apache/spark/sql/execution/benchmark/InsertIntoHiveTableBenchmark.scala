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
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.test.TestHive

/**
 * Benchmark to measure hive table write performance.
 * To run this benchmark:
 * {{{
 *   1. without sbt: bin/spark-submit --class <this class>
 *        --jars <spark catalyst test jar>,<spark core test jar>,<spark sql test jar>
 *        <spark hive test jar>
 *   2. build/sbt "hive/Test/runMain <this class>"
 *   3. generate result:
 *   SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "hive/Test/runMain <this class>"
 *      Results will be written to "benchmarks/InsertIntoHiveTableBenchmark-hive2.3-results.txt".
 * }}}
 */
object InsertIntoHiveTableBenchmark extends SqlBasedBenchmark {

  override def getSparkSession: SparkSession = TestHive.sparkSession

  val tempView = "temp"
  val numRows = 1024 * 10
  val sql = spark.sql _

  // scalastyle:off hadoopconfiguration
  private val hadoopConf = spark.sparkContext.hadoopConfiguration
  // scalastyle:on hadoopconfiguration
  hadoopConf.set("hive.exec.dynamic.partition", "true")
  hadoopConf.set("hive.exec.dynamic.partition.mode", "nonstrict")
  hadoopConf.set("hive.exec.max.dynamic.partitions", numRows.toString)

  def withTable(tableNames: String*)(f: => Unit): Unit = {
    tableNames.foreach { name =>
      sql(s"CREATE TABLE $name(a INT) STORED AS TEXTFILE PARTITIONED BY (b INT, c INT)")
    }
    try f finally {
      tableNames.foreach { name =>
        spark.sql(s"DROP TABLE IF EXISTS $name")
      }
    }
  }

  def insertOverwriteDynamic(table: String, benchmark: Benchmark): Unit = {
    benchmark.addCase("INSERT OVERWRITE DYNAMIC") { _ =>
      sql(s"INSERT OVERWRITE TABLE $table SELECT CAST(id AS INT) AS a," +
        s" CAST(id % 10 AS INT) AS b, CAST(id % 100 AS INT) AS c FROM $tempView")
    }
  }

  def insertOverwriteHybrid(table: String, benchmark: Benchmark): Unit = {
    benchmark.addCase("INSERT OVERWRITE HYBRID") { _ =>
      sql(s"INSERT OVERWRITE TABLE $table partition(b=1, c) SELECT CAST(id AS INT) AS a," +
        s" CAST(id % 10 AS INT) AS c FROM $tempView")
    }
  }

  def insertOverwriteStatic(table: String, benchmark: Benchmark): Unit = {
    benchmark.addCase("INSERT OVERWRITE STATIC") { _ =>
      sql(s"INSERT OVERWRITE TABLE $table partition(b=1, c=10) SELECT CAST(id AS INT) AS a" +
        s" FROM $tempView")
    }
  }

  def insertIntoDynamic(table: String, benchmark: Benchmark): Unit = {
    benchmark.addCase("INSERT INTO DYNAMIC") { _ =>
      sql(s"INSERT INTO TABLE $table SELECT CAST(id AS INT) AS a," +
        s" CAST(id % 10 AS INT) AS b, CAST(id % 100 AS INT) AS c FROM $tempView")
    }
  }

  def insertIntoHybrid(table: String, benchmark: Benchmark): Unit = {
    benchmark.addCase("INSERT INTO HYBRID") { _ =>
      sql(s"INSERT INTO TABLE $table partition(b=1, c) SELECT CAST(id AS INT) AS a," +
        s" CAST(id % 10 AS INT) AS c FROM $tempView")
    }
  }

  def insertIntoStatic(table: String, benchmark: Benchmark): Unit = {
    benchmark.addCase("INSERT INTO STATIC") { _ =>
      sql(s"INSERT INTO TABLE $table partition(b=1, c=10) SELECT CAST(id AS INT) AS a" +
        s" FROM $tempView")
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    spark.range(numRows).createOrReplaceTempView(tempView)

    try {
      val t1 = "t1"
      val t2 = "t2"
      val t3 = "t3"
      val t4 = "t4"
      val t5 = "t5"
      val t6 = "t6"

      val benchmark = new Benchmark(s"insert hive table benchmark", numRows, output = output)

      withTable(t1, t2, t3, t4, t5, t6) {

        insertIntoDynamic(t1, benchmark)
        insertIntoHybrid(t2, benchmark)
        insertIntoStatic(t3, benchmark)

        insertOverwriteDynamic(t4, benchmark)
        insertOverwriteHybrid(t5, benchmark)
        insertOverwriteStatic(t6, benchmark)

        benchmark.run()
      }
    } finally {
      spark.catalog.dropTempView(tempView)
    }
  }

  override def suffix: String = "-hive2.3"
}
