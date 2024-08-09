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

/**
 * Benchmark to measure insert into table with complex data type.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class>
 *        --jars <spark core test jar>,<spark catalyst test jar> < spark sql test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to
 *      "benchmarks/InsertTableWithComplexTypeBenchmark-results.txt".
 * }}}
 */
object InsertTableWithComplexTypeBenchmark extends DataSourceWriteBenchmark {

  val tempMapView = "tempMapView"
  val tempArrayView = "tempArrayView"
  override val numRows = 1024 * 10

  def prepareTempViews(): Unit = {
    // Prepare a temp view with map type
    import spark.implicits._
    spark.range(numRows).map { _ =>
      (0 until 100).map(i => i->i).toMap
    }.createOrReplaceTempView(tempMapView)

    // Prepare a temp view with array type
    spark.range(numRows).map { _ =>
      (0 until 100).toArray
    }.createOrReplaceTempView(tempArrayView)
  }

  def insertMapTypeTableWithoutConversion(table: String, benchmark: Benchmark): Unit = {
    spark.sql(s"CREATE TABLE $table(i MAP<INT, INT>) USING parquet")
    benchmark.addCase(s"insert map type table without conversion") { _ =>
      spark.sql(s"INSERT INTO $table SELECT * FROM $tempMapView")
    }
  }

  def insertMapTypeTableWithConversion(table: String, benchmark: Benchmark): Unit = {
    spark.sql(s"CREATE TABLE $table(i MAP<String, String>) USING parquet")
    benchmark.addCase(s"insert map type table with conversion") { _ =>
      spark.sql(s"INSERT INTO $table SELECT * FROM $tempMapView")
    }
  }

  def insertArrayTypeTableWithoutConversion(table: String, benchmark: Benchmark): Unit = {
    spark.sql(s"CREATE TABLE $table(i ARRAY<INT>) USING parquet")
    benchmark.addCase(s"insert array type table without conversion") { _ =>
      spark.sql(s"INSERT INTO $table SELECT * FROM $tempArrayView")
    }
  }

  def insertArrayTypeTableWithConversion(table: String, benchmark: Benchmark): Unit = {
    spark.sql(s"CREATE TABLE $table(i ARRAY<String>) USING parquet")
    benchmark.addCase(s"insert array type table with conversion") { _ =>
      spark.sql(s"INSERT INTO $table SELECT * FROM $tempArrayView")
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val mapTable1 = "mapTable1"
    val mapTable2 = "mapTable2"
    val arrayTable1 = "arrayTable1"
    val arrayTable2 = "arrayTable2"

    withTempTable(tempMapView, tempArrayView) {
      prepareTempViews()
      withTable(mapTable1, mapTable2, arrayTable1, arrayTable2) {
        val mapTypeBenchmark =
          new Benchmark(s"Insert map type table benchmark, totalRows = $numRows",
            numRows, output = output)
        insertMapTypeTableWithConversion(mapTable2, mapTypeBenchmark)
        insertMapTypeTableWithoutConversion(mapTable1, mapTypeBenchmark)
        mapTypeBenchmark.run()

        val arrayTypeBenchmark =
          new Benchmark(s"Insert array type table benchmark, totalRows = $numRows",
            numRows, output = output)
        insertArrayTypeTableWithConversion(arrayTable2, arrayTypeBenchmark)
        insertArrayTypeTableWithoutConversion(arrayTable1, arrayTypeBenchmark)
        arrayTypeBenchmark.run()
      }
    }
  }
}
