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

trait DataSourceWriteBenchmark extends SqlBasedBenchmark {

  val tempTable = "temp"
  val numRows = 1024 * 1024 * 15

  def withTempTable(tableNames: String*)(f: => Unit): Unit = {
    try f finally tableNames.foreach(spark.catalog.dropTempView)
  }

  def withTable(tableNames: String*)(f: => Unit): Unit = {
    try f finally {
      tableNames.foreach { name =>
        spark.sql(s"DROP TABLE IF EXISTS $name")
      }
    }
  }

  def writeNumeric(table: String, format: String, benchmark: Benchmark, dataType: String): Unit = {
    spark.sql(s"create table $table(id $dataType) using $format")
    benchmark.addCase(s"Output Single $dataType Column") { _ =>
      spark.sql(s"INSERT OVERWRITE TABLE $table SELECT CAST(id AS $dataType) AS c1 FROM $tempTable")
    }
  }

  def writeIntString(table: String, format: String, benchmark: Benchmark): Unit = {
    spark.sql(s"CREATE TABLE $table(c1 INT, c2 STRING) USING $format")
    benchmark.addCase("Output Int and String Column") { _ =>
      spark.sql(s"INSERT OVERWRITE TABLE $table SELECT CAST(id AS INT) AS " +
        s"c1, CAST(id AS STRING) AS c2 FROM $tempTable")
    }
  }

  def writePartition(table: String, format: String, benchmark: Benchmark): Unit = {
    spark.sql(s"CREATE TABLE $table(p INT, id INT) USING $format PARTITIONED BY (p)")
    benchmark.addCase("Output Partitions") { _ =>
      spark.sql(s"INSERT OVERWRITE TABLE $table SELECT CAST(id AS INT) AS id," +
        s" CAST(id % 2 AS INT) AS p FROM $tempTable")
    }
  }

  def writeBucket(table: String, format: String, benchmark: Benchmark): Unit = {
    spark.sql(s"CREATE TABLE $table(c1 INT, c2 INT) USING $format CLUSTERED BY (c2) INTO 2 BUCKETS")
    benchmark.addCase("Output Buckets") { _ =>
      spark.sql(s"INSERT OVERWRITE TABLE $table SELECT CAST(id AS INT) AS " +
        s"c1, CAST(id AS INT) AS c2 FROM $tempTable")
    }
  }

  def runDataSourceBenchmark(format: String, extraInfo: Option[String] = None): Unit = {
    val tableInt = "tableInt"
    val tableDouble = "tableDouble"
    val tableIntString = "tableIntString"
    val tablePartition = "tablePartition"
    val tableBucket = "tableBucket"
    withTempTable(tempTable) {
      spark.range(numRows).createOrReplaceTempView(tempTable)
      withTable(tableInt, tableDouble, tableIntString, tablePartition, tableBucket) {
        val writerName = extraInfo match {
          case Some(extra) => s"$format($extra)"
          case _ => format
        }
        val benchmark =
          new Benchmark(s"$writerName writer benchmark", numRows, output = output)
        writeNumeric(tableInt, format, benchmark, "Int")
        writeNumeric(tableDouble, format, benchmark, "Double")
        writeIntString(tableIntString, format, benchmark)
        writePartition(tablePartition, format, benchmark)
        writeBucket(tableBucket, format, benchmark)
        benchmark.run()
      }
    }
  }
}

