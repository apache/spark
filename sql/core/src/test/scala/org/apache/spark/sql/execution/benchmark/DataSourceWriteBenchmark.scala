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

  def structSchemaGenTop(num: Int)(func: String => String): String => String = {
    x => {
      var string = ""
      Range(1, num).foreach(i => string += s"${x}${i} struct<${func(s"${x}${i}")}>, ")
      string + s"${x}${num} struct<${func(s"${x}${num}")}>"
    }
  }

  def structSchemaGen(num: Int)(func: String => String): String => String = {
    x => {
      var string = ""
      Range(1, num).foreach(i => string += s"${x}${i}: struct<${func(s"${x}${i}")}>, ")
      string + s"${x}${num}: struct<${func(s"${x}${num}")}>"
    }
  }

  def structWriteGen(num: Int)(func: String => String): String => String = {
    x => {
      var string = ""
      Range(1, num).foreach(_ => string += s"struct(${func(x)}), ")
      string + s"struct(${func(x)})"
    }
  }

  def writeNestedStruct(table: String, format: String, benchmark: Benchmark): Unit = {
    val writeString = structWriteGen(2) {
      structWriteGen(2) {
        x => s"${x}, ${x}, ${x}, ${x}"
      }
    }.apply("CAST(id AS INT)")

    spark.sql(s"CREATE TABLE $table(${structSchemaGenTop(2) {
      structSchemaGen(2) {
        x => s"${x}1: INT, ${x}2: INT, ${x}3: INT, ${x}4: INT"
      }
    }.apply("c")}) USING $format")
    benchmark.addCase("Nested Struct") { _ =>
      spark.sql(s"INSERT OVERWRITE TABLE $table SELECT ${writeString} FROM $tempTable")
    }
  }

  def writeArray(table: String, format: String, benchmark: Benchmark): Unit = {
    spark.sql(s"CREATE TABLE $table(c1 ARRAY<INT>) USING $format")
    benchmark.addCase("Array") { _ =>
      spark.sql(s"INSERT OVERWRITE TABLE $table SELECT " +
          s"ARRAY(CAST(id AS INT), CAST(id AS INT), CAST(id AS INT), CAST(id AS INT))" +
          s" FROM $tempTable")
    }
  }

  def writeArrayOfStruct(table: String, format: String, benchmark: Benchmark): Unit = {
    val writeString = structWriteGen(1) {
      x => s"${x}, ${x}, ${x}, ${x}"
    }.apply("CAST(id AS INT)")

    spark.sql(s"CREATE TABLE $table(c1 ARRAY<" +
      s"struct<c11: INT, c12: INT, c13: INT, c14: INT>" +
      s">) USING $format")
    benchmark.addCase("Array of Struct") { _ =>
      spark.sql(s"INSERT OVERWRITE TABLE $table SELECT " +
        s"ARRAY(${writeString}, ${writeString}, ${writeString}, ${writeString})" +
        s" FROM $tempTable")
    }
  }

  def runDataSourceBenchmark(format: String): Unit = {
    val tableInt = "tableInt"
    val tableDouble = "tableDouble"
    val tableIntString = "tableIntString"
    val tablePartition = "tablePartition"
    val tableBucket = "tableBucket"
    val tableStruct = "tableStruct"
    val tableArray = "tableArray"
    val tableArrayOfStruct = "tableArrayOfStruct"
    withTempTable(tempTable) {
      spark.range(numRows).createOrReplaceTempView(tempTable)
      withTable(tableInt, tableDouble, tableIntString, tablePartition, tableBucket, tableStruct,
          tableArray, tableArrayOfStruct) {
        val benchmark = new Benchmark(s"$format writer benchmark", numRows, output = output)
        writeNumeric(tableInt, format, benchmark, "Int")
        writeNumeric(tableDouble, format, benchmark, "Double")
        writeIntString(tableIntString, format, benchmark)
        writePartition(tablePartition, format, benchmark)
        writeBucket(tableBucket, format, benchmark)
        if (!format.equalsIgnoreCase( "CSV")) {
          writeNestedStruct(tableStruct, format, benchmark)
          writeArray(tableArray, format, benchmark)
          writeArrayOfStruct(tableArrayOfStruct, format, benchmark)
        }
        benchmark.run()
      }
    }
  }
}

