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
package org.apache.spark.sql.execution.datasources.parquet

import java.io.File

import scala.collection.JavaConverters._
import scala.util.Try

import org.apache.spark.SparkConf
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.{Benchmark, Utils}

/**
 * Benchmark to measure parquet read performance.
 * To run this:
 *  spark-submit --class <this class> --jars <spark sql test jar>
 */
object ParquetReadBenchmark {
  val conf = new SparkConf()
  conf.set("spark.sql.parquet.compression.codec", "snappy")

  val spark = SparkSession.builder
    .master("local[1]")
    .appName("test-sql-context")
    .config(conf)
    .getOrCreate()

  // Set default configs. Individual cases will change them if necessary.
  spark.conf.set(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key, "true")
  spark.conf.set(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "true")

  def withTempPath(f: File => Unit): Unit = {
    val path = Utils.createTempDir()
    path.delete()
    try f(path) finally Utils.deleteRecursively(path)
  }

  def withTempTable(tableNames: String*)(f: => Unit): Unit = {
    try f finally tableNames.foreach(spark.catalog.dropTempView)
  }

  def withSQLConf(pairs: (String, String)*)(f: => Unit): Unit = {
    val (keys, values) = pairs.unzip
    val currentValues = keys.map(key => Try(spark.conf.get(key)).toOption)
    (keys, values).zipped.foreach(spark.conf.set)
    try f finally {
      keys.zip(currentValues).foreach {
        case (key, Some(value)) => spark.conf.set(key, value)
        case (key, None) => spark.conf.unset(key)
      }
    }
  }

  def intScanBenchmark(values: Int): Unit = {
    // Benchmarks running through spark sql.
    val sqlBenchmark = new Benchmark("SQL Single Int Column Scan", values)
    // Benchmarks driving reader component directly.
    val parquetReaderBenchmark = new Benchmark("Parquet Reader Single Int Column Scan", values)

    withTempPath { dir =>
      withTempTable("t1", "tempTable") {
        spark.range(values).createOrReplaceTempView("t1")
        spark.sql("select cast(id as INT) as id from t1")
            .write.parquet(dir.getCanonicalPath)
        spark.read.parquet(dir.getCanonicalPath).createOrReplaceTempView("tempTable")

        sqlBenchmark.addCase("SQL Parquet Vectorized") { iter =>
          spark.sql("select sum(id) from tempTable").collect()
        }

        sqlBenchmark.addCase("SQL Parquet MR") { iter =>
          withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false") {
            spark.sql("select sum(id) from tempTable").collect()
          }
        }

        val files = SpecificParquetRecordReaderBase.listDirectory(dir).toArray
        // Driving the parquet reader in batch mode directly.
        parquetReaderBenchmark.addCase("ParquetReader Vectorized") { num =>
          var sum = 0L
          files.map(_.asInstanceOf[String]).foreach { p =>
            val reader = new VectorizedParquetRecordReader
            try {
              reader.initialize(p, ("id" :: Nil).asJava)
              val batch = reader.resultBatch()
              val col = batch.column(0)
              while (reader.nextBatch()) {
                val numRows = batch.numRows()
                var i = 0
                while (i < numRows) {
                  if (!col.isNullAt(i)) sum += col.getInt(i)
                  i += 1
                }
              }
            } finally {
              reader.close()
            }
          }
        }

        // Decoding in vectorized but having the reader return rows.
        parquetReaderBenchmark.addCase("ParquetReader Vectorized -> Row") { num =>
          var sum = 0L
          files.map(_.asInstanceOf[String]).foreach { p =>
            val reader = new VectorizedParquetRecordReader
            try {
              reader.initialize(p, ("id" :: Nil).asJava)
              val batch = reader.resultBatch()
              while (reader.nextBatch()) {
                val it = batch.rowIterator()
                while (it.hasNext) {
                  val record = it.next()
                  if (!record.isNullAt(0)) sum += record.getInt(0)
                }
              }
            } finally {
              reader.close()
            }
          }
        }

        /*
        Intel(R) Core(TM) i7-4870HQ CPU @ 2.50GHz
        SQL Single Int Column Scan:         Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        -------------------------------------------------------------------------------------------
        SQL Parquet Vectorized                    215 /  262         73.0          13.7       1.0X
        SQL Parquet MR                           1946 / 2083          8.1         123.7       0.1X
        */
        sqlBenchmark.run()

        /*
        Intel(R) Core(TM) i7-4870HQ CPU @ 2.50GHz
        Parquet Reader Single Int Column    Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        -------------------------------------------------------------------------------------------
        ParquetReader Vectorized                  123 /  152        127.8           7.8       1.0X
        ParquetReader Vectorized -> Row           165 /  180         95.2          10.5       0.7X
        */
        parquetReaderBenchmark.run()
      }
    }
  }

  def intStringScanBenchmark(values: Int): Unit = {
    withTempPath { dir =>
      withTempTable("t1", "tempTable") {
        spark.range(values).createOrReplaceTempView("t1")
        spark.sql("select cast(id as INT) as c1, cast(id as STRING) as c2 from t1")
            .write.parquet(dir.getCanonicalPath)
        spark.read.parquet(dir.getCanonicalPath).createOrReplaceTempView("tempTable")

        val benchmark = new Benchmark("Int and String Scan", values)

        benchmark.addCase("SQL Parquet Vectorized") { iter =>
          spark.sql("select sum(c1), sum(length(c2)) from tempTable").collect
        }

        benchmark.addCase("SQL Parquet MR") { iter =>
          withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false") {
            spark.sql("select sum(c1), sum(length(c2)) from tempTable").collect
          }
        }

        val files = SpecificParquetRecordReaderBase.listDirectory(dir).toArray

        /*
        Intel(R) Core(TM) i7-4870HQ CPU @ 2.50GHz
        Int and String Scan:                Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        -------------------------------------------------------------------------------------------
        SQL Parquet Vectorized                    628 /  720         16.7          59.9       1.0X
        SQL Parquet MR                           1905 / 2239          5.5         181.7       0.3X
        */
        benchmark.run()
      }
    }
  }

  def stringDictionaryScanBenchmark(values: Int): Unit = {
    withTempPath { dir =>
      withTempTable("t1", "tempTable") {
        spark.range(values).createOrReplaceTempView("t1")
        spark.sql("select cast((id % 200) + 10000 as STRING) as c1 from t1")
          .write.parquet(dir.getCanonicalPath)
        spark.read.parquet(dir.getCanonicalPath).createOrReplaceTempView("tempTable")

        val benchmark = new Benchmark("String Dictionary", values)

        benchmark.addCase("SQL Parquet Vectorized") { iter =>
          spark.sql("select sum(length(c1)) from tempTable").collect
        }

        benchmark.addCase("SQL Parquet MR") { iter =>
          withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false") {
            spark.sql("select sum(length(c1)) from tempTable").collect
          }
        }

        /*
        Intel(R) Core(TM) i7-4870HQ CPU @ 2.50GHz
        String Dictionary:                  Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        -------------------------------------------------------------------------------------------
        SQL Parquet Vectorized                    329 /  337         31.9          31.4       1.0X
        SQL Parquet MR                           1131 / 1325          9.3         107.8       0.3X
        */
        benchmark.run()
      }
    }
  }

  def partitionTableScanBenchmark(values: Int): Unit = {
    withTempPath { dir =>
      withTempTable("t1", "tempTable") {
        spark.range(values).createOrReplaceTempView("t1")
        spark.sql("select id % 2 as p, cast(id as INT) as id from t1")
          .write.partitionBy("p").parquet(dir.getCanonicalPath)
        spark.read.parquet(dir.getCanonicalPath).createOrReplaceTempView("tempTable")

        val benchmark = new Benchmark("Partitioned Table", values)

        benchmark.addCase("Read data column") { iter =>
          spark.sql("select sum(id) from tempTable").collect
        }

        benchmark.addCase("Read partition column") { iter =>
          spark.sql("select sum(p) from tempTable").collect
        }

        benchmark.addCase("Read both columns") { iter =>
          spark.sql("select sum(p), sum(id) from tempTable").collect
        }

        /*
        Intel(R) Core(TM) i7-4870HQ CPU @ 2.50GHz
        Partitioned Table:                  Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        -------------------------------------------------------------------------------------------
        Read data column                          191 /  250         82.1          12.2       1.0X
        Read partition column                      82 /   86        192.4           5.2       2.3X
        Read both columns                         220 /  248         71.5          14.0       0.9X
        */
        benchmark.run()
      }
    }
  }

  def stringWithNullsScanBenchmark(values: Int, fractionOfNulls: Double): Unit = {
    withTempPath { dir =>
      withTempTable("t1", "tempTable") {
        spark.range(values).createOrReplaceTempView("t1")
        spark.sql(s"select IF(rand(1) < $fractionOfNulls, NULL, cast(id as STRING)) as c1, " +
          s"IF(rand(2) < $fractionOfNulls, NULL, cast(id as STRING)) as c2 from t1")
          .write.parquet(dir.getCanonicalPath)
        spark.read.parquet(dir.getCanonicalPath).createOrReplaceTempView("tempTable")

        val benchmark = new Benchmark("String with Nulls Scan", values)

        benchmark.addCase("SQL Parquet Vectorized") { iter =>
          spark.sql("select sum(length(c2)) from tempTable where c1 is " +
            "not NULL and c2 is not NULL").collect()
        }

        val files = SpecificParquetRecordReaderBase.listDirectory(dir).toArray
        benchmark.addCase("PR Vectorized") { num =>
          var sum = 0
          files.map(_.asInstanceOf[String]).foreach { p =>
            val reader = new VectorizedParquetRecordReader
            try {
              reader.initialize(p, ("c1" :: "c2" :: Nil).asJava)
              val batch = reader.resultBatch()
              while (reader.nextBatch()) {
                val rowIterator = batch.rowIterator()
                while (rowIterator.hasNext) {
                  val row = rowIterator.next()
                  val value = row.getUTF8String(0)
                  if (!row.isNullAt(0) && !row.isNullAt(1)) sum += value.numBytes()
                }
              }
            } finally {
              reader.close()
            }
          }
        }

        benchmark.addCase("PR Vectorized (Null Filtering)") { num =>
          var sum = 0L
          files.map(_.asInstanceOf[String]).foreach { p =>
            val reader = new VectorizedParquetRecordReader
            try {
              reader.initialize(p, ("c1" :: "c2" :: Nil).asJava)
              val batch = reader.resultBatch()
              batch.filterNullsInColumn(0)
              batch.filterNullsInColumn(1)
              while (reader.nextBatch()) {
                val rowIterator = batch.rowIterator()
                while (rowIterator.hasNext) {
                  sum += rowIterator.next().getUTF8String(0).numBytes()
                }
              }
            } finally {
              reader.close()
            }
          }
        }

        /*
        Intel(R) Core(TM) i7-4960HQ CPU @ 2.60GHz
        String with Nulls Scan (0%):        Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        -------------------------------------------------------------------------------------------
        SQL Parquet Vectorized                   1229 / 1648          8.5         117.2       1.0X
        PR Vectorized                             833 /  846         12.6          79.4       1.5X
        PR Vectorized (Null Filtering)            732 /  782         14.3          69.8       1.7X

        Intel(R) Core(TM) i7-4960HQ CPU @ 2.60GHz
        String with Nulls Scan (50%):       Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        -------------------------------------------------------------------------------------------
        SQL Parquet Vectorized                    995 / 1053         10.5          94.9       1.0X
        PR Vectorized                             732 /  772         14.3          69.8       1.4X
        PR Vectorized (Null Filtering)            725 /  790         14.5          69.1       1.4X

        Intel(R) Core(TM) i7-4960HQ CPU @ 2.60GHz
        String with Nulls Scan (95%):       Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        -------------------------------------------------------------------------------------------
        SQL Parquet Vectorized                    326 /  333         32.2          31.1       1.0X
        PR Vectorized                             190 /  200         55.1          18.2       1.7X
        PR Vectorized (Null Filtering)            168 /  172         62.2          16.1       1.9X
        */

        benchmark.run()
      }
    }
  }

  def main(args: Array[String]): Unit = {
    intScanBenchmark(1024 * 1024 * 15)
    intStringScanBenchmark(1024 * 1024 * 10)
    stringDictionaryScanBenchmark(1024 * 1024 * 10)
    partitionTableScanBenchmark(1024 * 1024 * 15)
    for (fractionOfNulls <- List(0.0, 0.50, 0.95)) {
      stringWithNullsScanBenchmark(1024 * 1024 * 10, fractionOfNulls)
    }
  }
}
