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

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.{Benchmark, Utils}

/**
 * Benchmark to measure parquet read performance.
 * To run this:
 *  spark-submit --class <this class> --jars <spark sql test jar>
 */
object ParquetReadBenchmark {
  val conf = new SparkConf()
  conf.set("spark.sql.parquet.compression.codec", "snappy")
  val sc = new SparkContext("local[1]", "test-sql-context", conf)
  val sqlContext = new SQLContext(sc)

  def withTempPath(f: File => Unit): Unit = {
    val path = Utils.createTempDir()
    path.delete()
    try f(path) finally Utils.deleteRecursively(path)
  }

  def withTempTable(tableNames: String*)(f: => Unit): Unit = {
    try f finally tableNames.foreach(sqlContext.dropTempTable)
  }

  def withSQLConf(pairs: (String, String)*)(f: => Unit): Unit = {
    val (keys, values) = pairs.unzip
    val currentValues = keys.map(key => Try(sqlContext.conf.getConfString(key)).toOption)
    (keys, values).zipped.foreach(sqlContext.conf.setConfString)
    try f finally {
      keys.zip(currentValues).foreach {
        case (key, Some(value)) => sqlContext.conf.setConfString(key, value)
        case (key, None) => sqlContext.conf.unsetConf(key)
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
        sqlContext.range(values).registerTempTable("t1")
        sqlContext.sql("select cast(id as INT) as id from t1")
            .write.parquet(dir.getCanonicalPath)
        sqlContext.read.parquet(dir.getCanonicalPath).registerTempTable("tempTable")

        sqlBenchmark.addCase("SQL Parquet Reader") { iter =>
          sqlContext.sql("select sum(id) from tempTable").collect()
        }

        sqlBenchmark.addCase("SQL Parquet MR") { iter =>
          withSQLConf(SQLConf.PARQUET_UNSAFE_ROW_RECORD_READER_ENABLED.key -> "false") {
            sqlContext.sql("select sum(id) from tempTable").collect()
          }
        }

        sqlBenchmark.addCase("SQL Parquet Vectorized") { iter =>
          withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "true") {
            sqlContext.sql("select sum(id) from tempTable").collect()
          }
        }

        val files = SpecificParquetRecordReaderBase.listDirectory(dir).toArray
        // Driving the parquet reader directly without Spark.
        parquetReaderBenchmark.addCase("ParquetReader") { num =>
          var sum = 0L
          files.map(_.asInstanceOf[String]).foreach { p =>
            val reader = new UnsafeRowParquetRecordReader
            reader.initialize(p, ("id" :: Nil).asJava)

            while (reader.nextKeyValue()) {
              val record = reader.getCurrentValue
              if (!record.isNullAt(0)) sum += record.getInt(0)
            }
            reader.close()
          }
        }

        // Driving the parquet reader in batch mode directly.
        parquetReaderBenchmark.addCase("ParquetReader(Batched)") { num =>
          var sum = 0L
          files.map(_.asInstanceOf[String]).foreach { p =>
            val reader = new UnsafeRowParquetRecordReader
            try {
              reader.initialize(p, ("id" :: Nil).asJava)
              val batch = reader.resultBatch()
              val col = batch.column(0)
              while (reader.nextBatch()) {
                val numRows = batch.numRows()
                var i = 0
                while (i < numRows) {
                  if (!col.getIsNull(i)) sum += col.getInt(i)
                  i += 1
                }
              }
            } finally {
              reader.close()
            }
          }
        }

        // Decoding in vectorized but having the reader return rows.
        parquetReaderBenchmark.addCase("ParquetReader(Batch -> Row)") { num =>
          var sum = 0L
          files.map(_.asInstanceOf[String]).foreach { p =>
            val reader = new UnsafeRowParquetRecordReader
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
        SQL Parquet Reader                       1042 / 1208         15.1          66.2       1.0X
        SQL Parquet MR                           1544 / 1607         10.2          98.2       0.7X
        SQL Parquet Vectorized                    674 /  739         23.3          42.9       1.5X
        */
        sqlBenchmark.run()

        /*
        Intel(R) Core(TM) i7-4870HQ CPU @ 2.50GHz
        Parquet Reader Single Int Column    Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        -------------------------------------------------------------------------------------------
        ParquetReader                             565 /  609         27.8          35.9       1.0X
        ParquetReader(Batched)                    165 /  174         95.3          10.5       3.4X
        ParquetReader(Batch -> Row)               158 /  188         99.3          10.1       3.6X
        */
        parquetReaderBenchmark.run()
      }
    }
  }

  def intStringScanBenchmark(values: Int): Unit = {
    val benchmark = new Benchmark("Int and String Scan", values)

    withTempPath { dir =>
      withTempTable("t1", "tempTable") {
        sqlContext.range(values).registerTempTable("t1")
        sqlContext.sql("select cast(id as INT) as c1, cast(id as STRING) as c2 from t1")
            .write.parquet(dir.getCanonicalPath)
        sqlContext.read.parquet(dir.getCanonicalPath).registerTempTable("tempTable")

        val benchmark = new Benchmark("Int and String Scan", values)

        benchmark.addCase("SQL Parquet Reader") { iter =>
          sqlContext.sql("select sum(c1), sum(length(c2)) from tempTable").collect
        }

        benchmark.addCase("SQL Parquet MR") { iter =>
          withSQLConf(SQLConf.PARQUET_UNSAFE_ROW_RECORD_READER_ENABLED.key -> "false") {
            sqlContext.sql("select sum(c1), sum(length(c2)) from tempTable").collect
          }
        }

        benchmark.addCase("SQL Parquet Vectorized") { iter =>
          withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "true") {
            sqlContext.sql("select sum(c1), sum(length(c2)) from tempTable").collect
          }
        }


        val files = SpecificParquetRecordReaderBase.listDirectory(dir).toArray
        benchmark.addCase("ParquetReader") { num =>
          var sum1 = 0L
          var sum2 = 0L
          files.map(_.asInstanceOf[String]).foreach { p =>
            val reader = new UnsafeRowParquetRecordReader
            reader.initialize(p, null)
            while (reader.nextKeyValue()) {
              val record = reader.getCurrentValue
              if (!record.isNullAt(0)) sum1 += record.getInt(0)
              if (!record.isNullAt(1)) sum2 += record.getUTF8String(1).numBytes()
            }
            reader.close()
          }
        }

        /*
        Intel(R) Core(TM) i7-4870HQ CPU @ 2.50GHz
        Int and String Scan:                Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        -------------------------------------------------------------------------------
        SQL Parquet Reader                       1381 / 1679          7.6         131.7       1.0X
        SQL Parquet MR                           2005 / 2177          5.2         191.2       0.7X
        SQL Parquet Vectorized                    919 / 1044         11.4          87.6       1.5X
        ParquetReader                            1035 / 1163         10.1          98.7       1.3X
        */
        benchmark.run()
      }
    }
  }

  def main(args: Array[String]): Unit = {
    intScanBenchmark(1024 * 1024 * 15)
    intStringScanBenchmark(1024 * 1024 * 10)
  }
}
