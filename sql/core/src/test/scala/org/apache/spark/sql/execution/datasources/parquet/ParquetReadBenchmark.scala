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

import org.apache.spark.sql.{SQLConf, SQLContext}
import org.apache.spark.util.{Benchmark, Utils}
import org.apache.spark.{SparkConf, SparkContext}

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
    withTempPath { dir =>
      sqlContext.range(values).write.parquet(dir.getCanonicalPath)
      withTempTable("tempTable") {
        sqlContext.read.parquet(dir.getCanonicalPath).registerTempTable("tempTable")
        val benchmark = new Benchmark("Single Int Column Scan", values)

        benchmark.addCase("SQL Parquet Reader") { iter =>
          sqlContext.sql("select sum(id) from tempTable").collect()
        }

        benchmark.addCase("SQL Parquet MR") { iter =>
          withSQLConf(SQLConf.PARQUET_UNSAFE_ROW_RECORD_READER_ENABLED.key -> "false") {
            sqlContext.sql("select sum(id) from tempTable").collect()
          }
        }

        val files = SpecificParquetRecordReaderBase.listDirectory(dir).toArray
        benchmark.addCase("ParquetReader") { num =>
          var sum = 0L
          files.map(_.asInstanceOf[String]).foreach { p =>
            val reader = new UnsafeRowParquetRecordReader
            reader.initialize(p, ("id" :: Nil).asJava)

            while (reader.nextKeyValue()) {
              val record = reader.getCurrentValue
              if (!record.isNullAt(0)) sum += record.getInt(0)
            }
            reader.close()
        }}

        /*
          Intel(R) Core(TM) i7-4870HQ CPU @ 2.50GHz
          Single Int Column Scan:      Avg Time(ms)    Avg Rate(M/s)  Relative Rate
          -------------------------------------------------------------------------
          SQL Parquet Reader                 1910.0            13.72         1.00 X
          SQL Parquet MR                     2330.0            11.25         0.82 X
          ParquetReader                      1252.6            20.93         1.52 X
        */
        benchmark.run()
      }
    }
  }

  def intStringScanBenchmark(values: Int): Unit = {
    withTempPath { dir =>
      withTempTable("t1", "tempTable") {
        sqlContext.range(values).registerTempTable("t1")
        sqlContext.sql("select id as c1, cast(id as STRING) as c2 from t1")
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
          Int and String Scan:         Avg Time(ms)    Avg Rate(M/s)  Relative Rate
          -------------------------------------------------------------------------
          SQL Parquet Reader                 2245.6             7.00         1.00 X
          SQL Parquet MR                     2914.2             5.40         0.77 X
          ParquetReader                      1544.6            10.18         1.45 X
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
