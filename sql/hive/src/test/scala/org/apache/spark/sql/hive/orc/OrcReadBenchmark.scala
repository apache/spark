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

package org.apache.spark.sql.hive.orc

import java.io.File

import scala.util.Try

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.mapreduce.{JobID, TaskAttemptID, TaskID, TaskType}
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.orc.OrcFile
import org.apache.orc.mapred.OrcStruct
import org.apache.orc.mapreduce.OrcInputFormat
import org.apache.orc.storage.ql.exec.vector.{BytesColumnVector, LongColumnVector}

import org.apache.spark.SparkConf
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.parquet.SpecificParquetRecordReaderBase
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.{Benchmark, Utils}


/**
 * Benchmark to measure orc read performance.
 *
 * This is in `sql/hive` module in order to compare `sql/core` and `sql/hive` ORC data sources.
 * After removing `sql/hive` ORC data sources, we need to move this into `sql/core` module
 * like the other ORC test suites.
 */
object OrcReadBenchmark {
  val conf = new SparkConf()
  conf.set("orc.compression", "snappy")

  private val spark = SparkSession.builder()
    .master("local[1]")
    .appName("OrcReadBenchmark")
    .config(conf)
    .getOrCreate()

  // Set default configs. Individual cases will change them if necessary.
  spark.conf.set(SQLConf.ORC_VECTORIZED_READER_ENABLED.key, "true")
  spark.conf.set(SQLConf.ORC_FILTER_PUSHDOWN_ENABLED.key, "true")
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

  private val SQL_ORC_FILE_FORMAT = "org.apache.spark.sql.execution.datasources.orc.OrcFileFormat"
  private val HIVE_ORC_FILE_FORMAT = "org.apache.spark.sql.hive.orc.OrcFileFormat"

  // scalastyle:off line.size.limit
  def intScanBenchmark(values: Int): Unit = {
    // Benchmarks running through spark sql.
    val sqlBenchmark = new Benchmark("SQL Single Int Column Scan", values)
    // Benchmarks driving reader component directly.
    val orcReaderBenchmark = new Benchmark("ORC Reader Single Int Column Scan", values)

    withTempPath { dir =>
      withTempTable("t1", "coreOrcTable", "hiveOrcTable") {
        spark.range(values).createOrReplaceTempView("t1")
        spark.sql("select cast(id as INT) as id from t1")
            .write.orc(dir.getCanonicalPath)
        spark.read.format(SQL_ORC_FILE_FORMAT)
          .load(dir.getCanonicalPath).createOrReplaceTempView("coreOrcTable")
        spark.read.format(HIVE_ORC_FILE_FORMAT)
          .load(dir.getCanonicalPath).createOrReplaceTempView("hiveOrcTable")

        sqlBenchmark.addCase("SQL ORC Vectorized") { _ =>
          spark.sql("select sum(id) from coreOrcTable").collect
        }

        sqlBenchmark.addCase("SQL ORC MR") { _ =>
          withSQLConf(SQLConf.ORC_VECTORIZED_READER_ENABLED.key -> "false") {
            spark.sql("select sum(id) from coreOrcTable").collect
          }
        }

        sqlBenchmark.addCase("HIVE ORC MR") { _ =>
          spark.sql("select sum(id) from hiveOrcTable").collect
        }

        val files = SpecificParquetRecordReaderBase.listDirectory(dir).toArray
        // Driving the orc reader in batch mode directly.
        val conf = new Configuration
        orcReaderBenchmark.addCase("OrcReader Vectorized") { _ =>
          var sum = 0L
          files.map(_.asInstanceOf[String]).foreach { p =>
            val reader = OrcFile.createReader(new Path(p), OrcFile.readerOptions(conf))
            val rows = reader.rows()
            try {
              val batch = reader.getSchema.createRowBatch
              val longColumnVector = batch.cols(0).asInstanceOf[LongColumnVector]

              while (rows.nextBatch(batch)) {
                for (r <- 0 until batch.size) {
                  if (longColumnVector.noNulls || !longColumnVector.isNull(r)) {
                    val record = longColumnVector.vector(r)
                    sum += record
                  }
                }
              }
            } finally {
              rows.close()
            }
          }
        }

        orcReaderBenchmark.addCase("OrcReader") { _ =>
          var sum = 0L
          files.map(_.asInstanceOf[String]).foreach { p =>
            val oif = new OrcInputFormat[OrcStruct]
            val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
            val hadoopAttemptContext = new TaskAttemptContextImpl(conf, attemptId)
            val fileSplit = new FileSplit(new Path(p), 0L, Long.MaxValue, new Array[String](0))
            val reader = oif.createRecordReader(fileSplit, hadoopAttemptContext)
            try {
              while (reader.nextKeyValue()) {
                sum += reader.getCurrentValue.getFieldValue(0).asInstanceOf[IntWritable].get
              }
            } finally {
              reader.close()
            }
          }
        }

        /*
        Java HotSpot(TM) 64-Bit Server VM 1.8.0_131-b11 on Mac OS X 10.12.4
        Intel(R) Core(TM) i7-3615QM CPU @ 2.30GHz

        SQL Single Int Column Scan:              Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        SQL ORC Vectorized                             170 /  194         92.5          10.8       1.0X
        SQL ORC MR                                     388 /  396         40.5          24.7       0.4X
        HIVE ORC MR                                    488 /  496         32.3          31.0       0.3X
        */
        sqlBenchmark.run()

        /*
        Java HotSpot(TM) 64-Bit Server VM 1.8.0_131-b11 on Mac OS X 10.12.4
        Intel(R) Core(TM) i7-3615QM CPU @ 2.30GHz

        ORC Reader Single Int Column Scan:       Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        OrcReader Vectorized                           119 /  124        132.7           7.5       1.0X
        OrcReader                                      369 /  377         42.6          23.5       0.3X
        */
        orcReaderBenchmark.run()
      }
    }
  }

  def intStringScanBenchmark(values: Int): Unit = {
    val benchmark = new Benchmark("Int and String Scan", values)

    withTempPath { dir =>
      withTempTable("t1", "coreOrcTable", "hiveOrcTable") {
        spark.range(values).createOrReplaceTempView("t1")
        spark.sql("select cast(id as INT) as c1, cast(id as STRING) as c2 from t1")
            .write.orc(dir.getCanonicalPath)
        spark.read.format(SQL_ORC_FILE_FORMAT)
          .load(dir.getCanonicalPath).createOrReplaceTempView("coreOrcTable")
        spark.read.format(HIVE_ORC_FILE_FORMAT)
          .load(dir.getCanonicalPath).createOrReplaceTempView("hiveOrcTable")

        benchmark.addCase("SQL ORC Vectorized") { _ =>
          spark.sql("select sum(c1), sum(length(c2)) from coreOrcTable").collect
        }

        benchmark.addCase("SQL ORC MR") { _ =>
          withSQLConf(SQLConf.ORC_VECTORIZED_READER_ENABLED.key -> "false") {
            spark.sql("select sum(c1), sum(length(c2)) from coreOrcTable").collect
          }
        }

        benchmark.addCase("HIVE ORC MR") { _ =>
          spark.sql("select sum(c1), sum(length(c2)) from hiveOrcTable").collect
        }

        /*
        Java HotSpot(TM) 64-Bit Server VM 1.8.0_131-b11 on Mac OS X 10.12.4
        Intel(R) Core(TM) i7-3615QM CPU @ 2.30GHz

        Int and String Scan:                     Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        SQL ORC Vectorized                             310 /  373         33.8          29.5       1.0X
        SQL ORC MR                                     580 /  617         18.1          55.4       0.5X
        HIVE ORC MR                                    881 /  938         11.9          84.0       0.4X
        */
        benchmark.run()
      }
    }
  }

  def stringDictionaryScanBenchmark(values: Int): Unit = {
    val benchmark = new Benchmark("String Dictionary", values)

    withTempPath { dir =>
      withTempTable("t1", "coreOrcTable", "hiveOrcTable") {
        spark.range(values).createOrReplaceTempView("t1")
        spark.sql("select cast((id % 200) + 10000 as STRING) as c1 from t1")
          .write.orc(dir.getCanonicalPath)
        spark.read.format(SQL_ORC_FILE_FORMAT)
          .load(dir.getCanonicalPath).createOrReplaceTempView("coreOrcTable")
        spark.read.format(HIVE_ORC_FILE_FORMAT)
          .load(dir.getCanonicalPath).createOrReplaceTempView("hiveOrcTable")

        benchmark.addCase("SQL ORC Vectorized") { _ =>
          spark.sql("select sum(length(c1)) from coreOrcTable").collect
        }

        benchmark.addCase("SQL ORC MR") { _ =>
          withSQLConf(SQLConf.ORC_VECTORIZED_READER_ENABLED.key -> "false") {
            spark.sql("select sum(length(c1)) from coreOrcTable").collect
          }
        }

        benchmark.addCase("HIVE ORC MR") { _ =>
          spark.sql("select sum(length(c1)) from hiveOrcTable").collect
        }

        /*
        Java HotSpot(TM) 64-Bit Server VM 1.8.0_131-b11 on Mac OS X 10.12.4
        Intel(R) Core(TM) i7-3615QM CPU @ 2.30GHz

        String Dictionary:                       Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        SQL ORC Vectorized                             165 /  173         63.7          15.7       1.0X
        SQL ORC MR                                     401 /  406         26.2          38.2       0.4X
        HIVE ORC MR                                    620 /  629         16.9          59.1       0.3X
        */
        benchmark.run()
      }
    }
  }

  def partitionTableScanBenchmark(values: Int): Unit = {
    val benchmark = new Benchmark("Partitioned Table", values)

    withTempPath { dir =>
      withTempTable("t1", "coreOrcTable", "hiveOrcTable") {
        spark.range(values).createOrReplaceTempView("t1")
        spark.sql("select id % 2 as p, cast(id as INT) as id from t1")
          .write.partitionBy("p").orc(dir.getCanonicalPath)
        spark.read.format(SQL_ORC_FILE_FORMAT)
          .load(dir.getCanonicalPath).createOrReplaceTempView("coreOrcTable")
        spark.read.format(HIVE_ORC_FILE_FORMAT)
          .load(dir.getCanonicalPath).createOrReplaceTempView("hiveOrcTable")

        benchmark.addCase("SQL Read data column") { _ =>
          spark.sql("select sum(id) from coreOrcTable").collect
        }

        benchmark.addCase("SQL Read partition column") { _ =>
          spark.sql("select sum(p) from coreOrcTable").collect
        }

        benchmark.addCase("SQL Read both columns") { _ =>
          spark.sql("select sum(p), sum(id) from coreOrcTable").collect
        }

        benchmark.addCase("HIVE Read data column") { _ =>
          spark.sql("select sum(id) from hiveOrcTable").collect
        }

        benchmark.addCase("HIVE Read partition column") { _ =>
          spark.sql("select sum(p) from hiveOrcTable").collect
        }

        benchmark.addCase("HIVE Read both columns") { _ =>
          spark.sql("select sum(p), sum(id) from hiveOrcTable").collect
        }
        /*
        Java HotSpot(TM) 64-Bit Server VM 1.8.0_131-b11 on Mac OS X 10.12.4
        Intel(R) Core(TM) i7-3615QM CPU @ 2.30GHz

        Partitioned Table:                       Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        SQL Read data column                           188 /  227         83.6          12.0       1.0X
        SQL Read partition column                       98 /  109        161.2           6.2       1.9X
        SQL Read both columns                          193 /  227         81.5          12.3       1.0X
        HIVE Read data column                          530 /  530         29.7          33.7       0.4X
        HIVE Read partition column                     420 /  423         37.4          26.7       0.4X
        HIVE Read both columns                         558 /  562         28.2          35.5       0.3X
        */
        benchmark.run()
      }
    }
  }

  def stringWithNullsScanBenchmark(values: Int, fractionOfNulls: Double): Unit = {
    withTempPath { dir =>
      withTempTable("t1", "coreOrcTable", "hiveOrcTable") {
        spark.range(values).createOrReplaceTempView("t1")
        spark.sql(s"select IF(rand(1) < $fractionOfNulls, NULL, cast(id as STRING)) as c1, " +
          s"IF(rand(2) < $fractionOfNulls, NULL, cast(id as STRING)) as c2 from t1")
          .write.orc(dir.getCanonicalPath)
        spark.read.format(SQL_ORC_FILE_FORMAT)
          .load(dir.getCanonicalPath).createOrReplaceTempView("coreOrcTable")
        spark.read.format(HIVE_ORC_FILE_FORMAT)
          .load(dir.getCanonicalPath).createOrReplaceTempView("hiveOrcTable")

        val benchmark = new Benchmark("String with Nulls Scan", values)

        benchmark.addCase(s"SQL ORC Vectorized ($fractionOfNulls%)") { iter =>
          spark.sql("select sum(length(c2)) from coreOrcTable where c1 is " +
            "not NULL and c2 is not NULL").collect()
        }

        benchmark.addCase(s"HIVE ORC ($fractionOfNulls%)") { iter =>
          spark.sql("select sum(length(c2)) from hiveOrcTable where c1 is " +
            "not NULL and c2 is not NULL").collect()
        }

        val files = SpecificParquetRecordReaderBase.listDirectory(dir).toArray
        // Driving the orc reader in batch mode directly.
        val conf = new Configuration
        benchmark.addCase("OrcReader Vectorized") { _ =>
          var sum = 0L
          files.map(_.asInstanceOf[String]).foreach { p =>
            val reader = OrcFile.createReader(new Path(p), OrcFile.readerOptions(conf))
            val rows = reader.rows()
            try {
              val batch = reader.getSchema.createRowBatch
              val col = batch.cols(0).asInstanceOf[BytesColumnVector]

              while (rows.nextBatch(batch)) {
                for (r <- 0 until batch.size) {
                  val value = UTF8String.fromBytes(col.vector(r), col.start(r), col.length(r))
                  if (!col.isNull(r)) sum += value.numBytes()
                }
              }
            } finally {
              rows.close()
            }
          }
        }

        /*
        Java HotSpot(TM) 64-Bit Server VM 1.8.0_131-b11 on Mac OS X 10.12.4
        Intel(R) Core(TM) i7-3615QM CPU @ 2.30GHz

        String with Nulls Scan:                  Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        SQL ORC Vectorized (0.0%)                      501 /  596         20.9          47.7       1.0X
        HIVE ORC (0.0%)                               1225 / 1322          8.6         116.8       0.4X
        OrcReader Vectorized                           757 /  761         13.9          72.2       0.7X

        String with Nulls Scan:                  Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        SQL ORC Vectorized (0.5%)                      415 /  453         25.3          39.6       1.0X
        HIVE ORC (0.5%)                                884 /  940         11.9          84.3       0.5X
        OrcReader Vectorized                           905 /  940         11.6          86.3       0.5X

        String with Nulls Scan:                  Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        SQL ORC Vectorized (0.95%)                     221 /  239         47.4          21.1       1.0X
        HIVE ORC (0.95%)                               517 /  527         20.3          49.3       0.4X
        OrcReader Vectorized                           358 /  365         29.3          34.1       0.6X
        */

        benchmark.run()
      }
    }
  }
  // scalastyle:on line.size.limit

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
