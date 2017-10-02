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
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.parquet.SpecificParquetRecordReaderBase
import org.apache.spark.sql.internal.SQLConf
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

        sqlBenchmark.addCase("SQL ORC Vectorized Reader") { _ =>
          spark.sql("select sum(id) from coreOrcTable").collect
        }

        sqlBenchmark.addCase("SQL ORC MR Reader") { _ =>
          withSQLConf(SQLConf.ORC_VECTORIZED_READER_ENABLED.key -> "false") {
            spark.sql("select sum(id) from coreOrcTable").collect
          }
        }

        sqlBenchmark.addCase("HIVE ORC MR Reader") { _ =>
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
        Intel(R) Core(TM) i7-4770HQ CPU @ 2.20GHz

        SQL Single Int Column Scan:              Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        SQL ORC Vectorized Reader                      143 /  151        110.1           9.1       1.0X
        SQL ORC MR Reader                              413 /  431         38.1          26.2       0.3X
        HIVE ORC MR Reader                             443 /  456         35.5          28.2       0.3X
        */
        sqlBenchmark.run()

        /*
        Java HotSpot(TM) 64-Bit Server VM 1.8.0_131-b11 on Mac OS X 10.12.4
        Intel(R) Core(TM) i7-4770HQ CPU @ 2.20GHz

        ORC Reader Single Int Column Scan:       Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        OrcReader Vectorized                           161 /  167         97.6          10.2       1.0X
        OrcReader                                      352 /  354         44.7          22.4       0.5X
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

        benchmark.addCase("SQL ORC Vectorized Reader") { _ =>
          spark.sql("select sum(c1), sum(length(c2)) from coreOrcTable").collect
        }

        benchmark.addCase("SQL ORC MR Reader") { _ =>
          withSQLConf(SQLConf.ORC_VECTORIZED_READER_ENABLED.key -> "false") {
            spark.sql("select sum(c1), sum(length(c2)) from coreOrcTable").collect
          }
        }

        benchmark.addCase("HIVE ORC MR Reader") { _ =>
          spark.sql("select sum(c1), sum(length(c2)) from hiveOrcTable").collect
        }

        /*
        Java HotSpot(TM) 64-Bit Server VM 1.8.0_131-b11 on Mac OS X 10.12.4
        Intel(R) Core(TM) i7-4770HQ CPU @ 2.20GHz

        Int and String Scan:                     Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        SQL ORC Vectorized Reader                      253 /  300         41.5          24.1       1.0X
        SQL ORC MR Reader                              588 /  644         17.8          56.1       0.4X
        HIVE ORC MR Reader                             765 /  803         13.7          73.0       0.3X
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

        benchmark.addCase("SQL ORC Vectorized Reader") { _ =>
          spark.sql("select sum(length(c1)) from coreOrcTable").collect
        }

        benchmark.addCase("SQL ORC MR Reader") { _ =>
          withSQLConf(SQLConf.ORC_VECTORIZED_READER_ENABLED.key -> "false") {
            spark.sql("select sum(length(c1)) from coreOrcTable").collect
          }
        }

        benchmark.addCase("HIVE ORC MR Reader") { _ =>
          spark.sql("select sum(length(c1)) from hiveOrcTable").collect
        }

        /*
        Java HotSpot(TM) 64-Bit Server VM 1.8.0_131-b11 on Mac OS X 10.12.4
        Intel(R) Core(TM) i7-4770HQ CPU @ 2.20GHz

        String Dictionary:                       Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        SQL ORC Vectorized Reader                      141 /  149         74.6          13.4       1.0X
        SQL ORC MR Reader                              410 /  415         25.5          39.1       0.3X
        HIVE ORC MR Reader                             573 /  579         18.3          54.7       0.2X
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
        Intel(R) Core(TM) i7-4770HQ CPU @ 2.20GHz

        Partitioned Table:                       Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        SQL Read data column                           167 /  184         94.1          10.6       1.0X
        SQL Read partition column                       82 /   91        191.8           5.2       2.0X
        SQL Read both columns                          170 /  187         92.7          10.8       1.0X
        HIVE Read data column                          513 /  516         30.7          32.6       0.3X
        HIVE Read partition column                     362 /  367         43.4          23.0       0.5X
        HIVE Read both columns                         515 /  520         30.5          32.7       0.3X
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

        benchmark.addCase(s"SQL ORC Vectorized Reader ($fractionOfNulls%)") { iter =>
          spark.sql("select sum(length(c2)) from coreOrcTable where c1 is " +
            "not NULL and c2 is not NULL").collect()
        }

        benchmark.addCase(s"HIVE ORC ($fractionOfNulls%)") { iter =>
          spark.sql("select sum(length(c2)) from hiveOrcTable where c1 is " +
            "not NULL and c2 is not NULL").collect()
        }

        /*
        Java HotSpot(TM) 64-Bit Server VM 1.8.0_131-b11 on Mac OS X 10.12.4
        Intel(R) Core(TM) i7-4770HQ CPU @ 2.20GHz

        String with Nulls Scan:                  Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        SQL ORC Vectorized Reader (0.0%)               455 /  510         23.0          43.4       1.0X
        HIVE ORC (0.0%)                               1128 / 1165          9.3         107.5       0.4X

        String with Nulls Scan:                  Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        SQL ORC Vectorized Reader (0.5%)               366 /  394         28.6          34.9       1.0X
        HIVE ORC (0.5%)                                859 /  899         12.2          81.9       0.4X

        String with Nulls Scan:                  Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        SQL ORC Vectorized Reader (0.95%)              188 /  204         55.7          17.9       1.0X
        HIVE ORC (0.95%)                               488 /  508         21.5          46.5       0.4X
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
