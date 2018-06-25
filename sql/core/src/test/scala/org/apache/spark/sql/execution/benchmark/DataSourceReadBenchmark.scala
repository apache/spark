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

import java.io.File

import scala.collection.JavaConverters._
import scala.util.{Random, Try}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.parquet.{SpecificParquetRecordReaderBase, VectorizedParquetRecordReader}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnVector
import org.apache.spark.util.{Benchmark, Utils}


/**
 * Benchmark to measure data source read performance.
 * To run this:
 *  spark-submit --class <this class> <spark sql test jar>
 */
object DataSourceReadBenchmark {
  val conf = new SparkConf()
    .setAppName("DataSourceReadBenchmark")
    // Since `spark.master` always exists, overrides this value
    .set("spark.master", "local[1]")
    .setIfMissing("spark.driver.memory", "3g")
    .setIfMissing("spark.executor.memory", "3g")
    .setIfMissing("spark.ui.enabled", "false")

  val spark = SparkSession.builder.config(conf).getOrCreate()

  // Set default configs. Individual cases will change them if necessary.
  spark.conf.set(SQLConf.ORC_FILTER_PUSHDOWN_ENABLED.key, "true")
  spark.conf.set(SQLConf.ORC_COPY_BATCH_TO_SPARK.key, "false")
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
  private def prepareTable(dir: File, df: DataFrame, partition: Option[String] = None): Unit = {
    val testDf = if (partition.isDefined) {
      df.write.partitionBy(partition.get)
    } else {
      df.write
    }

    saveAsCsvTable(testDf, dir.getCanonicalPath + "/csv")
    saveAsJsonTable(testDf, dir.getCanonicalPath + "/json")
    saveAsParquetTable(testDf, dir.getCanonicalPath + "/parquet")
    saveAsOrcTable(testDf, dir.getCanonicalPath + "/orc")
  }

  private def saveAsCsvTable(df: DataFrameWriter[Row], dir: String): Unit = {
    df.mode("overwrite").option("compression", "gzip").option("header", true).csv(dir)
    spark.read.option("header", true).csv(dir).createOrReplaceTempView("csvTable")
  }

  private def saveAsJsonTable(df: DataFrameWriter[Row], dir: String): Unit = {
    df.mode("overwrite").option("compression", "gzip").json(dir)
    spark.read.json(dir).createOrReplaceTempView("jsonTable")
  }

  private def saveAsParquetTable(df: DataFrameWriter[Row], dir: String): Unit = {
    df.mode("overwrite").option("compression", "snappy").parquet(dir)
    spark.read.parquet(dir).createOrReplaceTempView("parquetTable")
  }

  private def saveAsOrcTable(df: DataFrameWriter[Row], dir: String): Unit = {
    df.mode("overwrite").option("compression", "snappy").orc(dir)
    spark.read.orc(dir).createOrReplaceTempView("orcTable")
  }

  def numericScanBenchmark(values: Int, dataType: DataType): Unit = {
    // Benchmarks running through spark sql.
    val sqlBenchmark = new Benchmark(s"SQL Single ${dataType.sql} Column Scan", values)

    // Benchmarks driving reader component directly.
    val parquetReaderBenchmark = new Benchmark(
      s"Parquet Reader Single ${dataType.sql} Column Scan", values)

    withTempPath { dir =>
      withTempTable("t1", "csvTable", "jsonTable", "parquetTable", "orcTable") {
        import spark.implicits._
        spark.range(values).map(_ => Random.nextLong).createOrReplaceTempView("t1")

        prepareTable(dir, spark.sql(s"SELECT CAST(value as ${dataType.sql}) id FROM t1"))

        sqlBenchmark.addCase("SQL CSV") { _ =>
          spark.sql("select sum(id) from csvTable").collect()
        }

        sqlBenchmark.addCase("SQL Json") { _ =>
          spark.sql("select sum(id) from jsonTable").collect()
        }

        sqlBenchmark.addCase("SQL Parquet Vectorized") { _ =>
          spark.sql("select sum(id) from parquetTable").collect()
        }

        sqlBenchmark.addCase("SQL Parquet MR") { _ =>
          withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false") {
            spark.sql("select sum(id) from parquetTable").collect()
          }
        }

        sqlBenchmark.addCase("SQL ORC Vectorized") { _ =>
          spark.sql("SELECT sum(id) FROM orcTable").collect()
        }

        sqlBenchmark.addCase("SQL ORC Vectorized with copy") { _ =>
          withSQLConf(SQLConf.ORC_COPY_BATCH_TO_SPARK.key -> "true") {
            spark.sql("SELECT sum(id) FROM orcTable").collect()
          }
        }

        sqlBenchmark.addCase("SQL ORC MR") { _ =>
          withSQLConf(SQLConf.ORC_VECTORIZED_READER_ENABLED.key -> "false") {
            spark.sql("SELECT sum(id) FROM orcTable").collect()
          }
        }

        /*
        OpenJDK 64-Bit Server VM 1.8.0_171-b10 on Linux 4.14.33-51.37.amzn1.x86_64
        Intel(R) Xeon(R) CPU E5-2670 v2 @ 2.50GHz
        SQL Single TINYINT Column Scan:      Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        --------------------------------------------------------------------------------------------
        SQL CSV                                 22964 / 23096          0.7        1460.0       1.0X
        SQL Json                                  8469 / 8593          1.9         538.4       2.7X
        SQL Parquet Vectorized                     164 /  177         95.8          10.4     139.9X
        SQL Parquet MR                            1687 / 1706          9.3         107.2      13.6X
        SQL ORC Vectorized                         191 /  197         82.3          12.2     120.2X
        SQL ORC Vectorized with copy               215 /  219         73.2          13.7     106.9X
        SQL ORC MR                                1392 / 1412         11.3          88.5      16.5X


        SQL Single SMALLINT Column Scan:     Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        --------------------------------------------------------------------------------------------
        SQL CSV                                 24090 / 24097          0.7        1531.6       1.0X
        SQL Json                                  8791 / 8813          1.8         558.9       2.7X
        SQL Parquet Vectorized                     204 /  212         77.0          13.0     117.9X
        SQL Parquet MR                            1813 / 1850          8.7         115.3      13.3X
        SQL ORC Vectorized                         226 /  230         69.7          14.4     106.7X
        SQL ORC Vectorized with copy               295 /  298         53.3          18.8      81.6X
        SQL ORC MR                                1526 / 1549         10.3          97.1      15.8X


        SQL Single INT Column Scan:          Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        --------------------------------------------------------------------------------------------
        SQL CSV                                 25637 / 25791          0.6        1629.9       1.0X
        SQL Json                                  9532 / 9570          1.7         606.0       2.7X
        SQL Parquet Vectorized                     181 /  191         86.8          11.5     141.5X
        SQL Parquet MR                            2210 / 2227          7.1         140.5      11.6X
        SQL ORC Vectorized                         309 /  317         50.9          19.6      83.0X
        SQL ORC Vectorized with copy               316 /  322         49.8          20.1      81.2X
        SQL ORC MR                                1650 / 1680          9.5         104.9      15.5X


        SQL Single BIGINT Column Scan:       Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        --------------------------------------------------------------------------------------------
        SQL CSV                                 31617 / 31764          0.5        2010.1       1.0X
        SQL Json                                12440 / 12451          1.3         790.9       2.5X
        SQL Parquet Vectorized                     284 /  315         55.4          18.0     111.4X
        SQL Parquet MR                            2382 / 2390          6.6         151.5      13.3X
        SQL ORC Vectorized                         398 /  403         39.5          25.3      79.5X
        SQL ORC Vectorized with copy               410 /  413         38.3          26.1      77.1X
        SQL ORC MR                                1783 / 1813          8.8         113.4      17.7X


        SQL Single FLOAT Column Scan:        Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        --------------------------------------------------------------------------------------------
        SQL CSV                                 26679 / 26742          0.6        1696.2       1.0X
        SQL Json                                12490 / 12541          1.3         794.1       2.1X
        SQL Parquet Vectorized                     174 /  183         90.4          11.1     153.3X
        SQL Parquet MR                            2201 / 2223          7.1         140.0      12.1X
        SQL ORC Vectorized                         415 /  429         37.9          26.4      64.3X
        SQL ORC Vectorized with copy               422 /  428         37.2          26.9      63.2X
        SQL ORC MR                                1767 / 1773          8.9         112.3      15.1X


        SQL Single DOUBLE Column Scan:       Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        --------------------------------------------------------------------------------------------
        SQL CSV                                 34223 / 34324          0.5        2175.8       1.0X
        SQL Json                                17784 / 17785          0.9        1130.7       1.9X
        SQL Parquet Vectorized                     277 /  283         56.7          17.6     123.4X
        SQL Parquet MR                            2356 / 2386          6.7         149.8      14.5X
        SQL ORC Vectorized                         533 /  536         29.5          33.9      64.2X
        SQL ORC Vectorized with copy               541 /  546         29.1          34.4      63.3X
        SQL ORC MR                                2166 / 2177          7.3         137.7      15.8X
        */
        sqlBenchmark.run()

        // Driving the parquet reader in batch mode directly.
        val files = SpecificParquetRecordReaderBase.listDirectory(new File(dir, "parquet")).toArray
        val enableOffHeapColumnVector = spark.sessionState.conf.offHeapColumnVectorEnabled
        val vectorizedReaderBatchSize = spark.sessionState.conf.parquetVectorizedReaderBatchSize
        parquetReaderBenchmark.addCase("ParquetReader Vectorized") { _ =>
          var longSum = 0L
          var doubleSum = 0.0
          val aggregateValue: (ColumnVector, Int) => Unit = dataType match {
            case ByteType => (col: ColumnVector, i: Int) => longSum += col.getByte(i)
            case ShortType => (col: ColumnVector, i: Int) => longSum += col.getShort(i)
            case IntegerType => (col: ColumnVector, i: Int) => longSum += col.getInt(i)
            case LongType => (col: ColumnVector, i: Int) => longSum += col.getLong(i)
            case FloatType => (col: ColumnVector, i: Int) => doubleSum += col.getFloat(i)
            case DoubleType => (col: ColumnVector, i: Int) => doubleSum += col.getDouble(i)
          }

          files.map(_.asInstanceOf[String]).foreach { p =>
            val reader = new VectorizedParquetRecordReader(
              null, enableOffHeapColumnVector, vectorizedReaderBatchSize)
            try {
              reader.initialize(p, ("id" :: Nil).asJava)
              val batch = reader.resultBatch()
              val col = batch.column(0)
              while (reader.nextBatch()) {
                val numRows = batch.numRows()
                var i = 0
                while (i < numRows) {
                  if (!col.isNullAt(i)) aggregateValue(col, i)
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
          var longSum = 0L
          var doubleSum = 0.0
          val aggregateValue: (InternalRow) => Unit = dataType match {
            case ByteType => (col: InternalRow) => longSum += col.getByte(0)
            case ShortType => (col: InternalRow) => longSum += col.getShort(0)
            case IntegerType => (col: InternalRow) => longSum += col.getInt(0)
            case LongType => (col: InternalRow) => longSum += col.getLong(0)
            case FloatType => (col: InternalRow) => doubleSum += col.getFloat(0)
            case DoubleType => (col: InternalRow) => doubleSum += col.getDouble(0)
          }

          files.map(_.asInstanceOf[String]).foreach { p =>
            val reader = new VectorizedParquetRecordReader(
              null, enableOffHeapColumnVector, vectorizedReaderBatchSize)
            try {
              reader.initialize(p, ("id" :: Nil).asJava)
              val batch = reader.resultBatch()
              while (reader.nextBatch()) {
                val it = batch.rowIterator()
                while (it.hasNext) {
                  val record = it.next()
                  if (!record.isNullAt(0)) aggregateValue(record)
                }
              }
            } finally {
              reader.close()
            }
          }
        }

        /*
        OpenJDK 64-Bit Server VM 1.8.0_171-b10 on Linux 4.14.33-51.37.amzn1.x86_64
        Intel(R) Xeon(R) CPU E5-2670 v2 @ 2.50GHz
        Single TINYINT Column Scan:          Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        --------------------------------------------------------------------------------------------
        ParquetReader Vectorized                   198 /  202         79.4          12.6       1.0X
        ParquetReader Vectorized -> Row            119 /  121        132.3           7.6       1.7X


        Single SMALLINT Column Scan:         Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        --------------------------------------------------------------------------------------------
        ParquetReader Vectorized                   282 /  287         55.8          17.9       1.0X
        ParquetReader Vectorized -> Row            246 /  247         64.0          15.6       1.1X


        Single INT Column Scan:              Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        --------------------------------------------------------------------------------------------
        ParquetReader Vectorized                   258 /  262         60.9          16.4       1.0X
        ParquetReader Vectorized -> Row            259 /  260         60.8          16.5       1.0X


        Single BIGINT Column Scan:           Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        --------------------------------------------------------------------------------------------
        ParquetReader Vectorized                   361 /  369         43.6          23.0       1.0X
        ParquetReader Vectorized -> Row            361 /  371         43.6          22.9       1.0X


        Single FLOAT Column Scan:            Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        --------------------------------------------------------------------------------------------
        ParquetReader Vectorized                   253 /  261         62.2          16.1       1.0X
        ParquetReader Vectorized -> Row            254 /  256         61.9          16.2       1.0X


        Single DOUBLE Column Scan:           Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        --------------------------------------------------------------------------------------------
        ParquetReader Vectorized                   357 /  364         44.0          22.7       1.0X
        ParquetReader Vectorized -> Row            358 /  366         44.0          22.7       1.0X
        */
        parquetReaderBenchmark.run()
      }
    }
  }

  def intStringScanBenchmark(values: Int): Unit = {
    val benchmark = new Benchmark("Int and String Scan", values)

    withTempPath { dir =>
      withTempTable("t1", "csvTable", "jsonTable", "parquetTable", "orcTable") {
        import spark.implicits._
        spark.range(values).map(_ => Random.nextLong).createOrReplaceTempView("t1")

        prepareTable(
          dir,
          spark.sql("SELECT CAST(value AS INT) AS c1, CAST(value as STRING) AS c2 FROM t1"))

        benchmark.addCase("SQL CSV") { _ =>
          spark.sql("select sum(c1), sum(length(c2)) from csvTable").collect()
        }

        benchmark.addCase("SQL Json") { _ =>
          spark.sql("select sum(c1), sum(length(c2)) from jsonTable").collect()
        }

        benchmark.addCase("SQL Parquet Vectorized") { _ =>
          spark.sql("select sum(c1), sum(length(c2)) from parquetTable").collect()
        }

        benchmark.addCase("SQL Parquet MR") { _ =>
          withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false") {
            spark.sql("select sum(c1), sum(length(c2)) from parquetTable").collect()
          }
        }

        benchmark.addCase("SQL ORC Vectorized") { _ =>
          spark.sql("SELECT sum(c1), sum(length(c2)) FROM orcTable").collect()
        }

        benchmark.addCase("SQL ORC Vectorized with copy") { _ =>
          withSQLConf(SQLConf.ORC_COPY_BATCH_TO_SPARK.key -> "true") {
            spark.sql("SELECT sum(c1), sum(length(c2)) FROM orcTable").collect()
          }
        }

        benchmark.addCase("SQL ORC MR") { _ =>
          withSQLConf(SQLConf.ORC_VECTORIZED_READER_ENABLED.key -> "false") {
            spark.sql("SELECT sum(c1), sum(length(c2)) FROM orcTable").collect()
          }
        }

        /*
        OpenJDK 64-Bit Server VM 1.8.0_171-b10 on Linux 4.14.33-51.37.amzn1.x86_64
        Intel(R) Xeon(R) CPU E5-2670 v2 @ 2.50GHz
        Int and String Scan:                 Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        --------------------------------------------------------------------------------------------
        SQL CSV                                 27145 / 27158          0.4        2588.7       1.0X
        SQL Json                                12969 / 13337          0.8        1236.8       2.1X
        SQL Parquet Vectorized                    2419 / 2448          4.3         230.7      11.2X
        SQL Parquet MR                            4631 / 4633          2.3         441.7       5.9X
        SQL ORC Vectorized                        2412 / 2465          4.3         230.0      11.3X
        SQL ORC Vectorized with copy              2633 / 2675          4.0         251.1      10.3X
        SQL ORC MR                                4280 / 4350          2.4         408.2       6.3X
        */
        benchmark.run()
      }
    }
  }

  def repeatedStringScanBenchmark(values: Int): Unit = {
    val benchmark = new Benchmark("Repeated String", values)

    withTempPath { dir =>
      withTempTable("t1", "csvTable", "jsonTable", "parquetTable", "orcTable") {
        import spark.implicits._
        spark.range(values).map(_ => Random.nextLong).createOrReplaceTempView("t1")

        prepareTable(
          dir,
          spark.sql("select cast((value % 200) + 10000 as STRING) as c1 from t1"))

        benchmark.addCase("SQL CSV") { _ =>
          spark.sql("select sum(length(c1)) from csvTable").collect()
        }

        benchmark.addCase("SQL Json") { _ =>
          spark.sql("select sum(length(c1)) from jsonTable").collect()
        }

        benchmark.addCase("SQL Parquet Vectorized") { _ =>
          spark.sql("select sum(length(c1)) from parquetTable").collect()
        }

        benchmark.addCase("SQL Parquet MR") { _ =>
          withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false") {
            spark.sql("select sum(length(c1)) from parquetTable").collect()
          }
        }

        benchmark.addCase("SQL ORC Vectorized") { _ =>
          spark.sql("select sum(length(c1)) from orcTable").collect()
        }

        benchmark.addCase("SQL ORC Vectorized with copy") { _ =>
          withSQLConf(SQLConf.ORC_COPY_BATCH_TO_SPARK.key -> "true") {
            spark.sql("select sum(length(c1)) from orcTable").collect()
          }
        }

        benchmark.addCase("SQL ORC MR") { _ =>
          withSQLConf(SQLConf.ORC_VECTORIZED_READER_ENABLED.key -> "false") {
            spark.sql("select sum(length(c1)) from orcTable").collect()
          }
        }

        /*
        OpenJDK 64-Bit Server VM 1.8.0_171-b10 on Linux 4.14.33-51.37.amzn1.x86_64
        Intel(R) Xeon(R) CPU E5-2670 v2 @ 2.50GHz
        Repeated String:                     Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        --------------------------------------------------------------------------------------------
        SQL CSV                                 17345 / 17424          0.6        1654.1       1.0X
        SQL Json                                  8639 / 8664          1.2         823.9       2.0X
        SQL Parquet Vectorized                     839 /  854         12.5          80.0      20.7X
        SQL Parquet MR                            1771 / 1775          5.9         168.9       9.8X
        SQL ORC Vectorized                         550 /  569         19.1          52.4      31.6X
        SQL ORC Vectorized with copy               785 /  849         13.4          74.9      22.1X
        SQL ORC MR                                2168 / 2202          4.8         206.7       8.0X
        */
        benchmark.run()
      }
    }
  }

  def partitionTableScanBenchmark(values: Int): Unit = {
    val benchmark = new Benchmark("Partitioned Table", values)

    withTempPath { dir =>
      withTempTable("t1", "csvTable", "jsonTable", "parquetTable", "orcTable") {
        import spark.implicits._
        spark.range(values).map(_ => Random.nextLong).createOrReplaceTempView("t1")

        prepareTable(dir, spark.sql("SELECT value % 2 AS p, value AS id FROM t1"), Some("p"))

        benchmark.addCase("Data column - CSV") { _ =>
          spark.sql("select sum(id) from csvTable").collect()
        }

        benchmark.addCase("Data column - Json") { _ =>
          spark.sql("select sum(id) from jsonTable").collect()
        }

        benchmark.addCase("Data column - Parquet Vectorized") { _ =>
          spark.sql("select sum(id) from parquetTable").collect()
        }

        benchmark.addCase("Data column - Parquet MR") { _ =>
          withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false") {
            spark.sql("select sum(id) from parquetTable").collect()
          }
        }

        benchmark.addCase("Data column - ORC Vectorized") { _ =>
          spark.sql("SELECT sum(id) FROM orcTable").collect()
        }

        benchmark.addCase("Data column - ORC Vectorized with copy") { _ =>
          withSQLConf(SQLConf.ORC_COPY_BATCH_TO_SPARK.key -> "true") {
            spark.sql("SELECT sum(id) FROM orcTable").collect()
          }
        }

        benchmark.addCase("Data column - ORC MR") { _ =>
          withSQLConf(SQLConf.ORC_VECTORIZED_READER_ENABLED.key -> "false") {
            spark.sql("SELECT sum(id) FROM orcTable").collect()
          }
        }

        benchmark.addCase("Partition column - CSV") { _ =>
          spark.sql("select sum(p) from csvTable").collect()
        }

        benchmark.addCase("Partition column - Json") { _ =>
          spark.sql("select sum(p) from jsonTable").collect()
        }

        benchmark.addCase("Partition column - Parquet Vectorized") { _ =>
          spark.sql("select sum(p) from parquetTable").collect()
        }

        benchmark.addCase("Partition column - Parquet MR") { _ =>
          withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false") {
            spark.sql("select sum(p) from parquetTable").collect()
          }
        }

        benchmark.addCase("Partition column - ORC Vectorized") { _ =>
          spark.sql("SELECT sum(p) FROM orcTable").collect()
        }

        benchmark.addCase("Partition column - ORC Vectorized with copy") { _ =>
          withSQLConf(SQLConf.ORC_COPY_BATCH_TO_SPARK.key -> "true") {
            spark.sql("SELECT sum(p) FROM orcTable").collect()
          }
        }

        benchmark.addCase("Partition column - ORC MR") { _ =>
          withSQLConf(SQLConf.ORC_VECTORIZED_READER_ENABLED.key -> "false") {
            spark.sql("SELECT sum(p) FROM orcTable").collect()
          }
        }

        benchmark.addCase("Both columns - CSV") { _ =>
          spark.sql("select sum(p), sum(id) from csvTable").collect()
        }

        benchmark.addCase("Both columns - Json") { _ =>
          spark.sql("select sum(p), sum(id) from jsonTable").collect()
        }

        benchmark.addCase("Both columns - Parquet Vectorized") { _ =>
          spark.sql("select sum(p), sum(id) from parquetTable").collect()
        }

        benchmark.addCase("Both columns - Parquet MR") { _ =>
          withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false") {
            spark.sql("select sum(p), sum(id) from parquetTable").collect
          }
        }

        benchmark.addCase("Both columns - ORC Vectorized") { _ =>
          spark.sql("SELECT sum(p), sum(id) FROM orcTable").collect()
        }

        benchmark.addCase("Both column - ORC Vectorized with copy") { _ =>
          withSQLConf(SQLConf.ORC_COPY_BATCH_TO_SPARK.key -> "true") {
            spark.sql("SELECT sum(p), sum(id) FROM orcTable").collect()
          }
        }

        benchmark.addCase("Both columns - ORC MR") { _ =>
          withSQLConf(SQLConf.ORC_VECTORIZED_READER_ENABLED.key -> "false") {
            spark.sql("SELECT sum(p), sum(id) FROM orcTable").collect()
          }
        }

        /*
        OpenJDK 64-Bit Server VM 1.8.0_171-b10 on Linux 4.14.33-51.37.amzn1.x86_64
        Intel(R) Xeon(R) CPU E5-2670 v2 @ 2.50GHz
        Partitioned Table:                   Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        --------------------------------------------------------------------------------------------
        Data column - CSV                       32613 / 32841          0.5        2073.4       1.0X
        Data column - Json                      13343 / 13469          1.2         848.3       2.4X
        Data column - Parquet Vectorized           302 /  318         52.1          19.2     108.0X
        Data column - Parquet MR                  2908 / 2924          5.4         184.9      11.2X
        Data column - ORC Vectorized               412 /  425         38.1          26.2      79.1X
        Data column - ORC Vectorized with copy     442 /  446         35.6          28.1      73.8X
        Data column - ORC MR                      2390 / 2396          6.6         152.0      13.6X
        Partition column - CSV                    9626 / 9683          1.6         612.0       3.4X
        Partition column - Json                 10909 / 10923          1.4         693.6       3.0X
        Partition column - Parquet Vectorized       69 /   76        228.4           4.4     473.6X
        Partition column - Parquet MR             1898 / 1933          8.3         120.7      17.2X
        Partition column - ORC Vectorized           67 /   74        236.0           4.2     489.4X
        Partition column - ORC Vectorized with copy 65 /   72        241.9           4.1     501.6X
        Partition column - ORC MR                 1743 / 1749          9.0         110.8      18.7X
        Both columns - CSV                      35523 / 35552          0.4        2258.5       0.9X
        Both columns - Json                     13676 / 13681          1.2         869.5       2.4X
        Both columns - Parquet Vectorized          317 /  326         49.5          20.2     102.7X
        Both columns - Parquet MR                 3333 / 3336          4.7         211.9       9.8X
        Both columns - ORC Vectorized              441 /  446         35.6          28.1      73.9X
        Both column - ORC Vectorized with copy     517 /  524         30.4          32.9      63.1X
        Both columns - ORC MR                     2574 / 2577          6.1         163.6      12.7X
        */
        benchmark.run()
      }
    }
  }

  def stringWithNullsScanBenchmark(values: Int, fractionOfNulls: Double): Unit = {
    val benchmark = new Benchmark("String with Nulls Scan", values)

    withTempPath { dir =>
      withTempTable("t1", "csvTable", "jsonTable", "parquetTable", "orcTable") {
        spark.range(values).createOrReplaceTempView("t1")

        prepareTable(
          dir,
          spark.sql(
            s"SELECT IF(RAND(1) < $fractionOfNulls, NULL, CAST(id as STRING)) AS c1, " +
            s"IF(RAND(2) < $fractionOfNulls, NULL, CAST(id as STRING)) AS c2 FROM t1"))

        benchmark.addCase("SQL CSV") { _ =>
          spark.sql("select sum(length(c2)) from csvTable where c1 is " +
            "not NULL and c2 is not NULL").collect()
        }

        benchmark.addCase("SQL Json") { _ =>
          spark.sql("select sum(length(c2)) from jsonTable where c1 is " +
            "not NULL and c2 is not NULL").collect()
        }

        benchmark.addCase("SQL Parquet Vectorized") { _ =>
          spark.sql("select sum(length(c2)) from parquetTable where c1 is " +
            "not NULL and c2 is not NULL").collect()
        }

        benchmark.addCase("SQL Parquet MR") { _ =>
          withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false") {
            spark.sql("select sum(length(c2)) from parquetTable where c1 is " +
              "not NULL and c2 is not NULL").collect()
          }
        }

        val files = SpecificParquetRecordReaderBase.listDirectory(new File(dir, "parquet")).toArray
        val enableOffHeapColumnVector = spark.sessionState.conf.offHeapColumnVectorEnabled
        val vectorizedReaderBatchSize = spark.sessionState.conf.parquetVectorizedReaderBatchSize
        benchmark.addCase("ParquetReader Vectorized") { num =>
          var sum = 0
          files.map(_.asInstanceOf[String]).foreach { p =>
            val reader = new VectorizedParquetRecordReader(
              null, enableOffHeapColumnVector, vectorizedReaderBatchSize)
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

        benchmark.addCase("SQL ORC Vectorized") { _ =>
          spark.sql("SELECT SUM(LENGTH(c2)) FROM orcTable " +
            "WHERE c1 IS NOT NULL AND c2 IS NOT NULL").collect()
        }

        benchmark.addCase("SQL ORC Vectorized with copy") { _ =>
          withSQLConf(SQLConf.ORC_COPY_BATCH_TO_SPARK.key -> "true") {
            spark.sql("SELECT SUM(LENGTH(c2)) FROM orcTable " +
              "WHERE c1 IS NOT NULL AND c2 IS NOT NULL").collect()
          }
        }

        benchmark.addCase("SQL ORC MR") { _ =>
          withSQLConf(SQLConf.ORC_VECTORIZED_READER_ENABLED.key -> "false") {
            spark.sql("SELECT SUM(LENGTH(c2)) FROM orcTable " +
              "WHERE c1 IS NOT NULL AND c2 IS NOT NULL").collect()
          }
        }

        /*
        OpenJDK 64-Bit Server VM 1.8.0_171-b10 on Linux 4.14.33-51.37.amzn1.x86_64
        Intel(R) Xeon(R) CPU E5-2670 v2 @ 2.50GHz
        String with Nulls Scan:              Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        --------------------------------------------------------------------------------------------
        SQL CSV                                 14875 / 14920          0.7        1418.6       1.0X
        SQL Json                                10974 / 10992          1.0        1046.5       1.4X
        SQL Parquet Vectorized                    1711 / 1750          6.1         163.2       8.7X
        SQL Parquet MR                            3838 / 3884          2.7         366.0       3.9X
        ParquetReader Vectorized                  1155 / 1168          9.1         110.2      12.9X
        SQL ORC Vectorized                        1341 / 1380          7.8         127.9      11.1X
        SQL ORC Vectorized with copy              1659 / 1716          6.3         158.2       9.0X
        SQL ORC MR                                3594 / 3634          2.9         342.7       4.1X


        String with Nulls Scan:              Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        --------------------------------------------------------------------------------------------
        SQL CSV                                 17219 / 17264          0.6        1642.1       1.0X
        SQL Json                                  8843 / 8864          1.2         843.3       1.9X
        SQL Parquet Vectorized                    1169 / 1178          9.0         111.4      14.7X
        SQL Parquet MR                            2676 / 2697          3.9         255.2       6.4X
        ParquetReader Vectorized                  1068 / 1071          9.8         101.8      16.1X
        SQL ORC Vectorized                        1319 / 1319          7.9         125.8      13.1X
        SQL ORC Vectorized with copy              1638 / 1639          6.4         156.2      10.5X
        SQL ORC MR                                3230 / 3257          3.2         308.1       5.3X


        String with Nulls Scan:              Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        --------------------------------------------------------------------------------------------
        SQL CSV                                 13976 / 14053          0.8        1332.8       1.0X
        SQL Json                                  5166 / 5176          2.0         492.6       2.7X
        SQL Parquet Vectorized                     274 /  282         38.2          26.2      50.9X
        SQL Parquet MR                            1553 / 1555          6.8         148.1       9.0X
        ParquetReader Vectorized                   241 /  246         43.5          23.0      57.9X
        SQL ORC Vectorized                         476 /  479         22.0          45.4      29.3X
        SQL ORC Vectorized with copy               584 /  588         17.9          55.7      23.9X
        SQL ORC MR                                1720 / 1734          6.1         164.1       8.1X
        */
        benchmark.run()
      }
    }
  }

  def columnsBenchmark(values: Int, width: Int): Unit = {
    val benchmark = new Benchmark(s"Single Column Scan from $width columns", values)

    withTempPath { dir =>
      withTempTable("t1", "csvTable", "jsonTable", "parquetTable", "orcTable") {
        import spark.implicits._
        val middle = width / 2
        val selectExpr = (1 to width).map(i => s"value as c$i")
        spark.range(values).map(_ => Random.nextLong).toDF()
          .selectExpr(selectExpr: _*).createOrReplaceTempView("t1")

        prepareTable(dir, spark.sql("SELECT * FROM t1"))

        benchmark.addCase("SQL CSV") { _ =>
          spark.sql(s"SELECT sum(c$middle) FROM csvTable").collect()
        }

        benchmark.addCase("SQL Json") { _ =>
          spark.sql(s"SELECT sum(c$middle) FROM jsonTable").collect()
        }

        benchmark.addCase("SQL Parquet Vectorized") { _ =>
          spark.sql(s"SELECT sum(c$middle) FROM parquetTable").collect()
        }

        benchmark.addCase("SQL Parquet MR") { _ =>
          withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false") {
            spark.sql(s"SELECT sum(c$middle) FROM parquetTable").collect()
          }
        }

        benchmark.addCase("SQL ORC Vectorized") { _ =>
          spark.sql(s"SELECT sum(c$middle) FROM orcTable").collect()
        }

        benchmark.addCase("SQL ORC Vectorized with copy") { _ =>
          withSQLConf(SQLConf.ORC_COPY_BATCH_TO_SPARK.key -> "true") {
            spark.sql(s"SELECT sum(c$middle) FROM orcTable").collect()
          }
        }

        benchmark.addCase("SQL ORC MR") { _ =>
          withSQLConf(SQLConf.ORC_VECTORIZED_READER_ENABLED.key -> "false") {
            spark.sql(s"SELECT sum(c$middle) FROM orcTable").collect()
          }
        }

        /*
        OpenJDK 64-Bit Server VM 1.8.0_171-b10 on Linux 4.14.33-51.37.amzn1.x86_64
        Intel(R) Xeon(R) CPU E5-2670 v2 @ 2.50GHz
        Single Column Scan from 10 columns:  Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        --------------------------------------------------------------------------------------------
        SQL CSV                                   3478 / 3481          0.3        3316.4       1.0X
        SQL Json                                  2646 / 2654          0.4        2523.6       1.3X
        SQL Parquet Vectorized                      67 /   72         15.8          63.5      52.2X
        SQL Parquet MR                             207 /  214          5.1         197.6      16.8X
        SQL ORC Vectorized                          69 /   76         15.2          66.0      50.3X
        SQL ORC Vectorized with copy                70 /   76         15.0          66.5      49.9X
        SQL ORC MR                                 299 /  303          3.5         285.1      11.6X


        Single Column Scan from 50 columns:  Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        --------------------------------------------------------------------------------------------
        SQL CSV                                   9214 / 9236          0.1        8786.7       1.0X
        SQL Json                                  9943 / 9978          0.1        9482.7       0.9X
        SQL Parquet Vectorized                      77 /   86         13.6          73.3     119.8X
        SQL Parquet MR                             229 /  235          4.6         218.6      40.2X
        SQL ORC Vectorized                          84 /   96         12.5          80.0     109.9X
        SQL ORC Vectorized with copy                83 /   91         12.6          79.4     110.7X
        SQL ORC MR                                 843 /  854          1.2         804.0      10.9X


        Single Column Scan from 100 columns  Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        --------------------------------------------------------------------------------------------
        SQL CSV                                 16503 / 16622          0.1       15738.9       1.0X
        SQL Json                                19109 / 19184          0.1       18224.2       0.9X
        SQL Parquet Vectorized                      99 /  108         10.6          94.3     166.8X
        SQL Parquet MR                             253 /  264          4.1         241.6      65.1X
        SQL ORC Vectorized                         107 /  114          9.8         101.6     154.8X
        SQL ORC Vectorized with copy               107 /  118          9.8         102.1     154.1X
        SQL ORC MR                                1526 / 1529          0.7        1455.3      10.8X
        */
        benchmark.run()
      }
    }
  }

  def main(args: Array[String]): Unit = {
    Seq(ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType).foreach { dataType =>
      numericScanBenchmark(1024 * 1024 * 15, dataType)
    }
    intStringScanBenchmark(1024 * 1024 * 10)
    repeatedStringScanBenchmark(1024 * 1024 * 10)
    partitionTableScanBenchmark(1024 * 1024 * 15)
    for (fractionOfNulls <- List(0.0, 0.50, 0.95)) {
      stringWithNullsScanBenchmark(1024 * 1024 * 10, fractionOfNulls)
    }
    for (columnWidth <- List(10, 50, 100)) {
      columnsBenchmark(1024 * 1024 * 1, columnWidth)
    }
  }
}
