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
    .setIfMissing("spark.master", "local[1]")
    .setIfMissing("spark.driver.memory", "3g")
    .setIfMissing("spark.executor.memory", "3g")

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
        Intel(R) Xeon(R) CPU E5-2686 v4 @ 2.30GHz
        SQL Single TINYINT Column Scan:      Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        --------------------------------------------------------------------------------------------
        SQL CSV                                 15231 / 15267          1.0         968.3       1.0X
        SQL Json                                  8476 / 8498          1.9         538.9       1.8X
        SQL Parquet Vectorized                     121 /  127        130.0           7.7     125.9X
        SQL Parquet MR                            1515 / 1543         10.4          96.3      10.1X
        SQL ORC Vectorized                         164 /  171         95.9          10.4      92.9X
        SQL ORC Vectorized with copy               228 /  234         69.0          14.5      66.8X
        SQL ORC MR                                1297 / 1309         12.1          82.5      11.7X


        SQL Single SMALLINT Column Scan:     Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        --------------------------------------------------------------------------------------------
        SQL CSV                                 16344 / 16374          1.0        1039.1       1.0X
        SQL Json                                  8634 / 8648          1.8         548.9       1.9X
        SQL Parquet Vectorized                     172 /  177         91.5          10.9      95.1X
        SQL Parquet MR                            1744 / 1746          9.0         110.9       9.4X
        SQL ORC Vectorized                         189 /  194         83.1          12.0      86.4X
        SQL ORC Vectorized with copy               244 /  250         64.5          15.5      67.0X
        SQL ORC MR                                1341 / 1386         11.7          85.3      12.2X


        SQL Single INT Column Scan:          Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        --------------------------------------------------------------------------------------------
        SQL CSV                                 17874 / 17875          0.9        1136.4       1.0X
        SQL Json                                  9190 / 9204          1.7         584.3       1.9X
        SQL Parquet Vectorized                     141 /  160        111.2           9.0     126.4X
        SQL Parquet MR                            1930 / 2049          8.2         122.7       9.3X
        SQL ORC Vectorized                         259 /  264         60.7          16.5      69.0X
        SQL ORC Vectorized with copy               265 /  272         59.4          16.8      67.5X
        SQL ORC MR                                1528 / 1569         10.3          97.2      11.7X


        SQL Single BIGINT Column Scan:       Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        --------------------------------------------------------------------------------------------
        SQL CSV                                 22812 / 22839          0.7        1450.4       1.0X
        SQL Json                                12026 / 12054          1.3         764.6       1.9X
        SQL Parquet Vectorized                     222 /  227         70.8          14.1     102.6X
        SQL Parquet MR                            2199 / 2204          7.2         139.8      10.4X
        SQL ORC Vectorized                         331 /  335         47.6          21.0      69.0X
        SQL ORC Vectorized with copy               338 /  343         46.6          21.5      67.6X
        SQL ORC MR                                1618 / 1622          9.7         102.9      14.1X


        SQL Single FLOAT Column Scan:        Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        --------------------------------------------------------------------------------------------
        SQL CSV                                 18703 / 18740          0.8        1189.1       1.0X
        SQL Json                                11779 / 11869          1.3         748.9       1.6X
        SQL Parquet Vectorized                     143 /  145        110.1           9.1     130.9X
        SQL Parquet MR                            1954 / 1963          8.0         124.2       9.6X
        SQL ORC Vectorized                         347 /  355         45.3          22.1      53.8X
        SQL ORC Vectorized with copy               356 /  359         44.1          22.7      52.5X
        SQL ORC MR                                1570 / 1598         10.0          99.8      11.9X


        SQL Single DOUBLE Column Scan:       Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        --------------------------------------------------------------------------------------------
        SQL CSV                                 23832 / 23838          0.7        1515.2       1.0X
        SQL Json                                16204 / 16226          1.0        1030.2       1.5X
        SQL Parquet Vectorized                     242 /  306         65.1          15.4      98.6X
        SQL Parquet MR                            2462 / 2482          6.4         156.5       9.7X
        SQL ORC Vectorized                         419 /  451         37.6          26.6      56.9X
        SQL ORC Vectorized with copy               426 /  447         36.9          27.1      55.9X
        SQL ORC MR                                1885 / 1931          8.3         119.8      12.6X
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
        Intel(R) Xeon(R) CPU E5-2686 v4 @ 2.30GHz
        Single TINYINT Column Scan:          Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        --------------------------------------------------------------------------------------------
        ParquetReader Vectorized                   187 /  201         84.2          11.9       1.0X
        ParquetReader Vectorized -> Row            101 /  103        156.4           6.4       1.9X


        Single SMALLINT Column Scan:         Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        --------------------------------------------------------------------------------------------
        ParquetReader Vectorized                   272 /  288         57.8          17.3       1.0X
        ParquetReader Vectorized -> Row            213 /  219         73.7          13.6       1.3X


        Single INT Column Scan:              Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        --------------------------------------------------------------------------------------------
        ParquetReader Vectorized                   252 /  288         62.5          16.0       1.0X
        ParquetReader Vectorized -> Row            232 /  246         67.7          14.8       1.1X


        Single BIGINT Column Scan:           Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        --------------------------------------------------------------------------------------------
        ParquetReader Vectorized                   415 /  454         37.9          26.4       1.0X
        ParquetReader Vectorized -> Row            407 /  432         38.6          25.9       1.0X


        Single FLOAT Column Scan:            Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        --------------------------------------------------------------------------------------------
        ParquetReader Vectorized                   251 /  302         62.7          16.0       1.0X
        ParquetReader Vectorized -> Row            220 /  234         71.5          14.0       1.1X


        Single DOUBLE Column Scan:           Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        --------------------------------------------------------------------------------------------
        ParquetReader Vectorized                   432 /  436         36.4          27.5       1.0X
        ParquetReader Vectorized -> Row            414 /  422         38.0          26.4       1.0X
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
        Intel(R) Xeon(R) CPU E5-2686 v4 @ 2.30GHz
        Int and String Scan:                 Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        --------------------------------------------------------------------------------------------
        SQL CSV                                 19172 / 19173          0.5        1828.4       1.0X
        SQL Json                                12799 / 12873          0.8        1220.6       1.5X
        SQL Parquet Vectorized                    2558 / 2564          4.1         244.0       7.5X
        SQL Parquet MR                            4514 / 4583          2.3         430.4       4.2X
        SQL ORC Vectorized                        2561 / 2697          4.1         244.3       7.5X
        SQL ORC Vectorized with copy              3076 / 3110          3.4         293.4       6.2X
        SQL ORC MR                                4197 / 4283          2.5         400.2       4.6X
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
        Intel(R) Xeon(R) CPU E5-2686 v4 @ 2.30GHz
        Repeated String:                     Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        --------------------------------------------------------------------------------------------
        SQL CSV                                 10889 / 10924          1.0        1038.5       1.0X
        SQL Json                                  7903 / 7931          1.3         753.7       1.4X
        SQL Parquet Vectorized                     777 /  799         13.5          74.1      14.0X
        SQL Parquet MR                            1682 / 1708          6.2         160.4       6.5X
        SQL ORC Vectorized                         532 /  534         19.7          50.7      20.5X
        SQL ORC Vectorized with copy               742 /  743         14.1          70.7      14.7X
        SQL ORC MR                                1996 / 2002          5.3         190.4       5.5X
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
        Intel(R) Xeon(R) CPU E5-2686 v4 @ 2.30GHz
        Partitioned Table:                   Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        --------------------------------------------------------------------------------------------
        Data column - CSV                       25428 / 25454          0.6        1616.7       1.0X
        Data column - Json                      12689 / 12774          1.2         806.7       2.0X
        Data column - Parquet Vectorized           222 /  231         70.7          14.1     114.3X
        Data column - Parquet MR                  3355 / 3397          4.7         213.3       7.6X
        Data column - ORC Vectorized               332 /  338         47.4          21.1      76.6X
        Data column - ORC Vectorized with copy     338 /  341         46.5          21.5      75.2X
        Data column - ORC MR                      2329 / 2356          6.8         148.0      10.9X
        Partition column - CSV                  17465 / 17502          0.9        1110.4       1.5X
        Partition column - Json                 10865 / 10876          1.4         690.8       2.3X
        Partition column - Parquet Vectorized       48 /   52        325.4           3.1     526.1X
        Partition column - Parquet MR             1695 / 1696          9.3         107.8      15.0X
        Partition column - ORC Vectorized           49 /   54        319.9           3.1     517.2X
        Partition column - ORC Vectorized with copy 49 /   52        324.1           3.1     524.0X
        Partition column - ORC MR                 1548 / 1549         10.2          98.4      16.4X
        Both columns - CSV                      25568 / 25595          0.6        1625.6       1.0X
        Both columns - Json                     13658 / 13673          1.2         868.4       1.9X
        Both columns - Parquet Vectorized          270 /  296         58.3          17.1      94.3X
        Both columns - Parquet MR                 3501 / 3521          4.5         222.6       7.3X
        Both columns - ORC Vectorized              377 /  380         41.7          24.0      67.4X
        Both column - ORC Vectorized with copy     447 /  448         35.2          28.4      56.9X
        Both columns - ORC MR                     2440 / 2446          6.4         155.2      10.4X
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
        Intel(R) Xeon(R) CPU E5-2686 v4 @ 2.30GHz
        String with Nulls Scan:              Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        --------------------------------------------------------------------------------------------
        SQL CSV                                 13518 / 13529          0.8        1289.2       1.0X
        SQL Json                                10895 / 10926          1.0        1039.0       1.2X
        SQL Parquet Vectorized                    1539 / 1581          6.8         146.8       8.8X
        SQL Parquet MR                            3746 / 3811          2.8         357.3       3.6X
        ParquetReader Vectorized                  1070 / 1112          9.8         102.0      12.6X
        SQL ORC Vectorized                        1389 / 1408          7.6         132.4       9.7X
        SQL ORC Vectorized with copy              1736 / 1750          6.0         165.6       7.8X
        SQL ORC MR                                3799 / 3892          2.8         362.3       3.6X


        String with Nulls Scan:              Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        --------------------------------------------------------------------------------------------
        SQL CSV                                 10854 / 10892          1.0        1035.2       1.0X
        SQL Json                                  8129 / 8138          1.3         775.3       1.3X
        SQL Parquet Vectorized                    1053 / 1104         10.0         100.4      10.3X
        SQL Parquet MR                            2840 / 2854          3.7         270.8       3.8X
        ParquetReader Vectorized                   978 / 1008         10.7          93.2      11.1X
        SQL ORC Vectorized                        1312 / 1387          8.0         125.1       8.3X
        SQL ORC Vectorized with copy              1764 / 1772          5.9         168.2       6.2X
        SQL ORC MR                                3435 / 3445          3.1         327.6       3.2X


        String with Nulls Scan:              Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        --------------------------------------------------------------------------------------------
        SQL CSV                                   8043 / 8048          1.3         767.1       1.0X
        SQL Json                                  4911 / 4923          2.1         468.4       1.6X
        SQL Parquet Vectorized                     206 /  209         51.0          19.6      39.1X
        SQL Parquet MR                            1528 / 1537          6.9         145.8       5.3X
        ParquetReader Vectorized                   216 /  219         48.6          20.6      37.2X
        SQL ORC Vectorized                         462 /  466         22.7          44.1      17.4X
        SQL ORC Vectorized with copy               568 /  572         18.5          54.2      14.2X
        SQL ORC MR                                1647 / 1649          6.4         157.1       4.9X
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
        Intel(R) Xeon(R) CPU E5-2686 v4 @ 2.30GHz
        Single Column Scan from 10 columns:  Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        --------------------------------------------------------------------------------------------
        SQL CSV                                   3663 / 3665          0.3        3493.2       1.0X
        SQL Json                                  3122 / 3160          0.3        2977.5       1.2X
        SQL Parquet Vectorized                      40 /   42         26.2          38.2      91.5X
        SQL Parquet MR                             189 /  192          5.5         180.2      19.4X
        SQL ORC Vectorized                          48 /   51         21.6          46.2      75.6X
        SQL ORC Vectorized with copy                49 /   52         21.4          46.7      74.9X
        SQL ORC MR                                 280 /  289          3.7         267.1      13.1X


        Single Column Scan from 50 columns:  Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        --------------------------------------------------------------------------------------------
        SQL CSV                                 11420 / 11505          0.1       10891.1       1.0X
        SQL Json                                11905 / 12120          0.1       11353.6       1.0X
        SQL Parquet Vectorized                      50 /   54         20.9          47.8     227.7X
        SQL Parquet MR                             195 /  199          5.4         185.8      58.6X
        SQL ORC Vectorized                          61 /   65         17.3          57.8     188.3X
        SQL ORC Vectorized with copy                62 /   65         17.0          58.8     185.2X
        SQL ORC MR                                 847 /  865          1.2         807.4      13.5X


        Single Column Scan from 100 columns: Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        --------------------------------------------------------------------------------------------
        SQL CSV                                 21278 / 21404          0.0       20292.4       1.0X
        SQL Json                                22455 / 22625          0.0       21414.7       0.9X
        SQL Parquet Vectorized                      73 /   75         14.4          69.3     292.8X
        SQL Parquet MR                             220 /  226          4.8         209.7      96.8X
        SQL ORC Vectorized                          82 /   86         12.8          78.2     259.4X
        SQL ORC Vectorized with copy                82 /   90         12.7          78.7     258.0X
        SQL ORC MR                                1568 / 1582          0.7        1495.4      13.6X
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
