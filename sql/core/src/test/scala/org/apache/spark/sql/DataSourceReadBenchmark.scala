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
package org.apache.spark.sql

import java.io.File

import scala.collection.JavaConverters._
import scala.util.{Random, Try}

import org.apache.spark.SparkConf
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
    .setMaster("local[1]")
    .setAppName("DataSourceReadBenchmark")
    .set("spark.driver.memory", "3g")
    .set("spark.executor.memory", "3g")
    .set("orc.compression", "snappy")
    .set("spark.sql.parquet.compression.codec", "snappy")

  val spark = SparkSession.builder.config(conf).getOrCreate()

  // Set default configs. Individual cases will change them if necessary.
  spark.conf.set(SQLConf.ORC_FILTER_PUSHDOWN_ENABLED.key, "true")
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
    df.mode("overwrite").parquet(dir)
    spark.read.parquet(dir).createOrReplaceTempView("parquetTable")
  }

  private def saveAsOrcTable(df: DataFrameWriter[Row], dir: String): Unit = {
    df.mode("overwrite").orc(dir)
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
        Intel(R) Core(TM) i7-4870HQ CPU @ 2.50GHz
        SQL Single Int Column Scan:         Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        -------------------------------------------------------------------------------------------
        SQL Parquet Vectorized                    215 /  262         73.0          13.7       1.0X
        SQL Parquet MR                           1946 / 2083          8.1         123.7       0.1X
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
        Intel(R) Core(TM) i7-4960HQ CPU @ 2.60GHz
        String with Nulls Scan (0%):        Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        -------------------------------------------------------------------------------------------
        SQL Parquet Vectorized                   1229 / 1648          8.5         117.2       1.0X
        PR Vectorized                             833 /  846         12.6          79.4       1.5X

        Intel(R) Core(TM) i7-4960HQ CPU @ 2.60GHz
        String with Nulls Scan (50%):       Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        -------------------------------------------------------------------------------------------
        SQL Parquet Vectorized                    995 / 1053         10.5          94.9       1.0X
        PR Vectorized                             732 /  772         14.3          69.8       1.4X

        Intel(R) Core(TM) i7-4960HQ CPU @ 2.60GHz
        String with Nulls Scan (95%):       Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        -------------------------------------------------------------------------------------------
        SQL Parquet Vectorized                    326 /  333         32.2          31.1       1.0X
        PR Vectorized                             190 /  200         55.1          18.2       1.7X
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
        Java HotSpot(TM) 64-Bit Server VM 1.8.0_60-b27 on Mac OS X 10.13.1
        Intel(R) Core(TM) i7-4960HQ CPU @ 2.60GHz

        Single Column Scan from 100 columns: Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        --------------------------------------------------------------------------------------------
        Native ORC MR                             1050 / 1053          1.0        1001.1       1.0X
        Native ORC Vectorized                       95 /  101         11.0          90.9      11.0X
        Native ORC Vectorized with copy             95 /  102         11.0          90.9      11.0X
        Hive built-in ORC                          348 /  358          3.0         331.8       3.0X

        Single Column Scan from 200 columns: Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        --------------------------------------------------------------------------------------------
        Native ORC MR                             2099 / 2108          0.5        2002.1       1.0X
        Native ORC Vectorized                      179 /  187          5.8         171.1      11.7X
        Native ORC Vectorized with copy            176 /  188          6.0         167.6      11.9X
        Hive built-in ORC                          562 /  581          1.9         535.9       3.7X

        Single Column Scan from 300 columns: Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        --------------------------------------------------------------------------------------------
        Native ORC MR                             3221 / 3246          0.3        3071.4       1.0X
        Native ORC Vectorized                      312 /  322          3.4         298.0      10.3X
        Native ORC Vectorized with copy            306 /  320          3.4         291.6      10.5X
        Hive built-in ORC                          815 /  824          1.3         777.3       4.0X
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
