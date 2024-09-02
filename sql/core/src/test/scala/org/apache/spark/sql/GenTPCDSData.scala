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

import java.util.concurrent.LinkedBlockingQueue

import scala.sys.process._
import scala.util.Try

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, rpad}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{CharType, StringType, StructField, StructType, VarcharType}
import org.apache.spark.util.ArrayImplicits._

// The classes in this file are basically moved from https://github.com/databricks/spark-sql-perf

/**
 * Using ProcessBuilder.lineStream produces a stream, that uses
 * a LinkedBlockingQueue with a default capacity of Integer.MAX_VALUE.
 *
 * This causes OOM if the consumer cannot keep up with the producer.
 *
 * See scala.sys.process.ProcessBuilderImpl.lineStream
 */
object BlockingLineStream {

  // See scala.sys.process.Streamed
  private final class BlockingStreamed[T](
    val process: T => Unit,
    val done: Int => Unit,
    val stream: () => LazyList[T])

  // See scala.sys.process.Streamed
  private object BlockingStreamed {
    // scala.process.sys.Streamed uses default of Integer.MAX_VALUE,
    // which causes OOMs if the consumer cannot keep up with producer.
    val maxQueueSize = 65536

    def apply[T](nonzeroException: Boolean): BlockingStreamed[T] = {
      val q = new LinkedBlockingQueue[Either[Int, T]](maxQueueSize)

      def next(): LazyList[T] = q.take match {
        case Left(0) => LazyList.empty
        case Left(code) =>
          if (nonzeroException) scala.sys.error("Nonzero exit code: " + code) else LazyList.empty
        case Right(s) => LazyList.cons(s, next())
      }

      new BlockingStreamed((s: T) => q put Right(s), code => q put Left(code), () => next())
    }
  }

  // See scala.sys.process.ProcessImpl.Spawn
  private object Spawn {
    def apply(f: => Unit): Thread = apply(f, daemon = false)
    def apply(f: => Unit, daemon: Boolean): Thread = {
      val thread = new Thread() { override def run() = { f } }
      thread.setDaemon(daemon)
      thread.start()
      thread
    }
  }

  def apply(command: Seq[String]): LazyList[String] = {
    val streamed = BlockingStreamed[String](true)
    val process = command.run(BasicIO(false, streamed.process, None))
    Spawn(streamed.done(process.exitValue()))
    streamed.stream()
  }
}

class Dsdgen(dsdgenDir: String) extends Serializable {
  private val dsdgen = s"$dsdgenDir/dsdgen"

  def generate(
      sparkContext: SparkContext,
      tableName: String,
      partitions: Int,
      scaleFactor: Int): RDD[String] = {
    val generatedData = {
      sparkContext.parallelize(1 to partitions, partitions).flatMap { i =>
        val localToolsDir = if (new java.io.File(dsdgen).exists) {
          dsdgenDir
        } else if (new java.io.File(s"/$dsdgen").exists) {
          s"/$dsdgenDir"
        } else {
          throw new IllegalStateException(
            s"Could not find dsdgen at $dsdgen or /$dsdgen. Run install")
        }

        // NOTE: RNGSEED is the RNG seed used by the data generator. Right now, it is fixed to 100.
        val parallel = if (partitions > 1) s"-parallel $partitions -child $i" else ""
        val commands = Seq(
          "bash", "-c",
          s"cd $localToolsDir && ./dsdgen -table $tableName -filter Y -scale $scaleFactor " +
          s"-RNGSEED 100 $parallel")
        BlockingLineStream(commands)
      }
    }

    generatedData.setName(s"$tableName, sf=$scaleFactor, strings")
    generatedData
  }
}

class TPCDSTables(spark: SparkSession, dsdgenDir: String, scaleFactor: Int)
  extends TPCDSSchema with Logging with Serializable {

  private val dataGenerator = new Dsdgen(dsdgenDir)

  private def tables: Seq[Table] = tableColumns.map { case (tableName, schemaString) =>
    val partitionColumns = tablePartitionColumns.getOrElse(tableName, Nil)
      .map(_.stripPrefix("`").stripSuffix("`"))
    Table(tableName, partitionColumns, StructType.fromDDL(schemaString))
  }.toSeq

  private case class Table(name: String, partitionColumns: Seq[String], schema: StructType) {
    def nonPartitioned: Table = {
      Table(name, Nil, schema)
    }

    private def df(numPartition: Int) = {
      val generatedData = dataGenerator.generate(
        spark.sparkContext, name, numPartition, scaleFactor)
      val rows = generatedData.mapPartitions { iter =>
        iter.map { l =>
          val values = l.split("\\|", -1).dropRight(1).map { v =>
            if (v.equals("")) {
              // If the string value is an empty string, we turn it to a null
              null
            } else {
              v
            }
          }
          Row.fromSeq(values.toImmutableArraySeq)
        }
      }

      val stringData =
        spark.createDataFrame(
          rows,
          StructType(schema.fields.map(f => StructField(f.name, StringType))))

      val convertedData = {
        val columns = schema.fields.map { f =>
          val c = f.dataType match {
            // Needs right-padding for char types
            case CharType(n) => rpad(Column(f.name), n, " ")
            // Don't need a cast for varchar types
            case _: VarcharType => col(f.name)
            case _ => col(f.name).cast(f.dataType)
          }
          c.as(f.name)
        }
        import org.apache.spark.util.ArrayImplicits._
        stringData.select(columns.toImmutableArraySeq: _*)
      }

      convertedData
    }

    def genData(
        location: String,
        format: String,
        overwrite: Boolean,
        clusterByPartitionColumns: Boolean,
        filterOutNullPartitionValues: Boolean,
        numPartitions: Int): Unit = {
      val mode = if (overwrite) SaveMode.Overwrite else SaveMode.Ignore

      val data = df(numPartitions)
      val tempTableName = s"${name}_text"
      data.createOrReplaceTempView(tempTableName)

      val writer = if (partitionColumns.nonEmpty) {
        if (clusterByPartitionColumns) {
          val columnString = data.schema.fields.map { field =>
            field.name
          }.mkString(",")
          val partitionColumnString = partitionColumns.mkString(",")
          val predicates = if (filterOutNullPartitionValues) {
            partitionColumns.map(col => s"$col IS NOT NULL").mkString("WHERE ", " AND ", "")
          } else {
            ""
          }

          val query =
            s"""
               |SELECT
               |  $columnString
               |FROM
               |  $tempTableName
               |$predicates
               |DISTRIBUTE BY
               |  $partitionColumnString
            """.stripMargin
          val grouped = spark.sql(query)
          logInfo(s"Pre-clustering with partitioning columns with query $query.")
          grouped.write
        } else {
          data.write
        }
      } else {
        // treat non-partitioned tables as "one partition" that we want to coalesce
        if (clusterByPartitionColumns) {
          // in case data has more than maxRecordsPerFile, split into multiple writers to improve
          // datagen speed files will be truncated to maxRecordsPerFile value, so the final
          // result will be the same.
          val numRows = data.count()
          val maxRecordPerFile = spark.conf.get(SQLConf.MAX_RECORDS_PER_FILE)

          if (maxRecordPerFile > 0 && numRows > maxRecordPerFile) {
            val numFiles = (numRows.toDouble/maxRecordPerFile).ceil.toInt
            logInfo(s"Coalescing into $numFiles files")
            data.coalesce(numFiles).write
          } else {
            data.coalesce(1).write
          }
        } else {
          data.write
        }
      }
      writer.format(format).mode(mode)
      if (partitionColumns.nonEmpty) {
        writer.partitionBy(partitionColumns: _*)
      }
      logInfo(s"Generating table $name in database to $location with save mode $mode.")
      writer.save(location)
      spark.catalog.dropTempView(tempTableName)
    }
  }

  def genData(
      location: String,
      format: String,
      overwrite: Boolean,
      partitionTables: Boolean,
      clusterByPartitionColumns: Boolean,
      filterOutNullPartitionValues: Boolean,
      tableFilter: String = "",
      numPartitions: Int = 100): Unit = {
    var tablesToBeGenerated = if (partitionTables) {
      tables
    } else {
      tables.map(_.nonPartitioned)
    }

    if (!tableFilter.isEmpty) {
      tablesToBeGenerated = tablesToBeGenerated.filter(_.name == tableFilter)
      if (tablesToBeGenerated.isEmpty) {
        throw new RuntimeException("Bad table name filter: " + tableFilter)
      }
    }

    tablesToBeGenerated.foreach { table =>
      val tableLocation = s"$location/${table.name}"
      table.genData(tableLocation, format, overwrite, clusterByPartitionColumns,
        filterOutNullPartitionValues, numPartitions)
    }
  }
}

class GenTPCDSDataConfig(args: Array[String]) {
  var master: String = "local[*]"
  var dsdgenDir: String = null
  var location: String = null
  var scaleFactor: Int = 1
  var format: String = "parquet"
  var overwrite: Boolean = false
  var partitionTables: Boolean = false
  var clusterByPartitionColumns: Boolean = false
  var filterOutNullPartitionValues: Boolean = false
  var tableFilter: String = ""
  var numPartitions: Int = 100

  parseArgs(args.toList)

  private def parseArgs(inputArgs: List[String]): Unit = {
    var args = inputArgs

    while (args.nonEmpty) {
      args match {
        case "--master" :: value :: tail =>
          master = value
          args = tail

        case "--dsdgenDir" :: value :: tail =>
          dsdgenDir = value
          args = tail

        case "--location" :: value :: tail =>
          location = value
          args = tail

        case "--scaleFactor" :: value :: tail =>
          scaleFactor = toPositiveIntValue("Scale factor", value)
          args = tail

        case "--format" :: value :: tail =>
          format = value
          args = tail

        case "--overwrite" :: tail =>
          overwrite = true
          args = tail

        case "--partitionTables" :: tail =>
          partitionTables = true
          args = tail

        case "--clusterByPartitionColumns" :: tail =>
          clusterByPartitionColumns = true
          args = tail

        case "--filterOutNullPartitionValues" :: tail =>
          filterOutNullPartitionValues = true
          args = tail

        case "--tableFilter" :: value :: tail =>
          tableFilter = value
          args = tail

        case "--numPartitions" :: value :: tail =>
          numPartitions = toPositiveIntValue("Number of partitions", value)
          args = tail

        case "--help" :: tail =>
          printUsageAndExit(0)

        case _ =>
          // scalastyle:off println
          System.err.println("Unknown/unsupported param " + args)
          // scalastyle:on println
          printUsageAndExit(1)
      }
    }

    checkRequiredArguments()
  }

  private def printUsageAndExit(exitCode: Int): Unit = {
    // scalastyle:off
    System.err.println("""
      |build/sbt "Test/runMain <this class> [Options]"
      |Options:
      |  --master                        the Spark master to use, default to local[*]
      |  --dsdgenDir                     location of dsdgen
      |  --location                      root directory of location to generate data in
      |  --scaleFactor                   size of the dataset to generate (in GB)
      |  --format                        generated data format, Parquet, ORC ...
      |  --overwrite                     whether to overwrite the data that is already there
      |  --partitionTables               whether to create the partitioned fact tables
      |  --clusterByPartitionColumns     whether to shuffle to get partitions coalesced into single files
      |  --filterOutNullPartitionValues  whether to filter out the partition with NULL key value
      |  --tableFilter                   comma-separated list of table names to generate (e.g., store_sales,store_returns),
      |                                  all the tables are generated by default
      |  --numPartitions                 how many dsdgen partitions to run - number of input tasks
      """.stripMargin)
    // scalastyle:on
    System.exit(exitCode)
  }

  private def toPositiveIntValue(name: String, v: String): Int = {
    if (Try(v.toInt).getOrElse(-1) <= 0) {
      // scalastyle:off println
      System.err.println(s"$name must be a positive number")
      // scalastyle:on println
      printUsageAndExit(-1)
    }
    v.toInt
  }

  private def checkRequiredArguments(): Unit = {
    if (dsdgenDir == null) {
      // scalastyle:off println
      System.err.println("Must specify a dsdgen path")
      // scalastyle:on println
      printUsageAndExit(-1)
    }
    if (location == null) {
      // scalastyle:off println
      System.err.println("Must specify an output location")
      // scalastyle:on println
      printUsageAndExit(-1)
    }
  }
}

/**
 * This class generates TPCDS table data by using tpcds-kit:
 *  - https://github.com/databricks/tpcds-kit
 *
 * To run this:
 * {{{
 *   build/sbt "sql/Test/runMain <this class> --dsdgenDir <path> --location <path> --scaleFactor 1"
 * }}}
 *
 * Note: if users specify a small scale factor, GenTPCDSData works good. Otherwise, may encounter
 * OOM and cause failure. Users can retry by setting a larger value for the environment variable
 * HEAP_SIZE(the default size is 4g), e.g. export HEAP_SIZE=10g.
 */
object GenTPCDSData {

  def main(args: Array[String]): Unit = {
    val config = new GenTPCDSDataConfig(args)

    val spark = SparkSession
      .builder()
      .appName(getClass.getName)
      .master(config.master)
      .getOrCreate()

    val tables = new TPCDSTables(
      spark,
      dsdgenDir = config.dsdgenDir,
      scaleFactor = config.scaleFactor)

    tables.genData(
      location = config.location,
      format = config.format,
      overwrite = config.overwrite,
      partitionTables = config.partitionTables,
      clusterByPartitionColumns = config.clusterByPartitionColumns,
      filterOutNullPartitionValues = config.filterOutNullPartitionValues,
      tableFilter = config.tableFilter,
      numPartitions = config.numPartitions)

    spark.stop()
  }
}
