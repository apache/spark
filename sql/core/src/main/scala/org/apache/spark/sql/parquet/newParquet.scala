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
package org.apache.spark.sql.parquet

import java.util.{List => JList}

import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce.{JobContext, InputSplit, Job}

import parquet.hadoop.ParquetInputFormat
import parquet.hadoop.util.ContextUtil

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.{Partition => SparkPartition, Logging}
import org.apache.spark.rdd.{NewHadoopPartition, RDD}

import org.apache.spark.sql.{SQLConf, Row, SQLContext}
import org.apache.spark.sql.catalyst.expressions.{SpecificMutableRow, And, Expression, Attribute}
import org.apache.spark.sql.catalyst.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.sources._

import scala.collection.JavaConversions._

/**
 * Allows creation of parquet based tables using the syntax
 * `CREATE TABLE ... USING org.apache.spark.sql.parquet`.  Currently the only option required
 * is `path`, which should be the location of a collection of, optionally partitioned,
 * parquet files.
 */
class DefaultSource extends RelationProvider {
  /** Returns a new base relation with the given parameters. */
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    val path =
      parameters.getOrElse("path", sys.error("'path' must be specifed for parquet tables."))

    ParquetRelation2(path)(sqlContext)
  }
}

private[parquet] case class Partition(partitionValues: Map[String, Any], files: Seq[FileStatus])

/**
 * An alternative to [[ParquetRelation]] that plugs in using the data sources API.  This class is
 * currently not intended as a full replacement of the parquet support in Spark SQL though it is
 * likely that it will eventually subsume the existing physical plan implementation.
 *
 * Compared with the current implementation, this class has the following notable differences:
 *
 * Partitioning: Partitions are auto discovered and must be in the form of directories `key=value/`
 * located at `path`.  Currently only a single partitioning column is supported and it must
 * be an integer.  This class supports both fully self-describing data, which contains the partition
 * key, and data where the partition key is only present in the folder structure.  The presence
 * of the partitioning key in the data is also auto-detected.  The `null` partition is not yet
 * supported.
 *
 * Metadata: The metadata is automatically discovered by reading the first parquet file present.
 * There is currently no support for working with files that have different schema.  Additionally,
 * when parquet metadata caching is turned on, the FileStatus objects for all data will be cached
 * to improve the speed of interactive querying.  When data is added to a table it must be dropped
 * and recreated to pick up any changes.
 *
 * Statistics: Statistics for the size of the table are automatically populated during metadata
 * discovery.
 */
@DeveloperApi
case class ParquetRelation2(path: String)(@transient val sqlContext: SQLContext)
  extends CatalystScan with Logging {

  def sparkContext = sqlContext.sparkContext

  // Minor Hack: scala doesnt seem to respect @transient for vals declared via extraction
  @transient
  private var partitionKeys: Seq[String] = _
  @transient
  private var partitions: Seq[Partition] = _
  discoverPartitions()

  // TODO: Only finds the first partition, assumes the key is of type Integer...
  private def discoverPartitions() = {
    val fs = FileSystem.get(new java.net.URI(path), sparkContext.hadoopConfiguration)
    val partValue = "([^=]+)=([^=]+)".r

    val childrenOfPath = fs.listStatus(new Path(path)).filterNot(_.getPath.getName.startsWith("_"))
    val childDirs = childrenOfPath.filter(s => s.isDir)

    if (childDirs.size > 0) {
      val partitionPairs = childDirs.map(_.getPath.getName).map {
        case partValue(key, value) => (key, value)
      }

      val foundKeys = partitionPairs.map(_._1).distinct
      if (foundKeys.size > 1) {
        sys.error(s"Too many distinct partition keys: $foundKeys")
      }

      // Do a parallel lookup of partition metadata.
      val partitionFiles =
        childDirs.par.map { d =>
          fs.listStatus(d.getPath)
            // TODO: Is there a standard hadoop function for this?
            .filterNot(_.getPath.getName.startsWith("_"))
            .filterNot(_.getPath.getName.startsWith("."))
        }.seq

      partitionKeys = foundKeys.toSeq
      partitions = partitionFiles.zip(partitionPairs).map { case (files, (key, value)) =>
        Partition(Map(key -> value.toInt), files)
      }.toSeq
    } else {
      partitionKeys = Nil
      partitions = Partition(Map.empty, childrenOfPath) :: Nil
    }
  }

  override val sizeInBytes = partitions.flatMap(_.files).map(_.getLen).sum

  val dataSchema = StructType.fromAttributes( // TODO: Parquet code should not deal with attributes.
    ParquetTypesConverter.readSchemaFromFile(
      partitions.head.files.head.getPath,
      Some(sparkContext.hadoopConfiguration),
      sqlContext.isParquetBinaryAsString))

  val dataIncludesKey =
    partitionKeys.headOption.map(dataSchema.fieldNames.contains(_)).getOrElse(true)

  override val schema =
    if (dataIncludesKey) {
      dataSchema
    } else {
      StructType(dataSchema.fields :+ StructField(partitionKeys.head, IntegerType))
    }

  override def buildScan(output: Seq[Attribute], predicates: Seq[Expression]): RDD[Row] = {
    // This is mostly a hack so that we can use the existing parquet filter code.
    val requiredColumns = output.map(_.name)
    // TODO: Parquet filters should be based on data sources API, not catalyst expressions.
    val filters = DataSourceStrategy.selectFilters(predicates)

    val job = new Job(sparkContext.hadoopConfiguration)
    ParquetInputFormat.setReadSupportClass(job, classOf[RowReadSupport])
    val jobConf: Configuration = ContextUtil.getConfiguration(job)

    val requestedSchema = StructType(requiredColumns.map(schema(_)))

    // TODO: Make folder based partitioning a first class citizen of the Data Sources API.
    val partitionFilters = filters.collect {
      case e @ EqualTo(attr, value) if partitionKeys.contains(attr) =>
        logInfo(s"Parquet scan partition filter: $attr=$value")
        (p: Partition) => p.partitionValues(attr) == value

      case e @ In(attr, values) if partitionKeys.contains(attr) =>
        logInfo(s"Parquet scan partition filter: $attr IN ${values.mkString("{", ",", "}")}")
        val set = values.toSet
        (p: Partition) => set.contains(p.partitionValues(attr))

      case e @ GreaterThan(attr, value) if partitionKeys.contains(attr) =>
        logInfo(s"Parquet scan partition filter: $attr > $value")
        (p: Partition) => p.partitionValues(attr).asInstanceOf[Int] > value.asInstanceOf[Int]

      case e @ GreaterThanOrEqual(attr, value) if partitionKeys.contains(attr) =>
        logInfo(s"Parquet scan partition filter: $attr >= $value")
        (p: Partition) => p.partitionValues(attr).asInstanceOf[Int] >= value.asInstanceOf[Int]

      case e @ LessThan(attr, value) if partitionKeys.contains(attr) =>
        logInfo(s"Parquet scan partition filter: $attr < $value")
        (p: Partition) => p.partitionValues(attr).asInstanceOf[Int] < value.asInstanceOf[Int]

      case e @ LessThanOrEqual(attr, value) if partitionKeys.contains(attr) =>
        logInfo(s"Parquet scan partition filter: $attr <= $value")
        (p: Partition) => p.partitionValues(attr).asInstanceOf[Int] <= value.asInstanceOf[Int]
    }

    val selectedPartitions = partitions.filter(p => partitionFilters.forall(_(p)))
    val fs = FileSystem.get(new java.net.URI(path), sparkContext.hadoopConfiguration)
    val selectedFiles = selectedPartitions.flatMap(_.files).map(f => fs.makeQualified(f.getPath))
    org.apache.hadoop.mapreduce.lib.input.FileInputFormat.setInputPaths(job, selectedFiles:_*)

    // Push down filters when possible
    predicates
      .reduceOption(And)
      .flatMap(ParquetFilters.createFilter)
      .filter(_ => sqlContext.parquetFilterPushDown)
      .foreach(ParquetInputFormat.setFilterPredicate(jobConf, _))

    def percentRead = selectedPartitions.size.toDouble / partitions.size.toDouble * 100
    logInfo(s"Reading $percentRead% of $path partitions")

    // Store both requested and original schema in `Configuration`
    jobConf.set(
      RowReadSupport.SPARK_ROW_REQUESTED_SCHEMA,
      ParquetTypesConverter.convertToString(requestedSchema.toAttributes))
    jobConf.set(
      RowWriteSupport.SPARK_ROW_SCHEMA,
      ParquetTypesConverter.convertToString(schema.toAttributes))

    // Tell FilteringParquetRowInputFormat whether it's okay to cache Parquet and FS metadata
    val useCache = sqlContext.getConf(SQLConf.PARQUET_CACHE_METADATA, "true").toBoolean
    jobConf.set(SQLConf.PARQUET_CACHE_METADATA, useCache.toString)

    val baseRDD =
      new org.apache.spark.rdd.NewHadoopRDD(
          sparkContext,
          classOf[FilteringParquetRowInputFormat],
          classOf[Void],
          classOf[Row],
          jobConf) {
        val cacheMetadata = useCache

        @transient
        val cachedStatus = selectedPartitions.flatMap(_.files)

        // Overridden so we can inject our own cached files statuses.
        override def getPartitions: Array[SparkPartition] = {
          val inputFormat =
            if (cacheMetadata) {
              new FilteringParquetRowInputFormat {
                override def listStatus(jobContext: JobContext): JList[FileStatus] = cachedStatus
              }
            } else {
              new FilteringParquetRowInputFormat
            }

          inputFormat match {
            case configurable: Configurable =>
              configurable.setConf(getConf)
            case _ =>
          }
          val jobContext = newJobContext(getConf, jobId)
          val rawSplits = inputFormat.getSplits(jobContext).toArray
          val result = new Array[SparkPartition](rawSplits.size)
          for (i <- 0 until rawSplits.size) {
            result(i) =
              new NewHadoopPartition(id, i, rawSplits(i).asInstanceOf[InputSplit with Writable])
          }
          result
        }
      }

    // The ordinal for the partition key in the result row, if requested.
    val partitionKeyLocation =
      partitionKeys
        .headOption
        .map(requiredColumns.indexOf(_))
        .getOrElse(-1)

    // When the data does not include the key and the key is requested then we must fill it in
    // based on information from the input split.
    if (!dataIncludesKey && partitionKeyLocation != -1) {
      baseRDD.mapPartitionsWithInputSplit { case (split, iter) =>
        val partValue = "([^=]+)=([^=]+)".r
        val partValues =
          split.asInstanceOf[parquet.hadoop.ParquetInputSplit]
            .getPath
            .toString
            .split("/")
            .flatMap {
            case partValue(key, value) => Some(key -> value)
            case _ => None
          }.toMap

        val currentValue = partValues.values.head.toInt
        iter.map { pair =>
          val res = pair._2.asInstanceOf[SpecificMutableRow]
          res.setInt(partitionKeyLocation, currentValue)
          res
        }
      }
    } else {
      baseRDD.map(_._2)
    }
  }
}
