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

import java.lang.{Double => JDouble, Float => JFloat, Long => JLong}
import java.math.{BigDecimal => JBigDecimal}
import java.net.URI
import java.util.{List => JList}

import scala.collection.JavaConversions._
import scala.util.Try

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.{InputSplit, Job, JobContext}
import parquet.filter2.predicate.FilterApi
import parquet.format.converter.ParquetMetadataConverter
import parquet.hadoop.{ParquetInputFormat, _}
import parquet.hadoop.util.ContextUtil

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rdd.{NewHadoopPartition, RDD}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.parquet.ParquetTypesConverter._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{IntegerType, StructField, StructType, _}
import org.apache.spark.sql.{Row, SQLConf, SQLContext}
import org.apache.spark.{Logging, Partition => SparkPartition, SparkException}


/**
 * Allows creation of parquet based tables using the syntax
 * `CREATE TEMPORARY TABLE ... USING org.apache.spark.sql.parquet`.  Currently the only option
 * required is `path`, which should be the location of a collection of, optionally partitioned,
 * parquet files.
 */
class DefaultSource extends RelationProvider {
  /** Returns a new base relation with the given parameters. */
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    val path = parameters.getOrElse("path",
      sys.error("'path' must be specified for parquet tables."))

    ParquetRelation2(path, parameters)(sqlContext)
  }
}

private[parquet] case class Partition(values: Row, path: String)

private[parquet] case class PartitionSpec(partitionColumns: StructType, partitions: Seq[Partition])

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
case class ParquetRelation2
    (path: String, parameters: Map[String, String])
    (@transient val sqlContext: SQLContext)
  extends CatalystScan with Logging {

  // Should we merge schemas from all Parquet part-files?
  private val shouldMergeSchemas =
    parameters.getOrElse("parquet.schema.merge", "true").toBoolean

  def sparkContext = sqlContext.sparkContext

  private val fs = FileSystem.get(new URI(path), sparkContext.hadoopConfiguration)

  private val qualifiedBasePath = fs.makeQualified(new Path(path))

  // Cache `FileStatus` objects for Parquet data files, "_metadata", and "_common_metadata".
  private val (dataFiles, metadataFile, commonMetadataFile) = {
    val leaves = SparkHadoopUtil.get.listLeafStatuses(fs, qualifiedBasePath).filter { f =>
      isSummaryFile(f.getPath) ||
        (!f.getPath.getName.startsWith("_") && !f.getPath.getName.startsWith("."))
    }

    assert(leaves.nonEmpty, s"$qualifiedBasePath is either an empty folder or nonexistent.")

    (leaves.filterNot(f => isSummaryFile(f.getPath)),
      leaves.find(_.getPath.getName == ParquetFileWriter.PARQUET_METADATA_FILE),
      leaves.find(_.getPath.getName == ParquetFileWriter.PARQUET_COMMON_METADATA_FILE))
  }

  private val PartitionSpec(partitionColumns, partitions) = {
    val partitionDirPaths = dataFiles
      // When reading a single raw Parquet part-file, base path points to that single data file
      // rather than its parent directory, shouldn't use it for partition discovery.
      .filterNot(_.getPath == qualifiedBasePath)
      .map(f => fs.makeQualified(f.getPath.getParent))
      .filterNot(_ == qualifiedBasePath)
      .distinct

    if (partitionDirPaths.nonEmpty) {
      ParquetRelation2.parsePartitions(qualifiedBasePath, partitionDirPaths)
    } else {
      // No partition directories found, makes an empty specification
      PartitionSpec(StructType(Seq.empty[StructField]), Seq.empty[Partition])
    }
  }

  private def isPartitioned = partitionColumns.nonEmpty

  private val footers = {
    // TODO Issue a Spark job to gather footers if there are too many files
    (dataFiles ++ metadataFile ++ commonMetadataFile).par.map { f =>
      val parquetMetadata = ParquetFileReader.readFooter(
        sparkContext.hadoopConfiguration, f, ParquetMetadataConverter.NO_FILTER)
      f -> new Footer(f.getPath, parquetMetadata)
    }.seq.toMap
  }

  private def readSchema(): StructType = {
    // Figures out which file(s) we need to touch in order to retrieve the schema.
    val filesToTouch =
      // Always tries the summary files first if users don't require a merged schema.  In this case,
      // "_common_metadata" is more preferable than "_metadata" because it doesn't contain row
      // groups information, and could be much smaller for large Parquet files with lots of row
      // groups.
      //
      // NOTE: Metadata stored in the summary files are merged from all part-files.  However, for
      // user defined key-value metadata (in which we store Spark SQL schema), Parquet doesn't know
      // how to merge them correctly if some key is associated with different values in different
      // part-files.  When this happens, Parquet simply gives up generating the summary file.  This
      // implies that if a summary file presents, then:
      //
      //   1. Either all part-files have exactly the same Spark SQL schema, or
      //   2. Some part-files don't contain Spark SQL schema in the key-value metadata at all (thus
      //      their schemas may differ from each other).
      //
      // Here we tend to be pessimistic and take the second case into account.  Basically this means
      // we can't trust the summary files if users require a merged schema, and must touch all part-
      // files to do the merge.
      if (shouldMergeSchemas) {
        dataFiles
      } else {
        commonMetadataFile
          .orElse(metadataFile)
          // Summary file(s) not found, falls back to the first part-file.
          .orElse(dataFiles.headOption).toSeq
      }

    filesToTouch.map { file =>
      val metadata = footers(file).getParquetMetadata.getFileMetaData
      val parquetSchema = metadata.getSchema
      val maybeSparkSchema = metadata
        .getKeyValueMetaData
        .toMap
        .get(RowReadSupport.SPARK_METADATA_KEY)
        .map(DataType.fromJson(_).asInstanceOf[StructType])

      maybeSparkSchema.getOrElse {
        // Falls back to Parquet schema if Spark SQL schema is absent.
        StructType.fromAttributes(
          // TODO Really no need to use `Attribute` here, we only need to know the data type.
          convertToAttributes(parquetSchema, sqlContext.conf.isParquetBinaryAsString))
      }
    }.reduce { (left, right) =>
      try mergeCatalystSchemas(left, right) catch { case e: Throwable =>
        throw new SparkException(s"Failed to merge incompatible schemas $left and $right", e)
      }
    }
  }

  private def isSummaryFile(file: Path): Boolean = {
    file.getName == ParquetFileWriter.PARQUET_COMMON_METADATA_FILE ||
      file.getName == ParquetFileWriter.PARQUET_METADATA_FILE
  }

  // TODO Should calculate per scan size
  // It's common that a query only scans a fraction of a large Parquet file.  Returning size of the
  // whole Parquet file disables some optimizations in this case (e.g. broadcast join).
  override val sizeInBytes = dataFiles.map(_.getLen).sum

  private val dataSchema = readSchema()

  private val dataSchemaIncludesPartitionKeys =
    partitionColumns.forall(f => dataSchema.fieldNames.contains(f.name))

  override val schema = if (dataSchemaIncludesPartitionKeys) {
    dataSchema
  } else {
    StructType(dataSchema.fields ++ partitionColumns.fields)
  }

  // This is mostly a hack so that we can use the existing parquet filter code.
  override def buildScan(output: Seq[Attribute], predicates: Seq[Expression]): RDD[Row] = {
    val job = new Job(sparkContext.hadoopConfiguration)
    ParquetInputFormat.setReadSupportClass(job, classOf[RowReadSupport])
    val jobConf: Configuration = ContextUtil.getConfiguration(job)

    val partitionKeySet = partitionColumns.map(_.name).toSet
    val partitionPruningPredicate =
      predicates
        .filter(_.references.map(_.name).toSet.subsetOf(partitionKeySet))
        .reduceOption(And)
        .getOrElse(Literal(true))

    val pruningCondition = InterpretedPredicate(partitionPruningPredicate transform {
      case a: AttributeReference =>
        val idx = partitionColumns.indexWhere(a.name == _.name)
        BoundReference(idx, partitionColumns(idx).dataType, nullable = true)
    })

    val selectedPartitions = if (isPartitioned && predicates.nonEmpty) {
      partitions.filter(p => pruningCondition(p.values))
    } else {
      partitions
    }

    val selectedFiles = if (isPartitioned) {
      selectedPartitions.flatMap(p => dataFiles.filter(_.getPath.getParent.toString == p.path))
    } else {
      dataFiles
    }

    // FileInputFormat cannot handle empty lists.
    if (selectedFiles.nonEmpty) {
      FileInputFormat.setInputPaths(job, selectedFiles.map(_.getPath): _*)
    }

    // Push down filters when possible. Notice that not all filters can be converted to Parquet
    // filter predicate. Here we try to convert each individual predicate and only collect those
    // convertible ones.
    predicates
      .flatMap(ParquetFilters.createFilter)
      .reduceOption(FilterApi.and)
      .filter(_ => sqlContext.conf.parquetFilterPushDown)
      .foreach(ParquetInputFormat.setFilterPredicate(jobConf, _))

    if (isPartitioned) {
      def percentRead = selectedPartitions.size.toDouble / partitions.size.toDouble * 100
      logInfo(s"Reading $percentRead% of $path partitions")
    }

    val requiredColumns = output.map(_.name)
    val requestedSchema = StructType(requiredColumns.map(schema(_)))

    // Store both requested and original schema in `Configuration`
    jobConf.set(
      RowReadSupport.SPARK_ROW_REQUESTED_SCHEMA,
      convertToString(requestedSchema.toAttributes))
    jobConf.set(
      RowWriteSupport.SPARK_ROW_SCHEMA,
      convertToString(schema.toAttributes))

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
        val cachedStatus = selectedFiles

        // Overridden so we can inject our own cached files statuses.
        override def getPartitions: Array[SparkPartition] = {
          val inputFormat = if (cacheMetadata) {
            new FilteringParquetRowInputFormat {
              override def listStatus(jobContext: JobContext): JList[FileStatus] = cachedStatus
            }
          } else {
            new FilteringParquetRowInputFormat
          }

          val jobContext = newJobContext(getConf, jobId)
          val rawSplits = inputFormat.getSplits(jobContext)

          Array.tabulate[SparkPartition](rawSplits.size) { i =>
            new NewHadoopPartition(id, i, rawSplits(i).asInstanceOf[InputSplit with Writable])
          }
        }
      }

    // The ordinals for partition keys in the result row, if requested.
    val partitionKeyLocations = partitionColumns.fieldNames.zipWithIndex.map {
      case (name, index) => index -> requiredColumns.indexOf(name)
    }.toMap.filter {
      case (_, index) => index >= 0
    }

    // When the data does not include the key and the key is requested then we must fill it in
    // based on information from the input split.
    if (!dataSchemaIncludesPartitionKeys && partitionKeyLocations.nonEmpty) {
      baseRDD.mapPartitionsWithInputSplit { case (split: ParquetInputSplit, iterator) =>
        val partValues = selectedPartitions.collectFirst {
          case p if split.getPath.getParent.toString == p.path => p.values
        }.get

        iterator.map { pair =>
          val row = pair._2.asInstanceOf[SpecificMutableRow]
          var i = 0
          while (i < partValues.size) {
            // TODO Avoids boxing cost here!
            row.update(partitionKeyLocations(i), partValues(i))
            i += 1
          }
          row
        }
      }
    } else {
      baseRDD.map(_._2)
    }
  }
}

object ParquetRelation2 {
  // TODO Data source implementations shouldn't touch Catalyst types (`Literal`).
  // However, we are already using Catalyst expressions for partition pruning and predicate
  // push-down here...
  case class PartitionDesc(columnNames: Seq[String], literals: Seq[Literal]) {
    require(columnNames.size == literals.size)
  }

  /**
   * Given a base path and all data file paths in it, returns a partition specification.
   */
  private[parquet] def parsePartitions(basePath: Path, dataPaths: Seq[Path]): PartitionSpec = {
    val partitionDescs = resolvePartitions(dataPaths.map(parsePartition(basePath, _)))
    val PartitionDesc(columnNames, columnLiterals) = partitionDescs.head
    val fields = columnNames.zip(columnLiterals).map { case (name, Literal(_, dataType)) =>
      StructField(name, dataType, nullable = true)
    }

    val partitions = (partitionDescs, dataPaths).zipped.map { (desc, path) =>
      val values = desc.literals.map(_.value)
      Partition(Row(values: _*), path.toString)
    }

    PartitionSpec(StructType(fields), partitions)
  }

  /**
   * Parses a single partition, returns column names and values of each partition column.  For
   * example, given:
   * {{{
   *   basePath = hdfs://host:9000/base/path/
   *   dataPath = hdfs://host:9000/base/path/a=42/b=hello/c=3.14
   * }}}
   * we have:
   * {{{
   *   PartitionSpec(
   *     Seq("a", "b", "c"),
   *     Seq(
   *       Literal(42, IntegerType),
   *       Literal("hello", StringType),
   *       Literal(3.14, FloatType)))
   * }}}
   */
  private[parquet] def parsePartition(basePath: Path, dataPath: Path): PartitionDesc = {
    val rawSpec = dataPath.toString.stripPrefix(basePath.toString).stripPrefix(Path.SEPARATOR)
    val (columnNames, values) = rawSpec.split(Path.SEPARATOR).map { column =>
      val equalSignIndex = column.indexOf('=')
      assert(equalSignIndex > 0, s"Invalid partition column spec '$column' found in $dataPath")
      val columnName = rawSpec.take(equalSignIndex)
      val literal = inferPartitionColumnValue(rawSpec.drop(equalSignIndex + 1))
      columnName -> literal
    }.unzip

    PartitionDesc(columnNames, values)
  }

  /**
   * Resolves possible type conflicts between partitions by up-casting "lower" types.  The up-
   * casting order is:
   * {{{
   *   IntegerType -> LongType -> FloatType -> DoubleType -> DecimalType.Unlimited -> StringType
   * }}}
   */
  private[parquet] def resolvePartitions(descs: Seq[PartitionDesc]): Seq[PartitionDesc] = {
    val distinctColNamesOfPartitions = descs.map(_.columnNames).distinct
    val columnCount = descs.head.columnNames.size

    // Column names of all partitions must match
    assert(distinctColNamesOfPartitions.size == 1, {
      val list = distinctColNamesOfPartitions.mkString("\t", "\n", "")
      s"Conflicting partition column names detected:\n$list"
    })

    // Resolves possible type conflicts for each column
    val resolvedValues = (0 until columnCount).map { i =>
      resolveTypeConflicts(descs.map(_.literals(i)))
    }

    // Fills resolved literals back to each partition
    descs.zipWithIndex.map { case (d, index) =>
      d.copy(literals = resolvedValues.map(_(index)))
    }
  }

  /**
   * Converts a string to a `Literal` with automatic type inference.  Currently only supports
   * [[IntegerType]], [[LongType]], [[FloatType]], [[DoubleType]], [[DecimalType.Unlimited]], and
   * [[StringType]].
   */
  private[parquet] def inferPartitionColumnValue(raw: String): Literal = {
    // First tries integral types
    Try(Literal(Integer.parseInt(raw), IntegerType))
      .orElse(Try(Literal(JLong.parseLong(raw), LongType)))
      // Then falls back to fractional types
      .orElse(Try(Literal(JFloat.parseFloat(raw), FloatType)))
      .orElse(Try(Literal(JDouble.parseDouble(raw), DoubleType)))
      .orElse(Try(Literal(new JBigDecimal(raw), DecimalType.Unlimited)))
      // Then falls back to string
      .getOrElse(Literal(raw, StringType))
  }

  private val upCastingOrder: Seq[DataType] =
    Seq(IntegerType, LongType, FloatType, DoubleType, DecimalType.Unlimited, StringType)

  /**
   * Given a collection of [[Literal]]s, resolves possible type conflicts by up-casting "lower"
   * types.
   */
  private def resolveTypeConflicts(literals: Seq[Literal]): Seq[Literal] = {
    val desiredType = literals.map(_.dataType).maxBy(upCastingOrder.indexOf(_))
    literals.map { case l @ Literal(_, dataType) =>
      Literal(Cast(l, desiredType).eval(), desiredType)
    }
  }
}
