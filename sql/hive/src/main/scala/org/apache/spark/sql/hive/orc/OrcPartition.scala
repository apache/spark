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

import java.math.BigDecimal
import java.lang.{Double => JDouble, Float => JFloat, Long => JLong}
import java.math.{BigDecimal => JBigDecimal}
import java.net.URI


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.parquet.RowReadSupport
import org.apache.spark.sql.types._
import parquet.format.converter.ParquetMetadataConverter
import parquet.hadoop.util.ContextUtil
import parquet.hadoop.{ParquetInputFormat, Footer, ParquetFileReader, ParquetFileWriter}

import scala.collection.mutable.ArrayBuffer
import scala.util.Try


case class Partition(values: Row, path: String)

case class PartitionSpec(partitionColumns: StructType, partitions: Seq[Partition])

// TODO Data source implementations shouldn't touch Catalyst types (`Literal`).
// However, we are already using Catalyst expressions for partition pruning and predicate
// push-down here...
private[orc] case class PartitionValues(columnNames: Seq[String], literals: Seq[Literal]) {
  require(columnNames.size == literals.size)
}

// If the maybeSchema is not None, it is for scan
// Otherwise, it is for insert
case class OrcPartition (relation: OrcRelation,
    parameters: Map[String, String],
    paths: Seq[String],
    maybeSchema: Option[StructType] = None,
    maybePartitionSpec: Option[PartitionSpec] = None) {
  // The orcFile schema
  var orcSchema: StructType = _
  // The schema consists of orcSchema and partitions.
  var schema: StructType = _
  var partitionSpec: PartitionSpec = _
  var isPartitioned: Boolean = _
  var partitionColumns: StructType = _
  var partitions: Seq[Partition] = _
  // all the valid files names if no partition
  var fileCandidates: Seq[FileStatus] = _
  @transient val sqlContext = relation.sqlContext
  @transient val conf = sqlContext.sparkContext.hadoopConfiguration

  // Hive uses this as part of the default partition name when the partition column value is null
  // or empty string
  private val defaultPartitionName = parameters.getOrElse(
    OrcPartition.DEFAULT_PARTITION_NAME, "__HIVE_DEFAULT_PARTITION__")

  /**
   * Refreshes `FileStatus`es, footers, partition spec, and table schema.
   */
  def refresh(): Unit = {
    // Support either reading a collection of raw ORC part-files, or a collection of folders
    // containing ORC files (e.g. partitioned ORC table).
    val baseStatuses = paths.distinct.map { p =>
      val fs = FileSystem.get(URI.create(p), sqlContext.sparkContext.hadoopConfiguration)
      val qualified = fs.makeQualified(new Path(p))
      // it is for insert
      if (!fs.exists(qualified) && maybeSchema.isDefined) {
        fs.mkdirs(qualified)
      }

      fs.getFileStatus(qualified)
    }.toArray
    assert(baseStatuses.forall(!_.isDir) || baseStatuses.forall(_.isDir))

    // Lists `FileStatus`es of all leaf nodes (files) under all base directories.
    val leaves = baseStatuses.flatMap { f =>
      val fs = FileSystem.get(f.getPath.toUri, sqlContext.sparkContext.hadoopConfiguration)
      SparkHadoopUtil.get.listLeafStatuses(fs, f.getPath)
        .filterNot(_.getPath.getName.startsWith("_"))
    }

    fileCandidates = leaves //.asInstanceOf[Seq[FileStatus]]
    println(s"filelist: $fileCandidates")
    partitionSpec = maybePartitionSpec.getOrElse {
      val partitionDirs = leaves
        .filterNot(baseStatuses.contains)
        .map(_.getPath.getParent)
        .distinct

      if (partitionDirs.nonEmpty) {
        // Parses names and values of partition columns, and infer their data types.
        parsePartitions(partitionDirs, defaultPartitionName)
      } else {
        // No partition directories found, makes an empty specification
        PartitionSpec(StructType(Seq.empty[StructField]), Seq.empty[Partition])
      }
    }
    println(s"partitionSpec: $partitionSpec")
    partitionColumns = partitionSpec.partitionColumns
    println(s"partitionColumns: $partitionColumns")
    partitions = partitionSpec.partitions

    isPartitioned = partitionColumns.nonEmpty


    // To get the schema. We first try to get the schema defined in maybeSchema for insertion case
    // If maybeSchema is not defined (scan), we will try to get the schema from existing orc file.
    // If data does not exist, we will try to get the schema defined in
    // maybeMetastoreSchema (defined in the options of the data source).
    // Finally, if we still could not get the schema. We throw an error.
    orcSchema =
      maybeSchema.getOrElse(OrcFileOperator.readSchema(paths(0), Some(conf)))


    val partitionKeysIncludedInOrcSchema =
      isPartitioned &&
        partitionColumns.forall(f => orcSchema.fieldNames.contains(f.name))

    schema = {
      if (partitionKeysIncludedInOrcSchema) {
        orcSchema
      } else {
        StructType(orcSchema.fields ++ partitionColumns.fields)
      }
    }
  }


  /**
   * Given a group of qualified paths, tries to parse them and returns a partition specification.
   * For example, given:
   * {{{
   *   hdfs://<host>:<port>/path/to/partition/a=1/b=hello/c=3.14
   *   hdfs://<host>:<port>/path/to/partition/a=2/b=world/c=6.28
   * }}}
   * it returns:
   * {{{
   *   PartitionSpec(
   *     partitionColumns = StructType(
   *       StructField(name = "a", dataType = IntegerType, nullable = true),
   *       StructField(name = "b", dataType = StringType, nullable = true),
   *       StructField(name = "c", dataType = DoubleType, nullable = true)),
   *     partitions = Seq(
   *       Partition(
   *         values = Row(1, "hello", 3.14),
   *         path = "hdfs://<host>:<port>/path/to/partition/a=1/b=hello/c=3.14"),
   *       Partition(
   *         values = Row(2, "world", 6.28),
   *         path = "hdfs://<host>:<port>/path/to/partition/a=2/b=world/c=6.28")))
   * }}}
   */
  private[orc] def parsePartitions(paths: Seq[Path],
      defaultPartitionName: String): PartitionSpec = {
    val partitionValues = resolvePartitions(paths.map(parsePartition(_, defaultPartitionName)))
    val fields = {
      val (PartitionValues(columnNames, literals)) = partitionValues.head
      columnNames.zip(literals).map { case (name, Literal(_, dataType)) =>
        StructField(name, dataType, nullable = true)
      }
    }

    val partitions = partitionValues.zip(paths).map {
      case (PartitionValues(_, literals), path) =>
        Partition(Row(literals.map(_.value): _*), path.toString)
    }

    PartitionSpec(StructType(fields), partitions)
  }

  /**
   * Parses a single partition, returns column names and values of each partition column.  For
   * example, given:
   * {{{
   *   path = hdfs://<host>:<port>/path/to/partition/a=42/b=hello/c=3.14
   * }}}
   * it returns:
   * {{{
   *   PartitionValues(
   *     Seq("a", "b", "c"),
   *     Seq(
   *       Literal(42, IntegerType),
   *       Literal("hello", StringType),
   *       Literal(3.14, FloatType)))
   * }}}
   */
  private[orc] def parsePartition(path: Path,
      defaultPartitionName: String): PartitionValues = {
    val columns = ArrayBuffer.empty[(String, Literal)]
    // Old Hadoop versions don't have `Path.isRoot`
    var finished = path.getParent == null
    var chopped = path

    while (!finished) {
      val maybeColumn = parsePartitionColumn(chopped.getName, defaultPartitionName)
      maybeColumn.foreach(columns += _)
      chopped = chopped.getParent
      finished = maybeColumn.isEmpty || chopped.getParent == null
    }

    val (columnNames, values) = columns.reverse.unzip
    PartitionValues(columnNames, values)
  }

  private def parsePartitionColumn(columnSpec: String,
      defaultPartitionName: String): Option[(String, Literal)] = {
    val equalSignIndex = columnSpec.indexOf('=')
    if (equalSignIndex == -1) {
      None
    } else {
      val columnName = columnSpec.take(equalSignIndex)
      assert(columnName.nonEmpty, s"Empty partition column name in '$columnSpec'")

      val rawColumnValue = columnSpec.drop(equalSignIndex + 1)
      assert(rawColumnValue.nonEmpty, s"Empty partition column value in '$columnSpec'")

      val literal = inferPartitionColumnValue(rawColumnValue, defaultPartitionName)
      Some(columnName -> literal)
    }
  }

  /**
   * Converts a string to a `Literal` with automatic type inference.  Currently only supports
   * [[IntegerType]], [[LongType]], [[FloatType]], [[DoubleType]], [[DecimalType.Unlimited]], and
   * [[StringType]].
   */
  private[orc] def inferPartitionColumnValue(raw: String,
      defaultPartitionName: String): Literal = {
    // First tries integral types
    Try(Literal(Integer.parseInt(raw), IntegerType))
      .orElse(Try(Literal(JLong.parseLong(raw), LongType)))
      // Then falls back to fractional types
      .orElse(Try(Literal(JFloat.parseFloat(raw), FloatType)))
      .orElse(Try(Literal(JDouble.parseDouble(raw), DoubleType)))
      .orElse(Try(Literal(new JBigDecimal(raw), DecimalType.Unlimited)))
      // Then falls back to string
      .getOrElse {
      if (raw == defaultPartitionName) Literal(null, NullType) else Literal(raw, StringType)
    }
  }

  private val upCastingOrder: Seq[DataType] =
    Seq(NullType, IntegerType, LongType, FloatType, DoubleType, DecimalType.Unlimited, StringType)

  /**
   * Given a collection of [[Literal]]s, resolves possible type conflicts by up-casting "lower"
   * types.
   */
  private def resolveTypeConflicts(literals: Seq[Literal]): Seq[Literal] = {
    val desiredType = {
      val topType = literals.map(_.dataType).maxBy(upCastingOrder.indexOf(_))
      // Falls back to string if all values of this column are null or empty string
      if (topType == NullType) StringType else topType
    }

    literals.map { case l @ Literal(_, dataType) =>
      Literal(Cast(l, desiredType).eval(), desiredType)
    }
  }

  /**
   * Resolves possible type conflicts between partitions by up-casting "lower" types.  The up-
   * casting order is:
   * {{{
   *   NullType ->
   *   IntegerType -> LongType ->
   *   FloatType -> DoubleType -> DecimalType.Unlimited ->
   *   StringType
   * }}}
   */
  private[orc] def resolvePartitions(values: Seq[PartitionValues]): Seq[PartitionValues] = {
    // Column names of all partitions must match
    val distinctPartitionsColNames = values.map(_.columnNames).distinct
    assert(distinctPartitionsColNames.size == 1, {
      val list = distinctPartitionsColNames.mkString("\t", "\n", "")
      s"Conflicting partition column names detected:\n$list"
    })

    // Resolves possible type conflicts for each column
    val columnCount = values.head.columnNames.size
    val resolvedValues = (0 until columnCount).map { i =>
      resolveTypeConflicts(values.map(_.literals(i)))
    }

    // Fills resolved literals back to each partition
    values.zipWithIndex.map { case (d, index) =>
      d.copy(literals = resolvedValues.map(_(index)))
    }
  }

  // we should only pushdown the following predicates and avoid those referring to
  // partition columns.
   def getPushDownFilters(predicates: Seq[Expression]): Seq[Expression] = {
    println(s"old predicates: $predicates")
    val p = predicates
      // Don't push down predicates which reference partition columns
      .filter { pred =>
      val partitionColNames = partitionColumns.map(_.name).toSet
      val referencedColNames = pred.references.map(_.name).toSet
      referencedColNames.intersect(partitionColNames).isEmpty
    }
    println(s"selected predicates: $p")
    p
  }

  // We should only scan these files satisfying partitioning
  def getPartitionFiles(predicates: Seq[Expression]): Seq[FileStatus] = {
    val selectedPartitions = getSelectedPartitions(predicates)
    val selectedFiles = if (isPartitioned) {
      selectedPartitions.flatMap { p =>
        fileCandidates.filter(_.getPath.getParent.toString == p.path)
      }
    } else {
      fileCandidates.toSeq
    }
    println(s"selected files: $selectedFiles")
    selectedFiles
  }

  // Here it is different from parquet implementation. As long as the attribute
  // presents in the partition columns, we skip the reading from file.
  // Assumption: the value in the file matches the partition file name.
  // Is it the right assumption?
  def getOrcReadColumn(attributes: Seq[Attribute]): Seq[Attribute]= {
    // we only need to scan columns that are not partition columns
    println(s"getOrcReadSchema: $attributes ${partitionColumns.toAttributes}")
    attributes.filterNot(a => partitionColumns.fieldNames.contains(a.name))
  }

  // The ordinals for partition keys in the result row, if requested.
  def getPartitionKeyMap(attributes: Seq[Attribute]): Map[Int, Int] = {
    partitionColumns.fieldNames.zipWithIndex.map {
      case (name, index) => index -> attributes.map(_.name).indexOf(name)
    }.toMap.filter {
      case (_, index) => index >= 0
    }
  }



  def getSelectedPartitions(predicates: Seq[Expression]): Seq[Partition] = {
    val partitionColumnNames = partitionColumns.map(_.name).toSet
    val partitionPruningPredicates = predicates.filter {
      _.references.map(_.name).toSet.subsetOf(partitionColumnNames)
    }

    val rawPredicate =
      partitionPruningPredicates.reduceOption(expressions.And).getOrElse(Literal(true))
    val boundPredicate = InterpretedPredicate(rawPredicate transform {
      case a: AttributeReference =>
        val index = partitionColumns.indexWhere(a.name == _.name)
        BoundReference(index, partitionColumns(index).dataType, nullable = true)
    })

    if (isPartitioned && partitionPruningPredicates.nonEmpty) {
      partitions.filter(p => boundPredicate(p.values))
    } else {
      partitions
    }
  }
}

private[sql] object OrcPartition extends Logging {
  // Default partition name to use when the partition column value is null or empty string.
  val DEFAULT_PARTITION_NAME = "partition.defaultName"
}
