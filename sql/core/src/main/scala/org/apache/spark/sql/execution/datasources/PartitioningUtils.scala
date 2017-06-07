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

package org.apache.spark.sql.execution.datasources

import java.lang.{Double => JDouble, Long => JLong}
import java.math.{BigDecimal => JBigDecimal}
import java.util.{Locale, TimeZone}

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.{Cast, Literal}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._

// TODO: We should tighten up visibility of the classes here once we clean up Hive coupling.

object PartitionPath {
  def apply(values: InternalRow, path: String): PartitionPath =
    apply(values, new Path(path))
}

/**
 * Holds a directory in a partitioned collection of files as well as the partition values
 * in the form of a Row.  Before scanning, the files at `path` need to be enumerated.
 */
case class PartitionPath(values: InternalRow, path: Path)

case class PartitionSpec(
    partitionColumns: StructType,
    partitions: Seq[PartitionPath])

object PartitionSpec {
  val emptySpec = PartitionSpec(StructType(Seq.empty[StructField]), Seq.empty[PartitionPath])
}

object PartitioningUtils {

  private[datasources] case class PartitionValues(columnNames: Seq[String], literals: Seq[Literal])
  {
    require(columnNames.size == literals.size)
  }

  import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils.DEFAULT_PARTITION_NAME
  import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils.escapePathName
  import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils.unescapePathName

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
  private[datasources] def parsePartitions(
      paths: Seq[Path],
      typeInference: Boolean,
      basePaths: Set[Path],
      timeZoneId: String): PartitionSpec = {
    parsePartitions(paths, typeInference, basePaths, DateTimeUtils.getTimeZone(timeZoneId))
  }

  private[datasources] def parsePartitions(
      paths: Seq[Path],
      typeInference: Boolean,
      basePaths: Set[Path],
      timeZone: TimeZone): PartitionSpec = {
    // First, we need to parse every partition's path and see if we can find partition values.
    val (partitionValues, optDiscoveredBasePaths) = paths.map { path =>
      parsePartition(path, typeInference, basePaths, timeZone)
    }.unzip

    // We create pairs of (path -> path's partition value) here
    // If the corresponding partition value is None, the pair will be skipped
    val pathsWithPartitionValues = paths.zip(partitionValues).flatMap(x => x._2.map(x._1 -> _))

    if (pathsWithPartitionValues.isEmpty) {
      // This dataset is not partitioned.
      PartitionSpec.emptySpec
    } else {
      // This dataset is partitioned. We need to check whether all partitions have the same
      // partition columns and resolve potential type conflicts.

      // Check if there is conflicting directory structure.
      // For the paths such as:
      // var paths = Seq(
      //   "hdfs://host:9000/invalidPath",
      //   "hdfs://host:9000/path/a=10/b=20",
      //   "hdfs://host:9000/path/a=10.5/b=hello")
      // It will be recognised as conflicting directory structure:
      //   "hdfs://host:9000/invalidPath"
      //   "hdfs://host:9000/path"
      // TODO: Selective case sensitivity.
      val discoveredBasePaths = optDiscoveredBasePaths.flatten.map(_.toString.toLowerCase())
      assert(
        discoveredBasePaths.distinct.size == 1,
        "Conflicting directory structures detected. Suspicious paths:\b" +
          discoveredBasePaths.distinct.mkString("\n\t", "\n\t", "\n\n") +
          "If provided paths are partition directories, please set " +
          "\"basePath\" in the options of the data source to specify the " +
          "root directory of the table. If there are multiple root directories, " +
          "please load them separately and then union them.")

      val resolvedPartitionValues = resolvePartitions(pathsWithPartitionValues)

      // Creates the StructType which represents the partition columns.
      val fields = {
        val PartitionValues(columnNames, literals) = resolvedPartitionValues.head
        columnNames.zip(literals).map { case (name, Literal(_, dataType)) =>
          // We always assume partition columns are nullable since we've no idea whether null values
          // will be appended in the future.
          StructField(name, dataType, nullable = true)
        }
      }

      // Finally, we create `Partition`s based on paths and resolved partition values.
      val partitions = resolvedPartitionValues.zip(pathsWithPartitionValues).map {
        case (PartitionValues(_, literals), (path, _)) =>
          PartitionPath(InternalRow.fromSeq(literals.map(_.value)), path)
      }

      PartitionSpec(StructType(fields), partitions)
    }
  }

  /**
   * Parses a single partition, returns column names and values of each partition column, also
   * the path when we stop partition discovery.  For example, given:
   * {{{
   *   path = hdfs://<host>:<port>/path/to/partition/a=42/b=hello/c=3.14
   * }}}
   * it returns the partition:
   * {{{
   *   PartitionValues(
   *     Seq("a", "b", "c"),
   *     Seq(
   *       Literal.create(42, IntegerType),
   *       Literal.create("hello", StringType),
   *       Literal.create(3.14, DoubleType)))
   * }}}
   * and the path when we stop the discovery is:
   * {{{
   *   hdfs://<host>:<port>/path/to/partition
   * }}}
   */
  private[datasources] def parsePartition(
      path: Path,
      typeInference: Boolean,
      basePaths: Set[Path],
      timeZone: TimeZone): (Option[PartitionValues], Option[Path]) = {
    val columns = ArrayBuffer.empty[(String, Literal)]
    // Old Hadoop versions don't have `Path.isRoot`
    var finished = path.getParent == null
    // currentPath is the current path that we will use to parse partition column value.
    var currentPath: Path = path

    while (!finished) {
      // Sometimes (e.g., when speculative task is enabled), temporary directories may be left
      // uncleaned. Here we simply ignore them.
      if (currentPath.getName.toLowerCase(Locale.ROOT) == "_temporary") {
        return (None, None)
      }

      if (basePaths.contains(currentPath)) {
        // If the currentPath is one of base paths. We should stop.
        finished = true
      } else {
        // Let's say currentPath is a path of "/table/a=1/", currentPath.getName will give us a=1.
        // Once we get the string, we try to parse it and find the partition column and value.
        val maybeColumn =
          parsePartitionColumn(currentPath.getName, typeInference, timeZone)
        maybeColumn.foreach(columns += _)

        // Now, we determine if we should stop.
        // When we hit any of the following cases, we will stop:
        //  - In this iteration, we could not parse the value of partition column and value,
        //    i.e. maybeColumn is None, and columns is not empty. At here we check if columns is
        //    empty to handle cases like /table/a=1/_temporary/something (we need to find a=1 in
        //    this case).
        //  - After we get the new currentPath, this new currentPath represent the top level dir
        //    i.e. currentPath.getParent == null. For the example of "/table/a=1/",
        //    the top level dir is "/table".
        finished =
          (maybeColumn.isEmpty && !columns.isEmpty) || currentPath.getParent == null

        if (!finished) {
          // For the above example, currentPath will be "/table/".
          currentPath = currentPath.getParent
        }
      }
    }

    if (columns.isEmpty) {
      (None, Some(path))
    } else {
      val (columnNames, values) = columns.reverse.unzip
      (Some(PartitionValues(columnNames, values)), Some(currentPath))
    }
  }

  private def parsePartitionColumn(
      columnSpec: String,
      typeInference: Boolean,
      timeZone: TimeZone): Option[(String, Literal)] = {
    val equalSignIndex = columnSpec.indexOf('=')
    if (equalSignIndex == -1) {
      None
    } else {
      val columnName = unescapePathName(columnSpec.take(equalSignIndex))
      assert(columnName.nonEmpty, s"Empty partition column name in '$columnSpec'")

      val rawColumnValue = columnSpec.drop(equalSignIndex + 1)
      assert(rawColumnValue.nonEmpty, s"Empty partition column value in '$columnSpec'")

      val literal = inferPartitionColumnValue(rawColumnValue, typeInference, timeZone)
      Some(columnName -> literal)
    }
  }

  /**
   * Given a partition path fragment, e.g. `fieldOne=1/fieldTwo=2`, returns a parsed spec
   * for that fragment as a `TablePartitionSpec`, e.g. `Map(("fieldOne", "1"), ("fieldTwo", "2"))`.
   */
  def parsePathFragment(pathFragment: String): TablePartitionSpec = {
    parsePathFragmentAsSeq(pathFragment).toMap
  }

  /**
   * Given a partition path fragment, e.g. `fieldOne=1/fieldTwo=2`, returns a parsed spec
   * for that fragment as a `Seq[(String, String)]`, e.g.
   * `Seq(("fieldOne", "1"), ("fieldTwo", "2"))`.
   */
  def parsePathFragmentAsSeq(pathFragment: String): Seq[(String, String)] = {
    pathFragment.split("/").map { kv =>
      val pair = kv.split("=", 2)
      (unescapePathName(pair(0)), unescapePathName(pair(1)))
    }
  }

  /**
   * This is the inverse of parsePathFragment().
   */
  def getPathFragment(spec: TablePartitionSpec, partitionSchema: StructType): String = {
    partitionSchema.map { field =>
      escapePathName(field.name) + "=" + escapePathName(spec(field.name))
    }.mkString("/")
  }

  /**
   * Normalize the column names in partition specification, w.r.t. the real partition column names
   * and case sensitivity. e.g., if the partition spec has a column named `monTh`, and there is a
   * partition column named `month`, and it's case insensitive, we will normalize `monTh` to
   * `month`.
   */
  def normalizePartitionSpec[T](
      partitionSpec: Map[String, T],
      partColNames: Seq[String],
      tblName: String,
      resolver: Resolver): Map[String, T] = {
    val normalizedPartSpec = partitionSpec.toSeq.map { case (key, value) =>
      val normalizedKey = partColNames.find(resolver(_, key)).getOrElse {
        throw new AnalysisException(s"$key is not a valid partition column in table $tblName.")
      }
      normalizedKey -> value
    }

    if (normalizedPartSpec.map(_._1).distinct.length != normalizedPartSpec.length) {
      val duplicateColumns = normalizedPartSpec.map(_._1).groupBy(identity).collect {
        case (x, ys) if ys.length > 1 => x
      }
      throw new AnalysisException(s"Found duplicated columns in partition specification: " +
        duplicateColumns.mkString(", "))
    }

    normalizedPartSpec.toMap
  }

  /**
   * Resolves possible type conflicts between partitions by up-casting "lower" types.  The up-
   * casting order is:
   * {{{
   *   NullType ->
   *   IntegerType -> LongType ->
   *   DoubleType -> StringType
   * }}}
   */
  def resolvePartitions(
      pathsWithPartitionValues: Seq[(Path, PartitionValues)]): Seq[PartitionValues] = {
    if (pathsWithPartitionValues.isEmpty) {
      Seq.empty
    } else {
      // TODO: Selective case sensitivity.
      val distinctPartColNames =
        pathsWithPartitionValues.map(_._2.columnNames.map(_.toLowerCase())).distinct
      assert(
        distinctPartColNames.size == 1,
        listConflictingPartitionColumns(pathsWithPartitionValues))

      // Resolves possible type conflicts for each column
      val values = pathsWithPartitionValues.map(_._2)
      val columnCount = values.head.columnNames.size
      val resolvedValues = (0 until columnCount).map { i =>
        resolveTypeConflicts(values.map(_.literals(i)))
      }

      // Fills resolved literals back to each partition
      values.zipWithIndex.map { case (d, index) =>
        d.copy(literals = resolvedValues.map(_(index)))
      }
    }
  }

  private[datasources] def listConflictingPartitionColumns(
      pathWithPartitionValues: Seq[(Path, PartitionValues)]): String = {
    val distinctPartColNames = pathWithPartitionValues.map(_._2.columnNames).distinct

    def groupByKey[K, V](seq: Seq[(K, V)]): Map[K, Iterable[V]] =
      seq.groupBy { case (key, _) => key }.mapValues(_.map { case (_, value) => value })

    val partColNamesToPaths = groupByKey(pathWithPartitionValues.map {
      case (path, partValues) => partValues.columnNames -> path
    })

    val distinctPartColLists = distinctPartColNames.map(_.mkString(", ")).zipWithIndex.map {
      case (names, index) =>
        s"Partition column name list #$index: $names"
    }

    // Lists out those non-leaf partition directories that also contain files
    val suspiciousPaths = distinctPartColNames.sortBy(_.length).flatMap(partColNamesToPaths)

    s"Conflicting partition column names detected:\n" +
      distinctPartColLists.mkString("\n\t", "\n\t", "\n\n") +
      "For partitioned table directories, data files should only live in leaf directories.\n" +
      "And directories at the same level should have the same partition column name.\n" +
      "Please check the following directories for unexpected files or " +
      "inconsistent partition column names:\n" +
      suspiciousPaths.map("\t" + _).mkString("\n", "\n", "")
  }

  /**
   * Converts a string to a [[Literal]] with automatic type inference.  Currently only supports
   * [[IntegerType]], [[LongType]], [[DoubleType]], [[DecimalType]], [[DateType]]
   * [[TimestampType]], and [[StringType]].
   */
  private[datasources] def inferPartitionColumnValue(
      raw: String,
      typeInference: Boolean,
      timeZone: TimeZone): Literal = {
    val decimalTry = Try {
      // `BigDecimal` conversion can fail when the `field` is not a form of number.
      val bigDecimal = new JBigDecimal(raw)
      // It reduces the cases for decimals by disallowing values having scale (eg. `1.1`).
      require(bigDecimal.scale <= 0)
      // `DecimalType` conversion can fail when
      //   1. The precision is bigger than 38.
      //   2. scale is bigger than precision.
      Literal(bigDecimal)
    }

    if (typeInference) {
      // First tries integral types
      Try(Literal.create(Integer.parseInt(raw), IntegerType))
        .orElse(Try(Literal.create(JLong.parseLong(raw), LongType)))
        .orElse(decimalTry)
        // Then falls back to fractional types
        .orElse(Try(Literal.create(JDouble.parseDouble(raw), DoubleType)))
        // Then falls back to date/timestamp types
        .orElse(Try(
          Literal.create(
            DateTimeUtils.getThreadLocalTimestampFormat(timeZone)
              .parse(unescapePathName(raw)).getTime * 1000L,
            TimestampType)))
        .orElse(Try(
          Literal.create(
            DateTimeUtils.millisToDays(
              DateTimeUtils.getThreadLocalDateFormat.parse(raw).getTime),
            DateType)))
        // Then falls back to string
        .getOrElse {
          if (raw == DEFAULT_PARTITION_NAME) {
            Literal.create(null, NullType)
          } else {
            Literal.create(unescapePathName(raw), StringType)
          }
        }
    } else {
      if (raw == DEFAULT_PARTITION_NAME) {
        Literal.create(null, NullType)
      } else {
        Literal.create(unescapePathName(raw), StringType)
      }
    }
  }

  private val upCastingOrder: Seq[DataType] =
    Seq(NullType, IntegerType, LongType, FloatType, DoubleType, StringType)

  def validatePartitionColumn(
      schema: StructType,
      partitionColumns: Seq[String],
      caseSensitive: Boolean): Unit = {

    partitionColumnsSchema(schema, partitionColumns, caseSensitive).foreach {
      field => field.dataType match {
        case _: AtomicType => // OK
        case _ => throw new AnalysisException(s"Cannot use ${field.dataType} for partition column")
      }
    }

    if (partitionColumns.nonEmpty && partitionColumns.size == schema.fields.length) {
      throw new AnalysisException(s"Cannot use all columns for partition columns")
    }
  }

  def partitionColumnsSchema(
      schema: StructType,
      partitionColumns: Seq[String],
      caseSensitive: Boolean): StructType = {
    val equality = columnNameEquality(caseSensitive)
    StructType(partitionColumns.map { col =>
      schema.find(f => equality(f.name, col)).getOrElse {
        throw new AnalysisException(s"Partition column $col not found in schema $schema")
      }
    }).asNullable
  }

  private def columnNameEquality(caseSensitive: Boolean): (String, String) => Boolean = {
    if (caseSensitive) {
      org.apache.spark.sql.catalyst.analysis.caseSensitiveResolution
    } else {
      org.apache.spark.sql.catalyst.analysis.caseInsensitiveResolution
    }
  }

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
      Literal.create(Cast(l, desiredType).eval(), desiredType)
    }
  }
}
