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

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

import org.apache.hadoop.fs.Path
import org.apache.hadoop.util.Shell

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Cast, Literal}
import org.apache.spark.sql.types._


private[sql] case class Partition(values: InternalRow, path: String)

private[sql] case class PartitionSpec(partitionColumns: StructType, partitions: Seq[Partition])

private[sql] object PartitionSpec {
  val emptySpec = PartitionSpec(StructType(Seq.empty[StructField]), Seq.empty[Partition])
}

private[sql] object PartitioningUtils {
  // This duplicates default value of Hive `ConfVars.DEFAULTPARTITIONNAME`, since sql/core doesn't
  // depend on Hive.
  private[sql] val DEFAULT_PARTITION_NAME = "__HIVE_DEFAULT_PARTITION__"

  private[sql] case class PartitionValues(columnNames: Seq[String], literals: Seq[Literal]) {
    require(columnNames.size == literals.size)
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
  private[sql] def parsePartitions(
      paths: Seq[Path],
      defaultPartitionName: String,
      typeInference: Boolean): PartitionSpec = {
    // First, we need to parse every partition's path and see if we can find partition values.
    val pathsWithPartitionValues = paths.flatMap { path =>
      parsePartition(path, defaultPartitionName, typeInference).map(path -> _)
    }

    if (pathsWithPartitionValues.isEmpty) {
      // This dataset is not partitioned.
      PartitionSpec.emptySpec
    } else {
      // This dataset is partitioned. We need to check whether all partitions have the same
      // partition columns and resolve potential type conflicts.
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
          Partition(InternalRow.fromSeq(literals.map(_.value)), path.toString)
      }

      PartitionSpec(StructType(fields), partitions)
    }
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
   *       Literal.create(42, IntegerType),
   *       Literal.create("hello", StringType),
   *       Literal.create(3.14, FloatType)))
   * }}}
   */
  private[sql] def parsePartition(
      path: Path,
      defaultPartitionName: String,
      typeInference: Boolean): Option[PartitionValues] = {
    val columns = ArrayBuffer.empty[(String, Literal)]
    // Old Hadoop versions don't have `Path.isRoot`
    var finished = path.getParent == null
    var chopped = path

    while (!finished) {
      // Sometimes (e.g., when speculative task is enabled), temporary directories may be left
      // uncleaned.  Here we simply ignore them.
      if (chopped.getName.toLowerCase == "_temporary") {
        return None
      }

      val maybeColumn = parsePartitionColumn(chopped.getName, defaultPartitionName, typeInference)
      maybeColumn.foreach(columns += _)
      chopped = chopped.getParent
      finished = maybeColumn.isEmpty || chopped.getParent == null
    }

    if (columns.isEmpty) {
      None
    } else {
      val (columnNames, values) = columns.reverse.unzip
      Some(PartitionValues(columnNames, values))
    }
  }

  private def parsePartitionColumn(
      columnSpec: String,
      defaultPartitionName: String,
      typeInference: Boolean): Option[(String, Literal)] = {
    val equalSignIndex = columnSpec.indexOf('=')
    if (equalSignIndex == -1) {
      None
    } else {
      val columnName = columnSpec.take(equalSignIndex)
      assert(columnName.nonEmpty, s"Empty partition column name in '$columnSpec'")

      val rawColumnValue = columnSpec.drop(equalSignIndex + 1)
      assert(rawColumnValue.nonEmpty, s"Empty partition column value in '$columnSpec'")

      val literal = inferPartitionColumnValue(rawColumnValue, defaultPartitionName, typeInference)
      Some(columnName -> literal)
    }
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
  private[sql] def resolvePartitions(
      pathsWithPartitionValues: Seq[(Path, PartitionValues)]): Seq[PartitionValues] = {
    if (pathsWithPartitionValues.isEmpty) {
      Seq.empty
    } else {
      val distinctPartColNames = pathsWithPartitionValues.map(_._2.columnNames).distinct
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

  private[sql] def listConflictingPartitionColumns(
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
   * [[IntegerType]], [[LongType]], [[DoubleType]], [[DecimalType.SYSTEM_DEFAULT]], and
   * [[StringType]].
   */
  private[sql] def inferPartitionColumnValue(
      raw: String,
      defaultPartitionName: String,
      typeInference: Boolean): Literal = {
    if (typeInference) {
      // First tries integral types
      Try(Literal.create(Integer.parseInt(raw), IntegerType))
        .orElse(Try(Literal.create(JLong.parseLong(raw), LongType)))
        // Then falls back to fractional types
        .orElse(Try(Literal.create(JDouble.parseDouble(raw), DoubleType)))
        .orElse(Try(Literal(new JBigDecimal(raw))))
        // Then falls back to string
        .getOrElse {
          if (raw == defaultPartitionName) {
            Literal.create(null, NullType)
          } else {
            Literal.create(unescapePathName(raw), StringType)
          }
        }
    } else {
      if (raw == defaultPartitionName) {
        Literal.create(null, NullType)
      } else {
        Literal.create(unescapePathName(raw), StringType)
      }
    }
  }

  private val upCastingOrder: Seq[DataType] =
    Seq(NullType, IntegerType, LongType, FloatType, DoubleType, StringType)

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

  //////////////////////////////////////////////////////////////////////////////////////////////////
  // The following string escaping code is mainly copied from Hive (o.a.h.h.common.FileUtils).
  //////////////////////////////////////////////////////////////////////////////////////////////////

  val charToEscape = {
    val bitSet = new java.util.BitSet(128)

    /**
     * ASCII 01-1F are HTTP control characters that need to be escaped.
     * \u000A and \u000D are \n and \r, respectively.
     */
    val clist = Array(
      '\u0001', '\u0002', '\u0003', '\u0004', '\u0005', '\u0006', '\u0007', '\u0008', '\u0009',
      '\n', '\u000B', '\u000C', '\r', '\u000E', '\u000F', '\u0010', '\u0011', '\u0012', '\u0013',
      '\u0014', '\u0015', '\u0016', '\u0017', '\u0018', '\u0019', '\u001A', '\u001B', '\u001C',
      '\u001D', '\u001E', '\u001F', '"', '#', '%', '\'', '*', '/', ':', '=', '?', '\\', '\u007F',
      '{', '[', ']', '^')

    clist.foreach(bitSet.set(_))

    if (Shell.WINDOWS) {
      Array(' ', '<', '>', '|').foreach(bitSet.set(_))
    }

    bitSet
  }

  def needsEscaping(c: Char): Boolean = {
    c >= 0 && c < charToEscape.size() && charToEscape.get(c)
  }

  def escapePathName(path: String): String = {
    val builder = new StringBuilder()
    path.foreach { c =>
      if (needsEscaping(c)) {
        builder.append('%')
        builder.append(f"${c.asInstanceOf[Int]}%02x")
      } else {
        builder.append(c)
      }
    }

    builder.toString()
  }

  def unescapePathName(path: String): String = {
    val sb = new StringBuilder
    var i = 0

    while (i < path.length) {
      val c = path.charAt(i)
      if (c == '%' && i + 2 < path.length) {
        val code: Int = try {
          Integer.valueOf(path.substring(i + 1, i + 3), 16)
        } catch { case e: Exception =>
          -1: Integer
        }
        if (code >= 0) {
          sb.append(code.asInstanceOf[Char])
          i += 3
        } else {
          sb.append(c)
          i += 1
        }
      } else {
        sb.append(c)
        i += 1
      }
    }

    sb.toString()
  }
}
