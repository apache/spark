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

package org.apache.spark.sql.sources

import java.lang.{Double => JDouble, Float => JFloat, Long => JLong}
import java.math.{BigDecimal => JBigDecimal}

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

import com.google.common.cache.{CacheBuilder, Cache}
import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{Cast, Literal}
import org.apache.spark.sql.types._

private[sql] case class Partition(values: Row, path: String)

private[sql] case class PartitionSpec(partitionColumns: StructType, partitions: Seq[Partition])

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
   *       Literal.create(42, IntegerType),
   *       Literal.create("hello", StringType),
   *       Literal.create(3.14, FloatType)))
   * }}}
   */
  private[sql] def parsePartition(
      path: Path,
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

  private def parsePartitionColumn(
      columnSpec: String,
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
   * Resolves possible type conflicts between partitions by up-casting "lower" types.  The up-
   * casting order is:
   * {{{
   *   NullType ->
   *   IntegerType -> LongType ->
   *   FloatType -> DoubleType -> DecimalType.Unlimited ->
   *   StringType
   * }}}
   */
  private[sql] def resolvePartitions(values: Seq[PartitionValues]): Seq[PartitionValues] = {
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

  /**
   * Converts a string to a `Literal` with automatic type inference.  Currently only supports
   * [[IntegerType]], [[LongType]], [[FloatType]], [[DoubleType]], [[DecimalType.Unlimited]], and
   * [[StringType]].
   */
  private[sql] def inferPartitionColumnValue(
      raw: String,
      defaultPartitionName: String): Literal = {
    // First tries integral types
    Try(Literal.create(Integer.parseInt(raw), IntegerType))
      .orElse(Try(Literal.create(JLong.parseLong(raw), LongType)))
      // Then falls back to fractional types
      .orElse(Try(Literal.create(JFloat.parseFloat(raw), FloatType)))
      .orElse(Try(Literal.create(JDouble.parseDouble(raw), DoubleType)))
      .orElse(Try(Literal.create(new JBigDecimal(raw), DecimalType.Unlimited)))
      // Then falls back to string
      .getOrElse {
        if (raw == defaultPartitionName) Literal.create(null, NullType)
        else Literal.create(raw, StringType)
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
      Literal.create(Cast(l, desiredType).eval(), desiredType)
    }
  }
}
