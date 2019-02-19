package org.apache.spark.graph.cypher.udfs

import org.apache.spark.sql.catalyst.analysis.UnresolvedExtractValue
import org.apache.spark.sql.catalyst.expressions.{ArrayContains, StringTranslate, XxHash64}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{monotonically_increasing_id, udf}
import org.apache.spark.sql.types.{ArrayType, StringType}
import org.apache.spark.sql.{Column, functions}

object LegacyUdfs {

  implicit class RichColumn(column: Column) {

    /**
      * This is a copy of {{{org.apache.spark.sql.Column#getItem}}}. The original method only allows fixed
      * values (Int, or String) as index although the underlying implementation seem capable of processing arbitrary
      * expressions. This method exposes these features
      */
    def get(idx: Column): Column =
      new Column(UnresolvedExtractValue(column.expr, idx.expr))
  }

  val rangeUdf: UserDefinedFunction =
    udf[Array[Int], Int, Int, Int]((from: Int, to: Int, step: Int) => from.to(to, step).toArray)

  private[spark] val rowIdSpaceBitsUsedByMonotonicallyIncreasingId = 33

  /**
    * Configurable wrapper around `monotonically_increasing_id`
    *
    * @param partitionStartDelta Conceptually this number is added to the `partitionIndex` from which the Spark function
    *                            `monotonically_increasing_id` starts assigning IDs.
    */
  // TODO: Document inherited limitations with regard to the maximum number of rows per data frame
  // TODO: Document the maximum number of partitions (before entering tag space)
  def partitioned_id_assignment(partitionStartDelta: Int): Column =
    monotonically_increasing_id() + (partitionStartDelta.toLong << rowIdSpaceBitsUsedByMonotonicallyIncreasingId)

  /**
    * Alternative version of `array_contains` that takes a column as the value.
    */
  def array_contains(column: Column, value: Column): Column =
    new Column(ArrayContains(column.expr, value.expr))

  def hash64(columns: Column*): Column =
    new Column(new XxHash64(columns.map(_.expr)))

  def array_append_long(array: Column, value: Column): Column =
    appendLongUDF(array, value)

  private val appendLongUDF =
    functions.udf(appendLong _)

  private def appendLong(array: Seq[Long], element: Long): Seq[Long] =
    array :+ element

  def get_rel_type(relTypeNames: Seq[String]): UserDefinedFunction = {
    val extractRelTypes = (booleanMask: Seq[Boolean]) => filterWithMask(relTypeNames)(booleanMask)
    functions.udf(extractRelTypes.andThen(_.headOption.orNull), StringType)
  }

  def get_node_labels(labelNames: Seq[String]): UserDefinedFunction =
    functions.udf(filterWithMask(labelNames) _, ArrayType(StringType, containsNull = false))

  private def filterWithMask(dataToFilter: Seq[String])(mask: Seq[Boolean]): Seq[String] =
    dataToFilter.zip(mask).collect {
      case (label, true) => label
    }

  def get_property_keys(propertyKeys: Seq[String]): UserDefinedFunction =
    functions.udf(filterNotNull(propertyKeys) _, ArrayType(StringType, containsNull = false))

  private def filterNotNull(dataToFilter: Seq[String])(values: Seq[Any]): Seq[String] =
    dataToFilter.zip(values).collect {
      case (key, value) if value != null => key
    }

  /**
    * Alternative version of {{{org.apache.spark.sql.functions.translate}}} that takes {{{org.apache.spark.sql.Column}}}s for search and replace strings.
    */
  def translateColumn(src: Column, matchingString: Column, replaceString: Column): Column = {
    new Column(StringTranslate(src.expr, matchingString.expr, replaceString.expr))
  }

}

