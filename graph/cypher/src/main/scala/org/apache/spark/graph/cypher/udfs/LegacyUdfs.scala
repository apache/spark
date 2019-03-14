package org.apache.spark.graph.cypher.udfs

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions
import org.apache.spark.sql.types.{ArrayType, StringType}

object LegacyUdfs {

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
}
