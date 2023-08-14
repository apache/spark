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
package org.apache.spark.sql.execution.datasources.xml

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, SQLContext}
import org.apache.spark.sql.catalyst.util.FailFastMode
import org.apache.spark.sql.execution.datasources.xml.util.XmlFile
import org.apache.spark.sql.types.StructType

/**
 * A collection of static functions for working with XML files in Spark SQL
 */
class XmlReader(private var schema: StructType,
                private val options: Map[String, Any]) extends Serializable {

  private val parameters = collection.mutable.Map.empty[String, String]
  options.foreach { case (k, v) => parameters(k) = v.toString }

  // Explicit constructors for Java compatibility

  def this() = {
    this(null, Map.empty)
  }

  def this(schema: StructType) = {
    this(schema, Map.empty)
  }

  def this(options: Map[String, Any]) = {
    this(null, options)
  }

  @deprecated("Use XmlReader(Map) with key 'charset' to specify options", "0.13.0")
  def withCharset(charset: String): XmlReader = {
    parameters += ("charset" -> charset)
    this
  }

  @deprecated("Use XmlReader(Map) with key 'codec' to specify options", "0.13.0")
  def withCompression(codec: String): XmlReader = {
    parameters += ("codec" -> codec)
    this
  }

  @deprecated("Use XmlReader(Map) with key 'rowTag' to specify options", "0.13.0")
  def withRowTag(rowTag: String): XmlReader = {
    parameters += ("rowTag" -> rowTag)
    this
  }

  @deprecated("Use XmlReader(Map) with key 'samplingRatio' to specify options", "0.13.0")
  def withSamplingRatio(samplingRatio: Double): XmlReader = {
    parameters += ("samplingRatio" -> samplingRatio.toString)
    this
  }

  @deprecated("Use XmlReader(Map) with key 'excludeAttribute' to specify options", "0.13.0")
  def withExcludeAttribute(exclude: Boolean): XmlReader = {
    parameters += ("excludeAttribute" -> exclude.toString)
    this
  }

  @deprecated("Use XmlReader(Map) with key 'treatEmptyValuesAsNulls' to specify options", "0.13.0")
  def withTreatEmptyValuesAsNulls(treatAsNull: Boolean): XmlReader = {
    parameters += ("treatEmptyValuesAsNulls" -> treatAsNull.toString)
    this
  }

  @deprecated("Use XmlReader(Map) with key 'mode' as 'FAILFAST' to specify options", "0.10.0")
  def withFailFast(failFast: Boolean): XmlReader = {
    if (failFast) {
      parameters += ("mode" -> FailFastMode.name)
    } else {
      parameters -= "mode"
    }
    this
  }

  @deprecated("Use XmlReader(Map) with key 'mode' to specify options", "0.13.0")
  def withParseMode(mode: String): XmlReader = {
    parameters += ("mode" -> mode)
    this
  }

  @deprecated("Use XmlReader(Map) with key 'attributePrefix' to specify options", "0.13.0")
  def withAttributePrefix(attributePrefix: String): XmlReader = {
    parameters += ("attributePrefix" -> attributePrefix)
    this
  }

  @deprecated("Use XmlReader(Map) with key 'valueTag' to specify options", "0.13.0")
  def withValueTag(valueTag: String): XmlReader = {
    parameters += ("valueTag" -> valueTag)
    this
  }

  @deprecated("Use XmlReader(Map) with key 'columnNameOfCorruptRecord' to specify options",
              "0.13.0")
  def withColumnNameOfCorruptRecord(name: String): XmlReader = {
    parameters += ("columnNameOfCorruptRecord" -> name)
    this
  }

  @deprecated("Use XmlReader(Map) with key 'ignoreSurroundingSpaces' to specify options", "0.13.0")
  def withIgnoreSurroundingSpaces(ignore: Boolean): XmlReader = {
    parameters += ("ignoreSurroundingSpaces" -> ignore.toString)
    this
  }

  @deprecated("Use XmlReader(StructType) to specify schema", "0.13.0")
  def withSchema(schema: StructType): XmlReader = {
    this.schema = schema
    this
  }

  @deprecated("Use XmlReader(Map) with key 'rowValidationXSDPath' to specify options", "0.13.0")
  def withRowValidationXSDPath(path: String): XmlReader = {
    parameters += ("rowValidationXSDPath" -> path)
    this
  }

  /**
   * @param spark current SparkSession
   * @param path path to XML files to parse
   * @return XML parsed as a DataFrame
   */
  def xmlFile(spark: SparkSession, path: String): DataFrame = {
    // We need the `charset` and `rowTag` before creating the relation.
    val (charset, rowTag) = {
      val options = XmlOptions(parameters.toMap)
      (options.charset, options.rowTag)
    }
    val relation = XmlRelation(
      () => XmlFile.withCharset(spark.sparkContext, path, charset, rowTag),
      Some(path),
      parameters.toMap,
      schema)(spark.sqlContext)
    spark.baseRelationToDataFrame(relation)
  }

  /**
   * @param spark current SparkSession
   * @param ds XML for individual 'rows' as Strings
   * @return XML parsed as a DataFrame
   */
  def xmlDataset(spark: SparkSession, ds: Dataset[String]): DataFrame = {
    xmlRdd(spark, ds.rdd)
  }

  /**
   * @param spark current SparkSession
   * @param xmlRDD XML for individual 'rows' as Strings
   * @return XML parsed as a DataFrame
   */
  def xmlRdd(spark: SparkSession, xmlRDD: RDD[String]): DataFrame = {
    val relation = XmlRelation(
      () => xmlRDD,
      None,
      parameters.toMap,
      schema)(spark.sqlContext)
    spark.baseRelationToDataFrame(relation)
  }

  /** Returns a Schema RDD for the given XML path. */
  @deprecated("Use xmlFile(SparkSession, ...)", "0.5.0")
  def xmlFile(sqlContext: SQLContext, path: String): DataFrame = {
    // We need the `charset` and `rowTag` before creating the relation.
    val (charset, rowTag) = {
      val options = XmlOptions(parameters.toMap)
      (options.charset, options.rowTag)
    }
    val relation = XmlRelation(
      () => XmlFile.withCharset(sqlContext.sparkContext, path, charset, rowTag),
      Some(path),
      parameters.toMap,
      schema)(sqlContext)
    sqlContext.baseRelationToDataFrame(relation)
  }

  @deprecated("Use xmlRdd(SparkSession, ...)", "0.5.0")
  def xmlRdd(sqlContext: SQLContext, xmlRDD: RDD[String]): DataFrame = {
    val relation = XmlRelation(
      () => xmlRDD,
      None,
      parameters.toMap,
      schema)(sqlContext)
    sqlContext.baseRelationToDataFrame(relation)
  }

}
