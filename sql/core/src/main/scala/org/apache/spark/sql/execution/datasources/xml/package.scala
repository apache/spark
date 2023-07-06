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

import org.apache.hadoop.io.compress.CompressionCodec

import org.apache.spark.sql.execution.datasources.xml.parsers.StaxXmlParser
import org.apache.spark.sql.execution.datasources.xml.util.{InferSchema, XmlFile}
import org.apache.spark.sql.types.{ArrayType, StructType}

package object xml {
  /**
   * Adds a method, `xmlFile`, to [[SQLContext]] that allows reading XML data.
   */
  implicit class XmlContext(sqlContext: SQLContext) extends Serializable {
    @deprecated("Use read.format(\"xml\") or read.xml", "0.4.0")
    def xmlFile(
        filePath: String,
        rowTag: String = XmlOptions.DEFAULT_ROW_TAG,
        samplingRatio: Double = 1.0,
        excludeAttribute: Boolean = false,
        treatEmptyValuesAsNulls: Boolean = false,
        failFast: Boolean = false,
        attributePrefix: String = XmlOptions.DEFAULT_ATTRIBUTE_PREFIX,
        valueTag: String = XmlOptions.DEFAULT_VALUE_TAG,
        charset: String = XmlOptions.DEFAULT_CHARSET): DataFrame = {

      val parameters = Map(
        "rowTag" -> rowTag,
        "samplingRatio" -> samplingRatio.toString,
        "excludeAttribute" -> excludeAttribute.toString,
        "treatEmptyValuesAsNulls" -> treatEmptyValuesAsNulls.toString,
        "failFast" -> failFast.toString,
        "attributePrefix" -> attributePrefix,
        "valueTag" -> valueTag,
        "charset" -> charset)
      val xmlRelation = XmlRelation(
        () => XmlFile.withCharset(sqlContext.sparkContext, filePath, charset, rowTag),
        location = Some(filePath),
        parameters = parameters)(sqlContext)
      sqlContext.baseRelationToDataFrame(xmlRelation)
    }
  }

  /**
   * Adds a method, `saveAsXmlFile`, to [[DataFrame]] that allows writing XML data.
   * If compressionCodec is not null the resulting output will be compressed.
   * Note that a codec entry in the parameters map will be ignored.
   */
  implicit class XmlSchemaRDD(dataFrame: DataFrame) {
    @deprecated("Use write.format(\"xml\") or write.xml", "0.4.0")
    def saveAsXmlFile(
        path: String, parameters: scala.collection.Map[String, String] = Map(),
        compressionCodec: Class[_ <: CompressionCodec] = null): Unit = {
      val mutableParams = collection.mutable.Map(parameters.toSeq: _*)
      val safeCodec = mutableParams.get("codec")
        .orElse(Option(compressionCodec).map(_.getCanonicalName))
        .orNull
      mutableParams.put("codec", safeCodec)
      XmlFile.saveAsXmlFile(dataFrame, path, mutableParams.toMap)
    }
  }

  /**
   * Adds a method, `xml`, to DataFrameReader that allows you to read XML files using
   * the DataFileReader
   */
  implicit class XmlDataFrameReader(reader: DataFrameReader) {
    def xml: String => DataFrame = reader.format("org.apache.spark.sql.xml").load

    @deprecated("Use XmlReader directly", "0.13.0")
    def xml(xmlDataset: Dataset[String]): DataFrame = {
      val spark = SparkSession.builder().getOrCreate()
      new XmlReader().xmlDataset(spark, xmlDataset)
    }
  }

  /**
   * Adds a method, `xml`, to DataFrameWriter that allows you to write XML files using
   * the DataFileWriter
   */
  implicit class XmlDataFrameWriter[T](writer: DataFrameWriter[T]) {
    // Note that writing a XML file from [[DataFrame]] having a field [[ArrayType]] with
    // its element as [[ArrayType]] would have an additional nested field for the element.
    // For example, the [[DataFrame]] having a field below,
    //
    //   fieldA [[data1, data2]]
    //
    // would produce a XML file below.
    //
    //   <fieldA>
    //       <item>data1</item>
    //   </fieldA>
    //   <fieldA>
    //       <item>data2</item>
    //   </fieldA>
    //
    // Namely, roundtrip in writing and reading can end up in different schema structure.
    def xml: String => Unit = writer.format("org.apache.spark.sql.xml").save
  }

  /**
   * Infers the schema of XML documents as strings.
   *
   * @param ds Dataset of XML strings
   * @param options additional XML parsing options
   * @return inferred schema for XML
   */
  def schema_of_xml(ds: Dataset[String], options: Map[String, String] = Map.empty): StructType =
    InferSchema.infer(ds.rdd, XmlOptions(options))

  /**
   * Infers the schema of XML documents as strings.
   *
   * @param df one-column DataFrame of XML strings
   * @param options additional XML parsing options
   * @return inferred schema for XML
   */
  def schema_of_xml_df(df: DataFrame, options: Map[String, String] = Map.empty): StructType =
    schema_of_xml(df.as[String](Encoders.STRING), options)

  /**
   * Infers the schema of XML documents when inputs are arrays of strings, each an XML doc.
   *
   * @param ds Dataset of XML strings
   * @param options additional XML parsing options
   * @return inferred schema for XML. Will be an ArrayType[StructType].
   */
  def schema_of_xml_array(ds: Dataset[Array[String]],
                          options: Map[String, String] = Map.empty): ArrayType =
    ArrayType(InferSchema.infer(ds.rdd.flatMap(a => a), XmlOptions(options)))

  /**
   * @param xml XML document to parse, as string
   * @param schema the schema to use when parsing the XML string
   * @param options key-value pairs that correspond to those supported by [[XmlOptions]]
   * @return [[Row]] representing the parsed XML structure
   */
  def from_xml_string(xml: String, schema: StructType,
                      options: Map[String, String] = Map.empty): Row = {
    StaxXmlParser.parseColumn(xml, schema, XmlOptions(options))
  }

}
