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
package org.apache.spark.sql.execution.datasources.xml.util

import java.io.CharArrayWriter
import java.nio.charset.Charset
import javax.xml.stream.XMLOutputFactory

import scala.collection.Map

import com.sun.xml.txw2.output.IndentingXMLStreamWriter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.io.compress.CompressionCodec

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.util.CompressionCodecs
import org.apache.spark.sql.execution.datasources.xml.{XmlInputFormat, XmlOptions}
import org.apache.spark.sql.execution.datasources.xml.parsers.StaxXmlGenerator

private[xml] object XmlFile {
  val DEFAULT_INDENT = "    "

  def withCharset(
      context: SparkContext,
      location: String,
      charset: String,
      rowTag: String): RDD[String] = {
    // This just checks the charset's validity early, to keep behavior
    Charset.forName(charset)
    val config = new Configuration(context.hadoopConfiguration)
    config.set(XmlInputFormat.START_TAG_KEY, s"<$rowTag>")
    config.set(XmlInputFormat.END_TAG_KEY, s"</$rowTag>")
    config.set(XmlInputFormat.ENCODING_KEY, charset)
    context.newAPIHadoopFile(location,
      classOf[XmlInputFormat],
      classOf[LongWritable],
      classOf[Text],
      config).map { case (_, text) => text.toString }
  }

  /**
   * Note that writing a XML file from [[DataFrame]] having a field
   * [[org.apache.spark.sql.types.ArrayType]] with its element as nested array would have
   * an additional nested field for the element. For example, the [[DataFrame]] having
   * a field below,
   *
   *   fieldA Array(Array(data1, data2))
   *
   * would produce a XML file below.
   *
   * <fieldA>
   *     <item>data1</item>
   * </fieldA>
   * <fieldA>
   *     <item>data2</item>
   * </fieldA>
   *
   * Namely, roundtrip in writing and reading can end up in different schema structure.
   */
  def saveAsXmlFile(
      dataFrame: DataFrame,
      path: String,
      parameters: Map[String, String] = Map()): Unit = {
    val options = XmlOptions(parameters.toMap)
    val codec = Option(options.codec).map(CompressionCodecs.getCodecClassName)
    // scalastyle:off classforname
    val codecClass: Option[Class[_ <: CompressionCodec]] =
      codec.map(Class.forName(_).asInstanceOf[Class[CompressionCodec]])
    val rowSchema = dataFrame.schema
    val indent = XmlFile.DEFAULT_INDENT

    // Allow a root tag to be like "rootTag foo='bar'"
    // This is hacky; won't deal correctly with spaces in attributes, but want
    // to make this at least work for simple cases without much complication
    val rootTagTokens = options.rootTag.split(" ")
    val rootElementName = rootTagTokens.head
    val rootAttributes: Map[String, String] =
      if (rootTagTokens.length > 1) {
        rootTagTokens.tail.map { kv =>
          val Array(k, v) = kv.split("=")
          k -> v.replaceAll("['\"]", "")
        }.toMap
      } else {
        Map.empty
      }
    val declaration = options.declaration

    val xmlRDD = dataFrame.rdd.mapPartitions { iter =>
      val factory = XMLOutputFactory.newInstance()
      val writer = new CharArrayWriter()
      val xmlWriter = factory.createXMLStreamWriter(writer)
      val indentingXmlWriter = new IndentingXMLStreamWriter(xmlWriter)
      indentingXmlWriter.setIndentStep(indent)

      new Iterator[String] {
        var firstRow: Boolean = true
        var lastRow: Boolean = true

        override def hasNext: Boolean = iter.hasNext || firstRow || lastRow

        override def next(): String = {
          if (iter.nonEmpty) {
            if (firstRow) {
              if (declaration != null && declaration.nonEmpty) {
                indentingXmlWriter.writeProcessingInstruction("xml", declaration)
                indentingXmlWriter.writeCharacters("\n")
              }
              indentingXmlWriter.writeStartElement(rootElementName)
              rootAttributes.foreach { case (k, v) =>
                indentingXmlWriter.writeAttribute(k, v)
              }
              firstRow = false
            }
            val xml = {
              StaxXmlGenerator(
                rowSchema,
                indentingXmlWriter,
                options)(iter.next())
              indentingXmlWriter.flush()
              writer.toString
            }
            writer.reset()
            xml
          } else {
            if (!firstRow) {
              lastRow = false
              indentingXmlWriter.writeEndElement()
              indentingXmlWriter.close()
              writer.toString
            } else {
              // This means the iterator was initially empty.
              firstRow = false
              lastRow = false
              ""
            }
          }
        }
      }
    }

    codecClass match {
      case None => xmlRDD.saveAsTextFile(path)
      case Some(codec) => xmlRDD.saveAsTextFile(path, codec)
    }
  }
}
