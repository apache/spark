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

import java.nio.charset.Charset
import javax.xml.stream.XMLOutputFactory

import com.sun.xml.txw2.output.IndentingXMLStreamWriter
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.TaskAttemptContext

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.{xml, InternalRow}
import org.apache.spark.sql.catalyst.xml.XmlOptions
import org.apache.spark.sql.execution.datasources.{CodecStreams, OutputWriter}
import org.apache.spark.sql.types.StructType

class XmlOutputWriter(
    val path: String,
    dataSchema: StructType,
    context: TaskAttemptContext,
    options: XmlOptions) extends OutputWriter with Logging {

  private val DEFAULT_INDENT = "    "
  private val charset = Charset.forName(options.charset)

  private val writer = CodecStreams.createOutputStreamWriter(context, new Path(path), charset)
  private val factory = XMLOutputFactory.newInstance()
  private val xmlWriter = factory.createXMLStreamWriter(writer)
  private val indentingXmlWriter = new IndentingXMLStreamWriter(xmlWriter)
  indentingXmlWriter.setIndentStep(DEFAULT_INDENT)

  // Allow a root tag to be like "rootTag foo='bar'"
  // This is hacky; won't deal correctly with spaces in attributes, but want
  // to make this at least work for simple cases without much complication
  private val rootTagTokens = options.rootTag.split(" ")
  private val rootElementName = rootTagTokens.head
  private val rootAttributes: Map[String, String] =
    if (rootTagTokens.length > 1) {
      rootTagTokens.tail.map { kv =>
        val Array(k, v) = kv.split("=")
        k -> v.replaceAll("['\"]", "")
      }.toMap
    } else {
      Map.empty
    }
  private val declaration = options.declaration


  // private val gen = new UnivocityGenerator(dataSchema, writer, params)

  private var firstRow: Boolean = true

  override def write(row: InternalRow): Unit = {
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
    xml.StaxXmlGenerator(
      dataSchema,
      indentingXmlWriter,
      options)(row)
  }

  override def close(): Unit = {
    if (!firstRow) {
      indentingXmlWriter.writeEndElement()
      indentingXmlWriter.close()
    }
    writer.close()
  }
}
