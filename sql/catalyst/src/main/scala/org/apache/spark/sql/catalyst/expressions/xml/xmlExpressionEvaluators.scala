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

package org.apache.spark.sql.catalyst.expressions.xml

import java.io.CharArrayWriter

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.xml.{StaxXmlGenerator, XmlOptions}
import org.apache.spark.sql.types.StructType

class StructsToXmlEvaluator(
    inputSchema: StructType,
    options: Map[String, String],
    timeZoneId: Option[String] = None) extends Serializable {

  @transient
  private lazy val writer = new CharArrayWriter()

  @transient
  private lazy val gen = new StaxXmlGenerator(
    inputSchema, writer, new XmlOptions(options, timeZoneId.get), false)

  // This converts rows to the XML output according to the given schema.
  @transient
  private lazy val converter: Any => String = {
    def getAndReset(): String = {
      gen.flush()
      val xmlString = writer.toString
      writer.reset()
      xmlString
    }

    (row: Any) =>
      gen.write(row.asInstanceOf[InternalRow])
      getAndReset()
  }

  final def evaluate(input: Any): String = {
    if (input == null) return null
    converter(input)
  }
}
