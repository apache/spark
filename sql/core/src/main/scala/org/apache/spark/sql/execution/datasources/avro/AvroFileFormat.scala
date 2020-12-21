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

package org.apache.spark.sql.execution.datasources.avro

import org.apache.avro.SchemaParseException

object AvroFileFormat {
  val IgnoreFilesWithoutExtensionProperty = "avro.mapred.ignore.inputs.without.extension"

  def checkFieldNames(names: Seq[String]): Unit = {
    names.foreach(checkFieldName)
  }

  private def checkFieldName(name: String): Unit = {
    val length = name.length
    if (length == 0) {
      throw new SchemaParseException("Empty name")
    } else {
      val first = name.charAt(0)
      if (!Character.isLetter(first) && first != '_') {
        throw new SchemaParseException("Illegal initial character: " + name)
      } else {
        var i = 1
        while (i < length) {
          val c = name.charAt(i)
          if (!Character.isLetterOrDigit(c) && c != '_') {
            throw new SchemaParseException("Illegal character in: " + name)
          }
          i += 1
        }
      }
    }
  }
}
