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

package org.apache.spark.sql.execution.streaming

import scala.util.control.Exception._

import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

/**
 * Offset for the [[FileStreamSource]].
 *
 * @param logOffset  Position in the [[FileStreamSourceLog]]
 */
case class FileStreamSourceOffset(logOffset: Long) extends Offset {
  override def json: String = {
    Serialization.write(this)(FileStreamSourceOffset.format)
  }
}

object FileStreamSourceOffset {
  implicit val format = Serialization.formats(NoTypeHints)

  def apply(offset: Offset): FileStreamSourceOffset = {
    offset match {
      case f: FileStreamSourceOffset => f
      case SerializedOffset(str) =>
        catching(classOf[NumberFormatException]).opt {
          FileStreamSourceOffset(str.toLong)
        }.getOrElse {
          Serialization.read[FileStreamSourceOffset](str)
        }
      case _ =>
        throw new IllegalArgumentException(
          s"Invalid conversion from offset of ${offset.getClass} to FileStreamSourceOffset")
    }
  }
}

