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

package org.apache.spark.sql.util

import java.io.{File, FileNotFoundException}
import java.nio.file.NoSuchFileException

import scala.util.control.NonFatal

import org.apache.commons.io.FileUtils

import org.apache.spark.sql.errors.CompilationErrors

object ProtobufUtils {
  def readDescriptorFileContent(filePath: String): Array[Byte] = {
    try {
      FileUtils.readFileToByteArray(new File(filePath))
    } catch {
      case ex: FileNotFoundException =>
        throw CompilationErrors.cannotFindDescriptorFileError(filePath, ex)
      case ex: NoSuchFileException =>
        throw CompilationErrors.cannotFindDescriptorFileError(filePath, ex)
      case NonFatal(ex) => throw CompilationErrors.descriptorParseError(ex)
    }
  }
}
