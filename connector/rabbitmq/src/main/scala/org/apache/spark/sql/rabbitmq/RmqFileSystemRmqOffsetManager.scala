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
package org.apache.spark.sql.rabbitmq

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, NoSuchFileException, Path}

import scala.util.Try

import org.apache.spark.internal.Logging

class RmqFileSystemRmqOffsetManager(path: Path) extends
  RmqOffsetManagerTrait with Logging {
  private val checkpointPath: Path = path
  Files.createDirectories(checkpointPath)

  def saveLongToFile(value: Long): Unit = {
    try {
      Files.write(checkpointPath.resolve("offsetcheckpoint"),
        value.toString.getBytes(StandardCharsets.UTF_8))
      logDebug("Successfully update the custom offset checkpoint")
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  def readLongFromFile(): Option[Long] = {
    try {
      val lines = Files.readAllLines(checkpointPath.resolve("offsetcheckpoint"),
        StandardCharsets.UTF_8)
      if (lines.size() > 0) Try(lines.get(0).toLong).toOption else None
    } catch {
      case _: NoSuchFileException => logError("Missing custom checkpoint file")
        None
      case e: Exception =>
        e.printStackTrace()
        None
    }
  }
}
