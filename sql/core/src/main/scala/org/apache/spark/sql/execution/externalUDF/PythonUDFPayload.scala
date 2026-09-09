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
package org.apache.spark.sql.execution.externalUDF

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream,
  EOFException}
import java.nio.charset.StandardCharsets.UTF_8

import scala.jdk.CollectionConverters._

import org.apache.spark.api.python.PythonFunction

/**
 * Versioned static payload for a scalar PySpark UDF.
 *
 * Only Python-private state which does not change when Catalyst rewrites an invocation is stored
 * here. Logical input types, argument bindings, resource locations, and runtime state are supplied
 * by language-neutral Init fields.
 */
private[sql] final class PythonUDFPayload private (
    commandBytes: Array[Byte],
    val pythonIncludes: Vector[String],
    val pythonVersion: String) {

  def command: Array[Byte] = commandBytes.clone()
}

private[sql] object PythonUDFPayload {
  // ASCII "PYUD" followed by a format version.
  private val MAGIC = 0x50595544
  private val VERSION = 2

  /** Encodes all Python-specific state which is invariant across task attempts. */
  def encode(func: PythonFunction): Array[Byte] = {
    // TODO(SPARK-59366): Carry `func.broadcastVars` through language-neutral UDF resource
    // metadata before routing PySpark scalar UDFs through unified execution.
    val pythonIncludes = Option(func.pythonIncludes)
      .map(_.asScala.toVector)
      .getOrElse(Vector.empty)
    encodeFields(
      func.command.toArray,
      pythonIncludes,
      func.pythonVer)
  }

  private def encodeFields(
      command: Array[Byte],
      pythonIncludes: Seq[String],
      pythonVersion: String): Array[Byte] = {
    val buffer = new ByteArrayOutputStream()
    val output = new DataOutputStream(buffer)

    output.writeInt(MAGIC)
    output.writeInt(VERSION)
    writeBytes(command, output)
    output.writeInt(pythonIncludes.size)
    pythonIncludes.foreach { pythonInclude =>
      require(pythonInclude != null, "Python include cannot be null")
      writeString(pythonInclude, output)
    }
    writeString(pythonVersion, output)
    output.flush()
    buffer.toByteArray
  }

  def decode(payload: Array[Byte]): PythonUDFPayload = {
    require(payload != null, "Python UDF payload cannot be null")
    val input = new DataInputStream(new ByteArrayInputStream(payload))
    try {
      requireField(input.readInt() == MAGIC, "invalid magic")
      val version = input.readInt()
      requireField(version == VERSION, s"unsupported version $version")

      val command = readBytes(input, "command")
      val includeCount = input.readInt()
      requireField(
        includeCount >= 0 && includeCount <= input.available() / Integer.BYTES,
        s"invalid Python include count $includeCount")
      val pythonIncludes = Vector.fill(includeCount) {
        readString(input, "Python include")
      }
      val pythonVersion = readString(input, "Python version")
      requireField(input.available() == 0, "trailing bytes")
      new PythonUDFPayload(
        command,
        pythonIncludes,
        pythonVersion)
    } catch {
      case e: EOFException =>
        throw new IllegalArgumentException("Malformed Python UDF payload", e)
    }
  }

  private def writeBytes(bytes: Array[Byte], output: DataOutputStream): Unit = {
    output.writeInt(bytes.length)
    output.write(bytes)
  }

  private def writeString(value: String, output: DataOutputStream): Unit = {
    require(value != null, "Python UDF payload string cannot be null")
    writeBytes(value.getBytes(UTF_8), output)
  }

  private def readBytes(input: DataInputStream, field: String): Array[Byte] = {
    val length = input.readInt()
    requireField(length >= 0 && length <= input.available(), s"invalid $field length $length")
    val bytes = new Array[Byte](length)
    input.readFully(bytes)
    bytes
  }

  private def readString(input: DataInputStream, field: String): String = {
    new String(readBytes(input, field), UTF_8)
  }

  private def requireField(condition: Boolean, detail: => String): Unit = {
    if (!condition) {
      throw new IllegalArgumentException(s"Malformed Python UDF payload: $detail")
    }
  }
}
