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

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.jar.JarFile

import scala.jdk.CollectionConverters._

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.ExternalUserDefinedFunction

/**
 * Fails when an engine class that names the UDF worker also names the protobuf runtime.
 *
 * Maven shades every `com.google.protobuf` reference in `sql/core` to Spark Core's protobuf
 * package. The packaged worker runtime uses a different package. A descriptor that still names
 * protobuf at an engine call site cannot link to the worker message. The scan looks for both
 * spellings shade rewrites, the internal name and the dotted name, and does not parse bytecode.
 */
class ExternalUDFProtobufBoundarySuite extends SparkFunSuite {

  private val UdfWorker = "org/apache/spark/udf/worker/".getBytes(StandardCharsets.UTF_8)
  private val ProtobufSlash = "com/google/protobuf/".getBytes(StandardCharsets.UTF_8)
  private val ProtobufDot = "com.google.protobuf.".getBytes(StandardCharsets.UTF_8)

  test("engine classes that name the UDF worker do not name protobuf") {
    val roots = Seq(
      "core" -> classOf[org.apache.spark.SparkEnv],
      "catalyst" -> classOf[ExternalUserDefinedFunction],
      "sql" -> classOf[ExecuteExternalUDFExec])
    val offenders = scala.collection.mutable.ArrayBuffer.empty[String]
    roots.foreach { case (module, cls) =>
      val root = codeSource(cls)
      val udfClasses = classEntries(root).filter { case (_, bytes) => contains(bytes, UdfWorker) }
      assert(udfClasses.nonEmpty, s"$module output $root contains no UDF worker class reference")
      udfClasses.foreach { case (name, bytes) =>
        if (contains(bytes, ProtobufSlash) || contains(bytes, ProtobufDot)) {
          offenders += s"$module:$name"
        }
      }
    }
    assert(offenders.isEmpty, offenders.mkString("\n"))
  }

  private def codeSource(cls: Class[_]): File = {
    val location = cls.getProtectionDomain.getCodeSource.getLocation
    val file = new File(location.toURI)
    require(file.exists(), s"missing class output for ${cls.getName}: $file")
    file
  }

  private def classEntries(root: File): Seq[(String, Array[Byte])] = {
    if (root.isDirectory) {
      val stream = Files.walk(root.toPath)
      try {
        stream
          .iterator()
          .asScala
          .filter(path => Files.isRegularFile(path) && path.toString.endsWith(".class"))
          .map { path =>
            root.toPath.relativize(path).toString -> Files.readAllBytes(path)
          }
          .toSeq
      } finally {
        stream.close()
      }
    } else {
      val jar = new JarFile(root)
      try {
        jar
          .entries()
          .asScala
          .filter(_.getName.endsWith(".class"))
          .map { entry =>
            val in = jar.getInputStream(entry)
            try {
              entry.getName -> in.readAllBytes()
            } finally {
              in.close()
            }
          }
          .toSeq
      } finally {
        jar.close()
      }
    }
  }

  private def contains(haystack: Array[Byte], needle: Array[Byte]): Boolean = {
    var start = 0
    val last = haystack.length - needle.length
    while (start <= last) {
      var offset = 0
      while (offset < needle.length && haystack(start + offset) == needle(offset)) {
        offset += 1
      }
      if (offset == needle.length) {
        return true
      }
      start += 1
    }
    false
  }
}
