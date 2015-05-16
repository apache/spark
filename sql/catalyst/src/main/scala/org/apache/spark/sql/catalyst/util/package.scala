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

package org.apache.spark.sql.catalyst

import java.io._

import org.apache.spark.util.Utils

package object util {

  /** Silences output to stderr or stdout for the duration of f */
  def quietly[A](f: => A): A = {
    val origErr = System.err
    val origOut = System.out
    try {
      System.setErr(new PrintStream(new OutputStream {
        def write(b: Int) = {}
      }))
      System.setOut(new PrintStream(new OutputStream {
        def write(b: Int) = {}
      }))

      f
    } finally {
      System.setErr(origErr)
      System.setOut(origOut)
    }
  }

  def fileToString(file: File, encoding: String = "UTF-8"): String = {
    val inStream = new FileInputStream(file)
    val outStream = new ByteArrayOutputStream
    try {
      var reading = true
      while ( reading ) {
        inStream.read() match {
          case -1 => reading = false
          case c => outStream.write(c)
        }
      }
      outStream.flush()
    }
    finally {
      inStream.close()
    }
    new String(outStream.toByteArray, encoding)
  }

  def resourceToBytes(
      resource: String,
      classLoader: ClassLoader = Utils.getSparkClassLoader): Array[Byte] = {
    val inStream = classLoader.getResourceAsStream(resource)
    val outStream = new ByteArrayOutputStream
    try {
      var reading = true
      while ( reading ) {
        inStream.read() match {
          case -1 => reading = false
          case c => outStream.write(c)
        }
      }
      outStream.flush()
    }
    finally {
      inStream.close()
    }
    outStream.toByteArray
  }

  def resourceToString(
      resource:String,
      encoding: String = "UTF-8",
      classLoader: ClassLoader = Utils.getSparkClassLoader): String = {
    new String(resourceToBytes(resource, classLoader), encoding)
  }

  def stringToFile(file: File, str: String): File = {
    val out = new PrintWriter(file)
    out.write(str)
    out.close()
    file
  }

  def sideBySide(left: String, right: String): Seq[String] = {
    sideBySide(left.split("\n"), right.split("\n"))
  }

  def sideBySide(left: Seq[String], right: Seq[String]): Seq[String] = {
    val maxLeftSize = left.map(_.size).max
    val leftPadded = left ++ Seq.fill(math.max(right.size - left.size, 0))("")
    val rightPadded = right ++ Seq.fill(math.max(left.size - right.size, 0))("")

    leftPadded.zip(rightPadded).map {
      case (l, r) => (if (l == r) " " else "!") + l + (" " * ((maxLeftSize - l.size) + 3)) + r
    }
  }

  def stackTraceToString(t: Throwable): String = {
    val out = new java.io.ByteArrayOutputStream
    val writer = new PrintWriter(out)
    t.printStackTrace(writer)
    writer.flush()
    new String(out.toByteArray)
  }

  def stringOrNull(a: AnyRef): String = if (a == null) null else a.toString

  def benchmark[A](f: => A): A = {
    val startTime = System.nanoTime()
    val ret = f
    val endTime = System.nanoTime()
    println(s"${(endTime - startTime).toDouble / 1000000}ms")
    ret
  }

  /* FIX ME
  implicit class debugLogging(a: Any) {
    def debugLogging() {
      org.apache.log4j.Logger.getLogger(a.getClass.getName).setLevel(org.apache.log4j.Level.DEBUG)
    }
  } */
}
