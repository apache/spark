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

package org.apache.spark.internal

import java.io.{OutputStream, PrintStream}

// scalastyle:off println
private[spark] class LoggingPrintStream(out: OutputStream) extends PrintStream(out) with Logging {
  override def print(b: Boolean) {
    println(b)
  }

  override def print(c: Char) {
    println(c)
  }

  override def print(i: Int) {
    println(i)
  }

  override def print(l: Long) {
    println(l)
  }

  override def print(f: Float) {
    println(f)
  }

  override def print(d: Double) {
    println(d)
  }

  override def print(s: Array[Char]) {
    println(s)
  }

  override def print(s: String) {
    println(s)
  }

  override def print(obj: Object) {
    println(obj)
  }

  override def println(b: Boolean) {
    logInfo(String.valueOf(b))
  }

  override def println(c: Char) {
    logInfo(String.valueOf(c))
  }

  override def println(s: Array[Char]) {
    logInfo(String.valueOf(s))
  }

  override def println(i: Int) {
    logInfo(String.valueOf(i))
  }

  override def println(l: Long) {
    logInfo(String.valueOf(l))
  }

  override def println(f: Float) {
    logInfo(String.valueOf(f))
  }

  override def println(d: Double) {
    logInfo(String.valueOf(d))
  }

  override def println(s: String) {
    logInfo(s)
  }

  override def println(obj: Object) {
    logInfo(String.valueOf(obj))
  }
}
// scalastyle:on println

object LoggingPrintStream {
  private val outInstance: PrintStream = new LoggingPrintStream(System.out)
  private val errInstance: PrintStream = new LoggingPrintStream(System.err)

  /**
   * In a production environment, user behavior is highly random and uncertain.
   * For example: Users use `System.out` or `System.err` to print information.
   * But the system print stream may cause some trouble, such as: the disk file is too large.
   * Redirecting the system print stream to `Log4j` can take advantage of `Log4j`'s split log.
   */
  def withOutSystem[A](redirect: Boolean, f: => A): A = {
    val origErr = System.err
    val origOut = System.out
    try {
      if (redirect) {
        System.setOut(outInstance)
        System.setErr(errInstance)
      }

      f
    } finally {
      if (redirect) {
        System.setErr(origErr)
        System.setOut(origOut)
      }
    }
  }
}

