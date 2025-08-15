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

package org.apache.spark.util

import java.io.{Closeable, IOException}

import scala.annotation.nowarn

import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite // scalastyle:ignore funsuite

class SparkErrorUtilsSuite
    extends AnyFunSuite // scalastyle:ignore funsuite
    with BeforeAndAfterEach {

  private var withoutCause: Throwable = _
  private var withCause: Throwable = _
  private var jdkNoCause: Throwable = _
  private var nested: NestableException = _
  private var cyclicCause: ExceptionWithCause = _

  override def beforeEach(): Unit = {
    withoutCause = createExceptionWithoutCause
    nested = new NestableException(withoutCause)
    withCause = new ExceptionWithCause(nested)
    jdkNoCause = new NullPointerException
    val exceptionA = new ExceptionWithCause(null.asInstanceOf[Throwable])
    val exceptionB = new ExceptionWithCause(exceptionA)
    exceptionA.setCauseValue(exceptionB)
    cyclicCause = new ExceptionWithCause(exceptionA)
  }

  override def afterEach(): Unit = {
    withoutCause = null
    nested = null
    withCause = null
    jdkNoCause = null
    cyclicCause = null
  }

  test("getRootCause") {
    assert(SparkErrorUtils.getRootCause(null) == null)
    assert(SparkErrorUtils.getRootCause(withoutCause) == withoutCause)
    assert(SparkErrorUtils.getRootCause(nested) == withoutCause)
    assert(SparkErrorUtils.getRootCause(withCause) == withoutCause)
    assert(SparkErrorUtils.getRootCause(jdkNoCause) == jdkNoCause)
    assert(SparkErrorUtils.getRootCause(cyclicCause) == cyclicCause.getCause.getCause)
  }

  test("tryWithResource / tryInitializeResource") {
    val closeable = new Closeable {
      override def close(): Unit = {
        throw new IOException("Catch me if you can")
      }
    }
    val e1 = intercept[IOException] {
      SparkErrorUtils.tryWithResource(closeable)(_ => throw new IOException("You got me!"))
    }
    assert(e1.getMessage === "You got me!")
    val e2 = intercept[IOException] {
      SparkErrorUtils.tryInitializeResource(closeable)(_ => throw new IOException("You got me!"))
    }
    assert(e2.getMessage === "You got me!")
  }

  private def createExceptionWithoutCause: Throwable =
    try throw new ExceptionWithoutCause
    catch {
      case t: Throwable => t
    }

  private final class ExceptionWithoutCause extends Exception {
    @nowarn def getTargetException(): Unit = {}
  }

  private final class NestableException extends Exception {
    def this(t: Throwable) = {
      this()
      initCause(t)
    }
  }

  private final class ExceptionWithCause(message: String = null) extends Exception(message) {

    private var _cause: Throwable = _

    def this(message: String, cause: Throwable) = {
      this(message)
      this._cause = cause
    }

    def this(cause: Throwable) = {
      this(null.asInstanceOf[String])
      this._cause = cause
    }

    override def getCause: Throwable = synchronized {
      _cause
    }

    def setCauseValue(cause: Throwable): Unit = {
      this._cause = cause
    }
  }
}
