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

import java.io.File

import scala.util.Try

import org.apache.spark.SparkFunSuite

class SparkUncaughtExceptionHandlerSuite extends SparkFunSuite {

  private val sparkHome =
    sys.props.getOrElse("spark.test.home", fail("spark.test.home is not set!"))

  // creates a spark-class process that invokes the exception thrower
  // the testcases will detect the process's exit code
  def getThrowerProcess(exceptionThrower: Any, exitOnUncaughtException: Boolean): Process = {
    Utils.executeCommand(
      Seq(s"$sparkHome/bin/spark-class",
        exceptionThrower.getClass.getCanonicalName.dropRight(1), // drops the "$" at the end
        exitOnUncaughtException.toString),
      new File(sparkHome),
      Map("SPARK_TESTING" -> "1", "SPARK_HOME" -> sparkHome))
  }

  test("SPARK-30310: Test uncaught RuntimeException, exitOnUncaughtException = true") {
    val process = getThrowerProcess(RuntimeExceptionThrower, exitOnUncaughtException = true)
    assert(process.waitFor == SparkExitCode.UNCAUGHT_EXCEPTION)
  }

  test("SPARK-30310: Test uncaught RuntimeException, exitOnUncaughtException = false") {
    val process = getThrowerProcess(RuntimeExceptionThrower, exitOnUncaughtException = false)
    assert(process.waitFor == 0)
  }

  test("SPARK-30310: Test uncaught OutOfMemoryError, exitOnUncaughtException = true") {
    val process = getThrowerProcess(OutOfMemoryErrorThrower, exitOnUncaughtException = true)
    assert(process.waitFor == SparkExitCode.OOM)
  }

  test("SPARK-30310: Test uncaught OutOfMemoryError, exitOnUncaughtException = false") {
    val process = getThrowerProcess(OutOfMemoryErrorThrower, exitOnUncaughtException = false)
    assert(process.waitFor == SparkExitCode.OOM)
  }

  test("SPARK-30310: Test uncaught SparkFatalException, exitOnUncaughtException = true") {
    val process = getThrowerProcess(SparkFatalExceptionThrower, exitOnUncaughtException = true)
    assert(process.waitFor == SparkExitCode.UNCAUGHT_EXCEPTION)
  }

  test("SPARK-30310: Test uncaught SparkFatalException, exitOnUncaughtException = false") {
    val process = getThrowerProcess(SparkFatalExceptionThrower, exitOnUncaughtException = false)
    assert(process.waitFor == 0)
  }

  test("SPARK-30310: Test uncaught SparkFatalException (OOM), exitOnUncaughtException = true") {
    val process = getThrowerProcess(SparkFatalExceptionWithOOMThrower,
      exitOnUncaughtException = true)
    assert(process.waitFor == SparkExitCode.OOM)
  }

  test("SPARK-30310: Test uncaught SparkFatalException (OOM), exitOnUncaughtException = false") {
    val process = getThrowerProcess(SparkFatalExceptionWithOOMThrower,
      exitOnUncaughtException = false)
    assert(process.waitFor == SparkExitCode.OOM)
  }

}

// a thread that uses SparkUncaughtExceptionHandler, then throws the throwable
class ThrowableThrowerThread(t: Throwable,
    exitOnUncaughtException: Boolean) extends Thread {
  override def run() {
    Thread.setDefaultUncaughtExceptionHandler(
      new SparkUncaughtExceptionHandler(exitOnUncaughtException))
    throw t
  }
}

// Objects to be invoked by spark-class for different Throwable types
// that SparkUncaughtExceptionHandler handles.  spark-class will exit with
// exit code dictated by either:
// - SparkUncaughtExceptionHandler (SparkExitCode)
// - main() (0, or -1 when args is empty)

object RuntimeExceptionThrower {
  def main(args: Array[String]): Unit = {
    if (args.length > 0) {
      val t = new ThrowableThrowerThread(new RuntimeException,
        Try(args(0).toBoolean).getOrElse(false))
      t.start()
      t.join()
      System.exit(0)
    } else {
      System.exit(-1)
    }
  }
}

object OutOfMemoryErrorThrower {
  def main(args: Array[String]): Unit = {
    if (args.length > 0) {
      val t = new ThrowableThrowerThread(new OutOfMemoryError,
        Try(args(0).toBoolean).getOrElse(false))
      t.start()
      t.join()
      System.exit(0)
    } else {
      System.exit(-1)
    }
  }
}

object SparkFatalExceptionThrower {
  def main(args: Array[String]): Unit = {
    if (args.length > 0) {
      val t = new ThrowableThrowerThread(new SparkFatalException(new RuntimeException),
        Try(args(0).toBoolean).getOrElse(false))
      t.start()
      t.join()
      System.exit(0)
    } else {
      System.exit(-1)
    }
  }
}

object SparkFatalExceptionWithOOMThrower {
  def main(args: Array[String]): Unit = {
    if (args.length > 0) {
      val t = new ThrowableThrowerThread(new SparkFatalException(new OutOfMemoryError),
        Try(args(0).toBoolean).getOrElse(false))
      t.start()
      t.join()
      System.exit(0)
    } else {
      System.exit(-1)
    }
  }
}
