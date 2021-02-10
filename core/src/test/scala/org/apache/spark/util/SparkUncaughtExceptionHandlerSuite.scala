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

  Seq(
    (ThrowableTypes.RuntimeException, true, SparkExitCode.UNCAUGHT_EXCEPTION),
    (ThrowableTypes.RuntimeException, false, 0),
    (ThrowableTypes.OutOfMemoryError, true, SparkExitCode.OOM),
    (ThrowableTypes.OutOfMemoryError, false, SparkExitCode.OOM),
    (ThrowableTypes.SparkFatalRuntimeException, true, SparkExitCode.UNCAUGHT_EXCEPTION),
    (ThrowableTypes.SparkFatalRuntimeException, false, 0),
    (ThrowableTypes.SparkFatalOutOfMemoryError, true, SparkExitCode.OOM),
    (ThrowableTypes.SparkFatalOutOfMemoryError, false, SparkExitCode.OOM)
  ).foreach {
    case (throwable: ThrowableTypes.ThrowableTypesVal,
    exitOnUncaughtException: Boolean, expectedExitCode) =>
      test(s"SPARK-30310: Test uncaught $throwable, " +
          s"exitOnUncaughtException = $exitOnUncaughtException") {

        // creates a ThrowableThrower process via spark-class and verify the exit code
        val process = Utils.executeCommand(
          Seq(s"$sparkHome/bin/spark-class",
            ThrowableThrower.getClass.getCanonicalName.dropRight(1), // drops the "$" at the end
            throwable.name,
            exitOnUncaughtException.toString),
          new File(sparkHome),
          Map("SPARK_TESTING" -> "1", "SPARK_HOME" -> sparkHome)
        )
        assert(process.waitFor == expectedExitCode)
      }
  }
}

// enumeration object for the Throwable types that SparkUncaughtExceptionHandler handles
object ThrowableTypes extends Enumeration {

  sealed case class ThrowableTypesVal(name: String, t: Throwable) extends Val(name)

  val RuntimeException = ThrowableTypesVal("RuntimeException", new RuntimeException)
  val OutOfMemoryError = ThrowableTypesVal("OutOfMemoryError", new OutOfMemoryError)
  val SparkFatalRuntimeException = ThrowableTypesVal("SparkFatalException(RuntimeException)",
    new SparkFatalException(new RuntimeException))
  val SparkFatalOutOfMemoryError = ThrowableTypesVal("SparkFatalException(OutOfMemoryError)",
    new SparkFatalException(new OutOfMemoryError))

  // returns the actual Throwable by its name
  def getThrowableByName(name: String): Throwable = {
    super.withName(name).asInstanceOf[ThrowableTypesVal].t
  }
}

// Invoked by spark-class for throwing a Throwable
object ThrowableThrower {

  // a thread that uses SparkUncaughtExceptionHandler and throws a Throwable by name
  class ThrowerThread(name: String, exitOnUncaughtException: Boolean) extends Thread {
    override def run(): Unit = {
      Thread.setDefaultUncaughtExceptionHandler(
        new SparkUncaughtExceptionHandler(exitOnUncaughtException))
      throw ThrowableTypes.getThrowableByName(name)
    }
  }

  // main() requires 2 args:
  // - args(0): name of the Throwable defined in ThrowableTypes
  // - args(1): exitOnUncaughtException (true/false)
  //
  // it exits with the exit code dictated by either:
  // - SparkUncaughtExceptionHandler (SparkExitCode)
  // - main() (0, or -1 when number of args is wrong)
  def main(args: Array[String]): Unit = {
    if (args.length == 2) {
      val t = new ThrowerThread(args(0),
        Try(args(1).toBoolean).getOrElse(false))
      t.start()
      t.join()
      System.exit(0)
    } else {
      System.exit(-1)
    }
  }
}
