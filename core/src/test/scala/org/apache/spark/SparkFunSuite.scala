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

package org.apache.spark

// scalastyle:off
import java.io.File

import org.apache.spark.internal.Logging
import org.apache.spark.util.AccumulatorContext
import org.scalatest.{BeforeAndAfterAll, FunSuite, Outcome}

import scala.collection.JavaConversions._
import scala.io.Source

/**
 * Base abstract class for all unit tests in Spark for handling common functionality.
 */
abstract class SparkFunSuite
  extends FunSuite
  with BeforeAndAfterAll
  with Logging {
// scalastyle:on

  var beforeAllTestThreadNames: Set[String] = Set.empty

  protected override def beforeAll(): Unit = {
    saveThreadNames()
    super.beforeAll()
  }

  protected override def afterAll(): Unit = {
    try {
      // Avoid leaking map entries in tests that use accumulators without SparkContext
      AccumulatorContext.clear()
    } finally {
      super.afterAll()
      printRemainingThreadNames()
    }
  }

  // helper function
  protected final def getTestResourceFile(file: String): File = {
    new File(getClass.getClassLoader.getResource(file).getFile)
  }

  protected final def getTestResourcePath(file: String): String = {
    getTestResourceFile(file).getCanonicalPath
  }

  private def saveThreadNames(): Unit = {
    beforeAllTestThreadNames = Thread.getAllStackTraces.keySet().map(_.getName).toSet
  }

  private def printRemainingThreadNames(): Unit = {
    val currentThreadNames = Thread.getAllStackTraces.keySet().map(_.getName).toSet
    val whitelistedThreadNames = currentThreadNames.
      filterNot(s => SparkFunSuite.threadWhiteList.exists(s.matches(_)))
    val remainingThreadNames = whitelistedThreadNames.diff(beforeAllTestThreadNames)
    if (!remainingThreadNames.isEmpty) {
      logInfo("\n\n===== THREADS NOT STOPPED PROPERLY =====\n")
      remainingThreadNames.foreach(logInfo(_))
      logInfo("\n\n===== END OF THREAD DUMP =====\n")
      logInfo("\n\n===== EITHER PUT THREAD NAME INTO THE WHITELIST FILE " +
        "OR SHUT IT DOWN PROPERLY =====\n")
    }
  }

  /**
   * Log the suite name and the test name before and after each test.
   *
   * Subclasses should never override this method. If they wish to run
   * custom code before and after each test, they should mix in the
   * {{org.scalatest.BeforeAndAfter}} trait instead.
   */
  final protected override def withFixture(test: NoArgTest): Outcome = {
    val testName = test.text
    val suiteName = this.getClass.getName
    val shortSuiteName = suiteName.replaceAll("org.apache.spark", "o.a.s")
    try {
      logInfo(s"\n\n===== TEST OUTPUT FOR $shortSuiteName: '$testName' =====\n")
      test()
    } finally {
      logInfo(s"\n\n===== FINISHED $shortSuiteName: '$testName' =====\n")
    }
  }

}

object SparkFunSuite
  extends Logging {
  val threadWhitelistFileName = "/thread_whitelist"
  val threadWhiteList: Set[String] = try {
     val whileListStream = getClass.getResourceAsStream(threadWhitelistFileName)
     if (whileListStream == null) {
       logWarning(s"\n\n===== Could not find global thread whitelist file with " +
         s"name $threadWhitelistFileName on classpath' =====\n")
       Set.empty
     } else {
       val whiteList = Source.fromInputStream(whileListStream)
         .getLines().filterNot(s => s.isEmpty || s.startsWith("#")).toSet
       logInfo(s"\n\n===== Global thread whitelist loaded with name " +
         s"$threadWhitelistFileName from classpath: ${whiteList.mkString(", ")}' =====\n")
       whiteList
     }
  } catch {
    case e: Exception =>
      logWarning(s"\n\n===== Could not read global thread whitelist file with " +
        s"name $threadWhitelistFileName from classpath: ${e.getMessage}' =====\n")
      Set.empty
  }
}
