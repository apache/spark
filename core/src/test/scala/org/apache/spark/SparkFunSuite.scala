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

import org.scalatest.{BeforeAndAfterAll, FunSuite, Outcome}

import org.apache.spark.internal.Logging
import org.apache.spark.util.AccumulatorContext

/**
 * Base abstract class for all unit tests in Spark for handling common functionality.
 */
abstract class SparkFunSuite
  extends FunSuite
  with BeforeAndAfterAll
  with Logging {
// scalastyle:on

  protected override def afterAll(): Unit = {
    try {
      // Avoid leaking map entries in tests that use accumulators without SparkContext
      AccumulatorContext.clear()
    } finally {
      super.afterAll()
    }
  }

  // helper function
  protected final def getFile(file: String): File = {
    new File(getClass.getClassLoader.getResource(file).getFile)
  }

  protected final def getPath(file: String): String = {
    getFile(file).getCanonicalPath
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
