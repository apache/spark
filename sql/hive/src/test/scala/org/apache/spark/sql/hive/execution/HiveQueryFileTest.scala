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

package org.apache.spark.sql.hive.execution

import java.io.File

import org.apache.spark.sql.catalyst.util._

/**
 * A framework for running the query tests that are listed as a set of text files.
 *
 * TestSuites that derive from this class must provide a map of testCaseName to testCaseFiles
 * that should be included. Additionally, there is support for including and excluding
 * tests as development progresses.
 */
abstract class HiveQueryFileTest extends HiveComparisonTest {
  /** A list of tests deemed out of scope and thus completely disregarded */
  def excludeList: Seq[String] = Nil

  /**
   * The set of tests that are believed to be working in catalyst. Tests not in includeList or
   * excludeList are implicitly marked as ignored.
   */
  def includeList: Seq[String] = ".*" :: Nil

  def testCases: Seq[(String, File)]

  val runAll: Boolean =
    !(System.getProperty("spark.hive.alltests") == null) ||
    runOnlyDirectories.nonEmpty ||
    skipDirectories.nonEmpty

  val deprecatedIncludeListProperty: String = "spark.hive.whitelist"
  val includeListProperty: String = "spark.hive.includelist"
  if (System.getProperty(deprecatedIncludeListProperty) != null) {
    logWarning(s"System property `$deprecatedIncludeListProperty` is deprecated; please update " +
        s"to use new property: $includeListProperty")
  }
  // Allow the includeList to be overridden by a system property
  val realIncludeList: Seq[String] =
    Option(System.getProperty(includeListProperty))
        .orElse(Option(System.getProperty(deprecatedIncludeListProperty)))
        .map(_.split(",").toSeq)
        .getOrElse(includeList)

  // Go through all the test cases and add them to scala test.
  testCases.sorted.foreach {
    case (testCaseName, testCaseFile) =>
      if (excludeList.map(_.r.pattern.matcher(testCaseName).matches()).reduceLeft(_||_)) {
        logDebug(s"Excluded test skipped $testCaseName")
      } else if (
        realIncludeList.map(_.r.pattern.matcher(testCaseName).matches()).reduceLeft(_||_) ||
        runAll) {
        // Build a test case and submit it to scala test framework...
        val queriesString = fileToString(testCaseFile)
        createQueryTest(testCaseName, queriesString, reset = true, tryWithoutResettingFirst = true)
      } else {
        // Only output warnings for the built in includeList as this clutters the output when the
        // user is trying to execute a single test from the commandline.
        if (System.getProperty(includeListProperty) == null && !runAll) {
          ignore(testCaseName) {}
        }
      }
  }
}
