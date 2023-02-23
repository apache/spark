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
package org.apache.spark.sql.connect.client

import java.io.File
import java.net.URLClassLoader
import java.util.regex.Pattern

import com.typesafe.tools.mima.core._
import com.typesafe.tools.mima.lib.MiMaLib
import org.scalatest.funsuite.AnyFunSuite // scalastyle:ignore funsuite
import org.apache.spark.sql.connect.client.util.IntegrationTestUtils._

/**
 * This test checks the binary compatibility of the connect client API against the spark SQL API
 * using MiMa. We did not write this check using a SBT build rule as the rule cannot provide the
 * same level of freedom as a test. With a test we can:
 *   1. Specify any two jars to run the compatibility check.
 *   1. Easily make the test automatically pick up all new methods added while the client is being
 *      built.
 *
 * The test requires the following artifacts built before running:
 * {{{
 *     spark-sql
 *     spark-connect-client-jvm
 * }}}
 * To build the above artifact, use e.g. `sbt package` or `mvn clean install -DskipTests`.
 *
 * When debugging this test, if any changes to the client API, the client jar need to be built
 * before running the test. An example workflow with SBT for this test:
 *   1. Compatibility test has reported an unexpected client API change.
 *   1. Fix the wrong client API.
 *   1. Build the client jar: `sbt package`
 *   1. Run the test again: `sbt "testOnly
 *      org.apache.spark.sql.connect.client.CompatibilitySuite"`
 */
class CompatibilitySuite extends AnyFunSuite { // scalastyle:ignore funsuite

  private lazy val clientJar: File =
    findJar(
      "connector/connect/client/jvm",
      "spark-connect-client-jvm-assembly",
      "spark-connect-client-jvm")

  private lazy val sqlJar: File = findJar("sql/core", "spark-sql", "spark-sql")

  /**
   * MiMa takes an old jar (sql jar) and a new jar (client jar) as inputs and then reports all
   * incompatibilities found in the new jar. The incompatibility result is then filtered using
   * include and exclude rules. Include rules are first applied to find all client classes that
   * need to be checked. Then exclude rules are applied to filter out all unsupported methods in
   * the client classes.
   */
  test("compatibility MiMa tests") {
    val mima = new MiMaLib(Seq(clientJar, sqlJar))
    val allProblems = mima.collectProblems(sqlJar, clientJar, List.empty)
    val includedRules = Seq(
      IncludeByName("org.apache.spark.sql.Column"),
      IncludeByName("org.apache.spark.sql.Column$"),
      IncludeByName("org.apache.spark.sql.Dataset"),
      // TODO(SPARK-42175) Add the Dataset object definition
      // IncludeByName("org.apache.spark.sql.Dataset$"),
      IncludeByName("org.apache.spark.sql.DataFrame"),
      IncludeByName("org.apache.spark.sql.DataFrameReader.*"),
      IncludeByName("org.apache.spark.sql.DataFrameWriter.*"),
      IncludeByName("org.apache.spark.sql.DataFrameWriterV2.*"),
      IncludeByName("org.apache.spark.sql.SparkSession"),
      IncludeByName("org.apache.spark.sql.SparkSession$")) ++ includeImplementedMethods(clientJar)
    val excludeRules = Seq(
      // Filter unsupported rules:
      // Two sql overloading methods are marked experimental in the API and skipped in the client.
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.SparkSession.sql"),
      // Deprecated json methods and RDD related methods are skipped in the client.
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.DataFrameReader.json"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.DataFrameReader.csv"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.DataFrameReader.jdbc"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.DataFrameWriter.jdbc"),
      // Skip all shaded dependencies in the client.
      ProblemFilters.exclude[Problem]("org.sparkproject.*"),
      ProblemFilters.exclude[Problem]("org.apache.spark.connect.proto.*"),
      // Disable Range until we support typed APIs
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.SparkSession.range"))
    val problems = allProblems
      .filter { p =>
        includedRules.exists(rule => rule(p))
      }
      .filter { p =>
        excludeRules.forall(rule => rule(p))
      }

    if (problems.nonEmpty) {
      fail(
        s"\nComparing client jar: $clientJar\nand sql jar: $sqlJar\n" +
          problems.map(p => p.description("client")).mkString("\n"))
    }
  }

  test("compatibility API tests: Dataset") {
    val clientClassLoader: URLClassLoader = new URLClassLoader(Seq(clientJar.toURI.toURL).toArray)
    val sqlClassLoader: URLClassLoader = new URLClassLoader(Seq(sqlJar.toURI.toURL).toArray)

    val clientClass = clientClassLoader.loadClass("org.apache.spark.sql.Dataset")
    val sqlClass = sqlClassLoader.loadClass("org.apache.spark.sql.Dataset")

    val newMethods = clientClass.getMethods
    val oldMethods = sqlClass.getMethods

    // For now we simply check the new methods is a subset of the old methods.
    newMethods
      .map(m => m.toString)
      .foreach(method => {
        assert(oldMethods.map(m => m.toString).contains(method))
      })
  }

  /**
   * Find all methods that are implemented in the client jar. Once all major methods are
   * implemented we can switch to include all methods under the class using ".*" e.g.
   * "org.apache.spark.sql.Dataset.*"
   */
  private def includeImplementedMethods(clientJar: File): Seq[IncludeByName] = {
    val clsNames = Seq(
      "org.apache.spark.sql.Column",
      // TODO(SPARK-42175) Add all overloading methods. Temporarily mute compatibility check for \
      //  the Dataset methods, as too many overload methods are missing.
      // "org.apache.spark.sql.Dataset",
      "org.apache.spark.sql.SparkSession")

    val clientClassLoader: URLClassLoader = new URLClassLoader(Seq(clientJar.toURI.toURL).toArray)
    clsNames
      .flatMap { clsName =>
        val cls = clientClassLoader.loadClass(clsName)
        // all distinct method names
        cls.getMethods.map(m => s"$clsName.${m.getName}").toSet
      }
      .map { fullName =>
        IncludeByName(fullName)
      }
  }

  private case class IncludeByName(name: String) extends ProblemFilter {
    private[this] val pattern =
      Pattern.compile(name.split("\\*", -1).map(Pattern.quote).mkString(".*"))

    override def apply(problem: Problem): Boolean = {
      pattern.matcher(problem.matchName.getOrElse("")).matches
    }
  }
}
