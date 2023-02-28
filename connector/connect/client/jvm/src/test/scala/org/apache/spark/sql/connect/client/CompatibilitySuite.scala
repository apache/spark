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

import org.apache.spark.sql.connect.client.util.ConnectFunSuite
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
 * To build the above artifact, use e.g. `build/sbt package` or `build/mvn clean install
 * -DskipTests`.
 *
 * When debugging this test, if any changes to the client API, the client jar need to be built
 * before running the test. An example workflow with SBT for this test:
 *   1. Compatibility test has reported an unexpected client API change.
 *   1. Fix the wrong client API.
 *   1. Build the client jar: `build/sbt package`
 *   1. Run the test again: `build/sbt "testOnly
 *      org.apache.spark.sql.connect.client.CompatibilitySuite"`
 */
class CompatibilitySuite extends ConnectFunSuite {

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
      IncludeByName("org.apache.spark.sql.Column.*"),
      IncludeByName("org.apache.spark.sql.ColumnName.*"),
      IncludeByName("org.apache.spark.sql.DataFrame.*"),
      IncludeByName("org.apache.spark.sql.DataFrameReader.*"),
      IncludeByName("org.apache.spark.sql.DataFrameWriter.*"),
      IncludeByName("org.apache.spark.sql.DataFrameWriterV2.*"),
      IncludeByName("org.apache.spark.sql.Dataset.*"),
      IncludeByName("org.apache.spark.sql.functions.*"),
      IncludeByName("org.apache.spark.sql.RelationalGroupedDataset.*"),
      IncludeByName("org.apache.spark.sql.SparkSession.*"),
      IncludeByName("org.apache.spark.sql.RuntimeConfig.*"),
      IncludeByName("org.apache.spark.sql.TypedColumn.*"))
    val excludeRules = Seq(
      // Filter unsupported rules:
      // Note when muting errors for a method, checks on all overloading methods are also muted.

      // Skip all shaded dependencies and proto files in the client.
      ProblemFilters.exclude[Problem]("org.sparkproject.*"),
      ProblemFilters.exclude[Problem]("org.apache.spark.connect.proto.*"),

      // DataFrame Reader & Writer
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.DataFrameReader.json"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.DataFrameReader.csv"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.DataFrameReader.jdbc"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.DataFrameWriter.jdbc"),

      // Dataset
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.Dataset.ofRows"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.Dataset.DATASET_ID_TAG"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.Dataset.COL_POS_KEY"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.Dataset.DATASET_ID_KEY"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.Dataset.curId"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.Dataset.observe"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.Dataset.queryExecution"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.Dataset.encoder"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.Dataset.sqlContext"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.Dataset.as"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.Dataset.na"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.Dataset.stat"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.Dataset.joinWith"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.Dataset.select"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.Dataset.selectUntyped"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.Dataset.reduce"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.Dataset.groupByKey"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.Dataset.explode"), // deprecated
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.Dataset.filter"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.Dataset.map"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.Dataset.mapPartitions"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.Dataset.flatMap"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.Dataset.foreach"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.Dataset.foreachPartition"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.Dataset.persist"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.Dataset.storageLevel"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.Dataset.rdd"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.Dataset.toJavaRDD"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.Dataset.javaRDD"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.Dataset.writeStream"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.Dataset.this"),

      // functions
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.functions.udf"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.functions.call_udf"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.functions.callUDF"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.functions.unwrap_udt"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.functions.udaf"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.functions.broadcast"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.functions.typedlit"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.functions.typedLit"),

      // RelationalGroupedDataset
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.RelationalGroupedDataset.apply"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.RelationalGroupedDataset.as"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.RelationalGroupedDataset.this"),

      // SparkSession
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.SparkSession.getDefaultSession"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.SparkSession.clearDefaultSession"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.SparkSession.setDefaultSession"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.SparkSession.sparkContext"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.SparkSession.sharedState"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.SparkSession.sessionState"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.SparkSession.sqlContext"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.SparkSession.listenerManager"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.SparkSession.experimental"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.SparkSession.udf"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.SparkSession.streams"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.SparkSession.newSession"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.SparkSession.emptyDataFrame"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.SparkSession.emptyDataset"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.SparkSession.createDataFrame"),
      ProblemFilters.exclude[Problem](
        "org.apache.spark.sql.SparkSession.baseRelationToDataFrame"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.SparkSession.createDataset"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.SparkSession.catalog"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.SparkSession.executeCommand"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.SparkSession.readStream"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.SparkSession.stop"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.SparkSession.this"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.SparkSession.setDefaultSession"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.SparkSession.clearDefaultSession"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.SparkSession.getDefaultSession"),

      // RuntimeConfig
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.RuntimeConfig.this"),

      // TypedColumn
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.TypedColumn.this"))
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

  private case class IncludeByName(name: String) extends ProblemFilter {
    private[this] val pattern =
      Pattern.compile(name.split("\\*", -1).map(Pattern.quote).mkString(".*"))

    override def apply(problem: Problem): Boolean = {
      pattern.matcher(problem.matchName.getOrElse("")).matches
    }
  }
}
