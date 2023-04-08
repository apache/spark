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

import java.io.{File, Writer}
import java.net.URLClassLoader
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util.regex.Pattern

import scala.reflect.runtime.universe.runtimeMirror

import com.typesafe.tools.mima.core.{Problem, ProblemFilter, ProblemFilters}
import com.typesafe.tools.mima.lib.MiMaLib

import org.apache.spark.sql.connect.client.util.IntegrationTestUtils._
import org.apache.spark.util.ChildFirstURLClassLoader

/**
 * A tool for checking the binary compatibility of the connect client API against the spark SQL
 * API using MiMa. We did not write this check using a SBT build rule as the rule cannot provide
 * the same level of freedom as a test. With a test we can:
 *   1. Specify any two jars to run the compatibility check.
 *   1. Easily make the test automatically pick up all new methods added while the client is being
 *      built.
 *
 * We can run this check by executing the `dev/connect-jvm-client-mima-check`.
 */
// scalastyle:off println
object CheckConnectJvmClientCompatibility {

  private lazy val sparkHome: String = {
    if (!sys.env.contains("SPARK_HOME")) {
      throw new IllegalArgumentException("SPARK_HOME is not set.")
    }
    sys.env("SPARK_HOME")
  }

  def main(args: Array[String]): Unit = {
    var resultWriter: Writer = null
    try {
      resultWriter = Files.newBufferedWriter(
        Paths.get(s"$sparkHome/.connect-mima-check-result"),
        StandardCharsets.UTF_8)
      val clientJar: File =
        findJar(
          "connector/connect/client/jvm",
          "spark-connect-client-jvm-assembly",
          "spark-connect-client-jvm")
      val sqlJar: File = findJar("sql/core", "spark-sql", "spark-sql")
      val problems = checkMiMaCompatibility(clientJar, sqlJar)
      if (problems.nonEmpty) {
        resultWriter.write(s"ERROR: Comparing client jar: $clientJar and and sql jar: $sqlJar \n")
        resultWriter.write(s"problems: \n")
        resultWriter.write(s"${problems.map(p => p.description("client")).mkString("\n")}")
        resultWriter.write("\n")
        resultWriter.write(
          "Exceptions to binary compatibility can be added in " +
            "'CheckConnectJvmClientCompatibility#checkMiMaCompatibility'\n")
      }
      val incompatibleApis = checkDatasetApiCompatibility(clientJar, sqlJar)
      if (incompatibleApis.nonEmpty) {
        resultWriter.write(
          "ERROR: The Dataset apis only exist in the connect client " +
            "module and not belong to the sql module include: \n")
        resultWriter.write(incompatibleApis.mkString("\n"))
        resultWriter.write("\n")
        resultWriter.write(
          "Exceptions can be added to exceptionMethods in " +
            "'CheckConnectJvmClientCompatibility#checkDatasetApiCompatibility'\n")
      }
    } catch {
      case e: Throwable =>
        println(e.getMessage)
        resultWriter.write(s"ERROR: ${e.getMessage}")
    } finally {
      if (resultWriter != null) {
        resultWriter.close()
      }
    }
  }

  /**
   * MiMa takes an old jar (sql jar) and a new jar (client jar) as inputs and then reports all
   * incompatibilities found in the new jar. The incompatibility result is then filtered using
   * include and exclude rules. Include rules are first applied to find all client classes that
   * need to be checked. Then exclude rules are applied to filter out all unsupported methods in
   * the client classes.
   */
  private def checkMiMaCompatibility(clientJar: File, sqlJar: File): List[Problem] = {
    val mima = new MiMaLib(Seq(clientJar, sqlJar))
    val allProblems = mima.collectProblems(sqlJar, clientJar, List.empty)
    val includedRules = Seq(
      IncludeByName("org.apache.spark.sql.catalog.Catalog.*"),
      IncludeByName("org.apache.spark.sql.catalog.CatalogMetadata.*"),
      IncludeByName("org.apache.spark.sql.catalog.Column.*"),
      IncludeByName("org.apache.spark.sql.catalog.Database.*"),
      IncludeByName("org.apache.spark.sql.catalog.Function.*"),
      IncludeByName("org.apache.spark.sql.catalog.Table.*"),
      IncludeByName("org.apache.spark.sql.Column.*"),
      IncludeByName("org.apache.spark.sql.ColumnName.*"),
      IncludeByName("org.apache.spark.sql.DataFrame.*"),
      IncludeByName("org.apache.spark.sql.DataFrameReader.*"),
      IncludeByName("org.apache.spark.sql.DataFrameNaFunctions.*"),
      IncludeByName("org.apache.spark.sql.DataFrameStatFunctions.*"),
      IncludeByName("org.apache.spark.sql.DataFrameWriter.*"),
      IncludeByName("org.apache.spark.sql.DataFrameWriterV2.*"),
      IncludeByName("org.apache.spark.sql.Dataset.*"),
      IncludeByName("org.apache.spark.sql.functions.*"),
      IncludeByName("org.apache.spark.sql.RelationalGroupedDataset.*"),
      IncludeByName("org.apache.spark.sql.SparkSession.*"),
      IncludeByName("org.apache.spark.sql.RuntimeConfig.*"),
      IncludeByName("org.apache.spark.sql.TypedColumn.*"),
      IncludeByName("org.apache.spark.sql.SQLImplicits.*"),
      IncludeByName("org.apache.spark.sql.DatasetHolder.*"))
    val excludeRules = Seq(
      // Filter unsupported rules:
      // Note when muting errors for a method, checks on all overloading methods are also muted.

      // Skip all shaded dependencies and proto files in the client.
      ProblemFilters.exclude[Problem]("org.sparkproject.*"),
      ProblemFilters.exclude[Problem]("org.apache.spark.connect.proto.*"),

      // DataFrame Reader & Writer
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.DataFrameReader.json"), // deprecated

      // DataFrameNaFunctions
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.DataFrameNaFunctions.this"),

      // DataFrameStatFunctions
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.DataFrameStatFunctions.this"),

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
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.Dataset.joinWith"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.Dataset.metadataColumn"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.Dataset.selectUntyped"), // protected
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.Dataset.reduce"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.Dataset.groupByKey"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.Dataset.explode"), // deprecated
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
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.functions.typedlit"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.functions.typedLit"),

      // RelationalGroupedDataset
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.RelationalGroupedDataset.apply"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.RelationalGroupedDataset.as"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.RelationalGroupedDataset.this"),

      // SparkSession
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
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.SparkSession.createDataFrame"),
      ProblemFilters.exclude[Problem](
        "org.apache.spark.sql.SparkSession.baseRelationToDataFrame"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.SparkSession.createDataset"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.SparkSession.executeCommand"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.SparkSession.readStream"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.SparkSession.this"),

      // RuntimeConfig
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.RuntimeConfig.this"),

      // TypedColumn
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.TypedColumn.this"),

      // SQLImplicits
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.SQLImplicits.this"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.SQLImplicits.rddToDatasetHolder"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.SQLImplicits._sqlContext"))
    val problems = allProblems
      .filter { p =>
        includedRules.exists(rule => rule(p))
      }
      .filter { p =>
        excludeRules.forall(rule => rule(p))
      }
    problems
  }

  private def checkDatasetApiCompatibility(clientJar: File, sqlJar: File): Seq[String] = {

    def methods(jar: File, className: String): Seq[String] = {
      val classLoader: URLClassLoader =
        new ChildFirstURLClassLoader(Seq(jar.toURI.toURL).toArray, this.getClass.getClassLoader)
      val mirror = runtimeMirror(classLoader)
      // scalastyle:off classforname
      val classSymbol =
        mirror.classSymbol(Class.forName(className, false, classLoader))
      // scalastyle:on classforname
      classSymbol.typeSignature.members
        .filter(_.isMethod)
        .map(_.asMethod)
        .filter(m => m.isPublic)
        .map(_.fullName)
        .toSeq
    }

    val className = "org.apache.spark.sql.Dataset"
    val clientMethods = methods(clientJar, className)
    val sqlMethods = methods(sqlJar, className)
    // Exclude some public methods that must be added through `exceptionMethods`
    val exceptionMethods =
      Seq("org.apache.spark.sql.Dataset.collectResult", "org.apache.spark.sql.Dataset.plan")

    // Find new public functions that are not in sql module `Dataset`.
    clientMethods.diff(sqlMethods).diff(exceptionMethods)
  }

  private case class IncludeByName(name: String) extends ProblemFilter {
    private[this] val pattern =
      Pattern.compile(name.split("\\*", -1).map(Pattern.quote).mkString(".*"))

    override def apply(problem: Problem): Boolean = {
      pattern.matcher(problem.matchName.getOrElse("")).matches
    }
  }
}
