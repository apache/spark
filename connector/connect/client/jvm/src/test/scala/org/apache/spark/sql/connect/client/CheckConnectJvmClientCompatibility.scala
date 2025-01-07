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
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util.regex.Pattern

import scala.collection.mutable.{Set => MutableSet}

import com.typesafe.tools.mima.core._
import com.typesafe.tools.mima.lib.MiMaLib

import org.apache.spark.SparkBuildInfo.spark_version
import org.apache.spark.sql.test.IntegrationTestUtils._

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

  private val sqlJar = {
    val path = Paths.get(
      sparkHome,
      "sql",
      "core",
      "target",
      "scala-" + scalaVersion,
      "spark-sql_" + scalaVersion + "-" + spark_version + ".jar")
    assert(Files.exists(path), s"$path does not exist")
    path.toFile
  }

  private val clientJar = {
    val path = Paths.get(
      sparkHome,
      "connector",
      "connect",
      "client",
      "jvm",
      "target",
      "scala-" + scalaVersion,
      "spark-connect-client-jvm_" + scalaVersion + "-" + spark_version + ".jar")
    assert(Files.exists(path), s"$path does not exist")
    path.toFile
  }

  def main(args: Array[String]): Unit = {
    var resultWriter: Writer = null
    try {
      resultWriter = Files.newBufferedWriter(
        Paths.get(s"$sparkHome/.connect-mima-check-result"),
        StandardCharsets.UTF_8)
      val problemsWithSqlModule = checkMiMaCompatibilityWithSqlModule(clientJar, sqlJar)
      appendMimaCheckErrorMessageIfNeeded(
        resultWriter,
        problemsWithSqlModule,
        clientJar,
        sqlJar,
        "Sql")

      val problemsWithClientModule =
        checkMiMaCompatibilityWithReversedSqlModule(clientJar, sqlJar)
      appendMimaCheckErrorMessageIfNeeded(
        resultWriter,
        problemsWithClientModule,
        clientJar,
        sqlJar,
        "ReversedSql",
        "Sql")

      val avroJar: File = findJar("connector/avro", "spark-avro", "spark-avro")
      val problemsWithAvroModule = checkMiMaCompatibilityWithAvroModule(clientJar, avroJar)
      appendMimaCheckErrorMessageIfNeeded(
        resultWriter,
        problemsWithAvroModule,
        clientJar,
        avroJar,
        "Avro")

      val protobufJar: File =
        findJar("connector/protobuf", "spark-protobuf-assembly", "spark-protobuf")
      val problemsWithProtobufModule =
        checkMiMaCompatibilityWithProtobufModule(clientJar, protobufJar)
      appendMimaCheckErrorMessageIfNeeded(
        resultWriter,
        problemsWithProtobufModule,
        clientJar,
        protobufJar,
        "Protobuf")
    } catch {
      case e: Throwable =>
        println(e.getMessage)
        resultWriter.write(s"ERROR: ${e.getMessage}\n")
    } finally {
      if (resultWriter != null) {
        resultWriter.close()
      }
    }
  }

  private def checkMiMaCompatibilityWithAvroModule(
      clientJar: File,
      avroJar: File): List[Problem] = {
    val includedRules = Seq(IncludeByName("org.apache.spark.sql.avro.functions.*"))
    val excludeRules = Seq.empty
    checkMiMaCompatibility(clientJar, avroJar, includedRules, excludeRules)
  }

  private def checkMiMaCompatibilityWithProtobufModule(
      clientJar: File,
      protobufJar: File): List[Problem] = {
    val includedRules = Seq(IncludeByName("org.apache.spark.sql.protobuf.functions.*"))
    val excludeRules = Seq.empty
    checkMiMaCompatibility(clientJar, protobufJar, includedRules, excludeRules)
  }

  private def checkMiMaCompatibilityWithSqlModule(
      clientJar: File,
      sqlJar: File): List[Problem] = {
    val includedRules = Seq(IncludeByName("org.apache.spark.sql.*"))
    val excludeRules = Seq(
      // Filter unsupported rules:
      // Note when muting errors for a method, checks on all overloading methods are also muted.

      // Skip any avro files
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.avro.*"),

      // Skip unsupported packages
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.api.*"), // Java, Python, R
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.catalyst.*"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.columnar.*"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.connector.*"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.classic.*"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.execution.*"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.internal.*"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.jdbc.*"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.scripting.*"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.sources.*"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.streaming.ui.*"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.test.*"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.util.*"),

      // Skip private[sql] constructors
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.*.this"),

      // Skip unsupported classes
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.ExperimentalMethods"),
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.SparkSessionExtensions"),
      ProblemFilters.exclude[MissingClassProblem](
        "org.apache.spark.sql.SparkSessionExtensionsProvider"),
      ProblemFilters.exclude[MissingClassProblem](
        "org.apache.spark.sql.ExtendedExplainGenerator"),
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.UDTFRegistration"),
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.DataSourceRegistration"),
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.TableArg"),
      ProblemFilters.exclude[MissingClassProblem](
        "org.apache.spark.sql.artifact.ArtifactStateForCleanup"),
      ProblemFilters.exclude[MissingClassProblem](
        "org.apache.spark.sql.artifact.ArtifactStateForCleanup$"),

      // DataFrameNaFunctions
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.DataFrameNaFunctions.fillValue"),

      // Dataset
      ProblemFilters.exclude[MissingClassProblem](
        "org.apache.spark.sql.Dataset$" // private[sql]
      ),
      // TODO (SPARK-49096):
      // Mima check might complain the following Dataset rules does not filter any problem.
      // This is due to a potential bug in Mima that all methods in `class Dataset` are not being
      // checked for problems due to the presence of a private[sql] companion object.
      // Further investigation is needed.
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.Dataset.selectUntyped"), // protected

      // RelationalGroupedDataset
      ProblemFilters.exclude[MissingClassProblem](
        "org.apache.spark.sql.RelationalGroupedDataset$*" // private[sql]
      ),

      // SparkSession
      ProblemFilters.exclude[DirectMissingMethodProblem](
        "org.apache.spark.sql.SparkSession.baseRelationToDataFrame"),
      ProblemFilters.exclude[DirectMissingMethodProblem](
        "org.apache.spark.sql.SparkSession.canUseSession"),

      // DataStreamWriter
      ProblemFilters.exclude[MissingClassProblem](
        "org.apache.spark.sql.streaming.DataStreamWriter$"),
      ProblemFilters.exclude[Problem](
        "org.apache.spark.sql.streaming.DataStreamWriter.SOURCE*" // These are constant vals.
      ),

      // Classes missing from streaming API
      ProblemFilters.exclude[MissingClassProblem](
        "org.apache.spark.sql.streaming.TestGroupState"),
      ProblemFilters.exclude[MissingClassProblem](
        "org.apache.spark.sql.streaming.TestGroupState$"),

      // Artifact Manager, client has a totally different implementation.
      ProblemFilters.exclude[MissingClassProblem](
        "org.apache.spark.sql.artifact.ArtifactManager"),
      ProblemFilters.exclude[MissingClassProblem](
        "org.apache.spark.sql.artifact.ArtifactManager$"),
      ProblemFilters.exclude[MissingClassProblem](
        "org.apache.spark.sql.artifact.ArtifactManager$SparkContextResourceType$"),

      // ColumnNode conversions
      ProblemFilters.exclude[MissingTypesProblem]("org.apache.spark.sql.SparkSession"),
      ProblemFilters.exclude[DirectMissingMethodProblem](
        "org.apache.spark.sql.SparkSession.expression"),
      ProblemFilters.exclude[DirectMissingMethodProblem](
        "org.apache.spark.sql.SparkSession.toRichColumn"),

      // UDFRegistration
      ProblemFilters.exclude[DirectMissingMethodProblem](
        "org.apache.spark.sql.UDFRegistration.register"),
      ProblemFilters.exclude[MissingTypesProblem]("org.apache.spark.sql.UDFRegistration"),
      ProblemFilters.exclude[DirectMissingMethodProblem](
        "org.apache.spark.sql.UDFRegistration.log*"),
      ProblemFilters.exclude[DirectMissingMethodProblem](
        "org.apache.spark.sql.UDFRegistration.LogStringContext"),
      ProblemFilters.exclude[DirectMissingMethodProblem](
        "org.apache.spark.sql.UDFRegistration.withLogContext"),
      ProblemFilters.exclude[DirectMissingMethodProblem](
        "org.apache.spark.sql.UDFRegistration.isTraceEnabled"),
      ProblemFilters.exclude[DirectMissingMethodProblem](
        "org.apache.spark.sql.UDFRegistration.initializeForcefully"),
      ProblemFilters.exclude[DirectMissingMethodProblem](
        "org.apache.spark.sql.UDFRegistration.initializeLogIfNecessary"),
      ProblemFilters.exclude[DirectMissingMethodProblem](
        "org.apache.spark.sql.UDFRegistration.initializeLogIfNecessary$default$2"),

      // Protected DataFrameReader methods...
      ProblemFilters.exclude[DirectMissingMethodProblem](
        "org.apache.spark.sql.DataFrameReader.validateSingleVariantColumn"),
      ProblemFilters.exclude[DirectMissingMethodProblem](
        "org.apache.spark.sql.DataFrameReader.validateJsonSchema"),
      ProblemFilters.exclude[DirectMissingMethodProblem](
        "org.apache.spark.sql.DataFrameReader.validateXmlSchema"),

      // Protected DataStreamReader methods...
      ProblemFilters.exclude[DirectMissingMethodProblem](
        "org.apache.spark.sql.streaming.DataStreamReader.validateJsonSchema"),
      ProblemFilters.exclude[DirectMissingMethodProblem](
        "org.apache.spark.sql.streaming.DataStreamReader.validateXmlSchema"))

    checkMiMaCompatibility(clientJar, sqlJar, includedRules, excludeRules)
  }

  /**
   * This check ensures client jar dose not expose any unwanted APIs by mistake.
   */
  private def checkMiMaCompatibilityWithReversedSqlModule(
      clientJar: File,
      sqlJar: File): List[Problem] = {
    val includedRules = Seq(IncludeByName("org.apache.spark.sql.*"))
    val excludeRules = Seq(
      // Skipped packages
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.application.*"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.connect.*"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.internal.*"),

      // private[sql]
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.*.this"),
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.DataFrameStatFunctions$"),
      ProblemFilters.exclude[MissingClassProblem](
        "org.apache.spark.sql.KeyValueGroupedDatasetImpl"),
      ProblemFilters.exclude[MissingClassProblem](
        "org.apache.spark.sql.KeyValueGroupedDatasetImpl$"),

      // ColumnNode conversions
      ProblemFilters.exclude[DirectMissingMethodProblem](
        "org.apache.spark.sql.SparkSession.RichColumn"),
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.SparkSession$RichColumn"),

      // New public APIs added in the client
      // Dataset
      ProblemFilters.exclude[DirectMissingMethodProblem](
        "org.apache.spark.sql.Dataset.plan"
      ), // developer API
      ProblemFilters.exclude[DirectMissingMethodProblem](
        "org.apache.spark.sql.Dataset.collectResult"),

      // SparkSession
      // developer API
      ProblemFilters.exclude[DirectMissingMethodProblem](
        "org.apache.spark.sql.SparkSession.newDataFrame"),
      ProblemFilters.exclude[DirectMissingMethodProblem](
        "org.apache.spark.sql.SparkSession.newDataset"),
      ProblemFilters.exclude[DirectMissingMethodProblem](
        "org.apache.spark.sql.SparkSession.execute"),
      // Experimental
      ProblemFilters.exclude[DirectMissingMethodProblem](
        "org.apache.spark.sql.SparkSession.registerClassFinder"),
      ProblemFilters.exclude[IncompatibleSignatureProblem](
        "org.apache.spark.sql.SparkSession.baseRelationToDataFrame"),
      // SparkSession#Builder
      ProblemFilters.exclude[DirectMissingMethodProblem](
        "org.apache.spark.sql.SparkSession#Builder.client"),
      ProblemFilters.exclude[DirectMissingMethodProblem](
        "org.apache.spark.sql.SparkSession#Builder.build" // deprecated
      ),
      ProblemFilters.exclude[DirectMissingMethodProblem](
        "org.apache.spark.sql.SparkSession#Builder.interceptor"),

      // Private case class in SQLContext
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.SQLContext$ListTableRow"),
      ProblemFilters.exclude[MissingClassProblem](
        "org.apache.spark.sql.SQLContext$ListTableRow$"),

      // SQLImplicits
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.SQLImplicits.session"),

      // Steaming API
      ProblemFilters.exclude[MissingClassProblem](
        "org.apache.spark.sql.streaming.RemoteStreamingQuery"),
      ProblemFilters.exclude[MissingClassProblem](
        "org.apache.spark.sql.streaming.RemoteStreamingQuery$"),
      // Skip client side listener specific class
      ProblemFilters.exclude[MissingClassProblem](
        "org.apache.spark.sql.streaming.StreamingQueryListenerBus"))

    checkMiMaCompatibility(sqlJar, clientJar, includedRules, excludeRules)
  }

  /**
   * MiMa takes a new jar and an old jar as inputs and then reports all incompatibilities found in
   * the new jar. The incompatibility result is then filtered using include and exclude rules.
   * Include rules are first applied to find all client classes that need to be checked. Then
   * exclude rules are applied to filter out all unsupported methods in the client classes.
   */
  private def checkMiMaCompatibility(
      newJar: File,
      oldJar: File,
      includedRules: Seq[IncludeByName],
      excludeRules: Seq[ProblemFilter]): List[Problem] = {
    val mima = new MiMaLib(Seq(newJar, oldJar))
    val allProblems = mima.collectProblems(oldJar, newJar, List.empty)

    val effectiveExcludeRules = MutableSet.empty[ProblemFilter]
    val problems = allProblems
      .filter { p =>
        includedRules.exists(rule => rule(p))
      }
      .filter { p =>
        excludeRules.forall { rule =>
          val passedRule = rule(p)
          if (!passedRule) {
            effectiveExcludeRules += rule
          }
          passedRule
        }
      }
    excludeRules.filterNot(effectiveExcludeRules.contains).foreach { rule =>
      println(s"Warning: $rule did not filter out any problems.")
    }
    problems
  }

  private def appendMimaCheckErrorMessageIfNeeded(
      resultWriter: Writer,
      problems: List[Problem],
      clientModule: File,
      targetModule: File,
      targetName: String,
      description: String = "client"): Unit = {
    if (problems.nonEmpty) {
      resultWriter.write(
        s"ERROR: Comparing Client jar: $clientModule and $targetName jar: $targetModule \n")
      resultWriter.write(s"problems with $targetName module: \n")
      val problemDescriptions =
        problems.map(p => s"${p.getClass.getSimpleName}: ${p.description(description)}")
      resultWriter.write(problemDescriptions.mkString("\n"))
      resultWriter.write("\n\n")
      resultWriter.write("Exceptions to binary compatibility can be added in " +
        s"'CheckConnectJvmClientCompatibility#checkMiMaCompatibilityWith${targetName}Module':\n")
      resultWriter.write(problems.flatMap(_.howToFilter).distinct.mkString(",\n"))
      resultWriter.write("\n\n")
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
