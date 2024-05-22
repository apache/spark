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

      // Skip unsupported packages
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.api.*"), // Java, Python, R
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.catalyst.*"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.columnar.*"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.connector.*"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.execution.*"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.expressions.*"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.internal.*"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.jdbc.*"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.sources.*"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.streaming.ui.*"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.test.*"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.util.*"),

      // Skip private[sql] constructors
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.*.this"),

      // Skip unsupported classes
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.ExperimentalMethods"),
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.SQLContext"),
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.SQLContext$*"),
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.SparkSessionExtensions"),
      ProblemFilters.exclude[MissingClassProblem](
        "org.apache.spark.sql.SparkSessionExtensionsProvider"),
      ProblemFilters.exclude[MissingClassProblem](
        "org.apache.spark.sql.ExtendedExplainGenerator"),
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.UDTFRegistration"),
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.UDFRegistration$"),
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.DataSourceRegistration"),

      // DataFrame Reader & Writer
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.DataFrameReader.json"), // rdd

      // DataFrameNaFunctions
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.DataFrameNaFunctions.fillValue"),

      // Dataset
      ProblemFilters.exclude[MissingClassProblem](
        "org.apache.spark.sql.Dataset$" // private[sql]
      ),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.Dataset.ofRows"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.Dataset.DATASET_ID_TAG"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.Dataset.COL_POS_KEY"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.Dataset.DATASET_ID_KEY"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.Dataset.curId"),
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.ObservationListener"),
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.ObservationListener$"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.Dataset.queryExecution"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.Dataset.sqlContext"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.Dataset.selectUntyped"), // protected
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.Dataset.rdd"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.Dataset.toJavaRDD"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.Dataset.javaRDD"),

      // functions
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.functions.unwrap_udt"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.functions.udaf"),

      // KeyValueGroupedDataset
      ProblemFilters.exclude[Problem](
        "org.apache.spark.sql.KeyValueGroupedDataset.queryExecution"),

      // RelationalGroupedDataset
      ProblemFilters.exclude[MissingClassProblem](
        "org.apache.spark.sql.RelationalGroupedDataset$*" // private[sql]
      ),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.RelationalGroupedDataset.apply"),

      // SparkSession
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.SparkSession.sparkContext"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.SparkSession.sharedState"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.SparkSession.sessionState"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.SparkSession.sqlContext"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.SparkSession.listenerManager"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.SparkSession.experimental"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.SparkSession.udtf"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.SparkSession.dataSource"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.SparkSession.createDataFrame"),
      ProblemFilters.exclude[Problem](
        "org.apache.spark.sql.SparkSession.baseRelationToDataFrame"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.SparkSession.createDataset"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.SparkSession.executeCommand"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.SparkSession.this"),

      // SparkSession#implicits
      ProblemFilters.exclude[DirectMissingMethodProblem](
        "org.apache.spark.sql.SparkSession#implicits._sqlContext"),

      // SparkSession#Builder
      ProblemFilters.exclude[DirectMissingMethodProblem](
        "org.apache.spark.sql.SparkSession#Builder.config"),
      ProblemFilters.exclude[DirectMissingMethodProblem](
        "org.apache.spark.sql.SparkSession#Builder.withExtensions"),

      // RuntimeConfig
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.RuntimeConfig$"),
      ProblemFilters.exclude[DirectMissingMethodProblem](
        "org.apache.spark.sql.RuntimeConfig.sqlConf"),

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
      ProblemFilters.exclude[MissingClassProblem](
        "org.apache.spark.sql.streaming.PythonStreamingQueryListener"),
      ProblemFilters.exclude[MissingClassProblem](
        "org.apache.spark.sql.streaming.PythonStreamingQueryListenerWrapper"),
      ProblemFilters.exclude[MissingTypesProblem](
        "org.apache.spark.sql.streaming.StreamingQueryListener$Event"),
      ProblemFilters.exclude[MissingTypesProblem](
        "org.apache.spark.sql.streaming.StreamingQueryListener$QueryIdleEvent"),
      ProblemFilters.exclude[DirectMissingMethodProblem](
        "org.apache.spark.sql.streaming.StreamingQueryListener#QueryIdleEvent.logEvent"),
      ProblemFilters.exclude[MissingTypesProblem](
        "org.apache.spark.sql.streaming.StreamingQueryListener$QueryProgressEvent"),
      ProblemFilters.exclude[DirectMissingMethodProblem](
        "org.apache.spark.sql.streaming.StreamingQueryListener#QueryProgressEvent.logEvent"),
      ProblemFilters.exclude[MissingTypesProblem](
        "org.apache.spark.sql.streaming.StreamingQueryListener$QueryStartedEvent"),
      ProblemFilters.exclude[DirectMissingMethodProblem](
        "org.apache.spark.sql.streaming.StreamingQueryListener#QueryStartedEvent.logEvent"),
      ProblemFilters.exclude[MissingTypesProblem](
        "org.apache.spark.sql.streaming.StreamingQueryListener$QueryTerminatedEvent"),
      ProblemFilters.exclude[DirectMissingMethodProblem](
        "org.apache.spark.sql.streaming.StreamingQueryListener#QueryTerminatedEvent.logEvent"),

      // SQLImplicits
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.SQLImplicits.rddToDatasetHolder"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.SQLImplicits._sqlContext"),

      // Artifact Manager
      ProblemFilters.exclude[MissingClassProblem](
        "org.apache.spark.sql.artifact.ArtifactManager"),
      ProblemFilters.exclude[MissingClassProblem](
        "org.apache.spark.sql.artifact.ArtifactManager$"),
      ProblemFilters.exclude[MissingClassProblem](
        "org.apache.spark.sql.artifact.util.ArtifactUtils"),
      ProblemFilters.exclude[MissingClassProblem](
        "org.apache.spark.sql.artifact.util.ArtifactUtils$"),

      // MergeIntoWriter
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.MergeIntoWriter"),
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.MergeIntoWriter$"),
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.WhenMatched"),
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.WhenMatched$"),
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.WhenNotMatched"),
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.WhenNotMatched$"),
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.WhenNotMatchedBySource"),
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.WhenNotMatchedBySource$"))
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
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.avro.*"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.connect.*"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.protobuf.*"),

      // private[sql]
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.*.this"),
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.DataFrameStatFunctions$"),
      ProblemFilters.exclude[MissingClassProblem](
        "org.apache.spark.sql.KeyValueGroupedDatasetImpl"),
      ProblemFilters.exclude[MissingClassProblem](
        "org.apache.spark.sql.KeyValueGroupedDatasetImpl$"),
      ProblemFilters.exclude[ReversedMissingMethodProblem](
        "org.apache.spark.sql.SQLImplicits._sqlContext" // protected
      ),

      // Catalyst Refactoring
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.catalyst.util.SparkCollectionUtils"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.catalyst.util.SparkCollectionUtils$"),

      // New public APIs added in the client
      // ScalarUserDefinedFunction
      ProblemFilters
        .exclude[MissingClassProblem](
          "org.apache.spark.sql.expressions.ScalarUserDefinedFunction"),
      ProblemFilters.exclude[MissingClassProblem](
        "org.apache.spark.sql.expressions.ScalarUserDefinedFunction$"),

      // New private API added in the client
      ProblemFilters
        .exclude[MissingClassProblem](
          "org.apache.spark.sql.expressions.SparkConnectClosureCleaner"),
      ProblemFilters
        .exclude[MissingClassProblem](
          "org.apache.spark.sql.expressions.SparkConnectClosureCleaner$"),

      // Column
      // developer API
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.sql.Column.apply"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.sql.Column.expr"),

      // Dataset
      ProblemFilters.exclude[DirectMissingMethodProblem](
        "org.apache.spark.sql.Dataset.plan"
      ), // developer API
      ProblemFilters.exclude[DirectMissingMethodProblem](
        "org.apache.spark.sql.Dataset.collectResult"),

      // RuntimeConfig
      ProblemFilters.exclude[MissingTypesProblem](
        "org.apache.spark.sql.RuntimeConfig" // Client version extends Logging
      ),
      ProblemFilters.exclude[Problem](
        "org.apache.spark.sql.RuntimeConfig.*" // Mute missing Logging methods
      ),
      // ConnectRepl
      ProblemFilters.exclude[MissingClassProblem](
        "org.apache.spark.sql.application.ConnectRepl" // developer API
      ),
      ProblemFilters.exclude[MissingClassProblem](
        "org.apache.spark.sql.application.ConnectRepl$" // developer API
      ),
      ProblemFilters.exclude[MissingClassProblem](
        "org.apache.spark.sql.application.ExtendedCodeClassWrapper" // developer API
      ),
      ProblemFilters.exclude[MissingClassProblem](
        "org.apache.spark.sql.application.ExtendedCodeClassWrapper$" // developer API
      ),

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
        "org.apache.spark.sql.SparkSession.addArtifact"),
      ProblemFilters.exclude[DirectMissingMethodProblem](
        "org.apache.spark.sql.SparkSession.addArtifacts"),
      ProblemFilters.exclude[DirectMissingMethodProblem](
        "org.apache.spark.sql.SparkSession.registerClassFinder"),
      // public
      ProblemFilters.exclude[DirectMissingMethodProblem](
        "org.apache.spark.sql.SparkSession.interruptAll"),
      ProblemFilters.exclude[DirectMissingMethodProblem](
        "org.apache.spark.sql.SparkSession.interruptTag"),
      ProblemFilters.exclude[DirectMissingMethodProblem](
        "org.apache.spark.sql.SparkSession.interruptOperation"),
      ProblemFilters.exclude[DirectMissingMethodProblem](
        "org.apache.spark.sql.SparkSession.addTag"),
      ProblemFilters.exclude[DirectMissingMethodProblem](
        "org.apache.spark.sql.SparkSession.removeTag"),
      ProblemFilters.exclude[DirectMissingMethodProblem](
        "org.apache.spark.sql.SparkSession.getTags"),
      ProblemFilters.exclude[DirectMissingMethodProblem](
        "org.apache.spark.sql.SparkSession.clearTags"),
      // SparkSession#Builder
      ProblemFilters.exclude[DirectMissingMethodProblem](
        "org.apache.spark.sql.SparkSession#Builder.remote"),
      ProblemFilters.exclude[DirectMissingMethodProblem](
        "org.apache.spark.sql.SparkSession#Builder.client"),
      ProblemFilters.exclude[DirectMissingMethodProblem](
        "org.apache.spark.sql.SparkSession#Builder.build" // deprecated
      ),
      ProblemFilters.exclude[DirectMissingMethodProblem](
        "org.apache.spark.sql.SparkSession#Builder.create"),
      ProblemFilters.exclude[DirectMissingMethodProblem](
        "org.apache.spark.sql.SparkSession#Builder.interceptor"),

      // Steaming API
      ProblemFilters.exclude[MissingTypesProblem](
        "org.apache.spark.sql.streaming.DataStreamWriter" // Client version extends Logging
      ),
      ProblemFilters.exclude[Problem](
        "org.apache.spark.sql.streaming.DataStreamWriter.*" // Mute missing Logging methods
      ),
      ProblemFilters.exclude[MissingClassProblem](
        "org.apache.spark.sql.streaming.RemoteStreamingQuery"),
      ProblemFilters.exclude[MissingClassProblem](
        "org.apache.spark.sql.streaming.RemoteStreamingQuery$"),
      // Skip client side listener specific class
      ProblemFilters.exclude[MissingClassProblem](
        "org.apache.spark.sql.streaming.StreamingQueryListenerBus"),

      // Encoders are in the wrong JAR
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.Encoders"),
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.Encoders$"))

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
    val problems = allProblems
      .filter { p =>
        includedRules.exists(rule => rule(p))
      }
      .filter { p =>
        excludeRules.forall(rule => rule(p))
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
      resultWriter.write(s"${problems.map(p => p.description(description)).mkString("\n")}")
      resultWriter.write("\n")
      resultWriter.write(
        "Exceptions to binary compatibility can be added in " +
          s"'CheckConnectJvmClientCompatibility#checkMiMaCompatibilityWith${targetName}Module'\n")
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
