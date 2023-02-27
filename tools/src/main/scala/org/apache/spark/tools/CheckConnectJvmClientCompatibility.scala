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

package org.apache.spark.tools

import java.io.{File, Writer}
import java.lang.reflect.{Method, Modifier}
import java.net.URLClassLoader
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util.regex.Pattern

import scala.reflect.runtime.universe.runtimeMirror

import com.typesafe.tools.mima.core._
import com.typesafe.tools.mima.lib.MiMaLib

import org.apache.spark.tools.GenerateMIMAIgnore.{classLoader, mirror}

/**
 * A tool for checking the binary compatibility of the connect client API against the spark SQL API
 * using MiMa. We did not write this check using a SBT build rule as the rule cannot provide the
 * same level of freedom as a test. With a test we can:
 *   1. Specify any two jars to run the compatibility check.
 *   1. Easily make the test automatically pick up all new methods added while the client is being
 *      built.
 *
 * We can run this check by executing the `dev/connect-jvm-client-mima-check`。
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
      resultWriter =
        Files.newBufferedWriter(Paths.get(
          s"$sparkHome/.connect-mima-check-result"), StandardCharsets.UTF_8)
      val clientJar: File =
        findJar("connector/connect/client/jvm", "spark-connect-client-jvm-assembly")
      val sqlJar: File = findJar("sql/core", "spark-sql")
      val problems = checkMiMaCompatibility(clientJar, sqlJar)
      if (problems.nonEmpty) {
        resultWriter.write(s"ERROR: Comparing client jar: $clientJar and and sql jar: $sqlJar \n")
        resultWriter.write(s"problems: \n")
        resultWriter.write(s"${problems.map(p => p.description("client")).mkString("\n")}")
        resultWriter.write("\n")
      }
      val incompatibleApis = checkDatasetApiCompatibility(clientJar, sqlJar)
      if (incompatibleApis.nonEmpty) {
        resultWriter.write("ERROR: Dataset apis incompatible with the sql module include: \n")
        resultWriter.write(incompatibleApis.mkString("\n"))
        resultWriter.write("\n")
      }
    } catch {
      case e: Exception =>
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
      IncludeByName("org.apache.spark.sql.Column.*"),
      IncludeByName("org.apache.spark.sql.ColumnName.*"),
      IncludeByName("org.apache.spark.sql.DataFrame.*"),
      IncludeByName("org.apache.spark.sql.DataFrameReader.*"),
      IncludeByName("org.apache.spark.sql.DataFrameWriter.*"),
      IncludeByName("org.apache.spark.sql.DataFrameWriterV2.*"),
      IncludeByName("org.apache.spark.sql.Dataset.*"),
      IncludeByName("org.apache.spark.sql.functions.*"),
      IncludeByName("org.apache.spark.sql.RelationalGroupedDataset.*"),
      IncludeByName("org.apache.spark.sql.SparkSession.*"))
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
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.Dataset.groupBy"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.Dataset.observe"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.Dataset.queryExecution"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.Dataset.encoder"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.Dataset.sqlContext"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.Dataset.as"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.Dataset.checkpoint"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.Dataset.localCheckpoint"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.Dataset.withWatermark"),
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
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.Dataset.cache"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.Dataset.storageLevel"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.Dataset.unpersist"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.Dataset.rdd"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.Dataset.toJavaRDD"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.Dataset.javaRDD"),
      ProblemFilters.exclude[Problem](
        "org.apache.spark.sql.Dataset.registerTempTable"
      ), // deprecated
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.Dataset.createTempView"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.Dataset.createOrReplaceTempView"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.Dataset.createGlobalTempView"),
      ProblemFilters.exclude[Problem](
        "org.apache.spark.sql.Dataset.createOrReplaceGlobalTempView"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.Dataset.writeStream"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.Dataset.toJSON"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.Dataset.sameSemantics"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.Dataset.semanticHash"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.Dataset.this"),

      // functions
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.functions.udf"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.functions.call_udf"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.functions.callUDF"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.functions.unwrap_udt"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.functions.udaf"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.functions.broadcast"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.functions.count"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.functions.typedlit"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.functions.typedLit"),

      // RelationalGroupedDataset
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.RelationalGroupedDataset.apply"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.RelationalGroupedDataset.as"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.RelationalGroupedDataset.pivot"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.RelationalGroupedDataset.this"),

      // SparkSession
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.SparkSession.active"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.SparkSession.getDefaultSession"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.SparkSession.getActiveSession"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.SparkSession.clearDefaultSession"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.SparkSession.setDefaultSession"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.SparkSession.implicits"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.SparkSession.sparkContext"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.SparkSession.version"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.SparkSession.sharedState"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.SparkSession.sessionState"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.SparkSession.sqlContext"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.SparkSession.conf"),
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
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.SparkSession.time"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.SparkSession.stop"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.SparkSession.this"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.SparkSession.setActiveSession"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.SparkSession.clearActiveSession"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.SparkSession.setDefaultSession"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.SparkSession.clearDefaultSession"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.SparkSession.getActiveSession"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.SparkSession.getDefaultSession"),
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.SparkSession.range"))
    allProblems
      .filter { p =>
        includedRules.exists(rule => rule(p))
      }
      .filter { p =>
        excludeRules.forall(rule => rule(p))
      }
  }

  private def checkDatasetApiCompatibility(clientJar: File, sqlJar: File): Array[String] = {

    def publicMethodsNames(methods: Array[Method]): Array[String] = {
      methods.filter(m => Modifier.isPublic(m.getModifiers))
        .filterNot(_.toString.contains(".$anonfun")).map(_.toString)
    }

    val clientClassLoader: URLClassLoader = new URLClassLoader(Seq(clientJar.toURI.toURL).toArray)
    val sqlClassLoader: URLClassLoader = new URLClassLoader(Seq(sqlJar.toURI.toURL).toArray)

    val clientClass = clientClassLoader.loadClass("org.apache.spark.sql.Dataset")
    val sqlClass = sqlClassLoader.loadClass("org.apache.spark.sql.Dataset")

    val newMethods = publicMethodsNames(clientClass.getMethods)
    val oldMethods = publicMethodsNames(sqlClass.getMethods)

    // For now we simply check the new methods is a subset of the old methods.
    newMethods.diff(oldMethods)
  }

  /**
   * Find a jar in the Spark project artifacts. It requires a build first (e.g. sbt package)
   * so that this method can find the jar in the target folders.
   *
   * @return the jar
   */
  private def findJar(path: String, sbtName: String): File = {
    val targetDir = new File(new File(sparkHome, path), "target")
    if (!targetDir.exists()) {
      throw new IllegalStateException("Fail to locate the target folder: " +
        s"'${targetDir.getCanonicalPath}'. " +
        s"SPARK_HOME='${new File(sparkHome).getCanonicalPath}'. " +
        "Make sure the spark project jars has been built (e.g. using sbt package)" +
        "and the env variable `SPARK_HOME` is set correctly.")
    }
    val jars = recursiveListFiles(targetDir).filter { f =>
      // SBT jar
      f.getParentFile.getName.startsWith("scala-") &&
        f.getName.startsWith(sbtName) && f.getName.endsWith(".jar")
    }
    // It is possible we found more than one: one built by maven, and another by SBT
    if (jars.isEmpty) {
      throw new IllegalStateException(
        s"Failed to find the jar inside folder: ${targetDir.getCanonicalPath}")
    }
    println("Using jar: " + jars(0).getCanonicalPath)
    jars(0) // return the first jar found
  }

  private def recursiveListFiles(f: File): Array[File] = {
    val these = f.listFiles
    these ++ these.filter(_.isDirectory).flatMap(recursiveListFiles)
  }

  private case class IncludeByName(name: String) extends ProblemFilter {
    private[this] val pattern =
      Pattern.compile(name.split("\\*", -1).map(Pattern.quote).mkString(".*"))

    override def apply(problem: Problem): Boolean = {
      pattern.matcher(problem.matchName.getOrElse("")).matches
    }
  }
}
