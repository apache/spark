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

package org.apache.spark.sql.connect.pipelines

import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.TimeUnit

import scala.concurrent.duration.Duration

import org.apache.spark.api.python.PythonUtils
import org.apache.spark.sql.connect.{PythonTestDepsChecker, SparkConnectServerTest}
import org.apache.spark.sql.pipelines.utils.{APITest, PipelineReference, PipelineSourceFile, PipelineTest, TestPipelineConfiguration, TestPipelineSpec}

case class PipelineReferenceImpl(executionProcess: Process) extends PipelineReference

/**
 * End-to-end test suite for the Spark Declarative Pipelines API using the CLI.
 *
 * This suite creates a temporary directory for each test case, writes the necessary pipeline
 * specification and source files, and invokes the CLI as a separate process.
 */
class EndToEndAPISuite extends PipelineTest with APITest with SparkConnectServerTest {

  // Directory where the pipeline files will be created
  private var projectDir: Path = _

  override def test(testName: String, testTags: org.scalatest.Tag*)(testFun: => Any)(implicit
      pos: org.scalactic.source.Position): Unit = {
    super.test(testName, testTags: _*) {
      withTempDir { dir =>
        projectDir = dir.toPath
        testFun
      }
    }
  }

  override def createAndRunPipeline(
      config: TestPipelineConfiguration,
      sources: Seq[PipelineSourceFile]): PipelineReference = {
    // Create each source file in the temporary directory
    sources.foreach { file =>
      val filePath = Paths.get(file.name)
      val tempFilePath = projectDir.resolve(filePath)

      // Create any necessary parent directories
      val parentDir = tempFilePath.getParent
      if (parentDir != null) {
        Files.createDirectories(parentDir)
      }

      // Create the file with the specified contents
      Files.write(tempFilePath, file.contents.getBytes("UTF-8"))
      logInfo(s"Created file: ${tempFilePath.toAbsolutePath}")
    }

    val specFilePath = writePipelineSpecFile(config.pipelineSpec)
    val cliCommand: Seq[String] = generateCliCommand(config, specFilePath)
    val sourcePath = Paths.get(sparkHome, "python").toAbsolutePath
    val py4jPath = Paths.get(sparkHome, "python", "lib", PythonUtils.PY4J_ZIP_NAME).toAbsolutePath
    val pythonPath = PythonUtils.mergePythonPaths(
      sourcePath.toString,
      py4jPath.toString,
      sys.env.getOrElse("PYTHONPATH", ""))

    val processBuilder = new ProcessBuilder(cliCommand: _*)
    processBuilder.environment().put("PYTHONPATH", pythonPath)
    val process = processBuilder.start()

    PipelineReferenceImpl(process)
  }

  private def generateCliCommand(
      config: TestPipelineConfiguration,
      specFilePath: Path): Seq[String] = {
    val pythonExec =
      sys.env.getOrElse("PYSPARK_DRIVER_PYTHON", sys.env.getOrElse("PYSPARK_PYTHON", "python3"))

    var cliCommand = Seq(
      pythonExec,
      Paths.get(sparkHome, "python", "pyspark", "pipelines", "cli.py").toAbsolutePath.toString,
      if (config.dryRun) "dry-run" else "run",
      "--spec",
      specFilePath.toString)
    if (config.fullRefreshAll) {
      cliCommand :+= "--full-refresh-all"
    }
    if (config.refreshSelection.nonEmpty) {
      cliCommand :+= "--refresh"
      cliCommand :+= config.refreshSelection.mkString(",")
    }
    if (config.fullRefreshSelection.nonEmpty) {
      cliCommand :+= "--full-refresh"
      cliCommand :+= config.fullRefreshSelection.mkString(",")
    }
    cliCommand
  }

  override def awaitPipelineTermination(pipeline: PipelineReference, duration: Duration): Unit = {
    assume(PythonTestDepsChecker.isConnectDepsAvailable)
    assume(PythonTestDepsChecker.isYamlAvailable)
    pipeline match {
      case ref: PipelineReferenceImpl =>
        val process = ref.executionProcess
        process.waitFor(duration.toSeconds, TimeUnit.SECONDS)
        val exitCode = process.exitValue()
        if (exitCode != 0) {
          throw new RuntimeException(s"""Pipeline update process failed with exit code $exitCode.
               |Output: ${new String(process.getInputStream.readAllBytes(), "UTF-8")}
               |Error: ${new String(
                                         process.getErrorStream.readAllBytes(),
                                         "UTF-8")}""".stripMargin)
        } else {
          logInfo("Pipeline update process completed successfully")
          logDebug(s"""Output: ${new String(process.getInputStream.readAllBytes(), "UTF-8")}
               |Error: ${new String(
                       process.getErrorStream.readAllBytes(),
                       "UTF-8")}""".stripMargin)
        }
      case _ => throw new IllegalArgumentException("Invalid UpdateReference type")
    }
  }

  override def stopPipeline(pipeline: PipelineReference): Unit = {
    pipeline match {
      case ref: PipelineReferenceImpl =>
        val process = ref.executionProcess
        if (process.isAlive) {
          process.destroy()
          logInfo("Pipeline update process has been stopped")
        } else {
          logInfo("Pipeline update process was not running")
        }
      case _ => throw new IllegalArgumentException("Invalid UpdateReference type")
    }
  }

  private def writePipelineSpecFile(spec: TestPipelineSpec): Path = {
    val libraries = spec.include
      .map { includePattern =>
        s"""  - glob:
         |      include: "$includePattern"
         |""".stripMargin
      }
      .mkString("\n")

    val pipelineSpec = s"""
      |name: test-pipeline
      |${spec.catalog.map(catalog => s"""catalog: "$catalog"""").getOrElse("")}
      |${spec.database.map(database => s"""database: "$database"""").getOrElse("")}
      |storage: "file://${projectDir.resolve("storage").toAbsolutePath}"
      |configuration:
      |  "spark.remote": "sc://localhost:$serverPort"
      |libraries:
      |$libraries
      |""".stripMargin
    logInfo("""
        |Generated pipeline spec:
        |
        |$pipelineSpec
        |""".stripMargin)
    val specFilePath = projectDir.resolve("pipeline.yaml")
    Files.write(specFilePath, pipelineSpec.getBytes("UTF-8"))
    logDebug(s"Created pipeline spec: ${specFilePath.toAbsolutePath}")
    specFilePath
  }
}
