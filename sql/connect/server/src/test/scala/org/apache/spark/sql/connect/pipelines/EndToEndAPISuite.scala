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

import org.apache.spark.api.python.PythonUtils
import org.apache.spark.sql.connect.SparkConnectServerTest
import org.apache.spark.sql.pipelines.utils.PipelineTest

trait SDPUpdateReference extends UpdateReference {
  val executionProcess: Process
}

/**
 * End-to-end test suite for the Spark Data Pipelines API using the CLI.
 *
 * This suite creates a temporary directory for each test case, writes the necessary pipeline
 * specification and source files, and invokes the CLI as a separate process.
 */
class EndToEndAPISuite extends PipelineTest with APITest with SparkConnectServerTest {

  // Directory where the pipeline files will be created
  private var projectDir: Path = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    projectDir = Files.createTempDirectory("spark-pipeline-test-")
    logInfo(s"Created temporary directory: ${projectDir.toAbsolutePath}")
  }

  override def createAndRunPipeline(
      config: TestPipelineConfiguration,
      sources: Seq[File]): (PipelineReference, SDPUpdateReference) = {
    // Create each source file in the temporary directory
    sources.foreach { file =>
      val filePath = Paths.get(file.name)
      val fileName = filePath.getFileName.toString
      val tempFilePath = projectDir.resolve(fileName)

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

    (
      new PipelineReference {},
      new SDPUpdateReference {
        override val executionProcess: Process = process
      })
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

  override def awaitPipelineUpdateTermination(update: UpdateReference): Unit = {
    update match {
      case ref: SDPUpdateReference =>
        val process = ref.executionProcess
        process.waitFor(60, TimeUnit.SECONDS)
        val exitCode = process.exitValue()
        if (exitCode != 0) {
          throw new RuntimeException(
            s"Pipeline update process failed with exit code $exitCode. " +
              s"Output: ${new String(process.getInputStream.readAllBytes(), "UTF-8")}\n" +
              s"Error: ${new String(process.getErrorStream.readAllBytes(), "UTF-8")}")
        } else {
          logInfo("Pipeline update process completed successfully")
          logDebug(
            s"Output: ${new String(process.getInputStream.readAllBytes(), "UTF-8")}\n" +
              s"Error: ${new String(process.getErrorStream.readAllBytes(), "UTF-8")}")
        }
      case _ => throw new IllegalArgumentException("Invalid UpdateReference type")
    }
  }

  override def stopPipelineUpdate(update: UpdateReference): Unit = {
    update match {
      case ref: SDPUpdateReference =>
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

  override def afterEach(): Unit = {
    super.afterEach()
    cleanupTempDir()
  }

  private def writePipelineSpecFile(spec: TestPipelineSpec): Path = {
    val definitions = spec.include
      .map { includePattern =>
        s"""  - glob:
         |      include: "$includePattern"
         |""".stripMargin
      }
      .mkString("\n")

    val pipelineSpec = s"""
      |name: test-pipeline
      |catalog: ${spec.catalog}
      |database: ${spec.database}
      |configuration:
      |  "spark.remote": "sc://localhost:$serverPort"
      |definitions:
      |$definitions
      |""".stripMargin
    val specFilePath = projectDir.resolve("pipeline.yaml")
    Files.write(specFilePath, pipelineSpec.getBytes("UTF-8"))
    logDebug(s"Created pipeline spec: ${specFilePath.toAbsolutePath}")
    specFilePath
  }

  private def cleanupTempDir(): Unit = {
    try {
      if (Files.exists(projectDir)) {
        Files
          .walk(projectDir)
          .sorted(java.util.Comparator.reverseOrder())
          .forEach(path => Files.deleteIfExists(path))
        logInfo(s"Cleaned up temporary directory: ${projectDir.toAbsolutePath}")
      }
    } catch {
      case e: Exception =>
        logInfo(s"Warning: Failed to clean up temporary directory: ${e.getMessage}")
    } finally {
      projectDir = null
    }
  }
}
