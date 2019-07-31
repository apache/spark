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
package org.apache.spark.deploy.k8s.integrationtest

import java.io.{Closeable, File, PrintWriter}
import java.nio.file.{Files, Path}

import scala.collection.JavaConverters._

import org.apache.commons.io.output.ByteArrayOutputStream

import org.apache.spark.{SPARK_VERSION, SparkException}
import org.apache.spark.internal.Logging

object Utils extends Logging {

  def getExamplesJarName(): String = {
    val scalaVersion = scala.util.Properties.versionNumberString
      .split("\\.")
      .take(2)
      .mkString(".")
    s"spark-examples_$scalaVersion-${SPARK_VERSION}.jar"
  }

  def tryWithResource[R <: Closeable, T](createResource: => R)(f: R => T): T = {
    val resource = createResource
    try f.apply(resource) finally resource.close()
  }

  def executeCommand(cmd: String*)(
      implicit podName: String,
      kubernetesTestComponents: KubernetesTestComponents): String = {
    val out = new ByteArrayOutputStream()
    val watch = kubernetesTestComponents
      .kubernetesClient
      .pods()
      .withName(podName)
      .readingInput(System.in)
      .writingOutput(out)
      .writingError(System.err)
      .withTTY()
      .exec(cmd.toArray: _*)
    // wait to get some result back
    Thread.sleep(1000)
    watch.close()
    out.flush()
    out.toString()
  }

  def createTempFile(contents: String, hostPath: String): String = {
    val filename = try {
      val f = File.createTempFile("tmp", ".txt", new File(hostPath))
      f.deleteOnExit()
      new PrintWriter(f) {
        try {
          write(contents)
        } finally {
          close()
        }
      }
      f.getName
    } catch {
      case e: Exception => e.printStackTrace(); throw e;
    }
    filename
  }

  def getExamplesJarAbsolutePath(sparkHomeDir: Path): String = {
    val jarName = getExamplesJarName()
    val jarPathsFound = Files
      .walk(sparkHomeDir)
      .filter(Files.isRegularFile(_))
      .filter((f: Path) => {f.toFile.getName == jarName})
    // we should not have more than one here under current test build dir
    // we only need one though
    val jarPath = jarPathsFound
      .iterator()
      .asScala
      .map(_.toAbsolutePath.toString)
      .toArray
      .headOption
    jarPath match {
      case Some(jar) => jar
      case _ => throw new SparkException(s"No valid $jarName file was found " +
        s"under spark home test dir ${sparkHomeDir.toAbsolutePath}!")
    }
  }
}
