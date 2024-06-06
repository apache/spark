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

import java.io.{Closeable, File, FileInputStream, FileOutputStream, PrintWriter}
import java.nio.file.{Files, Path}
import java.util.concurrent.CountDownLatch
import java.util.zip.{ZipEntry, ZipOutputStream}

import scala.jdk.CollectionConverters._

import io.fabric8.kubernetes.client.dsl.ExecListener
import io.fabric8.kubernetes.client.dsl.ExecListener.Response
import org.apache.commons.compress.archivers.tar.{TarArchiveEntry, TarArchiveOutputStream}
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream
import org.apache.commons.compress.utils.IOUtils
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
    val pod = kubernetesTestComponents
      .kubernetesClient
      .pods()
      .inNamespace(kubernetesTestComponents.namespace)
      .withName(podName)
    // Avoid timing issues by looking for open/close
    class ReadyListener extends ExecListener {
      val openLatch: CountDownLatch = new CountDownLatch(1)
      val closeLatch: CountDownLatch = new CountDownLatch(1)

      override def onOpen(): Unit = {
        openLatch.countDown()
      }

      override def onClose(a: Int, b: String): Unit = {
        closeLatch.countDown()
      }

      override def onFailure(e: Throwable, r: Response): Unit = {
      }

      def waitForInputStreamToConnect(): Unit = {
        openLatch.await()
      }

      def waitForClose(): Unit = {
        closeLatch.await()
      }
    }
    val listener = new ReadyListener()
    val watch = pod
      .readingInput(System.in)
      .writingOutput(out)
      .writingError(System.err)
      .withTTY()
      .usingListener(listener)
      .exec(cmd.toArray: _*)
    // under load sometimes the stdout isn't connected by the time we try to read from it.
    listener.waitForInputStreamToConnect()
    listener.waitForClose()
    watch.close()
    out.flush()
    val result = out.toString()
    result
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

  def getTestFileAbsolutePath(fileName: String, sparkHomeDir: Path): String = {
    val filePathsFound = Files
      .walk(sparkHomeDir)
      .filter(Files.isRegularFile(_))
      .filter((f: Path) => {f.toFile.getName == fileName})
    // we should not have more than one here under current test build dir
    // we only need one though
    val filePath = filePathsFound
      .iterator()
      .asScala
      .map(_.toAbsolutePath.toString)
      .toArray
      .headOption
    filePath match {
      case Some(file) => file
      case _ => throw new SparkException(s"No valid $fileName file was found " +
        s"under spark home test dir ${sparkHomeDir.toAbsolutePath}!")
    }
  }

  def createZipFile(inFile: String, outFile: String): Unit = {
    val fileToZip = new File(inFile)
    val fis = new FileInputStream(fileToZip)
    val fos = new FileOutputStream(outFile)
    val zipOut = new ZipOutputStream(fos)
    val zipEntry = new ZipEntry(fileToZip.getName)
    zipOut.putNextEntry(zipEntry)
    IOUtils.copy(fis, zipOut)
    IOUtils.closeQuietly(fis)
    IOUtils.closeQuietly(zipOut)
  }

  def createTarGzFile(inFile: String, outFile: String): Unit = {
    val oFile = new File(outFile)
    val fileToTarGz = new File(inFile)
    Utils.tryWithResource(
      new FileInputStream(fileToTarGz)
    ) { fis =>
      Utils.tryWithResource(
        new TarArchiveOutputStream(
          new GzipCompressorOutputStream(
            new FileOutputStream(oFile)))
      ) { tOut =>
        val tarEntry = new TarArchiveEntry(fileToTarGz, fileToTarGz.getName)
        // Each entry does not keep the file permission from the input file.
        // Setting permissions in the input file do not work. Just simply set
        // to 777.
        tarEntry.setMode(0x81ff)
        tOut.putArchiveEntry(tarEntry)
        IOUtils.copy(fis, tOut)
        tOut.closeArchiveEntry()
        tOut.finish()
      }
    }
    oFile.deleteOnExit()
  }
}
