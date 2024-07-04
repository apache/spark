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

package org.apache.spark

import java.io.{File, FileInputStream, FileOutputStream}
import java.util.zip.{ZipEntry, ZipOutputStream}

import org.apache.commons.io.IOUtils


class JobArtifactSetSuite extends SparkFunSuite with LocalSparkContext {

  private def createZipFile(inFile: String, outFile: String): Unit = {
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

  test("JobArtifactSet uses resources from SparkContext") {
    withTempDir { dir =>
      val jarPath = File.createTempFile("testJar", ".jar", dir).getAbsolutePath
      val filePath = File.createTempFile("testFile", ".txt", dir).getAbsolutePath
      val fileToZip = File.createTempFile("testFile", "", dir).getAbsolutePath
      val archivePath = s"$fileToZip.zip"
      createZipFile(fileToZip, archivePath)

      val conf = new SparkConf()
        .setAppName("test")
        .setMaster("local")
        .set("spark.repl.class.uri", "dummyUri")
      sc = new SparkContext(conf)

      sc.addJar(jarPath)
      sc.addFile(filePath)
      sc.addArchive(archivePath)

      val artifacts = JobArtifactSet.getActiveOrDefault(sc)
      assert(artifacts.archives == sc.allAddedArchives)
      assert(artifacts.files == sc.allAddedFiles)
      assert(artifacts.jars == sc.allAddedJars)
      assert(artifacts.state.isEmpty)
    }
  }

  test("The active JobArtifactSet is fetched if set") {
    withTempDir { dir =>
      val conf = new SparkConf()
        .setAppName("test")
        .setMaster("local")
        .set("spark.repl.class.uri", "dummyUri")
      sc = new SparkContext(conf)

      val artifactState1 = JobArtifactState("123", Some("abc"))
      val artifactState2 = JobArtifactState("789", Some("hjk"))

      JobArtifactSet.withActiveJobArtifactState(artifactState1) {
        JobArtifactSet.withActiveJobArtifactState(artifactState2) {
          assert(JobArtifactSet.getActiveOrDefault(sc).state.get == artifactState2)
          assert(JobArtifactSet.getActiveOrDefault(sc).state.get.replClassDirUri.get == "hjk")
        }
        assert(JobArtifactSet.getActiveOrDefault(sc).state.get == artifactState1)
        assert(JobArtifactSet.getActiveOrDefault(sc).state.get.replClassDirUri.get == "abc")
      }

      assert(JobArtifactSet.getActiveOrDefault(sc).state.isEmpty)
    }
  }

  test("SPARK-44476: JobArtifactState is not populated with all artifacts if none are " +
    "explicitly added to it.") {
    withTempDir { dir =>
      val conf = new SparkConf()
        .setAppName("test")
        .setMaster("local")
        .set("spark.repl.class.uri", "dummyUri")
      sc = new SparkContext(conf)

      val jarPath = File.createTempFile("testJar", ".jar", dir).getAbsolutePath
      val filePath = File.createTempFile("testFile", ".txt", dir).getAbsolutePath
      val fileToZip = File.createTempFile("testFile", "", dir).getAbsolutePath
      val archivePath = s"$fileToZip.zip"
      createZipFile(fileToZip, archivePath)

      val otherJobArtifactState = JobArtifactState("other", Some("state"))

      JobArtifactSet.withActiveJobArtifactState(otherJobArtifactState) {
        sc.addJar(jarPath)
        sc.addFile(filePath)
        sc.addArchive(archivePath)
      }

      val artifactState = JobArtifactState("abc", Some("xyz"))
      JobArtifactSet.withActiveJobArtifactState(artifactState) {
        val jobArtifactSet = JobArtifactSet.getActiveOrDefault(sc)

        // Artifacts from the other state must be not visible to this state.
        assert(jobArtifactSet.state.contains(artifactState))
        assert(jobArtifactSet.jars.isEmpty)
        assert(jobArtifactSet.files.isEmpty)
        assert(jobArtifactSet.archives.isEmpty)
      }
    }
  }
}
