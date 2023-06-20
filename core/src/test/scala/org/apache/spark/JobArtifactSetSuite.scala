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

import java.io.File

class JobArtifactSetSuite extends SparkFunSuite with LocalSparkContext {
  test("JobArtifactSet uses resources from SparkContext") {
    withTempDir { dir =>
      val jarPath = File.createTempFile("testJar", ".jar", dir).getAbsolutePath
      val filePath = File.createTempFile("testFile", ".txt", dir).getAbsolutePath
      val archivePath = File.createTempFile("testZip", ".zip", dir).getAbsolutePath

      val conf = new SparkConf()
        .setAppName("test")
        .setMaster("local")
        .set("spark.repl.class.uri", "dummyUri")
      sc = new SparkContext(conf)

      sc.addJar(jarPath)
      sc.addFile(filePath)
      sc.addJar(archivePath)

      val artifacts = JobArtifactSet.getActiveOrDefault(sc)
      assert(artifacts.archives == sc.addedArchives)
      assert(artifacts.files == sc.addedFiles)
      assert(artifacts.jars == sc.addedJars)
      assert(artifacts.replClassDirUri.contains("dummyUri"))
    }
  }

  test("The active JobArtifactSet is fetched if set") {
    withTempDir { dir =>
      val jarPath = File.createTempFile("testJar", ".jar", dir).getAbsolutePath
      val filePath = File.createTempFile("testFile", ".txt", dir).getAbsolutePath
      val archivePath = File.createTempFile("testZip", ".zip", dir).getAbsolutePath

      val conf = new SparkConf()
        .setAppName("test")
        .setMaster("local")
        .set("spark.repl.class.uri", "dummyUri")
      sc = new SparkContext(conf)

      sc.addJar(jarPath)
      sc.addFile(filePath)
      sc.addJar(archivePath)

      val artifactSet1 = new JobArtifactSet(
        Some("123"),
        Some("abc"),
        Map("a" -> 1),
        Map("b" -> 2),
        Map("c" -> 3)
      )

      val artifactSet2 = new JobArtifactSet(
        Some("789"),
        Some("hjk"),
        Map("x" -> 7),
        Map("y" -> 8),
        Map("z" -> 9)
      )

      JobArtifactSet.withActive(artifactSet1) {
        JobArtifactSet.withActive(artifactSet2) {
          assert(JobArtifactSet.getActiveOrDefault(sc) == artifactSet2)
        }
        assert(JobArtifactSet.getActiveOrDefault(sc) == artifactSet1)
      }
    }
  }
}
