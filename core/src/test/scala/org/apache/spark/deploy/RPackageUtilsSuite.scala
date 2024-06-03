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

package org.apache.spark.deploy

import java.io.{File, OutputStream, PrintStream}
import java.net.URI
import java.util.jar.{JarFile, Manifest}
import java.util.jar.Attributes.Name
import java.util.zip.ZipFile

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import org.apache.commons.io.FileUtils
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.SparkFunSuite
import org.apache.spark.api.r.RUtils
import org.apache.spark.util.{IvyTestUtils, ResetSystemProperties, Utils}
import org.apache.spark.util.MavenUtils.MavenCoordinate

class RPackageUtilsSuite
  extends SparkFunSuite
  with BeforeAndAfterEach
  with ResetSystemProperties {

  private val main = MavenCoordinate("a", "b", "c")
  private val dep1 = MavenCoordinate("a", "dep1", "c")
  private val dep2 = MavenCoordinate("a", "dep2", "d")

  private def getJarPath(coord: MavenCoordinate, repo: File): File = {
    new File(IvyTestUtils.pathFromCoordinate(coord, repo, "jar", useIvyLayout = false),
      IvyTestUtils.artifactName(coord, useIvyLayout = false, ".jar"))
  }

  private val lineBuffer = ArrayBuffer[String]()

  private val noOpOutputStream = new OutputStream {
    def write(b: Int) = {}
  }

  /** Simple PrintStream that reads data into a buffer */
  private class BufferPrintStream extends PrintStream(noOpOutputStream) {
    // scalastyle:off println
    override def println(line: String): Unit = {
    // scalastyle:on println
      lineBuffer += line
    }
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    lineBuffer.clear()
  }

  test("pick which jars to unpack using the manifest") {
    val deps = Seq(dep1, dep2).mkString(",")
    IvyTestUtils.withRepository(main, Some(deps), None, withR = true) { repo =>
      val jars = Seq(main, dep1, dep2).map(c => new JarFile(getJarPath(c, new File(new URI(repo)))))
      Utils.tryWithSafeFinally {
        assert(RPackageUtils.checkManifestForR(jars(0)), "should have R code")
        assert(!RPackageUtils.checkManifestForR(jars(1)), "should not have R code")
        assert(!RPackageUtils.checkManifestForR(jars(2)), "should not have R code")
      } {
        jars.foreach(_.close())
      }
    }
  }

  test("build an R package from a jar end to end") {
    assume(RUtils.isRInstalled, "R isn't installed on this machine.")
    val deps = Seq(dep1, dep2).mkString(",")
    IvyTestUtils.withRepository(main, Some(deps), None, withR = true) { repo =>
      val jars = Seq(main, dep1, dep2).map { c =>
        getJarPath(c, new File(new URI(repo)))
      }.mkString(",")
      RPackageUtils.checkAndBuildRPackage(jars, new BufferPrintStream, verbose = true)
      val firstJar = jars.substring(0, jars.indexOf(","))
      val output = lineBuffer.mkString("\n")
      assert(output.contains("Building R package"))
      assert(output.contains("Extracting"))
      assert(output.contains(s"$firstJar contains R source code. Now installing package."))
      assert(output.contains("doesn't contain R source code, skipping..."))
    }
  }

  test("jars that don't exist are skipped and print warning") {
    assume(RUtils.isRInstalled, "R isn't installed on this machine.")
    val deps = Seq(dep1, dep2).mkString(",")
    IvyTestUtils.withRepository(main, Some(deps), None, withR = true) { repo =>
      val jars = Seq(main, dep1, dep2).map { c =>
        getJarPath(c, new File(new URI(repo))).toString + "dummy"
      }.mkString(",")
      RPackageUtils.checkAndBuildRPackage(jars, new BufferPrintStream, verbose = true)
      val individualJars = jars.split(",")
      val output = lineBuffer.mkString("\n")
      individualJars.foreach { jarFile =>
        assert(output.contains(s"$jarFile"))
      }
    }
  }

  test("faulty R package shows documentation") {
    assume(RUtils.isRInstalled, "R isn't installed on this machine.")
    IvyTestUtils.withRepository(main, None, None) { repo =>
      val manifest = new Manifest
      val attr = manifest.getMainAttributes
      attr.put(Name.MANIFEST_VERSION, "1.0")
      attr.put(new Name("Spark-HasRPackage"), "true")
      val jar = IvyTestUtils.packJar(new File(new URI(repo)), dep1, Nil,
        useIvyLayout = false, withR = false, Some(manifest))
      RPackageUtils.checkAndBuildRPackage(jar.getAbsolutePath, new BufferPrintStream,
        verbose = true)
      val output = lineBuffer.mkString("\n")
      assert(output.contains(RPackageUtils.RJarDoc.message))
    }
  }

  test("jars without manifest return false") {
    IvyTestUtils.withRepository(main, None, None) { repo =>
      val jar = IvyTestUtils.packJar(new File(new URI(repo)), dep1, Nil,
        useIvyLayout = false, withR = false, None)
      Utils.tryWithResource(new JarFile(jar)) { jarFile =>
        assert(jarFile.getManifest == null, "jar file should have null manifest")
        assert(!RPackageUtils.checkManifestForR(jarFile), "null manifest should return false")
      }
    }
  }

  test("SparkR zipping works properly") {
    val tempDir = Utils.createTempDir()
    Utils.tryWithSafeFinally {
      IvyTestUtils.writeFile(tempDir, "test.R", "abc")
      val fakeSparkRDir = new File(tempDir, "SparkR")
      assert(fakeSparkRDir.mkdirs())
      IvyTestUtils.writeFile(fakeSparkRDir, "abc.R", "abc")
      IvyTestUtils.writeFile(fakeSparkRDir, "DESCRIPTION", "abc")
      IvyTestUtils.writeFile(tempDir, "package.zip", "abc") // fake zip file :)
      val fakePackageDir = new File(tempDir, "packageTest")
      assert(fakePackageDir.mkdirs())
      IvyTestUtils.writeFile(fakePackageDir, "def.R", "abc")
      IvyTestUtils.writeFile(fakePackageDir, "DESCRIPTION", "abc")
      val finalZip = RPackageUtils.zipRLibraries(tempDir, "sparkr.zip")
      assert(finalZip.exists())
      val zipFile = new ZipFile(finalZip)
      Utils.tryWithSafeFinally {
        val entries = zipFile.entries().asScala.map(_.getName).toSeq
        assert(entries.contains("/test.R"))
        assert(entries.contains("/SparkR/abc.R"))
        assert(entries.contains("/SparkR/DESCRIPTION"))
        assert(!entries.contains("/package.zip"))
        assert(entries.contains("/packageTest/def.R"))
        assert(entries.contains("/packageTest/DESCRIPTION"))
      } {
        zipFile.close()
      }
    } {
      FileUtils.deleteDirectory(tempDir)
    }
  }
}
