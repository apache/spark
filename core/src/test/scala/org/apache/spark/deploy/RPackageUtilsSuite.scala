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

import java.io.{PrintStream, OutputStream, File}
import java.net.URI
import java.util.jar.Attributes.Name
import java.util.jar.{JarFile, Manifest}

import org.apache.spark.SparkFunSuite
import org.apache.spark.api.r.RUtils
import org.apache.spark.deploy.SparkSubmitUtils.MavenCoordinate
import org.scalatest.BeforeAndAfterEach

import scala.collection.mutable.ArrayBuffer

class RPackageUtilsSuite extends SparkFunSuite with BeforeAndAfterEach {

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
    override def println(line: String) {
      lineBuffer += line
    }
  }

  def beforeAll() {
    System.setProperty("spark.testing", "true")
  }

  override def beforeEach(): Unit = {
    lineBuffer.clear()
  }

  test("pick which jars to unpack using the manifest") {
    val deps = Seq(dep1, dep2).mkString(",")
    IvyTestUtils.withRepository(main, Some(deps), None, withR = true) { repo =>
      val jars = Seq(main, dep1, dep2).map(c => new JarFile(getJarPath(c, new File(new URI(repo)))))
      assert(RPackageUtils.checkManifestForR(jars(0)), "should have R code")
      assert(!RPackageUtils.checkManifestForR(jars(1)), "should not have R code")
      assert(!RPackageUtils.checkManifestForR(jars(2)), "should not have R code")
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
        getJarPath(c, new File(new URI(repo))) + "dummy"
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
      assert(output.contains(RPackageUtils.RJarDoc))
    }
  }
}
