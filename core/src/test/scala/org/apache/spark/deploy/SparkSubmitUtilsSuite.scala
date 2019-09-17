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
import java.nio.charset.StandardCharsets

import scala.collection.mutable.ArrayBuffer

import com.google.common.io.Files
import org.apache.ivy.core.module.descriptor.MDArtifact
import org.apache.ivy.core.settings.IvySettings
import org.apache.ivy.plugins.resolver.{AbstractResolver, ChainResolver, FileSystemResolver, IBiblioResolver}
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.SparkFunSuite
import org.apache.spark.deploy.SparkSubmitUtils.MavenCoordinate
import org.apache.spark.util.Utils

class SparkSubmitUtilsSuite extends SparkFunSuite with BeforeAndAfterAll {

  private var tempIvyPath: String = _

  private val noOpOutputStream = new OutputStream {
    def write(b: Int) = {}
  }

  /** Simple PrintStream that reads data into a buffer */
  private class BufferPrintStream extends PrintStream(noOpOutputStream) {
    var lineBuffer = ArrayBuffer[String]()
    // scalastyle:off println
    override def println(line: String) {
      lineBuffer += line
    }
    // scalastyle:on println
  }

  override def beforeAll() {
    super.beforeAll()
    // We don't want to write logs during testing
    SparkSubmitUtils.printStream = new BufferPrintStream
    tempIvyPath = Utils.createTempDir(namePrefix = "ivy").getAbsolutePath()
  }

  test("incorrect maven coordinate throws error") {
    val coordinates = Seq("a:b: ", " :a:b", "a: :b", "a:b:", ":a:b", "a::b", "::", "a:b", "a")
    for (coordinate <- coordinates) {
      intercept[IllegalArgumentException] {
        SparkSubmitUtils.extractMavenCoordinates(coordinate)
      }
    }
  }

  test("create repo resolvers") {
    val settings = new IvySettings
    val res1 = SparkSubmitUtils.createRepoResolvers(settings.getDefaultIvyUserDir)
    // should have central and spark-packages by default
    assert(res1.getResolvers.size() === 4)
    assert(res1.getResolvers.get(0).asInstanceOf[IBiblioResolver].getName === "local-m2-cache")
    assert(res1.getResolvers.get(1).asInstanceOf[FileSystemResolver].getName === "local-ivy-cache")
    assert(res1.getResolvers.get(2).asInstanceOf[IBiblioResolver].getName === "central")
    assert(res1.getResolvers.get(3).asInstanceOf[IBiblioResolver].getName === "spark-packages")
  }

  test("create additional resolvers") {
    val repos = "a/1,b/2,c/3"
    val settings = SparkSubmitUtils.buildIvySettings(Option(repos), None)
    val resolver = settings.getDefaultResolver.asInstanceOf[ChainResolver]
    assert(resolver.getResolvers.size() === 4)
    val expected = repos.split(",").map(r => s"$r/")
    resolver.getResolvers.toArray.map(_.asInstanceOf[AbstractResolver]).zipWithIndex.foreach {
      case (r, i) =>
        if (1 < i && i < 3) {
          assert(r.getName === s"repo-$i")
          assert(r.asInstanceOf[IBiblioResolver].getRoot === expected(i - 1))
        }
    }
  }

  test("add dependencies works correctly") {
    val md = SparkSubmitUtils.getModuleDescriptor
    val artifacts = SparkSubmitUtils.extractMavenCoordinates("com.databricks:spark-csv_2.12:0.1," +
      "com.databricks:spark-avro_2.12:0.1")

    SparkSubmitUtils.addDependenciesToIvy(md, artifacts, "default")
    assert(md.getDependencies.length === 2)
  }

  test("excludes works correctly") {
    val md = SparkSubmitUtils.getModuleDescriptor
    val excludes = Seq("a:b", "c:d")
    excludes.foreach { e =>
      md.addExcludeRule(SparkSubmitUtils.createExclusion(e + ":*", new IvySettings, "default"))
    }
    val rules = md.getAllExcludeRules
    assert(rules.length === 2)
    val rule1 = rules(0).getId.getModuleId
    assert(rule1.getOrganisation === "a")
    assert(rule1.getName === "b")
    val rule2 = rules(1).getId.getModuleId
    assert(rule2.getOrganisation === "c")
    assert(rule2.getName === "d")
    intercept[IllegalArgumentException] {
      SparkSubmitUtils.createExclusion("e:f:g:h", new IvySettings, "default")
    }
  }

  test("ivy path works correctly") {
    val md = SparkSubmitUtils.getModuleDescriptor
    val artifacts = for (i <- 0 until 3) yield new MDArtifact(md, s"jar-$i", "jar", "jar")
    var jPaths = SparkSubmitUtils.resolveDependencyPaths(artifacts.toArray, new File(tempIvyPath))
    for (i <- 0 until 3) {
      val index = jPaths.indexOf(tempIvyPath)
      assert(index >= 0)
      jPaths = jPaths.substring(index + tempIvyPath.length)
    }
    val main = MavenCoordinate("my.awesome.lib", "mylib", "0.1")
    IvyTestUtils.withRepository(main, None, None) { repo =>
      // end to end
      val jarPath = SparkSubmitUtils.resolveMavenCoordinates(
        main.toString,
        SparkSubmitUtils.buildIvySettings(Option(repo), Option(tempIvyPath)),
        isTest = true)
      assert(jarPath.indexOf(tempIvyPath) >= 0, "should use non-default ivy path")
    }
  }

  test("search for artifact at local repositories") {
    val main = new MavenCoordinate("my.great.lib", "mylib", "0.1")
    val dep = "my.great.dep:mydep:0.5"
    // Local M2 repository
    IvyTestUtils.withRepository(main, Some(dep), Some(SparkSubmitUtils.m2Path)) { repo =>
      val jarPath = SparkSubmitUtils.resolveMavenCoordinates(
        main.toString,
        SparkSubmitUtils.buildIvySettings(None, None),
        isTest = true)
      assert(jarPath.indexOf("mylib") >= 0, "should find artifact")
      assert(jarPath.indexOf("mydep") >= 0, "should find dependency")
    }
    // Local Ivy Repository
    val settings = new IvySettings
    val ivyLocal = new File(settings.getDefaultIvyUserDir, "local" + File.separator)
    IvyTestUtils.withRepository(main, Some(dep), Some(ivyLocal), useIvyLayout = true) { repo =>
      val jarPath = SparkSubmitUtils.resolveMavenCoordinates(
        main.toString,
        SparkSubmitUtils.buildIvySettings(None, None),
        isTest = true)
      assert(jarPath.indexOf("mylib") >= 0, "should find artifact")
      assert(jarPath.indexOf("mydep") >= 0, "should find dependency")
    }
    // Local ivy repository with modified home
    val dummyIvyLocal = new File(tempIvyPath, "local" + File.separator)
    settings.setDefaultIvyUserDir(new File(tempIvyPath))
    IvyTestUtils.withRepository(main, Some(dep), Some(dummyIvyLocal), useIvyLayout = true,
      ivySettings = settings) { repo =>
      val jarPath = SparkSubmitUtils.resolveMavenCoordinates(
        main.toString,
        SparkSubmitUtils.buildIvySettings(None, Some(tempIvyPath)),
        isTest = true)
      assert(jarPath.indexOf("mylib") >= 0, "should find artifact")
      assert(jarPath.indexOf(tempIvyPath) >= 0, "should be in new ivy path")
      assert(jarPath.indexOf("mydep") >= 0, "should find dependency")
    }
  }

  test("dependency not found throws RuntimeException") {
    intercept[RuntimeException] {
      SparkSubmitUtils.resolveMavenCoordinates(
      "a:b:c",
      SparkSubmitUtils.buildIvySettings(None, None),
      isTest = true)
    }
  }

  test("neglects Spark and Spark's dependencies") {
    val coordinates = SparkSubmitUtils.IVY_DEFAULT_EXCLUDES
      .map(comp => s"org.apache.spark:spark-${comp}2.12:2.4.0")
      .mkString(",") + ",org.apache.spark:spark-core_fake:1.2.0"

    val path = SparkSubmitUtils.resolveMavenCoordinates(
      coordinates,
      SparkSubmitUtils.buildIvySettings(None, None),
      isTest = true)
    assert(path === "", "should return empty path")
    val main = MavenCoordinate("org.apache.spark", "spark-streaming-kafka-assembly_2.12", "1.2.0")
    IvyTestUtils.withRepository(main, None, None) { repo =>
      val files = SparkSubmitUtils.resolveMavenCoordinates(
        coordinates + "," + main.toString,
        SparkSubmitUtils.buildIvySettings(Some(repo), None),
        isTest = true)
      assert(files.indexOf(main.artifactId) >= 0, "Did not return artifact")
    }
  }

  test("exclude dependencies end to end") {
    val main = new MavenCoordinate("my.great.lib", "mylib", "0.1")
    val dep = "my.great.dep:mydep:0.5"
    IvyTestUtils.withRepository(main, Some(dep), None) { repo =>
      val files = SparkSubmitUtils.resolveMavenCoordinates(
        main.toString,
        SparkSubmitUtils.buildIvySettings(Some(repo), None),
        Seq("my.great.dep:mydep"),
        isTest = true)
      assert(files.indexOf(main.artifactId) >= 0, "Did not return artifact")
      assert(files.indexOf("my.great.dep") < 0, "Returned excluded artifact")
    }
  }

  test("load ivy settings file") {
    val main = new MavenCoordinate("my.great.lib", "mylib", "0.1")
    val dep = "my.great.dep:mydep:0.5"
    val dummyIvyLocal = new File(tempIvyPath, "local" + File.separator)
    val settingsText =
      s"""
         |<ivysettings>
         |  <caches defaultCacheDir="$tempIvyPath/cache"/>
         |  <settings defaultResolver="local-ivy-settings-file-test"/>
         |  <resolvers>
         |    <filesystem name="local-ivy-settings-file-test">
         |      <ivy pattern=
         |        "$dummyIvyLocal/[organisation]/[module]/[revision]/[type]s/[artifact].[ext]"/>
         |      <artifact pattern=
         |        "$dummyIvyLocal/[organisation]/[module]/[revision]/[type]s/[artifact].[ext]"/>
         |    </filesystem>
         |  </resolvers>
         |</ivysettings>
         |""".stripMargin

    val settingsFile = new File(tempIvyPath, "ivysettings.xml")
    Files.write(settingsText, settingsFile, StandardCharsets.UTF_8)
    val settings = SparkSubmitUtils.loadIvySettings(settingsFile.toString, None, None)
    settings.setDefaultIvyUserDir(new File(tempIvyPath))  // NOTE - can't set this through file

    val testUtilSettings = new IvySettings
    testUtilSettings.setDefaultIvyUserDir(new File(tempIvyPath))
    IvyTestUtils.withRepository(main, Some(dep), Some(dummyIvyLocal), useIvyLayout = true,
      ivySettings = testUtilSettings) { repo =>
      val jarPath = SparkSubmitUtils.resolveMavenCoordinates(main.toString, settings, isTest = true)
      assert(jarPath.indexOf("mylib") >= 0, "should find artifact")
      assert(jarPath.indexOf(tempIvyPath) >= 0, "should be in new ivy path")
      assert(jarPath.indexOf("mydep") >= 0, "should find dependency")
    }
  }

  test("SPARK-10878: test resolution files cleaned after resolving artifact") {
    val main = new MavenCoordinate("my.great.lib", "mylib", "0.1")

    IvyTestUtils.withRepository(main, None, None) { repo =>
      val ivySettings = SparkSubmitUtils.buildIvySettings(Some(repo), Some(tempIvyPath))
      val jarPath = SparkSubmitUtils.resolveMavenCoordinates(
        main.toString,
        ivySettings,
        isTest = true)
      val r = """.*org.apache.spark-spark-submit-parent-.*""".r
      assert(!ivySettings.getDefaultCache.listFiles.map(_.getName)
        .exists(r.findFirstIn(_).isDefined), "resolution files should be cleaned")
    }
  }
}
