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

import java.io.{File, PrintStream, OutputStream}

import scala.collection.mutable.ArrayBuffer
import org.scalatest.BeforeAndAfterAll

import org.apache.ivy.core.module.descriptor.MDArtifact
import org.apache.ivy.core.settings.IvySettings
import org.apache.ivy.plugins.resolver.{AbstractResolver, FileSystemResolver, IBiblioResolver}

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
    val res1 = SparkSubmitUtils.createRepoResolvers(None, settings)
    // should have central and spark-packages by default
    assert(res1.getResolvers.size() === 4)
    assert(res1.getResolvers.get(0).asInstanceOf[IBiblioResolver].getName === "local-m2-cache")
    assert(res1.getResolvers.get(1).asInstanceOf[FileSystemResolver].getName === "local-ivy-cache")
    assert(res1.getResolvers.get(2).asInstanceOf[IBiblioResolver].getName === "central")
    assert(res1.getResolvers.get(3).asInstanceOf[IBiblioResolver].getName === "spark-packages")

    val repos = "a/1,b/2,c/3"
    val resolver2 = SparkSubmitUtils.createRepoResolvers(Option(repos), settings)
    assert(resolver2.getResolvers.size() === 7)
    val expected = repos.split(",").map(r => s"$r/")
    resolver2.getResolvers.toArray.zipWithIndex.foreach { case (resolver: AbstractResolver, i) =>
      if (i < 3) {
        assert(resolver.getName === s"repo-${i + 1}")
        assert(resolver.asInstanceOf[IBiblioResolver].getRoot === expected(i))
      }
    }
  }

  test("add dependencies works correctly") {
    val md = SparkSubmitUtils.getModuleDescriptor
    val artifacts = SparkSubmitUtils.extractMavenCoordinates("com.databricks:spark-csv_2.10:0.1," +
      "com.databricks:spark-avro_2.10:0.1")

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
      val jarPath = SparkSubmitUtils.resolveMavenCoordinates(main.toString, Option(repo),
        Option(tempIvyPath), isTest = true)
      assert(jarPath.indexOf(tempIvyPath) >= 0, "should use non-default ivy path")
    }
  }

  test("search for artifact at local repositories") {
    val main = new MavenCoordinate("my.great.lib", "mylib", "0.1")
    val dep = "my.great.dep:mydep:0.5"
    // Local M2 repository
    IvyTestUtils.withRepository(main, Some(dep), Some(SparkSubmitUtils.m2Path)) { repo =>
      val jarPath = SparkSubmitUtils.resolveMavenCoordinates(main.toString, None, None,
        isTest = true)
      assert(jarPath.indexOf("mylib") >= 0, "should find artifact")
      assert(jarPath.indexOf("mydep") >= 0, "should find dependency")
    }
    // Local Ivy Repository
    val settings = new IvySettings
    val ivyLocal = new File(settings.getDefaultIvyUserDir, "local" + File.separator)
    IvyTestUtils.withRepository(main, Some(dep), Some(ivyLocal), useIvyLayout = true) { repo =>
      val jarPath = SparkSubmitUtils.resolveMavenCoordinates(main.toString, None, None,
        isTest = true)
      assert(jarPath.indexOf("mylib") >= 0, "should find artifact")
      assert(jarPath.indexOf("mydep") >= 0, "should find dependency")
    }
    // Local ivy repository with modified home
    val dummyIvyLocal = new File(tempIvyPath, "local" + File.separator)
    settings.setDefaultIvyUserDir(new File(tempIvyPath))
    IvyTestUtils.withRepository(main, Some(dep), Some(dummyIvyLocal), useIvyLayout = true,
      ivySettings = settings) { repo =>
      val jarPath = SparkSubmitUtils.resolveMavenCoordinates(main.toString, None,
        Some(tempIvyPath), isTest = true)
      assert(jarPath.indexOf("mylib") >= 0, "should find artifact")
      assert(jarPath.indexOf(tempIvyPath) >= 0, "should be in new ivy path")
      assert(jarPath.indexOf("mydep") >= 0, "should find dependency")
    }
  }

  test("dependency not found throws RuntimeException") {
    intercept[RuntimeException] {
      SparkSubmitUtils.resolveMavenCoordinates("a:b:c", None, None, isTest = true)
    }
  }

  test("neglects Spark and Spark's dependencies") {
    val components = Seq("bagel_", "catalyst_", "core_", "graphx_", "hive_", "mllib_", "repl_",
      "sql_", "streaming_", "yarn_", "network-common_", "network-shuffle_", "network-yarn_")

    val coordinates =
      components.map(comp => s"org.apache.spark:spark-${comp}2.10:1.2.0").mkString(",") +
      ",org.apache.spark:spark-core_fake:1.2.0"

    val path = SparkSubmitUtils.resolveMavenCoordinates(coordinates, None, None, isTest = true)
    assert(path === "", "should return empty path")
    val main = MavenCoordinate("org.apache.spark", "spark-streaming-kafka-assembly_2.10", "1.2.0")
    IvyTestUtils.withRepository(main, None, None) { repo =>
      val files = SparkSubmitUtils.resolveMavenCoordinates(coordinates + "," + main.toString,
        Some(repo), None, isTest = true)
      assert(files.indexOf(main.artifactId) >= 0, "Did not return artifact")
    }
  }

  test("exclude dependencies end to end") {
    val main = new MavenCoordinate("my.great.lib", "mylib", "0.1")
    val dep = "my.great.dep:mydep:0.5"
    IvyTestUtils.withRepository(main, Some(dep), None) { repo =>
      val files = SparkSubmitUtils.resolveMavenCoordinates(main.toString,
        Some(repo), None, Seq("my.great.dep:mydep"), isTest = true)
      assert(files.indexOf(main.artifactId) >= 0, "Did not return artifact")
      assert(files.indexOf("my.great.dep") < 0, "Returned excluded artifact")
    }
  }
}
