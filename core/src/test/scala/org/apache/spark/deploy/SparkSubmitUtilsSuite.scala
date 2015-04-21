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

import scala.collection.mutable.ArrayBuffer

import org.scalatest.{BeforeAndAfterAll, FunSuite}

import org.apache.ivy.core.module.descriptor.MDArtifact
import org.apache.ivy.plugins.resolver.IBiblioResolver

class SparkSubmitUtilsSuite extends FunSuite with BeforeAndAfterAll {

  private val noOpOutputStream = new OutputStream {
    def write(b: Int) = {}
  }

  /** Simple PrintStream that reads data into a buffer */
  private class BufferPrintStream extends PrintStream(noOpOutputStream) {
    var lineBuffer = ArrayBuffer[String]()
    override def println(line: String) {
      lineBuffer += line
    }
  }

  override def beforeAll() {
    super.beforeAll()
    // We don't want to write logs during testing
    SparkSubmitUtils.printStream = new BufferPrintStream
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
    val resolver1 = SparkSubmitUtils.createRepoResolvers(None)
    // should have central and spark-packages by default
    assert(resolver1.getResolvers.size() === 2)
    assert(resolver1.getResolvers.get(0).asInstanceOf[IBiblioResolver].getName === "central")
    assert(resolver1.getResolvers.get(1).asInstanceOf[IBiblioResolver].getName === "spark-packages")

    val repos = "a/1,b/2,c/3"
    val resolver2 = SparkSubmitUtils.createRepoResolvers(Option(repos))
    assert(resolver2.getResolvers.size() === 5)
    val expected = repos.split(",").map(r => s"$r/")
    resolver2.getResolvers.toArray.zipWithIndex.foreach { case (resolver: IBiblioResolver, i) =>
      if (i == 0) {
        assert(resolver.getName === "central")
      } else if (i == 1) {
        assert(resolver.getName === "spark-packages")
      } else {
        assert(resolver.getName === s"repo-${i - 1}")
        assert(resolver.getRoot === expected(i - 2))
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

  test("ivy path works correctly") {
    val ivyPath = "dummy/ivy"
    val md = SparkSubmitUtils.getModuleDescriptor
    val artifacts = for (i <- 0 until 3) yield new MDArtifact(md, s"jar-$i", "jar", "jar")
    var jPaths = SparkSubmitUtils.resolveDependencyPaths(artifacts.toArray, new File(ivyPath))
    for (i <- 0 until 3) {
      val index = jPaths.indexOf(ivyPath)
      assert(index >= 0)
      jPaths = jPaths.substring(index + ivyPath.length)
    }
    // end to end
    val jarPath = SparkSubmitUtils.resolveMavenCoordinates(
      "com.databricks:spark-csv_2.10:0.1", None, Option(ivyPath), true)
    assert(jarPath.indexOf(ivyPath) >= 0, "should use non-default ivy path")
  }

  test("search for artifact at other repositories") {
    val path = SparkSubmitUtils.resolveMavenCoordinates("com.agimatec:agimatec-validation:0.9.3",
      Option("https://oss.sonatype.org/content/repositories/agimatec/"), None, true)
    assert(path.indexOf("agimatec-validation") >= 0, "should find package. If it doesn't, check" +
      "if package still exists. If it has been removed, replace the example in this test.")
  }

  test("dependency not found throws RuntimeException") {
    intercept[RuntimeException] {
      SparkSubmitUtils.resolveMavenCoordinates("a:b:c", None, None, true)
    }
  }

  test("neglects Spark and Spark's dependencies") {
    val components = Seq("bagel_", "catalyst_", "core_", "graphx_", "hive_", "mllib_", "repl_",
      "sql_", "streaming_", "yarn_", "network-common_", "network-shuffle_", "network-yarn_")

    val coordinates =
      components.map(comp => s"org.apache.spark:spark-${comp}2.10:1.2.0").mkString(",") +
      ",org.apache.spark:spark-core_fake:1.2.0"

    val path = SparkSubmitUtils.resolveMavenCoordinates(coordinates, None, None, true)
    assert(path === "", "should return empty path")
    // Should not exclude the following dependency. Will throw an error, because it doesn't exist,
    // but the fact that it is checking means that it wasn't excluded.
    intercept[RuntimeException] {
      SparkSubmitUtils.resolveMavenCoordinates(coordinates +
        ",org.apache.spark:spark-streaming-kafka-assembly_2.10:1.2.0", None, None, true)
    }
  }
}
