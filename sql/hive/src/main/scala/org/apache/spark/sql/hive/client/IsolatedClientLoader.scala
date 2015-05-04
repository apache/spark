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

package org.apache.spark.sql.hive.client

import java.io.File
import java.net.URLClassLoader
import java.util

import scala.language.reflectiveCalls
import scala.util.Try

import org.apache.commons.io.{FileUtils, IOUtils}

import org.apache.spark.Logging
import org.apache.spark.deploy.SparkSubmitUtils

import org.apache.spark.sql.catalyst.util.quietly

/** Factory for `IsolatedClientLoader` with specific versions of hive. */
object IsolatedClientLoader {
  /**
   * Creates isolated Hive client loaders by downloading the requested version from maven.
   */
  def forVersion(
      version: String,
      config: Map[String, String] = Map.empty): IsolatedClientLoader = synchronized {
    val resolvedVersion = hiveVersion(version)
    val files = resolvedVersions.getOrElseUpdate(resolvedVersion, downloadVersion(resolvedVersion))
    new IsolatedClientLoader(hiveVersion(version), files, config)
  }

  def hiveVersion(version: String): HiveVersion = version match {
    case "12" | "0.12" | "0.12.0" => hive.v12
    case "13" | "0.13" | "0.13.0" | "0.13.1" => hive.v13
  }

  private def downloadVersion(version: HiveVersion): Seq[File] = {
    val hiveArtifacts =
      (Seq("hive-metastore", "hive-exec", "hive-common", "hive-serde") ++
        (if (version.hasBuiltinsJar) "hive-builtins" :: Nil else Nil))
        .map(a => s"org.apache.hive:$a:${version.fullVersion}") :+
        "com.google.guava:guava:14.0.1" :+
        "org.apache.hadoop:hadoop-client:2.4.0" :+
        "mysql:mysql-connector-java:5.1.12"

    val classpath = quietly {
      SparkSubmitUtils.resolveMavenCoordinates(
        hiveArtifacts.mkString(","),
        Some("http://www.datanucleus.org/downloads/maven2"),
        None)
    }
    val allFiles = classpath.split(",").map(new File(_)).toSet

    // TODO: Remove copy logic.
    val tempDir = File.createTempFile("hive", "v" + version.toString)
    tempDir.delete()
    tempDir.mkdir()

    allFiles.foreach(f => FileUtils.copyFileToDirectory(f, tempDir))
    tempDir.listFiles()
  }

  private def resolvedVersions = new scala.collection.mutable.HashMap[HiveVersion, Seq[File]]
}

/**
 * Creates a Hive `ClientInterface` using a classloader that works according to the following rules:
 *  - Shared classes: Java, Scala, logging, and Spark classes are delegated to `baseClassLoader`
 *    allowing the results of calls to the `ClientInterface` to be visible externally.
 *  - Hive classes: new instances are loaded from `execJars`.  These classes are not
 *    accessible externally due to their custom loading.
 *  - ClientWrapper: a new copy is created for each instance of `IsolatedClassLoader`.
 *    This new instance is able to see a specific version of hive without using reflection. Since
 *    this is a unique instance, it is not visible externally other than as a generic
 *    `ClientInterface`, unless `isolationOn` is set to `false`.
 *
 * @param version The version of hive on the classpath.  used to pick specific function signatures
 *                that are not compatibile accross versions.
 * @param execJars A collection of jar files that must include hive and hadoop.
 * @param config   A set of options that will be added to the HiveConf of the constructed client.
 * @param isolationOn When true, custom versions of barrier classes will be constructed.  Must be
 *                    true unless loading the version of hive that is on Sparks classloader.
 * @param rootClassLoader The system root classloader.  Must not know about hive classes.
 * @param baseClassLoader The spark classloader that is used to load shared classes.
 *
 */
class IsolatedClientLoader(
    val version: HiveVersion,
    val execJars: Seq[File] = Seq.empty,
    val config: Map[String, String] = Map.empty,
    val isolationOn: Boolean = true,
    val rootClassLoader: ClassLoader = ClassLoader.getSystemClassLoader.getParent.getParent,
    val baseClassLoader: ClassLoader = Thread.currentThread().getContextClassLoader)
  extends Logging {

  // Check to make sure that the root classloader does not know about Hive.
  assert(Try(baseClassLoader.loadClass("org.apache.hive.HiveConf")).isFailure)

  /** All jars used by the hive specific classloader. */
  protected def allJars = execJars.map(_.toURI.toURL).toArray

  protected def isSharedClass(name: String): Boolean =
    name.contains("slf4j") ||
    name.contains("log4j") ||
    name.startsWith("org.apache.spark.") ||
    name.startsWith("scala.") ||
    name.startsWith("com.google") ||
    name.startsWith("java.lang.") ||
    name.startsWith("java.net")

  /** True if `name` refers to a spark class that must see specific version of Hive. */
  protected def isBarrierClass(name: String): Boolean =
    name.startsWith("org.apache.spark.sql.hive.execution.PairSerDe") ||
    name.startsWith(classOf[ClientWrapper].getName) ||
    name.startsWith(classOf[ReflectionMagic].getName)

  protected def classToPath(name: String): String =
    name.replaceAll("\\.", "/") + ".class"

  /** The classloader that is used to load an isolated version of Hive. */
  protected val classLoader: ClassLoader = new URLClassLoader(allJars, rootClassLoader) {
    override def loadClass(name: String, resolve: Boolean): Class[_] = {
      val loaded = findLoadedClass(name)
      if (loaded == null) doLoadClass(name, resolve) else loaded
    }

    def doLoadClass(name: String, resolve: Boolean): Class[_] = {
      val classFileName = name.replaceAll("\\.", "/") + ".class"
      if (isBarrierClass(name) && isolationOn) {
        val bytes = IOUtils.toByteArray(baseClassLoader.getResourceAsStream(classFileName))
        logDebug(s"custom defining: $name - ${util.Arrays.hashCode(bytes)}")
        defineClass(name, bytes, 0, bytes.length)
      } else if (!isSharedClass(name)) {
        logDebug(s"hive class: $name - ${getResource(classToPath(name))}")
        super.loadClass(name, resolve)
      } else {
        logDebug(s"shared class: $name")
        baseClassLoader.loadClass(name)
      }
    }
  }

  // Pre-reflective instantiation setup.
  logDebug("Initializing the logger to avoid disaster...")
  Thread.currentThread.setContextClassLoader(classLoader)

  /** The isolated client interface to Hive. */
  val client: ClientInterface = try {
    classLoader
      .loadClass(classOf[ClientWrapper].getName)
      .getConstructors.head
      .newInstance(version, config)
      .asInstanceOf[ClientInterface]
  } finally {
    Thread.currentThread.setContextClassLoader(baseClassLoader)
  }
}
