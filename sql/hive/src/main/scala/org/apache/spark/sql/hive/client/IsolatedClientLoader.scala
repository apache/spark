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
import java.lang.reflect.InvocationTargetException
import java.net.{URL, URLClassLoader}
import java.util

import scala.util.Try

import org.apache.commons.io.{FileUtils, IOUtils}
import org.apache.commons.lang3.{JavaVersion, SystemUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hadoop.hive.shims.ShimLoader

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkSubmitUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.util.quietly
import org.apache.spark.sql.hive.HiveUtils
import org.apache.spark.sql.internal.NonClosableMutableURLClassLoader
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.{MutableURLClassLoader, Utils}

/** Factory for `IsolatedClientLoader` with specific versions of hive. */
private[hive] object IsolatedClientLoader extends Logging {
  /**
   * Creates isolated Hive client loaders by downloading the requested version from maven.
   */
  def forVersion(
      hiveMetastoreVersion: String,
      hadoopVersion: String,
      sparkConf: SparkConf,
      hadoopConf: Configuration,
      config: Map[String, String] = Map.empty,
      ivyPath: Option[String] = None,
      sharedPrefixes: Seq[String] = Seq.empty,
      barrierPrefixes: Seq[String] = Seq.empty,
      sharesHadoopClasses: Boolean = true): IsolatedClientLoader = synchronized {
    val resolvedVersion = hiveVersion(hiveMetastoreVersion)
    // We will first try to share Hadoop classes. If we cannot resolve the Hadoop artifact
    // with the given version, we will use Hadoop 2.7 and then will not share Hadoop classes.
    var _sharesHadoopClasses = sharesHadoopClasses
    val files = if (resolvedVersions.contains((resolvedVersion, hadoopVersion))) {
      resolvedVersions((resolvedVersion, hadoopVersion))
    } else {
      val remoteRepos = sparkConf.get(SQLConf.ADDITIONAL_REMOTE_REPOSITORIES)
      val (downloadedFiles, actualHadoopVersion) =
        try {
          (downloadVersion(resolvedVersion, hadoopVersion, ivyPath, remoteRepos), hadoopVersion)
        } catch {
          case e: RuntimeException if e.getMessage.contains("hadoop") =>
            // If the error message contains hadoop, it is probably because the hadoop
            // version cannot be resolved.
            val fallbackVersion = "2.7.4"
            logWarning(s"Failed to resolve Hadoop artifacts for the version $hadoopVersion. We " +
              s"will change the hadoop version from $hadoopVersion to $fallbackVersion and try " +
              "again. Hadoop classes will not be shared between Spark and Hive metastore client. " +
              "It is recommended to set jars used by Hive metastore client through " +
              "spark.sql.hive.metastore.jars in the production environment.")
            _sharesHadoopClasses = false
            (downloadVersion(
              resolvedVersion, fallbackVersion, ivyPath, remoteRepos), fallbackVersion)
        }
      resolvedVersions.put((resolvedVersion, actualHadoopVersion), downloadedFiles)
      resolvedVersions((resolvedVersion, actualHadoopVersion))
    }

    new IsolatedClientLoader(
      hiveVersion(hiveMetastoreVersion),
      sparkConf,
      execJars = files,
      hadoopConf = hadoopConf,
      config = config,
      sharesHadoopClasses = _sharesHadoopClasses,
      sharedPrefixes = sharedPrefixes,
      barrierPrefixes = barrierPrefixes)
  }

  def hiveVersion(version: String): HiveVersion = version match {
    case "12" | "0.12" | "0.12.0" => hive.v12
    case "13" | "0.13" | "0.13.0" | "0.13.1" => hive.v13
    case "14" | "0.14" | "0.14.0" => hive.v14
    case "1.0" | "1.0.0" | "1.0.1" => hive.v1_0
    case "1.1" | "1.1.0" | "1.1.1" => hive.v1_1
    case "1.2" | "1.2.0" | "1.2.1" | "1.2.2" => hive.v1_2
    case "2.0" | "2.0.0" | "2.0.1" => hive.v2_0
    case "2.1" | "2.1.0" | "2.1.1" => hive.v2_1
    case "2.2" | "2.2.0" => hive.v2_2
    case "2.3" | "2.3.0" | "2.3.1" | "2.3.2" | "2.3.3" | "2.3.4" | "2.3.5" | "2.3.6" | "2.3.7" =>
      hive.v2_3
    case "3.0" | "3.0.0" => hive.v3_0
    case "3.1" | "3.1.0" | "3.1.1" | "3.1.2" => hive.v3_1
    case version =>
      throw new UnsupportedOperationException(s"Unsupported Hive Metastore version ($version). " +
        s"Please set ${HiveUtils.HIVE_METASTORE_VERSION.key} with a valid version.")
  }

  private def downloadVersion(
      version: HiveVersion,
      hadoopVersion: String,
      ivyPath: Option[String],
      remoteRepos: String): Seq[URL] = {
    val hadoopJarName = if (hadoopVersion.startsWith("3")) {
      s"org.apache.hadoop:hadoop-client-runtime:$hadoopVersion"
    } else {
      s"org.apache.hadoop:hadoop-client:$hadoopVersion"
    }
    val hiveArtifacts = version.extraDeps ++
      Seq("hive-metastore", "hive-exec", "hive-common", "hive-serde")
        .map(a => s"org.apache.hive:$a:${version.fullVersion}") ++
      Seq("com.google.guava:guava:14.0.1", hadoopJarName)

    val classpath = quietly {
      SparkSubmitUtils.resolveMavenCoordinates(
        hiveArtifacts.mkString(","),
        SparkSubmitUtils.buildIvySettings(
          Some(remoteRepos),
          ivyPath),
        exclusions = version.exclusions)
    }
    val allFiles = classpath.split(",").map(new File(_)).toSet

    // TODO: Remove copy logic.
    val tempDir = Utils.createTempDir(namePrefix = s"hive-${version}")
    allFiles.foreach(f => FileUtils.copyFileToDirectory(f, tempDir))
    logInfo(s"Downloaded metastore jars to ${tempDir.getCanonicalPath}")
    tempDir.listFiles().map(_.toURI.toURL)
  }

  // A map from a given pair of HiveVersion and Hadoop version to jar files.
  // It is only used by forVersion.
  private val resolvedVersions =
    new scala.collection.mutable.HashMap[(HiveVersion, String), Seq[URL]]
}

/**
 * Creates a [[HiveClient]] using a classloader that works according to the following rules:
 *  - Shared classes: Java, Scala, logging, and Spark classes are delegated to `baseClassLoader`
 *    allowing the results of calls to the [[HiveClient]] to be visible externally.
 *  - Hive classes: new instances are loaded from `execJars`.  These classes are not
 *    accessible externally due to their custom loading.
 *  - [[HiveClientImpl]]: a new copy is created for each instance of `IsolatedClassLoader`.
 *    This new instance is able to see a specific version of hive without using reflection. Since
 *    this is a unique instance, it is not visible externally other than as a generic
 *    [[HiveClient]], unless `isolationOn` is set to `false`.
 *
 * @param version The version of hive on the classpath.  used to pick specific function signatures
 *                that are not compatible across versions.
 * @param execJars A collection of jar files that must include hive and hadoop.
 * @param config   A set of options that will be added to the HiveConf of the constructed client.
 * @param isolationOn When true, custom versions of barrier classes will be constructed.  Must be
 *                    true unless loading the version of hive that is on Spark's classloader.
 * @param sharesHadoopClasses When true, we will share Hadoop classes between Spark and
 * @param baseClassLoader The spark classloader that is used to load shared classes.
 */
private[hive] class IsolatedClientLoader(
    val version: HiveVersion,
    val sparkConf: SparkConf,
    val hadoopConf: Configuration,
    val execJars: Seq[URL] = Seq.empty,
    val config: Map[String, String] = Map.empty,
    val isolationOn: Boolean = true,
    val sharesHadoopClasses: Boolean = true,
    val baseClassLoader: ClassLoader = Thread.currentThread().getContextClassLoader,
    val sharedPrefixes: Seq[String] = Seq.empty,
    val barrierPrefixes: Seq[String] = Seq.empty)
  extends Logging {

  /** All jars used by the hive specific classloader. */
  protected def allJars = execJars.toArray

  protected def isSharedClass(name: String): Boolean = {
    val isHadoopClass =
      name.startsWith("org.apache.hadoop.") && !name.startsWith("org.apache.hadoop.hive.")

    name.startsWith("org.slf4j") ||
    name.startsWith("org.apache.log4j") || // log4j1.x
    name.startsWith("org.apache.logging.log4j") || // log4j2
    name.startsWith("org.apache.spark.") ||
    (sharesHadoopClasses && isHadoopClass) ||
    name.startsWith("scala.") ||
    (name.startsWith("com.google") && !name.startsWith("com.google.cloud")) ||
    name.startsWith("java.") ||
    name.startsWith("javax.sql.") ||
    sharedPrefixes.exists(name.startsWith)
  }

  /** True if `name` refers to a spark class that must see specific version of Hive. */
  protected def isBarrierClass(name: String): Boolean =
    name.startsWith(classOf[HiveClientImpl].getName) ||
    name.startsWith(classOf[Shim].getName) ||
    name.startsWith(classOf[ShimLoader].getName) ||
    barrierPrefixes.exists(name.startsWith)

  protected def classToPath(name: String): String =
    name.replaceAll("\\.", "/") + ".class"

  /**
   * The classloader that is used to load an isolated version of Hive.
   * This classloader is a special URLClassLoader that exposes the addURL method.
   * So, when we add jar, we can add this new jar directly through the addURL method
   * instead of stacking a new URLClassLoader on top of it.
   */
  private[hive] val classLoader: MutableURLClassLoader = {
    val isolatedClassLoader =
      if (isolationOn) {
        if (allJars.isEmpty) {
          // See HiveUtils; this is the Java 9+ + builtin mode scenario
          baseClassLoader
        } else {
          val rootClassLoader: ClassLoader =
            if (SystemUtils.isJavaVersionAtLeast(JavaVersion.JAVA_9)) {
              // In Java 9, the boot classloader can see few JDK classes. The intended parent
              // classloader for delegation is now the platform classloader.
              // See http://java9.wtf/class-loading/
              val platformCL =
              classOf[ClassLoader].getMethod("getPlatformClassLoader").
                invoke(null).asInstanceOf[ClassLoader]
              // Check to make sure that the root classloader does not know about Hive.
              assert(Try(platformCL.loadClass("org.apache.hadoop.hive.conf.HiveConf")).isFailure)
              platformCL
            } else {
              // The boot classloader is represented by null (the instance itself isn't accessible)
              // and before Java 9 can see all JDK classes
              null
            }
          new URLClassLoader(allJars, rootClassLoader) {
            override def loadClass(name: String, resolve: Boolean): Class[_] = {
              val loaded = findLoadedClass(name)
              if (loaded == null) doLoadClass(name, resolve) else loaded
            }
            def doLoadClass(name: String, resolve: Boolean): Class[_] = {
              val classFileName = name.replaceAll("\\.", "/") + ".class"
              if (isBarrierClass(name)) {
                // For barrier classes, we construct a new copy of the class.
                val bytes = IOUtils.toByteArray(baseClassLoader.getResourceAsStream(classFileName))
                logDebug(s"custom defining: $name - ${util.Arrays.hashCode(bytes)}")
                defineClass(name, bytes, 0, bytes.length)
              } else if (!isSharedClass(name)) {
                logDebug(s"hive class: $name - ${getResource(classToPath(name))}")
                super.loadClass(name, resolve)
              } else {
                // For shared classes, we delegate to baseClassLoader, but fall back in case the
                // class is not found.
                logDebug(s"shared class: $name")
                try {
                  baseClassLoader.loadClass(name)
                } catch {
                  case _: ClassNotFoundException =>
                    super.loadClass(name, resolve)
                }
              }
            }
          }
        }
      } else {
        baseClassLoader
      }
    // Right now, we create a URLClassLoader that gives preference to isolatedClassLoader
    // over its own URLs when it loads classes and resources.
    // We may want to use ChildFirstURLClassLoader based on
    // the configuration of spark.executor.userClassPathFirst, which gives preference
    // to its own URLs over the parent class loader (see Executor's createClassLoader method).
    new NonClosableMutableURLClassLoader(isolatedClassLoader)
  }

  private[hive] def addJar(path: URL): Unit = synchronized {
    classLoader.addURL(path)
  }

  /** The isolated client interface to Hive. */
  private[hive] def createClient(): HiveClient = synchronized {
    val warehouseDir = Option(hadoopConf.get(ConfVars.METASTOREWAREHOUSE.varname))
    if (!isolationOn) {
      return new HiveClientImpl(version, warehouseDir, sparkConf, hadoopConf, config,
        baseClassLoader, this)
    }
    // Pre-reflective instantiation setup.
    logDebug("Initializing the logger to avoid disaster...")
    val origLoader = Thread.currentThread().getContextClassLoader
    Thread.currentThread.setContextClassLoader(classLoader)

    try {
      classLoader
        .loadClass(classOf[HiveClientImpl].getName)
        .getConstructors.head
        .newInstance(version, warehouseDir, sparkConf, hadoopConf, config, classLoader, this)
        .asInstanceOf[HiveClient]
    } catch {
      case e: InvocationTargetException =>
        if (e.getCause().isInstanceOf[NoClassDefFoundError]) {
          val cnf = e.getCause().asInstanceOf[NoClassDefFoundError]
          throw new ClassNotFoundException(
            s"$cnf when creating Hive client using classpath: ${execJars.mkString(", ")}\n" +
            "Please make sure that jars for your version of hive and hadoop are included in the " +
            s"paths passed to ${HiveUtils.HIVE_METASTORE_JARS.key}.", e)
        } else {
          throw e
        }
    } finally {
      Thread.currentThread.setContextClassLoader(origLoader)
    }
  }

  /**
   * The place holder for shared Hive client for all the HiveContext sessions (they share an
   * IsolatedClientLoader).
   */
  private[hive] var cachedHive: Any = null
}
