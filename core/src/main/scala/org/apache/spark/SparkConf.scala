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

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.JavaConverters._
import scala.collection.mutable.LinkedHashSet

import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.util.Utils

/**
 * Configuration for a Spark application. Used to set various Spark parameters as key-value pairs.
 *
 * Most of the time, you would create a SparkConf object with `new SparkConf()`, which will load
 * values from any `spark.*` Java system properties set in your application as well. In this case,
 * parameters you set directly on the `SparkConf` object take priority over system properties.
 *
 * For unit tests, you can also call `new SparkConf(false)` to skip loading external settings and
 * get the same configuration no matter what the system properties are.
 *
 * All setter methods in this class support chaining. For example, you can write
 * `new SparkConf().setMaster("local").setAppName("My app")`.
 *
 * Note that once a SparkConf object is passed to Spark, it is cloned and can no longer be modified
 * by the user. Spark does not support modifying the configuration at runtime.
 *
 * @param loadDefaults whether to also load values from Java system properties
 */
class SparkConf(loadDefaults: Boolean) extends Cloneable with Logging {

  import SparkConf._

  /** Create a SparkConf that loads defaults from system properties and the classpath */
  def this() = this(true)

  private val settings = new ConcurrentHashMap[String, String]()

  if (loadDefaults) {
    // Load any spark.* system properties
    for ((key, value) <- Utils.getSystemProperties if key.startsWith("spark.")) {
      set(key, value)
    }
  }

  /** Set a configuration variable. */
  def set(key: String, value: String): SparkConf = {
    if (key == null) {
      throw new NullPointerException("null key")
    }
    if (value == null) {
      throw new NullPointerException("null value for " + key)
    }
    settings.put(key, value)
    this
  }

  /**
   * The master URL to connect to, such as "local" to run locally with one thread, "local[4]" to
   * run locally with 4 cores, or "spark://master:7077" to run on a Spark standalone cluster.
   */
  def setMaster(master: String): SparkConf = {
    set("spark.master", master)
  }

  /** Set a name for your application. Shown in the Spark web UI. */
  def setAppName(name: String): SparkConf = {
    set("spark.app.name", name)
  }

  /** Set JAR files to distribute to the cluster. */
  def setJars(jars: Seq[String]): SparkConf = {
    for (jar <- jars if (jar == null)) logWarning("null jar passed to SparkContext constructor")
    set("spark.jars", jars.filter(_ != null).mkString(","))
  }

  /** Set JAR files to distribute to the cluster. (Java-friendly version.) */
  def setJars(jars: Array[String]): SparkConf = {
    setJars(jars.toSeq)
  }

  /**
   * Set an environment variable to be used when launching executors for this application.
   * These variables are stored as properties of the form spark.executorEnv.VAR_NAME
   * (for example spark.executorEnv.PATH) but this method makes them easier to set.
   */
  def setExecutorEnv(variable: String, value: String): SparkConf = {
    set("spark.executorEnv." + variable, value)
  }

  /**
   * Set multiple environment variables to be used when launching executors.
   * These variables are stored as properties of the form spark.executorEnv.VAR_NAME
   * (for example spark.executorEnv.PATH) but this method makes them easier to set.
   */
  def setExecutorEnv(variables: Seq[(String, String)]): SparkConf = {
    for ((k, v) <- variables) {
      setExecutorEnv(k, v)
    }
    this
  }

  /**
   * Set multiple environment variables to be used when launching executors.
   * (Java-friendly version.)
   */
  def setExecutorEnv(variables: Array[(String, String)]): SparkConf = {
    setExecutorEnv(variables.toSeq)
  }

  /**
   * Set the location where Spark is installed on worker nodes.
   */
  def setSparkHome(home: String): SparkConf = {
    set("spark.home", home)
  }

  /** Set multiple parameters together */
  def setAll(settings: Traversable[(String, String)]): SparkConf = {
    this.settings.putAll(settings.toMap.asJava)
    this
  }

  /** Set a parameter if it isn't already configured */
  def setIfMissing(key: String, value: String): SparkConf = {
    settings.putIfAbsent(key, value)
    this
  }

  /**
   * Use Kryo serialization and register the given set of classes with Kryo.
   * If called multiple times, this will append the classes from all calls together.
   */
  def registerKryoClasses(classes: Array[Class[_]]): SparkConf = {
    val allClassNames = new LinkedHashSet[String]()
    allClassNames ++= get("spark.kryo.classesToRegister", "").split(',').filter(!_.isEmpty)
    allClassNames ++= classes.map(_.getName)

    set("spark.kryo.classesToRegister", allClassNames.mkString(","))
    set("spark.serializer", classOf[KryoSerializer].getName)
    this
  }

  /** Remove a parameter from the configuration */
  def remove(key: String): SparkConf = {
    settings.remove(key)
    this
  }

  /** Get a parameter; throws a NoSuchElementException if it's not set */
  def get(key: String): String = {
    getOption(key).getOrElse(throw new NoSuchElementException(key))
  }

  /** Get a parameter, falling back to a default if not set */
  def get(key: String, defaultValue: String): String = {
    getOption(key).getOrElse(defaultValue)
  }

  /** Get a parameter as an Option */
  def getOption(key: String): Option[String] = {
    Option(settings.get(key))
  }

  /** Get all parameters as a list of pairs */
  def getAll: Array[(String, String)] = {
    settings.entrySet().asScala.map(x => (x.getKey, x.getValue)).toArray
  }

  /** Get a parameter as an integer, falling back to a default if not set */
  def getInt(key: String, defaultValue: Int): Int = {
    getOption(key).map(_.toInt).getOrElse(defaultValue)
  }

  /** Get a parameter as a long, falling back to a default if not set */
  def getLong(key: String, defaultValue: Long): Long = {
    getOption(key).map(_.toLong).getOrElse(defaultValue)
  }

  /** Get a parameter as a double, falling back to a default if not set */
  def getDouble(key: String, defaultValue: Double): Double = {
    getOption(key).map(_.toDouble).getOrElse(defaultValue)
  }

  /** Get a parameter as a boolean, falling back to a default if not set */
  def getBoolean(key: String, defaultValue: Boolean): Boolean = {
    getOption(key).map(_.toBoolean).getOrElse(defaultValue)
  }

  /** Get all executor environment variables set on this SparkConf */
  def getExecutorEnv: Seq[(String, String)] = {
    val prefix = "spark.executorEnv."
    getAll.filter{case (k, v) => k.startsWith(prefix)}
          .map{case (k, v) => (k.substring(prefix.length), v)}
  }

  /** Get all akka conf variables set on this SparkConf */
  def getAkkaConf: Seq[(String, String)] =
    /* This is currently undocumented. If we want to make this public we should consider
     * nesting options under the spark namespace to avoid conflicts with user akka options.
     * Otherwise users configuring their own akka code via system properties could mess up
     * spark's akka options.
     *
     *   E.g. spark.akka.option.x.y.x = "value"
     */
    getAll.filter { case (k, _) => isAkkaConf(k) }

  /**
   * Returns the Spark application id, valid in the Driver after TaskScheduler registration and
   * from the start in the Executor.
   */
  def getAppId: String = get("spark.app.id")

  /** Does the configuration contain a given parameter? */
  def contains(key: String): Boolean = settings.containsKey(key)

  /** Copy this object */
  override def clone: SparkConf = {
    new SparkConf(false).setAll(getAll)
  }

  /**
   * By using this instead of System.getenv(), environment variables can be mocked
   * in unit tests.
   */
  private[spark] def getenv(name: String): String = System.getenv(name)

  /** Checks for illegal or deprecated config settings. Throws an exception for the former. Not
    * idempotent - may mutate this conf object to convert deprecated settings to supported ones. */
  private[spark] def validateSettings() {
    if (contains("spark.local.dir")) {
      val msg = "In Spark 1.0 and later spark.local.dir will be overridden by the value set by " +
        "the cluster manager (via SPARK_LOCAL_DIRS in mesos/standalone and LOCAL_DIRS in YARN)."
      logWarning(msg)
    }

    val executorOptsKey = "spark.executor.extraJavaOptions"
    val executorClasspathKey = "spark.executor.extraClassPath"
    val driverOptsKey = "spark.driver.extraJavaOptions"
    val driverClassPathKey = "spark.driver.extraClassPath"
    val driverLibraryPathKey = "spark.driver.extraLibraryPath"

    // Used by Yarn in 1.1 and before
    sys.props.get("spark.driver.libraryPath").foreach { value =>
      val warning =
        s"""
          |spark.driver.libraryPath was detected (set to '$value').
          |This is deprecated in Spark 1.2+.
          |
          |Please instead use: $driverLibraryPathKey
        """.stripMargin
      logWarning(warning)
    }

    // Validate spark.executor.extraJavaOptions
    getOption(executorOptsKey).map { javaOpts =>
      if (javaOpts.contains("-Dspark")) {
        val msg = s"$executorOptsKey is not allowed to set Spark options (was '$javaOpts'). " +
          "Set them directly on a SparkConf or in a properties file when using ./bin/spark-submit."
        throw new Exception(msg)
      }
      if (javaOpts.contains("-Xmx") || javaOpts.contains("-Xms")) {
        val msg = s"$executorOptsKey is not allowed to alter memory settings (was '$javaOpts'). " +
          "Use spark.executor.memory instead."
        throw new Exception(msg)
      }
    }

    // Validate memory fractions
    val memoryKeys = Seq(
      "spark.storage.memoryFraction",
      "spark.shuffle.memoryFraction",
      "spark.shuffle.safetyFraction",
      "spark.storage.unrollFraction",
      "spark.storage.safetyFraction")
    for (key <- memoryKeys) {
      val value = getDouble(key, 0.5)
      if (value > 1 || value < 0) {
        throw new IllegalArgumentException("$key should be between 0 and 1 (was '$value').")
      }
    }

    // Check for legacy configs
    sys.env.get("SPARK_JAVA_OPTS").foreach { value =>
      val warning =
        s"""
          |SPARK_JAVA_OPTS was detected (set to '$value').
          |This is deprecated in Spark 1.0+.
          |
          |Please instead use:
          | - ./spark-submit with conf/spark-defaults.conf to set defaults for an application
          | - ./spark-submit with --driver-java-options to set -X options for a driver
          | - spark.executor.extraJavaOptions to set -X options for executors
          | - SPARK_DAEMON_JAVA_OPTS to set java options for standalone daemons (master or worker)
        """.stripMargin
      logWarning(warning)

      for (key <- Seq(executorOptsKey, driverOptsKey)) {
        if (getOption(key).isDefined) {
          throw new SparkException(s"Found both $key and SPARK_JAVA_OPTS. Use only the former.")
        } else {
          logWarning(s"Setting '$key' to '$value' as a work-around.")
          set(key, value)
        }
      }
    }

    sys.env.get("SPARK_CLASSPATH").foreach { value =>
      val warning =
        s"""
          |SPARK_CLASSPATH was detected (set to '$value').
          |This is deprecated in Spark 1.0+.
          |
          |Please instead use:
          | - ./spark-submit with --driver-class-path to augment the driver classpath
          | - spark.executor.extraClassPath to augment the executor classpath
        """.stripMargin
      logWarning(warning)

      for (key <- Seq(executorClasspathKey, driverClassPathKey)) {
        if (getOption(key).isDefined) {
          throw new SparkException(s"Found both $key and SPARK_CLASSPATH. Use only the former.")
        } else {
          logWarning(s"Setting '$key' to '$value' as a work-around.")
          set(key, value)
        }
      }
    }

    // Warn against the use of deprecated configs
    deprecatedConfigs.values.foreach { dc =>
      if (contains(dc.oldName)) {
        dc.warn()
      }
    }
  }

  /**
   * Return a string listing all keys and values, one per line. This is useful to print the
   * configuration out for debugging.
   */
  def toDebugString: String = {
    getAll.sorted.map{case (k, v) => k + "=" + v}.mkString("\n")
  }

}

private[spark] object SparkConf extends Logging {

  private val deprecatedConfigs: Map[String, DeprecatedConfig] = {
    val configs = Seq(
      DeprecatedConfig("spark.files.userClassPathFirst", "spark.executor.userClassPathFirst",
        "1.3"),
      DeprecatedConfig("spark.yarn.user.classpath.first", null, "1.3",
        "Use spark.{driver,executor}.userClassPathFirst instead."),
      DeprecatedConfig("spark.history.fs.updateInterval",
        "spark.history.fs.update.interval.seconds",
        "1.3", "Use spark.history.fs.update.interval.seconds instead"),
      DeprecatedConfig("spark.history.updateInterval",
        "spark.history.fs.update.interval.seconds",
        "1.3", "Use spark.history.fs.update.interval.seconds instead"))
    configs.map { x => (x.oldName, x) }.toMap
  }

  /**
   * Return whether the given config is an akka config (e.g. akka.actor.provider).
   * Note that this does not include spark-specific akka configs (e.g. spark.akka.timeout).
   */
  def isAkkaConf(name: String): Boolean = name.startsWith("akka.")

  /**
   * Return whether the given config should be passed to an executor on start-up.
   *
   * Certain akka and authentication configs are required of the executor when it connects to
   * the scheduler, while the rest of the spark configs can be inherited from the driver later.
   */
  def isExecutorStartupConf(name: String): Boolean = {
    isAkkaConf(name) ||
    name.startsWith("spark.akka") ||
    name.startsWith("spark.auth") ||
    name.startsWith("spark.ssl") ||
    isSparkPortConf(name)
  }

  /**
   * Return true if the given config matches either `spark.*.port` or `spark.port.*`.
   */
  def isSparkPortConf(name: String): Boolean = {
    (name.startsWith("spark.") && name.endsWith(".port")) || name.startsWith("spark.port.")
  }

  /**
   * Translate the configuration key if it is deprecated and has a replacement, otherwise just
   * returns the provided key.
   *
   * @param userKey Configuration key from the user / caller.
   * @param warn Whether to print a warning if the key is deprecated. Warnings will be printed
   *             only once for each key.
   */
  private def translateConfKey(userKey: String, warn: Boolean = false): String = {
    deprecatedConfigs.get(userKey)
      .map { deprecatedKey =>
        if (warn) {
          deprecatedKey.warn()
        }
        deprecatedKey.newName.getOrElse(userKey)
      }.getOrElse(userKey)
  }

  /**
   * Holds information about keys that have been deprecated or renamed.
   *
   * @param oldName Old configuration key.
   * @param newName New configuration key, or `null` if key has no replacement, in which case the
   *                deprecated key will be used (but the warning message will still be printed).
   * @param version Version of Spark where key was deprecated.
   * @param deprecationMessage Message to include in the deprecation warning; mandatory when
   *                           `newName` is not provided.
   */
  private case class DeprecatedConfig(
      oldName: String,
      _newName: String,
      version: String,
      deprecationMessage: String = null) {

    private val warned = new AtomicBoolean(false)
    val newName = Option(_newName)

    if (newName == null && (deprecationMessage == null || deprecationMessage.isEmpty())) {
      throw new IllegalArgumentException("Need new config name or deprecation message.")
    }

    def warn(): Unit = {
      if (warned.compareAndSet(false, true)) {
        if (newName != null) {
          val message = Option(deprecationMessage).getOrElse(
            s"Please use the alternative '$newName' instead.")
          logWarning(
            s"The configuration option '$oldName' has been replaced as of Spark $version and " +
            s"may be removed in the future. $message")
        } else {
          logWarning(
            s"The configuration option '$oldName' has been deprecated as of Spark $version and " +
            s"may be removed in the future. $deprecationMessage")
        }
      }
    }

  }
}
