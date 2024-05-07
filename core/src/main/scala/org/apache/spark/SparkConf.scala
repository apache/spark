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

import java.util.{Map => JMap}
import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable.LinkedHashSet
import scala.jdk.CollectionConverters._

import org.apache.avro.{Schema, SchemaNormalization}

import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys
import org.apache.spark.internal.config._
import org.apache.spark.internal.config.History._
import org.apache.spark.internal.config.Kryo._
import org.apache.spark.internal.config.Network._
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.util.ArrayImplicits._
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
 * @param loadDefaults whether to also load values from Java system properties
 *
 * @note Once a SparkConf object is passed to Spark, it is cloned and can no longer be modified
 * by the user. Spark does not support modifying the configuration at runtime.
 */
class SparkConf(loadDefaults: Boolean) extends Cloneable with Logging with Serializable {

  import SparkConf._

  /** Create a SparkConf that loads defaults from system properties and the classpath */
  def this() = this(true)

  private val settings = new ConcurrentHashMap[String, String]()

  @transient private lazy val reader: ConfigReader = {
    val _reader = new ConfigReader(new SparkConfigProvider(settings))
    _reader.bindEnv((key: String) => Option(getenv(key)))
    _reader
  }

  if (loadDefaults) {
    loadFromSystemProperties(false)
  }

  private[spark] def loadFromSystemProperties(silent: Boolean): SparkConf = {
    // Load any spark.* system properties
    for ((key, value) <- Utils.getSystemProperties if key.startsWith("spark.")) {
      set(key, value, silent)
    }
    this
  }

  /** Set a configuration variable. */
  def set(key: String, value: String): SparkConf = {
    set(key, value, false)
  }

  private[spark] def set(key: String, value: String, silent: Boolean): SparkConf = {
    if (key == null) {
      throw new NullPointerException("null key")
    }
    if (value == null) {
      throw new NullPointerException("null value for " + key)
    }
    if (!silent) {
      logDeprecationWarning(key)
    }
    settings.put(key, value)
    this
  }

  private[spark] def set[T](entry: ConfigEntry[T], value: T): SparkConf = {
    set(entry.key, entry.stringConverter(value))
    this
  }

  private[spark] def set[T](entry: OptionalConfigEntry[T], value: T): SparkConf = {
    set(entry.key, entry.rawStringConverter(value))
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
    set(JARS, jars.filter(_ != null))
  }

  /** Set JAR files to distribute to the cluster. (Java-friendly version.) */
  def setJars(jars: Array[String]): SparkConf = {
    setJars(jars.toImmutableArraySeq)
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
    setExecutorEnv(variables.toImmutableArraySeq)
  }

  /**
   * Set the location where Spark is installed on worker nodes.
   */
  def setSparkHome(home: String): SparkConf = {
    set("spark.home", home)
  }

  /** Set multiple parameters together */
  def setAll(settings: Iterable[(String, String)]): SparkConf = {
    settings.foreach { case (k, v) => set(k, v) }
    this
  }

  /** Set a parameter if it isn't already configured */
  def setIfMissing(key: String, value: String): SparkConf = {
    if (settings.putIfAbsent(key, value) == null) {
      logDeprecationWarning(key)
    }
    this
  }

  private[spark] def setIfMissing[T](entry: ConfigEntry[T], value: T): SparkConf = {
    if (settings.putIfAbsent(entry.key, entry.stringConverter(value)) == null) {
      logDeprecationWarning(entry.key)
    }
    this
  }

  private[spark] def setIfMissing[T](entry: OptionalConfigEntry[T], value: T): SparkConf = {
    if (settings.putIfAbsent(entry.key, entry.rawStringConverter(value)) == null) {
      logDeprecationWarning(entry.key)
    }
    this
  }

  /**
   * Use Kryo serialization and register the given set of classes with Kryo.
   * If called multiple times, this will append the classes from all calls together.
   */
  def registerKryoClasses(classes: Array[Class[_]]): SparkConf = {
    val allClassNames = new LinkedHashSet[String]()
    allClassNames ++= get(KRYO_CLASSES_TO_REGISTER).map(_.trim)
      .filter(!_.isEmpty)
    allClassNames ++= classes.map(_.getName)

    set(KRYO_CLASSES_TO_REGISTER, allClassNames.toSeq)
    set(SERIALIZER, classOf[KryoSerializer].getName)
    this
  }

  private final val avroNamespace = "avro.schema."

  /**
   * Use Kryo serialization and register the given set of Avro schemas so that the generic
   * record serializer can decrease network IO
   */
  def registerAvroSchemas(schemas: Schema*): SparkConf = {
    for (schema <- schemas) {
      set(avroNamespace + SchemaNormalization.parsingFingerprint64(schema), schema.toString)
    }
    this
  }

  /** Gets all the avro schemas in the configuration used in the generic Avro record serializer */
  def getAvroSchema: Map[Long, String] = {
    getAll.filter { case (k, v) => k.startsWith(avroNamespace) }
      .map { case (k, v) => (k.substring(avroNamespace.length).toLong, v) }
      .toMap
  }

  /** Remove a parameter from the configuration */
  def remove(key: String): SparkConf = {
    settings.remove(key)
    this
  }

  private[spark] def remove(entry: ConfigEntry[_]): SparkConf = {
    remove(entry.key)
  }

  /** Get a parameter; throws a NoSuchElementException if it's not set */
  def get(key: String): String = {
    getOption(key).getOrElse(throw new NoSuchElementException(key))
  }

  /** Get a parameter, falling back to a default if not set */
  def get(key: String, defaultValue: String): String = {
    getOption(key).getOrElse(defaultValue)
  }

  /**
   * Retrieves the value of a pre-defined configuration entry.
   *
   * - This is an internal Spark API.
   * - The return type if defined by the configuration entry.
   * - This will throw an exception is the config is not optional and the value is not set.
   */
  private[spark] def get[T](entry: ConfigEntry[T]): T = {
    entry.readFrom(reader)
  }

  /**
   * Get a time parameter as seconds; throws a NoSuchElementException if it's not set. If no
   * suffix is provided then seconds are assumed.
   * @throws java.util.NoSuchElementException If the time parameter is not set
   * @throws NumberFormatException If the value cannot be interpreted as seconds
   */
  def getTimeAsSeconds(key: String): Long = catchIllegalValue(key) {
    Utils.timeStringAsSeconds(get(key))
  }

  /**
   * Get a time parameter as seconds, falling back to a default if not set. If no
   * suffix is provided then seconds are assumed.
   * @throws NumberFormatException If the value cannot be interpreted as seconds
   */
  def getTimeAsSeconds(key: String, defaultValue: String): Long = catchIllegalValue(key) {
    Utils.timeStringAsSeconds(get(key, defaultValue))
  }

  /**
   * Get a time parameter as milliseconds; throws a NoSuchElementException if it's not set. If no
   * suffix is provided then milliseconds are assumed.
   * @throws java.util.NoSuchElementException If the time parameter is not set
   * @throws NumberFormatException If the value cannot be interpreted as milliseconds
   */
  def getTimeAsMs(key: String): Long = catchIllegalValue(key) {
    Utils.timeStringAsMs(get(key))
  }

  /**
   * Get a time parameter as milliseconds, falling back to a default if not set. If no
   * suffix is provided then milliseconds are assumed.
   * @throws NumberFormatException If the value cannot be interpreted as milliseconds
   */
  def getTimeAsMs(key: String, defaultValue: String): Long = catchIllegalValue(key) {
    Utils.timeStringAsMs(get(key, defaultValue))
  }

  /**
   * Get a size parameter as bytes; throws a NoSuchElementException if it's not set. If no
   * suffix is provided then bytes are assumed.
   * @throws java.util.NoSuchElementException If the size parameter is not set
   * @throws NumberFormatException If the value cannot be interpreted as bytes
   */
  def getSizeAsBytes(key: String): Long = catchIllegalValue(key) {
    Utils.byteStringAsBytes(get(key))
  }

  /**
   * Get a size parameter as bytes, falling back to a default if not set. If no
   * suffix is provided then bytes are assumed.
   * @throws NumberFormatException If the value cannot be interpreted as bytes
   */
  def getSizeAsBytes(key: String, defaultValue: String): Long = catchIllegalValue(key) {
    Utils.byteStringAsBytes(get(key, defaultValue))
  }

  /**
   * Get a size parameter as bytes, falling back to a default if not set.
   * @throws NumberFormatException If the value cannot be interpreted as bytes
   */
  def getSizeAsBytes(key: String, defaultValue: Long): Long = catchIllegalValue(key) {
    Utils.byteStringAsBytes(get(key, s"${defaultValue}B"))
  }

  /**
   * Get a size parameter as Kibibytes; throws a NoSuchElementException if it's not set. If no
   * suffix is provided then Kibibytes are assumed.
   * @throws java.util.NoSuchElementException If the size parameter is not set
   * @throws NumberFormatException If the value cannot be interpreted as Kibibytes
   */
  def getSizeAsKb(key: String): Long = catchIllegalValue(key) {
    Utils.byteStringAsKb(get(key))
  }

  /**
   * Get a size parameter as Kibibytes, falling back to a default if not set. If no
   * suffix is provided then Kibibytes are assumed.
   * @throws NumberFormatException If the value cannot be interpreted as Kibibytes
   */
  def getSizeAsKb(key: String, defaultValue: String): Long = catchIllegalValue(key) {
    Utils.byteStringAsKb(get(key, defaultValue))
  }

  /**
   * Get a size parameter as Mebibytes; throws a NoSuchElementException if it's not set. If no
   * suffix is provided then Mebibytes are assumed.
   * @throws java.util.NoSuchElementException If the size parameter is not set
   * @throws NumberFormatException If the value cannot be interpreted as Mebibytes
   */
  def getSizeAsMb(key: String): Long = catchIllegalValue(key) {
    Utils.byteStringAsMb(get(key))
  }

  /**
   * Get a size parameter as Mebibytes, falling back to a default if not set. If no
   * suffix is provided then Mebibytes are assumed.
   * @throws NumberFormatException If the value cannot be interpreted as Mebibytes
   */
  def getSizeAsMb(key: String, defaultValue: String): Long = catchIllegalValue(key) {
    Utils.byteStringAsMb(get(key, defaultValue))
  }

  /**
   * Get a size parameter as Gibibytes; throws a NoSuchElementException if it's not set. If no
   * suffix is provided then Gibibytes are assumed.
   * @throws java.util.NoSuchElementException If the size parameter is not set
   * @throws NumberFormatException If the value cannot be interpreted as Gibibytes
   */
  def getSizeAsGb(key: String): Long = catchIllegalValue(key) {
    Utils.byteStringAsGb(get(key))
  }

  /**
   * Get a size parameter as Gibibytes, falling back to a default if not set. If no
   * suffix is provided then Gibibytes are assumed.
   * @throws NumberFormatException If the value cannot be interpreted as Gibibytes
   */
  def getSizeAsGb(key: String, defaultValue: String): Long = catchIllegalValue(key) {
    Utils.byteStringAsGb(get(key, defaultValue))
  }

  /** Get a parameter as an Option */
  def getOption(key: String): Option[String] = {
    Option(settings.get(key)).orElse(getDeprecatedConfig(key, settings))
  }

  /** Get an optional value, applying variable substitution. */
  private[spark] def getWithSubstitution(key: String): Option[String] = {
    getOption(key).map(reader.substitute)
  }

  /** Get all parameters as a list of pairs */
  def getAll: Array[(String, String)] = {
    settings.entrySet().asScala.map(x => (x.getKey, x.getValue)).toArray
  }

  /**
   * Get all parameters that start with `prefix`
   */
  def getAllWithPrefix(prefix: String): Array[(String, String)] = {
    getAll.filter { case (k, v) => k.startsWith(prefix) }
      .map { case (k, v) => (k.substring(prefix.length), v) }
  }

  /**
   * Get a parameter as an integer, falling back to a default if not set
   * @throws NumberFormatException If the value cannot be interpreted as an integer
   */
  def getInt(key: String, defaultValue: Int): Int = catchIllegalValue(key) {
    getOption(key).map(_.toInt).getOrElse(defaultValue)
  }

  /**
   * Get a parameter as a long, falling back to a default if not set
   * @throws NumberFormatException If the value cannot be interpreted as a long
   */
  def getLong(key: String, defaultValue: Long): Long = catchIllegalValue(key) {
    getOption(key).map(_.toLong).getOrElse(defaultValue)
  }

  /**
   * Get a parameter as a double, falling back to a default if not ste
   * @throws NumberFormatException If the value cannot be interpreted as a double
   */
  def getDouble(key: String, defaultValue: Double): Double = catchIllegalValue(key) {
    getOption(key).map(_.toDouble).getOrElse(defaultValue)
  }

  /**
   * Get a parameter as a boolean, falling back to a default if not set
   * @throws IllegalArgumentException If the value cannot be interpreted as a boolean
   */
  def getBoolean(key: String, defaultValue: Boolean): Boolean = catchIllegalValue(key) {
    getOption(key).map(_.toBoolean).getOrElse(defaultValue)
  }

  /** Get all executor environment variables set on this SparkConf */
  def getExecutorEnv: Seq[(String, String)] = {
    getAllWithPrefix("spark.executorEnv.").toImmutableArraySeq
  }

  /**
   * Returns the Spark application id, valid in the Driver after TaskScheduler registration and
   * from the start in the Executor.
   */
  def getAppId: String = get("spark.app.id")

  /** Does the configuration contain a given parameter? */
  def contains(key: String): Boolean = {
    settings.containsKey(key) ||
      configsWithAlternatives.get(key).toSeq.flatten.exists { alt => contains(alt.key) }
  }

  private[spark] def contains(entry: ConfigEntry[_]): Boolean = contains(entry.key)

  /** Copy this object */
  override def clone: SparkConf = {
    val cloned = new SparkConf(false)
    settings.entrySet().asScala.foreach { e =>
      cloned.set(e.getKey(), e.getValue(), true)
    }
    cloned
  }

  /**
   * By using this instead of System.getenv(), environment variables can be mocked
   * in unit tests.
   */
  private[spark] def getenv(name: String): String = System.getenv(name)

  /**
   * Wrapper method for get() methods which require some specific value format. This catches
   * any [[NumberFormatException]] or [[IllegalArgumentException]] and re-raises it with the
   * incorrectly configured key in the exception message.
   */
  private def catchIllegalValue[T](key: String)(getValue: => T): T = {
    try {
      getValue
    } catch {
      case e: NumberFormatException =>
        // NumberFormatException doesn't have a constructor that takes a cause for some reason.
        throw new NumberFormatException(s"Illegal value for config key $key: ${e.getMessage}")
            .initCause(e)
      case e: IllegalArgumentException =>
        throw new IllegalArgumentException(s"Illegal value for config key $key: ${e.getMessage}", e)
    }
  }


  /**
   * Checks for illegal or deprecated config settings. Throws an exception for the former. Not
   * idempotent - may mutate this conf object to convert deprecated settings to supported ones.
   */
  private[spark] def validateSettings(): Unit = {
    if (contains("spark.local.dir")) {
      val msg = "Note that spark.local.dir will be overridden by the value set by " +
        "the cluster manager (via SPARK_LOCAL_DIRS in standalone/kubernetes and LOCAL_DIRS" +
        " in YARN)."
      logWarning(msg)
    }

    // Used by Yarn in 1.1 and before
    sys.props.get("spark.driver.libraryPath").foreach { value =>
      val warning =
        log"""
          |spark.driver.libraryPath was detected (set to '${MDC(LogKeys.CONFIG, value)}').
          |This is deprecated in Spark 1.2+.
          |
          |Please instead use: ${MDC(LogKeys.CONFIG2, DRIVER_LIBRARY_PATH.key)}
        """.stripMargin
      logWarning(warning)
    }

    // Validate spark.executor.extraJavaOptions
    Seq(EXECUTOR_JAVA_OPTIONS.key, "spark.executor.defaultJavaOptions").foreach { executorOptsKey =>
      getOption(executorOptsKey).foreach { javaOpts =>
        if (javaOpts.contains("-Dspark")) {
          val msg = s"$executorOptsKey is not allowed to set Spark options (was '$javaOpts'). " +
            "Set them directly on a SparkConf or in a properties file " +
            "when using ./bin/spark-submit."
          throw new Exception(msg)
        }
        if (javaOpts.contains("-Xmx")) {
          val msg = s"$executorOptsKey is not allowed to specify max heap memory settings " +
            s"(was '$javaOpts'). Use spark.executor.memory instead."
          throw new Exception(msg)
        }
      }
    }

    // Validate memory fractions
    for (key <- Seq(MEMORY_FRACTION.key, MEMORY_STORAGE_FRACTION.key)) {
      val value = getDouble(key, 0.5)
      if (value > 1 || value < 0) {
        throw new IllegalArgumentException(s"$key should be between 0 and 1 (was '$value').")
      }
    }

    if (contains(SUBMIT_DEPLOY_MODE)) {
      get(SUBMIT_DEPLOY_MODE) match {
        case "cluster" | "client" =>
        case e => throw new SparkException(s"${SUBMIT_DEPLOY_MODE.key} can only be " +
          "\"cluster\" or \"client\".")
      }
    }

    if (contains(CORES_MAX) && contains(EXECUTOR_CORES)) {
      val totalCores = getInt(CORES_MAX.key, 1)
      val executorCores = get(EXECUTOR_CORES)
      val leftCores = totalCores % executorCores
      if (leftCores != 0) {
        logWarning(log"Total executor cores: " +
          log"${MDC(LogKeys.NUM_EXECUTOR_CORES_TOTAL, totalCores)} " +
          log"is not divisible by cores per executor: " +
          log"${MDC(LogKeys.NUM_EXECUTOR_CORES, executorCores)}, " +
          log"the left cores: " +
          log"${MDC(LogKeys.NUM_EXECUTOR_CORES_REMAINING, leftCores)} " +
          log"will not be allocated")
      }
    }

    val encryptionEnabled = get(NETWORK_CRYPTO_ENABLED) || get(SASL_ENCRYPTION_ENABLED)
    require(!encryptionEnabled || get(NETWORK_AUTH_ENABLED),
      s"${NETWORK_AUTH_ENABLED.key} must be enabled when enabling encryption.")

    val executorTimeoutThresholdMs = get(NETWORK_TIMEOUT) * 1000
    val executorHeartbeatIntervalMs = get(EXECUTOR_HEARTBEAT_INTERVAL)
    val networkTimeout = NETWORK_TIMEOUT.key
    // If spark.executor.heartbeatInterval bigger than spark.network.timeout,
    // it will almost always cause ExecutorLostFailure. See SPARK-22754.
    require(executorTimeoutThresholdMs > executorHeartbeatIntervalMs, "The value of " +
      s"${networkTimeout}=${executorTimeoutThresholdMs}ms must be greater than the value of " +
      s"${EXECUTOR_HEARTBEAT_INTERVAL.key}=${executorHeartbeatIntervalMs}ms.")
  }

  /**
   * Return a string listing all keys and values, one per line. This is useful to print the
   * configuration out for debugging.
   */
  def toDebugString: String = {
    Utils.redact(this, getAll).sorted.map { case (k, v) => k + "=" + v }.mkString("\n")
  }

}

private[spark] object SparkConf extends Logging {

  /**
   * Maps deprecated config keys to information about the deprecation.
   *
   * The extra information is logged as a warning when the config is present in the user's
   * configuration.
   */
  private val deprecatedConfigs: Map[String, DeprecatedConfig] = {
    val configs = Seq(
      DeprecatedConfig("spark.cache.class", "0.8",
        "The spark.cache.class property is no longer being used! Specify storage levels using " +
        "the RDD.persist() method instead."),
      DeprecatedConfig("spark.yarn.user.classpath.first", "1.3",
        "Please use spark.{driver,executor}.userClassPathFirst instead."),
      DeprecatedConfig("spark.kryoserializer.buffer.mb", "1.4",
        "Please use spark.kryoserializer.buffer instead. The default value for " +
          "spark.kryoserializer.buffer.mb was previously specified as '0.064'. Fractional values " +
          "are no longer accepted. To specify the equivalent now, one may use '64k'."),
      DeprecatedConfig("spark.rpc", "2.0", "Not used anymore."),
      DeprecatedConfig("spark.scheduler.executorTaskBlacklistTime", "2.1.0",
        "Please use the new excludedOnFailure options, spark.excludeOnFailure.*"),
      DeprecatedConfig("spark.yarn.am.port", "2.0.0", "Not used anymore"),
      DeprecatedConfig("spark.executor.port", "2.0.0", "Not used anymore"),
      DeprecatedConfig("spark.rpc.numRetries", "2.2.0", "Not used anymore"),
      DeprecatedConfig("spark.rpc.retry.wait", "2.2.0", "Not used anymore"),
      DeprecatedConfig("spark.shuffle.service.index.cache.entries", "2.3.0",
        "Not used anymore. Please use spark.shuffle.service.index.cache.size"),
      DeprecatedConfig("spark.yarn.credentials.file.retention.count", "2.4.0", "Not used anymore."),
      DeprecatedConfig("spark.yarn.credentials.file.retention.days", "2.4.0", "Not used anymore."),
      DeprecatedConfig("spark.yarn.services", "3.0.0", "Feature no longer available."),
      DeprecatedConfig("spark.executor.plugins", "3.0.0",
        "Feature replaced with new plugin API. See Monitoring documentation."),
      DeprecatedConfig("spark.blacklist.enabled", "3.1.0",
        "Please use spark.excludeOnFailure.enabled"),
      DeprecatedConfig("spark.blacklist.task.maxTaskAttemptsPerExecutor", "3.1.0",
        "Please use spark.excludeOnFailure.task.maxTaskAttemptsPerExecutor"),
      DeprecatedConfig("spark.blacklist.task.maxTaskAttemptsPerNode", "3.1.0",
        "Please use spark.excludeOnFailure.task.maxTaskAttemptsPerNode"),
      DeprecatedConfig("spark.blacklist.application.maxFailedTasksPerExecutor", "3.1.0",
        "Please use spark.excludeOnFailure.application.maxFailedTasksPerExecutor"),
      DeprecatedConfig("spark.blacklist.stage.maxFailedTasksPerExecutor", "3.1.0",
        "Please use spark.excludeOnFailure.stage.maxFailedTasksPerExecutor"),
      DeprecatedConfig("spark.blacklist.application.maxFailedExecutorsPerNode", "3.1.0",
        "Please use spark.excludeOnFailure.application.maxFailedExecutorsPerNode"),
      DeprecatedConfig("spark.blacklist.stage.maxFailedExecutorsPerNode", "3.1.0",
        "Please use spark.excludeOnFailure.stage.maxFailedExecutorsPerNode"),
      DeprecatedConfig("spark.blacklist.timeout", "3.1.0",
        "Please use spark.excludeOnFailure.timeout"),
      DeprecatedConfig("spark.blacklist.application.fetchFailure.enabled", "3.1.0",
        "Please use spark.excludeOnFailure.application.fetchFailure.enabled"),
      DeprecatedConfig("spark.scheduler.blacklist.unschedulableTaskSetTimeout", "3.1.0",
        "Please use spark.scheduler.excludeOnFailure.unschedulableTaskSetTimeout"),
      DeprecatedConfig("spark.blacklist.killBlacklistedExecutors", "3.1.0",
        "Please use spark.excludeOnFailure.killExcludedExecutors"),
      DeprecatedConfig("spark.yarn.blacklist.executor.launch.blacklisting.enabled", "3.1.0",
        "Please use spark.yarn.executor.launch.excludeOnFailure.enabled")
    )

    Map(configs.map { cfg => (cfg.key -> cfg) } : _*)
  }

  /**
   * Maps a current config key to alternate keys that were used in previous version of Spark.
   *
   * The alternates are used in the order defined in this map. If deprecated configs are
   * present in the user's configuration, a warning is logged.
   *
   * TODO: consolidate it with `ConfigBuilder.withAlternative`.
   */
  private val configsWithAlternatives = Map[String, Seq[AlternateConfig]](
    EXECUTOR_USER_CLASS_PATH_FIRST.key -> Seq(
      AlternateConfig("spark.files.userClassPathFirst", "1.3")),
    UPDATE_INTERVAL_S.key -> Seq(
      AlternateConfig("spark.history.fs.update.interval.seconds", "1.4"),
      AlternateConfig("spark.history.fs.updateInterval", "1.3"),
      AlternateConfig("spark.history.updateInterval", "1.3")),
    CLEANER_INTERVAL_S.key -> Seq(
      AlternateConfig("spark.history.fs.cleaner.interval.seconds", "1.4")),
    MAX_LOG_AGE_S.key -> Seq(
      AlternateConfig("spark.history.fs.cleaner.maxAge.seconds", "1.4")),
    "spark.yarn.am.waitTime" -> Seq(
      AlternateConfig("spark.yarn.applicationMaster.waitTries", "1.3",
        // Translate old value to a duration, with 10s wait time per try.
        translation = s => s"${s.toLong * 10}s")),
    REDUCER_MAX_SIZE_IN_FLIGHT.key -> Seq(
      AlternateConfig("spark.reducer.maxMbInFlight", "1.4")),
    KRYO_SERIALIZER_BUFFER_SIZE.key -> Seq(
      AlternateConfig("spark.kryoserializer.buffer.mb", "1.4",
        translation = s => s"${(s.toDouble * 1000).toInt}k")),
    KRYO_SERIALIZER_MAX_BUFFER_SIZE.key -> Seq(
      AlternateConfig("spark.kryoserializer.buffer.max.mb", "1.4")),
    SHUFFLE_FILE_BUFFER_SIZE.key -> Seq(
      AlternateConfig("spark.shuffle.file.buffer.kb", "1.4")),
    EXECUTOR_LOGS_ROLLING_MAX_SIZE.key -> Seq(
      AlternateConfig("spark.executor.logs.rolling.size.maxBytes", "1.4")),
    IO_COMPRESSION_SNAPPY_BLOCKSIZE.key -> Seq(
      AlternateConfig("spark.io.compression.snappy.block.size", "1.4")),
    IO_COMPRESSION_LZ4_BLOCKSIZE.key -> Seq(
      AlternateConfig("spark.io.compression.lz4.block.size", "1.4")),
    "spark.streaming.fileStream.minRememberDuration" -> Seq(
      AlternateConfig("spark.streaming.minRememberDuration", "1.5")),
    "spark.yarn.max.executor.failures" -> Seq(
      AlternateConfig("spark.yarn.max.worker.failures", "1.5")),
    MEMORY_OFFHEAP_ENABLED.key -> Seq(
      AlternateConfig("spark.unsafe.offHeap", "1.6")),
    "spark.yarn.jars" -> Seq(
      AlternateConfig("spark.yarn.jar", "2.0")),
    MAX_REMOTE_BLOCK_SIZE_FETCH_TO_MEM.key -> Seq(
      AlternateConfig("spark.reducer.maxReqSizeShuffleToMem", "2.3"),
      AlternateConfig("spark.maxRemoteBlockSizeFetchToMem", "3.0")),
    LISTENER_BUS_EVENT_QUEUE_CAPACITY.key -> Seq(
      AlternateConfig("spark.scheduler.listenerbus.eventqueue.size", "2.3")),
    DRIVER_MEMORY_OVERHEAD.key -> Seq(
      AlternateConfig("spark.yarn.driver.memoryOverhead", "2.3")),
    EXECUTOR_MEMORY_OVERHEAD.key -> Seq(
      AlternateConfig("spark.yarn.executor.memoryOverhead", "2.3")),
    KEYTAB.key -> Seq(
      AlternateConfig("spark.yarn.keytab", "3.0")),
    PRINCIPAL.key -> Seq(
      AlternateConfig("spark.yarn.principal", "3.0")),
    KERBEROS_RELOGIN_PERIOD.key -> Seq(
      AlternateConfig("spark.yarn.kerberos.relogin.period", "3.0")),
    KERBEROS_FILESYSTEMS_TO_ACCESS.key -> Seq(
      AlternateConfig("spark.yarn.access.namenodes", "2.2"),
      AlternateConfig("spark.yarn.access.hadoopFileSystems", "3.0")),
    "spark.kafka.consumer.cache.capacity" -> Seq(
      AlternateConfig("spark.sql.kafkaConsumerCache.capacity", "3.0")),
    MAX_EXECUTOR_FAILURES.key -> Seq(
      AlternateConfig("spark.yarn.max.executor.failures", "3.5")),
    EXECUTOR_ATTEMPT_FAILURE_VALIDITY_INTERVAL_MS.key -> Seq(
      AlternateConfig("spark.yarn.executor.failuresValidityInterval", "3.5"))
  )

  /**
   * A view of `configsWithAlternatives` that makes it more efficient to look up deprecated
   * config keys.
   *
   * Maps the deprecated config name to a 2-tuple (new config name, alternate config info).
   */
  private val allAlternatives: Map[String, (String, AlternateConfig)] = {
    configsWithAlternatives.keys.flatMap { key =>
      configsWithAlternatives(key).map { cfg => (cfg.key -> (key -> cfg)) }
    }.toMap
  }

  /**
   * Return whether the given config should be passed to an executor on start-up.
   *
   * Certain authentication configs are required from the executor when it connects to
   * the scheduler, while the rest of the spark configs can be inherited from the driver later.
   */
  def isExecutorStartupConf(name: String): Boolean = {
    (name.startsWith("spark.auth") && name != SecurityManager.SPARK_AUTH_SECRET_CONF) ||
    name.startsWith("spark.rpc") ||
    name.startsWith("spark.network") ||
    // We need SSL configs to propagate as they may be needed for RPCs.
    // Passwords are propagated separately though.
    (name.startsWith("spark.ssl") && !name.contains("Password")) ||
    isSparkPortConf(name)
  }

  /**
   * Return true if the given config matches either `spark.*.port` or `spark.port.*`.
   */
  def isSparkPortConf(name: String): Boolean = {
    (name.startsWith("spark.") && name.endsWith(".port")) || name.startsWith("spark.port.")
  }

  /**
   * Looks for available deprecated keys for the given config option, and return the first
   * value available.
   */
  def getDeprecatedConfig(key: String, conf: JMap[String, String]): Option[String] = {
    configsWithAlternatives.get(key).flatMap { alts =>
      alts.collectFirst { case alt if conf.containsKey(alt.key) =>
        val value = conf.get(alt.key)
        if (alt.translation != null) alt.translation(value) else value
      }
    }
  }

  /**
   * Logs a warning message if the given config key is deprecated.
   */
  def logDeprecationWarning(key: String): Unit = {
    deprecatedConfigs.get(key).foreach { cfg =>
      logWarning(
        log"The configuration key '${MDC(LogKeys.CONFIG, key)}' has been deprecated " +
          log"as of Spark ${MDC(LogKeys.CONFIG_VERSION, cfg.version)} and " +
          log"may be removed in the future. " +
          log"${MDC(LogKeys.CONFIG_DEPRECATION_MESSAGE, cfg.deprecationMessage)}")
      return
    }

    allAlternatives.get(key).foreach { case (newKey, cfg) =>
      logWarning(
        log"The configuration key '${MDC(LogKeys.CONFIG, key)}' " +
          log"has been deprecated as of " +
          log"Spark ${MDC(LogKeys.CONFIG_VERSION, cfg.version)} and " +
          log"may be removed in the future. Please use the new key " +
          log"'${MDC(LogKeys.CONFIG_KEY_UPDATED, newKey)}' instead.")
      return
    }
  }

  /**
   * Holds information about keys that have been deprecated and do not have a replacement.
   *
   * @param key The deprecated key.
   * @param version Version of Spark where key was deprecated.
   * @param deprecationMessage Message to include in the deprecation warning.
   */
  private case class DeprecatedConfig(
      key: String,
      version: String,
      deprecationMessage: String)

  /**
   * Information about an alternate configuration key that has been deprecated.
   *
   * @param key The deprecated config key.
   * @param version The Spark version in which the key was deprecated.
   * @param translation A translation function for converting old config values into new ones.
   */
  private case class AlternateConfig(
      key: String,
      version: String,
      translation: String => String = null)
}
