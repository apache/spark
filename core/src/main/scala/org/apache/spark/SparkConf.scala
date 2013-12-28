package org.apache.spark

import scala.collection.JavaConverters._
import scala.collection.mutable.HashMap

import com.typesafe.config.ConfigFactory

/**
 * Configuration for a Spark application. Used to set various Spark parameters as key-value pairs.
 *
 * Most of the time, you would create a SparkConf object with `new SparkConf()`, which will load
 * values from both the `spark.*` Java system properties and any `spark.conf` on your application's
 * classpath (if it has one). In this case, system properties take priority over `spark.conf`, and
 * any parameters you set directly on the `SparkConf` object take priority over both of those.
 *
 * For unit tests, you can also call `new SparkConf(false)` to skip loading external settings and
 * get the same configuration no matter what is on the classpath.
 *
 * @param loadDefaults whether to load values from the system properties and classpath
 */
class SparkConf(loadDefaults: Boolean) extends Serializable with Cloneable {

  /** Create a SparkConf that loads defaults from system properties and the classpath */
  def this() = this(true)

  private val settings = new HashMap[String, String]()

  if (loadDefaults) {
    val typesafeConfig = ConfigFactory.systemProperties()
      .withFallback(ConfigFactory.parseResources("spark.conf"))
    for (e <- typesafeConfig.entrySet().asScala if e.getKey.startsWith("spark.")) {
      settings(e.getKey) = e.getValue.unwrapped.toString
    }
  }

  /** Set a configuration variable. */
  def set(key: String, value: String): SparkConf = {
    settings(key) = value
    this
  }

  /**
   * The master URL to connect to, such as "local" to run locally with one thread, "local[4]" to
   * run locally with 4 cores, or "spark://master:7077" to run on a Spark standalone cluster.
   */
  def setMaster(master: String): SparkConf = {
    if (master != null) {
      settings("spark.master") = master
    }
    this
  }

  /** Set a name for your application. Shown in the Spark web UI. */
  def setAppName(name: String): SparkConf = {
    if (name != null) {
      settings("spark.appName") = name
    }
    this
  }

  /** Set JAR files to distribute to the cluster. */
  def setJars(jars: Seq[String]): SparkConf = {
    if (!jars.isEmpty) {
      settings("spark.jars") = jars.mkString(",")
    }
    this
  }

  /** Set JAR files to distribute to the cluster. (Java-friendly version.) */
  def setJars(jars: Array[String]): SparkConf = {
    if (!jars.isEmpty) {
      settings("spark.jars") = jars.mkString(",")
    }
    this
  }

  /** Set an environment variable to be used when launching executors for this application. */
  def setExecutorEnv(variable: String, value: String): SparkConf = {
    settings("spark.executorEnv." + variable) = value
    this
  }

  /** Set multiple environment variables to be used when launching executors. */
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
    for ((k, v) <- variables) {
      setExecutorEnv(k, v)
    }
    this
  }

  /**
   * Set the location where Spark is installed on worker nodes. This is only needed on Mesos if
   * you are not using `spark.executor.uri` to disseminate the Spark binary distribution.
   */
  def setSparkHome(home: String): SparkConf = {
    if (home != null) {
      settings("spark.home") = home
    }
    this
  }

  /** Set multiple parameters together */
  def setAll(settings: Traversable[(String, String)]) = {
    this.settings ++= settings
    this
  }

  /** Set a parameter if it isn't already configured */
  def setIfMissing(key: String, value: String): SparkConf = {
    if (!settings.contains(key)) {
      settings(key) = value
    }
    this
  }

  /** Get a parameter; throws an exception if it's not set */
  def get(key: String): String = {
    settings(key)
  }

  /** Get a parameter as an Option */
  def getOption(key: String): Option[String] = {
    settings.get(key)
  }

  /** Get all parameters as a list of pairs */
  def getAll: Seq[(String, String)] = settings.clone().toSeq

  /** Get a parameter, falling back to a default if not set */
  def getOrElse(k: String, defaultValue: String): String = {
    settings.getOrElse(k, defaultValue)
  }

  /** Get all executor environment variables set on this SparkConf */
  def getExecutorEnv: Seq[(String, String)] = {
    val prefix = "spark.executorEnv."
    getAll.filter(pair => pair._1.startsWith(prefix))
          .map(pair => (pair._1.substring(prefix.length), pair._2))
  }

  /** Does the configuration contain a given parameter? */
  def contains(key: String): Boolean = settings.contains(key)

  /** Copy this object */
  override def clone: SparkConf = {
    new SparkConf(false).setAll(settings)
  }
}
