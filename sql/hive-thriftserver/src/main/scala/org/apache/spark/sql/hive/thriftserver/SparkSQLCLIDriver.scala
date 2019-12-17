package org.apache.spark.sql.hive.thriftserver

import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.sql.hive.HiveUtils
import org.apache.spark.sql.hive.security.HiveDelegationTokenProvider

import scala.collection.JavaConverters._

private[hive] object SparkSQLCLIDriver extends Logging with App {


  val sparkSQLArgs = SparkSQLCLIArguments(args)
  val sparkConf = new SparkConf(loadDefaults = true)

  sparkSQLArgs
    .getSparkConfigs
    .foreach {
      case (k, v) =>
      sparkConf.set(k, v)
    }

  sparkSQLArgs
    .getHiveConfigs
    .filter {case (k,v) => k.startsWith("hive.")}
    .foreach {
    case (k, v) =>
      sparkConf.setIfMissing("spark.hadoop." + k, v)
    }

  println(sparkConf.toDebugString)

  val hadoopConf = SparkHadoopUtil.get.newConfiguration(sparkConf)
  val extraConfigs = HiveUtils.formatTimeVarsForHiveClient(hadoopConf)

  SparkSQLEnv.init(sparkConf)

  val allConf: Map[String, String] = (hadoopConf
    .iterator()
    .asScala
    .map(kv => kv.getKey -> kv.getValue)
    ++ sparkConf.getAll.toMap ++ extraConfigs).toMap

  val tokenProvider = new HiveDelegationTokenProvider()
  if (tokenProvider.delegationTokensRequired(sparkConf, hadoopConf)) {
    val credentials = new Credentials()
    tokenProvider.obtainDelegationTokens(hadoopConf, sparkConf, credentials)
    UserGroupInformation.getCurrentUser.addCredentials(credentials)
  }


  // TODO: add hive.aux.jars.path as an option to local HiveUtils
  val auxJars: Option[String] = sparkSQLArgs
    .getHiveConf("hive.aux.jars.path")
    .orElse(allConf.get("hive.aux.jars.path"))

  if (auxJars.nonEmpty) {
    val resourceLoader = SparkSQLEnv.sqlContext.sessionState.resourceLoader
    auxJars.get.split(",").foreach(resourceLoader.addJar)
  }

  // Adding Hive configs (--hiveconf) to the spark session.
  sparkSQLArgs.getHiveConfigs.foreach {
    case (k, v) =>
      SparkSQLEnv.sqlContext.setConf(k, v)
  }

  // Sets current database, from --database.
  val currentDB = sparkSQLArgs.getDatabase.getOrElse("default")
  SparkSQLEnv.sqlContext.sessionState.catalog
    .setCurrentDatabase(currentDB)

  val driver = SparkSQLDriver(SparkSQLEnv.sqlContext, hadoopConf)

  val master = SparkSQLEnv.sparkContext.master
  val appId = SparkSQLEnv.sparkContext.applicationId
  println(s"Spark master: $master, Application Id: $appId")

  // Executing init files, if any, (-i) first, after applying settings.
  sparkSQLArgs.getInitFile.foreach { initFile =>
    driver.processFile(initFile)
  }

  sparkSQLArgs.getQueryString.foreach { query =>
    driver.processLine(query)
    SparkSQLEnv.stop()
    sys.exit(0)
  }

  // Executing files, if any, (-f|--files), and exiting.
  sparkSQLArgs.getFile.foreach { file =>
    driver.processFile(file)
    SparkSQLEnv.stop()
    sys.exit(0)
  }

  /**
   *
   * @param previousLine
   */
  @scala.annotation.tailrec
  def readLines(previousLine: List[String] = Nil): Unit = {
    val currentDB = SparkSQLEnv.sqlContext.sessionState.catalog.getCurrentDatabase

    val promptTemplate = s"spark-sql: ($currentDB)> "
    val prompt = if (previousLine.isEmpty) {
      promptTemplate
    } else {
      val spaces = promptTemplate.length - 2
      " " * spaces + "> "
    }
    scala.io.StdIn.readLine(prompt) match {
      case s if s.trim == "" =>
        readLines(previousLine)
      case s if s.startsWith("--") =>
        readLines(previousLine)
      case s if s.endsWith(";") =>
        driver.processLine((previousLine :+ s).mkString("\\\\"))
        readLines()
      case s: String =>
        readLines(previousLine :+ s)
      case _ =>
        SparkSQLEnv.stop()
        sys.exit(1)
    }
  }

  readLines()
}

private[hive] class SparkSQLCLIDriver