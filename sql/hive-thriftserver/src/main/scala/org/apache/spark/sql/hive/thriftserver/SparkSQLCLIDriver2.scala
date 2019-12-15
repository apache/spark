package org.apache.spark.sql.hive.thriftserver

import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.sql.hive.HiveUtils
import org.apache.spark.sql.hive.security.HiveDelegationTokenProvider

import scala.collection.JavaConverters._

private[hive] object SparkSQLCLIDriver2 extends Logging with App {

  val sparkSQLArgs = SparkSQLCLIArguments(args)
  sparkSQLArgs.parse()

  val sparkConf = new SparkConf(loadDefaults = true)
  val hadoopConf = SparkHadoopUtil.get.newConfiguration(sparkConf)
  val extraConfigs = HiveUtils.formatTimeVarsForHiveClient(hadoopConf)

  val sparkEnv: SparkSQLEnv2 = SparkSQLEnv2(sparkConf)

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

  val auxJars: Option[String] = sparkSQLArgs
    .getHiveConf("hive.aux.jars.path")
    .orElse(allConf.get("hive.aux.jars.path"))

  if (auxJars.nonEmpty) {
    val resourceLoader = sparkEnv.sqlContext.sessionState.resourceLoader
    auxJars.get.split(",").foreach(resourceLoader.addJar)
  }

  // Adding Hive configs (--hiveconf) to the spark session.
  sparkSQLArgs.getHiveConfigs.foreach {
    case (k, v) =>
      sparkEnv.sqlContext.setConf(k, v)
  }

  // Sets current database, from --database.
  val currentDB = sparkSQLArgs.getCurrentDatabase.getOrElse("default")
  sparkEnv.sqlContext.sessionState.catalog
    .setCurrentDatabase(currentDB)

  val driver = SparkSQLDriver2(sparkEnv.sqlContext, hadoopConf)

  new SparkSQLCLIDriver2().printMasterAndAppId(sparkEnv)

  // Executing init files, if any, (-i) first, after applying settings.
  sparkSQLArgs.getInitFile.foreach { initFile =>
    driver.processFile(initFile)
  }

  // Executing files, if any, (-f|--files), and exiting.
  sparkSQLArgs.getFile.foreach { file =>
    driver.processFile(file)
    sparkEnv.stop()
    sys.exit(0)
  }

  @scala.annotation.tailrec
  def readLines(previousLine: List[String] = Nil): Unit = {
    val currentDB = sparkEnv.sqlContext.sessionState.catalog.getCurrentDatabase

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
        sparkEnv.stop()
        sys.exit(1)
    }
  }

  readLines()

  sparkEnv.stop()

}

private[hive] class SparkSQLCLIDriver2 extends Logging {

  private val console = ThriftserverShimUtils.getConsole

  def printMasterAndAppId(sparkEnv: SparkSQLEnv2): Unit = {
    val master = sparkEnv.sparkContext.master
    val appId = sparkEnv.sparkContext.applicationId
    console.printInfo(s"Spark master: $master, Application Id: $appId")
  }

}
