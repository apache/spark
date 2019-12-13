package org.apache.spark.sql.hive.thriftserver


import java.util.Locale

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

    val allConfs: Map[String, String] = (
      hadoopConf
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


//    val auxJars = sparkSQLArgs.getHiveConf(HiveConf.ConfVars.HIVEAUXJARS)
    val auxJars: Option[String] = sparkSQLArgs.getHiveConf("hive.aux.jars.path")
      .orElse(allConfs.get("hive.aux.jars.path"))

    if (auxJars.nonEmpty) {
        val resourceLoader = SparkSQLEnv.sqlContext.sessionState.resourceLoader
        auxJars.get.split(",").foreach(resourceLoader.addJar)
    }

    sparkSQLArgs.getHiveConfs.foreach {
        case (k, v) =>
        SparkSQLEnv.sqlContext.setConf(k, v)
    }

    val cli = new SparkSQLCLIDriver2

    cli.processCmd("source t.q")


}

private[hive] class SparkSQLCLIDriver2 extends Logging {

    private val console = ThriftserverShimUtils.getConsole

    def printMasterAndAppId(): Unit = {
        val master = SparkSQLEnv.sparkContext.master
        val appId = SparkSQLEnv.sparkContext.applicationId
        console.printInfo(s"Spark master: $master, Application Id: $appId")
    }

    def processFile(file: String): Int = {
        SparkSQLDriver2(SparkSQLEnv.sqlContext).processFile(file)
        1
        //
    }

    def processShellCmd(cmd: String): Int = {
        1
        //
    }

    def processLine(cmd: String): Int = {
        1
        //
    }

    def processCmd(cmd: String): Int = {
        val cmd_cleaned = cmd.trim.toLowerCase(Locale.ROOT)
        val tokens = cmd_cleaned.split("\\s+").toList

        tokens match {
            case ("quit" | "exit") :: tail =>
                System.exit(0)
                0
            case "source" :: filepath :: tail =>
                processFile(filepath)
            case s :: tail if s startsWith "!" =>
                processShellCmd(cmd_cleaned.tail)
            case _ =>
                processLine(cmd_cleaned)
        }
    }
}
