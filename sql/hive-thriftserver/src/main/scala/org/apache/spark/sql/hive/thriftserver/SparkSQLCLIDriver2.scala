package org.apache.spark.sql.hive.thriftserver

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.sql.hive.HiveUtils
import org.apache.spark.sql.hive.security.HiveDelegationTokenProvider

import scala.collection.JavaConverters._




private[hive] object SparkSQLCLIDriver2 extends Logging {
    def main(args: Array[String]): Unit = {
        val sparkSQLArgs = SparkSQLCLIArguments(args)
        sparkSQLArgs.parse()

        val sparkConf = new SparkConf(loadDefaults = true)
        sparkConf.setAll(sparkSQLArgs.getSparkConfs)
        val hadoopConf = SparkHadoopUtil.get.newConfiguration(sparkConf)
        val extraConfigs = HiveUtils.formatTimeVarsForHiveClient(hadoopConf)

        println(hadoopConf.iterator().asScala)

        val tokenProvider = new HiveDelegationTokenProvider()
        if (tokenProvider.delegationTokensRequired(sparkConf, hadoopConf)) {
            val credentials = new Credentials()
            tokenProvider.obtainDelegationTokens(hadoopConf, sparkConf, credentials)
            UserGroupInformation.getCurrentUser.addCredentials(credentials)
        }

        // In SparkSQL CLI, we may want to use jars augmented by hiveconf
        // hive.aux.jars.path, here we add jars augmented by hiveconf to
        // Spark's SessionResourceLoader to obtain these jars.
        val auxJars = HiveConf.getVar(conf, HiveConf.ConfVars.HIVEAUXJARS)
        if (StringUtils.isNotBlank(auxJars)) {
            val resourceLoader = SparkSQLEnv.sqlContext.sessionState.resourceLoader
            StringUtils.split(auxJars, ",").foreach(resourceLoader.addJar)
        }


    }
}

private[hive] class SparkSQLCLIDriver2 extends Logging {

}