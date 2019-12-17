package org.apache.spark.sql.hive.thriftserver

import java.io.PrintStream
import java.nio.charset.StandardCharsets.UTF_8

import org.apache.spark.internal.Logging
import org.apache.spark.sql.hive.client.HiveClient
import org.apache.spark.sql.hive.{HiveExternalCatalog, HiveUtils}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.util.Utils
import org.apache.spark.{SparkConf, SparkContext}

private[hive] case class SparkSQLEnv(sparkConf: SparkConf) extends Logging {
  // If user doesn't specify the appName, we want to get [SparkSQL::localHostName] instead of
  // the default appName [SparkSQLCLIDriver] in cli or beeline.
  private val maybeAppName: Option[String] = sparkConf
    .getOption("spark.app.name")
    .filterNot(_ == classOf[SparkSQLCLIDriver].getName)
    .filterNot(_ == classOf[HiveThriftServer2].getName)

  sparkConf
    .setAppName(maybeAppName.getOrElse(s"SparkSQL::${Utils.localHostName()}"))

  val sparkSession: SparkSession =
    SparkSession.builder.config(sparkConf).enableHiveSupport().getOrCreate()
  val sparkContext: SparkContext = sparkSession.sparkContext
  val sqlContext: SQLContext = sparkSession.sqlContext

  // SPARK-29604: force initialization of the session state with the Spark class loader,
  // instead of having it happen during the initialization of the Hive client (which may use a
  // different class loader).
  sparkSession.sessionState

  val metadataHive: HiveClient =
    sparkSession.sharedState.externalCatalog.unwrapped.asInstanceOf[HiveExternalCatalog].client
  metadataHive.setOut(new PrintStream(System.out, true, UTF_8.name()))
  metadataHive.setInfo(new PrintStream(System.err, true, UTF_8.name()))
  metadataHive.setError(new PrintStream(System.err, true, UTF_8.name()))
  sparkSession.conf.set(HiveUtils.FAKE_HIVE_VERSION.key, HiveUtils.builtinHiveVersion)

  /** Cleans up and shuts down the Spark SQL environments. */
  def stop(): Unit = {
    logDebug("Shutting down Spark SQL Environment")
    sparkContext.stop()
  }

}
