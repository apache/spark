
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

package org.apache.spark.sql.hbase

import java.util.{Date, Random}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.{HBaseConfiguration, HBaseTestingUtility, MiniHBaseCluster}
import org.apache.log4j.Logger
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAllConfigMap, ConfigMap, FunSuite, Suite}

abstract class HBaseIntegrationTestBase(useMiniCluster: Boolean = true,
                                        nRegionServers: Int = 2,
                                        nDataNodes: Int = 2,
                                        nMasters: Int = 1)
  extends FunSuite with BeforeAndAfterAllConfigMap with Logging {
  self: Suite =>

  @transient var sc: SparkContext = _
  @transient var cluster: MiniHBaseCluster = null
  @transient var config: Configuration = null
  @transient var hbaseAdmin: HBaseAdmin = null
  @transient var hbc: HBaseSQLContext = null
  @transient var catalog: HBaseCatalog = null
  @transient var testUtil: HBaseTestingUtility = null
  @transient private val logger = Logger.getLogger(getClass.getName)

  def sparkContext: SparkContext = sc

  val startTime = (new Date).getTime
  val sparkUiPort = 0xc000 + new Random().nextInt(0x3f00)
  logInfo(s"SparkUIPort = $sparkUiPort\n")

  //  def simpleSetupShutdown() {
  //      testUtil = new HBaseTestingUtility
  //      config = testUtil.getConfiguration
  //      testUtil.startMiniCluster(nMasters, nRegionServers, nDataNodes)
  //      testUtil.shutdownMiniCluster()
  //  }
  //

  val useMiniClusterInt = useMiniCluster // false

  val WorkDirProperty = "test.build.data.basedirectory"
  val DefaultWorkDir = "/tmp/minihbase"



    val workDir = System.getProperty(WorkDirProperty, DefaultWorkDir)
    System.setProperty(WorkDirProperty, workDir)

    logInfo(s"useMiniCluster=$useMiniClusterInt workingDir ($WorkDirProperty})=$workDir\n")
    if (useMiniClusterInt) {
      logger.debug(s"Spin up hbase minicluster w/ $nMasters mast, $nRegionServers RS, $nDataNodes dataNodes")
      testUtil = new HBaseTestingUtility
      config = testUtil.getConfiguration
    } else {
      config = HBaseConfiguration.create
    }

    val sconf = new SparkConf()
    if (useMiniClusterInt) {
      config.set("dfs.client.socket-timeout", "480000")
      config.set("dfs.datanode.socket.write.timeout", "480000")
      config.set("zookeeper.session.timeout", "480000")
      config.set("zookeeper.minSessionTimeout", "10")
      config.set("zookeeper.tickTime", "10")
      config.set("hbase.rpc.timeout", "480000")
      config.set("ipc.client.connect.timeout", "480000")
      config.set("dfs.namenode.stale.datanode.interval", "480000")
      config.set("hbase.rpc.shortoperation.timeout", "480000")
      config.set("hbase.master.port", "50001")
      config.set("hbase.master.info.port", "50002")
      config.set("hbase.regionserver.port", "50001")
      config.set("hbase.regionserver.info.port", "50003")
      config.set("hbase.regionserver.thrift.port", "50004")
      config.set("hbase.rest.port", "50005")
      config.set("hbase.rest.info.port", "50006")
      config.set("hbase.thrift.info.port", "50007")

      cluster = testUtil.startMiniCluster(nMasters, nRegionServers, nDataNodes)
      logInfo(s"Started HBaseMiniCluster with region servers = ${cluster.countServedRegions}")

      // Need to retrieve zkPort AFTER mini cluster is started
      val zkPort = config.get("hbase.zookeeper.property.clientPort")
      logInfo(s"After testUtil.getConfiguration the hbase.zookeeper.quorum="
        + s"${config.get("hbase.zookeeper.quorum")} port=$zkPort")

      // Inject the zookeeper port/quorum obtained from the HBaseMiniCluster
      // into the SparkConf.
      // The motivation: the SparkContext searches the SparkConf values for entries
      // that start with "spark.hadoop" and then copies those values to the
      // sparkContext.hadoopConfiguration (after stripping the "spark.hadoop" from the key/name)
      sconf.set("spark.hadoop.hbase.zookeeper.property.clientPort", zkPort)
      sconf.set("spark.hadoop.hbase.zookeeper.quorum",
        "%s:%s".format(config.get("hbase.zookeeper.quorum"), zkPort))
      // Do not use the default ui port: helps avoid BindException's
      sconf.set("spark.ui.port", sparkUiPort.toString)
      //      sconf.set("spark.hadoop.hbase.regionserver.info.port", "-1")
      //      sconf.set("spark.hadoop.hbase.master.info.port", "-1")
      //    // Increase the various timeout's to allow for debugging/breakpoints. If we simply
      //    // leave default values then ZK connection timeouts tend to occur
      sconf.set("spark.hadoop.dfs.client.socket-timeout", "480000")
      sconf.set("spark.hadoop.dfs.datanode.socket.write.timeout", "480000")
      sconf.set("spark.hadoop.zookeeper.session.timeout", "480000")
      sconf.set("spark.hadoop.zookeeper.minSessionTimeout", "10")
      sconf.set("spark.hadoop.zookeeper.tickTime", "10")
      sconf.set("spark.hadoop.hbase.rpc.timeout", "480000")
      sconf.set("spark.hadoop.ipc.client.connect.timeout", "480000")
      sconf.set("spark.hadoop.dfs.namenode.stale.datanode.interval", "480000")
      sconf.set("spark.hadoop.hbase.rpc.shortoperation.timeout", "480000")
      sconf.set("spark.hadoop.hbase.master.port", "50001")
      sconf.set("spark.hadoop.hbase.master.info.port", "50002")
      sconf.set("spark.hadoop.hbase.regionserver.port", "50001")
      sconf.set("spark.hadoop.hbase.regionserver.info.port", "50003")
      sconf.set("spark.hadoop.hbase.regionserver.thrift.port", "50004")
      sconf.set("spark.hadoop.hbase.rest.port", "50005")
      sconf.set("spark.hadoop.hbase.rest.info.port", "50006")
      sconf.set("spark.hadoop.hbase.thrift.info.port", "50007")

      hbaseAdmin = testUtil.getHBaseAdmin
    } else {
      hbaseAdmin = new HBaseAdmin(config)
    }

    sc = new SparkContext("local[2]", "TestSQLContext", sconf)

    hbc = new HBaseSQLContext(sc)
    hbc.optConfiguration = Some(config)

    //        hbc.catalog.hBaseAdmin = hbaseAdmin
    logger.debug(s"In testbase: HBaseAdmin.configuration zkPort="
      + s"${hbaseAdmin.getConfiguration.get("hbase.zookeeper.property.clientPort")}")

  override protected def afterAll(configMap: ConfigMap): Unit = {
    var msg = s"Test ${getClass.getName} completed at ${(new java.util.Date).toString} duration=${((new java.util.Date).getTime - startTime) / 1000}"
    logger.info(msg)
    logInfo(msg)
    try {
      hbc.sparkContext.stop()
    } catch {
      case e: Throwable =>
        logger.error(s"Exception shutting down sparkContext: ${e.getMessage}")
    }
    hbc = null
    msg = "HBaseSQLContext was shut down"

    try {
      testUtil.cleanupTestDir
      testUtil.shutdownMiniCluster()
    } catch {
      case e: Throwable =>
        logger.error(s"Exception shutting down HBaseMiniCluster: ${e.getMessage}")
    }
  }
}