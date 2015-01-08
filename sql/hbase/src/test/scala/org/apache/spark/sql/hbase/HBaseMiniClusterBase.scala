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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{Logging, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Suite}

class HBaseMiniClusterBase extends FunSuite with BeforeAndAfterAll with Logging { self: Suite =>
  val NMasters = 1
  val NRegionServers = 2

  @transient var sc: SparkContext = null
  @transient var cluster: MiniHBaseCluster = null
  @transient var config: Configuration = null
  @transient var hbc: HBaseSQLContext = null
  @transient var testUtil: HBaseTestingUtility = null

  def sparkContext: SparkContext = sc

  override def beforeAll(): Unit = {
    sc = new SparkContext("local", "hbase sql test")
    testUtil = new HBaseTestingUtility
    cluster = testUtil.startMiniCluster(NMasters, NRegionServers)
    config = testUtil.getConfiguration
    hbc = new HBaseSQLContext(sc)
    hbc.optConfiguration = Some(config)
  }

  test("test whether minicluster work") {
    val hbaseAdmin = new HBaseAdmin(config)
    logInfo(s"1: ${hbaseAdmin.tableExists("wf")}")

    val desc = new HTableDescriptor(TableName.valueOf("wf"))
    val family = Bytes.toBytes("fam")
    val hcd = new HColumnDescriptor(family)
      .setMaxVersions(10)
      .setTimeToLive(1)
    desc.addFamily(hcd)

    hbaseAdmin.createTable(desc)
    logInfo(s"2: ${hbaseAdmin.tableExists("wf")}")
  }

  override def afterAll() = {
    sc.stop()
    cluster.shutdown()
  }
}
