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

package org.apache.spark.deploy.security

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.scalatest.Matchers

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.internal.config.STAGING_DIR

class HadoopFSDelegationTokenProviderSuite extends SparkFunSuite with Matchers {
  test("hadoopFSsToAccess should return defaultFS even if not configured") {
    val sparkConf = new SparkConf()
    val defaultFS = "hdfs://localhost:8020"
    val statingDir = "hdfs://localhost:8021"
    sparkConf.set("spark.master", "yarn-client")
    sparkConf.set(STAGING_DIR, statingDir)
    val hadoopConf = new Configuration()
    hadoopConf.set("fs.defaultFS", defaultFS)
    val expected = Set(
      new Path(defaultFS).getFileSystem(hadoopConf),
      new Path(statingDir).getFileSystem(hadoopConf)
    )
    val result = HadoopFSDelegationTokenProvider.hadoopFSsToAccess(sparkConf, hadoopConf)
    result should be (expected)
  }

  test("SPARK-24149: retrieve all namenodes from HDFS") {
    val sparkConf = new SparkConf()
    sparkConf.set("spark.master", "yarn-client")
    val basicFederationConf = new Configuration()
    basicFederationConf.set("fs.defaultFS", "hdfs://localhost:8020")
    basicFederationConf.set("dfs.nameservices", "ns1,ns2")
    basicFederationConf.set("dfs.namenode.rpc-address.ns1", "localhost:8020")
    basicFederationConf.set("dfs.namenode.rpc-address.ns2", "localhost:8021")
    val basicFederationExpected = Set(
      new Path("hdfs://localhost:8020").getFileSystem(basicFederationConf),
      new Path("hdfs://localhost:8021").getFileSystem(basicFederationConf))
    val basicFederationResult = HadoopFSDelegationTokenProvider.hadoopFSsToAccess(
      sparkConf, basicFederationConf)
    basicFederationResult should be (basicFederationExpected)

    // when viewfs is enabled, namespaces are handled by it, so we don't need to take care of them
    val viewFsConf = new Configuration()
    viewFsConf.addResource(basicFederationConf)
    viewFsConf.set("fs.defaultFS", "viewfs://clusterX/")
    viewFsConf.set("fs.viewfs.mounttable.clusterX.link./home", "hdfs://localhost:8020/")
    val viewFsExpected = Set(new Path("viewfs://clusterX/").getFileSystem(viewFsConf))
    HadoopFSDelegationTokenProvider
      .hadoopFSsToAccess(sparkConf, viewFsConf) should be (viewFsExpected)

    // invalid config should not throw NullPointerException
    val invalidFederationConf = new Configuration()
    invalidFederationConf.addResource(basicFederationConf)
    invalidFederationConf.unset("dfs.namenode.rpc-address.ns2")
    val invalidFederationExpected = Set(
      new Path("hdfs://localhost:8020").getFileSystem(invalidFederationConf))
    val invalidFederationResult = HadoopFSDelegationTokenProvider.hadoopFSsToAccess(
      sparkConf, invalidFederationConf)
    invalidFederationResult should be (invalidFederationExpected)

    // no namespaces defined, ie. old case
    val noFederationConf = new Configuration()
    noFederationConf.set("fs.defaultFS", "hdfs://localhost:8020")
    val noFederationExpected = Set(
      new Path("hdfs://localhost:8020").getFileSystem(noFederationConf))
    val noFederationResult = HadoopFSDelegationTokenProvider.hadoopFSsToAccess(sparkConf,
      noFederationConf)
    noFederationResult should be (noFederationExpected)

    // federation and HA enabled
    val federationAndHAConf = new Configuration()
    federationAndHAConf.set("fs.defaultFS", "hdfs://clusterXHA")
    federationAndHAConf.set("dfs.nameservices", "clusterXHA,clusterYHA")
    federationAndHAConf.set("dfs.ha.namenodes.clusterXHA", "x-nn1,x-nn2")
    federationAndHAConf.set("dfs.ha.namenodes.clusterYHA", "y-nn1,y-nn2")
    federationAndHAConf.set("dfs.namenode.rpc-address.clusterXHA.x-nn1", "localhost:8020")
    federationAndHAConf.set("dfs.namenode.rpc-address.clusterXHA.x-nn2", "localhost:8021")
    federationAndHAConf.set("dfs.namenode.rpc-address.clusterYHA.y-nn1", "localhost:8022")
    federationAndHAConf.set("dfs.namenode.rpc-address.clusterYHA.y-nn2", "localhost:8023")
    federationAndHAConf.set("dfs.client.failover.proxy.provider.clusterXHA",
      "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider")
    federationAndHAConf.set("dfs.client.failover.proxy.provider.clusterYHA",
      "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider")

    val federationAndHAExpected = Set(
      new Path("hdfs://clusterXHA").getFileSystem(federationAndHAConf),
      new Path("hdfs://clusterYHA").getFileSystem(federationAndHAConf))
    val federationAndHAResult = HadoopFSDelegationTokenProvider.hadoopFSsToAccess(
      sparkConf, federationAndHAConf)
    federationAndHAResult should be (federationAndHAExpected)
  }
}
