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
package org.apache.spark.deploy.k8s.integrationtest

import org.scalatest.concurrent.Eventually

import org.apache.spark.deploy.k8s.integrationtest.KerberosTestSuite._
import org.apache.spark.deploy.k8s.integrationtest.KubernetesSuite.{k8sTestTag, INTERVAL, TIMEOUT}
import org.apache.spark.deploy.k8s.integrationtest.kerberos._

private[spark] trait KerberosTestSuite { k8sSuite: KubernetesSuite =>

  test("Secure HDFS test with HDFS keytab (Cluster Mode)", k8sTestTag) {
    val kubernetesClient = kubernetesTestComponents.kubernetesClient

    // Launches single-noded psuedo-distributed kerberized hadoop cluster
    kerberizedHadoopClusterLauncher.launchKerberizedCluster(kerberosUtils)

    // Launches Kerberos test
    val driverWatcherCache = new KerberosDriverWatcherCache(
      kerberosUtils,
      Map("spark-app-locator" -> appLocator))
    driverWatcherCache.deploy(kerberosUtils.getKerberosTest(
      containerLocalSparkDistroExamplesJar,
      HDFS_TEST_CLASS,
      appLocator,
      KERB_YAML_LOCATION))
    driverWatcherCache.stopWatch()

    val expectedLogOnCompletion = Seq(
      "File contents: [Michael, 29],[Andy, 30],[Justin, 19]",
      "Returned length(s) of: 1,1,1")
    val driverPod = kubernetesClient
      .pods()
      .inNamespace(kubernetesTestComponents.namespace)
      .withLabel("spark-app-locator", appLocator)
      .list()
      .getItems
      .get(0)
    Eventually.eventually(TIMEOUT, INTERVAL) {
      expectedLogOnCompletion.foreach { e =>
        assert(kubernetesClient
          .pods()
          .withName(driverPod.getMetadata.getName)
          .getLog
          .contains(e), "The application did not complete.")
      }
    }
  }
}

private[spark] object KerberosTestSuite {
  val HDFS_TEST_CLASS = "org.apache.spark.examples.HdfsTest"
  val KERB_YAML_LOCATION = "kerberos-yml/kerberos-test.yml"
}
