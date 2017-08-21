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
package org.apache.spark.scheduler.cluster.kubernetes

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.CommonConfigurationKeysPublic

import org.apache.spark.SparkFunSuite

class RackResolverUtilImplSuite extends SparkFunSuite {

  test("Detects if topology plugin is configured") {
    val hadoopConfiguration = new Configuration
    val rackResolverUtil = new RackResolverUtilImpl(hadoopConfiguration)

    assert(!rackResolverUtil.checkConfigured(hadoopConfiguration))
    hadoopConfiguration.set(CommonConfigurationKeysPublic.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
      rackResolverUtil.scriptPlugin)
    assert(!rackResolverUtil.checkConfigured(hadoopConfiguration))
    hadoopConfiguration.set(CommonConfigurationKeysPublic.NET_TOPOLOGY_SCRIPT_FILE_NAME_KEY,
      "my-script")
    assert(rackResolverUtil.checkConfigured(hadoopConfiguration))

    hadoopConfiguration.set(CommonConfigurationKeysPublic.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
      rackResolverUtil.tablePlugin)
    assert(!rackResolverUtil.checkConfigured(hadoopConfiguration))
    hadoopConfiguration.set(CommonConfigurationKeysPublic.NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY,
      "my-table")
    assert(rackResolverUtil.checkConfigured(hadoopConfiguration))

    hadoopConfiguration.set(CommonConfigurationKeysPublic.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
      "my.Plugin")
    assert(rackResolverUtil.checkConfigured(hadoopConfiguration))
  }
}
