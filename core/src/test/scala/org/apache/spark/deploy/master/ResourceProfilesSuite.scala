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

package org.apache.spark.deploy.master

import org.apache.spark.deploy._
import org.apache.spark.resource.ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID
import org.apache.spark.resource.ResourceRequirement
import org.apache.spark.resource.ResourceUtils.{FPGA, GPU}

class ResourceProfilesSuite extends MasterSuiteBase {
  test("scheduling for app with multiple resource profiles") {
    scheduleExecutorsForAppWithMultiRPs(withMaxCores = false)
  }

  test("resource description with multiple resource profiles") {
    val appInfo = makeAppInfo(128, Some(4), None, Map(GPU -> 2))
    val rp1 = DeployTestUtils.createResourceProfile(None, Map(FPGA -> 2), None)
    val rp2 = DeployTestUtils.createResourceProfile(Some(256), Map(GPU -> 3, FPGA -> 3), Some(2))

    val resourceProfileToTotalExecs = Map(
      appInfo.desc.defaultProfile -> 1,
      rp1 -> 2,
      rp2 -> 3
    )
    appInfo.requestExecutors(resourceProfileToTotalExecs)

    // Default resource profile take it's own resource request.
    var resourceDesc = appInfo.getResourceDescriptionForRpId(DEFAULT_RESOURCE_PROFILE_ID)
    assert(resourceDesc.memoryMbPerExecutor === 128)
    assert(resourceDesc.coresPerExecutor === Some(4))
    assert(resourceDesc.customResourcesPerExecutor === Seq(ResourceRequirement(GPU, 2)))

    // Non-default resource profiles take cores and memory from default profile if not specified.
    resourceDesc = appInfo.getResourceDescriptionForRpId(rp1.id)
    assert(resourceDesc.memoryMbPerExecutor === 128)
    assert(resourceDesc.coresPerExecutor === Some(4))
    assert(resourceDesc.customResourcesPerExecutor === Seq(ResourceRequirement(FPGA, 2)))

    resourceDesc = appInfo.getResourceDescriptionForRpId(rp2.id)
    assert(resourceDesc.memoryMbPerExecutor === 256)
    assert(resourceDesc.coresPerExecutor === Some(2))
    assert(resourceDesc.customResourcesPerExecutor ===
      Seq(ResourceRequirement(FPGA, 3), ResourceRequirement(GPU, 3)))
  }
}

class ResourceProfilesMaxCoresSuite extends MasterSuiteBase {
  test("scheduling for app with multiple resource profiles with max cores") {
    scheduleExecutorsForAppWithMultiRPs(withMaxCores = true)
  }
}
