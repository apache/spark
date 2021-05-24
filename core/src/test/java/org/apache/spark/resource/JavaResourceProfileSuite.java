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

package org.apache.spark.resource;

import java.util.Map;

import static org.junit.Assert.*;
import org.junit.Test;

// Test the ResourceProfile and Request api's from Java
public class JavaResourceProfileSuite {

  String GpuResource = "resource.gpu";
  String FPGAResource = "resource.fpga";

  @Test
  public void testResourceProfileAccessFromJava() throws Exception {
    ExecutorResourceRequests execReqGpu =
      new ExecutorResourceRequests().resource(GpuResource, 2,"myscript", "");
    ExecutorResourceRequests execReqFpga =
      new ExecutorResourceRequests().resource(FPGAResource, 3, "myfpgascript", "nvidia");

    ResourceProfileBuilder rprof = new ResourceProfileBuilder();
    rprof.require(execReqGpu);
    rprof.require(execReqFpga);
    TaskResourceRequests taskReq1 = new TaskResourceRequests().resource(GpuResource, 1);
    rprof.require(taskReq1);

    assertEquals(rprof.executorResources().size(), 2);
    Map<String, ExecutorResourceRequest> eresources = rprof.executorResourcesJMap();
    assert(eresources.containsKey(GpuResource));
    ExecutorResourceRequest gpuReq = eresources.get(GpuResource);
    assertEquals(gpuReq.amount(), 2);
    assertEquals(gpuReq.discoveryScript(), "myscript");
    assertEquals(gpuReq.vendor(), "");

    assert(eresources.containsKey(FPGAResource));
    ExecutorResourceRequest fpgaReq = eresources.get(FPGAResource);
    assertEquals(fpgaReq.amount(), 3);
    assertEquals(fpgaReq.discoveryScript(), "myfpgascript");
    assertEquals(fpgaReq.vendor(), "nvidia");

    assertEquals(rprof.taskResources().size(), 1);
    Map<String, TaskResourceRequest> tresources = rprof.taskResourcesJMap();
    assert(tresources.containsKey(GpuResource));
    TaskResourceRequest taskReq = tresources.get(GpuResource);
    assertEquals(taskReq.amount(), 1.0, 0);
    assertEquals(taskReq.resourceName(), GpuResource);
  }
}

