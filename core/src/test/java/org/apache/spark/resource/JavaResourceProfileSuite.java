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

import static org.junit.Assert.*;
import org.junit.Test;

// Test the ResourceProfile and Request api's from Java
public class JavaResourceProfileSuite {

  @Test
  public void testSortingOnlyByPrefix() throws Exception {
    ExecutorResourceRequest execReq1 = new ExecutorResourceRequest("resource.gpu", 2, "", "myscript");
    ExecutorResourceRequest execReq2 = new ExecutorResourceRequest("resource.fpga", 3, "", "myfpgascript", "nvidia");

    ResourceProfile rprof = new ResourceProfile();
    rprof.require(execReq1);
    TaskResourceRequest taskReq1 = new TaskResourceRequest("resource.gpu", 1);
    rprof.require(taskReq1);

    assertEquals(rprof.getExecutorResources().size(), 1);
    assert(rprof.getExecutorResources().contains("resource.gpu"));
    // ExecutorResourceRequest res1 = rprof.getExecutorResources().asJava;

    assertEquals(rprof.getTaskResources().size(), 1);
    assert(rprof.getTaskResources().contains("resource.gpu"));
  }
}
