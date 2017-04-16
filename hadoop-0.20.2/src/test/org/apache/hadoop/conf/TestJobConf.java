/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.conf;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.hadoop.mapred.JobConf;

public class TestJobConf extends TestCase {

  public void testProfileParamsDefaults() {
    JobConf configuration = new JobConf();

    Assert.assertNull(configuration.get("mapred.task.profile.params"));

    String result = configuration.getProfileParams();

    Assert.assertNotNull(result);
    Assert.assertTrue(result.contains("file=%s"));
    Assert.assertTrue(result.startsWith("-agentlib:hprof"));
  }

  public void testProfileParamsSetter() {
    JobConf configuration = new JobConf();

    configuration.setProfileParams("test");
    Assert.assertEquals("test", configuration.get("mapred.task.profile.params"));
  }

  public void testProfileParamsGetter() {
    JobConf configuration = new JobConf();

    configuration.set("mapred.task.profile.params", "test");
    Assert.assertEquals("test", configuration.getProfileParams());
  }

  /**
   * Testing mapred.task.maxvmem replacement with new values
   *
   */
  public void testMemoryConfigForMapOrReduceTask(){
    JobConf configuration = new JobConf();
    configuration.set("mapred.job.map.memory.mb",String.valueOf(300));
    configuration.set("mapred.job.reduce.memory.mb",String.valueOf(300));
    Assert.assertEquals(configuration.getMemoryForMapTask(),300);
    Assert.assertEquals(configuration.getMemoryForReduceTask(),300);

    configuration.set("mapred.task.maxvmem" , String.valueOf(2*1024 * 1024));
    configuration.set("mapred.job.map.memory.mb",String.valueOf(300));
    configuration.set("mapred.job.reduce.memory.mb",String.valueOf(300));
    Assert.assertEquals(configuration.getMemoryForMapTask(),2);
    Assert.assertEquals(configuration.getMemoryForReduceTask(),2);

    configuration = new JobConf();
    configuration.set("mapred.task.maxvmem" , "-1");
    configuration.set("mapred.job.map.memory.mb",String.valueOf(300));
    configuration.set("mapred.job.reduce.memory.mb",String.valueOf(400));
    Assert.assertEquals(configuration.getMemoryForMapTask(), 300);
    Assert.assertEquals(configuration.getMemoryForReduceTask(), 400);

    configuration = new JobConf();
    configuration.set("mapred.task.maxvmem" , String.valueOf(2*1024 * 1024));
    configuration.set("mapred.job.map.memory.mb","-1");
    configuration.set("mapred.job.reduce.memory.mb","-1");
    Assert.assertEquals(configuration.getMemoryForMapTask(),2);
    Assert.assertEquals(configuration.getMemoryForReduceTask(),2);

    configuration = new JobConf();
    configuration.set("mapred.task.maxvmem" , String.valueOf(-1));
    configuration.set("mapred.job.map.memory.mb","-1");
    configuration.set("mapred.job.reduce.memory.mb","-1");
    Assert.assertEquals(configuration.getMemoryForMapTask(),-1);
    Assert.assertEquals(configuration.getMemoryForReduceTask(),-1);    

    configuration = new JobConf();
    configuration.set("mapred.task.maxvmem" , String.valueOf(2*1024 * 1024));
    configuration.set("mapred.job.map.memory.mb","3");
    configuration.set("mapred.job.reduce.memory.mb","3");
    Assert.assertEquals(configuration.getMemoryForMapTask(),2);
    Assert.assertEquals(configuration.getMemoryForReduceTask(),2);
  }
  
  /**
   * Test that negative values for MAPRED_TASK_MAXVMEM_PROPERTY cause
   * new configuration keys' values to be used.
   */
  
  public void testNegativeValueForTaskVmem() {
    JobConf configuration = new JobConf();
    
    configuration.set(JobConf.MAPRED_TASK_MAXVMEM_PROPERTY, "-3");
    configuration.set("mapred.job.map.memory.mb", "4");
    configuration.set("mapred.job.reduce.memory.mb", "5");
    Assert.assertEquals(4, configuration.getMemoryForMapTask());
    Assert.assertEquals(5, configuration.getMemoryForReduceTask());
    
  }
  
  /**
   * Test that negative values for all memory configuration properties causes
   * APIs to disable memory limits
   */
  
  public void testNegativeValuesForMemoryParams() {
    JobConf configuration = new JobConf();
    
    configuration.set(JobConf.MAPRED_TASK_MAXVMEM_PROPERTY, "-4");
    configuration.set("mapred.job.map.memory.mb", "-5");
    configuration.set("mapred.job.reduce.memory.mb", "-6");
    
    Assert.assertEquals(JobConf.DISABLED_MEMORY_LIMIT,
                        configuration.getMemoryForMapTask());
    Assert.assertEquals(JobConf.DISABLED_MEMORY_LIMIT,
                        configuration.getMemoryForReduceTask());
    Assert.assertEquals(JobConf.DISABLED_MEMORY_LIMIT,
                        configuration.getMaxVirtualMemoryForTask());
  }

  /**
   *   Test deprecated accessor and mutator method for mapred.task.maxvmem
   */
  public void testMaxVirtualMemoryForTask() {
    JobConf configuration = new JobConf();

    //get test case
    configuration.set("mapred.job.map.memory.mb", String.valueOf(300));
    configuration.set("mapred.job.reduce.memory.mb", String.valueOf(-1));
    Assert.assertEquals(
      configuration.getMaxVirtualMemoryForTask(), 300 * 1024 * 1024);

    configuration = new JobConf();
    configuration.set("mapred.job.map.memory.mb", String.valueOf(-1));
    configuration.set("mapred.job.reduce.memory.mb", String.valueOf(200));
    Assert.assertEquals(
      configuration.getMaxVirtualMemoryForTask(), 200 * 1024 * 1024);

    configuration = new JobConf();
    configuration.set("mapred.job.map.memory.mb", String.valueOf(-1));
    configuration.set("mapred.job.reduce.memory.mb", String.valueOf(-1));
    configuration.set("mapred.task.maxvmem", String.valueOf(1 * 1024 * 1024));
    Assert.assertEquals(
      configuration.getMaxVirtualMemoryForTask(), 1 * 1024 * 1024);

    configuration = new JobConf();
    configuration.set("mapred.task.maxvmem", String.valueOf(1 * 1024 * 1024));
    Assert.assertEquals(
      configuration.getMaxVirtualMemoryForTask(), 1 * 1024 * 1024);

    //set test case

    configuration = new JobConf();
    configuration.setMaxVirtualMemoryForTask(2 * 1024 * 1024);
    Assert.assertEquals(configuration.getMemoryForMapTask(), 2);
    Assert.assertEquals(configuration.getMemoryForReduceTask(), 2);

    configuration = new JobConf();   
    configuration.set("mapred.job.map.memory.mb", String.valueOf(300));
    configuration.set("mapred.job.reduce.memory.mb", String.valueOf(400));
    configuration.setMaxVirtualMemoryForTask(2 * 1024 * 1024);
    Assert.assertEquals(configuration.getMemoryForMapTask(), 2);
    Assert.assertEquals(configuration.getMemoryForReduceTask(), 2);
  }
}
