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
package org.apache.hadoop.mapred;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.examples.SleepJob;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobTracker;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapred.TaskStatus;
import org.apache.hadoop.mapreduce.server.jobtracker.TaskTracker;
import org.apache.hadoop.mapred.TaskTrackerStatus;
import org.apache.hadoop.mapred.Task.Counter;
import org.apache.hadoop.util.DummyResourceCalculatorPlugin;
import org.apache.hadoop.util.LinuxResourceCalculatorPlugin;
import org.apache.hadoop.util.ResourceCalculatorPlugin;
import org.apache.hadoop.util.ToolRunner;

import junit.framework.TestCase;
import org.junit.Test;
import org.junit.After;

/**
 * This test class tests the functionality related to configuring, reporting
 * and computing memory related parameters in a Map/Reduce cluster.
 * 
 * Each test sets up a {@link MiniMRCluster} with a locally defined 
 * {@link org.apache.hadoop.mapred.TaskScheduler}. This scheduler validates 
 * the memory related configuration is correctly computed and reported from 
 * the tasktracker in 
 * {@link org.apache.hadoop.mapred.TaskScheduler#assignTasks(TaskTrackerStatus)}.
 */
public class TestTTResourceReporting extends TestCase {

  static final Log LOG = LogFactory.getLog(TestTTResourceReporting.class);
  
  private MiniMRCluster miniMRCluster;

  /**
   * Fake scheduler to test the proper reporting of memory values by TT
   */
  public static class FakeTaskScheduler extends JobQueueTaskScheduler {
    
    private boolean hasPassed = true;
    private boolean hasDynamicValuePassed = true;
    private String message;
    
    public FakeTaskScheduler() {
      super();
    }
    
    public boolean hasTestPassed() {
      return hasPassed;
    }

    public boolean hasDynamicTestPassed() {
      return hasDynamicValuePassed;
    }
    
    public String getFailureMessage() {
      return message;
    }
    
    @Override
    public List<Task> assignTasks(TaskTracker taskTracker)
        throws IOException {
      TaskTrackerStatus status = taskTracker.getStatus();
      long totalVirtualMemoryOnTT =
          getConf().getLong("totalVmemOnTT", -1);
      long totalPhysicalMemoryOnTT =
          getConf().getLong("totalPmemOnTT", -1);
      long mapSlotMemorySize =
          getConf().getLong("mapSlotMemorySize", -1);
      long reduceSlotMemorySize =
          getConf()
              .getLong("reduceSlotMemorySize", -1);
      long availableVirtualMemoryOnTT =
          getConf().getLong("availableVmemOnTT", -1);
      long availablePhysicalMemoryOnTT =
          getConf().getLong("availablePmemOnTT", -1);
      long cumulativeCpuTime =
          getConf().getLong("cumulativeCpuTime", -1);
      long cpuFrequency =
          getConf().getLong("cpuFrequency", -1);
      int numProcessors =
          getConf().getInt("numProcessors", -1);
      float cpuUsage =
          getConf().getFloat("cpuUsage", -1);

      long reportedTotalVirtualMemoryOnTT =
          status.getResourceStatus().getTotalVirtualMemory();
      long reportedTotalPhysicalMemoryOnTT =
          status.getResourceStatus().getTotalPhysicalMemory();
      long reportedMapSlotMemorySize =
          status.getResourceStatus().getMapSlotMemorySizeOnTT();
      long reportedReduceSlotMemorySize =
          status.getResourceStatus().getReduceSlotMemorySizeOnTT();
      long reportedAvailableVirtualMemoryOnTT =
          status.getResourceStatus().getAvailableVirtualMemory();
      long reportedAvailablePhysicalMemoryOnTT =
          status.getResourceStatus().getAvailablePhysicalMemory();
      long reportedCumulativeCpuTime =
          status.getResourceStatus().getCumulativeCpuTime();
      long reportedCpuFrequency = status.getResourceStatus().getCpuFrequency();
      int reportedNumProcessors = status.getResourceStatus().getNumProcessors();
      float reportedCpuUsage = status.getResourceStatus().getCpuUsage();

      message =
          "expected memory values : "
              + "(totalVirtualMemoryOnTT, totalPhysicalMemoryOnTT, "
              + "availableVirtualMemoryOnTT, availablePhysicalMemoryOnTT, "
              + "mapSlotMemSize, reduceSlotMemorySize, cumulativeCpuTime, "
              + "cpuFrequency, numProcessors, cpuUsage) = ("
              + totalVirtualMemoryOnTT + ", "
              + totalPhysicalMemoryOnTT + ","
              + availableVirtualMemoryOnTT + ", "
              + availablePhysicalMemoryOnTT + ","
              + mapSlotMemorySize + ","
              + reduceSlotMemorySize + ","
              + cumulativeCpuTime + ","
              + cpuFrequency + ","
              + numProcessors + ","
              + cpuUsage
              +")";
      message +=
          "\nreported memory values : "
              + "(totalVirtualMemoryOnTT, totalPhysicalMemoryOnTT, "
              + "availableVirtualMemoryOnTT, availablePhysicalMemoryOnTT, "
              + "reportedMapSlotMemorySize, reportedReduceSlotMemorySize, "
              + "reportedCumulativeCpuTime, reportedCpuFrequency, "
              + "reportedNumProcessors, cpuUsage) = ("
              + reportedTotalVirtualMemoryOnTT + ", "
              + reportedTotalPhysicalMemoryOnTT + ","
              + reportedAvailableVirtualMemoryOnTT + ", "
              + reportedAvailablePhysicalMemoryOnTT + ","
              + reportedMapSlotMemorySize + ","
              + reportedReduceSlotMemorySize + ","
              + reportedCumulativeCpuTime + ","
              + reportedCpuFrequency + ","
              + reportedNumProcessors + ","
              + reportedCpuUsage
               + ")";
      LOG.info(message);
      hasDynamicValuePassed = true;
      // Check task resource status in task reports
      for (TaskStatus taskStatus : status.getTaskReports()) {
        Counters counters = taskStatus.getCounters();
        // This should be zero because the initial CPU time is subtracted.
        long procCumulativeCpuTime = 0;
        long procVirtualMemorySize =
          getConf().getLong("procVirtualMemorySize", -1);
        long procPhysicalMemorySize =
          getConf().getLong("procPhysicalMemorySize", -1);
        long reportedProcCumulativeCpuTime =
          counters.findCounter(Task.Counter.CPU_MILLISECONDS).getValue();
        long reportedProcVirtualMemorySize =
          counters.findCounter(Task.Counter.VIRTUAL_MEMORY_BYTES).getValue();
        long reportedProcPhysicalMemorySize =
          counters.findCounter(Task.Counter.PHYSICAL_MEMORY_BYTES).getValue();
        String procMessage =
          "expected values : "
              + "(procCumulativeCpuTime, procVirtualMemorySize,"
              + " procPhysicalMemorySize) = ("
              + procCumulativeCpuTime + ", "
              + procVirtualMemorySize + ", "
              + procPhysicalMemorySize + ")";
        procMessage +=
          "\nreported values : "
              + "(procCumulativeCpuTime, procVirtualMemorySize,"
              + " procPhysicalMemorySize) = ("
              + reportedProcCumulativeCpuTime + ", "
              + reportedProcVirtualMemorySize + ", "
              + reportedProcPhysicalMemorySize + ")";
        LOG.info(procMessage);
        message += "\n" + procMessage;
        if (procCumulativeCpuTime != reportedProcCumulativeCpuTime ||
            procVirtualMemorySize != reportedProcVirtualMemorySize ||
            procPhysicalMemorySize != reportedProcPhysicalMemorySize) {
          hasDynamicValuePassed = false;
        }
      }
      hasPassed = true;
      if (totalVirtualMemoryOnTT != reportedTotalVirtualMemoryOnTT
          || totalPhysicalMemoryOnTT != reportedTotalPhysicalMemoryOnTT
          || mapSlotMemorySize != reportedMapSlotMemorySize
          || reduceSlotMemorySize != reportedReduceSlotMemorySize
          || numProcessors != reportedNumProcessors) {
        hasPassed = false;
      }
      // These values changes every moment on the node so it can only be
      // tested by DummyMemoryCalculatorPlugin. Need to check them separately
      if (availableVirtualMemoryOnTT != reportedAvailableVirtualMemoryOnTT
          || availablePhysicalMemoryOnTT != reportedAvailablePhysicalMemoryOnTT
          || cumulativeCpuTime != reportedCumulativeCpuTime
          || cpuFrequency != reportedCpuFrequency
          || cpuUsage != reportedCpuUsage) {
        hasDynamicValuePassed = false;
      }
      return super.assignTasks(taskTracker);
    }
  }

  /**
   * Test that verifies default values are configured and reported correctly.
   * 
   * @throws Exception
   */
  public void testDefaultResourceValues()
      throws Exception {
    JobConf conf = new JobConf();
    try {
      // Memory values are disabled by default.
      conf.setClass(
          org.apache.hadoop.mapred.TaskTracker.TT_RESOURCE_CALCULATOR_PLUGIN,       
          DummyResourceCalculatorPlugin.class, ResourceCalculatorPlugin.class);
      setUpCluster(conf);
      JobConf jobConf = miniMRCluster.createJobConf();
      jobConf.setClass(
          org.apache.hadoop.mapred.TaskTracker.TT_RESOURCE_CALCULATOR_PLUGIN,
          DummyResourceCalculatorPlugin.class, ResourceCalculatorPlugin.class);
      runSleepJob(jobConf);
      verifyTestResults();
    } finally {
      tearDownCluster();
    }
  }

  /**
   * Test that verifies that configured values are reported correctly.
   * 
   * @throws Exception
   */
  public void testConfiguredResourceValues()
      throws Exception {
    JobConf conf = new JobConf();
    conf.setLong("totalVmemOnTT", 4 * 1024 * 1024 * 1024L);
    conf.setLong("totalPmemOnTT", 2 * 1024 * 1024 * 1024L);
    conf.setLong("mapSlotMemorySize", 1 * 512L);
    conf.setLong("reduceSlotMemorySize", 1 * 1024L);
    conf.setLong("availableVmemOnTT", 4 * 1024 * 1024 * 1024L);
    conf.setLong("availablePmemOnTT", 2 * 1024 * 1024 * 1024L);
    conf.setLong("cumulativeCpuTime", 10000L);
    conf.setLong("cpuFrequency", 2000000L);
    conf.setInt("numProcessors", 8);
    conf.setFloat("cpuUsage", 15.5F);
    conf.setLong("procCumulativeCpuTime", 1000L);
    conf.setLong("procVirtualMemorySize", 2 * 1024 * 1024 * 1024L);
    conf.setLong("procPhysicalMemorySize", 1024 * 1024 * 1024L);

    conf.setClass(
        org.apache.hadoop.mapred.TaskTracker.TT_RESOURCE_CALCULATOR_PLUGIN,       
        DummyResourceCalculatorPlugin.class, ResourceCalculatorPlugin.class);
    conf.setLong(DummyResourceCalculatorPlugin.MAXVMEM_TESTING_PROPERTY,
        4 * 1024 * 1024 * 1024L);
    conf.setLong(DummyResourceCalculatorPlugin.MAXPMEM_TESTING_PROPERTY,
        2 * 1024 * 1024 * 1024L);
    conf.setLong(JobTracker.MAPRED_CLUSTER_MAP_MEMORY_MB_PROPERTY, 512L);
    conf.setLong(JobTracker.MAPRED_CLUSTER_REDUCE_MEMORY_MB_PROPERTY, 1024L);
    conf.setLong(DummyResourceCalculatorPlugin.CUMULATIVE_CPU_TIME, 10000L);
    conf.setLong(DummyResourceCalculatorPlugin.CPU_FREQUENCY, 2000000L);
    conf.setInt(DummyResourceCalculatorPlugin.NUM_PROCESSORS, 8);
    conf.setFloat(DummyResourceCalculatorPlugin.CPU_USAGE, 15.5F);
    try {
      setUpCluster(conf);
      JobConf jobConf = miniMRCluster.createJobConf();
      jobConf.setMemoryForMapTask(1 * 1024L);
      jobConf.setMemoryForReduceTask(2 * 1024L);
      jobConf.setClass(
        org.apache.hadoop.mapred.TaskTracker.TT_RESOURCE_CALCULATOR_PLUGIN,
        DummyResourceCalculatorPlugin.class, ResourceCalculatorPlugin.class);
      jobConf.setLong(DummyResourceCalculatorPlugin.PROC_CUMULATIVE_CPU_TIME, 1000L);
      jobConf.setLong(DummyResourceCalculatorPlugin.PROC_VMEM_TESTING_PROPERTY,
                      2 * 1024 * 1024 * 1024L);
      jobConf.setLong(DummyResourceCalculatorPlugin.PROC_PMEM_TESTING_PROPERTY,
                      1024 * 1024 * 1024L);
      runSleepJob(jobConf);
      verifyTestResults();
    } finally {
      tearDownCluster();
    }
  }

  /**
   * Test that verifies that total memory values are calculated and reported
   * correctly.
   * 
   * @throws Exception
   */
  public void testResourceValuesOnLinux()
      throws Exception {
    if (!System.getProperty("os.name").startsWith("Linux")) {
      return;
    }

    JobConf conf = new JobConf();
    LinuxResourceCalculatorPlugin plugin = new LinuxResourceCalculatorPlugin();
    // In this case, we only check these four fields because they are static
    conf.setLong("totalVmemOnTT", plugin.getVirtualMemorySize());
    conf.setLong("totalPmemOnTT", plugin.getPhysicalMemorySize());
    conf.setLong("numProcessors", plugin.getNumProcessors());

    try {
      setUpCluster(conf);
      runSleepJob(miniMRCluster.createJobConf());
      verifyTestResults(true);
    } finally {
      tearDownCluster();
    }
  }

  private void setUpCluster(JobConf conf)
                                throws Exception {
    conf.setClass("mapred.jobtracker.taskScheduler",
        TestTTResourceReporting.FakeTaskScheduler.class, TaskScheduler.class);
    conf.set("mapred.job.tracker.handler.count", "1");
    miniMRCluster = new MiniMRCluster(1, "file:///", 3, null, null, conf);
  }
  
  private void runSleepJob(JobConf conf) throws Exception {
    String[] args = { "-m", "1", "-r", "1",
                      "-mt", "10", "-rt", "10" };
    ToolRunner.run(conf, new SleepJob(), args);
  }

  private void verifyTestResults() {
    verifyTestResults(false);
  }

  private void verifyTestResults(boolean excludeDynamic) {
    FakeTaskScheduler scheduler = 
      (FakeTaskScheduler)miniMRCluster.getJobTrackerRunner().
                              getJobTracker().getTaskScheduler();
    assertTrue(scheduler.getFailureMessage(), scheduler.hasTestPassed());
    if (!excludeDynamic) {
      assertTrue(scheduler.getFailureMessage(),
                 scheduler.hasDynamicTestPassed());
    }
  }
  
  //TODO make it after
  private void tearDownCluster() {
    if (miniMRCluster != null) {
      miniMRCluster.shutdown();
    }
  }
}
