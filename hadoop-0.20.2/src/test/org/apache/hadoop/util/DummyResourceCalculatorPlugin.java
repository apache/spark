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

package org.apache.hadoop.util;

/**
 * Plugin class to test resource information reported by TT. Use
 * configuration items {@link #MAXVMEM_TESTING_PROPERTY} and
 * {@link #MAXPMEM_TESTING_PROPERTY} to tell TT the total vmem and the total
 * pmem. Use configuration items {@link #NUM_PROCESSORS},
 * {@link #CPU_FREQUENCY}, {@link #CUMULATIVE_CPU_TIME} and {@link #CPU_USAGE}
 * to tell TT the CPU information.
 */
public class DummyResourceCalculatorPlugin extends ResourceCalculatorPlugin {

  /** max vmem on the TT */
  public static final String MAXVMEM_TESTING_PROPERTY =
      "mapred.tasktracker.maxvmem.testing";
  /** max pmem on the TT */
  public static final String MAXPMEM_TESTING_PROPERTY =
      "mapred.tasktracker.maxpmem.testing";
  /** number of processors for testing */
  public static final String NUM_PROCESSORS =
      "mapred.tasktracker.numprocessors.testing";
  /** CPU frequency for testing */
  public static final String CPU_FREQUENCY =
      "mapred.tasktracker.cpufrequency.testing";
  /** cumulative CPU usage time for testing */
  public static final String CUMULATIVE_CPU_TIME =
      "mapred.tasktracker.cumulativecputime.testing";
  /** CPU usage percentage for testing */
  public static final String CPU_USAGE =
      "mapred.tasktracker.cpuusage.testing";
  /** process cumulative CPU usage time for testing */
  public static final String PROC_CUMULATIVE_CPU_TIME =
      "mapred.tasktracker.proccumulativecputime.testing";
  /** process pmem for testing*/
  public static final String PROC_PMEM_TESTING_PROPERTY =
      "mapred.tasktracker.procpmem.testing";
  /** process vmem for testing*/
  public static final String PROC_VMEM_TESTING_PROPERTY =
      "mapred.tasktracker.procvmem.testing";

  /** {@inheritDoc} */
  @Override
  public long getVirtualMemorySize() {
    return getConf().getLong(MAXVMEM_TESTING_PROPERTY, -1);
  }

  /** {@inheritDoc} */
  @Override
  public long getPhysicalMemorySize() {
    return getConf().getLong(MAXPMEM_TESTING_PROPERTY, -1);
  }

  /** {@inheritDoc} */
  @Override
  public long getAvailableVirtualMemorySize() {
    return getConf().getLong(MAXVMEM_TESTING_PROPERTY, -1);
  }

  /** {@inheritDoc} */
  @Override
  public long getAvailablePhysicalMemorySize() {
    return getConf().getLong(MAXPMEM_TESTING_PROPERTY, -1);
  }

  /** {@inheritDoc} */
  @Override
  public int getNumProcessors() {
    return getConf().getInt(NUM_PROCESSORS, -1);
  }

  /** {@inheritDoc} */
  @Override
  public long getCpuFrequency() {
    return getConf().getLong(CPU_FREQUENCY, -1);
  }

  /** {@inheritDoc} */
  @Override
  public long getCumulativeCpuTime() {
    return getConf().getLong(CUMULATIVE_CPU_TIME, -1);
  }

  /** {@inheritDoc} */
  @Override
  public float getCpuUsage() {
    return getConf().getFloat(CPU_USAGE, -1);
  }

  @Override
  public ProcResourceValues getProcResourceValues() {
    long cpuTime = getConf().getLong(PROC_CUMULATIVE_CPU_TIME, -1);
    long pMem = getConf().getLong(PROC_PMEM_TESTING_PROPERTY, -1);
    long vMem = getConf().getLong(PROC_VMEM_TESTING_PROPERTY, -1);
    return new ProcResourceValues(cpuTime, pMem, vMem);
  }
}
