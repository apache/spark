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

import org.apache.hadoop.util.MemoryCalculatorPlugin;

/**
 * Plugin class to test virtual and physical memories reported by TT. Use
 * configuration items {@link #MAXVMEM_TESTING_PROPERTY} and
 * {@link #MAXPMEM_TESTING_PROPERTY} to tell TT the total vmem and the total
 * pmem.
 */
public class DummyMemoryCalculatorPlugin extends MemoryCalculatorPlugin {

  /** max vmem on the TT */
  public static final String MAXVMEM_TESTING_PROPERTY =
      "mapred.tasktracker.maxvmem.testing";
  /** max pmem on the TT */
  public static final String MAXPMEM_TESTING_PROPERTY =
      "mapred.tasktracker.maxpmem.testing";

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
}