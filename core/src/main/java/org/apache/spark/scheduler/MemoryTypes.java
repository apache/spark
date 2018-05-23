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

package org.apache.spark.scheduler;

import org.apache.spark.executor.ExecutorMetrics;

public enum MemoryTypes {
  JvmUsedMemory{
    @Override
    long get(ExecutorMetrics em) {
      return em.jvmUsedMemory();
    }
  },
  OnHeapExecutionMemory {
    @Override
    long get(ExecutorMetrics em) {
      return em.onHeapExecutionMemory();
    }
  },
  OffHeapExecutionMemory {
    @Override
    long get(ExecutorMetrics em) {
      return em.offHeapExecutionMemory();
    }
  },
  OnHeapStorageMemory {
    @Override
    long get(ExecutorMetrics em) {
      return em.onHeapStorageMemory();
    }
  },
  OffHeapStorageMemory {
    @Override
    long get(ExecutorMetrics em) {
      return em.offHeapStorageMemory();
    }
  },
  OnHeapUnifiedMemory {
    @Override
    long get(ExecutorMetrics em) {
      return em.onHeapExecutionMemory() + em.onHeapStorageMemory();
    }
  },
  OffHeapUnifiedMemory {
    @Override
    long get(ExecutorMetrics em) {
      return em.offHeapExecutionMemory() + em.offHeapStorageMemory();
    }
  };

  abstract long get(ExecutorMetrics em);
}
