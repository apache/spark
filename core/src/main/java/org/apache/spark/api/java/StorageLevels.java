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

package org.apache.spark.api.java;

import org.apache.spark.storage.StorageLevel;

/**
 * Expose some commonly useful storage level constants.
 */
public class StorageLevels {
  public static final StorageLevel NONE = create(false, false, false, false, 1);
  public static final StorageLevel DISK_ONLY = create(true, false, false, false, 1);
  public static final StorageLevel DISK_ONLY_2 = create(true, false, false, false, 2);
  public static final StorageLevel MEMORY_ONLY = create(false, true, false, true, 1);
  public static final StorageLevel MEMORY_ONLY_2 = create(false, true, false, true, 2);
  public static final StorageLevel MEMORY_ONLY_SER = create(false, true, false, false, 1);
  public static final StorageLevel MEMORY_ONLY_SER_2 = create(false, true, false, false, 2);
  public static final StorageLevel MEMORY_AND_DISK = create(true, true, false, true, 1);
  public static final StorageLevel MEMORY_AND_DISK_2 = create(true, true, false, true, 2);
  public static final StorageLevel MEMORY_AND_DISK_SER = create(true, true, false, false, 1);
  public static final StorageLevel MEMORY_AND_DISK_SER_2 = create(true, true, false, false, 2);
  public static final StorageLevel OFF_HEAP = create(true, true, true, false, 1);

  /**
   * Create a new StorageLevel object.
   * @param useDisk saved to disk, if true
   * @param useMemory saved to on-heap memory, if true
   * @param useOffHeap saved to off-heap memory, if true
   * @param deserialized saved as deserialized objects, if true
   * @param replication replication factor
   */
  public static StorageLevel create(
    boolean useDisk,
    boolean useMemory,
    boolean useOffHeap,
    boolean deserialized,
    int replication) {
    return StorageLevel.apply(useDisk, useMemory, useOffHeap, deserialized, replication);
  }
}
