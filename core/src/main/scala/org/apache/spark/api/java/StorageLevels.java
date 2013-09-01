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
  public static final StorageLevel NONE = new StorageLevel(false, false, false, 1);
  public static final StorageLevel DISK_ONLY = new StorageLevel(true, false, false, 1);
  public static final StorageLevel DISK_ONLY_2 = new StorageLevel(true, false, false, 2);
  public static final StorageLevel MEMORY_ONLY = new StorageLevel(false, true, true, 1);
  public static final StorageLevel MEMORY_ONLY_2 = new StorageLevel(false, true, true, 2);
  public static final StorageLevel MEMORY_ONLY_SER = new StorageLevel(false, true, false, 1);
  public static final StorageLevel MEMORY_ONLY_SER_2 = new StorageLevel(false, true, false, 2);
  public static final StorageLevel MEMORY_AND_DISK = new StorageLevel(true, true, true, 1);
  public static final StorageLevel MEMORY_AND_DISK_2 = new StorageLevel(true, true, true, 2);
  public static final StorageLevel MEMORY_AND_DISK_SER = new StorageLevel(true, true, false, 1);
  public static final StorageLevel MEMORY_AND_DISK_SER_2 = new StorageLevel(true, true, false, 2);

  /**
   * Create a new StorageLevel object.
   * @param useDisk saved to disk, if true
   * @param useMemory saved to memory, if true
   * @param deserialized saved as deserialized objects, if true
   * @param replication replication factor
   */
  public static StorageLevel create(boolean useDisk, boolean useMemory, boolean deserialized, int replication) {
    return StorageLevel.apply(useDisk, useMemory, deserialized, replication);
  }
}
