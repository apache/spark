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

package org.apache.spark.sql.sources;

public interface PartitioningOptions {
  /**
   * When set to `false`, partition discovery is not performed.  This option
   * can be useful when, for example, the user only wants to load data stored
   * in a single partition directory without introducing partition columns
   * encoded in the directory path.
   */
  String PARTITION_DISCOVERY_ENABLED = "partition.discovery.enabled";
}
