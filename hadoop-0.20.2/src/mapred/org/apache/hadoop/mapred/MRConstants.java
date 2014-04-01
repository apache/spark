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

/*******************************
 * Some handy constants
 * 
 *******************************/
interface MRConstants {
  //
  // Timeouts, constants
  //
  public static final int HEARTBEAT_INTERVAL_MIN_DEFAULT = 300;
  
  public static final long COUNTER_UPDATE_INTERVAL = 60 * 1000;

  /**
   * How often TaskTracker needs to check the health of its disks, if not
   * configured using mapred.disk.healthChecker.interval
   */
  public static final long DEFAULT_DISK_HEALTH_CHECK_INTERVAL = 60 * 1000;

  //
  // Result codes
  //
  public static int SUCCESS = 0;
  public static int FILE_NOT_FOUND = -1;
  
  /**
   * The custom http header used for the map output length.
   */
  public static final String MAP_OUTPUT_LENGTH = "Map-Output-Length";

  /**
   * The custom http header used for the "raw" map output length.
   */
  public static final String RAW_MAP_OUTPUT_LENGTH = "Raw-Map-Output-Length";

  /**
   * The map task from which the map output data is being transferred
   */
  public static final String FROM_MAP_TASK = "from-map-task";
  
  /**
   * The reduce task number for which this map output is being transferred
   */
  public static final String FOR_REDUCE_TASK = "for-reduce-task";
  
  public static final String WORKDIR = "work";
}
