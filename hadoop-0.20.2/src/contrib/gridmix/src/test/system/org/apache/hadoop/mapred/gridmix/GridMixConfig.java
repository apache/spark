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
package org.apache.hadoop.mapred.gridmix;

public class GridMixConfig {
  /**
   *  Gridmix logger mode.
   */
  public static final String GRIDMIX_LOG_MODE = 
      "log4j.logger.org.apache.hadoop.mapred.gridmix";

  /**
   *  Gridmix output directory.
   */
  public static final String GRIDMIX_OUTPUT_DIR = 
      "gridmix.output.directory";

  /**
   * Gridmix job type (LOADJOB/SLEEPJOB).
   */
  public static final String GRIDMIX_JOB_TYPE = 
      "gridmix.job.type";

  /**
   *  Gridmix submission use queue.
   */
  public static final String GRIDMIX_JOB_SUBMISSION_QUEUE_IN_TRACE = 
      "gridmix.job-submission.use-queue-in-trace";
  
  /**
   *  Gridmix user resolver(RoundRobinUserResolver/
   *  SubmitterUserResolver/EchoUserResolver).
   */
  public static final String GRIDMIX_USER_RESOLVER = 
      "gridmix.user.resolve.class";

  /**
   *  Gridmix queue depth.
   */
  public static final String GRIDMIX_QUEUE_DEPTH = 
      "gridmix.client.pending.queue.depth";
  
  /**
   * Gridmix generate bytes per file.
   */
  public static final String GRIDMIX_BYTES_PER_FILE = 
      "gridmix.gen.bytes.per.file";
  
  /**
   *  Gridmix job submission policy(STRESS/REPLAY/SERIAL).
   */
  public static final String GRIDMIX_SUBMISSION_POLICY =
      "gridmix.job-submission.policy";

  /**
   *  Gridmix minimum file size.
   */
  public static final String GRIDMIX_MINIMUM_FILE_SIZE =
      "gridmix.min.file.size";
}
