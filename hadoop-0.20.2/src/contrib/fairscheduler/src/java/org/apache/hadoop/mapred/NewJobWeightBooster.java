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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.TaskType;

/**
 * A {@link WeightAdjuster} implementation that gives a weight boost to new jobs
 * for a certain amount of time -- by default, a 3x weight boost for 60 seconds.
 * This can be used to make shorter jobs finish faster, emulating Shortest Job
 * First scheduling while not starving long jobs. 
 */
public class NewJobWeightBooster extends Configured implements WeightAdjuster {
  private static final float DEFAULT_FACTOR = 3;
  private static final long DEFAULT_DURATION = 5 * 60 * 1000;

  private float factor;
  private long duration;

  public void setConf(Configuration conf) {
    if (conf != null) {
      factor = conf.getFloat("mapred.newjobweightbooster.factor",
          DEFAULT_FACTOR);
      duration = conf.getLong("mapred.newjobweightbooster.duration",
          DEFAULT_DURATION);
    }
    super.setConf(conf);
  }
  
  public double adjustWeight(JobInProgress job, TaskType taskType,
      double curWeight) {
    long start = job.getStartTime();
    long now = System.currentTimeMillis();
    if (now - start < duration) {
      return curWeight * factor;
    } else {
      return curWeight;
    }
  }
}
