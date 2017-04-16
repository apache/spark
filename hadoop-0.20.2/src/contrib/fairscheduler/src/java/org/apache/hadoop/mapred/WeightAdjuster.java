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

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.mapreduce.TaskType;

/**
 * A pluggable object for altering the weights of jobs in the fair scheduler,
 * which is used for example by {@link NewJobWeightBooster} to give higher
 * weight to new jobs so that short jobs finish faster.
 * 
 * May implement {@link Configurable} to access configuration parameters.
 */
public interface WeightAdjuster {
  public double adjustWeight(JobInProgress job, TaskType taskType,
      double curWeight);
}
