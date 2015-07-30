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

package org.apache.spark;

import org.apache.spark.scheduler.*;

/**
 * Java clients should extend this class instead of implementing
 * SparkListener directly. This is to prevent java clients
 * from breaking when new events are added to the SparkListener
 * trait.
 *
 * This is a concrete class instead of abstract to enforce
 * new events get added to both the SparkListener and this adapter
 * in lockstep.
 */
public class JavaSparkListener implements SparkListener {

  @Override
  public void onStageCompleted(SparkListenerStageCompleted stageCompleted) { }

  @Override
  public void onStageSubmitted(SparkListenerStageSubmitted stageSubmitted) { }

  @Override
  public void onTaskStart(SparkListenerTaskStart taskStart) { }

  @Override
  public void onTaskGettingResult(SparkListenerTaskGettingResult taskGettingResult) { }

  @Override
  public void onTaskEnd(SparkListenerTaskEnd taskEnd) { }

  @Override
  public void onJobStart(SparkListenerJobStart jobStart) { }

  @Override
  public void onJobEnd(SparkListenerJobEnd jobEnd) { }

  @Override
  public void onEnvironmentUpdate(SparkListenerEnvironmentUpdate environmentUpdate) { }

  @Override
  public void onBlockManagerAdded(SparkListenerBlockManagerAdded blockManagerAdded) { }

  @Override
  public void onBlockManagerRemoved(SparkListenerBlockManagerRemoved blockManagerRemoved) { }

  @Override
  public void onUnpersistRDD(SparkListenerUnpersistRDD unpersistRDD) { }

  @Override
  public void onApplicationStart(SparkListenerApplicationStart applicationStart) { }

  @Override
  public void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) { }

  @Override
  public void onExecutorMetricsUpdate(SparkListenerExecutorMetricsUpdate executorMetricsUpdate) { }

  @Override
  public void onExecutorAdded(SparkListenerExecutorAdded executorAdded) { }

  @Override
  public void onExecutorRemoved(SparkListenerExecutorRemoved executorRemoved) { }

  @Override
  public void onBlockUpdated(SparkListenerBlockUpdated blockUpdated) { }

}
