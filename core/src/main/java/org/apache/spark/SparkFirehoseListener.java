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
 * Class that allows users to receive all SparkListener events.
 * Users should override the onEvent method.
 *
 * This is a concrete class instead of abstract to enforce
 * new events get added to both the SparkListener and this adapter
 * in lockstep.
 */
public class SparkFirehoseListener implements SparkListener {

    public void onEvent(SparkListenerEvent event) { }

    @Override
    public final void onStageCompleted(SparkListenerStageCompleted stageCompleted) {
        onEvent(stageCompleted);
    }

    @Override
    public final void onStageSubmitted(SparkListenerStageSubmitted stageSubmitted) {
        onEvent(stageSubmitted);
    }

    @Override
    public final void onTaskStart(SparkListenerTaskStart taskStart) {
        onEvent(taskStart);
    }

    @Override
    public final void onTaskGettingResult(SparkListenerTaskGettingResult taskGettingResult) {
        onEvent(taskGettingResult);
    }

    @Override
    public final void onTaskEnd(SparkListenerTaskEnd taskEnd) {
        onEvent(taskEnd);
    }

    @Override
    public final void onJobStart(SparkListenerJobStart jobStart) {
        onEvent(jobStart);
    }

    @Override
    public final void onJobEnd(SparkListenerJobEnd jobEnd) {
        onEvent(jobEnd);
    }

    @Override
    public final void onEnvironmentUpdate(SparkListenerEnvironmentUpdate environmentUpdate) {
        onEvent(environmentUpdate);
    }

    @Override
    public final void onBlockManagerAdded(SparkListenerBlockManagerAdded blockManagerAdded) {
        onEvent(blockManagerAdded);
    }

    @Override
    public final void onBlockManagerRemoved(SparkListenerBlockManagerRemoved blockManagerRemoved) {
        onEvent(blockManagerRemoved);
    }

    @Override
    public final void onUnpersistRDD(SparkListenerUnpersistRDD unpersistRDD) {
        onEvent(unpersistRDD);
    }

    @Override
    public final void onApplicationStart(SparkListenerApplicationStart applicationStart) {
        onEvent(applicationStart);
    }

    @Override
    public final void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {
        onEvent(applicationEnd);
    }

    @Override
    public final void onExecutorMetricsUpdate(
            SparkListenerExecutorMetricsUpdate executorMetricsUpdate) {
        onEvent(executorMetricsUpdate);
    }

    @Override
    public final void onExecutorAdded(SparkListenerExecutorAdded executorAdded) {
        onEvent(executorAdded);
    }

    @Override
    public final void onExecutorRemoved(SparkListenerExecutorRemoved executorRemoved) {
        onEvent(executorRemoved);
    }
}
