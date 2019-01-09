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


package org.apache.spark.streaming;

import org.apache.spark.streaming.api.java.*;

public class JavaStreamingListenerAPISuite extends JavaStreamingListener {

  @Override
  public void onStreamingStarted(JavaStreamingListenerStreamingStarted streamingStarted) {
    super.onStreamingStarted(streamingStarted);
  }

  @Override
  public void onReceiverStarted(JavaStreamingListenerReceiverStarted receiverStarted) {
    JavaReceiverInfo receiverInfo = receiverStarted.receiverInfo();
    receiverInfo.streamId();
    receiverInfo.name();
    receiverInfo.active();
    receiverInfo.location();
    receiverInfo.executorId();
    receiverInfo.lastErrorMessage();
    receiverInfo.lastError();
    receiverInfo.lastErrorTime();
  }

  @Override
  public void onReceiverError(JavaStreamingListenerReceiverError receiverError) {
    JavaReceiverInfo receiverInfo = receiverError.receiverInfo();
    receiverInfo.streamId();
    receiverInfo.name();
    receiverInfo.active();
    receiverInfo.location();
    receiverInfo.executorId();
    receiverInfo.lastErrorMessage();
    receiverInfo.lastError();
    receiverInfo.lastErrorTime();
  }

  @Override
  public void onReceiverStopped(JavaStreamingListenerReceiverStopped receiverStopped) {
    JavaReceiverInfo receiverInfo = receiverStopped.receiverInfo();
    receiverInfo.streamId();
    receiverInfo.name();
    receiverInfo.active();
    receiverInfo.location();
    receiverInfo.executorId();
    receiverInfo.lastErrorMessage();
    receiverInfo.lastError();
    receiverInfo.lastErrorTime();
  }

  @Override
  public void onBatchSubmitted(JavaStreamingListenerBatchSubmitted batchSubmitted) {
    super.onBatchSubmitted(batchSubmitted);
  }

  @Override
  public void onBatchStarted(JavaStreamingListenerBatchStarted batchStarted) {
    super.onBatchStarted(batchStarted);
  }

  @Override
  public void onBatchCompleted(JavaStreamingListenerBatchCompleted batchCompleted) {
    super.onBatchCompleted(batchCompleted);
  }

  @Override
  public void onOutputOperationStarted(
      JavaStreamingListenerOutputOperationStarted outputOperationStarted) {
    super.onOutputOperationStarted(outputOperationStarted);
  }

  @Override
  public void onOutputOperationCompleted(
      JavaStreamingListenerOutputOperationCompleted outputOperationCompleted) {
    super.onOutputOperationCompleted(outputOperationCompleted);
  }
}
