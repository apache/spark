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

package org.apache.spark.sql.execution.joins;

import org.apache.spark.SparkContext;
import org.apache.spark.SparkEnv;
import org.apache.spark.broadcast.BroadcastLifeCycleListener;
import org.apache.spark.broadcast.BroadcastManager;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;

public abstract class AbstractBroadcastReaperManager extends SparkListener implements
    BroadcastLifeCycleListener {
  private volatile BroadcastManager bcmInUse;

  private volatile boolean isRegistered = false;

  private volatile boolean isDriverSide = false;

  @Override
  public void  onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {
    this.clearAll();
  }

  // TODO: Asif see if this will suffice at executor end as executors will not get application end
  @Override
  public void onBroadcastManagerStop() {
    this.clearAll();
  }

  public void customCleanupOnApplicationEnd() {}

  public boolean isDriverSide() {
    return this.isDriverSide;
  }

  private void clearAll() {
    this.isRegistered = false;
    this.customCleanupOnApplicationEnd();
  }

  protected void checkInstanceInitialized() {
    if (!this.isRegistered) {
      synchronized (this) {
        if (!this.isRegistered) {
          SparkEnv sparkEnv = SparkEnv.get();
          if (sparkEnv != null) {
            if (SparkContext.DRIVER_IDENTIFIER().equals(sparkEnv.executorId())){
              this.isDriverSide = true;
              // means we are in driver so register as life cycle listener else don't
              SparkContext.getOrCreate().addSparkListener(this);
            }
            this.bcmInUse = sparkEnv.broadcastManager();
            this.bcmInUse.registerBroadcastLifeCycleListener(this);
            this.isRegistered = true;
          }
        }
      }
    }
  }
}
