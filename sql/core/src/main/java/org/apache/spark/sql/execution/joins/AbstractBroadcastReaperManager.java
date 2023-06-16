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

  private void clearAll() {
    this.isRegistered = false;
    this.customCleanupOnApplicationEnd();
  }

  public void customCleanupOnApplicationEnd() {
  }

  public boolean isDriverSide() {
    return this.isDriverSide;
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
