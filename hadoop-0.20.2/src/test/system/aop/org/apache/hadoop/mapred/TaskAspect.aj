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
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.mapred.Task.TaskReporter;
import org.apache.hadoop.mapreduce.test.system.FinishTaskControlAction;
import org.apache.hadoop.test.system.ControlAction;
import org.apache.hadoop.test.system.DaemonProtocol;
import org.apache.hadoop.mapreduce.test.system.TTProtocol;

public privileged aspect TaskAspect {

  private static final Log LOG = LogFactory.getLog(TaskAspect.class);
  
  private Object waitObject = new Object();
  private AtomicBoolean isWaitingForSignal = new AtomicBoolean(false);
  
  private DaemonProtocol daemonProxy;

  pointcut taskDoneIntercept(Task task) : execution(
      public void Task.done(..)) && target(task);
  
  void around(Task task) : taskDoneIntercept(task) {
    if(task.isJobCleanupTask() || task.isJobSetupTask() || task.isTaskCleanupTask()) {
      proceed(task);
      return;
    }
    Configuration conf = task.getConf();
    boolean controlEnabled = FinishTaskControlAction.isControlActionEnabled(conf);
    if(controlEnabled) {
      LOG.info("Task control enabled, waiting till client sends signal to " +
      "complete");
      try {
        synchronized (waitObject) {
          isWaitingForSignal.set(true);
          waitObject.wait();
        }
      } catch (InterruptedException e) {
      }
    }
    proceed(task);
    return;
  }
  
  pointcut taskStatusUpdate(TaskReporter reporter, TaskAttemptID id, JvmContext context) : 
    call(public boolean TaskUmbilicalProtocol.ping(TaskAttemptID, JvmContext))
          && this(reporter) && args(id, context);
  
  after(TaskReporter reporter, TaskAttemptID id, JvmContext context) throws IOException : 
    taskStatusUpdate(reporter, id, context)  {
    synchronized (waitObject) {
      if(isWaitingForSignal.get()) {
        ControlAction[] actions = daemonProxy.getActions(
            id.getTaskID());
        if(actions.length == 0) {
          return;
        }
        boolean shouldProceed = false;
        for(ControlAction action : actions) {
          if (action instanceof FinishTaskControlAction) {
            LOG.info("Recv : Control task action to finish task id: " 
                + action.getTarget());
            shouldProceed = true;
            daemonProxy.removeAction(action);
            LOG.info("Removed the control action from TaskTracker");
            break;
          }
        }
        if(shouldProceed) {
          LOG.info("Notifying the task to completion");
          waitObject.notify();
        }
      }
    }
  }
  
  
  pointcut rpcInterceptor(Class k, long version,InetSocketAddress addr, 
      Configuration conf) : call(
          public static * RPC.getProxy(Class, long ,InetSocketAddress,
              Configuration)) && args(k, version,addr, conf) && 
              within(org.apache.hadoop.mapred.Child) ;
  
  after(Class k, long version, InetSocketAddress addr, Configuration conf) 
    throws IOException : rpcInterceptor(k, version, addr, conf) {
    daemonProxy = 
      (TTProtocol) RPC.getProxy(
          TTProtocol.class, TTProtocol.versionID, addr, conf);
  }
  
}
