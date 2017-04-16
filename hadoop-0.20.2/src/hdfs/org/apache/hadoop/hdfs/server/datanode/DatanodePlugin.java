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


package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.util.ServicePlugin;

public abstract class DatanodePlugin implements ServicePlugin {

  // ServicePlugin hooks

  /** {@inheritDoc} */
  public abstract void start(Object obj);

  /** {@inheritDoc} */
  public abstract void stop();

  // Datanode specific hooks

  /**
   * The DataNode has registered itself with the NameNode during the initial
   * startup sequence. This is called before the DataNode Thread has started
   * running.
   */
  public void initialRegistrationComplete() {}

  /**
   * The DataNode has re-registered itself with the NameNode in response to a
   * DNA_REGISTER event. This occurs when the NN has lost and regained contact
   * with the DataNode.
   */
  public void reregistrationComplete() {}
}