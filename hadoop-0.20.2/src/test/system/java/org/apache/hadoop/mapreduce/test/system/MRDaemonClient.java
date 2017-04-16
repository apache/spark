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

package org.apache.hadoop.mapreduce.test.system;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.test.system.AbstractDaemonClient;
import org.apache.hadoop.test.system.DaemonProtocol;
import org.apache.hadoop.test.system.process.RemoteProcess;

/**
 * Base class for JobTracker and TaskTracker clients.
 */
public abstract class MRDaemonClient<PROXY extends DaemonProtocol> 
    extends AbstractDaemonClient<PROXY>{

  public MRDaemonClient(Configuration conf, RemoteProcess process)
      throws IOException {
    super(conf, process);
  }

  public String[] getMapredLocalDirs() throws IOException {
    return getProxy().getDaemonConf().getStrings("mapred.local.dir");
  }

  public String getLogDir() throws IOException {
    return getProcessInfo().getSystemProperties().get("hadoop.log.dir");
  }
}
