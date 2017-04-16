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
package org.apache.hadoop.mapred.tools;

import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobTracker;
import org.apache.hadoop.tools.GetGroupsBase;
import org.apache.hadoop.util.ToolRunner;

/**
 * MR implementation of a tool for getting the groups which a given user
 * belongs to.
 */
public class GetGroups extends GetGroupsBase {

  static {
    Configuration.addDefaultResource("mapred-default.xml");
    Configuration.addDefaultResource("mapred-site.xml");
  }
  
  GetGroups(Configuration conf) {
    super(conf);
  }
  
  GetGroups(Configuration conf, PrintStream out) {
    super(conf, out);
  }

  @Override
  protected InetSocketAddress getProtocolAddress(Configuration conf)
      throws IOException {
    return JobTracker.getAddress(conf);
  }

  public static void main(String[] argv) throws Exception {
    int res = ToolRunner.run(new GetGroups(new Configuration()), argv);
    System.exit(res);
  }
}