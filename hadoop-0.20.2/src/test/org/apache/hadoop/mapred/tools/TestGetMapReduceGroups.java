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

import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapred.tools.GetGroups;
import org.apache.hadoop.tools.GetGroupsTestBase;
import org.apache.hadoop.util.Tool;
import org.junit.After;
import org.junit.Before;

/**
 * Tests for the MR implementation of {@link GetGroups}
 */
public class TestGetMapReduceGroups extends GetGroupsTestBase {
  
  private MiniMRCluster cluster;

  @Before
  public void setUpJobTracker() throws IOException, InterruptedException {
    cluster = new MiniMRCluster(0, "file:///", 1);
    conf = cluster.createJobConf();
  }
  
  @After
  public void tearDownJobTracker() throws IOException {
    cluster.shutdown();
  }

  @Override
  protected Tool getTool(PrintStream o) {
    return new GetGroups(conf, o);
  }

}