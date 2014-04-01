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


import java.util.Hashtable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.test.system.MRCluster;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestPushConfig {
  private static MRCluster cluster;
  private static final Log LOG = LogFactory.getLog(
      TestPushConfig.class.getName());
  
  @BeforeClass
  public static void before() throws Exception {
    String [] expExcludeList = new String[2];
    expExcludeList[0] = "java.net.ConnectException";
    expExcludeList[1] = "java.io.IOException";
    
    cluster = MRCluster.createCluster(new Configuration());
    cluster.setExcludeExpList(expExcludeList);
    cluster.setUp();
  }

  @AfterClass
  public static void after() throws Exception {
    cluster.tearDown();
  }
  
  /**
   * This test about testing the pushConfig feature. The pushConfig functionality
   * available as part of the cluster process manager. The functionality takes
   * in local input directory and pushes all the files from the local to the 
   * remote conf directory. This functionality is required is change the config
   * on the fly and restart the cluster which will be used by other test cases
   * @throws Exception if an I/O error occurs.
   */
  @Test
  public void testPushConfig() throws Exception {
    final String DUMMY_CONFIG_STRING = "mapred.newdummy.conf";
    String confFile = "mapred-site.xml";
    Hashtable<String,Long> prop = new Hashtable<String,Long>();
    prop.put(DUMMY_CONFIG_STRING, 1L);
    Configuration daemonConf =  cluster.getJTClient().getProxy().getDaemonConf();
    Assert.assertTrue("Dummy varialble is expected to be null before restart.",
        daemonConf.get(DUMMY_CONFIG_STRING) == null);
    cluster.restartClusterWithNewConfig(prop, confFile);    
    Configuration newconf = cluster.getJTClient().getProxy().getDaemonConf();
    Assert.assertTrue("Extra varialble is expected to be set",
        newconf.get(DUMMY_CONFIG_STRING).equals("1"));
    cluster.restart();
    daemonConf = cluster.getJTClient().getProxy().getDaemonConf();
    Assert.assertTrue("Dummy variable is expected to be null after restart.",
        daemonConf.get(DUMMY_CONFIG_STRING) == null);   
  }  
 }
