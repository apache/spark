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

package org.apache.hadoop.hdfsproxy;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;

/** Unit tests for ProxyUtil */
public class TestProxyUtil extends TestCase {
  
  private static String TEST_PROXY_CONF_DIR = System.getProperty("test.proxy.conf.dir", "./conf");
  private static String TEST_PROXY_HTTPS_PORT = System.getProperty("test.proxy.https.port", "8443");

  public void testSendCommand() throws Exception {
      
    Configuration conf = new Configuration(false);  
    conf.addResource("ssl-client.xml");
    conf.addResource("hdfsproxy-default.xml");
    String address = "localhost:" + TEST_PROXY_HTTPS_PORT;
    conf.set("hdfsproxy.https.address", address);
    String hostFname = TEST_PROXY_CONF_DIR + "/hdfsproxy-hosts";
    conf.set("hdfsproxy.hosts", hostFname);    
    
    assertTrue(ProxyUtil.sendCommand(conf, "/test/reloadPermFiles"));
    assertTrue(ProxyUtil.sendCommand(conf, "/test/clearUgiCache"));    
    
    conf.set("hdfsproxy.https.address", "localhost:0");
    assertFalse(ProxyUtil.sendCommand(conf, "/test/reloadPermFiles"));
    assertFalse(ProxyUtil.sendCommand(conf, "/test/reloadPermFiles"));
  }
 
}
