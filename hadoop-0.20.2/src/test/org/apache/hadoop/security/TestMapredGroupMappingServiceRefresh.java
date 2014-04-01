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

package org.apache.hadoop.security;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapred.tools.MRAdmin;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestMapredGroupMappingServiceRefresh {
  private MiniDFSCluster cluster;
  JobConf config;
  private static long groupRefreshTimeoutSec = 2;
  private String tempResource = null;
  private static final Log LOG = LogFactory
      .getLog(TestMapredGroupMappingServiceRefresh.class);
  
  public static class MockUnixGroupsMapping implements GroupMappingServiceProvider {
    private int i=0;
    
    @Override
    public List<String> getGroups(String user) throws IOException {
      String g1 = user + (10 * i + 1);
      String g2 = user + (10 * i + 2);
      List<String> l = new ArrayList<String>(2);
      l.add(g1);
      l.add(g2);
      i++;
      return l;
    }

    @Override
    public void cacheGroupsRefresh() throws IOException {
    }

    @Override
    public void cacheGroupsAdd(List<String> groups) throws IOException {
    }

  }
  
  @Before
  public void setUp() throws Exception {
    config = new JobConf(new Configuration());
    
    config.setClass("hadoop.security.group.mapping",
        TestMapredGroupMappingServiceRefresh.MockUnixGroupsMapping.class,
        GroupMappingServiceProvider.class);
    config.setLong("hadoop.security.groups.cache.secs",
        groupRefreshTimeoutSec);
    
    LOG.info("GROUP MAPPING class name=" + 
        config.getClass("hadoop.security.group.mapping",
        ShellBasedUnixGroupsMapping.class,GroupMappingServiceProvider.class).
        getName());
    
    Groups.getUserToGroupsMappingService(config);
    String namenodeUrl = "hdfs://localhost:" + "0";
    FileSystem.setDefaultUri(config, namenodeUrl);
    
    cluster = new MiniDFSCluster(0, config, 1, true, true, true,  null, null, 
        null, null);
    cluster.waitActive();
    URI uri = cluster.getFileSystem().getUri();
    
    MiniMRCluster miniMRCluster = new MiniMRCluster(0, uri.toString() , 
      3, null, null, config);
    
    config.set("mapred.job.tracker", "localhost:"+miniMRCluster.getJobTrackerPort());
    ProxyUsers.refreshSuperUserGroupsConfiguration(config);
  }

  @After
  public void tearDown() throws Exception {
    if(cluster!=null) {
      cluster.shutdown();
    }
    if(tempResource!=null) {
      File f = new File(tempResource);
      f.delete();
    }
  }
  
  @Test
  public void testGroupMappingRefresh() throws Exception {
    MRAdmin admin = new MRAdmin(config);
    String [] args = new String[] { "-refreshUserToGroupsMappings" };
    
    Groups groups = Groups.getUserToGroupsMappingService(config);
    String user = UserGroupInformation.getLoginUser().getShortUserName();
    System.out.println("first attempt:");
    List<String> g1 = groups.getGroups(user);
    String [] str_groups = new String [g1.size()];
    g1.toArray(str_groups);
    System.out.println(Arrays.toString(str_groups));
    
    System.out.println("second attempt, should be same:");
    List<String> g2 = groups.getGroups(user);
    g2.toArray(str_groups);
    System.out.println(Arrays.toString(str_groups));
    for(int i=0; i<g2.size(); i++) {
      assertEquals("Should be same group ", g1.get(i), g2.get(i));
    }
    // run refresh command
    admin.run(args);
    
    System.out.println("third attempt(after refresh command), should be different:");
    List<String> g3 = groups.getGroups(user);
    g3.toArray(str_groups);
    System.out.println(Arrays.toString(str_groups));
    for(int i=0; i<g3.size(); i++) {
      assertFalse("Should be different group ", g1.get(i).equals(g3.get(i)));
    }
    System.out.println("");
    
    // test time out
    Thread.sleep(groupRefreshTimeoutSec*1100);
    System.out.println("fourth attempt(after timeout), should be different:");
    List<String> g4 = groups.getGroups(user);
    g4.toArray(str_groups);
    System.out.println(Arrays.toString(str_groups));
    for(int i=0; i<g4.size(); i++) {
      assertFalse("Should be different group ", g3.get(i).equals(g4.get(i)));
    }
  }
  
  @Test
  public void testRefreshSuperUserGroupsConfiguration() throws Exception {
    final String SUPER_USER = "super_user";
    final String [] GROUP_NAMES1 = new String [] {"gr1" , "gr2"};
    final String [] GROUP_NAMES2 = new String [] {"gr3" , "gr4"};

    //keys in conf
    String userKeyGroups = ProxyUsers.getProxySuperuserGroupConfKey(SUPER_USER);
    String userKeyHosts = ProxyUsers.getProxySuperuserIpConfKey (SUPER_USER);

    config.set(userKeyGroups, "gr3,gr4,gr5"); // superuser can proxy for this group
    config.set(userKeyHosts,"127.0.0.1");
    ProxyUsers.refreshSuperUserGroupsConfiguration(config);

    UserGroupInformation ugi1 = mock(UserGroupInformation.class);
    UserGroupInformation ugi2 = mock(UserGroupInformation.class);
    UserGroupInformation suUgi = mock(UserGroupInformation.class);
    when(ugi1.getRealUser()).thenReturn(suUgi);
    when(ugi2.getRealUser()).thenReturn(suUgi);

    when(suUgi.getShortUserName()).thenReturn(SUPER_USER); // super user
    when(suUgi.getUserName()).thenReturn(SUPER_USER+"L"); // super user

    when(ugi1.getShortUserName()).thenReturn("user1");
    when(ugi2.getShortUserName()).thenReturn("user2");

    when(ugi1.getUserName()).thenReturn("userL1");
    when(ugi2.getUserName()).thenReturn("userL2");

    // set groups for users
    when(ugi1.getGroupNames()).thenReturn(GROUP_NAMES1);
    when(ugi2.getGroupNames()).thenReturn(GROUP_NAMES2);


    // check before
    try {
      ProxyUsers.authorize(ugi1, "127.0.0.1", config);
      fail("first auth for " + ugi1.getShortUserName() + " should've failed ");
    } catch (AuthorizationException e) {
      // expected
      System.err.println("auth for " + ugi1.getUserName() + " failed");
    }
    try {
      ProxyUsers.authorize(ugi2, "127.0.0.1", config);
      System.err.println("auth for " + ugi2.getUserName() + " succeeded");
      // expected
    } catch (AuthorizationException e) {
      fail("first auth for " + ugi2.getShortUserName() + " should've succeeded: " + e.getLocalizedMessage());
    }

    // refresh will look at configuration on the server side
    // add additional resource with the new value
    // so the server side will pick it up
    String rsrc = "testRefreshSuperUserGroupsConfiguration_rsrc.xml";
    addNewConfigResource(rsrc, userKeyGroups, "gr2", userKeyHosts, "127.0.0.1");  

    MRAdmin admin = new MRAdmin(config);
    String [] args = new String[]{"-refreshSuperUserGroupsConfiguration"};
    admin.run(args);

    try {
      ProxyUsers.authorize(ugi2, "127.0.0.1", config);
      fail("second auth for " + ugi2.getShortUserName() + " should've failed ");
    } catch (AuthorizationException e) {
      // expected
      System.err.println("auth for " + ugi2.getUserName() + " failed");
    }
    try {
      ProxyUsers.authorize(ugi1, "127.0.0.1", config);
      System.err.println("auth for " + ugi1.getUserName() + " succeeded");
      // expected
    } catch (AuthorizationException e) {
      fail("second auth for " + ugi1.getShortUserName() + " should've succeeded: " + e.getLocalizedMessage());
    }    
  }

  private void addNewConfigResource(String rsrcName, String keyGroup,
      String groups, String keyHosts, String hosts)  throws FileNotFoundException {
    // location for temp resource should be in CLASSPATH
    URL url = config.getResource("mapred-default.xml");
    Path p = new Path(url.getPath());
    Path dir = p.getParent();
    tempResource = dir.toString() + "/" + rsrcName;


    String newResource =
      "<configuration>"+
      "<property><name>" + keyGroup + "</name><value>"+groups+"</value></property>" +
      "<property><name>" + keyHosts + "</name><value>"+hosts+"</value></property>" +
      "</configuration>";
    PrintWriter writer = new PrintWriter(new FileOutputStream(tempResource));
    writer.println(newResource);
    writer.close();

    Configuration.addDefaultResource(rsrcName);
  }                                                                                                                                                                     


}
