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

import java.io.IOException;
import java.io.File;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Enumeration;

import java.net.URI;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.tools.DFSAdmin;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenInfo;
import org.junit.Test;
import org.apache.hadoop.ipc.TestSaslRPC;
import org.apache.hadoop.ipc.TestSaslRPC.TestTokenSecretManager;
import org.apache.hadoop.ipc.TestSaslRPC.TestTokenIdentifier;
import org.apache.hadoop.ipc.TestSaslRPC.TestTokenSelector;

import org.apache.hadoop.filecache.DistributedCache;

import org.apache.commons.logging.*;

/**
 *
 */
public class TestDistributedCacheAlternateFileSystem {
  final private static String REAL_USER_NAME = "realUser1@HADOOP.APACHE.ORG";
  final private static String REAL_USER_SHORT_NAME = "realUser1";
  final private static String PROXY_USER_NAME = "proxyUser";
  final private static String GROUP1_NAME = "group1";
  final private static String GROUP2_NAME = "group2";
  final private static String[] GROUP_NAMES = new String[] { GROUP1_NAME,
      GROUP2_NAME };
  private static final String ADDRESS = "0.0.0.0";
  private TestProtocol proxy;
  private static Configuration masterConf = new Configuration();

  final private static String ALTERNATE_FILE_BASE = "gqlpt";
  
  public static final Log LOG = LogFactory
      .getLog(TestDistributedCacheAlternateFileSystem.class);
  

  static {
    masterConf.set("hadoop.security.auth_to_local",
        "RULE:[2:$1@$0](.*@HADOOP.APACHE.ORG)s/@.*//" +
        "RULE:[1:$1@$0](.*@HADOOP.APACHE.ORG)s/@.*//"
        + "DEFAULT");
    UserGroupInformation.setConfiguration(masterConf);
  }
  
  private void configureSuperUserIPAddresses(Configuration conf,
      String superUserShortName) throws IOException {
    ArrayList<String> ipList = new ArrayList<String>();
    Enumeration<NetworkInterface> netInterfaceList = NetworkInterface
        .getNetworkInterfaces();
    while (netInterfaceList.hasMoreElements()) {
      NetworkInterface inf = netInterfaceList.nextElement();
      Enumeration<InetAddress> addrList = inf.getInetAddresses();
      while (addrList.hasMoreElements()) {
        InetAddress addr = addrList.nextElement();
        ipList.add(addr.getHostAddress());
      }
    }
    StringBuilder builder = new StringBuilder();
    for (String ip : ipList) {
      builder.append(ip);
      builder.append(',');
    }
    builder.append("127.0.1.1,");
    builder.append(InetAddress.getLocalHost().getCanonicalHostName());
    LOG.info("Local Ip addresses: "+builder.toString());
    conf.setStrings(ProxyUsers.getProxySuperuserIpConfKey(superUserShortName),
        builder.toString());
  }

  private FileSystem getFS
          (UserGroupInformation ugi, final Configuration conf)
       throws IOException {
    final Path sysDir = new Path("/" + ALTERNATE_FILE_BASE); // getSystemDir()

    try {
      return ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
                        public FileSystem run() throws IOException {
                          return sysDir.getFileSystem(conf);
                        }
                      });
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private void addToClasspath(final FileSystem proxyUserFileSystem,
                              final FileSystem realUserFileSystem,
                              final UserGroupInformation ugi,
                              final String corePathString,
                              final Configuration conf)
    throws IOException, InterruptedException {
    ugi.doAs(new PrivilegedExceptionAction<Boolean>() {
               public Boolean run()throws IOException {
                 DistributedCache.addFileToClassPath
                   (new Path("proxy-fs-as-" + corePathString), conf,
                    proxyUserFileSystem);
                 DistributedCache.addFileToClassPath
                   (new Path("real-fs-as-" + corePathString), conf,
                    realUserFileSystem);
                 DistributedCache.addFileToClassPath
                   (new Path("no-fs-as-" + corePathString), conf);
                 return true;
               }
             });
  }

  private static boolean uriUsesProxyFS(URI uri) {
    String uriString = uri.toString();

    return uriString.contains("file:/" + ALTERNATE_FILE_BASE + "/");
  }

  private static boolean uriShouldUseProxyFS(URI uri) {
    String uriString = uri.toString();

    int filenameStart = uriString.lastIndexOf(File.separator) + 1;

    String fileName = uriString.substring(filenameStart);

    int lastProxyOccurrence = uriString.lastIndexOf("proxy");

    return (fileName.contains("proxy-fs-as")
            || (fileName.contains("no-fs-as") && fileName.contains("as-proxy")));
  }

  @Test
  public void testDistributedCacheProxyUsers() throws Exception {
    // ensure that doAs works correctly
    UserGroupInformation realUserUgi = UserGroupInformation
        .createRemoteUser(REAL_USER_NAME);
    UserGroupInformation proxyUserUgi = UserGroupInformation.createProxyUser(
        PROXY_USER_NAME, realUserUgi);
    UserGroupInformation curUGI = proxyUserUgi
        .doAs(new PrivilegedExceptionAction<UserGroupInformation>() {
          public UserGroupInformation run() throws IOException {
            return UserGroupInformation.getCurrentUser();
          }
        });
    Assert.assertEquals(curUGI.toString(),
        PROXY_USER_NAME + " via " + REAL_USER_NAME + " (auth:SIMPLE) (auth:PROXY)");


    final Configuration conf = new Configuration();

    final FileSystem realUserFileSystem = getFS(realUserUgi, conf); 

    FileSystem proxyUserFileSystemTemp;

    String oldWorkingDir = System.getProperty("user.dir");
    try {
      System.setProperty("user.dir", "/" + ALTERNATE_FILE_BASE);
      
      proxyUserFileSystemTemp = getFS(proxyUserUgi, conf);
    } finally {
      System.setProperty("user.dir", oldWorkingDir);
    }

    final FileSystem proxyUserFileSystem = proxyUserFileSystemTemp;

    addToClasspath(proxyUserFileSystem, realUserFileSystem,
                   realUserUgi, "real.jar", conf);
    addToClasspath(proxyUserFileSystem, realUserFileSystem,
                   proxyUserUgi, "proxy.jar", conf);

    URI[] result = DistributedCache.getCacheFiles(conf);

    for (URI uri : result) {
      System.out.println("One URI is " + uri);
    }

    for (URI uri : result) {
      Assert.assertEquals("Inconsistent file system usage for URI " + uri,
                          uriUsesProxyFS(uri),
                          uriShouldUseProxyFS(uri));
    }
  }


  @TokenInfo(TestTokenSelector.class)
  public interface TestProtocol extends VersionedProtocol {
    public static final long versionID = 1L;

    String aMethod() throws IOException;
  }

  public class TestImpl implements TestProtocol {

    public String aMethod() throws IOException {
      return UserGroupInformation.getCurrentUser().toString();
    }

    public long getProtocolVersion(String protocol, long clientVersion)
        throws IOException {
      // TODO Auto-generated method stub
      return TestProtocol.versionID;
    }
  }

  //
  private void refreshConf(Configuration conf) throws IOException {
    ProxyUsers.refreshSuperUserGroupsConfiguration(conf);
  }
}
