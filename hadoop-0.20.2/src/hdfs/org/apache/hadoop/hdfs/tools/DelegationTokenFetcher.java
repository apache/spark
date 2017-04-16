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
package org.apache.hadoop.hdfs.tools;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.security.PrivilegedExceptionAction;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.namenode.CancelDelegationTokenServlet;
import org.apache.hadoop.hdfs.server.namenode.GetDelegationTokenServlet;
import org.apache.hadoop.hdfs.server.namenode.RenewDelegationTokenServlet;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.StringUtils;

/**
 * Fetch a DelegationToken from the current Namenode and store it in the
 * specified file.
 */
public class DelegationTokenFetcher {
  private static final String USAGE =
    "fetchdt retrieves delegation tokens (optionally over http)\n" +
    "and writes them to specified file.\n" +
    "Usage: fetchdt [--webservice <namenode http addr>] <output filename>";
  
  private final DistributedFileSystem dfs;
  private final UserGroupInformation ugi;
  private final DataOutputStream out;
  private final Configuration conf;
  private static final Log LOG = 
    LogFactory.getLog(DelegationTokenFetcher.class);
  
  static {
    // Enable Kerberos sockets
    System.setProperty("https.cipherSuites", "TLS_KRB5_WITH_3DES_EDE_CBC_SHA");
  }

  /**
   * Command-line interface
   */
  public static void main(final String [] args) throws Exception {
    // Login the current user
    UserGroupInformation.getCurrentUser().doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws Exception {
        
        if(args.length == 3 && "--webservice".equals(args[0])) {
          getDTfromRemoteIntoFile(args[1], args[2]);
          return null;
        }
        // avoid annoying mistake
        if(args.length == 1 && "--webservice".equals(args[0])) {
          System.out.println(USAGE);
          return null;
        }
        if(args.length != 1 || args[0].isEmpty()) {
          System.out.println(USAGE);
          return null;
        }
        
        DataOutputStream out = null;
        
        try {
          Configuration conf = new Configuration();
          DistributedFileSystem dfs = (DistributedFileSystem) FileSystem.get(conf);
          out = new DataOutputStream(new FileOutputStream(args[0]));
          UserGroupInformation ugi = UserGroupInformation.getCurrentUser();

          new DelegationTokenFetcher(dfs, out, ugi, conf).go();
          
          out.flush();
          System.out.println("Succesfully wrote token of size " + 
              out.size() + " bytes to "+ args[0]);
        } catch (IOException ioe) {
          System.out.println("Exception encountered:\n" +
              StringUtils.stringifyException(ioe));
        } finally {
          if(out != null) out.close();
        }
        return null;
      }
    });

  }
  
  public DelegationTokenFetcher(DistributedFileSystem dfs, 
      DataOutputStream out, UserGroupInformation ugi, Configuration conf) {
    checkNotNull("dfs", dfs); this.dfs = dfs;
    checkNotNull("out", out); this.out = out;
    checkNotNull("ugi", ugi); this.ugi = ugi;
    checkNotNull("conf",conf); this.conf = conf;
  }
  
  private void checkNotNull(String s, Object o) {
    if(o == null) throw new IllegalArgumentException(s + " cannot be null.");
  }

  public void go() throws IOException {
    String fullName = ugi.getUserName();
    String shortName = ugi.getShortUserName();
    Token<DelegationTokenIdentifier> token = 
      dfs.getDelegationToken(new Text(fullName));
    
    // Reconstruct the ip:port of the Namenode
    URI uri = FileSystem.getDefaultUri(conf);
    String nnAddress = 
      InetAddress.getByName(uri.getHost()).getHostAddress() + ":" + uri.getPort();
    token.setService(new Text(nnAddress));
    
    Credentials ts = new Credentials();
    ts.addToken(new Text(shortName), token);
    ts.writeTokenStorageToStream(out);
  }

  /**
   * Utility method to obtain a delegation token over http
   * @param nnHttpAddr Namenode http addr, such as http://namenode:50070
   */
  static public Credentials getDTfromRemote(String nnAddr, 
                                            String renewer) throws IOException {
    DataInputStream dis = null;

    try {
      StringBuffer url = new StringBuffer();
      if (renewer != null) {
        url.append(nnAddr).append(GetDelegationTokenServlet.PATH_SPEC).append("?").
        append(GetDelegationTokenServlet.RENEWER).append("=").append(renewer);
      } else {
        url.append(nnAddr).append(GetDelegationTokenServlet.PATH_SPEC);
      }
      System.out.println("Retrieving token from: " + url);
      URL remoteURL = new URL(url.toString());
      SecurityUtil.fetchServiceTicket(remoteURL);
      URLConnection connection = remoteURL.openConnection();

      InputStream in = connection.getInputStream();
      Credentials ts = new Credentials();
      dis = new DataInputStream(in);
      ts.readFields(dis);
      return ts;
    } catch (Exception e) {
      throw new IOException("Unable to obtain remote token", e);
    } finally {
      if(dis != null) dis.close();
    }
  }
  
  /**
   * Renew a Delegation Token.
   * @param nnAddr the NameNode's address
   * @param tok the token to renew
   * @return the Date that the token will expire next.
   * @throws IOException
   */
  static public long renewDelegationToken(String nnAddr,
                                          Token<DelegationTokenIdentifier> tok
                                          ) throws IOException {
    StringBuilder buf = new StringBuilder();
    buf.append(nnAddr);
    buf.append(RenewDelegationTokenServlet.PATH_SPEC);
    buf.append("?");
    buf.append(RenewDelegationTokenServlet.TOKEN);
    buf.append("=");
    buf.append(tok.encodeToUrlString());
    BufferedReader in = null;
    try {
      URL url = new URL(buf.toString());
      SecurityUtil.fetchServiceTicket(url);
      URLConnection connection = url.openConnection();
      in = new BufferedReader(new InputStreamReader
                              (connection.getInputStream()));
      long result = Long.parseLong(in.readLine());
      in.close();
      return result;
    } catch (IOException ie) {
      IOUtils.cleanup(LOG, in);
      throw ie;
    }
  }

  /**
   * Cancel a Delegation Token.
   * @param nnAddr the NameNode's address
   * @param tok the token to cancel
   * @throws IOException
   */
  static public void cancelDelegationToken(String nnAddr,
                                           Token<DelegationTokenIdentifier> tok
                                           ) throws IOException {
    StringBuilder buf = new StringBuilder();
    buf.append(nnAddr);
    buf.append(CancelDelegationTokenServlet.PATH_SPEC);
    buf.append("?");
    buf.append(CancelDelegationTokenServlet.TOKEN);
    buf.append("=");
    buf.append(tok.encodeToUrlString());
    BufferedReader in = null;
    try {
      URL url = new URL(buf.toString());
      SecurityUtil.fetchServiceTicket(url);
      HttpURLConnection connection = (HttpURLConnection) url.openConnection();
      if (connection.getResponseCode() != HttpURLConnection.HTTP_OK) {
        throw new IOException("Error cancelling token:" + 
                              connection.getResponseMessage());
      }
    } catch (IOException ie) {
      IOUtils.cleanup(LOG, in);
      throw ie;
    }
  }

  /**
   * Utility method to obtain a delegation token over http
   * @param nnHttpAddr Namenode http addr, such as http://namenode:50070
   * @param filename Name of file to store token in
   */
  static private void getDTfromRemoteIntoFile(String nnAddr, String filename) 
  throws IOException {
    Credentials ts = getDTfromRemote(nnAddr, null); 

    DataOutputStream file = new DataOutputStream(new FileOutputStream(filename));
    ts.writeTokenStorageToStream(file);
    file.flush();
    System.out.println("Successfully wrote token of " + file.size() 
        + " bytes  to " + filename);
  }
}
