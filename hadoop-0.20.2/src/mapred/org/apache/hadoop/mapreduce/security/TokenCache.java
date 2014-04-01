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

package org.apache.hadoop.mapreduce.security;

import java.io.IOException;
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HftpFileSystem;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobTracker;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.KerberosName;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;

/**
 * This class provides user facing APIs for transferring secrets from
 * the job client to the tasks.
 * The secrets can be stored just before submission of jobs and read during
 * the task execution.   
 */
//@InterfaceStability.Evolving
public class TokenCache {
  
  private static final Log LOG = LogFactory.getLog(TokenCache.class);

  static final String MAPREDUCE_JOB_CREDENTIALS_BINARY = 
    "mapreduce.job.credentials.binary";

  /**
   * auxiliary method to get user's secret keys..
   * @param alias
   * @return secret key from the storage
   */
  public static byte[] getSecretKey(Credentials credentials, Text alias) {
    if(credentials == null)
      return null;
    return credentials.getSecretKey(alias);
  }

  /**
   * Convenience method to obtain delegation tokens from namenodes 
   * corresponding to the paths passed.
   * @param ps array of paths
   * @param conf configuration
   * @throws IOException
   */
  public static void obtainTokensForNamenodes(Credentials credentials,
                                              Path [] ps, Configuration conf) 
  throws IOException {
    if (!UserGroupInformation.isSecurityEnabled()) {
      return;
    }
    obtainTokensForNamenodesInternal(credentials, ps, conf);
  }

  /**
   * Remove jobtoken referrals which don't make sense in the context
   * of the task execution.
   *
   * @param conf configuration object.
   */
  public static void cleanUpTokenReferral(Configuration conf) {
    conf.unset(MAPREDUCE_JOB_CREDENTIALS_BINARY);
  }

  static void obtainTokensForNamenodesInternal(Credentials credentials,
                                               Path [] ps, 
                                               Configuration conf
                                               ) throws IOException {
    // get jobtracker principal id (for the renewer)
    KerberosName jtKrbName = new KerberosName(conf.get(JobTracker.JT_USER_NAME, ""));
    String delegTokenRenewer = jtKrbName.getShortName();
    boolean readFile = true;
    for(Path p: ps) {
      FileSystem fs = FileSystem.get(p.toUri(), conf);
      String fsName = fs.getCanonicalServiceName();
      if (TokenCache.getDelegationToken(credentials, fsName) == null) {
        //TODO: Need to come up with a better place to put
        //this block of code to do with reading the file
        if (readFile) {
          readFile = false;
          String binaryTokenFilename =
            conf.get(MAPREDUCE_JOB_CREDENTIALS_BINARY);
          if (binaryTokenFilename != null) {
            Credentials binary;
            try {
              binary = Credentials.readTokenStorageFile(new Path("file:///" +  
                  binaryTokenFilename), conf);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
            credentials.addAll(binary);
          }
          if (TokenCache.getDelegationToken(credentials, fsName) != null) {
            LOG.debug("DT for " + fsName  + " is already present");
            continue;
          }
        }
        Token<?> token = fs.getDelegationToken(delegTokenRenewer);
        if (token != null) {
          Text fsNameText = new Text(fsName);
          token.setService(fsNameText);
          credentials.addToken(fsNameText, token);
          LOG.info("Got dt for " + p + ";uri="+ fsName + 
                   ";t.service="+token.getService());
        }
      }
    }
  }

  /**
   * file name used on HDFS for generated job token
   */
  //@InterfaceAudience.Private
  public static final String JOB_TOKEN_HDFS_FILE = "jobToken";

  /**
   * conf setting for job tokens cache file name
   */
  //@InterfaceAudience.Private
  public static final String JOB_TOKENS_FILENAME = "mapreduce.job.jobTokenFile";
  private static final Text JOB_TOKEN = new Text("ShuffleAndJobToken");

  /**
   * 
   * @param namenode
   * @return delegation token
   */
  @SuppressWarnings("unchecked")
  //@InterfaceAudience.Private
  public static Token<DelegationTokenIdentifier> 
  getDelegationToken(Credentials credentials, String namenode) {
    return (Token<DelegationTokenIdentifier>)
        credentials.getToken(new Text(namenode));
  }

  /**
   * load job token from a file
   * @param conf
   * @throws IOException
   */
  //@InterfaceAudience.Private
  public static Credentials loadTokens(String jobTokenFile, Configuration conf) 
  throws IOException {
    Path localJobTokenFile = new Path ("file:///" + jobTokenFile);
    
    Credentials ts = 
      Credentials.readTokenStorageFile(localJobTokenFile, conf);

    if(LOG.isDebugEnabled()) {
      LOG.debug("Task: Loaded jobTokenFile from: "+
          localJobTokenFile.toUri().getPath() 
        +"; num of sec keys  = " + ts.numberOfSecretKeys() +
        " Number of tokens " + 
        ts.numberOfTokens());
    }
    return ts;
  }

  /**
   * store job token
   * @param t
   */
  //@InterfaceAudience.Private
  public static void setJobToken(Token<? extends TokenIdentifier> t, 
      Credentials credentials) {
    credentials.addToken(JOB_TOKEN, t);
  }
  /**
   * 
   * @return job token
   */
  //@InterfaceAudience.Private
  @SuppressWarnings("unchecked")
  public static Token<JobTokenIdentifier> getJobToken(Credentials credentials) {
    return (Token<JobTokenIdentifier>) credentials.getToken(JOB_TOKEN);
  }
}
