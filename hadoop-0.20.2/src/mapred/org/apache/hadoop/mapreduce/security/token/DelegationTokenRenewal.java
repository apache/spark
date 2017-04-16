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

package org.apache.hadoop.mapreduce.security.token;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;

import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;


//@InterfaceAudience.Private
public class DelegationTokenRenewal {
  private static final Log LOG = LogFactory.getLog(DelegationTokenRenewal.class);
  public static final String SCHEME = "hdfs";
  
  /**
   * class that is used for keeping tracks of DT to renew
   *
   */
  private static class DelegationTokenToRenew {
    public final Token<DelegationTokenIdentifier> token;
    public final JobID jobId;
    public final Configuration conf;
    public long expirationDate;
    public TimerTask timerTask;
    
    public DelegationTokenToRenew(
        JobID jId, Token<DelegationTokenIdentifier> t, 
        Configuration newConf, long newExpirationDate) {
      token = t;
      jobId = jId;
      conf = newConf;
      expirationDate = newExpirationDate;
      timerTask = null;
      if(token==null || jobId==null || conf==null) {
        throw new IllegalArgumentException("invalid params for Renew Token" +
            ";t="+token+";j="+jobId+";c="+conf);
      }
    }
    public void setTimerTask(TimerTask tTask) {
      timerTask = tTask;
    }
    @Override
    public String toString() {
      return token + ";exp="+expirationDate;
    }
    @Override
    public boolean equals(Object obj) {
      return obj instanceof DelegationTokenToRenew &&
        token.equals(((DelegationTokenToRenew)obj).token);
    }
    @Override
    public int hashCode() {
      return token.hashCode();
    }
  }
  
  // global single timer (daemon)
  private static Timer renewalTimer = new Timer(true);
  
  //managing the list of tokens using Map
  // jobId=>List<tokens>
  private static Set<DelegationTokenToRenew> delegationTokens = 
    Collections.synchronizedSet(new HashSet<DelegationTokenToRenew>());
  //adding token
  private static void addTokenToList(DelegationTokenToRenew t) {
    delegationTokens.add(t);
  }
  
  // kind of tokens we currently renew
  private static final Text kindHdfs = 
    DelegationTokenIdentifier.HDFS_DELEGATION_KIND;
  
  @SuppressWarnings("unchecked")
  public static synchronized void registerDelegationTokensForRenewal(
      JobID jobId, Credentials ts, Configuration conf) {
    if(ts==null)
      return; //nothing to add
    
    Collection <Token<? extends TokenIdentifier>> tokens = ts.getAllTokens();
    long now = System.currentTimeMillis();
    
    for(Token<? extends TokenIdentifier> t : tokens) {
      // currently we only check for HDFS delegation tokens
      // later we can add more different types.
      if(! t.getKind().equals(kindHdfs)) {
        continue; 
      }
      Token<DelegationTokenIdentifier> dt = 
        (Token<DelegationTokenIdentifier>)t;
      
      // first renew happens immediately
      DelegationTokenToRenew dtr = 
        new DelegationTokenToRenew(jobId, dt, conf, now); 

      addTokenToList(dtr);
      
      setTimerForTokenRenewal(dtr, true);
      LOG.info("registering token for renewal for service =" + dt.getService()+
          " and jobID = " + jobId);
    }
  }
  
  private static long renewDelegationToken(DelegationTokenToRenew dttr) 
  throws Exception {
    long newExpirationDate=System.currentTimeMillis()+3600*1000;
    Token<DelegationTokenIdentifier> token = dttr.token;
    Configuration conf = dttr.conf;
    
    if(token.getKind().equals(kindHdfs)) {
      try {
        DistributedFileSystem dfs = getDFSForToken(token, conf);
        newExpirationDate = dfs.renewDelegationToken(token);
      } catch (InvalidToken ite) {
        LOG.warn("invalid token - not scheduling for renew");
        removeFailedDelegationToken(dttr);
        throw new IOException("failed to renew token", ite);
      } catch (AccessControlException ioe) {
        LOG.warn("failed to renew token:"+token, ioe);
        removeFailedDelegationToken(dttr);
        throw new IOException("failed to renew token", ioe);
      } catch (Exception e) {
        LOG.warn("failed to renew token:"+token, e);
        // returns default expiration date
      }
    } else {
      throw new Exception("unknown token type to renew+"+token.getKind());
    }
    return newExpirationDate;
  }

  
  /**
   * Task - to renew a token
   *
   */
  private static class RenewalTimerTask extends TimerTask {
    private DelegationTokenToRenew dttr;
    
    RenewalTimerTask(DelegationTokenToRenew t) {  dttr = t;  }
    
    @Override
    public void run() {
      Token<DelegationTokenIdentifier> token = dttr.token;
      long newExpirationDate=0;
      try {
        newExpirationDate = renewDelegationToken(dttr);
      } catch (Exception e) {
        return; // message logged in renewDT method
      }
      if (LOG.isDebugEnabled())
        LOG.debug("renewing for:"+token.getService()+";newED=" + 
            newExpirationDate);
      
      // new expiration date
      dttr.expirationDate = newExpirationDate;
      setTimerForTokenRenewal(dttr, false);// set the next one
    }
  }
  
  private static DistributedFileSystem getDFSForToken(
      Token<DelegationTokenIdentifier> token, final Configuration conf) 
  throws Exception {
    DistributedFileSystem dfs = null;
    try {
      //TODO: The service is usually an IPaddress:port. We convert
      //it to dns name and then obtain the filesystem just so that
      //we reuse the existing filesystem handle (that the jobtracker
      //might have for this namenode; the namenode is usually
      //specified as the dns name in the jobtracker).
      //THIS IS A WORKAROUND FOR NOW. NEED TO SOLVE THIS PROBLEM 
      //IN A BETTER WAY.
      String[] ipaddr = token.getService().toString().split(":");
      InetAddress iaddr = InetAddress.getByName(ipaddr[0]);
      String dnsName = iaddr.getCanonicalHostName();
      final URI uri = new URI (SCHEME + "://" + dnsName+":"+ipaddr[1]);
      dfs = (DistributedFileSystem)
      UserGroupInformation.getLoginUser().doAs(
          new PrivilegedExceptionAction<DistributedFileSystem>() {
        public DistributedFileSystem run() throws IOException {
          return (DistributedFileSystem) FileSystem.get(uri, conf);  
        }
      });

      
    } catch (Exception e) {
      LOG.warn("Failed to create a dfs to renew for:" + token.getService(), e);
      throw e;
    } 
    return dfs;
  }
  
  /**
   * set task to renew the token
   */
  private static void setTimerForTokenRenewal(
      DelegationTokenToRenew token, boolean firstTime) {
      
    // calculate timer time
    long now = System.currentTimeMillis();
    long renewIn;
    if(firstTime) {
      renewIn = now;
    } else {
      long expiresIn = (token.expirationDate - now); 
      renewIn = now + expiresIn - expiresIn/10; // little bit before the expiration
    }
    
    try {
      // need to create new task every time
      TimerTask tTask = new RenewalTimerTask(token);
      token.setTimerTask(tTask); // keep reference to the timer

      renewalTimer.schedule(token.timerTask, new Date(renewIn));
    } catch (Exception e) {
      LOG.warn("failed to schedule a task, token will not renew more", e);
    }
  }

  /**
   * removing all tokens renewals
   */
  static public void close() {
    renewalTimer.cancel();
    delegationTokens.clear();
  }
  
  // cancel a token
  private static void cancelToken(DelegationTokenToRenew t) {
    Token<DelegationTokenIdentifier> token = t.token;
    Configuration conf = t.conf;
    
    if(token.getKind().equals(kindHdfs)) {
      try {
        DistributedFileSystem dfs = getDFSForToken(token, conf);
        if (LOG.isDebugEnabled())
          LOG.debug("canceling token " + token.getService() + " for dfs=" +
              dfs);
        dfs.cancelDelegationToken(token);
      } catch (Exception e) {
        LOG.warn("Failed to cancel " + token, e);
      }
    }
  }
  
  /**
   * removing failed DT
   * @param jobId
   */
  private static void removeFailedDelegationToken(DelegationTokenToRenew t) {
    JobID jobId = t.jobId;
    if (LOG.isDebugEnabled())
      LOG.debug("removing failed delegation token for jobid=" + jobId + 
          ";t=" + t.token.getService());
    delegationTokens.remove(t);
    // cancel the timer
    if(t.timerTask!=null)
      t.timerTask.cancel();
  }
  
  /**
   * removing DT for completed jobs
   * @param jobId
   */
  public static void removeDelegationTokenRenewalForJob(JobID jobId) {
    synchronized (delegationTokens) {
      Iterator<DelegationTokenToRenew> it = delegationTokens.iterator();
      while(it.hasNext()) {
        DelegationTokenToRenew dttr = it.next();
        if (dttr.jobId.equals(jobId)) {
          if (LOG.isDebugEnabled())
            LOG.debug("removing delegation token for jobid=" + jobId + 
                ";t=" + dttr.token.getService());

          // cancel the timer
          if(dttr.timerTask!=null)
            dttr.timerTask.cancel();

          // cancel the token
          cancelToken(dttr);

          it.remove();
        }
      }
    }
  }
}
