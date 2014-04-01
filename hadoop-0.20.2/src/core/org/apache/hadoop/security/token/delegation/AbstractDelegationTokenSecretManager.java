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

package org.apache.hadoop.security.token.delegation;

//import org.apache.hadoop.classification.InterfaceAudience;
//import static org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate.Project.HDFS;
//import static org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate.Project.MAPREDUCE;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.crypto.SecretKey;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.KerberosName;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.StringUtils;

//@InterfaceAudience.LimitedPrivate({HDFS, MAPREDUCE})
public abstract 
class AbstractDelegationTokenSecretManager<TokenIdent 
extends AbstractDelegationTokenIdentifier> 
   extends SecretManager<TokenIdent> {
  private static final Log LOG = LogFactory
      .getLog(AbstractDelegationTokenSecretManager.class);

  /** 
   * Cache of currently valid tokens, mapping from DelegationTokenIdentifier 
   * to DelegationTokenInformation. Protected by this object lock.
   */
  protected final Map<TokenIdent, DelegationTokenInformation> currentTokens 
      = new HashMap<TokenIdent, DelegationTokenInformation>();
  
  /**
   * Sequence number to create DelegationTokenIdentifier.
   * Protected by this object lock.
   */
  protected int delegationTokenSequenceNumber = 0;
  
  /**
   * Access to allKeys is protected by this object lock
   */
  protected final Map<Integer, DelegationKey> allKeys 
      = new HashMap<Integer, DelegationKey>();
  
  /**
   * Access to currentId is protected by this object lock.
   */
  protected int currentId = 0;
  /**
   * Access to currentKey is protected by this object lock
   */
  private DelegationKey currentKey;
  
  private long keyUpdateInterval;
  private long tokenMaxLifetime;
  private long tokenRemoverScanInterval;
  private long tokenRenewInterval;
  private Thread tokenRemoverThread;
  protected volatile boolean running;

  public AbstractDelegationTokenSecretManager(long delegationKeyUpdateInterval,
      long delegationTokenMaxLifetime, long delegationTokenRenewInterval,
      long delegationTokenRemoverScanInterval) {
    this.keyUpdateInterval = delegationKeyUpdateInterval;
    this.tokenMaxLifetime = delegationTokenMaxLifetime;
    this.tokenRenewInterval = delegationTokenRenewInterval;
    this.tokenRemoverScanInterval = delegationTokenRemoverScanInterval;
  }

  /** should be called before this object is used */
  public void startThreads() throws IOException {
    updateCurrentKey();
    synchronized (this) {
      running = true;
      tokenRemoverThread = new Daemon(new ExpiredTokenRemover());
      tokenRemoverThread.start();
    }
  }
  
  /**
   * is secretMgr running
   * @return true if secret mgr is running
   */
  public synchronized boolean isRunning() {
    return running;
  }
  
  /** 
   * Add a previously used master key to cache (when NN restarts), 
   * should be called before activate().
   * */
  public synchronized void addKey(DelegationKey key) throws IOException {
    if (running) // a safety check
      throw new IOException("Can't add delegation key to a running SecretManager.");
    if (key.getKeyId() > currentId) {
      currentId = key.getKeyId();
    }
    allKeys.put(key.getKeyId(), key);
  }

  public synchronized DelegationKey[] getAllKeys() {
    return allKeys.values().toArray(new DelegationKey[0]);
  }
  
  protected void logUpdateMasterKey(DelegationKey key) throws IOException {
    return;
  }
  
  /** 
   * Update the current master key 
   * This is called once by startThreads before tokenRemoverThread is created, 
   * and only by tokenRemoverThread afterwards.
   */
  private void updateCurrentKey() throws IOException {
    LOG.info("Updating the current master key for generating delegation tokens");
    /* Create a new currentKey with an estimated expiry date. */
    int newCurrentId;
    synchronized (this) {
      newCurrentId = currentId+1;
    }
    DelegationKey newKey = new DelegationKey(newCurrentId, System
        .currentTimeMillis()
        + keyUpdateInterval + tokenMaxLifetime, generateSecret());
    //Log must be invoked outside the lock on 'this'
    logUpdateMasterKey(newKey);
    synchronized (this) {
      currentId = newKey.getKeyId();
      currentKey = newKey;
      allKeys.put(currentKey.getKeyId(), currentKey);
    }
  }
  
  /** 
   * Update the current master key for generating delegation tokens 
   * It should be called only by tokenRemoverThread.
   */
  void rollMasterKey() throws IOException {
    synchronized (this) {
      removeExpiredKeys();
      /* set final expiry date for retiring currentKey */
      currentKey.setExpiryDate(System.currentTimeMillis() + tokenMaxLifetime);
      /*
       * currentKey might have been removed by removeExpiredKeys(), if
       * updateMasterKey() isn't called at expected interval. Add it back to
       * allKeys just in case.
       */
      allKeys.put(currentKey.getKeyId(), currentKey);
    }
    updateCurrentKey();
  }

  private synchronized void removeExpiredKeys() {
    long now = System.currentTimeMillis();
    for (Iterator<Map.Entry<Integer, DelegationKey>> it = allKeys.entrySet()
        .iterator(); it.hasNext();) {
      Map.Entry<Integer, DelegationKey> e = it.next();
      if (e.getValue().getExpiryDate() < now) {
        it.remove();
      }
    }
  }
  
  @Override
  protected synchronized byte[] createPassword(TokenIdent identifier) {
    LOG.info("Creating password for identifier: "+identifier);
    int sequenceNum;
    long now = System.currentTimeMillis();
    sequenceNum = ++delegationTokenSequenceNumber;
    identifier.setIssueDate(now);
    identifier.setMaxDate(now + tokenMaxLifetime);
    identifier.setMasterKeyId(currentId);
    identifier.setSequenceNumber(sequenceNum);
    byte[] password = createPassword(identifier.getBytes(), currentKey.getKey());
    currentTokens.put(identifier, new DelegationTokenInformation(now
        + tokenRenewInterval, password));
    return password;
  }

  @Override
  public synchronized byte[] retrievePassword(TokenIdent identifier)
      throws InvalidToken {
    DelegationTokenInformation info = currentTokens.get(identifier);
    if (info == null) {
      throw new InvalidToken("token (" + identifier.toString()
          + ") can't be found in cache");
    }
    long now = System.currentTimeMillis();
    if (info.getRenewDate() < now) {
      throw new InvalidToken("token (" + identifier.toString() + ") is expired");
    }
    return info.getPassword();
  }

  /**
   * Renew a delegation token.
   * @param token the token to renew
   * @param renewer the full principal name of the user doing the renewal
   * @return the new expiration time
   * @throws InvalidToken if the token is invalid
   * @throws AccessControlException if the user can't renew token
   */
  public synchronized long renewToken(Token<TokenIdent> token,
                         String renewer) throws InvalidToken, IOException {
    long now = System.currentTimeMillis();
    ByteArrayInputStream buf = new ByteArrayInputStream(token.getIdentifier());
    DataInputStream in = new DataInputStream(buf);
    TokenIdent id = createIdentifier();
    id.readFields(in);
    LOG.info("Token renewal requested for identifier: "+id);
    
    if (id.getMaxDate() < now) {
      throw new InvalidToken("User " + renewer + 
                             " tried to renew an expired token");
    }
    if ((id.getRenewer() == null) || ("".equals(id.getRenewer().toString()))) {
      throw new AccessControlException("User " + renewer + 
                                       " tried to renew a token without " +
                                       "a renewer");
    }
    if (!id.getRenewer().toString().equals(renewer)) {
      throw new AccessControlException("Client " + renewer + 
                                       " tries to renew a token with " +
                                       "renewer specified as " + 
                                       id.getRenewer());
    }
    DelegationKey key = allKeys.get(id.getMasterKeyId());
    if (key == null) {
      throw new InvalidToken("Unable to find master key for keyId="
          + id.getMasterKeyId()
          + " from cache. Failed to renew an unexpired token"
          + " with sequenceNumber=" + id.getSequenceNumber());
    }
    byte[] password = createPassword(token.getIdentifier(), key.getKey());
    if (!Arrays.equals(password, token.getPassword())) {
      throw new AccessControlException("Client " + renewer
          + " is trying to renew a token with " + "wrong password");
    }
    long renewTime = Math.min(id.getMaxDate(), now + tokenRenewInterval);
    DelegationTokenInformation info = new DelegationTokenInformation(renewTime,
        password);

    if (currentTokens.get(id) == null) {
      throw new InvalidToken("Renewal request for unknown token");
    }
    currentTokens.put(id, info);
    return renewTime;
  }
  
  /**
   * Cancel a token by removing it from cache.
   * @return Identifier of the canceled token
   * @throws InvalidToken for invalid token
   * @throws AccessControlException if the user isn't allowed to cancel
   */
  public synchronized TokenIdent cancelToken(Token<TokenIdent> token,
      String canceller) throws IOException {
    ByteArrayInputStream buf = new ByteArrayInputStream(token.getIdentifier());
    DataInputStream in = new DataInputStream(buf);
    TokenIdent id = createIdentifier();
    id.readFields(in);
    LOG.info("Token cancelation requested for identifier: "+id);
    
    if (id.getUser() == null) {
      throw new InvalidToken("Token with no owner");
    }
    String owner = id.getUser().getUserName();
    Text renewer = id.getRenewer();
    KerberosName cancelerKrbName = new KerberosName(canceller);
    String cancelerShortName = cancelerKrbName.getShortName();
    if (!canceller.equals(owner)
        && (renewer == null || "".equals(renewer.toString()) || !cancelerShortName
            .equals(renewer.toString()))) {
      throw new AccessControlException(canceller
          + " is not authorized to cancel the token");
    }
    DelegationTokenInformation info = null;
    info = currentTokens.remove(id);
    if (info == null) {
      throw new InvalidToken("Token not found");
    }
    return id;
  }
  
  /**
   * Convert the byte[] to a secret key
   * @param key the byte[] to create the secret key from
   * @return the secret key
   */
  public static SecretKey createSecretKey(byte[] key) {
    return SecretManager.createSecretKey(key);
  }

  /** Class to encapsulate a token's renew date and password. */
  public static class DelegationTokenInformation {
    long renewDate;
    byte[] password;
    public DelegationTokenInformation(long renewDate, byte[] password) {
      this.renewDate = renewDate;
      this.password = password;
    }
    /** returns renew date */
    public long getRenewDate() {
      return renewDate;
    }
    /** returns password */
    byte[] getPassword() {
      return password;
    }
  }
  
  /** Remove expired delegation tokens from cache */
  private synchronized void removeExpiredToken() {
    long now = System.currentTimeMillis();
    Iterator<DelegationTokenInformation> i = currentTokens.values().iterator();
    while (i.hasNext()) {
      long renewDate = i.next().getRenewDate();
      if (now > renewDate) {
        i.remove();
      }
    }
  }

  public synchronized void stopThreads() {
    if (LOG.isDebugEnabled())
      LOG.debug("Stopping expired delegation token remover thread");
    running = false;
    if (tokenRemoverThread != null) {
      tokenRemoverThread.interrupt();
    }
  }
  
  private class ExpiredTokenRemover extends Thread {
    private long lastMasterKeyUpdate;
    private long lastTokenCacheCleanup;

    public void run() {
      LOG.info("Starting expired delegation token remover thread, "
          + "tokenRemoverScanInterval=" + tokenRemoverScanInterval
          / (60 * 1000) + " min(s)");
      try {
        while (running) {
          long now = System.currentTimeMillis();
          if (lastMasterKeyUpdate + keyUpdateInterval < now) {
            try {
              rollMasterKey();
              lastMasterKeyUpdate = now;
            } catch (IOException e) {
              LOG.error("Master key updating failed. "
                  + StringUtils.stringifyException(e));
            }
          }
          if (lastTokenCacheCleanup + tokenRemoverScanInterval < now) {
            removeExpiredToken();
            lastTokenCacheCleanup = now;
          }
          try {
            Thread.sleep(5000); // 5 seconds
          } catch (InterruptedException ie) {
            LOG
            .error("InterruptedExcpetion recieved for ExpiredTokenRemover thread "
                + ie);
          }
        }
      } catch (Throwable t) {
        LOG.error("ExpiredTokenRemover thread received unexpected exception. "
            + t);
        Runtime.getRuntime().exit(-1);
      }
    }
  }
}
