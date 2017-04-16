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

package org.apache.hadoop.hdfs.security.token.delegation;

//import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * A HDFS specific delegation token secret manager.
 * The secret manager is responsible for generating and accepting the password
 * for each token.
 */
//@InterfaceAudience.Private
public class DelegationTokenSecretManager
    extends AbstractDelegationTokenSecretManager<DelegationTokenIdentifier> {

  private static final Log LOG = LogFactory
      .getLog(DelegationTokenSecretManager.class);
  
  private final FSNamesystem namesystem;
  /**
   * Create a secret manager
   * @param delegationKeyUpdateInterval the number of seconds for rolling new
   *        secret keys.
   * @param delegationTokenMaxLifetime the maximum lifetime of the delegation
   *        tokens
   * @param delegationTokenRenewInterval how often the tokens must be renewed
   * @param delegationTokenRemoverScanInterval how often the tokens are scanned
   *        for expired tokens
   */
  public DelegationTokenSecretManager(long delegationKeyUpdateInterval,
      long delegationTokenMaxLifetime, long delegationTokenRenewInterval,
      long delegationTokenRemoverScanInterval, FSNamesystem namesystem) {
    super(delegationKeyUpdateInterval, delegationTokenMaxLifetime,
        delegationTokenRenewInterval, delegationTokenRemoverScanInterval);
    this.namesystem = namesystem;
  }

  @Override //SecretManager
  public DelegationTokenIdentifier createIdentifier() {
    return new DelegationTokenIdentifier();
  }

  /**
   * Returns expiry time of a token given its identifier.
   * 
   * @param dtId DelegationTokenIdentifier of a token
   * @return Expiry time of the token
   * @throws IOException
   */
  public synchronized long getTokenExpiryTime(
      DelegationTokenIdentifier dtId) throws IOException {
    DelegationTokenInformation info = currentTokens.get(dtId);
    if (info != null) {
      return info.getRenewDate();
    } else {
      throw new IOException("No delegation token found for this identifier");
    }
  }
  
  /**
   * Load SecretManager state from fsimage.
   * 
   * @param in input stream to read fsimage
   * @throws IOException
   */
  public synchronized void loadSecretManagerState(DataInputStream in)
      throws IOException {
    if (running) {
      // a safety check
      throw new IOException(
          "Can't load state from image in a running SecretManager.");
    }
    currentId = in.readInt();
    loadAllKeys(in);
    delegationTokenSequenceNumber = in.readInt();
    loadCurrentTokens(in);
  }
  
  /**
   * Store the current state of the SecretManager for persistence
   * 
   * @param out Output stream for writing into fsimage.
   * @throws IOException
   */
  public synchronized void saveSecretManagerState(DataOutputStream out)
      throws IOException {
    out.writeInt(currentId);
    saveAllKeys(out);
    out.writeInt(delegationTokenSequenceNumber);
    saveCurrentTokens(out);
  }
  
  /**
   * This method is intended to be used only while reading edit logs.
   * 
   * @param identifier DelegationTokenIdentifier read from the edit logs or
   * fsimage
   * 
   * @param expiryTime token expiry time
   * @throws IOException
   */
  public synchronized void addPersistedDelegationToken(
      DelegationTokenIdentifier identifier, long expiryTime) throws IOException {
    if (running) {
      // a safety check
      throw new IOException(
          "Can't add persisted delegation token to a running SecretManager.");
    }
    int keyId = identifier.getMasterKeyId();
    DelegationKey dKey = allKeys.get(keyId);
    if (dKey == null) {
      LOG
          .warn("No KEY found for persisted identifier "
              + identifier.toString());
      return;
    }
    byte[] password = createPassword(identifier.getBytes(), dKey.getKey());
    if (identifier.getSequenceNumber() > this.delegationTokenSequenceNumber) {
      this.delegationTokenSequenceNumber = identifier.getSequenceNumber();
    }
    if (currentTokens.get(identifier) == null) {
      currentTokens.put(identifier, new DelegationTokenInformation(expiryTime,
          password));
    } else {
      throw new IOException(
          "Same delegation token being added twice; invalid entry in fsimage or editlogs");
    }
  }

  /**
   * Add a MasterKey to the list of keys.
   * 
   * @param key DelegationKey
   * @throws IOException
   */
  public synchronized void updatePersistedMasterKey(DelegationKey key)
      throws IOException {
    addKey(key);
  }
  
  /**
   * Update the token cache with renewal record in edit logs.
   * 
   * @param identifier DelegationTokenIdentifier of the renewed token
   * @param expiryTime
   * @throws IOException
   */
  public synchronized void updatePersistedTokenRenewal(
      DelegationTokenIdentifier identifier, long expiryTime) throws IOException {
    if (running) {
      // a safety check
      throw new IOException(
          "Can't update persisted delegation token renewal to a running SecretManager.");
    }
    DelegationTokenInformation info = null;
    info = currentTokens.get(identifier);
    if (info != null) {
      int keyId = identifier.getMasterKeyId();
      byte[] password = createPassword(identifier.getBytes(), allKeys
          .get(keyId).getKey());
      currentTokens.put(identifier, new DelegationTokenInformation(expiryTime,
          password));
    }
  }

  /**
   *  Update the token cache with the cancel record in edit logs
   *  
   *  @param identifier DelegationTokenIdentifier of the canceled token
   *  @throws IOException
   */
  public synchronized void updatePersistedTokenCancellation(
      DelegationTokenIdentifier identifier) throws IOException {
    if (running) {
      // a safety check
      throw new IOException(
          "Can't update persisted delegation token renewal to a running SecretManager.");
    }
    currentTokens.remove(identifier);
  }
  
  /**
   * Returns the number of delegation keys currently stored.
   * @return number of delegation keys
   */
  public synchronized int getNumberOfKeys() {
    return allKeys.size();
  }

  /**
   * Private helper methods to save delegation keys and tokens in fsimage
   */
  private synchronized void saveCurrentTokens(DataOutputStream out)
      throws IOException {
    out.writeInt(currentTokens.size());
    Iterator<DelegationTokenIdentifier> iter = currentTokens.keySet()
        .iterator();
    while (iter.hasNext()) {
      DelegationTokenIdentifier id = iter.next();
      id.write(out);
      DelegationTokenInformation info = currentTokens.get(id);
      out.writeLong(info.getRenewDate());
    }
  }
  
  /*
   * Save the current state of allKeys
   */
  private synchronized void saveAllKeys(DataOutputStream out)
      throws IOException {
    out.writeInt(allKeys.size());
    Iterator<Integer> iter = allKeys.keySet().iterator();
    while (iter.hasNext()) {
      Integer key = iter.next();
      allKeys.get(key).write(out);
    }
  }
  
  /**
   * Private helper methods to load Delegation tokens from fsimage
   */
  private synchronized void loadCurrentTokens(DataInputStream in)
      throws IOException {
    int numberOfTokens = in.readInt();
    for (int i = 0; i < numberOfTokens; i++) {
      DelegationTokenIdentifier id = new DelegationTokenIdentifier();
      id.readFields(in);
      long expiryTime = in.readLong();
      addPersistedDelegationToken(id, expiryTime);
    }
  }

  /**
   * Private helper method to load delegation keys from fsimage.
   * @param in
   * @throws IOException
   */
  private synchronized void loadAllKeys(DataInputStream in) throws IOException {
    int numberOfKeys = in.readInt();
    for (int i = 0; i < numberOfKeys; i++) {
      DelegationKey value = new DelegationKey();
      value.readFields(in);
      addKey(value);
    }
  }

  /**
   * Call namesystem to update editlogs for new master key.
   */
  @Override //AbstractDelegationTokenManager
  protected void logUpdateMasterKey(DelegationKey key)
      throws IOException {
    namesystem.logUpdateMasterKey(key);
  }
}
