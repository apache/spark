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

package org.apache.hadoop.hdfs.security.token.block;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.Token;

/**
 * BlockTokenSecretManager can be instantiated in 2 modes, master mode and slave
 * mode. Master can generate new block keys and export block keys to slaves,
 * while slaves can only import and use block keys received from master. Both
 * master and slave can generate and verify block tokens. Typically, master mode
 * is used by NN and slave mode is used by DN.
 */
public class BlockTokenSecretManager extends
    SecretManager<BlockTokenIdentifier> {
  public static final Log LOG = LogFactory
      .getLog(BlockTokenSecretManager.class);
  public static final Token<BlockTokenIdentifier> DUMMY_TOKEN = new Token<BlockTokenIdentifier>();

  private final boolean isMaster;
  /*
   * keyUpdateInterval is the interval that NN updates its block keys. It should
   * be set long enough so that all live DN's and Balancer should have sync'ed
   * their block keys with NN at least once during each interval.
   */
  private final long keyUpdateInterval;
  private volatile long tokenLifetime;
  private int serialNo = new SecureRandom().nextInt();
  private BlockKey currentKey;
  private BlockKey nextKey;
  private Map<Integer, BlockKey> allKeys;

  public static enum AccessMode {
    READ, WRITE, COPY, REPLACE
  };

  /**
   * Constructor
   * 
   * @param isMaster
   * @param keyUpdateInterval
   * @param tokenLifetime
   * @throws IOException
   */
  public BlockTokenSecretManager(boolean isMaster, long keyUpdateInterval,
      long tokenLifetime) throws IOException {
    this.isMaster = isMaster;
    this.keyUpdateInterval = keyUpdateInterval;
    this.tokenLifetime = tokenLifetime;
    this.allKeys = new HashMap<Integer, BlockKey>();
    generateKeys();
  }

  /** Initialize block keys */
  private synchronized void generateKeys() {
    if (!isMaster)
      return;
    /*
     * Need to set estimated expiry dates for currentKey and nextKey so that if
     * NN crashes, DN can still expire those keys. NN will stop using the newly
     * generated currentKey after the first keyUpdateInterval, however it may
     * still be used by DN and Balancer to generate new tokens before they get a
     * chance to sync their keys with NN. Since we require keyUpdInterval to be
     * long enough so that all live DN's and Balancer will sync their keys with
     * NN at least once during the period, the estimated expiry date for
     * currentKey is set to now() + 2 * keyUpdateInterval + tokenLifetime.
     * Similarly, the estimated expiry date for nextKey is one keyUpdateInterval
     * more.
     */
    serialNo++;
    currentKey = new BlockKey(serialNo, System.currentTimeMillis() + 2
        * keyUpdateInterval + tokenLifetime, generateSecret());
    serialNo++;
    nextKey = new BlockKey(serialNo, System.currentTimeMillis() + 3
        * keyUpdateInterval + tokenLifetime, generateSecret());
    allKeys.put(currentKey.getKeyId(), currentKey);
    allKeys.put(nextKey.getKeyId(), nextKey);
  }

  /** Export block keys, only to be used in master mode */
  public synchronized ExportedBlockKeys exportKeys() {
    if (!isMaster)
      return null;
    if (LOG.isDebugEnabled())
      LOG.debug("Exporting access keys");
    return new ExportedBlockKeys(true, keyUpdateInterval, tokenLifetime,
        currentKey, allKeys.values().toArray(new BlockKey[0]));
  }

  private synchronized void removeExpiredKeys() {
    long now = System.currentTimeMillis();
    for (Iterator<Map.Entry<Integer, BlockKey>> it = allKeys.entrySet()
        .iterator(); it.hasNext();) {
      Map.Entry<Integer, BlockKey> e = it.next();
      if (e.getValue().getExpiryDate() < now) {
        it.remove();
      }
    }
  }

  /**
   * Set block keys, only to be used in slave mode
   */
  public synchronized void setKeys(ExportedBlockKeys exportedKeys)
      throws IOException {
    if (isMaster || exportedKeys == null)
      return;
    LOG.info("Setting block keys");
    removeExpiredKeys();
    this.currentKey = exportedKeys.getCurrentKey();
    BlockKey[] receivedKeys = exportedKeys.getAllKeys();
    for (int i = 0; i < receivedKeys.length; i++) {
      if (receivedKeys[i] == null)
        continue;
      this.allKeys.put(receivedKeys[i].getKeyId(), receivedKeys[i]);
    }
  }

  /**
   * Update block keys, only to be used in master mode
   */
  public synchronized void updateKeys() throws IOException {
    if (!isMaster)
      return;
    LOG.info("Updating block keys");
    removeExpiredKeys();
    // set final expiry date of retiring currentKey
    allKeys.put(currentKey.getKeyId(), new BlockKey(currentKey.getKeyId(),
        System.currentTimeMillis() + keyUpdateInterval + tokenLifetime,
        currentKey.getKey()));
    // update the estimated expiry date of new currentKey
    currentKey = new BlockKey(nextKey.getKeyId(), System.currentTimeMillis()
        + 2 * keyUpdateInterval + tokenLifetime, nextKey.getKey());
    allKeys.put(currentKey.getKeyId(), currentKey);
    // generate a new nextKey
    serialNo++;
    nextKey = new BlockKey(serialNo, System.currentTimeMillis() + 3
        * keyUpdateInterval + tokenLifetime, generateSecret());
    allKeys.put(nextKey.getKeyId(), nextKey);
  }

  /** Generate an block token for current user */
  public Token<BlockTokenIdentifier> generateToken(Block block,
      EnumSet<AccessMode> modes) throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    String userID = (ugi == null ? null : ugi.getShortUserName());
    return generateToken(userID, block, modes);
  }

  /** Generate a block token for a specified user */
  public Token<BlockTokenIdentifier> generateToken(String userId, Block block,
      EnumSet<AccessMode> modes) throws IOException {
    BlockTokenIdentifier id = new BlockTokenIdentifier(userId, 
        block.getBlockId(), modes);
    return new Token<BlockTokenIdentifier>(id, this);
  }

  /**
   * Check if access should be allowed. userID is not checked if null. This
   * method doesn't check if token password is correct. It should be used only
   * when token password has already been verified (e.g., in the RPC layer).
   */
  public void checkAccess(BlockTokenIdentifier id, String userId, Block block,
      AccessMode mode) throws InvalidToken {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Checking access for user=" + userId + ", block=" + block
          + ", access mode=" + mode + " using " + id.toString());
    }
    if (userId != null && !userId.equals(id.getUserId())) {
      throw new InvalidToken("Block token with " + id.toString()
          + " doesn't belong to user " + userId);
    }
    if (id.getBlockId() != block.getBlockId()) {
      throw new InvalidToken("Block token with " + id.toString()
          + " doesn't apply to block " + block);
    }
    if (isExpired(id.getExpiryDate())) {
      throw new InvalidToken("Block token with " + id.toString()
          + " is expired.");
    }
    if (!id.getAccessModes().contains(mode)) {
      throw new InvalidToken("Block token with " + id.toString()
          + " doesn't have " + mode + " permission");
    }
  }

  /** Check if access should be allowed. userID is not checked if null */
  public void checkAccess(Token<BlockTokenIdentifier> token, String userId,
      Block block, AccessMode mode) throws InvalidToken {
    BlockTokenIdentifier id = new BlockTokenIdentifier();
    try {
      id.readFields(new DataInputStream(new ByteArrayInputStream(token
          .getIdentifier())));
    } catch (IOException e) {
      throw new InvalidToken(
          "Unable to de-serialize block token identifier for user=" + userId
              + ", block=" + block + ", access mode=" + mode);
    }
    checkAccess(id, userId, block, mode);
    if (!Arrays.equals(retrievePassword(id), token.getPassword())) {
      throw new InvalidToken("Block token with " + id.toString()
          + " doesn't have the correct token password");
    }
  }

  private static boolean isExpired(long expiryDate) {
    return System.currentTimeMillis() > expiryDate;
  }

  /**
   * check if a token is expired. for unit test only. return true when token is
   * expired, false otherwise
   */
  static boolean isTokenExpired(Token<BlockTokenIdentifier> token)
      throws IOException {
    ByteArrayInputStream buf = new ByteArrayInputStream(token.getIdentifier());
    DataInputStream in = new DataInputStream(buf);
    long expiryDate = WritableUtils.readVLong(in);
    return isExpired(expiryDate);
  }

  /** set token lifetime. */
  public void setTokenLifetime(long tokenLifetime) {
    this.tokenLifetime = tokenLifetime;
  }

  /**
   * Create an empty block token identifier
   * 
   * @return a newly created empty block token identifier
   */
  @Override
  public BlockTokenIdentifier createIdentifier() {
    return new BlockTokenIdentifier();
  }

  /**
   * Create a new password/secret for the given block token identifier.
   * 
   * @param identifier
   *          the block token identifier
   * @return token password/secret
   */
  @Override
  protected byte[] createPassword(BlockTokenIdentifier identifier) {
    BlockKey key = null;
    synchronized (this) {
      key = currentKey;
    }
    if (key == null)
      throw new IllegalStateException("currentKey hasn't been initialized.");
    identifier.setExpiryDate(System.currentTimeMillis() + tokenLifetime);
    identifier.setKeyId(key.getKeyId());
    if (LOG.isDebugEnabled()) {
      LOG.debug("Generating block token for " + identifier.toString());
    }
    return createPassword(identifier.getBytes(), key.getKey());
  }

  /**
   * Look up the token password/secret for the given block token identifier.
   * 
   * @param identifier
   *          the block token identifier to look up
   * @return token password/secret as byte[]
   * @throws InvalidToken
   */
  @Override
  public byte[] retrievePassword(BlockTokenIdentifier identifier)
      throws InvalidToken {
    if (isExpired(identifier.getExpiryDate())) {
      throw new InvalidToken("Block token with " + identifier.toString()
          + " is expired.");
    }
    BlockKey key = null;
    synchronized (this) {
      key = allKeys.get(identifier.getKeyId());
    }
    if (key == null) {
      throw new InvalidToken("Can't re-compute password for "
          + identifier.toString() + ", since the required block key (keyID="
          + identifier.getKeyId() + ") doesn't exist.");
    }
    return createPassword(identifier.getBytes(), key.getKey());
  }
}
