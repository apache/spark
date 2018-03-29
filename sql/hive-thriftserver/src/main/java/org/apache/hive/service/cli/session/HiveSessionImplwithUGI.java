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

package org.apache.hive.service.cli.session;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.service.auth.HiveAuthFactory;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.rpc.thrift.TProtocolVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 *
 * HiveSessionImplwithUGI.
 * HiveSession with connecting user's UGI and delegation token if required.
 * Note: this object may be shared between threads in HS2.
 */
public class HiveSessionImplwithUGI extends HiveSessionImpl {
  public static final String HS2TOKEN = "HiveServer2ImpersonationToken";
  static final Logger LOG = LoggerFactory.getLogger(HiveSessionImplwithUGI.class);

  private UserGroupInformation sessionUgi = null;
  private String hmsDelegationTokenStr = null;
  private HiveSession proxySession = null;

  public HiveSessionImplwithUGI(SessionHandle sessionHandle, TProtocolVersion protocol, String username,
                                String password, HiveConf hiveConf, String ipAddress, String delegationToken) throws HiveSQLException {
    super(sessionHandle, protocol, username, password, hiveConf, ipAddress);
    setSessionUGI(username);
    setDelegationToken(delegationToken);
  }

  // setup appropriate UGI for the session
  public void setSessionUGI(String owner) throws HiveSQLException {
    if (owner == null) {
      throw new HiveSQLException("No username provided for impersonation");
    }
    try {
      sessionUgi = UserGroupInformation.createProxyUser(
          owner, UserGroupInformation.getLoginUser());
    } catch (IOException e) {
      throw new HiveSQLException("Couldn't setup proxy user", e);
    }
  }

  public UserGroupInformation getSessionUgi() {
    return this.sessionUgi;
  }

  public String getDelegationToken () {
    return this.hmsDelegationTokenStr;
  }

  /**
   * Close the file systems for the session and remove it from the FileSystem cache.
   * Cancel the session's delegation token and close the metastore connection
   */
  @Override
  public void close() throws HiveSQLException {
    try {
      acquire(true, false);
      cancelDelegationToken();
    } finally {
      release(true, false);
      try {
        super.close();
      } finally {
        try {
          FileSystem.closeAllForUGI(sessionUgi);
        } catch (IOException ioe) {
          throw new HiveSQLException("Could not clean up file-system handles for UGI: "
              + sessionUgi, ioe);
        }
      }
    }
  }

  /**
   * Enable delegation token for the session
   * save the token string and set the token.signature in hive conf. The metastore client uses
   * this token.signature to determine where to use kerberos or delegation token
   * @throws HiveException
   * @throws IOException
   */
  private void setDelegationToken(String hmsDelegationTokenStr) throws HiveSQLException {
    this.hmsDelegationTokenStr = hmsDelegationTokenStr;
    if (hmsDelegationTokenStr != null) {
      getHiveConf().setVar(HiveConf.ConfVars.METASTORE_TOKEN_SIGNATURE, HS2TOKEN);
      try {
        Utils.setTokenStr(sessionUgi, hmsDelegationTokenStr, HS2TOKEN);
      } catch (IOException e) {
        throw new HiveSQLException("Couldn't setup delegation token in the ugi: " + e, e);
      }
    }
  }

  // If the session has a delegation token obtained from the metastore, then cancel it
  private void cancelDelegationToken() throws HiveSQLException {
    if (hmsDelegationTokenStr != null) {
      try {
        Hive.get(getHiveConf()).cancelDelegationToken(hmsDelegationTokenStr);
      } catch (HiveException e) {
        throw new HiveSQLException("Couldn't cancel delegation token", e);
      }
    }
  }

  @Override
  public IMetaStoreClient getMetaStoreClient() throws HiveSQLException {
    return getMetaStoreClient(true);
  }

  private IMetaStoreClient getMetaStoreClient(boolean retryInCaseOfTokenExpiration) throws HiveSQLException {
    try {
      return Hive.get(getHiveConf()).getMSC();
    } catch (HiveException e) {
      throw new HiveSQLException("Failed to get metastore connection: " + e, e);
    } catch(MetaException e1) {
      if (hmsDelegationTokenStr != null && retryInCaseOfTokenExpiration) {
        LOG.info("Retrying failed metastore connection: " + e1, e1);
        Hive.closeCurrent();
        try {
          setDelegationToken(Hive.get(getHiveConf()).getDelegationToken(sessionUgi.getUserName(), getUserName()));
        } catch (HiveException e2) {
          throw new HiveSQLException("Error connect metastore to setup impersonation: " + e2, e2);
        }
        return getMetaStoreClient(false);
      } else {
        throw new HiveSQLException("Failed to get metastore connection: " + e1, e1);
      }
    }
  }

  @Override
  protected HiveSession getSession() {
    assert proxySession != null;

    return proxySession;
  }

  public void setProxySession(HiveSession proxySession) {
    this.proxySession = proxySession;
  }

  @Override
  public String getDelegationToken(HiveAuthFactory authFactory, String owner,
                                   String renewer) throws HiveSQLException {
    return authFactory.getDelegationToken(owner, renewer, getIpAddress());
  }

  @Override
  public void cancelDelegationToken(HiveAuthFactory authFactory, String tokenStr)
      throws HiveSQLException {
    authFactory.cancelDelegationToken(tokenStr);
  }

  @Override
  public void renewDelegationToken(HiveAuthFactory authFactory, String tokenStr)
      throws HiveSQLException {
    authFactory.renewDelegationToken(tokenStr);
  }

}
