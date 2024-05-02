/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.service.cli.session;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.service.auth.HiveAuthFactory;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.rpc.thrift.TProtocolVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * HiveSessionImplwithUGI.
 * HiveSession with connecting user's UGI and delegation token if required
 */
public class HiveSessionImplwithUGI extends HiveSessionImpl {
  public static final String HS2TOKEN = "HiveServer2ImpersonationToken";

  private UserGroupInformation sessionUgi = null;
  private String delegationTokenStr = null;
  private Hive sessionHive = null;
  private HiveSession proxySession = null;
  static final Logger LOG = LoggerFactory.getLogger(HiveSessionImplwithUGI.class);

  public HiveSessionImplwithUGI(TProtocolVersion protocol, String username, String password,
      HiveConf hiveConf, String ipAddress, String delegationToken) throws HiveSQLException {
    super(protocol, username, password, hiveConf, ipAddress);
    setSessionUGI(username);
    setDelegationToken(delegationToken);

    // create a new metastore connection for this particular user session
    Hive.set(null);
    try {
      sessionHive = Hive.getWithoutRegisterFns(getHiveConf());
    } catch (HiveException e) {
      throw new HiveSQLException("Failed to setup metastore connection", e);
    }
  }

  // setup appropriate UGI for the session
  public void setSessionUGI(String owner) throws HiveSQLException {
    if (owner == null) {
      throw new HiveSQLException("No username provided for impersonation");
    }
    if (UserGroupInformation.isSecurityEnabled()) {
      try {
        sessionUgi = UserGroupInformation.createProxyUser(
            owner, UserGroupInformation.getLoginUser());
      } catch (IOException e) {
        throw new HiveSQLException("Couldn't setup proxy user", e);
      }
    } else {
      sessionUgi = UserGroupInformation.createRemoteUser(owner);
    }
  }

  public UserGroupInformation getSessionUgi() {
    return this.sessionUgi;
  }

  public String getDelegationToken() {
    return this.delegationTokenStr;
  }

  @Override
  protected synchronized void acquire(boolean userAccess) {
    super.acquire(userAccess);
    // if we have a metastore connection with impersonation, then set it first
    if (sessionHive != null) {
      Hive.set(sessionHive);
    }
  }

  /**
   * Close the file systems for the session and remove it from the FileSystem cache.
   * Cancel the session's delegation token and close the metastore connection
   */
  @Override
  public void close() throws HiveSQLException {
    try {
      acquire(true);
      cancelDelegationToken();
    } finally {
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
  private void setDelegationToken(String delegationTokenStr) throws HiveSQLException {
    this.delegationTokenStr = delegationTokenStr;
    if (delegationTokenStr != null) {
      getHiveConf().set("hive.metastore.token.signature", HS2TOKEN);
      try {
        Utils.setTokenStr(sessionUgi, delegationTokenStr, HS2TOKEN);
      } catch (IOException e) {
        throw new HiveSQLException("Couldn't setup delegation token in the ugi", e);
      }
    }
  }

  // If the session has a delegation token obtained from the metastore, then cancel it
  private void cancelDelegationToken() throws HiveSQLException {
    if (delegationTokenStr != null) {
      try {
        Hive.getWithoutRegisterFns(getHiveConf()).cancelDelegationToken(delegationTokenStr);
      } catch (HiveException e) {
        throw new HiveSQLException("Couldn't cancel delegation token", e);
      }
      // close the metastore connection created with this delegation token
      Hive.closeCurrent();
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
