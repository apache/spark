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

package org.apache.hive.service.cli;

import java.util.Collections;

import org.apache.hive.service.auth.HiveAuthFactory;


/**
 * CLIServiceClient.
 *
 */
public abstract class CLIServiceClient implements ICLIService {
  private static final long DEFAULT_MAX_ROWS = 1000;

  public SessionHandle openSession(String username, String password)
      throws HiveSQLException {
    return openSession(username, password, Collections.<String, String>emptyMap());
  }

  @Override
  public RowSet fetchResults(OperationHandle opHandle) throws HiveSQLException {
    // TODO: provide STATIC default value
    return fetchResults(opHandle, FetchOrientation.FETCH_NEXT, DEFAULT_MAX_ROWS, FetchType.QUERY_OUTPUT);
  }

  @Override
  public abstract String getDelegationToken(SessionHandle sessionHandle, HiveAuthFactory authFactory,
      String owner, String renewer) throws HiveSQLException;

  @Override
  public abstract void cancelDelegationToken(SessionHandle sessionHandle, HiveAuthFactory authFactory,
      String tokenStr) throws HiveSQLException;

  @Override
  public abstract void renewDelegationToken(SessionHandle sessionHandle, HiveAuthFactory authFactory,
      String tokenStr) throws HiveSQLException;

}
