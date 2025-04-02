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
package org.apache.hive.service.auth;

import javax.security.sasl.AuthenticationException;

import net.sf.jpam.Pam;
import org.apache.hadoop.hive.conf.HiveConf;

public class PamAuthenticationProviderImpl implements PasswdAuthenticationProvider {

  private final String pamServiceNames;

  PamAuthenticationProviderImpl() {
    HiveConf conf = new HiveConf();
    pamServiceNames = conf.getVar(HiveConf.ConfVars.HIVE_SERVER2_PAM_SERVICES);
  }

  @Override
  public void Authenticate(String user, String password) throws AuthenticationException {

    if (pamServiceNames == null || pamServiceNames.trim().isEmpty()) {
      throw new AuthenticationException("No PAM services are set.");
    }

    String[] pamServices = pamServiceNames.split(",");
    for (String pamService : pamServices) {
      Pam pam = new Pam(pamService);
      boolean isAuthenticated = pam.authenticateSuccessful(user, password);
      if (!isAuthenticated) {
        throw new AuthenticationException(
          "Error authenticating with the PAM service: " + pamService);
      }
    }
  }
}
