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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * This authentication provider implements the {@code CUSTOM} authentication. It allows a {@link
 * PasswdAuthenticationProvider} to be specified at configuration time which may additionally
 * implement {@link org.apache.hadoop.conf.Configurable Configurable} to grab Hive's {@link
 * org.apache.hadoop.conf.Configuration Configuration}.
 */
public class CustomAuthenticationProviderImpl implements PasswdAuthenticationProvider {

  private final PasswdAuthenticationProvider customProvider;

  @SuppressWarnings("unchecked")
  CustomAuthenticationProviderImpl() {
    HiveConf conf = new HiveConf();
    Class<? extends PasswdAuthenticationProvider> customHandlerClass =
      (Class<? extends PasswdAuthenticationProvider>) conf.getClass(
        HiveConf.ConfVars.HIVE_SERVER2_CUSTOM_AUTHENTICATION_CLASS.varname,
        PasswdAuthenticationProvider.class);
    customProvider = ReflectionUtils.newInstance(customHandlerClass, conf);
  }

  @Override
  public void Authenticate(String user, String password) throws AuthenticationException {
    customProvider.Authenticate(user, password);
  }

}
