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
package org.apache.hadoop.security;

import java.io.IOException;
import java.security.Principal;

import javax.security.auth.login.LoginContext;

import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;

/**
 * Save the full and short name of the user as a principal. This allows us to
 * have a single type that we always look for when picking up user names.
 */
class User implements Principal {
  private final String fullName;
  private final String shortName;
  private AuthenticationMethod authMethod = null;
  private LoginContext login = null;
  private long lastLogin = 0;

  public User(String name) {
    this(name, null, null);
  }
  
  public User(String name, AuthenticationMethod authMethod, LoginContext login) {
    try {
      shortName = new KerberosName(name).getShortName();
    } catch (IOException ioe) {
      throw new IllegalArgumentException("Illegal principal name " + name, ioe);
    }
    fullName = name;
    this.authMethod = authMethod;
    this.login = login;
  }

  /**
   * Get the full name of the user.
   */
  @Override
  public String getName() {
    return fullName;
  }
  
  /**
   * Get the user name up to the first '/' or '@'
   * @return the leading part of the user name
   */
  public String getShortName() {
    return shortName;
  }
  
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (o == null || getClass() != o.getClass()) {
      return false;
    } else {
      return ((fullName.equals(((User) o).fullName)) && (authMethod == ((User) o).authMethod));
    }
  }
  
  @Override
  public int hashCode() {
    return fullName.hashCode();
  }
  
  @Override
  public String toString() {
    return fullName;
  }

  public void setAuthenticationMethod(AuthenticationMethod authMethod) {
    this.authMethod = authMethod;
  }

  public AuthenticationMethod getAuthenticationMethod() {
    return authMethod;
  }
  
  /**
   * Returns login object
   * @return login
   */
  public LoginContext getLogin() {
    return login;
  }
  
  /**
   * Set the login object
   * @param login
   */
  public void setLogin(LoginContext login) {
    this.login = login;
  }
  
  /**
   * Set the last login time.
   * @param time the number of milliseconds since the beginning of time
   */
  public void setLastLogin(long time) {
    lastLogin = time;
  }
  
  /**
   * Get the time of the last login.
   * @return the number of milliseconds since the beginning of time.
   */
  public long getLastLogin() {
    return lastLogin;
  }
}
