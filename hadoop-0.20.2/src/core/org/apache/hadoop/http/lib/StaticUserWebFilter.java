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
package org.apache.hadoop.http.lib;

import java.io.IOException;
import java.security.Principal;
import java.util.HashMap;

import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.FilterContainer;
import org.apache.hadoop.http.FilterInitializer;

import javax.servlet.Filter;

/**
 * Provides a servlet filter that pretends to authenticate a fake user (Dr.Who)
 * so that the web UI is usable for a secure cluster without authentication.
 */
public class StaticUserWebFilter extends FilterInitializer {
  private static final String WEB_USERNAME = "Dr.Who";
  private static final Principal WEB_USER = new User(WEB_USERNAME);

  static class User implements Principal {
    private final String name;
    public User(String name) {
      this.name = name;
    }
    @Override
    public String getName() {
      return name;
    }
    @Override
    public int hashCode() {
      return name.hashCode();
    }
    @Override
    public boolean equals(Object other) {
      if (other == this) {
        return true;
      } else if (other == null || other.getClass() != getClass()) {
        return false;
      }
      return ((User) other).name.equals(name);
    }
    @Override
    public String toString() {
      return name;
    }    
  }

  public static class StaticUserFilter implements Filter {

    @Override
    public void destroy() {
      // NOTHING
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response,
                         FilterChain chain
                         ) throws IOException, ServletException {
      HttpServletRequest httpRequest = (HttpServletRequest) request;
      // if the user is already authenticated, don't override it
      if (httpRequest.getRemoteUser() != null) {
        chain.doFilter(request, response);
      } else {
        HttpServletRequestWrapper wrapper = 
            new HttpServletRequestWrapper(httpRequest) {
          @Override
          public Principal getUserPrincipal() {
            return WEB_USER;
          }
          @Override
          public String getRemoteUser() {
            return WEB_USERNAME;
          }
        };
        chain.doFilter(wrapper, response);
      }
    }

    @Override
    public void init(FilterConfig conf) throws ServletException {
      // NOTHING
    }
    
  }

  @Override
  public void initFilter(FilterContainer container, Configuration conf) {
    container.addFilter("static_user_filter", StaticUserFilter.class.getName(), 
                        new HashMap<String,String>());
  }
}
