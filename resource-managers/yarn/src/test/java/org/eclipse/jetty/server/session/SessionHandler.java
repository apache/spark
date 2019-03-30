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

package org.eclipse.jetty.server.session;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.SessionManager;
import org.eclipse.jetty.server.handler.ScopedHandler;

/**
 * Adapted from https://github.com/eclipse/jetty.project/blob/jetty-9.3.25.v20180904/
 *   jetty-server/src/main/java/org/eclipse/jetty/server/session/SessionHandler.java
 */
public class SessionHandler extends ScopedHandler {
  private SessionManager _sessionManager;

  public SessionHandler() {
  }

  /**
   * @param manager
   *            The session manager
   */
  public SessionHandler(SessionManager manager) {
    setSessionManager(manager);
  }

  /**
   * @return Returns the sessionManager.
   */
  public SessionManager getSessionManager() {
    return _sessionManager;
  }

  /**
   * @param sessionManager
   *            The sessionManager to set.
   */
  public void setSessionManager(SessionManager sessionManager) {
    if (isStarted()) {
      throw new IllegalStateException();
    }
    if (sessionManager != null) {
      updateBean(_sessionManager,sessionManager);
      _sessionManager=sessionManager;
    }
  }

  /*
   * @see org.eclipse.jetty.server.Handler#handle(javax.servlet.http.HttpServletRequest,
   * javax.servlet.http.HttpServletResponse, int)
   */
  @Override
  public void doHandle(String target, Request baseRequest, HttpServletRequest request,
      HttpServletResponse response) throws IOException, ServletException {
    // start manual inline of nextHandle(target,baseRequest,request,response);
    if (_nextScope != null && _nextScope == _handler) {
      _nextScope.doHandle(target,baseRequest,request,response);
    } else if (_handler != null) {
      _handler.handle(target,baseRequest,request,response);
      // end manual inline
    }
  }

  public void clearEventListeners() {
    if (_sessionManager != null) {
      _sessionManager.clearEventListeners();
    }
  }
}
