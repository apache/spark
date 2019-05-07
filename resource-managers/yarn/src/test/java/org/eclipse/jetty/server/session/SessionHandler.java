//
//  ========================================================================
//  Copyright (c) 1995-2018 Mort Bay Consulting Pty. Ltd.
//  ------------------------------------------------------------------------
//  All rights reserved. This program and the accompanying materials
//  are made available under the terms of the Eclipse Public License v1.0
//  and Apache License v2.0 which accompanies this distribution.
//
//      The Eclipse Public License is available at
//      http://www.eclipse.org/legal/epl-v10.html
//
//      The Apache License v2.0 is available at
//      http://www.opensource.org/licenses/apache2.0.php
//
//  You may elect to redistribute this code under either of these licenses.
//  ========================================================================
//

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
