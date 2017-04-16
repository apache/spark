/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.io.PrintStream;
import java.security.PrivilegedExceptionAction;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;

/**
 * Cancel delegation tokens over http for use in hftp.
 */
@SuppressWarnings("serial")
public class CancelDelegationTokenServlet extends DfsServlet {
  private static final Log LOG = LogFactory.getLog(CancelDelegationTokenServlet.class);
  public static final String PATH_SPEC = "/cancelDelegationToken";
  public static final String TOKEN = "token";
  
  @Override
  protected void doGet(final HttpServletRequest req, final HttpServletResponse resp)
      throws ServletException, IOException {
    final UserGroupInformation ugi;
    final ServletContext context = getServletContext();
    final Configuration conf = 
      (Configuration) context.getAttribute(JspHelper.CURRENT_CONF);
    try {
      ugi = getUGI(req, conf);
    } catch(IOException ioe) {
      LOG.info("Request for token received with no authentication from "
          + req.getRemoteAddr(), ioe);
      resp.sendError(HttpServletResponse.SC_FORBIDDEN, 
          "Unable to identify or authenticate user");
      return;
    }
    final NameNode nn = (NameNode) context.getAttribute("name.node");
    String tokenString = req.getParameter(TOKEN);
    if (tokenString == null) {
      resp.sendError(HttpServletResponse.SC_MULTIPLE_CHOICES,
                     "Token to renew not specified");
    }
    final Token<DelegationTokenIdentifier> token = 
      new Token<DelegationTokenIdentifier>();
    token.decodeFromUrlString(tokenString);
    
    try {
      ugi.doAs(new PrivilegedExceptionAction<Void>() {
        public Void run() throws Exception {
          nn.cancelDelegationToken(token);
          return null;
        }
      });
    } catch(Exception e) {
      LOG.info("Exception while cancelling token. Re-throwing. ", e);
      resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                     e.getMessage());
    }
  }
}
