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

import java.io.DataOutputStream;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;

/**
 * Serve delegation tokens over http for use in hftp.
 */
@SuppressWarnings("serial")
public class GetDelegationTokenServlet extends DfsServlet {
  private static final Log LOG = LogFactory.getLog(GetDelegationTokenServlet.class);
  public static final String PATH_SPEC = "/getDelegationToken";
  public static final String RENEWER = "renewer";
  
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
    LOG.info("Sending token: {" + ugi.getUserName() + "," + req.getRemoteAddr() +"}");
    final NameNode nn = (NameNode) context.getAttribute("name.node");
    String renewer = req.getParameter(RENEWER);
    final String renewerFinal = (renewer == null) ? 
        req.getUserPrincipal().getName() : renewer;
    
    DataOutputStream dos = null;
    try {
      dos = new DataOutputStream(resp.getOutputStream());
      final DataOutputStream dosFinal = dos; // for doAs block
      ugi.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          
          Token<DelegationTokenIdentifier> token = 
            nn.getDelegationToken(new Text(renewerFinal));
          String s = NameNode.getAddress(conf).getAddress().getHostAddress()
                     + ":" + NameNode.getAddress(conf).getPort();
          token.setService(new Text(s));
          Credentials ts = new Credentials();
          ts.addToken(new Text(ugi.getShortUserName()), token);
          ts.write(dosFinal);
          dosFinal.close();
          return null;
        }
      });

    } catch(Exception e) {
      LOG.info("Exception while sending token. Re-throwing. ", e);
      resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    } finally {
      if(dos != null) dos.close();
    }
  }
}
