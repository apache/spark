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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.net.InetSocketAddress;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.UserGroupInformation;
import org.znerd.xmlenc.XMLOutputter;

/**
 * A base class for the servlets in DFS.
 */
abstract class DfsServlet extends HttpServlet {
  /** For java.io.Serializable */
  private static final long serialVersionUID = 1L;

  static final Log LOG = LogFactory.getLog(DfsServlet.class.getCanonicalName());

  /** Write the object to XML format */
  protected void writeXml(Exception except,
                          String path, XMLOutputter doc) throws IOException {
    doc.startTag(RemoteException.class.getSimpleName());
    doc.attribute("path", path);
    if (except instanceof RemoteException) {
      doc.attribute("class", ((RemoteException) except).getClassName());
    } else {
      doc.attribute("class", except.getClass().getName());      
    }
    String msg = except.getLocalizedMessage();
    int i = msg.indexOf("\n");
    if (i >= 0) {
      msg = msg.substring(0, i);
    }
    doc.attribute("message", msg.substring(msg.indexOf(":") + 1).trim());
    doc.endTag();
  }

  /** Get {@link UserGroupInformation} from request 
   *    * @throws IOException */
  protected UserGroupInformation getUGI(HttpServletRequest request,
                                        Configuration conf
					) throws IOException {
    return JspHelper.getUGI(request, conf);
  }

  /**
   * Create a {@link NameNode} proxy from the current {@link ServletContext}. 
   */
  protected ClientProtocol createNameNodeProxy() throws IOException {
    ServletContext context = getServletContext();
    // if we are running in the Name Node, use it directly rather than via 
    // rpc
    NameNode nn = (NameNode) context.getAttribute("name.node");
    if (nn != null) {
      return nn;
    }
    InetSocketAddress nnAddr = (InetSocketAddress)context.getAttribute("name.node.address");
    Configuration conf = new Configuration(
        (Configuration)context.getAttribute(JspHelper.CURRENT_CONF));
    return DFSClient.createNamenode(nnAddr, conf);
  }
}
