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
import java.net.URISyntaxException;
import java.net.URL;
import java.security.PrivilegedExceptionAction;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ServletUtil;

/** Redirect queries about the hosted filesystem to an appropriate datanode.
 * @see org.apache.hadoop.hdfs.HftpFileSystem
 */
public class FileDataServlet extends DfsServlet {

  /** Create a redirection URL */
  protected URL createRedirectURL(String path, String encodedPath, HdfsFileStatus status, 
      UserGroupInformation ugi, ClientProtocol nnproxy, HttpServletRequest request, String dt)
      throws IOException {
    String scheme = request.getScheme();
    final DatanodeID host = pickSrcDatanode(path, status, nnproxy);
    final String hostname;
    if (host instanceof DatanodeInfo) {
      hostname = ((DatanodeInfo)host).getHostName();
    } else {
      hostname = host.getHost();
    }
    
    String dtParam="";
    if (dt != null) {
      dtParam = JspHelper.getDelegationTokenUrlParam(dt);
    }
    final int port = "https".equals(scheme)
        ? (Integer)getServletContext().getAttribute("datanode.https.port")
        : host.getInfoPort();
    return new URL(scheme, hostname, port,
        "/streamFile" + encodedPath + '?' +
        "ugi=" + ServletUtil.encodeQueryValue(ugi.getShortUserName()) + dtParam);
  }

  private static JspHelper jspHelper = null;

  /** Select a datanode to service this request.
   * Currently, this looks at no more than the first five blocks of a file,
   * selecting a datanode randomly from the most represented.
   */
  private static DatanodeID pickSrcDatanode(String parent, HdfsFileStatus i,
      ClientProtocol nnproxy) throws IOException {
    // a race condition can happen by initializing a static member this way.
    // A proper fix should make JspHelper a singleton. Since it doesn't affect 
    // correctness, we leave it as is for now.
    if (jspHelper == null)
      jspHelper = new JspHelper();
    final LocatedBlocks blks = nnproxy.getBlockLocations(
        i.getFullPath(new Path(parent)).toUri().getPath(), 0, 1);
    if (i.getLen() == 0 || blks.getLocatedBlocks().size() <= 0) {
      // pick a random datanode
      return jspHelper.randomNode();
    }
    return jspHelper.bestNode(blks.get(0));
  }

  /**
   * Service a GET request as described below.
   * Request:
   * {@code
   * GET http://<nn>:<port>/data[/<path>] HTTP/1.1
   * }
   */
  public void doGet(final HttpServletRequest request,
                    final HttpServletResponse response)
    throws IOException {
    Configuration conf =
	(Configuration) getServletContext().getAttribute(JspHelper.CURRENT_CONF);
    final UserGroupInformation ugi = getUGI(request, conf);

    try {
      ugi.doAs(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws IOException {
              ClientProtocol nn = createNameNodeProxy();
              final String path = ServletUtil.getDecodedPath(request, "/data");
              final String encodedPath = ServletUtil.getRawPath(request, "/data");
              String delegationToken =
                request.getParameter(JspHelper.DELEGATION_PARAMETER_NAME);
              
              HdfsFileStatus info = nn.getFileInfo(path);
              if (info != null && !info.isDir()) {
                response.sendRedirect(createRedirectURL(path, encodedPath, 
                    info, ugi, nn, request, delegationToken).toString());
              } else if (info == null){
                response.sendError(400, "File not found " + path);
              } else {
                response.sendError(400, path + " is a directory");
              }
              return null;
            }
          });
    } catch (IOException e) {
      response.sendError(400, e.getMessage());
    } catch (InterruptedException e) {
      response.sendError(400, e.getMessage());
    }
  }
}

