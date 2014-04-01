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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_KRB_HTTPS_USER_NAME_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_USER_NAME_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_SECONDARY_NAMENODE_KRB_HTTPS_USER_NAME_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_SECONDARY_NAMENODE_USER_NAME_KEY;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Map;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.hadoop.security.SecurityUtil;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;

/**
 * This class is used in Namesystem's jetty to retrieve a file.
 * Typically used by the Secondary NameNode to retrieve image and
 * edit file for periodic checkpointing.
 */
public class GetImageServlet extends HttpServlet {
  private static final long serialVersionUID = -7669068179452648952L;
  private static final Log LOG = LogFactory.getLog(GetImageServlet.class);

  /**
   * A lock object to prevent multiple 2NNs from simultaneously uploading
   * fsimage snapshots.
   */
  private Object fsImageTransferLock = new Object();
  
  @SuppressWarnings("unchecked")
  public void doGet(final HttpServletRequest request,
                    final HttpServletResponse response
                    ) throws ServletException, IOException {
    Map<String,String[]> pmap = request.getParameterMap();
    try {
      ServletContext context = getServletContext();
      final FSImage nnImage = (FSImage)context.getAttribute("name.system.image");
      final TransferFsImage ff = new TransferFsImage(pmap, request, response);
      final Configuration conf = (Configuration)getServletContext().getAttribute(JspHelper.CURRENT_CONF);
      if(UserGroupInformation.isSecurityEnabled() && 
          !isValidRequestor(request.getRemoteUser(), conf)) {
        response.sendError(HttpServletResponse.SC_FORBIDDEN, 
            "Only Namenode and Secondary Namenode may access this servlet");
        LOG.warn("Received non-NN/SNN request for image or edits from " 
            + request.getRemoteHost());
        return;
      }
      
      UserGroupInformation.getCurrentUser().doAs(new PrivilegedExceptionAction<Void>() {

        @Override
        public Void run() throws Exception {
          if (ff.getImage()) {
            response.setHeader(TransferFsImage.CONTENT_LENGTH,
              String.valueOf(nnImage.getFsImageName().length()));
            // send fsImage
            TransferFsImage.getFileServer(response.getOutputStream(),
                                          nnImage.getFsImageName()); 
          } else if (ff.getEdit()) {
            response.setHeader(TransferFsImage.CONTENT_LENGTH,
              String.valueOf(nnImage.getFsEditName().length()));
            // send edits
            TransferFsImage.getFileServer(response.getOutputStream(),
                                          nnImage.getFsEditName());
          } else if (ff.putImage()) {
            synchronized (fsImageTransferLock) {
              final MD5Hash expectedChecksum = ff.getNewChecksum();
              // issue a HTTP get request to download the new fsimage 
              nnImage.validateCheckpointUpload(ff.getToken());
              reloginIfNecessary().doAs(new PrivilegedExceptionAction<Void>() {
                @Override
                public Void run() throws Exception {
                  MD5Hash actualChecksum = TransferFsImage.getFileClient(ff.getInfoServer(),
                      "getimage=1", nnImage.getFsImageNameCheckpoint(), true);
                  LOG.info("Downloaded new fsimage with checksum: " + actualChecksum);
                  if (!actualChecksum.equals(expectedChecksum)) {
                    throw new IOException("Actual checksum of transferred fsimage: "
                        + actualChecksum + " does not match expected checksum: "
                        + expectedChecksum);
                  }
                  return null;
                }
              });
              nnImage.checkpointUploadDone();
            }
          }
          return null;
        }

        // We may have lost our ticket since the last time we tried to open
        // an http connection, so log in just in case.
        private UserGroupInformation reloginIfNecessary() throws IOException {
          // This method is only called on the NN, therefore it is safe to
          // use these key values.
          return UserGroupInformation
          .loginUserFromKeytabAndReturnUGI(
                  SecurityUtil.getServerPrincipal(conf
                      .get(DFS_NAMENODE_KRB_HTTPS_USER_NAME_KEY), NameNode
                      .getAddress(conf).getHostName()),
              conf.get(DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY));
        }
      });

    } catch (Exception ie) {
      String errMsg = "GetImage failed. " + StringUtils.stringifyException(ie);
      response.sendError(HttpServletResponse.SC_GONE, errMsg);
      throw new IOException(errMsg);
    } finally {
      response.getOutputStream().close();
    }
  }
  
  private boolean isValidRequestor(String remoteUser, Configuration conf)
      throws IOException {
    if(remoteUser == null) { // This really shouldn't happen...
      LOG.warn("Received null remoteUser while authorizing access to getImage servlet");
      return false;
    }
    
    String[] validRequestors = {
        SecurityUtil.getServerPrincipal(conf
            .get(DFS_NAMENODE_KRB_HTTPS_USER_NAME_KEY), NameNode.getAddress(
            conf).getHostName()),
        SecurityUtil.getServerPrincipal(conf.get(DFS_NAMENODE_USER_NAME_KEY),
            NameNode.getAddress(conf).getHostName()),
        SecurityUtil.getServerPrincipal(conf
            .get(DFS_SECONDARY_NAMENODE_KRB_HTTPS_USER_NAME_KEY),
            SecondaryNameNode.getHttpAddress(conf).getHostName()),
        SecurityUtil.getServerPrincipal(conf
            .get(DFS_SECONDARY_NAMENODE_USER_NAME_KEY), SecondaryNameNode
            .getHttpAddress(conf).getHostName()) };
    
    for(String v : validRequestors) {
      if(v != null && v.equals(remoteUser)) {
        if(LOG.isDebugEnabled()) LOG.debug("isValidRequestor is allowing: " + remoteUser);
        return true;
      }
    }
    if(LOG.isDebugEnabled()) LOG.debug("isValidRequestor is rejecting: " + remoteUser);
    return false;
  }
}
