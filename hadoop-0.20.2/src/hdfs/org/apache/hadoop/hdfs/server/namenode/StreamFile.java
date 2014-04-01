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
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.util.Enumeration;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSClient.DFSInputStream;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ServletUtil;

import org.mortbay.jetty.InclusiveByteRange;

public class StreamFile extends DfsServlet {
  /** for java.io.Serializable */
  private static final long serialVersionUID = 1L;

  public static final String CONTENT_LENGTH = "Content-Length";

  static InetSocketAddress nameNodeAddr;
  static DataNode datanode = null;
  static {
    if ((datanode = DataNode.getDataNode()) != null) {
      nameNodeAddr = datanode.getNameNodeAddr();
    }
  }

  /* Return a DFS client to use to make the given HTTP request */
  protected DFSClient getDFSClient(HttpServletRequest request)
      throws IOException, InterruptedException {

    Configuration conf =
      (Configuration) getServletContext().getAttribute(JspHelper.CURRENT_CONF);
    UserGroupInformation ugi = getUGI(request, conf);

    return JspHelper.getDFSClient(ugi, nameNodeAddr, conf);
  }
  
  @SuppressWarnings("unchecked")
  public void doGet(HttpServletRequest request, HttpServletResponse response)
    throws ServletException, IOException {
    final String filename = ServletUtil.getDecodedPath(request, "/streamFile");
    final String rawFilename = ServletUtil.getRawPath(request, "/streamFile");

    if (filename == null || filename.length() == 0) {
      response.setContentType("text/plain");
      PrintWriter out = response.getWriter();
      out.print("Invalid input");
      return;
    }
    
    DFSClient dfs;
    try {
      dfs = getDFSClient(request);
    } catch (InterruptedException e) {
      response.sendError(400, e.getMessage());
      return;
    }

    Enumeration<String> reqRanges = request.getHeaders("Range");
    if (reqRanges != null && !reqRanges.hasMoreElements()) {
      reqRanges = null;
    }

    DFSInputStream in = null;
    OutputStream out = null;

    try {
      in = dfs.open(filename);
      out = response.getOutputStream();
      final long fileLen = in.getFileLength();

      if (reqRanges != null) {
        List<InclusiveByteRange> ranges =
          InclusiveByteRange.satisfiableRanges(reqRanges, fileLen);
        StreamFile.sendPartialData(in, out, response, fileLen, ranges);
      } else {
        // No ranges, so send entire file
        response.setHeader("Content-Disposition", "attachment; filename=\"" + 
                           rawFilename + "\"");
        response.setContentType("application/octet-stream");
        response.setHeader(CONTENT_LENGTH, "" + in.getFileLength());
        StreamFile.copyFromOffset(in, out, 0L, fileLen);
      }
      in.close();
      in = null;
      out.close();
      out = null;
      dfs.close();
      dfs = null;
    } catch (IOException ioe) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("response.isCommitted()=" + response.isCommitted(), ioe);
      }
      throw ioe;
    } finally {
      IOUtils.cleanup(LOG, in);
      IOUtils.cleanup(LOG, out);
      IOUtils.cleanup(LOG, dfs);
    }
  }

  /**
   * Send a partial content response with the given range. If there are
   * no satisfiable ranges, or if multiple ranges are requested, which
   * is unsupported, respond with range not satisfiable.
   *
   * @param in stream to read from
   * @param out stream to write to
   * @param response http response to use
   * @param contentLength for the response header
   * @param ranges to write to respond with
   * @throws IOException on error sending the response
   */
  static void sendPartialData(FSInputStream in,
                              OutputStream out,
                              HttpServletResponse response,
                              long contentLength,
                              List<InclusiveByteRange> ranges)
      throws IOException {
    if (ranges == null || ranges.size() != 1) {
      response.setContentLength(0);
      response.setStatus(HttpServletResponse.SC_REQUESTED_RANGE_NOT_SATISFIABLE);
      response.setHeader("Content-Range",
                InclusiveByteRange.to416HeaderRangeString(contentLength));
    } else {
      InclusiveByteRange singleSatisfiableRange = ranges.get(0);
      long singleLength = singleSatisfiableRange.getSize(contentLength);
      response.setStatus(HttpServletResponse.SC_PARTIAL_CONTENT);
      response.setHeader("Content-Range", 
        singleSatisfiableRange.toHeaderRangeString(contentLength));
      copyFromOffset(in, out,
                     singleSatisfiableRange.getFirst(contentLength),
                     singleLength);
    }
  }

  /* Copy count bytes at the given offset from one stream to another */
  static void copyFromOffset(FSInputStream in, OutputStream out, long offset,
      long count) throws IOException {
    in.seek(offset);
    IOUtils.copyBytes(in, out, count, false);
  }
}
