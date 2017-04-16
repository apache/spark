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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HftpFileSystem;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.util.ServletUtil;
import org.apache.hadoop.util.VersionInfo;

import org.znerd.xmlenc.*;

import java.io.IOException;
import java.io.PrintWriter;
import java.security.PrivilegedExceptionAction;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;
import java.util.regex.Pattern;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Obtain meta-information about a filesystem.
 * @see org.apache.hadoop.hdfs.HftpFileSystem
 */
public class ListPathsServlet extends DfsServlet {
  /** For java.io.Serializable */
  private static final long serialVersionUID = 1L;

  public static final ThreadLocal<SimpleDateFormat> df =
    new ThreadLocal<SimpleDateFormat>() {
      protected SimpleDateFormat initialValue() {
        return HftpFileSystem.getDateFormat();
      }
    };

  /**
   * Write a node to output.
   * Node information includes path, modification, permission, owner and group.
   * For files, it also includes size, replication and block-size. 
   */
  static void writeInfo(String parent, HdfsFileStatus i, XMLOutputter doc) throws IOException {
    final SimpleDateFormat ldf = df.get();
    doc.startTag(i.isDir() ? "directory" : "file");
    doc.attribute("path", i.getFullPath(new Path(parent)).toUri().getPath());
    doc.attribute("modified", ldf.format(new Date(i.getModificationTime())));
    doc.attribute("accesstime", ldf.format(new Date(i.getAccessTime())));
    if (!i.isDir()) {
      doc.attribute("size", String.valueOf(i.getLen()));
      doc.attribute("replication", String.valueOf(i.getReplication()));
      doc.attribute("blocksize", String.valueOf(i.getBlockSize()));
    }
    doc.attribute("permission", (i.isDir()? "d": "-") + i.getPermission());
    doc.attribute("owner", i.getOwner());
    doc.attribute("group", i.getGroup());
    doc.endTag();
  }

  /**
   * Build a map from the query string, setting values and defaults.
   */
  protected Map<String,String> buildRoot(HttpServletRequest request,
      XMLOutputter doc) {
    final String path = ServletUtil.getDecodedPath(request, "/listPaths");
    final String exclude = request.getParameter("exclude") != null
      ? request.getParameter("exclude") : "\\..*\\.crc";
    final String filter = request.getParameter("filter") != null
      ? request.getParameter("filter") : ".*";
    final boolean recur = request.getParameter("recursive") != null
      && "yes".equals(request.getParameter("recursive"));

    Map<String, String> root = new HashMap<String, String>();
    root.put("path", path);
    root.put("recursive", recur ? "yes" : "no");
    root.put("filter", filter);
    root.put("exclude", exclude);
    root.put("time", df.get().format(new Date()));
    root.put("version", VersionInfo.getVersion());
    return root;
  }

  /**
   * Service a GET request as described below.
   * Request:
   * {@code
   * GET http://<nn>:<port>/listPaths[/<path>][<?option>[&option]*] HTTP/1.1
   * }
   *
   * Where <i>option</i> (default) in:
   * recursive (&quot;no&quot;)
   * filter (&quot;.*&quot;)
   * exclude (&quot;\..*\.crc&quot;)
   *
   * Response: A flat list of files/directories in the following format:
   * {@code
   *   <listing path="..." recursive="(yes|no)" filter="..."
   *            time="yyyy-MM-dd hh:mm:ss UTC" version="...">
   *     <directory path="..." modified="yyyy-MM-dd hh:mm:ss"/>
   *     <file path="..." modified="yyyy-MM-dd'T'hh:mm:ssZ" accesstime="yyyy-MM-dd'T'hh:mm:ssZ" 
   *           blocksize="..."
   *           replication="..." size="..."/>
   *   </listing>
   * }
   */
  public void doGet(HttpServletRequest request, HttpServletResponse response)
    throws ServletException, IOException {
    final PrintWriter out = response.getWriter();
    final XMLOutputter doc = new XMLOutputter(out, "UTF-8");

    final Map<String, String> root = buildRoot(request, doc);
    final String path = root.get("path");
    final String filePath = ServletUtil.getDecodedPath(request, "/listPaths");

    try {
      final boolean recur = "yes".equals(root.get("recursive"));
      final Pattern filter = Pattern.compile(root.get("filter"));
      final Pattern exclude = Pattern.compile(root.get("exclude"));
      final Configuration conf = 
        (Configuration) getServletContext().getAttribute(JspHelper.CURRENT_CONF);
      
      getUGI(request, conf).doAs
        (new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws IOException {
          ClientProtocol nn = createNameNodeProxy();
          doc.declaration();
          doc.startTag("listing");
          for (Map.Entry<String,String> m : root.entrySet()) {
            doc.attribute(m.getKey(), m.getValue());
          }

          HdfsFileStatus base = nn.getFileInfo(filePath);
          if ((base != null) && base.isDir()) {
            writeInfo(path, base, doc);
          }

          Stack<String> pathstack = new Stack<String>();
          pathstack.push(path);
          while (!pathstack.empty()) {
            String p = pathstack.pop();
            try {
              byte[] lastReturnedName = HdfsFileStatus.EMPTY_NAME;         
              DirectoryListing thisListing;
              do {
                assert lastReturnedName != null;
                thisListing = nn.getListing(p, lastReturnedName);
                if (thisListing == null) {
                  if (lastReturnedName.length == 0) {
                    LOG.warn("ListPathsServlet - Path " + p + " does not exist");
                  }
                  break;
                }
                HdfsFileStatus[] listing = thisListing.getPartialListing();
                for (HdfsFileStatus i : listing) {
                  String localName = i.getLocalName();
                  if (exclude.matcher(localName).matches()
                      || !filter.matcher(localName).matches()) {
                    continue;
                  }
                  if (recur && i.isDir()) {
                    pathstack.push(new Path(p, localName).toUri().getPath());
                  }
                  writeInfo(p, i, doc);
                }
                lastReturnedName = thisListing.getLastName();
              } while (thisListing.hasMore());
            } catch(IOException re) {
              writeXml(re, p, doc);
            }
          }
          return null;
        }
      });
    } catch(IOException ioe) {
      writeXml(ioe, path, doc);
    } catch (InterruptedException e) {
      LOG.warn("ListPathServlet encountered InterruptedException", e);
      response.sendError(400, e.getMessage());
    } finally {
      if (doc != null) {
        doc.endDocument();
      }
      if (out != null) {
        out.close();
      }
    }
  }
}
