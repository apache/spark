<%
/*
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
%>
<%@ page
  contentType="text/html; charset=UTF-8"
  import="javax.servlet.*"
  import="javax.servlet.http.*"
  import="java.io.*"
  import="java.util.*"
  import="java.net.*"
  import="org.apache.hadoop.conf.Configuration"
  import="org.apache.hadoop.fs.*"
  import="org.apache.hadoop.hdfs.*"
  import="org.apache.hadoop.hdfs.server.namenode.*"
  import="org.apache.hadoop.hdfs.server.datanode.*"
  import="org.apache.hadoop.hdfs.server.common.*"
  import="org.apache.hadoop.hdfs.protocol.*"
  import="org.apache.hadoop.io.*"
  import="org.apache.hadoop.conf.*"
  import="org.apache.hadoop.net.DNS"
  import="org.apache.hadoop.security.UserGroupInformation"
  import="org.apache.hadoop.util.*"
  import="org.apache.hadoop.http.HtmlQuoting"
  import="java.text.DateFormat"
%>
<%!
  static JspHelper jspHelper = new JspHelper();
  
  public void generateDirectoryStructure( JspWriter out, 
                                          HttpServletRequest req,
                                          HttpServletResponse resp,
                                          Configuration conf
                                         ) throws IOException, 
                                                  InterruptedException {
    String dir = HtmlQuoting.unquoteHtmlChars(req.getParameter("dir"));

    if (dir == null || dir.length() == 0) {
      out.print("Invalid input");
      return;
    }
    
    String tokenString = req.getParameter(JspHelper.DELEGATION_PARAMETER_NAME);
    UserGroupInformation ugi = JspHelper.getUGI(req, conf);
    String namenodeInfoPortStr = req.getParameter("namenodeInfoPort");
    int namenodeInfoPort = -1;
    if (namenodeInfoPortStr != null)
      namenodeInfoPort = Integer.parseInt(namenodeInfoPortStr);
    
    DFSClient dfs = JspHelper.getDFSClient(ugi, jspHelper.nameNodeAddr, 
                                           conf);
    String target = dir;
    if (!dfs.exists(target)) {
      out.print("<h3>File or directory : " + target + " does not exist</h3>");
      JspHelper.printGotoForm(out, namenodeInfoPort, tokenString, 
                              HtmlQuoting.quoteHtmlChars(target));
    }
    else {
      if( !dfs.isDirectory(target) ) { // a file
        List<LocatedBlock> blocks = 
          dfs.namenode.getBlockLocations(dir, 0, 1).getLocatedBlocks();
	      
        LocatedBlock firstBlock = null;
        DatanodeInfo [] locations = null;
        if (blocks.size() > 0) {
          firstBlock = blocks.get(0);
          locations = firstBlock.getLocations();
        }
        if (locations == null || locations.length == 0) {
          out.print("Empty file");
        } else {
          DatanodeInfo chosenNode = jspHelper.bestNode(firstBlock);
          String fqdn = InetAddress.getByName(chosenNode.getHost()).
            getCanonicalHostName();
          String datanodeAddr = chosenNode.getName();
          int datanodePort = Integer.parseInt(
                                              datanodeAddr.substring(
                                                                     datanodeAddr.indexOf(':') + 1, 
                                                                     datanodeAddr.length())); 
          String redirectLocation = "http://"+fqdn+":" +
            chosenNode.getInfoPort() + 
            "/browseBlock.jsp?blockId=" +
            firstBlock.getBlock().getBlockId() +
            "&blockSize=" + firstBlock.getBlock().getNumBytes() +
            "&genstamp=" + firstBlock.getBlock().getGenerationStamp() +
            "&filename=" + URLEncoder.encode(dir, "UTF-8") + 
            "&datanodePort="
            + datanodePort + "&namenodeInfoPort=" + namenodeInfoPort
            + JspHelper.getDelegationTokenUrlParam(tokenString);
          resp.sendRedirect(redirectLocation);
        }
        return;
      }
      // directory
      //generate a table and dump the info
      String [] headings = { "Name", "Type", "Size", "Replication", 
                              "Block Size", "Modification Time",
                              "Permission", "Owner", "Group" };
      out.print("<h3>Contents of directory ");
      JspHelper.printPathWithLinks(HtmlQuoting.quoteHtmlChars(dir), 
                                   out, namenodeInfoPort, tokenString);
      out.print("</h3><hr>");
      JspHelper.printGotoForm(out, namenodeInfoPort, tokenString, 
                              HtmlQuoting.quoteHtmlChars(dir));
      out.print("<hr>");
	
      File f = new File(dir);
      String parent;
      if ((parent = f.getParent()) != null)
        out.print("<a href=\"" + req.getRequestURL() +
                  "?dir=" + URLEncoder.encode(parent, "UTF-8") +
                  "&namenodeInfoPort=" + namenodeInfoPort +
                  JspHelper.getDelegationTokenUrlParam(tokenString) +
                  "\">Go to parent directory</a><br>");

      DirectoryListing thisListing = dfs.listPaths(target, HdfsFileStatus.EMPTY_NAME);
      if (thisListing == null || thisListing.getPartialListing().length == 0) { 
        out.print("Empty directory");
      }
      else {
        jspHelper.addTableHeader(out);
        out.print("<thead>\n");
        jspHelper.addTableHeadingRow(out, headings);
        out.print("</thead><tbody>\n");
        String cols [] = new String[headings.length];
        do {
          HdfsFileStatus[] files = thisListing.getPartialListing();
          for (int i = 0; i < files.length; i++) {
            //Get the location of the first block of the file
            String localname = files[i].getLocalName();
            if (localname.endsWith(".crc")) continue;
            if (!files[i].isDir()) {
              cols[1] = "file";
              cols[2] = StringUtils.byteDesc(files[i].getLen());
              cols[3] = Short.toString(files[i].getReplication());
              cols[4] = StringUtils.byteDesc(files[i].getBlockSize());
            }
            else {
              cols[1] = "dir";
              cols[2] = "";
              cols[3] = "";
              cols[4] = "";
            }
            String datanodeUrl = req.getRequestURL()+"?dir="+
              URLEncoder.encode(files[i].getFullName(target), "UTF-8") + 
              "&namenodeInfoPort=" + namenodeInfoPort + 
              JspHelper.getDelegationTokenUrlParam(tokenString);
            cols[0] = "<a href=\""+datanodeUrl+"\">"+localname+"</a>";
            cols[5] = FsShell.dateForm.format(new Date((files[i].getModificationTime())));
            cols[6] = files[i].getPermission().toString();
            cols[7] = files[i].getOwner();
            cols[8] = files[i].getGroup();
            jspHelper.addTableRow(out, cols);
          }
          if (!thisListing.hasMore()) {
            out.print("</tbody>\n");
            break;
          }
          thisListing = dfs.listPaths(target, thisListing.getLastName());
        } while (thisListing != null);
        jspHelper.addTableFooter(out);
      }
    } 
    String namenodeHost = jspHelper.nameNodeAddr.getHostName();
    out.print("<br><a href=\"http://" + 
              InetAddress.getByName(namenodeHost).getCanonicalHostName() + ":" +
              namenodeInfoPort + "/dfshealth.jsp\">Go back to DFS home</a>");
    dfs.close();
  }

%>

<html>
<head>
<link rel="stylesheet" type="text/css" href="/static/hadoop.css">
<link rel="icon" type="image/vnd.microsoft.icon" href="/static/images/favicon.ico" />
<%JspHelper.createTitle(out, request, request.getParameter("dir")); %>
</head>

<body onload="document.goto.dir.focus()">
<% 
  Configuration conf = 
    (Configuration) getServletContext().getAttribute(JspHelper.CURRENT_CONF);
  try {
    generateDirectoryStructure(out,request,response,conf);
  }
  catch(IOException ioe) {
    String msg = ioe.getLocalizedMessage();
    int i = msg.indexOf("\n");
    if (i >= 0) {
      msg = msg.substring(0, i);
    }
    out.print("<h3>" + msg + "</h3>");
  }
%>
<hr>

<h2>Local logs</h2>
<a href="/logs/">Log</a> directory

<%
out.println(ServletUtil.htmlFooter());
%>
