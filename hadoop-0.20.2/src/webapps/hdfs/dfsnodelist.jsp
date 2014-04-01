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
    isThreadSafe="false"
	import="javax.servlet.*"
	import="javax.servlet.http.*"
	import="java.io.*"
	import="java.util.*"
	import="org.apache.hadoop.fs.*"
	import="org.apache.hadoop.hdfs.*"
	import="org.apache.hadoop.hdfs.server.common.*"
	import="org.apache.hadoop.hdfs.server.namenode.*"
	import="org.apache.hadoop.hdfs.server.datanode.*"
	import="org.apache.hadoop.hdfs.protocol.*"
	import="org.apache.hadoop.util.*"
	import="java.text.DateFormat"
	import="java.lang.Math"
	import="java.net.URLEncoder"
%>
<%!
	JspHelper jspHelper = new JspHelper();

	int rowNum = 0;
	int colNum = 0;

	String rowTxt() { colNum = 0;
	return "<tr class=\"" + (((rowNum++)%2 == 0)? "rowNormal" : "rowAlt")
	+ "\"> "; }
	String colTxt() { return "<td id=\"col" + ++colNum + "\"> "; }
	void counterReset () { colNum = 0; rowNum = 0 ; }

	long diskBytes = 1024 * 1024 * 1024;
	String diskByteStr = "GB";

	String sorterField = null;
	String sorterOrder = null;
	String whatNodes = "LIVE";

String NodeHeaderStr(String name) {
	String ret = "class=header";
	String order = "ASC";
	if ( name.equals( sorterField ) ) {
		ret += sorterOrder;
		if ( sorterOrder.equals("ASC") )
			order = "DSC";
	}
	ret += " onClick=\"window.document.location=" +
	"'/dfsnodelist.jsp?whatNodes="+whatNodes+"&sorter/field=" + name + "&sorter/order=" +
	order + "'\" title=\"sort on this column\"";

	return ret;
}

void generateDecommissioningNodeData(JspWriter out, DatanodeDescriptor d,
      String suffix, boolean alive, int nnHttpPort) throws IOException {
    String url = "http://" + d.getHostName() + ":" + d.getInfoPort()
      + "/browseDirectory.jsp?namenodeInfoPort=" + nnHttpPort + "&dir="
      + URLEncoder.encode("/", "UTF-8");

    String name = d.getHostName() + ":" + d.getPort();
    if (!name.matches("\\d+\\.\\d+.\\d+\\.\\d+.*"))
      name = name.replaceAll("\\.[^.:]*", "");
    int idx = (suffix != null && name.endsWith(suffix)) ? name
        .indexOf(suffix) : -1;

    out.print(rowTxt() + "<td class=\"name\"><a title=\"" + d.getHost() + ":"
        + d.getPort() + "\" href=\"" + url + "\">"
        + ((idx > 0) ? name.substring(0, idx) : name) + "</a>"
        + ((alive) ? "" : "\n"));
      if (!alive) {
        return;
      }

    long decommRequestTime = d.decommissioningStatus.getStartTime();
    long timestamp = d.getLastUpdate();
    long currentTime = System.currentTimeMillis();
    long hoursSinceDecommStarted = (currentTime - decommRequestTime)/3600000;
    long remainderMinutes = ((currentTime - decommRequestTime)/60000) % 60;
    out.print("<td class=\"lastcontact\"> "
        + ((currentTime - timestamp) / 1000)
        + "<td class=\"underreplicatedblocks\">"
        + d.decommissioningStatus.getUnderReplicatedBlocks()
        + "<td class=\"blockswithonlydecommissioningreplicas\">"
        + d.decommissioningStatus.getDecommissionOnlyReplicas()
        + "<td class=\"underrepblocksinfilesunderconstruction\">"
        + d.decommissioningStatus.getUnderReplicatedInOpenFiles()
        + "<td class=\"timesincedecommissionrequest\">"
        + hoursSinceDecommStarted + " hrs " + remainderMinutes + " mins"
        + "\n");
}


public void generateNodeData( JspWriter out, DatanodeDescriptor d,
		String suffix, boolean alive,
		int nnHttpPort )
throws IOException {

	/* Say the datanode is dn1.hadoop.apache.org with ip 192.168.0.5
we use:
1) d.getHostName():d.getPort() to display.
Domain and port are stripped if they are common across the nodes.
i.e. "dn1"
2) d.getHost():d.Port() for "title".
i.e. "192.168.0.5:50010"
3) d.getHostName():d.getInfoPort() for url.
i.e. "http://dn1.hadoop.apache.org:50075/..."
Note that "d.getHost():d.getPort()" is what DFS clients use
to interact with datanodes.
	 */
	// from nn_browsedfscontent.jsp:
	String url = "http://" + d.getHostName() + ":" + d.getInfoPort() +
	"/browseDirectory.jsp?namenodeInfoPort=" +
	nnHttpPort + "&dir=" +
	URLEncoder.encode("/", "UTF-8");

	String name = d.getHostName() + ":" + d.getPort();
	if ( !name.matches( "\\d+\\.\\d+.\\d+\\.\\d+.*" ) ) 
		name = name.replaceAll( "\\.[^.:]*", "" );    
	int idx = (suffix != null && name.endsWith( suffix )) ?
			name.indexOf( suffix ) : -1;

			out.print( rowTxt() + "<td class=\"name\"><a title=\""
					+ d.getHost() + ":" + d.getPort() +
					"\" href=\"" + url + "\">" +
					(( idx > 0 ) ? name.substring(0, idx) : name) + "</a>" +
					(( alive ) ? "" : "\n") );
			if ( !alive )
				return;

			long c = d.getCapacity();
			long u = d.getDfsUsed();
			long nu = d.getNonDfsUsed();
			long r = d.getRemaining();
			String percentUsed = StringUtils.limitDecimalTo2(d.getDfsUsedPercent());    
			String percentRemaining = StringUtils.limitDecimalTo2(d.getRemainingPercent());    

			String adminState = (d.isDecommissioned() ? "Decommissioned" :
				(d.isDecommissionInProgress() ? "Decommission In Progress":
				"In Service"));

			long timestamp = d.getLastUpdate();
			long currentTime = System.currentTimeMillis();
			out.print("<td class=\"lastcontact\"> " +
					((currentTime - timestamp)/1000) +
					"<td class=\"adminstate\">" +
					adminState +
					"<td align=\"right\" class=\"capacity\">" +
					StringUtils.limitDecimalTo2(c*1.0/diskBytes) +
					"<td align=\"right\" class=\"used\">" +
					StringUtils.limitDecimalTo2(u*1.0/diskBytes) +      
					"<td align=\"right\" class=\"nondfsused\">" +
					StringUtils.limitDecimalTo2(nu*1.0/diskBytes) +      
					"<td align=\"right\" class=\"remaining\">" +
					StringUtils.limitDecimalTo2(r*1.0/diskBytes) +      
					"<td align=\"right\" class=\"pcused\">" + percentUsed +
					"<td class=\"pcused\">" +
					ServletUtil.percentageGraph( (int)Double.parseDouble(percentUsed) , 100) +
					"<td align=\"right\" class=\"pcremaining`\">" + percentRemaining +
					"<td title=" + "\"blocks scheduled : " + d.getBlocksScheduled() + 
					"\" class=\"blocks\">" + d.numBlocks() +
					"<td class=\"volfails\">" + d.getVolumeFailures() + "\n");
}



public void generateDFSNodesList(JspWriter out, 
		NameNode nn,
		HttpServletRequest request)
throws IOException {
	ArrayList<DatanodeDescriptor> live = new ArrayList<DatanodeDescriptor>();    
	ArrayList<DatanodeDescriptor> dead = new ArrayList<DatanodeDescriptor>();
	jspHelper.DFSNodesStatus(live, dead);

       //verify input for correctness 
       String whatNodes = request.getParameter("whatNodes");// show only live or only dead nodes
       if (whatNodes == null || whatNodes.length() == 0) {
         out.print("Invalid input");
         return;
       }

	sorterField = request.getParameter("sorter/field");
	sorterOrder = request.getParameter("sorter/order");
	if ( sorterField == null )
		sorterField = "name";
	if ( sorterOrder == null )
		sorterOrder = "ASC";

	jspHelper.sortNodeList(live, sorterField, sorterOrder);
	jspHelper.sortNodeList(dead, "name", "ASC");

	// Find out common suffix. Should this be before or after the sort?
	String port_suffix = null;
	if ( live.size() > 0 ) {
		String name = live.get(0).getName();
		int idx = name.indexOf(':');
		if ( idx > 0 ) {
			port_suffix = name.substring( idx );
		}

		for ( int i=1; port_suffix != null && i < live.size(); i++ ) {
			if ( live.get(i).getName().endsWith( port_suffix ) == false ) {
				port_suffix = null;
				break;
			}
		}
	}

	counterReset();

	try {
		Thread.sleep(1000);
	} catch (InterruptedException e) {}

	if (live.isEmpty() && dead.isEmpty()) {
		out.print("There are no datanodes in the cluster");
	}
	else {

		int nnHttpPort = nn.getHttpAddress().getPort();
		out.print( "<div id=\"dfsnodetable\"> ");
		if(whatNodes.equals("LIVE")) {

			out.print( 
					"<a name=\"LiveNodes\" id=\"title\">" +
					"Live Datanodes : " + live.size() + "</a>" +
			"<br><br>\n<table border=1 cellspacing=0>\n" );

			counterReset();

			if ( live.size() > 0 ) {

				if ( live.get(0).getCapacity() > 1024 * diskBytes ) {
					diskBytes *= 1024;
					diskByteStr = "TB";
				}

				out.print( "<tr class=\"headerRow\"> <th " +
						NodeHeaderStr("name") + "> Node <th " +
						NodeHeaderStr("lastcontact") + "> Last <br>Contact <th " +
						NodeHeaderStr("adminstate") + "> Admin State <th " +
						NodeHeaderStr("capacity") + "> Configured <br>Capacity (" + 
						diskByteStr + ") <th " + 
						NodeHeaderStr("used") + "> Used <br>(" + 
						diskByteStr + ") <th " + 
						NodeHeaderStr("nondfsused") + "> Non DFS <br>Used (" + 
						diskByteStr + ") <th " + 
						NodeHeaderStr("remaining") + "> Remaining <br>(" + 
						diskByteStr + ") <th " + 
						NodeHeaderStr("pcused") + "> Used <br>(%) <th " + 
						NodeHeaderStr("pcused") + "> Used <br>(%) <th " +
						NodeHeaderStr("pcremaining") + "> Remaining <br>(%) <th " +
						NodeHeaderStr("blocks") + "> Blocks <th " +
						NodeHeaderStr("volfails") + "> Failed Volumes\n" );

				jspHelper.sortNodeList(live, sorterField, sorterOrder);
				for ( int i=0; i < live.size(); i++ ) {
					generateNodeData(out, live.get(i), port_suffix, true, nnHttpPort);
				}
			}
			out.print("</table>\n");
		} else if (whatNodes.equals("DEAD")) {

			out.print("<br> <a name=\"DeadNodes\" id=\"title\"> " +
					" Dead Datanodes : " +dead.size() + "</a><br><br>\n");

			if ( dead.size() > 0 ) {
				out.print( "<table border=1 cellspacing=0> <tr id=\"row1\"> " +
				"<td> Node \n" );

				jspHelper.sortNodeList(dead, "name", "ASC");
				for ( int i=0; i < dead.size() ; i++ ) {
					generateNodeData(out, dead.get(i), port_suffix, false, nnHttpPort);
				}

				out.print("</table>\n");
			}
		} else if (whatNodes.equals("DECOMMISSIONING")) {
			// Decommissioning Nodes
			ArrayList<DatanodeDescriptor> decommissioning = nn.getNamesystem()
			    .getDecommissioningNodes();
			out.print("<br> <a name=\"DecommissioningNodes\" id=\"title\"> "
			    + " Decommissioning Datanodes : " + decommissioning.size()
                            + "</a><br><br>\n");
	                if (decommissioning.size() > 0) {
                          out.print("<table border=1 cellspacing=0> <tr class=\"headRow\"> "
                              + "<th " + NodeHeaderStr("name")
                              + "> Node <th " + NodeHeaderStr("lastcontact")
                              + "> Last <br>Contact <th "
                              + NodeHeaderStr("underreplicatedblocks")
                              + "> Under Replicated Blocks <th "
                              + NodeHeaderStr("blockswithonlydecommissioningreplicas")
		              + "> Blocks With No <br> Live Replicas <th "
	                      + NodeHeaderStr("underrepblocksinfilesunderconstruction") 
	                      + "> Under Replicated Blocks <br> In Files Under Construction"
	                      + " <th " + NodeHeaderStr("timesincedecommissionrequest")
	                      + "> Time Since Decommissioning Started"
                          );
                          jspHelper.sortNodeList(decommissioning, "name", "ASC");
                          for (int i = 0; i < decommissioning.size(); i++) {
                            generateDecommissioningNodeData(out, decommissioning.get(i),
                                port_suffix, true, nnHttpPort);
                          }
                          out.print("</table>\n");
                        }
                        out.print("</div>");
                  } else {
                    // if nothing matches then print invalid input
                    out.println("Invalid input");
                  }
	}
}%>

<%
NameNode nn = (NameNode)application.getAttribute("name.node");
FSNamesystem fsn = nn.getNamesystem();
String namenodeLabel = nn.getNameNodeAddress().getHostName() + ":" + nn.getNameNodeAddress().getPort();
%>

<html>

<link rel="stylesheet" type="text/css" href="/static/hadoop.css">
<link rel="icon" type="image/vnd.microsoft.icon" href="/static/images/favicon.ico" />
<title>Hadoop NameNode <%=namenodeLabel%></title>
  
<body>
<h1>NameNode '<%=namenodeLabel%>'</h1>


<div id="dfstable"> <table>	  
<tr> <td id="col1"> Started: <td> <%= fsn.getStartTime()%>
<tr> <td id="col1"> Version: <td> <%= VersionInfo.getVersion()%>, r<%= VersionInfo.getRevision()%>
<tr> <td id="col1"> Compiled: <td> <%= VersionInfo.getDate()%> by <%= VersionInfo.getUser()%>
<tr> <td id="col1"> Upgrades: <td> <%= jspHelper.getUpgradeStatusText()%>
</table></div><br>				      

<b><a href="/nn_browsedfscontent.jsp">Browse the filesystem</a></b><br>
<b><a href="/logs/">Namenode Logs</a></b><br>
<b><a href=/dfshealth.jsp> Go back to DFS home</a></b>
<hr>
<%
	generateDFSNodesList(out, nn, request); 
%>

<%
out.println(ServletUtil.htmlFooter());
%>
