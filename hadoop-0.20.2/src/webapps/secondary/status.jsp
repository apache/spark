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
  import="org.apache.hadoop.util.*"
%>

<html>
<link rel="stylesheet" type="text/css" href="/static/hadoop.css">
<title>Hadoop SecondaryNameNode</title>
    
<body>
<h1>SecondaryNameNode</h1>
<%= JspHelper.getVersionTable() %>
<hr />
<pre>
<%= application.getAttribute("secondary.name.node").toString() %>
</pre>

<br />
<b><a href="/logs/">Logs</a></b>
<%= ServletUtil.htmlFooter() %>
