/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
var PREPREND_CRUMBS=new Array();
var link1="@skinconfig.trail.link1.name@";
var link2="@skinconfig.trail.link2.name@";
var link3="@skinconfig.trail.link3.name@";
if(!(link1=="")&&!link1.indexOf( "@" ) == 0){
  PREPREND_CRUMBS.push( new Array( link1, @skinconfig.trail.link1.href@ ) ); }
if(!(link2=="")&&!link2.indexOf( "@" ) == 0){
  PREPREND_CRUMBS.push( new Array( link2, @skinconfig.trail.link2.href@ ) ); }
if(!(link3=="")&&!link3.indexOf( "@" ) == 0){
  PREPREND_CRUMBS.push( new Array( link3, @skinconfig.trail.link3.href@ ) ); }
var DISPLAY_SEPARATOR=" &gt; ";
var DISPLAY_PREPREND=" &gt; ";
var DISPLAY_POSTPREND=":";
var CSS_CLASS_CRUMB="breadcrumb";
var CSS_CLASS_TRAIL="breadcrumbTrail";
var CSS_CLASS_SEPARATOR="crumbSeparator";
var FILE_EXTENSIONS=new Array( ".html", ".htm", ".jsp", ".php", ".php3", ".php4" );
var PATH_SEPARATOR="/";

function sc(s) {
	var l=s.toLowerCase();
	return l.substr(0,1).toUpperCase()+l.substr(1);
}
function getdirs() {
	var t=document.location.pathname.split(PATH_SEPARATOR);
	var lc=t[t.length-1];
	for(var i=0;i < FILE_EXTENSIONS.length;i++)
	{
		if(lc.indexOf(FILE_EXTENSIONS[i]))
			return t.slice(1,t.length-1); }
	return t.slice(1,t.length);
}
function getcrumbs( d )
{
	var pre = "/";
	var post = "/";
	var c = new Array();
	if( d != null )
	{
		for(var i=0;i < d.length;i++) {
			pre+=d[i]+postfix;
			c.push(new Array(d[i],pre)); }
	}
	if(PREPREND_CRUMBS.length > 0 )
		return PREPREND_CRUMBS.concat( c );
	return c;
}
function gettrail( c )
{
	var h=DISPLAY_PREPREND;
	for(var i=0;i < c.length;i++)
	{
		h+='<a href="'+c[i][1]+'" >'+sc(c[i][0])+'</a>';
		if(i!=(c.length-1))
			h+=DISPLAY_SEPARATOR; }
	return h+DISPLAY_POSTPREND;
}

function gettrailXHTML( c )
{
	var h='<span class="'+CSS_CLASS_TRAIL+'">'+DISPLAY_PREPREND;
	for(var i=0;i < c.length;i++)
	{
		h+='<a href="'+c[i][1]+'" class="'+CSS_CLASS_CRUMB+'">'+sc(c[i][0])+'</a>';
		if(i!=(c.length-1))
			h+='<span class="'+CSS_CLASS_SEPARATOR+'">'+DISPLAY_SEPARATOR+'</span>'; }
	return h+DISPLAY_POSTPREND+'</span>';
}

if(document.location.href.toLowerCase().indexOf("http://")==-1)
	document.write(gettrail(getcrumbs()));
else
	document.write(gettrail(getcrumbs(getdirs())));

