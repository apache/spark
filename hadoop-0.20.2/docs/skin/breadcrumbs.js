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
/**
 * This script, when included in a html file, builds a neat breadcrumb trail
 * based on its url. That is, if it doesn't contains bugs (I'm relatively
 * sure it does).
 *
 * Typical usage:
 * <script type="text/javascript" language="JavaScript" src="breadcrumbs.js"></script>
 */

/**
 * IE 5 on Mac doesn't know Array.push.
 *
 * Implement it - courtesy to fritz.
 */
var abc	= new Array();
if (!abc.push) {
  Array.prototype.push	= function(what){this[this.length]=what}
}

/* ========================================================================
	CONSTANTS
   ======================================================================== */

/**
 * Two-dimensional array containing extra crumbs to place at the front of
 * the trail. Specify first the name of the crumb, then the URI that belongs
 * to it. You'll need to modify this for every domain or subdomain where
 * you use this script (you can leave it as an empty array if you wish)
 */
var PREPREND_CRUMBS = new Array();

var link1 = "@skinconfig.trail.link1.name@";
var link2 = "@skinconfig.trail.link2.name@";
var link3 = "@skinconfig.trail.link3.name@";

var href1 = "@skinconfig.trail.link1.href@";
var href2 = "@skinconfig.trail.link2.href@";
var href3 = "@skinconfig.trail.link3.href@";

   if(!(link1=="")&&!link1.indexOf( "@" ) == 0){
     PREPREND_CRUMBS.push( new Array( link1, href1 ) );
   }
   if(!(link2=="")&&!link2.indexOf( "@" ) == 0){
     PREPREND_CRUMBS.push( new Array( link2, href2 ) );
   }
   if(!(link3=="")&&!link3.indexOf( "@" ) == 0){
     PREPREND_CRUMBS.push( new Array( link3, href3 ) );
   }

/**
 * String to include between crumbs:
 */
var DISPLAY_SEPARATOR = " &gt; ";
/**
 * String to include at the beginning of the trail
 */
var DISPLAY_PREPREND = " &gt; ";
/**
 * String to include at the end of the trail
 */
var DISPLAY_POSTPREND = "";

/**
 * CSS Class to use for a single crumb:
 */
var CSS_CLASS_CRUMB = "breadcrumb";

/**
 * CSS Class to use for the complete trail:
 */
var CSS_CLASS_TRAIL = "breadcrumbTrail";

/**
 * CSS Class to use for crumb separator:
 */
var CSS_CLASS_SEPARATOR = "crumbSeparator";

/**
 * Array of strings containing common file extensions. We use this to
 * determine what part of the url to ignore (if it contains one of the
 * string specified here, we ignore it).
 */
var FILE_EXTENSIONS = new Array( ".html", ".htm", ".jsp", ".php", ".php3", ".php4" );

/**
 * String that separates parts of the breadcrumb trail from each other.
 * When this is no longer a slash, I'm sure I'll be old and grey.
 */
var PATH_SEPARATOR = "/";

/* ========================================================================
	UTILITY FUNCTIONS
   ======================================================================== */
/**
 * Capitalize first letter of the provided string and return the modified
 * string.
 */
function sentenceCase( string )
{        return string;
	//var lower = string.toLowerCase();
	//return lower.substr(0,1).toUpperCase() + lower.substr(1);
}

/**
 * Returns an array containing the names of all the directories in the
 * current document URL
 */
function getDirectoriesInURL()
{
	var trail = document.location.pathname.split( PATH_SEPARATOR );

	// check whether last section is a file or a directory
	var lastcrumb = trail[trail.length-1];
	for( var i = 0; i < FILE_EXTENSIONS.length; i++ )
	{
		if( lastcrumb.indexOf( FILE_EXTENSIONS[i] ) )
		{
			// it is, remove it and send results
			return trail.slice( 1, trail.length-1 );
		}
	}

	// it's not; send the trail unmodified
	return trail.slice( 1, trail.length );
}

/* ========================================================================
	BREADCRUMB FUNCTIONALITY
   ======================================================================== */
/**
 * Return a two-dimensional array describing the breadcrumbs based on the
 * array of directories passed in.
 */
function getBreadcrumbs( dirs )
{
	var prefix = "/";
	var postfix = "/";

	// the array we will return
	var crumbs = new Array();

	if( dirs != null )
	{
		for( var i = 0; i < dirs.length; i++ )
		{
			prefix += dirs[i] + postfix;
			crumbs.push( new Array( dirs[i], prefix ) );
		}
	}

	// preprend the PREPREND_CRUMBS
	if(PREPREND_CRUMBS.length > 0 )
	{
		return PREPREND_CRUMBS.concat( crumbs );
	}

	return crumbs;
}

/**
 * Return a string containing a simple text breadcrumb trail based on the
 * two-dimensional array passed in.
 */
function getCrumbTrail( crumbs )
{
	var xhtml = DISPLAY_PREPREND;

	for( var i = 0; i < crumbs.length; i++ )
	{
		xhtml += '<a href="' + crumbs[i][1] + '" >';
		xhtml += unescape( crumbs[i][0] ) + '</a>';
		if( i != (crumbs.length-1) )
		{
			xhtml += DISPLAY_SEPARATOR;
		}
	}

	xhtml += DISPLAY_POSTPREND;

	return xhtml;
}

/**
 * Return a string containing an XHTML breadcrumb trail based on the
 * two-dimensional array passed in.
 */
function getCrumbTrailXHTML( crumbs )
{
	var xhtml = '<span class="' + CSS_CLASS_TRAIL  + '">';
	xhtml += DISPLAY_PREPREND;

	for( var i = 0; i < crumbs.length; i++ )
	{
		xhtml += '<a href="' + crumbs[i][1] + '" class="' + CSS_CLASS_CRUMB + '">';
		xhtml += unescape( crumbs[i][0] ) + '</a>';
		if( i != (crumbs.length-1) )
		{
			xhtml += '<span class="' + CSS_CLASS_SEPARATOR + '">' + DISPLAY_SEPARATOR + '</span>';
		}
	}

	xhtml += DISPLAY_POSTPREND;
	xhtml += '</span>';

	return xhtml;
}

/* ========================================================================
	PRINT BREADCRUMB TRAIL
   ======================================================================== */

// check if we're local; if so, only print the PREPREND_CRUMBS
if( document.location.href.toLowerCase().indexOf( "http://" ) == -1 )
{
	document.write( getCrumbTrail( getBreadcrumbs() ) );
}
else
{
	document.write( getCrumbTrail( getBreadcrumbs( getDirectoriesInURL() ) ) );
}

