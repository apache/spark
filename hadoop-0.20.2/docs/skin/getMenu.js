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
 * This script, when included in a html file, can be used to make collapsible menus
 *
 * Typical usage:
 * <script type="text/javascript" language="JavaScript" src="menu.js"></script>
 */

if (document.getElementById){ 
  document.write('<style type="text/css">.menuitemgroup{display: none;}</style>')
}


function SwitchMenu(obj, thePath)
{
var open = 'url("'+thePath + 'images/chapter_open.gif")';
var close = 'url("'+thePath + 'images/chapter.gif")';
  if(document.getElementById)  {
    var el = document.getElementById(obj);
    var title = document.getElementById(obj+'Title');

    if(el.style.display != "block"){ 
      title.style.backgroundImage = open;
      el.style.display = "block";
    }else{
      title.style.backgroundImage = close;
      el.style.display = "none";
    }
  }// end -  if(document.getElementById) 
}//end - function SwitchMenu(obj)
