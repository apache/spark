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
function init() 
{ //embedded in the doc
  //ndeSetTextSize();
}

function checkBrowser(){
  if (!document.getElementsByTagName){
    return true;
  }
  else{
    return false;
  }
}


function ndeSetTextSize(chgsize,rs) 
{
  var startSize;
  var newSize;

  if (!checkBrowser)
  {
    return;
  }

  startSize = parseInt(ndeGetDocTextSize());

  if (!startSize)
  {
    startSize = 16;
  }

  switch (chgsize)
  {
  case 'incr':
    newSize = startSize + 2;
    break;

  case 'decr':
    newSize = startSize - 2;
    break;

  case 'reset':
    if (rs) {newSize = rs;} else {newSize = 16;}
    break;

  default:
    try{
      newSize = parseInt(ndeReadCookie("nde-textsize"));
    }
    catch(e){
      alert(e);
    }
    
    if (!newSize || newSize == 'NaN')
    {
      newSize = startSize;
    }
    break;

  }

  if (newSize < 10) 
  {
    newSize = 10;
  }

  newSize += 'px';

  document.getElementsByTagName('html')[0].style.fontSize = newSize;
  document.getElementsByTagName('body')[0].style.fontSize = newSize;

  ndeCreateCookie("nde-textsize", newSize, 365);
}

function ndeGetDocTextSize() 
{
  if (!checkBrowser)
  {
    return 0;
  }

  var size = 0;
  var body = document.getElementsByTagName('body')[0];

  if (body.style && body.style.fontSize)
  {
    size = body.style.fontSize;
  }
  else if (typeof(getComputedStyle) != 'undefined')
  {
    size = getComputedStyle(body,'').getPropertyValue('font-size');
  }
  else if (body.currentStyle)
  {
   size = body.currentStyle.fontSize;
  }

  //fix IE bug
  if( isNaN(size)){
    if(size.substring(size.length-1)=="%"){
      return
    }

  }

  return size;

}



function ndeCreateCookie(name,value,days) 
{
  var cookie = name + "=" + value + ";";

  if (days) 
  {
    var date = new Date();
    date.setTime(date.getTime()+(days*24*60*60*1000));
    cookie += " expires=" + date.toGMTString() + ";";
  }
  cookie += " path=/";

  document.cookie = cookie;

}

function ndeReadCookie(name) 
{
  var nameEQ = name + "=";
  var ca = document.cookie.split(';');

 
  for(var i = 0; i < ca.length; i++) 
  {
    var c = ca[i];
    while (c.charAt(0) == ' ') 
    {
      c = c.substring(1, c.length);
    }

    ctest = c.substring(0,name.length);
 
    if(ctest == name){
      return c.substring(nameEQ.length,c.length);
    }
  }
  return null;
}
