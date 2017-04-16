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
 * getBlank script - when included in a html file and called from a form text field, will set the value of this field to ""
 * if the text value is still the standard value.
 * getPrompt script - when included in a html file and called from a form text field, will set the value of this field to the prompt
 * if the text value is empty.
 *
 * Typical usage:
 * <script type="text/javascript" language="JavaScript" src="getBlank.js"></script>
 * <input type="text" id="query" value="Search the site:" onFocus="getBlank (this, 'Search the site:');" onBlur="getBlank (this, 'Search the site:');"/>
 */
<!--
function getBlank (form, stdValue){
if (form.value == stdValue){
	form.value = '';
	}
return true;
}
function getPrompt (form, stdValue){
if (form.value == ''){
	form.value = stdValue;
	}
return true;
}
//-->
