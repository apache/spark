---
layout: global
title: DROP FUNCTION
displayTitle: DROP FUNCTION 
license: |
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at
 
     http://www.apache.org/licenses/LICENSE-2.0
 
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
---

### Description
Dropping a user-defined function(UDF). An exception will be thrown if the function does not exist
 in the system. 

### Syntax
{% highlight sql %}
DROP [TEMPORARY] FUNCTION [IF EXISTS] [db_name.]function_name;
{% endhighlight %}

### Example
{% highlight sql %}
DROP TEMPORARY FUNCTION tempfunction;
DROP FUNCTION testdb.permfunction;
{% endhighlight %}

### Parameters

### **function_name**

The name of an existing function.

### **TEMPORARY**

Should be used to delete the `temporary` function.

### **IF EXISTS**

If specified, no exception is thrown when the function does not exist.