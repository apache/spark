---
layout: global
title: Number patterns
displayTitle: Number Patterns for Formatting and Parsing
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

Spark uses pattern letters in the following table for number parsing and formatting:

|Symbol|Meaning|Examples|
|------|-------|--------|
|**9**|digit position (can be dropped if insignificant)|9999|
|**0**|digit position (will not be dropped, even if insignificant)|0999|
|**.**|decimal point (only allowed once)|99.99|
|**D**|decimal point (only allowed once)|99D99|
|**,**|group (thousands) separator|9,999|
|**G**|group (thousands) separator|9G999|
|**-**|sign anchored to number|-9999|
|**S**|sign anchored to number|S9999|
|**$**|returns value with a leading dollar sign|$9999|

Usage notes for numeric formatting:

- 0 specifies a digit position that will always be printed, even if it contains a leading/trailing zero. 9 also specifies a digit position, but if it is a leading zero then it will be replaced by a space, while if it is a trailing zero and fill mode is specified then it will be deleted. (For to_number(), these two pattern characters are equivalent.)

- The pattern characters S, D, and G represent the sign, decimal point, and thousands separator characters. The pattern characters period and comma represent those exact characters, with the meanings of decimal point and thousands separator.
