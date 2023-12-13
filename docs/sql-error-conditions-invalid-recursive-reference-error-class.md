---
layout: global
title: INVALID_RECURSIVE_REFERENCE error class
displayTitle: INVALID_RECURSIVE_REFERENCE error class
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

[SQLSTATE: 42836](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

Invalid recursive reference found.

This error class has the following derived error classes:

## DATA_TYPE

The data type of recursive references cannot change during resolution. Originally it was `<fromDataType>` but after resolution is `<toDataType>`.

## NUMBER

Recursive references cannot be used multiple times.

## PLACE

Recursive references cannot be used on the right side of left outer/semi/anti joins, on the left side of right outer joins, in full outer joins and in aggregates.


