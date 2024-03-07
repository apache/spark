---
layout: global
title: UNSUPPORTED_GENERATOR error class
displayTitle: UNSUPPORTED_GENERATOR error class
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

[SQLSTATE: 0A000](sql-error-conditions-sqlstates.html#class-0A-feature-not-supported)

The generator is not supported:

This error class has the following derived error classes:

## MULTI_GENERATOR

only one generator allowed per SELECT clause but found `<num>`: `<generators>`.

## NESTED_IN_EXPRESSIONS

nested in expressions `<expression>`.

## NOT_GENERATOR

`<functionName>` is expected to be a generator. However, its class is `<classCanonicalName>`, which is not a generator.

## OUTSIDE_SELECT

outside the SELECT clause, found: `<plan>`.


