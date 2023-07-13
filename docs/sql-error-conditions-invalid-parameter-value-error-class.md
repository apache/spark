---
layout: global
title: INVALID_PARAMETER_VALUE error class
displayTitle: INVALID_PARAMETER_VALUE error class
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

[SQLSTATE: 22023](sql-error-conditions-sqlstates.html#class-22-data-exception)

The value of parameter(s) `<parameter>` in `<functionName>` is invalid:

This error class has the following derived error classes:

## AES_CRYPTO_ERROR

detail message: `<detailMessage>`

## AES_IV_LENGTH

supports 16-byte CBC IVs and 12-byte GCM IVs, but got `<actualLength>` bytes for `<mode>`.

## AES_KEY_LENGTH

expects a binary value with 16, 24 or 32 bytes, but got `<actualLength>` bytes.

## DATETIME_UNIT

expects one of the units without quotes YEAR, QUARTER, MONTH, WEEK, DAY, DAYOFYEAR, HOUR, MINUTE, SECOND, MILLISECOND, MICROSECOND, but got the string literal `<invalidValue>`.

## PATTERN

`<value>`.

## REGEX_GROUP_INDEX

Expects group index between 0 and `<groupCount>`, but got `<groupIndex>`.

## ZERO_INDEX

expects `%1$`, `%2$` and so on, but got `%0$`.


