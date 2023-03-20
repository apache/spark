---
layout: global
title: INCOMPATIBLE_DATA_TO_TABLE error class
displayTitle: INCOMPATIBLE_DATA_TO_TABLE error class
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

SQLSTATE: none assigned

Cannot write incompatible data to table `<tableName>`:

This error class has the following derived error classes:

## AMBIGUOUS_COLUMN_NAME

Ambiguous column name in the input data: `<colPath>`.

## CANNOT_FIND_DATA

Cannot find data for output column `<colPath>`.

## CANNOT_SAFELY_CAST

Cannot safely cast `<colPath>`: `<from>` to `<to>`.

## EXTRA_STRUCT_FIELDS

Cannot write extra fields to struct `<colPath>`: `<extraCols>`.

## NULLABLE_ARRAY_ELEMENTS

Cannot write nullable elements to array of non-nulls: `<colPath>`.

## NULLABLE_COLUMN

Cannot write nullable values to non-null column `<colPath>`.

## NULLABLE_MAP_VALUES

Cannot write nullable elements to array of non-nulls: `<colPath>`.

## STRUCT_MISSING_FIELDS

Struct `<colPath>` missing fields: `<missingFields>`.

## UNEXPECTED_COLUMN_NAME

Struct `<colPath>` `<order>`-th field name does not match (may be out of order): expected `<expected>`, found `<found>`.


