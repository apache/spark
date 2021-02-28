---
layout: global
title: SQL Metrics
displayTitle: SQL Metrics
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

The SQL tab in the Spark UI displays the DAG of a query. This DAG is composed of multiple operators
and each operator displays various metrics that gives insight into the progress of the query and 
help us understand how spark is evaluating the query. Some metrics that are displayed in the SQL tab
are listed below.

## Number of output rows
This metric represents the number of rows that are outputted by an operator. Typically, the value
of this metric is the number of rows that are input to the next operator in the DAG

## Number of matched rows
When spark joins two relations(tables) in a query, it sometimes has to match a row on left side
of the relation with a row on the right side of the relation. This metric represents the number of 
times a row on the left side is matched with a row on the right side.
