---
layout: global
title: Error Conditions
displayTitle: Error Conditions
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

{% comment %}
Don't discuss error classes (e.g. `42`) or sub-classes (e.g. `K01`) with users. It's not helpful.
Keep this documentation focused on error states (e.g. `58002`) and conditions (e.g.
`AMBIGUOUS_COLUMN_REFERENCE`), which is what users see and what they will typically be searching
for when they encounter an error.

To update this information, edit `error-conditions.json`. The table below will be automatically
derived from that file via `docs/_plugins/build-error-docs.py`.

Also note that this is a Jekyll comment and not an HTML comment so that this comment does not show
up in the generated HTML to end users. :-)
{% endcomment %}

This is a list of error states and conditions that may be returned by Spark SQL.

{% include_api_gen _generated/error-conditions.html %}
