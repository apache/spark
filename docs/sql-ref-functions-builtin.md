---
layout: global
title: Built-in Functions
displayTitle: Built-in Functions
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

{% for static_file in site.static_files %}
    {% if static_file.name == 'generated-agg-funcs-table.html' %}
### Aggregate Functions
{% include_relative generated-agg-funcs-table.html %}
#### Examples
{% include_relative generated-agg-funcs-examples.html %}
        {% break %}
    {% endif %}
{% endfor %}

{% for static_file in site.static_files %}
    {% if static_file.name == 'generated-window-funcs-table.html' %}
### Window Functions
{% include_relative generated-window-funcs-table.html %}
#### Examples
{% include_relative generated-window-funcs-examples.html %}
        {% break %}
    {% endif %}
{% endfor %}

{% for static_file in site.static_files %}
    {% if static_file.name == 'generated-array-funcs-table.html' %}
### Array Functions
{% include_relative generated-array-funcs-table.html %}
#### Examples
{% include_relative generated-array-funcs-examples.html %}
        {% break %}
    {% endif %}
{% endfor %}

{% for static_file in site.static_files %}
    {% if static_file.name == 'generated-map-funcs-table.html' %}
### Map Functions
{% include_relative generated-map-funcs-table.html %}
#### Examples
{% include_relative generated-map-funcs-examples.html %}
        {% break %}
    {% endif %}
{% endfor %}

{% for static_file in site.static_files %}
    {% if static_file.name == 'generated-datetime-funcs-table.html' %}
### Date and Timestamp Functions
{% include_relative generated-datetime-funcs-table.html %}
#### Examples
{% include_relative generated-datetime-funcs-examples.html %}
        {% break %}
    {% endif %}
{% endfor %}

{% for static_file in site.static_files %}
    {% if static_file.name == 'generated-json-funcs-table.html' %}
### JSON Functions
{% include_relative generated-json-funcs-table.html %}
#### Examples
{% include_relative generated-json-funcs-examples.html %}
        {% break %}
    {% endif %}
{% endfor %}

