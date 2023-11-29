..  Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

..    http://www.apache.org/licenses/LICENSE-2.0

..  Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

{{ fullname }}
{{ underline }}

.. currentmodule:: {{ module + "." + objname.split(".")[0] }}

.. automethod:: {{ ".".join(objname.split(".")[1:]) }}

{% if '__init__' in methods %}
  {% set caught_result = methods.remove('__init__') %}
{% endif %}

{% block methods %}
{% if methods %}

   .. rubric:: Methods

   .. autosummary::
      {% for item in methods %}
         ~{{ name.split(".")[1] }}.{{ item }}
      {%- endfor %}

{% endif %}
{% endblock %}

{% block attributes_summary %}
{% if attributes %}

   .. rubric:: Attributes
   
   .. autosummary::
      {% for item in attributes %}
         ~{{ name.split(".")[1] }}.{{ item }}
      {%- endfor %}

{% endif %}
{% endblock %}
