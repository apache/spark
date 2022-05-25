/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.connector.expressions;

import org.apache.spark.annotation.Evolving;

import java.io.Serializable;

/**
 * Represent an extract function, which extracts and returns the value of a
 * and a source expression where the field should be extracted.
 * <p>
 * The currently supported field names:
 * <ol>
 *  <li>Field Name: <code>SECOND</code>
 *   <ul>
 *    <li>SQL semantic: <code>EXTRACT(SECOND FROM source)</code></li>
 *    <li>Since version: 3.4.0</li>
 *   </ul>
 *  </li>
 *  <li>Field Name: <code>MINUTE</code>
 *   <ul>
 *    <li>SQL semantic: <code>EXTRACT(MINUTE FROM source)</code></li>
 *    <li>Since version: 3.4.0</li>
 *   </ul>
 *  </li>
 *  <li>Field Name: <code>HOUR</code>
 *   <ul>
 *    <li>SQL semantic: <code>EXTRACT(HOUR FROM source)</code></li>
 *    <li>Since version: 3.4.0</li>
 *   </ul>
 *  </li>
 *  <li>Field Name: <code>MONTH</code>
 *   <ul>
 *    <li>SQL semantic: <code>EXTRACT(MONTH FROM source)</code></li>
 *    <li>Since version: 3.4.0</li>
 *   </ul>
 *  </li>
 *  <li>Field Name: <code>QUARTER</code>
 *   <ul>
 *    <li>SQL semantic: <code>EXTRACT(QUARTER FROM source)</code></li>
 *    <li>Since version: 3.4.0</li>
 *   </ul>
 *  </li>
 *  <li>Field Name: <code>YEAR</code>
 *   <ul>
 *    <li>SQL semantic: <code>EXTRACT(YEAR FROM source)</code></li>
 *    <li>Since version: 3.4.0</li>
 *   </ul>
 *  </li>
 *  <li>Field Name: <code>ISO_DAY_OF_WEEK</code>
 *   <ul>
 *    <li>SQL semantic: <code>EXTRACT(ISO_DAY_OF_WEEK FROM source)</code></li>
 *    <li>Database dialects need to follow ISO semantics when handling ISO_DAY_OF_WEEK.</li>
 *    <li>Since version: 3.4.0</li>
 *   </ul>
 *  </li>
 *  <li>Field Name: <code>DAY</code>
 *   <ul>
 *    <li>SQL semantic: <code>EXTRACT(DAY FROM source)</code></li>
 *    <li>Since version: 3.4.0</li>
 *   </ul>
 *  </li>
 *  <li>Field Name: <code>DOY</code>
 *   <ul>
 *    <li>SQL semantic: <code>EXTRACT(DOY FROM source)</code></li>
 *    <li>Since version: 3.4.0</li>
 *   </ul>
 *  </li>
 *  <li>Field Name: <code>WEEK</code>
 *   <ul>
 *    <li>SQL semantic: <code>EXTRACT(WEEK FROM source)</code></li>
 *    <li>Database dialects need to follow ISO semantics when handling WEEK.</li>
 *    <li>Since version: 3.4.0</li>
 *   </ul>
 *  </li>
 *  <li>Field Name: <code>YEAR_OF_WEEK</code>
 *   <ul>
 *    <li>SQL semantic: <code>EXTRACT(YEAR_OF_WEEK FROM source)</code></li>
 *    <li>Database dialects need to follow ISO semantics when handling YEAR_OF_WEEK.</li>
 *    <li>Since version: 3.4.0</li>
 *   </ul>
 *  </li>
 * </ol>
 *
 * @since 3.4.0
 */

@Evolving
public class Extract implements Expression, Serializable {

  private String field;
  private Expression source;

  public Extract(String field, Expression source) {
    this.field = field;
    this.source = source;
  }

  public String field() { return field; }
  public Expression source() { return source; }

  @Override
  public Expression[] children() { return new Expression[]{ source() }; }
}
