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

package org.apache.spark.sql.connector.join;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.NamedReference;

/**
 * Represents a column reference used in DSv2 Join pushdown.
 *
 * @since 4.0.0
 */
@Evolving
public final class JoinColumn implements NamedReference {
  public JoinColumn(String[] qualifier, String name, Boolean isInLeftSideOfJoin) {
    this.qualifier = qualifier;
    this.name = name;
    this.isInLeftSideOfJoin = isInLeftSideOfJoin;
  }

  public String[] qualifier;
  public String name;
  public Boolean isInLeftSideOfJoin;

  @Override
  public String[] fieldNames() {
    String[] fullyQualified = new String[qualifier.length + 1];
    System.arraycopy(qualifier, 0, fullyQualified, 0, qualifier.length);
    fullyQualified[qualifier.length] = name;
    return qualifier;
  }

  @Override
  public Expression[] children() { return EMPTY_EXPRESSION; }
}
