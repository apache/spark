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

package org.apache.spark.sql.connector.catalog;

import org.apache.arrow.util.Preconditions;

import java.util.Arrays;
import java.util.Objects;
import java.util.StringJoiner;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.catalyst.util.QuotingUtils;

/**
 *  An {@link Identifier} implementation.
 */
@Evolving
class IdentifierImpl implements Identifier {

  private String[] namespace;
  private String name;

  IdentifierImpl(String[] namespace, String name) {
    Preconditions.checkNotNull(namespace, "Identifier namespace cannot be null");
    Preconditions.checkNotNull(name, "Identifier name cannot be null");
    this.namespace = namespace;
    this.name = name;
  }

  @Override
  public String[] namespace() {
    return namespace;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public String toString() {
    StringJoiner joiner = new StringJoiner(".");
    for (String p : namespace) {
      joiner.add(QuotingUtils.quoteIfNeeded(p));
    }
    joiner.add(QuotingUtils.quoteIfNeeded(name));
    return joiner.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof IdentifierImpl that)) return false;
    return Arrays.equals(namespace, that.namespace) && name.equals(that.name);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(name);
    result = 31 * result + Arrays.hashCode(namespace);
    return result;
  }
}
