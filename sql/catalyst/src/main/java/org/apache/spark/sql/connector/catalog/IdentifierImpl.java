/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.connector.catalog;

import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;

import org.apache.spark.annotation.Evolving;

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
    return Stream.concat(Stream.of(namespace), Stream.of(name))
      .map(CatalogV2Implicits::quoteIfNeeded)
      .collect(Collectors.joining("."));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    IdentifierImpl that = (IdentifierImpl) o;
    return Arrays.equals(namespace, that.namespace) && name.equals(that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(Arrays.hashCode(namespace), name);
  }
}
