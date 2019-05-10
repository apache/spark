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

package org.apache.spark.sql.catalog.v2;

import com.google.common.base.Preconditions;
import org.apache.spark.annotation.Experimental;

import java.util.Arrays;
import java.util.Objects;

/**
 *  An {@link Identifier} implementation.
 */
@Experimental
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
