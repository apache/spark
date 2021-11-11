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

package org.apache.spark.sql.connector.expressions.filter;

import java.util.Objects;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.expressions.NamedReference;

/**
 * Base class for {@link And}, {@link Or}
 *
 * @since 3.3.0
 */
@Evolving
abstract class BinaryFilter extends Filter {
  protected final Filter left;
  protected final Filter right;

  protected BinaryFilter(Filter left, Filter right) {
    this.left = left;
    this.right = right;
  }

  public Filter left() { return left; }
  public Filter right() { return right; }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    BinaryFilter and = (BinaryFilter) o;
    return Objects.equals(left, and.left) && Objects.equals(right, and.right);
  }

  @Override
  public int hashCode() {
    return Objects.hash(left, right);
  }

  @Override
  public NamedReference[] references() {
    NamedReference[] refLeft = left.references();
    NamedReference[] refRight = right.references();
    NamedReference[] arr = new NamedReference[refLeft.length + refRight.length];
    System.arraycopy(refLeft, 0, arr, 0, refLeft.length);
    System.arraycopy(refRight, 0, arr, refLeft.length, refRight.length);
    return arr;
  }
}
