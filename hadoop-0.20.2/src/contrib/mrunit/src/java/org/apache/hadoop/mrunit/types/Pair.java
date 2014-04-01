/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mrunit.types;

import java.util.Comparator;


/**
 * A very basic pair type.
 */
public class Pair<S, T> implements Comparable<Pair<S, T>> {

  private final S first;
  private final T second;

  public Pair(final S car, final T cdr) {
    first = car;
    second = cdr;
  }

  public S getFirst() { return first; }
  public T getSecond() { return second; }

  @Override
  public boolean equals(Object o) {
    if (null == o) {
      return false;
    } else if (o instanceof Pair) {
      Pair<S, T> p = (Pair<S, T>) o;
      if (first == null && second == null) {
        return p.first == null && p.second == null;
      } else if (first == null) {
        return p.first == null && second.equals(p.second);
      } else if (second == null) {
        return p.second == null && first.equals(p.first);
      } else {
        return first.equals(p.first) && second.equals(p.second);
      }
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    int code = 0;

    if (null != first) {
      code += first.hashCode();
    }

    if (null != second) {
      code += second.hashCode() << 1;
    }

    return code;
  }

  @Override
  public int compareTo(Pair<S, T> p) {
    if (null == p) {
      return 1;
    }

    Comparable<S> firstCompare = (Comparable<S>) first;

    int firstResult = firstCompare.compareTo(p.first);
    if (firstResult == 0) {
      Comparable<T> secondCompare = (Comparable<T>) second;
      return secondCompare.compareTo(p.second);
    } else {
      return firstResult;
    }
  }

  // TODO: Can this be made static? Same with SecondElemComparator?
  public class FirstElemComparator implements Comparator<Pair<S, T>> {
    public FirstElemComparator() {
    }

    public int compare(Pair<S, T> p1, Pair<S, T> p2) {
      Comparable<S> cS = (Comparable<S>) p1.first;
      return cS.compareTo(p2.first);
    }
  }

  public class SecondElemComparator implements Comparator<Pair<S, T>> {
    public SecondElemComparator() {
    }

    public int compare(Pair<S, T> p1, Pair<S, T> p2) {
      Comparable<T> cT = (Comparable<T>) p1.second;
      return cT.compareTo(p2.second);
    }
  }

  @Override
  public String toString() {
    String firstString = "null";
    String secondString = "null";

    if (null != first) {
      firstString = first.toString();
    }

    if (null != second) {
      secondString = second.toString();
    }

    return "(" + firstString + ", " + secondString + ")";
  }
}
