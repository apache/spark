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

package org.apache.hadoop.tools.rumen;

/**
 * This describes a path from a node to the root. We use it when we compare two
 * trees during rumen unit tests. If the trees are not identical, this chain
 * will be converted to a string which describes the path from the root to the
 * fields that did not compare.
 * 
 */
public class TreePath {
  final TreePath parent;

  final String fieldName;

  final int index;

  public TreePath(TreePath parent, String fieldName) {
    super();

    this.parent = parent;
    this.fieldName = fieldName;
    this.index = -1;
  }

  public TreePath(TreePath parent, String fieldName, int index) {
    super();

    this.parent = parent;
    this.fieldName = fieldName;
    this.index = index;
  }

  @Override
  public String toString() {
    String mySegment = fieldName + (index == -1 ? "" : ("[" + index + "]"));

    return ((parent == null) ? "" : parent.toString() + "-->") + mySegment;
  }
}
