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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.codehaus.jackson.annotate.JsonAnySetter;

/**
 * A {@link LoggedLocation} is a representation of a point in an hierarchical
 * network, represented as a series of membership names, broadest first.
 * 
 * For example, if your network has <i>hosts</i> grouped into <i>racks</i>, then
 * in onecluster you might have a node {@code node1} on rack {@code rack1}. This
 * would be represented with a ArrayList of two layers, with two {@link String}
 * s being {@code "rack1"} and {@code "node1"}.
 * 
 * The details of this class are set up to meet the requirements of the Jackson
 * JSON parser/generator.
 * 
 * All of the public methods are simply accessors for the instance variables we
 * want to write out in the JSON files.
 * 
 */
public class LoggedLocation implements DeepCompare {
   static final Map<List<String>, List<String>> layersCache = 
    new HashMap<List<String>, List<String>>();

  /**
   * The full path from the root of the network to the host.
   * 
   * NOTE that this assumes that the network topology is a tree.
   */
  List<String> layers = Collections.emptyList();

  static private Set<String> alreadySeenAnySetterAttributes =
      new TreeSet<String>();

  public List<String> getLayers() {
    return layers;
  }

  void setLayers(List<String> layers) {
    if (layers == null || layers.isEmpty()) {
      this.layers = Collections.emptyList();
    } else {
      synchronized (layersCache) {
        List<String> found = layersCache.get(layers);
        if (found == null) {
          // make a copy with interned string.
          List<String> clone = new ArrayList<String>(layers.size());
          for (String s : layers) {
            clone.add(s.intern());
          }
          // making it read-only as we are sharing them.
          List<String> readonlyLayers = Collections.unmodifiableList(clone);
          layersCache.put(readonlyLayers, readonlyLayers);
          this.layers = readonlyLayers;
        } else {
          this.layers = found;
        }
      }
    }
  }

  @SuppressWarnings("unused")
  // for input parameter ignored.
  @JsonAnySetter
  public void setUnknownAttribute(String attributeName, Object ignored) {
    if (!alreadySeenAnySetterAttributes.contains(attributeName)) {
      alreadySeenAnySetterAttributes.add(attributeName);
      System.err.println("In LoggedJob, we saw the unknown attribute "
          + attributeName + ".");
    }
  }

  // I'll treat this as an atomic object type
  private void compareStrings(List<String> c1, List<String> c2, TreePath loc,
      String eltname) throws DeepInequalityException {
    if (c1 == null && c2 == null) {
      return;
    }

    TreePath recursePath = new TreePath(loc, eltname);

    if (c1 == null || c2 == null || !c1.equals(c2)) {
      throw new DeepInequalityException(eltname + " miscompared", recursePath);
    }
  }

  public void deepCompare(DeepCompare comparand, TreePath loc)
      throws DeepInequalityException {
    if (!(comparand instanceof LoggedLocation)) {
      throw new DeepInequalityException("comparand has wrong type", loc);
    }

    LoggedLocation other = (LoggedLocation) comparand;

    compareStrings(layers, other.layers, loc, "layers");

  }
}
