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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.ArrayList;
import java.util.Comparator;

import org.codehaus.jackson.annotate.JsonAnySetter;

/**
 * A {@link LoggedNetworkTopology} represents a tree that in turn represents a
 * hierarchy of hosts. The current version requires the tree to have all leaves
 * at the same level.
 * 
 * All of the public methods are simply accessors for the instance variables we
 * want to write out in the JSON files.
 * 
 */
public class LoggedNetworkTopology implements DeepCompare {
  String name;
  List<LoggedNetworkTopology> children = new ArrayList<LoggedNetworkTopology>();

  static private Set<String> alreadySeenAnySetterAttributes =
      new TreeSet<String>();

  public LoggedNetworkTopology() {
    super();
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

  /**
   * We need this because we have to sort the {@code children} field. That field
   * is set-valued, but if we sort these fields we ensure that comparisons won't
   * bogusly fail because the hash table happened to enumerate in a different
   * order.
   * 
   */
  static class TopoSort implements Comparator<LoggedNetworkTopology> {
    public int compare(LoggedNetworkTopology t1, LoggedNetworkTopology t2) {
      return t1.name.compareTo(t2.name);
    }
  }

  /**
   * @param hosts
   *          a HashSet of the {@link ParsedHost}
   * @param name
   *          the name of this level's host [for recursive descent]
   * @param level
   *          the level number
   */
  LoggedNetworkTopology(HashSet<ParsedHost> hosts, String name, int level) {

    this.name = name;
    this.children = null;

    if (level < ParsedHost.numberOfDistances() - 1) {
      HashMap<String, HashSet<ParsedHost>> topologies =
          new HashMap<String, HashSet<ParsedHost>>();

      Iterator<ParsedHost> iter = hosts.iterator();

      while (iter.hasNext()) {
        ParsedHost host = iter.next();

        String thisComponent = host.nameComponent(level);

        HashSet<ParsedHost> thisSet = topologies.get(thisComponent);

        if (thisSet == null) {
          thisSet = new HashSet<ParsedHost>();
          topologies.put(thisComponent, thisSet);
        }

        thisSet.add(host);
      }

      children = new ArrayList<LoggedNetworkTopology>();

      for (Map.Entry<String, HashSet<ParsedHost>> ent : topologies.entrySet()) {
        children.add(new LoggedNetworkTopology(ent.getValue(), ent.getKey(),
            level + 1));
      }
    } else {
      // nothing to do here
    }
  }

  public String getName() {
    return name;
  }

  void setName(String name) {
    this.name = name;
  }

  public List<LoggedNetworkTopology> getChildren() {
    return children;
  }

  void setChildren(List<LoggedNetworkTopology> children) {
    this.children = children;
  }

  private void compare1(List<LoggedNetworkTopology> c1,
      List<LoggedNetworkTopology> c2, TreePath loc, String eltname)
      throws DeepInequalityException {
    if (c1 == null && c2 == null) {
      return;
    }

    if (c1 == null || c2 == null || c1.size() != c2.size()) {
      throw new DeepInequalityException(eltname + " miscompared", new TreePath(
          loc, eltname));
    }

    Collections.sort(c1, new TopoSort());
    Collections.sort(c2, new TopoSort());

    for (int i = 0; i < c1.size(); ++i) {
      c1.get(i).deepCompare(c2.get(i), new TreePath(loc, eltname, i));
    }
  }

  public void deepCompare(DeepCompare comparand, TreePath loc)
      throws DeepInequalityException {
    if (!(comparand instanceof LoggedNetworkTopology)) {
      throw new DeepInequalityException("comparand has wrong type", loc);
    }

    LoggedNetworkTopology other = (LoggedNetworkTopology) comparand;

    compare1(children, other.children, loc, "children");
  }
}
