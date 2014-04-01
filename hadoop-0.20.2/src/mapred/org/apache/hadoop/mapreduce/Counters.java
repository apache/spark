package org.apache.hadoop.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class Counters implements Writable,Iterable<CounterGroup> {
  /**
   * A cache from enum values to the associated counter. Dramatically speeds up
   * typical usage.
   */
  private Map<Enum<?>, Counter> cache = new IdentityHashMap<Enum<?>, Counter>();

  private TreeMap<String, CounterGroup> groups = 
      new TreeMap<String, CounterGroup>();
  
  public Counters() {
  }
  
  Counters(org.apache.hadoop.mapred.Counters counters) {
    for(org.apache.hadoop.mapred.Counters.Group group: counters) {
      String name = group.getName();
      CounterGroup newGroup = new CounterGroup(name, group.getDisplayName());
      groups.put(name, newGroup);
      for(Counter counter: group) {
        newGroup.addCounter(counter);
      }
    }
  }

  public Counter findCounter(String groupName, String counterName) {
    CounterGroup grp = getGroup(groupName);
    return grp.findCounter(counterName);
  }

  /**
   * Find the counter for the given enum. The same enum will always return the
   * same counter.
   * @param key the counter key
   * @return the matching counter object
   */
  public synchronized Counter findCounter(Enum<?> key) {
    Counter counter = cache.get(key);
    if (counter == null) {
      counter = findCounter(key.getDeclaringClass().getName(), key.toString());
      cache.put(key, counter);
    }
    return counter;    
  }

  /**
   * Returns the names of all counter classes.
   * @return Set of counter names.
   */
  public synchronized Collection<String> getGroupNames() {
    return groups.keySet();
  }

  @Override
  public Iterator<CounterGroup> iterator() {
    return groups.values().iterator();
  }

  /**
   * Returns the named counter group, or an empty group if there is none
   * with the specified name.
   */
  public synchronized CounterGroup getGroup(String groupName) {
    CounterGroup grp = groups.get(groupName);
    if (grp == null) {
      grp = new CounterGroup(groupName);
      groups.put(groupName, grp);
    }
    return grp;
  }

  /**
   * Returns the total number of counters, by summing the number of counters
   * in each group.
   */
  public synchronized  int countCounters() {
    int result = 0;
    for (CounterGroup group : this) {
      result += group.size();
    }
    return result;
  }

  /**
   * Write the set of groups.
   * The external format is:
   *     #groups (groupName group)*
   *
   * i.e. the number of groups followed by 0 or more groups, where each 
   * group is of the form:
   *
   *     groupDisplayName #counters (false | true counter)*
   *
   * where each counter is of the form:
   *
   *     name (false | true displayName) value
   */
  @Override
  public synchronized void write(DataOutput out) throws IOException {
    out.writeInt(groups.size());
    for (org.apache.hadoop.mapreduce.CounterGroup group: groups.values()) {
      Text.writeString(out, group.getName());
      group.write(out);
    }
  }
  
  /**
   * Read a set of groups.
   */
  @Override
  public synchronized void readFields(DataInput in) throws IOException {
    int numClasses = in.readInt();
    groups.clear();
    while (numClasses-- > 0) {
      String groupName = Text.readString(in);
      CounterGroup group = new CounterGroup(groupName);
      group.readFields(in);
      groups.put(groupName, group);
    }
  }

  /**
   * Return textual representation of the counter values.
   */
  public synchronized String toString() {
    StringBuilder sb = new StringBuilder("Counters: " + countCounters());
    for (CounterGroup group: this) {
      sb.append("\n\t" + group.getDisplayName());
      for (Counter counter: group) {
        sb.append("\n\t\t" + counter.getDisplayName() + "=" + 
                  counter.getValue());
      }
    }
    return sb.toString();
  }

  /**
   * Increments multiple counters by their amounts in another Counters 
   * instance.
   * @param other the other Counters instance
   */
  public synchronized void incrAllCounters(Counters other) {
    for(Map.Entry<String, CounterGroup> rightEntry: other.groups.entrySet()) {
      CounterGroup left = groups.get(rightEntry.getKey());
      CounterGroup right = rightEntry.getValue();
      if (left == null) {
        left = new CounterGroup(right.getName(), right.getDisplayName());
        groups.put(rightEntry.getKey(), left);
      }
      left.incrAllCounters(right);
    }
  }

  public boolean equals(Object genericRight) {
    if (genericRight instanceof Counters) {
      Iterator<CounterGroup> right = ((Counters) genericRight).groups.
                                       values().iterator();
      Iterator<CounterGroup> left = groups.values().iterator();
      while (left.hasNext()) {
        if (!right.hasNext() || !left.next().equals(right.next())) {
          return false;
        }
      }
      return !right.hasNext();
    }
    return false;
  }
  
  public int hashCode() {
    return groups.hashCode();
  }
}
