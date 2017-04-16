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

package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.MissingResourceException;
import java.util.ResourceBundle;

import org.apache.commons.logging.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.StringUtils;

/**
 * A set of named counters.
 * 
 * <p><code>Counters</code> represent global counters, defined either by the 
 * Map-Reduce framework or applications. Each <code>Counter</code> can be of
 * any {@link Enum} type.</p>
 * 
 * <p><code>Counters</code> are bunched into {@link Group}s, each comprising of
 * counters from a particular <code>Enum</code> class. 
 */
public class Counters implements Writable, Iterable<Counters.Group> {
  private static final Log LOG = LogFactory.getLog(Counters.class);
  private static final char GROUP_OPEN = '{';
  private static final char GROUP_CLOSE = '}';
  private static final char COUNTER_OPEN = '[';
  private static final char COUNTER_CLOSE = ']';
  private static final char UNIT_OPEN = '(';
  private static final char UNIT_CLOSE = ')';
  private static char[] charsToEscape =  {GROUP_OPEN, GROUP_CLOSE, 
                                          COUNTER_OPEN, COUNTER_CLOSE, 
                                          UNIT_OPEN, UNIT_CLOSE};
  private static final JobConf conf = new JobConf();
  /** limit on the size of the name of the group **/
  private static final int GROUP_NAME_LIMIT = 
    conf.getInt("mapreduce.job.counters.group.name.max", 128);
  /** limit on the size of the counter name **/
  private static final int COUNTER_NAME_LIMIT = 
    conf.getInt("mapreduce.job.counters.counter.name.max", 64);
  
  /** limit on counters **/
  public static int MAX_COUNTER_LIMIT = 
    conf.getInt("mapreduce.job.counters.limit", // deprecated in 0.23
        conf.getInt("mapreduce.job.counters.max", 120));

  /** the max groups allowed **/
  public static final int MAX_GROUP_LIMIT = 
    conf.getInt("mapreduce.job.counters.groups.max", 50);
  
  /** the number of current counters**/
  private int numCounters = 0;

  //private static Log log = LogFactory.getLog("Counters.class");
  
  /**
   * A counter record, comprising its name and value. 
   */
  public static class Counter extends org.apache.hadoop.mapreduce.Counter {
    
    Counter() { 
    }

    Counter(String name, String displayName, long value) {
      super(name, displayName);
      increment(value);
    }
    
    public void setDisplayName(String newName) {
      super.setDisplayName(newName);
    }
    
    /**
     * Returns the compact stringified version of the counter in the format
     * [(actual-name)(display-name)(value)]
     */
    public synchronized String makeEscapedCompactString() {

      // First up, obtain the strings that need escaping. This will help us
      // determine the buffer length apriori.
      String escapedName = escape(getName());
      String escapedDispName = escape(getDisplayName());
      long currentValue = this.getValue();
      int length = escapedName.length() + escapedDispName.length() + 4;

      length += 8; // For the following delimiting characters
      StringBuilder builder = new StringBuilder(length);
      builder.append(COUNTER_OPEN);
      
      // Add the counter name
      builder.append(UNIT_OPEN);
      builder.append(escapedName);
      builder.append(UNIT_CLOSE);
      
      // Add the display name
      builder.append(UNIT_OPEN);
      builder.append(escapedDispName);
      builder.append(UNIT_CLOSE);
      
      // Add the value
      builder.append(UNIT_OPEN);
      builder.append(currentValue);
      builder.append(UNIT_CLOSE);
      
      builder.append(COUNTER_CLOSE);
      
      return builder.toString();
    }
    
    // Checks for (content) equality of two (basic) counters
    synchronized boolean contentEquals(Counter c) {
      return this.equals(c);
    }
    
    /**
     * What is the current value of this counter?
     * @return the current value
     */
    public synchronized long getCounter() {
      return getValue();
    }
    
  }
  
  /**
   *  <code>Group</code> of counters, comprising of counters from a particular 
   *  counter {@link Enum} class.  
   *
   *  <p><code>Group</code>handles localization of the class name and the 
   *  counter names.</p>
   */
  public class Group implements Writable, Iterable<Counter> {
    private String groupName;
    private String displayName;
    private Map<String, Counter> subcounters = new HashMap<String, Counter>();
    
    // Optional ResourceBundle for localization of group and counter names.
    private ResourceBundle bundle = null;    
    
    Group(String groupName) {
      try {
        bundle = getResourceBundle(groupName);
      }
      catch (MissingResourceException neverMind) {
      }
      this.groupName = groupName;
      this.displayName = localize("CounterGroupName", groupName);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Creating group " + groupName + " with " +
               (bundle == null ? "nothing" : "bundle"));
      }
    }
        
    /**
     * Returns raw name of the group.  This is the name of the enum class
     * for this group of counters.
     */
    public String getName() {
      return groupName;
    }
    
    /**
     * Returns localized name of the group.  This is the same as getName() by
     * default, but different if an appropriate ResourceBundle is found.
     */
    public String getDisplayName() {
      return displayName;
    }
    
    /**
     * Set the display name
     */
    public void setDisplayName(String displayName) {
      this.displayName = displayName;
    }
    
    /**
     * Returns the compact stringified version of the group in the format
     * {(actual-name)(display-name)(value)[][][]} where [] are compact strings for the
     * counters within.
     */
    public String makeEscapedCompactString() {
      String[] subcountersArray = new String[subcounters.size()];

      // First up, obtain the strings that need escaping. This will help us
      // determine the buffer length apriori.
      String escapedName = escape(getName());
      String escapedDispName = escape(getDisplayName());
      int i = 0;
      int length = escapedName.length() + escapedDispName.length();
      for (Counter counter : subcounters.values()) {
        String escapedStr = counter.makeEscapedCompactString();
        subcountersArray[i++] = escapedStr;
        length += escapedStr.length();
      }

      length += 6; // for all the delimiting characters below
      StringBuilder builder = new StringBuilder(length);
      builder.append(GROUP_OPEN); // group start
      
      // Add the group name
      builder.append(UNIT_OPEN);
      builder.append(escapedName);
      builder.append(UNIT_CLOSE);
      
      // Add the display name
      builder.append(UNIT_OPEN);
      builder.append(escapedDispName);
      builder.append(UNIT_CLOSE);
      
      // write the value
      for(String str : subcountersArray) {
        builder.append(str);
      }
      
      builder.append(GROUP_CLOSE); // group end
      return builder.toString();
    }

    @Override
    public int hashCode() {
      return subcounters.hashCode();
    }

    /** 
     * Checks for (content) equality of Groups
     */
    @Override
    public synchronized boolean equals(Object obj) {
      boolean isEqual = false;
      if (obj != null && obj instanceof Group) {
        Group g = (Group) obj;
        if (size() == g.size()) {
          isEqual = true;
          for (Map.Entry<String, Counter> entry : subcounters.entrySet()) {
            String key = entry.getKey();
            Counter c1 = entry.getValue();
            Counter c2 = g.getCounterForName(key);
            if (!c1.contentEquals(c2)) {
              isEqual = false;
              break;
            }
          }
        }
      }
      return isEqual;
    }
    
    /**
     * Returns the value of the specified counter, or 0 if the counter does
     * not exist.
     */
    public synchronized long getCounter(String counterName) {
      for(Counter counter: subcounters.values()) {
        if (counter != null && counter.getDisplayName().equals(counterName)) {
          return counter.getValue();
        }
      }
      return 0L;
    }
    
    /**
     * Get the counter for the given id and create it if it doesn't exist.
     * @param id the numeric id of the counter within the group
     * @param name the internal counter name
     * @return the counter
     * @deprecated use {@link #getCounter(String)} instead
     */
    @Deprecated
    public synchronized Counter getCounter(int id, String name) {
      return getCounterForName(name);
    }
    
    /**
     * Get the counter for the given name and create it if it doesn't exist.
     * @param name the internal counter name
     * @return the counter
     */
    public synchronized Counter getCounterForName(String name) {
      String shortName = getShortName(name, COUNTER_NAME_LIMIT);
      Counter result = subcounters.get(shortName);
      if (result == null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Adding " + shortName);
        }
        numCounters = (numCounters == 0) ? Counters.this.size(): numCounters; 
        if (numCounters >= MAX_COUNTER_LIMIT) {
          throw new CountersExceededException("Error: Exceeded limits on number of counters - " 
              + "Counters=" + numCounters + " Limit=" + MAX_COUNTER_LIMIT);
        }
        result = new Counter(shortName, localize(shortName + ".name", shortName), 0L);
        subcounters.put(shortName, result);
        numCounters++;
      }
      return result;
    }
    
    /**
     * Returns the number of counters in this group.
     */
    public synchronized int size() {
      return subcounters.size();
    }
    
    /**
     * Looks up key in the ResourceBundle and returns the corresponding value.
     * If the bundle or the key doesn't exist, returns the default value.
     */
    private String localize(String key, String defaultValue) {
      String result = defaultValue;
      if (bundle != null) {
        try {
          result = bundle.getString(key);
        }
        catch (MissingResourceException mre) {
        }
      }
      return result;
    }
    
    public synchronized void write(DataOutput out) throws IOException {
      Text.writeString(out, displayName);
      WritableUtils.writeVInt(out, subcounters.size());
      for(Counter counter: subcounters.values()) {
        counter.write(out);
      }
    }
    
    public synchronized void readFields(DataInput in) throws IOException {
      displayName = Text.readString(in);
      subcounters.clear();
      int size = WritableUtils.readVInt(in);
      for(int i=0; i < size; i++) {
        Counter counter = new Counter();
        counter.readFields(in);
        subcounters.put(counter.getName(), counter);
      }
    }

    public synchronized Iterator<Counter> iterator() {
      return new ArrayList<Counter>(subcounters.values()).iterator();
    }
  }
  
  // Map from group name (enum class name) to map of int (enum ordinal) to
  // counter record (name-value pair).
  private Map<String,Group> counters = new HashMap<String, Group>();

  /**
   * A cache from enum values to the associated counter. Dramatically speeds up
   * typical usage.
   */
  private Map<Enum, Counter> cache = new IdentityHashMap<Enum, Counter>();

  /**
   * Returns the specified resource bundle, or throws an exception.
   * @throws MissingResourceException if the bundle isn't found
   */
  private static ResourceBundle getResourceBundle(String enumClassName) {
    String bundleName = enumClassName.replace('$','_');
    return ResourceBundle.getBundle(bundleName);
  }

  /**
   * Returns the names of all counter classes.
   * @return Set of counter names.
   */
  public synchronized Collection<String> getGroupNames() {
    return counters.keySet();
  }

  public synchronized Iterator<Group> iterator() {
    return counters.values().iterator();
  }

  /**
   * Returns the named counter group, or an empty group if there is none
   * with the specified name.
   */
  public synchronized Group getGroup(String groupName) {
    String shortGroupName = getShortName(groupName, GROUP_NAME_LIMIT);
    Group result = counters.get(shortGroupName);
    if (result == null) {
      /** check if we have exceeded the max number on groups **/
      if (counters.size() > MAX_GROUP_LIMIT) {
        throw new RuntimeException(
            "Error: Exceeded limits on number of groups in counters - " +
            "Groups=" + counters.size() +" Limit=" + MAX_GROUP_LIMIT);
      }
      result = new Group(shortGroupName);
      counters.put(shortGroupName, result);
    }
    return result;
  } 

  /**
   * Find the counter for the given enum. The same enum will always return the
   * same counter.
   * @param key the counter key
   * @return the matching counter object
   */
  public synchronized Counter findCounter(Enum key) {
    Counter counter = cache.get(key);
    if (counter == null) {
      Group group = getGroup(key.getDeclaringClass().getName());
      if (group != null) {
        counter = group.getCounterForName(key.toString());
        if (counter != null)  cache.put(key, counter);
      }
    }
    return counter;    
  }

  /**
   * Find a counter given the group and the name.
   * @param group the name of the group
   * @param name the internal name of the counter
   * @return the counter for that name
   */
  public synchronized Counter findCounter(String group, String name) {
    Group retGroup = getGroup(group);
    return (retGroup == null) ? null: retGroup.getCounterForName(name);
  }

  /** 
   * Find a counter by using strings
   * @param group the name of the group
   * @param id the id of the counter within the group (0 to N-1)
   * @param name the internal name of the counter
   * @return the counter for that name
   * @deprecated
   */
  @Deprecated
  public synchronized Counter findCounter(String group, int id, String name) {
    Group retGroup = getGroup(group);
    return (retGroup == null) ? null: retGroup.getCounterForName(name);
  }

  /**
   * Increments the specified counter by the specified amount, creating it if
   * it didn't already exist.
   * @param key identifies a counter
   * @param amount amount by which counter is to be incremented
   */
  public synchronized void incrCounter(Enum key, long amount) {
    findCounter(key).increment(amount);
  }
  
  /**
   * Increments the specified counter by the specified amount, creating it if
   * it didn't already exist.
   * @param group the name of the group
   * @param counter the internal name of the counter
   * @param amount amount by which counter is to be incremented
   */
  public synchronized void incrCounter(String group, String counter, long amount) {
    Group retGroup = getGroup(group);
    if (retGroup != null) {
      Counter retCounter = retGroup.getCounterForName(counter);
      if (retCounter != null ) {
        retCounter.increment(amount);
      }
    }
  }
  
  /**
   * Returns current value of the specified counter, or 0 if the counter
   * does not exist.
   */
  public synchronized long getCounter(Enum key) {
    Counter retCounter = findCounter(key);
    return (retCounter == null) ? 0 : retCounter.getValue();
  }
  
  /**
   * Increments multiple counters by their amounts in another Counters 
   * instance.
   * @param other the other Counters instance
   */
  public synchronized void incrAllCounters(Counters other) {
    for (Group otherGroup: other) {
      Group group = getGroup(otherGroup.getName());
      if (group == null) {
        continue;
      }
      group.displayName = otherGroup.displayName;
      for (Counter otherCounter : otherGroup) {
        Counter counter = group.getCounterForName(otherCounter.getName());
        if (counter == null) {
          continue;
        }
        counter.setDisplayName(otherCounter.getDisplayName());
        counter.increment(otherCounter.getValue());
      }
    }
  }

  /**
   * Convenience method for computing the sum of two sets of counters.
   */
  public static Counters sum(Counters a, Counters b) {
    Counters counters = new Counters();
    counters.incrAllCounters(a);
    counters.incrAllCounters(b);
    return counters;
  }
  
  /**
   * Returns the total number of counters, by summing the number of counters
   * in each group.
   */
  public synchronized  int size() {
    int result = 0;
    for (Group group : this) {
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
  public synchronized void write(DataOutput out) throws IOException {
    out.writeInt(counters.size());
    for (Group group: counters.values()) {
      Text.writeString(out, group.getName());
      group.write(out);
    }
  }
  
  /**
   * Read a set of groups.
   */
  public synchronized void readFields(DataInput in) throws IOException {
    int numClasses = in.readInt();
    counters.clear();
    while (numClasses-- > 0) {
      String groupName = Text.readString(in);
      Group group = new Group(groupName);
      group.readFields(in);
      counters.put(groupName, group);
    }
  }
  
  /**
   * Logs the current counter values.
   * @param log The log to use.
   */
  public void log(Log log) {
    log.info("Counters: " + size());
    for(Group group: this) {
      log.info("  " + group.getDisplayName());
      for (Counter counter: group) {
        log.info("    " + counter.getDisplayName() + "=" + 
                 counter.getCounter());
      }   
    }
  }
  
  /**
   * Return textual representation of the counter values.
   */
  public synchronized String toString() {
    StringBuilder sb = new StringBuilder("Counters: " + size());
    for (Group group: this) {
      sb.append("\n\t" + group.getDisplayName());
      for (Counter counter: group) {
        sb.append("\n\t\t" + counter.getDisplayName() + "=" + 
                  counter.getCounter());
      }
    }
    return sb.toString();
  }

  /**
   * Convert a counters object into a single line that is easy to parse.
   * @return the string with "name=value" for each counter and separated by ","
   */
  public synchronized String makeCompactString() {
    StringBuffer buffer = new StringBuffer();
    boolean first = true;
    for(Group group: this){   
      for(Counter counter: group) {
        if (first) {
          first = false;
        } else {
          buffer.append(',');
        }
        buffer.append(group.getDisplayName());
        buffer.append('.');
        buffer.append(counter.getDisplayName());
        buffer.append(':');
        buffer.append(counter.getCounter());
      }
    }
    return buffer.toString();
  }
  
  /**
   * Represent the counter in a textual format that can be converted back to 
   * its object form
   * @return the string in the following format
   * {(groupname)(group-displayname)[(countername)(displayname)(value)][][]}{}{}
   */
  public synchronized String makeEscapedCompactString() {
    String[] groupsArray = new String[counters.size()];
    int i = 0;
    int length = 0;

    // First up, obtain the escaped string for each group so that we can
    // determine the buffer length apriori.
    for (Group group : this) {
      String escapedString = group.makeEscapedCompactString();
      groupsArray[i++] = escapedString;
      length += escapedString.length();
    }

    // Now construct the buffer
    StringBuilder builder = new StringBuilder(length);
    for (String group : groupsArray) {
      builder.append(group);
    }
    return builder.toString();
  }
  
  /**
   * return the short name of a counter/group name
   * truncates from beginning.
   * @param name the name of a group or counter
   * @param limit the limit of characters
   * @return the short name
   */
  static String getShortName(String name, int limit) {
    return (name.length() > limit ?
          name.substring(name.length() - limit, name.length()): name);
  }

  // Extracts a block (data enclosed within delimeters) ignoring escape 
  // sequences. Throws ParseException if an incomplete block is found else 
  // returns null.
  private static String getBlock(String str, char open, char close, 
                                IntWritable index) throws ParseException {
    StringBuilder split = new StringBuilder();
    int next = StringUtils.findNext(str, open, StringUtils.ESCAPE_CHAR, 
                                    index.get(), split);
    split.setLength(0); // clear the buffer
    if (next >= 0) {
      ++next; // move over '('
      
      next = StringUtils.findNext(str, close, StringUtils.ESCAPE_CHAR, 
                                   next, split);
      if (next >= 0) {
        ++next; // move over ')'
        index.set(next);
        return split.toString(); // found a block
      } else {
        throw new ParseException("Unexpected end of block", next);
      }
    }
    return null; // found nothing
  }
  
  /**
   * Convert a stringified counter representation into a counter object. Note 
   * that the counter can be recovered if its stringified using 
   * {@link #makeEscapedCompactString()}. 
   * @return a Counter
   */
  public static Counters fromEscapedCompactString(String compactString) 
  throws ParseException {
    Counters counters = new Counters();
    IntWritable index = new IntWritable(0);
    
    // Get the group to work on
    String groupString = 
      getBlock(compactString, GROUP_OPEN, GROUP_CLOSE, index);
    
    while (groupString != null) {
      IntWritable groupIndex = new IntWritable(0);
      
      // Get the actual name
      String groupName = 
        getBlock(groupString, UNIT_OPEN, UNIT_CLOSE, groupIndex);
      groupName = unescape(groupName);
      
      // Get the display name
      String groupDisplayName = 
        getBlock(groupString, UNIT_OPEN, UNIT_CLOSE, groupIndex);
      groupDisplayName = unescape(groupDisplayName);
      
      // Get the counters
      Group group = counters.getGroup(groupName);
      group.setDisplayName(groupDisplayName);
      
      String counterString = 
        getBlock(groupString, COUNTER_OPEN, COUNTER_CLOSE, groupIndex);
      
      while (counterString != null) {
        IntWritable counterIndex = new IntWritable(0);
        
        // Get the actual name
        String counterName = 
          getBlock(counterString, UNIT_OPEN, UNIT_CLOSE, counterIndex);
        counterName = unescape(counterName);
        
        // Get the display name
        String counterDisplayName = 
          getBlock(counterString, UNIT_OPEN, UNIT_CLOSE, counterIndex);
        counterDisplayName = unescape(counterDisplayName);
        
        // Get the value
        long value = 
          Long.parseLong(getBlock(counterString, UNIT_OPEN, UNIT_CLOSE, 
                                  counterIndex));
        
        // Add the counter
        Counter counter = group.getCounterForName(counterName);
        counter.setDisplayName(counterDisplayName);
        counter.increment(value);
        
        // Get the next counter
        counterString = 
          getBlock(groupString, COUNTER_OPEN, COUNTER_CLOSE, groupIndex);
      }
      
      groupString = getBlock(compactString, GROUP_OPEN, GROUP_CLOSE, index);
    }
    return counters;
  }

  // Escapes all the delimiters for counters i.e {,[,(,),],}
  private static String escape(String string) {
    return StringUtils.escapeString(string, StringUtils.ESCAPE_CHAR, 
                                    charsToEscape);
  }
  
  // Unescapes all the delimiters for counters i.e {,[,(,),],}
  private static String unescape(String string) {
    return StringUtils.unEscapeString(string, StringUtils.ESCAPE_CHAR, 
                                      charsToEscape);
  }

  @Override 
  public synchronized int hashCode() {
    return counters.hashCode();
  }

  @Override
  public synchronized boolean equals(Object obj) {
    boolean isEqual = false;
    if (obj != null && obj instanceof Counters) {
      Counters other = (Counters) obj;
      if (size() == other.size()) {
        isEqual = true;
        for (Map.Entry<String, Group> entry : this.counters.entrySet()) {
          String key = entry.getKey();
          Group sourceGroup = entry.getValue();
          Group targetGroup = other.getGroup(key);
          if (!sourceGroup.equals(targetGroup)) {
            isEqual = false;
            break;
          }
        }
      }
    }
    return isEqual;
  }
  
  /**
   * Counter exception thrown when the number of counters exceed 
   * the limit
   */
  public static class CountersExceededException extends RuntimeException {
  
    private static final long serialVersionUID = 1L;

    public CountersExceededException(String msg) {
      super(msg);
    }
  }
}
