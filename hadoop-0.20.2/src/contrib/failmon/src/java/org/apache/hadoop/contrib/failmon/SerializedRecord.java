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

package org.apache.hadoop.contrib.failmon;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.text.DateFormat;

/**********************************************************
 * Objects of this class hold the serialized representations
 * of EventRecords. A SerializedRecord is essentially an EventRecord
 * with all its property values converted to strings. It also provides 
 * some convenience methods for printing the property fields in a 
 * more readable way.
 *
 **********************************************************/

public class SerializedRecord {

  HashMap<String, String> fields;
  private static DateFormat dateFormatter =
    DateFormat.getDateTimeInstance(DateFormat.LONG, DateFormat.LONG);;

  /**
   * Create the SerializedRecord given an EventRecord.
   */
  
  public SerializedRecord(EventRecord source) {
    fields = new HashMap<String, String>();
    fields.clear();

    for (String k : source.getMap().keySet()) {
      ArrayList<String> strs = getStrings(source.getMap().get(k));
      if (strs.size() == 1)
        fields.put(k, strs.get(0));
      else
        for (int i = 0; i < strs.size(); i++)
          fields.put(k + "#" + i, strs.get(i));
    }

  }

  /**
   * Extract String representations from an Object.
   * 
   * @param o the input object
   * 
   * @return an ArrayList that contains Strings found in o
   */
  private ArrayList<String> getStrings(Object o) {
    ArrayList<String> retval = new ArrayList<String>();
    retval.clear();
    if (o == null)
      retval.add("null");
    else if (o instanceof String)
      retval.add((String) o);
    else if (o instanceof Calendar)
      retval.add(dateFormatter.format(((Calendar) o).getTime()));
    else if (o instanceof InetAddress[])
      for (InetAddress ip : ((InetAddress[]) o))
        retval.add(ip.getHostAddress());
    else if (o instanceof String[])
      for (String s : (String []) o)
        retval.add(s);
    else
      retval.add(o.toString());

    return retval;
  }

  /**
   * Set the value of a property of the EventRecord.
   * 
   * @param fieldName the name of the property to set
   * @param fieldValue the value of the property to set
   * 
   */
  public void set(String fieldName, String fieldValue) {
    fields.put(fieldName, fieldValue);
  }

  /**
   * Get the value of a property of the EventRecord.
   * If the property with the specific key is not found,
   * null is returned.
   * 
   * @param fieldName the name of the property to get.
   */
  public String get(String fieldName) {
    return fields.get(fieldName);
  }

  /**
   * Arrange the keys to provide a more readable printing order:
   * first goes the timestamp, then the hostname and then the type, followed
   * by all other keys found.
   * 
   * @param keys The input ArrayList of keys to re-arrange.
   */
  public static void arrangeKeys(ArrayList<String> keys) {
    move(keys, "timestamp", 0);
    move(keys, "hostname", 1);
    move(keys, "type", 2);
  }

  private static void move(ArrayList<String> keys, String key, int position) {
    int cur = keys.indexOf(key);
    if (cur == -1)
      return;
    keys.set(cur, keys.get(position));
    keys.set(position, key);
  }

  /**
   * Check if the SerializedRecord is a valid one, i.e., whether
   * it represents meaningful metric values.
   * 
   * @return true if the EventRecord is a valid one, false otherwise.
   */
  public boolean isValid() {
    return !("invalid".equalsIgnoreCase(fields.get("hostname")));
  }

  
  /**
   * Creates and returns a string reperssentation of the object
   * 
   * @return a String representing the object
   */

  public String toString() {
    String retval = "";
    ArrayList<String> keys = new ArrayList<String>(fields.keySet());
    arrangeKeys(keys);

    for (int i = 0; i < keys.size(); i++) {
      String value = fields.get(keys.get(i));
      if (value == null)
        retval += keys.get(i) + ":\tnull\n";
      else
        retval += keys.get(i) + ":\t" + value + "\n";
    }
    return retval;
  }
}
