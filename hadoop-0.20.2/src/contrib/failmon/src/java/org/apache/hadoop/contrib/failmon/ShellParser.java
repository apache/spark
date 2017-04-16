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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**********************************************************
 * Objects of this class parse the output of system command-line
 * utilities that can give information about the state of  
 * various hardware components in the system. Typically, each such
 * object either invokes a command and reads its output or reads the 
 * output of one such command from a file on the disk. Currently 
 * supported utilities include ifconfig, smartmontools, lm-sensors,
 * /proc/cpuinfo.
 *
 **********************************************************/

public abstract class ShellParser implements Monitored {

  /**
   * Find the first occurence ofa pattern in a piece of text 
   * and return a specific group.
   * 
   *  @param strPattern the regular expression to match
   *  @param text the text to search
   *  @param grp the number of the matching group to return
   *  
   *  @return a String containing the matched group of the regular expression
   */
  protected String findPattern(String strPattern, String text, int grp) {

    Pattern pattern = Pattern.compile(strPattern, Pattern.MULTILINE);
    Matcher matcher = pattern.matcher(text);

    if (matcher.find(0))
      return matcher.group(grp);

    return null;
  }

  /**
   * Finds all occurences of a pattern in a piece of text and returns 
   * the matching groups.
   * 
   *  @param strPattern the regular expression to match
   *  @param text the text to search
   *  @param grp the number of the matching group to return
   *  @param separator the string that separates occurences in the returned value
   *  
   *  @return a String that contains all occurences of strPattern in text, 
   *  separated by separator
   */
  protected String findAll(String strPattern, String text, int grp,
      String separator) {

    String retval = "";
    boolean firstTime = true;

    Pattern pattern = Pattern.compile(strPattern);
    Matcher matcher = pattern.matcher(text);

    while (matcher.find()) {
      retval += (firstTime ? "" : separator) + matcher.group(grp);
      firstTime = false;
    }

    return retval;
  }

  /**
   * Insert all EventRecords that can be extracted for
   * the represented hardware component into a LocalStore.
   * 
   * @param ls the LocalStore into which the EventRecords 
   * are to be stored.
   */
  public void monitor(LocalStore ls) {
    ls.insert(monitor());
  }

  abstract public EventRecord[] monitor();

  abstract public EventRecord query(String s) throws Exception;

}
