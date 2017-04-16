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

package org.apache.hadoop.streaming;

import java.io.*;
import java.util.*;
import java.util.Map.Entry;

/**
 * Maps a relative pathname to an absolute pathname using the
 * PATH enviroment.
 */
public class PathFinder
{
  String pathenv;        // a string of pathnames
  String pathSep;        // the path seperator
  String fileSep;        // the file seperator in a directory

  /**
   * Construct a PathFinder object using the path from
   * java.class.path
   */
  public PathFinder()
  {
    pathenv = System.getProperty("java.class.path");
    pathSep = System.getProperty("path.separator");
    fileSep = System.getProperty("file.separator");
  }

  /**
   * Construct a PathFinder object using the path from
   * the specified system environment variable.
   */
  public PathFinder(String envpath)
  {
    pathenv = System.getenv(envpath);
    pathSep = System.getProperty("path.separator");
    fileSep = System.getProperty("file.separator");
  }

  /**
   * Appends the specified component to the path list
   */
  public void prependPathComponent(String str)
  {
    pathenv = str + pathSep + pathenv;
  }

  /**
   * Returns the full path name of this file if it is listed in the
   * path
   */
  public File getAbsolutePath(String filename)
  {
    if (pathenv == null || pathSep == null  || fileSep == null)
      {
        return null;
      }
    int     val = -1;
    String    classvalue = pathenv + pathSep;

    while (((val = classvalue.indexOf(pathSep)) >= 0) &&
           classvalue.length() > 0) {
      //
      // Extract each entry from the pathenv
      //
      String entry = classvalue.substring(0, val).trim();
      File f = new File(entry);

      try {
        if (f.isDirectory()) {
          //
          // this entry in the pathenv is a directory.
          // see if the required file is in this directory
          //
          f = new File(entry + fileSep + filename);
        }
        //
        // see if the filename matches and  we can read it
        //
        if (f.isFile() && f.canRead()) {
          return f;
        }
      } catch (Exception exp){ }
      classvalue = classvalue.substring(val+1).trim();
    }
    return null;
  }

  /**
   * prints all environment variables for this process
   */
  private static void printEnvVariables() {
    System.out.println("Environment Variables: ");
    Map<String,String> map = System.getenv();
    Set<Entry<String, String>> entrySet = map.entrySet();
    for(Entry<String, String> entry : entrySet) {
      System.out.println(entry.getKey() + " = " + entry.getValue());
    }
  }

  /**
   * prints all system properties for this process
   */
  private static void printSystemProperties() {
    System.out.println("System properties: ");
    java.util.Properties p = System.getProperties();
    java.util.Enumeration keys = p.keys();
    while(keys.hasMoreElements()) {
      String thiskey = (String)keys.nextElement();
      String value = p.getProperty(thiskey);
      System.out.println(thiskey + " = " + value);
    }
  }

  public static void main(String args[]) throws IOException {

    if (args.length < 1) {
      System.out.println("Usage: java PathFinder <filename>");
      System.exit(1);
    }

    PathFinder finder = new PathFinder("PATH");
    File file = finder.getAbsolutePath(args[0]);
    if (file != null) {
      System.out.println("Full path name = " + file.getCanonicalPath());
    }
  }
}
