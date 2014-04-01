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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.*;
import org.apache.log4j.PropertyConfigurator;

/**********************************************************
 * This class provides various methods for interaction with
 * the configuration and the operating system environment. Also
 * provides some helper methods for use by other classes in
 * the package.
 **********************************************************/

public class Environment {

  public static final int DEFAULT_LOG_INTERVAL = 3600;

  public static final int DEFAULT_POLL_INTERVAL = 360;

  public static int MIN_INTERVAL = 5;

  public static final int MAX_OUTPUT_LENGTH = 51200;

  public static Log LOG;
  
  static Properties fmProperties = new Properties();

  static boolean superuser = false;

  static boolean ready = false;

  /**
   * Initializes structures needed by other methods. Also determines
   * whether the executing user has superuser privileges. 
   *  
   */
  public static void prepare(String fname) {

    if (!"Linux".equalsIgnoreCase(System.getProperty("os.name"))) {
      System.err.println("Linux system required for FailMon. Exiting...");
      System.exit(0);
    }

    System.setProperty("log4j.configuration", "conf/log4j.properties");
    PropertyConfigurator.configure("conf/log4j.properties");
    LOG = LogFactory.getLog("org.apache.hadoop.contrib.failmon");
    logInfo("********** FailMon started ***********");

    // read parseState file
    PersistentState.readState("conf/parsing.state");
    
    try {
      FileInputStream propFile = new FileInputStream(fname);
      fmProperties.load(propFile);
      propFile.close();
    } catch (FileNotFoundException e1) {
      e1.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
    ready = true;

    try {
      String sudo_prompt = "passwd_needed:";
      String echo_txt = "access_ok";
      
      Process p = Runtime.getRuntime().exec("sudo -S -p " + sudo_prompt + " echo " + echo_txt );
      InputStream inps = p.getInputStream();
      InputStream errs = p.getErrorStream();
      
      while (inps.available() < echo_txt.length() && errs.available() < sudo_prompt.length())
	Thread.sleep(100);

      byte [] buf;
      String s;
      
      if (inps.available() >= echo_txt.length()) {
        buf = new byte[inps.available()];
        inps.read(buf);
        s = new String(buf);
        if (s.startsWith(echo_txt)) {
          superuser = true;
	  logInfo("Superuser privileges found!");
	} else {
	  // no need to read errs
	  superuser = false;
	  logInfo("Superuser privileges not found.");
	}
      }
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  /**
   * Fetches the value of a property from the configuration file.
   * 
   *  @param key the name of the property
   *  
   *  @return the value of the property, if it exists and
   *  null otherwise
   */
  public static String getProperty(String key) {
    if (!ready)
      prepare("conf/failmon.properties");
    return fmProperties.getProperty(key);
  }

  /**
   * Sets the value of a property inthe configuration file.
   * 
   *  @param key the name of the property
   *  @param value the new value for the property
   *  
   */
  
  public static void setProperty(String key, String value) {
    fmProperties.setProperty(key, value);
  }

  /**
   * Scans the configuration file to determine which monitoring
   * utilities are available in the system. For each one of them, a
   * job is created. All such jobs are scheduled and executed by
   * Executor.
   * 
   * @return an ArrayList that contains jobs to be executed by theExecutor. 
   */
  public static ArrayList<MonitorJob> getJobs() {

    ArrayList<MonitorJob> monitors = new ArrayList<MonitorJob>();
    int timeInt = 0;

    // for Hadoop Log parsing
    String [] fnames_r = getProperty("log.hadoop.filenames").split(",\\s*");
    String tmp = getProperty("log.hadoop.enabled");

    String [] fnames = expandDirs(fnames_r, ".*(.log).*");

    timeInt = setValue("log.hadoop.interval", DEFAULT_LOG_INTERVAL);
    
    if ("true".equalsIgnoreCase(tmp) && fnames[0] != null)
      for (String fname : fnames) {
        File f = new File(fname);
        if (f.exists() && f.canRead()) {
          monitors.add(new MonitorJob(new HadoopLogParser(fname), "hadoopLog", timeInt));
	  logInfo("Created Monitor for Hadoop log file: " + f.getAbsolutePath());
	} else if (!f.exists())
	  logInfo("Skipping Hadoop log file " + fname + " (file not found)");
	else
	  logInfo("Skipping Hadoop log file " + fname + " (permission denied)");
    }
    
    
    // for System Log parsing
    fnames_r = getProperty("log.system.filenames").split(",\\s*");
    tmp = getProperty("log.system.enabled");

    fnames = expandDirs(fnames_r, ".*(messages).*");

    timeInt = setValue("log.system.interval", DEFAULT_LOG_INTERVAL);
    
    if ("true".equalsIgnoreCase(tmp))
      for (String fname : fnames) {
        File f = new File(fname);
        if (f.exists() && f.canRead()) {
          monitors.add(new MonitorJob(new SystemLogParser(fname), "systemLog", timeInt));
	  logInfo("Created Monitor for System log file: " + f.getAbsolutePath());
        } else if (!f.exists())
	  logInfo("Skipping system log file " + fname + " (file not found)");
	else
	  logInfo("Skipping system log file " + fname + " (permission denied)");
      }
        

    // for network interfaces
    tmp = getProperty("nic.enabled");

    timeInt = setValue("nics.interval", DEFAULT_POLL_INTERVAL);
    
    if ("true".equalsIgnoreCase(tmp)) {
      monitors.add(new MonitorJob(new NICParser(), "nics", timeInt));
      logInfo("Created Monitor for NICs");
    }

    // for cpu
    tmp = getProperty("cpu.enabled");

    timeInt = setValue("cpu.interval", DEFAULT_POLL_INTERVAL);
    
    if ("true".equalsIgnoreCase(tmp)) {
      monitors.add(new MonitorJob(new CPUParser(), "cpu", timeInt));
      logInfo("Created Monitor for CPUs");
    }

    // for disks
    tmp = getProperty("disks.enabled");

    timeInt = setValue("disks.interval", DEFAULT_POLL_INTERVAL);
    
    if ("true".equalsIgnoreCase(tmp)) {
      // check privileges if a disk with no disks./dev/xxx/.source is found
      boolean smart_present = checkExistence("smartctl");
      int disks_ok = 0;
      String devicesStr = getProperty("disks.list");
      String[] devices = null;

      if (devicesStr != null)
        devices = devicesStr.split(",\\s*");
      
      for (int i = 0; i< devices.length; i++) {
        boolean file_present = false;
        boolean disk_present = false;
        
        String fileloc = getProperty("disks." + devices[i] + ".source");
        if (fileloc != null && fileloc.equalsIgnoreCase("true"))
          file_present = true;
        
        if (!file_present) 
          if (superuser) {
              StringBuffer sb = runCommand("sudo smartctl -i " + devices[i]);
              String patternStr = "[(failed)(device not supported)]";
              Pattern pattern = Pattern.compile(patternStr);
              Matcher matcher = pattern.matcher(sb.toString());
              if (matcher.find(0))
                disk_present = false;
              else
                disk_present = true;            
          }
        if (file_present || (disk_present && smart_present)) {
          disks_ok++;
        } else
          devices[i] = null;
      } 
      
      // now remove disks that dont exist
      StringBuffer resetSB = new StringBuffer();
      for (int j = 0; j < devices.length; j++) {
        resetSB.append(devices[j] == null ? "" : devices[j] + ", ");
	if (devices[j] != null)
	    logInfo("Found S.M.A.R.T. attributes for disk " + devices[j]);
      }
      // fix the property
      if (resetSB.length() >= 2)
        setProperty("disks.list", resetSB.substring(0, resetSB.length() - 2));
      
      if (disks_ok > 0) {
        monitors.add(new MonitorJob(new SMARTParser(), "disks", timeInt));
	logInfo("Created Monitor for S.M.A.R.T disk attributes");
      }
    }

    // for lm-sensors
    tmp = getProperty("sensors.enabled");

    timeInt = setValue("sensors.interval", DEFAULT_POLL_INTERVAL);
    
    if ("true".equalsIgnoreCase(tmp) && checkExistence("sensors")) {
      monitors.add(new MonitorJob(new SensorsParser(), "sensors", timeInt));
      logInfo("Created Monitor for lm-sensors output");
    }

    return monitors;
  }

  /**
   * Determines the minimum interval at which the executor thread
   * needs to wake upto execute jobs. Essentially, this is interval 
   * equals the GCD of intervals of all scheduled jobs. 
   * 
   *  @param monitors the list of scheduled jobs
   *  
   *  @return the minimum interval between two scheduled jobs
   */
  public static int getInterval(ArrayList<MonitorJob> monitors) {
    String tmp = getProperty("executor.interval.min");
    if (tmp != null)
      MIN_INTERVAL = Integer.parseInt(tmp);

    int[] monIntervals = new int[monitors.size()];

    for (int i = 0; i < monitors.size(); i++)
      monIntervals[i] = monitors.get(i).interval;

    return Math.max(MIN_INTERVAL, gcd(monIntervals));
  }

  /**
   * Checks whether a specific shell command is available
   * in the system. 
   * 
   *  @param cmd the command to check against
   *
   *  @return true, if the command is availble, false otherwise
   */
  public static boolean checkExistence(String cmd) {
    StringBuffer sb = runCommand("which " + cmd);
    if (sb.length() > 1)
      return true;

    return false;
  }

  /**
   * Runs a shell command in the system and provides a StringBuffer
   * with the output of the command.
   * 
   *  @param cmd an array of string that form the command to run 
   *  
   *  @return a StringBuffer that contains the output of the command 
   */
  public static StringBuffer runCommand(String[] cmd) {
    StringBuffer retval = new StringBuffer(MAX_OUTPUT_LENGTH);
    Process p;
    try {
      p = Runtime.getRuntime().exec(cmd);
      InputStream tmp = p.getInputStream();
      p.waitFor();
      int c;
      while ((c = tmp.read()) != -1)
        retval.append((char) c);
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    return retval;
  }

  /**
   * Runs a shell command in the system and provides a StringBuffer
   * with the output of the command.
   * 
   *  @param cmd the command to run 
   *  
   *  @return a StringBuffer that contains the output of the command 
   */
  public static StringBuffer runCommand(String cmd) {
    return runCommand(cmd.split("\\s+"));
  }

  /**
   * Determines the greatest common divisor (GCD) of two integers.
   * 
   *  @param m the first integer
   *  @param n the second integer
   *  
   *  @return the greatest common divisor of m and n
   */
  public static int gcd(int m, int n) {
    if (m == 0 && n == 0)
      return 0;
    if (m < n) {
      int t = m;
      m = n;
      n = t;
    }
    int r = m % n;
    if (r == 0) {
      return n;
    } else {
      return gcd(n, r);
    }
  }

  /**
   * Determines the greatest common divisor (GCD) of a list
   * of integers.
   * 
   *  @param numbers the list of integers to process
   *  
   *  @return the greatest common divisor of all numbers
   */
  public static int gcd(int[] numbers) {

    if (numbers.length == 1)
      return numbers[0];

    int g = gcd(numbers[0], numbers[1]);

    for (int i = 2; i < numbers.length; i++)
      g = gcd(g, numbers[i]);

    return g;
  }

  private static String [] expandDirs(String [] input, String patternStr) {

    ArrayList<String> fnames = new ArrayList<String>();
    Pattern pattern = Pattern.compile(patternStr);
    Matcher matcher;
    File f;
    
    for (String fname : input) {
      f = new File(fname);
      if (f.exists()) {
	if (f.isDirectory()) {
	  // add all matching files
	  File [] fcs = f.listFiles();
	  for (File fc : fcs) {
	    matcher = pattern.matcher(fc.getName());
	    if (matcher.find() && fc.isFile())
	      fnames.add(fc.getAbsolutePath());
	  }
	} else {
	  // normal file, just add to output
	  fnames.add(f.getAbsolutePath());
	}
      }
    }
    return fnames.toArray(input);
  }

  private static int setValue(String propname, int defaultValue) {

    String v = getProperty(propname);

    if (v != null)
      return Integer.parseInt(v);
    else
      return defaultValue;
  }

  
  public static void logInfo(String str) {
    LOG.info(str);
  }
}
