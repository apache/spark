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

package org.apache.hadoop.util;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Plugin to calculate resource information on Linux systems.
 */
public class LinuxResourceCalculatorPlugin extends ResourceCalculatorPlugin {
  private static final Log LOG =
      LogFactory.getLog(LinuxResourceCalculatorPlugin.class);

  /**
   * proc's meminfo virtual file has keys-values in the format
   * "key:[ \t]*value[ \t]kB".
   */
  private static final String PROCFS_MEMFILE = "/proc/meminfo";
  private static final Pattern PROCFS_MEMFILE_FORMAT =
      Pattern.compile("^([a-zA-Z]*):[ \t]*([0-9]*)[ \t]kB");

  // We need the values for the following keys in meminfo
  private static final String MEMTOTAL_STRING = "MemTotal";
  private static final String SWAPTOTAL_STRING = "SwapTotal";
  private static final String MEMFREE_STRING = "MemFree";
  private static final String SWAPFREE_STRING = "SwapFree";
  private static final String INACTIVE_STRING = "Inactive";
  private static final int UNAVAILABLE = -1;

  /**
   * Patterns for parsing /proc/cpuinfo
   */
  private static final String PROCFS_CPUINFO = "/proc/cpuinfo";
  private static final Pattern PROCESSOR_FORMAT =
      Pattern.compile("^processor[ \t]:[ \t]*([0-9]*)");
  private static final Pattern FREQUENCY_FORMAT =
      Pattern.compile("^cpu MHz[ \t]*:[ \t]*([0-9.]*)");

  /**
   * Pattern for parsing /proc/stat
   */
  private static final String PROCFS_STAT = "/proc/stat";
  private static final Pattern CPU_TIME_FORMAT =
    Pattern.compile("^cpu[ \t]*([0-9]*)" +
    		            "[ \t]*([0-9]*)[ \t]*([0-9]*)[ \t].*");
  
  private String procfsMemFile;
  private String procfsCpuFile;
  private String procfsStatFile;
  long jiffyLengthInMillis;

  private long ramSize = 0;
  private long swapSize = 0;
  private long ramSizeFree = 0;  // free ram space on the machine (kB)
  private long swapSizeFree = 0; // free swap space on the machine (kB)
  private long inactiveSize = 0; // inactive cache memory (kB)
  private int numProcessors = 0; // number of processors on the system
  private long cpuFrequency = 0L; // CPU frequency on the system (kHz)
  private long cumulativeCpuTime = 0L; // CPU used time since system is on (ms)
  private long lastCumulativeCpuTime = 0L; // CPU used time read last time (ms)
  // Unix timestamp while reading the CPU time (ms)
  private float cpuUsage = UNAVAILABLE;
  private long sampleTime = UNAVAILABLE;
  private long lastSampleTime = UNAVAILABLE;
  private ProcfsBasedProcessTree pTree = null;

  boolean readMemInfoFile = false;
  boolean readCpuInfoFile = false;
  
  /**
   * Get current time
   * @return Unix time stamp in millisecond
   */
  long getCurrentTime() {
    return System.currentTimeMillis();
  }
  
  public LinuxResourceCalculatorPlugin() {
    procfsMemFile = PROCFS_MEMFILE;
    procfsCpuFile = PROCFS_CPUINFO;
    procfsStatFile = PROCFS_STAT;
    jiffyLengthInMillis = ProcfsBasedProcessTree.JIFFY_LENGTH_IN_MILLIS;
    String pid = System.getenv().get("JVM_PID");
    pTree = new ProcfsBasedProcessTree(pid);
  }
  
  /**
   * Constructor which allows assigning the /proc/ directories. This will be
   * used only in unit tests
   * @param procfsMemFile fake file for /proc/meminfo
   * @param procfsCpuFile fake file for /proc/cpuinfo
   * @param procfsStatFile fake file for /proc/stat
   * @param jiffyLengthInMillis fake jiffy length value
   */
  public LinuxResourceCalculatorPlugin(String procfsMemFile,
                                       String procfsCpuFile,
                                       String procfsStatFile,
                                       long jiffyLengthInMillis) {
    this.procfsMemFile = procfsMemFile;
    this.procfsCpuFile = procfsCpuFile;
    this.procfsStatFile = procfsStatFile;
    this.jiffyLengthInMillis = jiffyLengthInMillis;
    String pid = System.getenv().get("JVM_PID");
    pTree = new ProcfsBasedProcessTree(pid);
  }

  /**
   * Read /proc/meminfo, parse and compute memory information only once
   */
  private void readProcMemInfoFile() {
    readProcMemInfoFile(false);
  }

  /**
   * Read /proc/meminfo, parse and compute memory information
   * @param readAgain if false, read only on the first time
   */
  private void readProcMemInfoFile(boolean readAgain) {

    if (readMemInfoFile && !readAgain) {
      return;
    }

    // Read "/proc/memInfo" file
    BufferedReader in = null;
    FileReader fReader = null;
    try {
      fReader = new FileReader(procfsMemFile);
      in = new BufferedReader(fReader);
    } catch (FileNotFoundException f) {
      // shouldn't happen....
      return;
    }

    Matcher mat = null;

    try {
      String str = in.readLine();
      while (str != null) {
        mat = PROCFS_MEMFILE_FORMAT.matcher(str);
        if (mat.find()) {
          if (mat.group(1).equals(MEMTOTAL_STRING)) {
            ramSize = Long.parseLong(mat.group(2));
          } else if (mat.group(1).equals(SWAPTOTAL_STRING)) {
            swapSize = Long.parseLong(mat.group(2));
          } else if (mat.group(1).equals(MEMFREE_STRING)) {
            ramSizeFree = Long.parseLong(mat.group(2));
          } else if (mat.group(1).equals(SWAPFREE_STRING)) {
            swapSizeFree = Long.parseLong(mat.group(2));
          } else if (mat.group(1).equals(INACTIVE_STRING)) {
            inactiveSize = Long.parseLong(mat.group(2));
          }
        }
        str = in.readLine();
      }
    } catch (IOException io) {
      LOG.warn("Error reading the stream " + io);
    } finally {
      // Close the streams
      try {
        fReader.close();
        try {
          in.close();
        } catch (IOException i) {
          LOG.warn("Error closing the stream " + in);
        }
      } catch (IOException i) {
        LOG.warn("Error closing the stream " + fReader);
      }
    }

    readMemInfoFile = true;
  }

  /**
   * Read /proc/cpuinfo, parse and calculate CPU information
   */
  private void readProcCpuInfoFile() {
    // This directory needs to be read only once
    if (readCpuInfoFile) {
      return;
    }
    // Read "/proc/cpuinfo" file
    BufferedReader in = null;
    FileReader fReader = null;
    try {
      fReader = new FileReader(procfsCpuFile);
      in = new BufferedReader(fReader);
    } catch (FileNotFoundException f) {
      // shouldn't happen....
      return;
    }
    Matcher mat = null;
    try {
      numProcessors = 0;
      String str = in.readLine();
      while (str != null) {
        mat = PROCESSOR_FORMAT.matcher(str);
        if (mat.find()) {
          numProcessors++;
        }
        mat = FREQUENCY_FORMAT.matcher(str);
        if (mat.find()) {
          cpuFrequency = (long)(Double.parseDouble(mat.group(1)) * 1000); // kHz
        }
        str = in.readLine();
      }
    } catch (IOException io) {
      LOG.warn("Error reading the stream " + io);
    } finally {
      // Close the streams
      try {
        fReader.close();
        try {
          in.close();
        } catch (IOException i) {
          LOG.warn("Error closing the stream " + in);
        }
      } catch (IOException i) {
        LOG.warn("Error closing the stream " + fReader);
      }
    }
    readCpuInfoFile = true;
  }

  /**
   * Read /proc/stat file, parse and calculate cumulative CPU
   */
  private void readProcStatFile() {
    // Read "/proc/stat" file
    BufferedReader in = null;
    FileReader fReader = null;
    try {
      fReader = new FileReader(procfsStatFile);
      in = new BufferedReader(fReader);
    } catch (FileNotFoundException f) {
      // shouldn't happen....
      return;
    }

    Matcher mat = null;
    try {
      String str = in.readLine();
      while (str != null) {
        mat = CPU_TIME_FORMAT.matcher(str);
        if (mat.find()) {
          long uTime = Long.parseLong(mat.group(1));
          long nTime = Long.parseLong(mat.group(2));
          long sTime = Long.parseLong(mat.group(3));
          cumulativeCpuTime = uTime + nTime + sTime; // milliseconds
          break;
        }
        str = in.readLine();
      }
      cumulativeCpuTime *= jiffyLengthInMillis;
    } catch (IOException io) {
      LOG.warn("Error reading the stream " + io);
    } finally {
      // Close the streams
      try {
        fReader.close();
        try {
          in.close();
        } catch (IOException i) {
          LOG.warn("Error closing the stream " + in);
        }
      } catch (IOException i) {
        LOG.warn("Error closing the stream " + fReader);
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  public long getPhysicalMemorySize() {
    readProcMemInfoFile();
    return ramSize * 1024;
  }

  /** {@inheritDoc} */
  @Override
  public long getVirtualMemorySize() {
    readProcMemInfoFile();
    return (ramSize + swapSize) * 1024;
  }

  /** {@inheritDoc} */
  @Override
  public long getAvailablePhysicalMemorySize() {
    readProcMemInfoFile(true);
    return (ramSizeFree + inactiveSize) * 1024;
  }

  /** {@inheritDoc} */
  @Override
  public long getAvailableVirtualMemorySize() {
    readProcMemInfoFile(true);
    return (ramSizeFree + swapSizeFree + inactiveSize) * 1024;
  }

  /** {@inheritDoc} */
  @Override
  public int getNumProcessors() {
    readProcCpuInfoFile();
    return numProcessors;
  }

  /** {@inheritDoc} */
  @Override
  public long getCpuFrequency() {
    readProcCpuInfoFile();
    return cpuFrequency;
  }

  /** {@inheritDoc} */
  @Override
  public long getCumulativeCpuTime() {
    readProcStatFile();
    return cumulativeCpuTime;
  }

  /** {@inheritDoc} */
  @Override
  public float getCpuUsage() {
    readProcStatFile();
    sampleTime = getCurrentTime();
    if (lastSampleTime == UNAVAILABLE ||
        lastSampleTime > sampleTime) {
      // lastSampleTime > sampleTime may happen when the system time is changed
      lastSampleTime = sampleTime;
      lastCumulativeCpuTime = cumulativeCpuTime;
      return cpuUsage;
    }
    // When lastSampleTime is sufficiently old, update cpuUsage.
    // Also take a sample of the current time and cumulative CPU time for the
    // use of the next calculation.
    final long MINIMUM_UPDATE_INTERVAL = 10 * jiffyLengthInMillis;
    if (sampleTime > lastSampleTime + MINIMUM_UPDATE_INTERVAL) {
	    cpuUsage = (float)(cumulativeCpuTime - lastCumulativeCpuTime) * 100F /
	               ((float)(sampleTime - lastSampleTime) * getNumProcessors());
	    lastSampleTime = sampleTime;
      lastCumulativeCpuTime = cumulativeCpuTime;
    }
    return cpuUsage;
  }

  /**
   * Test the {@link LinuxResourceCalculatorPlugin}
   *
   * @param args
   */
  public static void main(String[] args) {
    LinuxResourceCalculatorPlugin plugin = new LinuxResourceCalculatorPlugin();
    System.out.println("Physical memory Size (bytes) : "
        + plugin.getPhysicalMemorySize());
    System.out.println("Total Virtual memory Size (bytes) : "
        + plugin.getVirtualMemorySize());
    System.out.println("Available Physical memory Size (bytes) : "
        + plugin.getAvailablePhysicalMemorySize());
    System.out.println("Total Available Virtual memory Size (bytes) : "
        + plugin.getAvailableVirtualMemorySize());
    System.out.println("Number of Processors : " + plugin.getNumProcessors());
    System.out.println("CPU frequency (kHz) : " + plugin.getCpuFrequency());
    System.out.println("Cumulative CPU time (ms) : " +
            plugin.getCumulativeCpuTime());
    try {
      // Sleep so we can compute the CPU usage
      Thread.sleep(500L);
    } catch (InterruptedException e) {
      // do nothing
    }
    System.out.println("CPU usage % : " + plugin.getCpuUsage());
  }

  @Override
  public ProcResourceValues getProcResourceValues() {
    pTree = pTree.getProcessTree();
    long cpuTime = pTree.getCumulativeCpuTime();
    long pMem = pTree.getCumulativeRssmem();
    long vMem = pTree.getCumulativeVmem();
    return new ProcResourceValues(cpuTime, pMem, vMem);
  }
}
