/*
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

package org.apache.spark;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class VersionInfo {

  private static final Logger LOG = LoggerFactory.getLogger(VersionInfo.class);
  private Properties info;

  protected VersionInfo(String component) {
    info = new Properties();
    String versionInfoFile = component + "-version-info.properties";
    InputStream is = null;
    try {
      is = Thread.currentThread().getContextClassLoader()
              .getResourceAsStream(versionInfoFile);
      if (is == null) {
        throw new IOException("Resource not found");
      }
      info.load(is);
    } catch (IOException ex) {
    } finally {
      if (is != null) {
        try {
          is.close();
        } catch (IOException ioex) {
        }
      }
    }
  }

  protected String _getVersion() {
    return info.getProperty("version", "Unknown");
  }

  protected String _getRevision() {
    return info.getProperty("revision", "Unknown");
  }

  protected String _getBranch() {
    return info.getProperty("branch", "Unknown");
  }

  protected String _getDate() {
    return info.getProperty("date", "Unknown");
  }

  protected String _getUser() {
    return info.getProperty("user", "Unknown");
  }

  protected String _getUrl() {
    return info.getProperty("url", "Unknown");
  }

  protected String _getSrcChecksum() {
    return info.getProperty("srcChecksum", "Unknown");
  }

  protected String _getBuildVersion(){
    return getVersion() +
            " from " + _getRevision() +
            " by " + _getUser() +
            " source checksum " + _getSrcChecksum();
  }

  private static VersionInfo COMMON_VERSION_INFO = new VersionInfo("spark");

  public static String getVersion() {
    return COMMON_VERSION_INFO._getVersion();
  }

  public static String getRevision() {
    return COMMON_VERSION_INFO._getRevision();
  }

  public static String getBranch() {
    return COMMON_VERSION_INFO._getBranch();
  }

  public static String getDate() {
    return COMMON_VERSION_INFO._getDate();
  }

  public static String getUser() {
    return COMMON_VERSION_INFO._getUser();
  }

  public static String getUrl() {
    return COMMON_VERSION_INFO._getUrl();
  }

  public static String getSrcChecksum() {
    return COMMON_VERSION_INFO._getSrcChecksum();
  }

  public static String getBuildVersion(){
    return COMMON_VERSION_INFO._getBuildVersion();
  }

  public static void main(String[] args) {
    System.out.println("Spark " + getVersion());
    System.out.println("URL " + getUrl() + " -r " + getRevision());
    System.out.println("Branch " + getBranch());
    System.out.println("Compiled by " + getUser() + " on " + getDate());
    System.out.println("From source with checksum " + getSrcChecksum());
  }
}
