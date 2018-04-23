/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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

import java.util.Date;

public final class Version {
  private static final Logger LOG = LoggerFactory.getLogger(Version.class);

  private static final String VERSION = "${project.version}";

  private static final String URL = "${project.scm.url}";
  private static final String BRANCH = "${scmBranch}";
  private static final String REVISION = "${buildNumber}";
  private static final long timestamp = ${timestamp}L;

  private static final String username = "${user.name}";

  public static String getVersion() {
    return VERSION;
  }

  public static String getUrl() {
    return URL;
  }

  public static String getRevision() {
    return REVISION;
  }

  public static String getBranch() {
    return BRANCH;
  }

  public static String getUser() {
    return username;
  }

  public static Date getDate() {
    return new Date(timestamp);
  }

  public static String getBuildInfo() {
    return "Compiled by " + getUser() + " on " + getDate();
  }

  static String[] versionReport() {
    return new String[]{
        "Spark " + getVersion(),
        "Source code repository " + getUrl() + " revision " + getRevision(),
        "Compiled by " + getUser() + " on " + getDate()
    };
  }

  public static void logVersion() {
    for (String line : versionReport()) {
      LOG.info(line);
    }
  }

  public static void main(String[] args) {
    logVersion();
  }
}
