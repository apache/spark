/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.maven.plugin.versioninfo;

import org.apache.maven.model.FileSet;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;
import org.apache.spark.maven.plugin.util.CommandExec;
import org.codehaus.plexus.util.FileUtils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

@Mojo(name = "version-info")
public class VersionInfoMojo extends AbstractMojo {

  @Parameter(defaultValue = "${project}")
  private MavenProject project;

  @Parameter(required = true)
  private FileSet source;

  @Parameter(defaultValue = "version-info.build.time")
  private String buildTimeProperty;

  @Parameter(defaultValue = "version-info.source.md5")
  private String md5Property;

  @Parameter(defaultValue = "version-info.scm.uri")
  private String scmUriProperty;

  @Parameter(defaultValue = "version-info.scm.branch")
  private String scmBranchProperty;

  @Parameter(defaultValue = "version-info.scm.commit")
  private String scmCommitProperty;

  @Parameter(defaultValue = "git")
  private String gitCommand;

  @Parameter(defaultValue = "svn")
  private String svnCommand;

  private enum SCM {
    NONE, SVN, GIT
  }

  @Override
  public void execute() throws MojoExecutionException {
    try {
      SCM scm = determineSCM();
      project.getProperties().setProperty(buildTimeProperty, getBuildTime());
      project.getProperties().setProperty(scmUriProperty, getSCMUri(scm));
      project.getProperties().setProperty(scmBranchProperty, getSCMBranch(scm));
      project.getProperties().setProperty(scmCommitProperty, getSCMCommit(scm));
      project.getProperties().setProperty(md5Property, computeMD5());
    } catch (Throwable ex) {
      throw new MojoExecutionException(ex.toString(), ex);
    }
  }

  private String getBuildTime() {
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
    dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    return dateFormat.format(new Date());
  }

  private List<String> scmOut;

  /**
   * Determines which SCM is in use (Subversion, git, or none) and captures
   * output of the SCM command for later parsing.
   * 
   * @return SCM in use for this build
   * @throws Exception if any error occurs attempting to determine SCM
   */
  private SCM determineSCM() throws Exception {
    CommandExec exec = new CommandExec(this);
    SCM scm = SCM.NONE;
    scmOut = new ArrayList<String>();
    int ret = exec.run(Arrays.asList(svnCommand, "info"), scmOut);
    if (ret == 0) {
      scm = SCM.SVN;
    } else {
      ret = exec.run(Arrays.asList(gitCommand, "branch"), scmOut);
      if (ret == 0) {
        ret = exec.run(Arrays.asList(gitCommand, "remote", "-v"), scmOut);
        if (ret != 0) {
          scm = SCM.NONE;
          scmOut = null;
        } else {
          ret = exec.run(Arrays.asList(gitCommand, "log", "-n", "1"), scmOut);
          if (ret != 0) {
            scm = SCM.NONE;
            scmOut = null;
          } else {
            scm = SCM.GIT;
          }
        }
      }
    }
    if (scmOut != null) {
      getLog().debug(scmOut.toString());
    }
    getLog().info("SCM: " + scm);
    return scm;
  }

  private String[] getSvnUriInfo(String str) {
    String[] res = new String[] { "Unknown", "Unknown" };
    try {
      String path = str;
      int index = path.indexOf("trunk");
      if (index > -1) {
        res[0] = path.substring(0, index - 1);
        res[1] = "trunk";
      } else {
        index = path.indexOf("branches");
        if (index > -1) {
          res[0] = path.substring(0, index - 1);
          int branchIndex = index + "branches".length() + 1;
          index = path.indexOf("/", branchIndex);
          if (index > -1) {
            res[1] = path.substring(branchIndex, index);
          } else {
            res[1] = path.substring(branchIndex);
          }
        }
      }
    } catch (Exception ex) {
      getLog().warn("Could not determine URI & branch from SVN URI: " + str);
    }
    return res;
  }

  private String getSCMUri(SCM scm) {
    String uri = "Unknown";
    switch (scm) {
    case SVN:
      for (String s : scmOut) {
        if (s.startsWith("URL:")) {
          uri = s.substring(4).trim();
          uri = getSvnUriInfo(uri)[0];
          break;
        }
      }
      break;
    case GIT:
      for (String s : scmOut) {
        if (s.startsWith("origin") && s.endsWith("(fetch)")) {
          uri = s.substring("origin".length());
          uri = uri.substring(0, uri.length() - "(fetch)".length());
          break;
        }
      }
      break;
    }
    return uri.trim();
  }

  private String getSCMCommit(SCM scm) {
    String commit = "Unknown";
    switch (scm) {
    case SVN:
      for (String s : scmOut) {
        if (s.startsWith("Revision:")) {
          commit = s.substring("Revision:".length());
          break;
        }
      }
      break;
    case GIT:
      for (String s : scmOut) {
        if (s.startsWith("commit")) {
          commit = s.substring("commit".length());
          break;
        }
      }
      break;
    }
    return commit.trim();
  }

  private String getSCMBranch(SCM scm) {
    String branch = "Unknown";
    switch (scm) {
    case SVN:
      for (String s : scmOut) {
        if (s.startsWith("URL:")) {
          branch = s.substring(4).trim();
          branch = getSvnUriInfo(branch)[1];
          break;
        }
      }
      break;
    case GIT:
      for (String s : scmOut) {
        if (s.startsWith("*")) {
          branch = s.substring("*".length());
          break;
        }
      }
      break;
    }
    return branch.trim();
  }

  private byte[] readFile(File file) throws IOException {
    RandomAccessFile raf = new RandomAccessFile(file, "r");
    byte[] buffer = new byte[(int) raf.length()];
    raf.readFully(buffer);
    raf.close();
    return buffer;
  }

  private byte[] computeMD5(List<File> files) throws IOException,
      NoSuchAlgorithmException {
    MessageDigest md5 = MessageDigest.getInstance("MD5");
    for (File file : files) {
      getLog().debug("Computing MD5 for: " + file);
      md5.update(readFile(file));
    }
    return md5.digest();
  }

  private String byteArrayToString(byte[] array) {
    StringBuilder sb = new StringBuilder();
    for (byte b : array) {
      sb.append(Integer.toHexString(0xff & b));
    }
    return sb.toString();
  }

  private String computeMD5() throws Exception {
    List<File> files = convertFileSetToFiles(source);
    // File order of MD5 calculation is significant. Sorting is done on
    // unix-format names, case-folded, in order to get a platform-independent
    // sort and calculate the same MD5 on all platforms.
    Collections.sort(files, new Comparator<File>() {
      @Override
      public int compare(File lhs, File rhs) {
        return normalizePath(lhs).compareTo(normalizePath(rhs));
      }

      private String normalizePath(File file) {
        return file.getPath().toUpperCase().replaceAll("\\\\", "/");
      }
    });
    byte[] md5 = computeMD5(files);
    String md5str = byteArrayToString(md5);
    getLog().info("Computed MD5: " + md5str);
    return md5str;
  }

  @SuppressWarnings("rawtypes")
  private static String getCommaSeparatedList(List list) {
    StringBuilder buffer = new StringBuilder();
    String separator = "";
    for (Object e : list) {
      buffer.append(separator).append(e);
      separator = ",";
    }
    return buffer.toString();
  }

  @SuppressWarnings("unchecked")
  public static List<File> convertFileSetToFiles(FileSet source)
      throws IOException {
    String includes = getCommaSeparatedList(source.getIncludes());
    String excludes = getCommaSeparatedList(source.getExcludes());
    return FileUtils.getFiles(new File(source.getDirectory()), includes,
        excludes);
  }
}
