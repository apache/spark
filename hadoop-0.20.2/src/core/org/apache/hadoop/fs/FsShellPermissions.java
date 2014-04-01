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
package org.apache.hadoop.fs;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FsShell.CmdHandler;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.ChmodParser;


/**
 * This class is the home for file permissions related commands.
 * Moved to this separate class since FsShell is getting too large.
 */
class FsShellPermissions {
  
  /*========== chmod ==========*/
   
  /* The pattern is alsmost as flexible as mode allowed by 
   * chmod shell command. The main restriction is that we recognize only rwxX.
   * To reduce errors we also enforce 3 digits for octal mode.
   */  
  
  static String CHMOD_USAGE = 
                            "-chmod [-R] <MODE[,MODE]... | OCTALMODE> PATH...";

  private static  ChmodParser pp;
  
  private static class ChmodHandler extends CmdHandler {

    ChmodHandler(FileSystem fs, String modeStr) throws IOException {
      super("chmod", fs);
      try {
        pp = new ChmodParser(modeStr);
      } catch(IllegalArgumentException iea) {
        patternError(iea.getMessage());
      }
    }

    private void patternError(String mode) throws IOException {
     throw new IOException("chmod : mode '" + mode + 
         "' does not match the expected pattern.");      
    }
    
    @Override
    public void run(FileStatus file, FileSystem srcFs) throws IOException {
      int newperms = pp.applyNewPermission(file);

      if (file.getPermission().toShort() != newperms) {
        try {
          srcFs.setPermission(file.getPath(), 
                                new FsPermission((short)newperms));
        } catch (IOException e) {
          FsShell.LOG.debug("Error", e);
          System.err.println(getName() + ": changing permissions of '" + 
                             file.getPath() + "':" + e.getMessage().split("\n")[0]);
        }
      }
    }
  }

  /*========== chown ==========*/
  
  static private String allowedChars = "[-_./@a-zA-Z0-9]";
  ///allows only "allowedChars" above in names for owner and group
  static private Pattern chownPattern = 
         Pattern.compile("^\\s*(" + allowedChars + "+)?" +
                          "([:](" + allowedChars + "*))?\\s*$");
  static private Pattern chgrpPattern = 
         Pattern.compile("^\\s*(" + allowedChars + "+)\\s*$");
  
  static String CHOWN_USAGE = "-chown [-R] [OWNER][:[GROUP]] PATH...";
  static String CHGRP_USAGE = "-chgrp [-R] GROUP PATH...";  

  private static class ChownHandler extends CmdHandler {
    protected String owner = null;
    protected String group = null;

    protected ChownHandler(String cmd, FileSystem fs) { //for chgrp
      super(cmd, fs);
    }

    ChownHandler(FileSystem fs, String ownerStr) throws IOException {
      super("chown", fs);
      Matcher matcher = chownPattern.matcher(ownerStr);
      if (!matcher.matches()) {
        throw new IOException("'" + ownerStr + "' does not match " +
                              "expected pattern for [owner][:group].");
      }
      owner = matcher.group(1);
      group = matcher.group(3);
      if (group != null && group.length() == 0) {
        group = null;
      }
      if (owner == null && group == null) {
        throw new IOException("'" + ownerStr + "' does not specify " +
                              " owner or group.");
      }
    }

    @Override
    public void run(FileStatus file, FileSystem srcFs) throws IOException {
      //Should we do case insensitive match?  
      String newOwner = (owner == null || owner.equals(file.getOwner())) ?
                        null : owner;
      String newGroup = (group == null || group.equals(file.getGroup())) ?
                        null : group;

      if (newOwner != null || newGroup != null) {
        try {
          srcFs.setOwner(file.getPath(), newOwner, newGroup);
        } catch (IOException e) {
          System.err.println(getName() + ": changing ownership of '" + 
                             file.getPath() + "':" + e.getMessage().split("\n")[0]);

        }
      }
    }
  }

  /*========== chgrp ==========*/    
  
  private static class ChgrpHandler extends ChownHandler {
    ChgrpHandler(FileSystem fs, String groupStr) throws IOException {
      super("chgrp", fs);

      Matcher matcher = chgrpPattern.matcher(groupStr);
      if (!matcher.matches()) {
        throw new IOException("'" + groupStr + "' does not match " +
        "expected pattern for group");
      }
      group = matcher.group(1);
    }
  }

  static int changePermissions(FileSystem fs, String cmd, 
                                String argv[], int startIndex, FsShell shell)
                                throws IOException {
    CmdHandler handler = null;
    boolean recursive = false;

    // handle common arguments, currently only "-R" 
    for (; startIndex < argv.length && argv[startIndex].equals("-R"); 
    startIndex++) {
      recursive = true;
    }

    if ( startIndex >= argv.length ) {
      throw new IOException("Not enough arguments for the command");
    }

    if (cmd.equals("-chmod")) {
      handler = new ChmodHandler(fs, argv[startIndex++]);
    } else if (cmd.equals("-chown")) {
      handler = new ChownHandler(fs, argv[startIndex++]);
    } else if (cmd.equals("-chgrp")) {
      handler = new ChgrpHandler(fs, argv[startIndex++]);
    }

    return shell.runCmdHandler(handler, argv, startIndex, recursive);
  } 
}
