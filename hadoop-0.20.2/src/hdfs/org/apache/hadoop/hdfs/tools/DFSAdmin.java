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
package org.apache.hadoop.hdfs.tools;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.shell.Command;
import org.apache.hadoop.fs.shell.CommandFormat;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.FSConstants.UpgradeAction;
import org.apache.hadoop.hdfs.server.common.UpgradeStatusReport;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.RefreshUserMappingsProtocol;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.RefreshAuthorizationPolicyProtocol;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;

/**
 * This class provides some DFS administrative access.
 */
public class DFSAdmin extends FsShell {

  /**
   * An abstract class for the execution of a file system command
   */
  abstract private static class DFSAdminCommand extends Command {
    final DistributedFileSystem dfs;
    /** Constructor */
    public DFSAdminCommand(FileSystem fs) {
      super(fs.getConf());
      if (!(fs instanceof DistributedFileSystem)) {
        throw new IllegalArgumentException("FileSystem " + fs.getUri() + 
            " is not a distributed file system");
      }
      this.dfs = (DistributedFileSystem)fs;
    }
  }
  
  /** A class that supports command clearQuota */
  private static class ClearQuotaCommand extends DFSAdminCommand {
    private static final String NAME = "clrQuota";
    private static final String USAGE = "-"+NAME+" <dirname>...<dirname>";
    private static final String DESCRIPTION = USAGE + ": " +
    "Clear the quota for each directory <dirName>.\n" +
    "\t\tBest effort for the directory. with fault reported if\n" +
    "\t\t1. the directory does not exist or is a file, or\n" +
    "\t\t2. user is not an administrator.\n" +
    "\t\tIt does not fault if the directory has no quota.";
    
    /** Constructor */
    ClearQuotaCommand(String[] args, int pos, FileSystem fs) {
      super(fs);
      CommandFormat c = new CommandFormat(NAME, 1, Integer.MAX_VALUE);
      List<String> parameters = c.parse(args, pos);
      this.args = parameters.toArray(new String[parameters.size()]);
    }
    
    /** Check if a command is the clrQuota command
     * 
     * @param cmd A string representation of a command starting with "-"
     * @return true if this is a clrQuota command; false otherwise
     */
    public static boolean matches(String cmd) {
      return ("-"+NAME).equals(cmd); 
    }

    @Override
    public String getCommandName() {
      return NAME;
    }

    @Override
    public void run(Path path) throws IOException {
      dfs.setQuota(path, FSConstants.QUOTA_RESET, FSConstants.QUOTA_DONT_SET);
    }
  }
  
  /** A class that supports command setQuota */
  private static class SetQuotaCommand extends DFSAdminCommand {
    private static final String NAME = "setQuota";
    private static final String USAGE =
      "-"+NAME+" <quota> <dirname>...<dirname>";
    private static final String DESCRIPTION = 
      "-setQuota <quota> <dirname>...<dirname>: " +
      "Set the quota <quota> for each directory <dirName>.\n" + 
      "\t\tThe directory quota is a long integer that puts a hard limit\n" +
      "\t\ton the number of names in the directory tree\n" +
      "\t\tBest effort for the directory, with faults reported if\n" +
      "\t\t1. N is not a positive integer, or\n" +
      "\t\t2. user is not an administrator, or\n" +
      "\t\t3. the directory does not exist or is a file, or\n";

    private final long quota; // the quota to be set
    
    /** Constructor */
    SetQuotaCommand(String[] args, int pos, FileSystem fs) {
      super(fs);
      CommandFormat c = new CommandFormat(NAME, 2, Integer.MAX_VALUE);
      List<String> parameters = c.parse(args, pos);
      this.quota = Long.parseLong(parameters.remove(0));
      this.args = parameters.toArray(new String[parameters.size()]);
    }
    
    /** Check if a command is the setQuota command
     * 
     * @param cmd A string representation of a command starting with "-"
     * @return true if this is a count command; false otherwise
     */
    public static boolean matches(String cmd) {
      return ("-"+NAME).equals(cmd); 
    }

    @Override
    public String getCommandName() {
      return NAME;
    }

    @Override
    public void run(Path path) throws IOException {
      dfs.setQuota(path, quota, FSConstants.QUOTA_DONT_SET);
    }
  }
  
  /** A class that supports command clearSpaceQuota */
  private static class ClearSpaceQuotaCommand extends DFSAdminCommand {
    private static final String NAME = "clrSpaceQuota";
    private static final String USAGE = "-"+NAME+" <dirname>...<dirname>";
    private static final String DESCRIPTION = USAGE + ": " +
    "Clear the disk space quota for each directory <dirName>.\n" +
    "\t\tBest effort for the directory. with fault reported if\n" +
    "\t\t1. the directory does not exist or is a file, or\n" +
    "\t\t2. user is not an administrator.\n" +
    "\t\tIt does not fault if the directory has no quota.";
    
    /** Constructor */
    ClearSpaceQuotaCommand(String[] args, int pos, FileSystem fs) {
      super(fs);
      CommandFormat c = new CommandFormat(NAME, 1, Integer.MAX_VALUE);
      List<String> parameters = c.parse(args, pos);
      this.args = parameters.toArray(new String[parameters.size()]);
    }
    
    /** Check if a command is the clrQuota command
     * 
     * @param cmd A string representation of a command starting with "-"
     * @return true if this is a clrQuota command; false otherwise
     */
    public static boolean matches(String cmd) {
      return ("-"+NAME).equals(cmd); 
    }

    @Override
    public String getCommandName() {
      return NAME;
    }

    @Override
    public void run(Path path) throws IOException {
      dfs.setQuota(path, FSConstants.QUOTA_DONT_SET, FSConstants.QUOTA_RESET);
    }
  }
  
  /** A class that supports command setQuota */
  private static class SetSpaceQuotaCommand extends DFSAdminCommand {
    private static final String NAME = "setSpaceQuota";
    private static final String USAGE =
      "-"+NAME+" <quota> <dirname>...<dirname>";
    private static final String DESCRIPTION = USAGE + ": " +
      "Set the disk space quota <quota> for each directory <dirName>.\n" + 
      "\t\tThe space quota is a long integer that puts a hard limit\n" +
      "\t\ton the total size of all the files under the directory tree.\n" +
      "\t\tThe extra space required for replication is also counted. E.g.\n" +
      "\t\ta 1GB file with replication of 3 consumes 3GB of the quota.\n\n" +
      "\t\tQuota can also be speciefied with a binary prefix for terabytes,\n" +
      "\t\tpetabytes etc (e.g. 50t is 50TB, 5m is 5MB, 3p is 3PB).\n" + 
      "\t\tBest effort for the directory, with faults reported if\n" +
      "\t\t1. N is not a positive integer, or\n" +
      "\t\t2. user is not an administrator, or\n" +
      "\t\t3. the directory does not exist or is a file, or\n";

    private long quota; // the quota to be set
    
    /** Constructor */
    SetSpaceQuotaCommand(String[] args, int pos, FileSystem fs) {
      super(fs);
      CommandFormat c = new CommandFormat(NAME, 2, Integer.MAX_VALUE);
      List<String> parameters = c.parse(args, pos);
      String str = parameters.remove(0).trim();
      quota = StringUtils.TraditionalBinaryPrefix.string2long(str);
      this.args = parameters.toArray(new String[parameters.size()]);
    }
    
    /** Check if a command is the setQuota command
     * 
     * @param cmd A string representation of a command starting with "-"
     * @return true if this is a count command; false otherwise
     */
    public static boolean matches(String cmd) {
      return ("-"+NAME).equals(cmd); 
    }

    @Override
    public String getCommandName() {
      return NAME;
    }

    @Override
    public void run(Path path) throws IOException {
      dfs.setQuota(path, FSConstants.QUOTA_DONT_SET, quota);
    }
  }
  
  /**
   * Construct a DFSAdmin object.
   */
  public DFSAdmin() {
    this(null);
  }

  /**
   * Construct a DFSAdmin object.
   */
  public DFSAdmin(Configuration conf) {
    super(conf);
  }
  
  /**
   * Gives a report on how the FileSystem is doing.
   * @exception IOException if the filesystem does not exist.
   */
  public void report() throws IOException {
    if (fs instanceof DistributedFileSystem) {
      DistributedFileSystem dfs = (DistributedFileSystem) fs;
      FsStatus ds = dfs.getStatus();
      long capacity = ds.getCapacity();
      long used = ds.getUsed();
      long remaining = ds.getRemaining();
      long presentCapacity = used + remaining;
      boolean mode = dfs.setSafeMode(FSConstants.SafeModeAction.SAFEMODE_GET);
      UpgradeStatusReport status = 
                      dfs.distributedUpgradeProgress(UpgradeAction.GET_STATUS);

      if (mode) {
        System.out.println("Safe mode is ON");
      }
      if (status != null) {
        System.out.println(status.getStatusText(false));
      }
      System.out.println("Configured Capacity: " + capacity
                         + " (" + StringUtils.byteDesc(capacity) + ")");
      System.out.println("Present Capacity: " + presentCapacity
          + " (" + StringUtils.byteDesc(presentCapacity) + ")");
      System.out.println("DFS Remaining: " + remaining
          + " (" + StringUtils.byteDesc(remaining) + ")");
      System.out.println("DFS Used: " + used
                         + " (" + StringUtils.byteDesc(used) + ")");
      System.out.println("DFS Used%: "
                         + StringUtils.limitDecimalTo2(((1.0 * used) / presentCapacity) * 100)
                         + "%");
      
      /* These counts are not always upto date. They are updated after  
       * iteration of an internal list. Should be updated in a few seconds to 
       * minutes. Use "-metaSave" to list of all such blocks and accurate 
       * counts.
       */
      System.out.println("Under replicated blocks: " + 
                         dfs.getUnderReplicatedBlocksCount());
      System.out.println("Blocks with corrupt replicas: " + 
                         dfs.getCorruptBlocksCount());
      System.out.println("Missing blocks: " + 
                         dfs.getMissingBlocksCount());
                           
      System.out.println();

      System.out.println("-------------------------------------------------");
      
      DatanodeInfo[] live = dfs.getClient().datanodeReport(
                                                   DatanodeReportType.LIVE);
      DatanodeInfo[] dead = dfs.getClient().datanodeReport(
                                                   DatanodeReportType.DEAD);
      System.out.println("Datanodes available: " + live.length +
                         " (" + (live.length + dead.length) + " total, " + 
                         dead.length + " dead)\n");
      
      for (DatanodeInfo dn : live) {
        System.out.println(dn.getDatanodeReport());
        System.out.println();
      }
      for (DatanodeInfo dn : dead) {
        System.out.println(dn.getDatanodeReport());
        System.out.println();
      }      
    }
  }

  /**
   * Safe mode maintenance command.
   * Usage: java DFSAdmin -safemode [enter | leave | get]
   * @param argv List of of command line parameters.
   * @param idx The index of the command that is being processed.
   * @exception IOException if the filesystem does not exist.
   */
  public void setSafeMode(String[] argv, int idx) throws IOException {
    if (!(fs instanceof DistributedFileSystem)) {
      System.err.println("FileSystem is " + fs.getUri());
      return;
    }
    if (idx != argv.length - 1) {
      printUsage("-safemode");
      return;
    }
    FSConstants.SafeModeAction action;
    Boolean waitExitSafe = false;

    if ("leave".equalsIgnoreCase(argv[idx])) {
      action = FSConstants.SafeModeAction.SAFEMODE_LEAVE;
    } else if ("enter".equalsIgnoreCase(argv[idx])) {
      action = FSConstants.SafeModeAction.SAFEMODE_ENTER;
    } else if ("get".equalsIgnoreCase(argv[idx])) {
      action = FSConstants.SafeModeAction.SAFEMODE_GET;
    } else if ("wait".equalsIgnoreCase(argv[idx])) {
      action = FSConstants.SafeModeAction.SAFEMODE_GET;
      waitExitSafe = true;
    } else {
      printUsage("-safemode");
      return;
    }
    DistributedFileSystem dfs = (DistributedFileSystem) fs;
    boolean inSafeMode = dfs.setSafeMode(action);

    //
    // If we are waiting for safemode to exit, then poll and
    // sleep till we are out of safemode.
    //
    if (waitExitSafe) {
      while (inSafeMode) {
        try {
          Thread.sleep(5000);
        } catch (java.lang.InterruptedException e) {
          throw new IOException("Wait Interrupted");
        }
        inSafeMode = dfs.setSafeMode(action);
      }
    }

    System.out.println("Safe mode is " + (inSafeMode ? "ON" : "OFF"));
  }

  /**
   * Command to ask the namenode to save the namespace.
   * Usage: java DFSAdmin -saveNamespace
   * @exception IOException 
   * @see org.apache.hadoop.hdfs.protocol.ClientProtocol#saveNamespace()
   */
  public int saveNamespace() throws IOException {
    int exitCode = -1;

    if (!(fs instanceof DistributedFileSystem)) {
      System.err.println("FileSystem is " + fs.getUri());
      return exitCode;
    }

    DistributedFileSystem dfs = (DistributedFileSystem) fs;
    dfs.saveNamespace();
    exitCode = 0;
   
    return exitCode;
  }

  /**
   * Command to ask the namenode to reread the hosts and excluded hosts 
   * file.
   * Usage: java DFSAdmin -refreshNodes
   * @exception IOException 
   */
  public int refreshNodes() throws IOException {
    int exitCode = -1;

    if (!(fs instanceof DistributedFileSystem)) {
      System.err.println("FileSystem is " + fs.getUri());
      return exitCode;
    }

    DistributedFileSystem dfs = (DistributedFileSystem) fs;
    dfs.refreshNodes();
    exitCode = 0;
   
    return exitCode;
  }

  private void printHelp(String cmd) {
    String summary = "hadoop dfsadmin is the command to execute DFS administrative commands.\n" +
      "The full syntax is: \n\n" +
      "hadoop dfsadmin [-report] [-safemode <enter | leave | get | wait>]\n" +
      "\t[-saveNamespace]\n" +
      "\t[-refreshNodes]\n" +
      "\t[" + SetQuotaCommand.USAGE + "]\n" +
      "\t[" + ClearQuotaCommand.USAGE +"]\n" +
      "\t[" + SetSpaceQuotaCommand.USAGE + "]\n" +
      "\t[" + ClearSpaceQuotaCommand.USAGE +"]\n" +
      "\t[-refreshServiceAcl]\n" +
      "\t[-refreshUserToGroupsMappings]\n" +
      "\t[refreshSuperUserGroupsConfiguration]\n" +
      "\t[-help [cmd]]\n";

    String report ="-report: \tReports basic filesystem information and statistics.\n";
        
    String safemode = "-safemode <enter|leave|get|wait>:  Safe mode maintenance command.\n" + 
      "\t\tSafe mode is a Namenode state in which it\n" +
      "\t\t\t1.  does not accept changes to the name space (read-only)\n" +
      "\t\t\t2.  does not replicate or delete blocks.\n" +
      "\t\tSafe mode is entered automatically at Namenode startup, and\n" +
      "\t\tleaves safe mode automatically when the configured minimum\n" +
      "\t\tpercentage of blocks satisfies the minimum replication\n" +
      "\t\tcondition.  Safe mode can also be entered manually, but then\n" +
      "\t\tit can only be turned off manually as well.\n";

    String saveNamespace = "-saveNamespace:\t" +
    "Save current namespace into storage directories and reset edits log.\n" +
    "\t\tRequires superuser permissions and safe mode.\n";

    String refreshNodes = "-refreshNodes: \tUpdates the set of hosts allowed " +
                          "to connect to namenode.\n\n" +
      "\t\tRe-reads the config file to update values defined by \n" +
      "\t\tdfs.hosts and dfs.host.exclude and reads the \n" +
      "\t\tentires (hostnames) in those files.\n\n" +
      "\t\tEach entry not defined in dfs.hosts but in \n" + 
      "\t\tdfs.hosts.exclude is decommissioned. Each entry defined \n" +
      "\t\tin dfs.hosts and also in dfs.host.exclude is stopped from \n" +
      "\t\tdecommissioning if it has aleady been marked for decommission.\n" + 
      "\t\tEntires not present in both the lists are decommissioned.\n";

    String finalizeUpgrade = "-finalizeUpgrade: Finalize upgrade of HDFS.\n" +
      "\t\tDatanodes delete their previous version working directories,\n" +
      "\t\tfollowed by Namenode doing the same.\n" + 
      "\t\tThis completes the upgrade process.\n";

    String upgradeProgress = "-upgradeProgress <status|details|force>: \n" +
      "\t\trequest current distributed upgrade status, \n" +
      "\t\ta detailed status or force the upgrade to proceed.\n";

    String metaSave = "-metasave <filename>: \tSave Namenode's primary data structures\n" +
      "\t\tto <filename> in the directory specified by hadoop.log.dir property.\n" +
      "\t\t<filename> will contain one line for each of the following\n" +
      "\t\t\t1. Datanodes heart beating with Namenode\n" +
      "\t\t\t2. Blocks waiting to be replicated\n" +
      "\t\t\t3. Blocks currrently being replicated\n" +
      "\t\t\t4. Blocks waiting to be deleted\n";

    String refreshServiceAcl = "-refreshServiceAcl: Reload the service-level authorization policy file\n" +
      "\t\tNamenode will reload the authorization policy file.\n";

    String refreshUserToGroupsMappings =
      "-refreshUserToGroupsMappings: Refresh user-to-groups mappings\n";
    
    String refreshSuperUserGroupsConfiguration = 
      "-refreshSuperUserGroupsConfiguration: Refresh superuser proxy groups mappings\n";
    
    String help = "-help [cmd]: \tDisplays help for the given command or all commands if none\n" +
      "\t\tis specified.\n";

    if ("report".equals(cmd)) {
      System.out.println(report);
    } else if ("safemode".equals(cmd)) {
      System.out.println(safemode);
    } else if ("saveNamespace".equals(cmd)) {
      System.out.println(saveNamespace);
    } else if ("refreshNodes".equals(cmd)) {
      System.out.println(refreshNodes);
    } else if ("finalizeUpgrade".equals(cmd)) {
      System.out.println(finalizeUpgrade);
    } else if ("upgradeProgress".equals(cmd)) {
      System.out.println(upgradeProgress);
    } else if ("metasave".equals(cmd)) {
      System.out.println(metaSave);
    } else if (SetQuotaCommand.matches("-"+cmd)) {
      System.out.println(SetQuotaCommand.DESCRIPTION);
    } else if (ClearQuotaCommand.matches("-"+cmd)) {
      System.out.println(ClearQuotaCommand.DESCRIPTION);
    } else if (SetSpaceQuotaCommand.matches("-"+cmd)) {
      System.out.println(SetSpaceQuotaCommand.DESCRIPTION);
    } else if (ClearSpaceQuotaCommand.matches("-"+cmd)) {
      System.out.println(ClearSpaceQuotaCommand.DESCRIPTION);
    } else if ("refreshServiceAcl".equals(cmd)) {
      System.out.println(refreshServiceAcl);
    } else if ("refreshUserToGroupsMappings".equals(cmd)) {
      System.out.println(refreshUserToGroupsMappings);
    } else if ("refreshSuperUserGroupsConfiguration".equals(cmd)) {
      System.out.println(refreshSuperUserGroupsConfiguration);
    } else if ("help".equals(cmd)) {
      System.out.println(help);
    } else {
      System.out.println(summary);
      System.out.println(report);
      System.out.println(safemode);
      System.out.println(saveNamespace);
      System.out.println(refreshNodes);
      System.out.println(finalizeUpgrade);
      System.out.println(upgradeProgress);
      System.out.println(metaSave);
      System.out.println(SetQuotaCommand.DESCRIPTION);
      System.out.println(ClearQuotaCommand.DESCRIPTION);
      System.out.println(SetSpaceQuotaCommand.DESCRIPTION);
      System.out.println(ClearSpaceQuotaCommand.DESCRIPTION);
      System.out.println(refreshServiceAcl);
      System.out.println(refreshUserToGroupsMappings);
      System.out.println(refreshSuperUserGroupsConfiguration);
      System.out.println(help);
      System.out.println();
      ToolRunner.printGenericCommandUsage(System.out);
    }

  }


  /**
   * Command to ask the namenode to finalize previously performed upgrade.
   * Usage: java DFSAdmin -finalizeUpgrade
   * @exception IOException 
   */
  public int finalizeUpgrade() throws IOException {
    int exitCode = -1;

    if (!(fs instanceof DistributedFileSystem)) {
      System.out.println("FileSystem is " + fs.getUri());
      return exitCode;
    }

    DistributedFileSystem dfs = (DistributedFileSystem) fs;
    dfs.finalizeUpgrade();
    exitCode = 0;
   
    return exitCode;
  }

  /**
   * Command to request current distributed upgrade status, 
   * a detailed status, or to force the upgrade to proceed.
   * 
   * Usage: java DFSAdmin -upgradeProgress [status | details | force]
   * @exception IOException 
   */
  public int upgradeProgress(String[] argv, int idx) throws IOException {
    if (!(fs instanceof DistributedFileSystem)) {
      System.out.println("FileSystem is " + fs.getUri());
      return -1;
    }
    if (idx != argv.length - 1) {
      printUsage("-upgradeProgress");
      return -1;
    }

    UpgradeAction action;
    if ("status".equalsIgnoreCase(argv[idx])) {
      action = UpgradeAction.GET_STATUS;
    } else if ("details".equalsIgnoreCase(argv[idx])) {
      action = UpgradeAction.DETAILED_STATUS;
    } else if ("force".equalsIgnoreCase(argv[idx])) {
      action = UpgradeAction.FORCE_PROCEED;
    } else {
      printUsage("-upgradeProgress");
      return -1;
    }

    DistributedFileSystem dfs = (DistributedFileSystem) fs;
    UpgradeStatusReport status = dfs.distributedUpgradeProgress(action);
    String statusText = (status == null ? 
        "There are no upgrades in progress." :
          status.getStatusText(action == UpgradeAction.DETAILED_STATUS));
    System.out.println(statusText);
    return 0;
  }

  /**
   * Dumps DFS data structures into specified file.
   * Usage: java DFSAdmin -metasave filename
   * @param argv List of of command line parameters.
   * @param idx The index of the command that is being processed.
   * @exception IOException if an error accoured wile accessing
   *            the file or path.
   */
  public int metaSave(String[] argv, int idx) throws IOException {
    String pathname = argv[idx];
    DistributedFileSystem dfs = (DistributedFileSystem) fs;
    dfs.metaSave(pathname);
    System.out.println("Created file " + pathname + " on server " +
                       dfs.getUri());
    return 0;
  }

  private static UserGroupInformation getUGI() 
  throws IOException {
    return UserGroupInformation.getCurrentUser();
  }

  /**
   * Refresh the authorization policy on the {@link NameNode}.
   * @return exitcode 0 on success, non-zero on failure
   * @throws IOException
   */
  public int refreshServiceAcl() throws IOException {
    // Get the current configuration
    Configuration conf = getConf();
    
    // for security authorization
    // server principal for this call   
    // should be NN's one.
    conf.set(CommonConfigurationKeys.HADOOP_SECURITY_SERVICE_USER_NAME_KEY, 
        conf.get(DFSConfigKeys.DFS_NAMENODE_USER_NAME_KEY, ""));
    
    
    // Create the client
    RefreshAuthorizationPolicyProtocol refreshProtocol = 
      (RefreshAuthorizationPolicyProtocol) 
      RPC.getProxy(RefreshAuthorizationPolicyProtocol.class, 
                   RefreshAuthorizationPolicyProtocol.versionID, 
                   NameNode.getAddress(conf), getUGI(), conf,
                   NetUtils.getSocketFactory(conf, 
                                             RefreshAuthorizationPolicyProtocol.class));
    
    // Refresh the authorization policy in-effect
    
    refreshProtocol.refreshServiceAcl();
    
    return 0;
  }
  
  /**
   * Refresh the user-to-groups mappings on the {@link NameNode}.
   * @return exitcode 0 on success, non-zero on failure
   * @throws IOException
   */
  public int refreshUserToGroupsMappings() throws IOException {
    // Get the current configuration
    Configuration conf = getConf();
    
    // for security authorization
    // server principal for this call 
    // should be NAMENODE's one.
    conf.set(CommonConfigurationKeys.HADOOP_SECURITY_SERVICE_USER_NAME_KEY, 
        conf.get(DFSConfigKeys.DFS_NAMENODE_USER_NAME_KEY, ""));
    
    // Create the client
    RefreshUserMappingsProtocol refreshProtocol = 
      (RefreshUserMappingsProtocol) 
      RPC.getProxy(RefreshUserMappingsProtocol.class, 
                   RefreshUserMappingsProtocol.versionID, 
                   NameNode.getAddress(conf), getUGI(), conf,
                   NetUtils.getSocketFactory(conf, 
                                             RefreshUserMappingsProtocol.class));
    
    // Refresh the user-to-groups mappings
    refreshProtocol.refreshUserToGroupsMappings();
    
    return 0;
  }

  /**
   * refreshSuperUserGroupsConfiguration {@link NameNode}.
   * @return exitcode 0 on success, non-zero on failure
   * @throws IOException
   */
  public int refreshSuperUserGroupsConfiguration() throws IOException {
    // Get the current configuration
    Configuration conf = getConf();
    
    // for security authorization
    // server principal for this call 
    // should be NAMENODE's one.
    conf.set(CommonConfigurationKeys.HADOOP_SECURITY_SERVICE_USER_NAME_KEY, 
        conf.get(DFSConfigKeys.DFS_NAMENODE_USER_NAME_KEY, ""));
    
    // Create the client
    RefreshUserMappingsProtocol refreshProtocol = 
      (RefreshUserMappingsProtocol) 
      RPC.getProxy(RefreshUserMappingsProtocol.class, 
                   RefreshUserMappingsProtocol.versionID, 
                   NameNode.getAddress(conf), getUGI(), conf,
                   NetUtils.getSocketFactory(conf, 
                       RefreshUserMappingsProtocol.class));
    
    // Refresh the user-to-groups mappings
    refreshProtocol.refreshSuperUserGroupsConfiguration();
    
    return 0;
  }
  

  /**
   * Displays format of commands.
   * @param cmd The command that is being executed.
   */
  private static void printUsage(String cmd) {
    if ("-report".equals(cmd)) {
      System.err.println("Usage: java DFSAdmin"
                         + " [-report]");
    } else if ("-safemode".equals(cmd)) {
      System.err.println("Usage: java DFSAdmin"
                         + " [-safemode enter | leave | get | wait]");
    } else if ("-saveNamespace".equals(cmd)) {
      System.err.println("Usage: java DFSAdmin"
                         + " [-saveNamespace]");
    } else if ("-refreshNodes".equals(cmd)) {
      System.err.println("Usage: java DFSAdmin"
                         + " [-refreshNodes]");
    } else if ("-finalizeUpgrade".equals(cmd)) {
      System.err.println("Usage: java DFSAdmin"
                         + " [-finalizeUpgrade]");
    } else if ("-upgradeProgress".equals(cmd)) {
      System.err.println("Usage: java DFSAdmin"
                         + " [-upgradeProgress status | details | force]");
    } else if ("-metasave".equals(cmd)) {
      System.err.println("Usage: java DFSAdmin"
          + " [-metasave filename]");
    } else if (SetQuotaCommand.matches(cmd)) {
      System.err.println("Usage: java DFSAdmin"
                         + " [" + SetQuotaCommand.USAGE+"]");
    } else if (ClearQuotaCommand.matches(cmd)) {
      System.err.println("Usage: java DFSAdmin"
                         + " ["+ClearQuotaCommand.USAGE+"]");
    } else if (SetSpaceQuotaCommand.matches(cmd)) {
      System.err.println("Usage: java DFSAdmin"
                         + " [" + SetSpaceQuotaCommand.USAGE+"]");
    } else if (ClearSpaceQuotaCommand.matches(cmd)) {
      System.err.println("Usage: java DFSAdmin"
                         + " ["+ClearSpaceQuotaCommand.USAGE+"]");
    } else if ("-refreshServiceAcl".equals(cmd)) {
      System.err.println("Usage: java DFSAdmin"
                         + " [-refreshServiceAcl]");
    } else if ("-refreshUserToGroupsMappings".equals(cmd)) {
      System.err.println("Usage: java DFSAdmin"
                         + " [-refreshUserToGroupsMappings]");
    } else if ("-refreshSuperUserGroupsConfiguration".equals(cmd)) {
      System.err.println("Usage: java DFSAdmin"
                         + " [-refreshSuperUserGroupsConfiguration]");
    } else {
      System.err.println("Usage: java DFSAdmin");
      System.err.println("           [-report]");
      System.err.println("           [-safemode enter | leave | get | wait]");
      System.err.println("           [-saveNamespace]");
      System.err.println("           [-refreshNodes]");
      System.err.println("           [-finalizeUpgrade]");
      System.err.println("           [-upgradeProgress status | details | force]");
      System.err.println("           [-metasave filename]");
      System.err.println("           [-refreshServiceAcl]");
      System.err.println("           [-refreshUserToGroupsMappings]");
      System.err.println("           [-refreshSuperUserGroupsConfiguration]");
      System.err.println("           ["+SetQuotaCommand.USAGE+"]");
      System.err.println("           ["+ClearQuotaCommand.USAGE+"]");
      System.err.println("           ["+SetSpaceQuotaCommand.USAGE+"]");
      System.err.println("           ["+ClearSpaceQuotaCommand.USAGE+"]");
      System.err.println("           [-help [cmd]]");
      System.err.println();
      ToolRunner.printGenericCommandUsage(System.err);
    }
  }

  /**
   * @param argv The parameters passed to this program.
   * @exception Exception if the filesystem does not exist.
   * @return 0 on success, non zero on error.
   */
  @Override
  public int run(String[] argv) throws Exception {

    if (argv.length < 1) {
      printUsage("");
      return -1;
    }

    int exitCode = -1;
    int i = 0;
    String cmd = argv[i++];

    //
    // verify that we have enough command line parameters
    //
    if ("-safemode".equals(cmd)) {
      if (argv.length != 2) {
        printUsage(cmd);
        return exitCode;
      }
    } else if ("-report".equals(cmd)) {
      if (argv.length != 1) {
        printUsage(cmd);
        return exitCode;
      }
    } else if ("-saveNamespace".equals(cmd)) {
      if (argv.length != 1) {
        printUsage(cmd);
        return exitCode;
      }
    } else if ("-refreshNodes".equals(cmd)) {
      if (argv.length != 1) {
        printUsage(cmd);
        return exitCode;
      }
    } else if ("-finalizeUpgrade".equals(cmd)) {
      if (argv.length != 1) {
        printUsage(cmd);
        return exitCode;
      }
    } else if ("-upgradeProgress".equals(cmd)) {
        if (argv.length != 2) {
          printUsage(cmd);
          return exitCode;
        }
    } else if ("-metasave".equals(cmd)) {
      if (argv.length != 2) {
        printUsage(cmd);
        return exitCode;
      }
    } else if ("-refreshServiceAcl".equals(cmd)) {
      if (argv.length != 1) {
        printUsage(cmd);
        return exitCode;
      }
    } else if ("-refreshUserToGroupsMappings".equals(cmd)) {
      if (argv.length != 1) {
        printUsage(cmd);
        return exitCode;
      }
    }
    
    // initialize DFSAdmin
    try {
      init();
    } catch (RPC.VersionMismatch v) {
      System.err.println("Version Mismatch between client and server"
                         + "... command aborted.");
      return exitCode;
    } catch (IOException e) {
      System.err.println("Bad connection to DFS... command aborted.");
      return exitCode;
    }

    exitCode = 0;
    try {
      if ("-report".equals(cmd)) {
        report();
      } else if ("-safemode".equals(cmd)) {
        setSafeMode(argv, i);
      } else if ("-saveNamespace".equals(cmd)) {
        exitCode = saveNamespace();
      } else if ("-refreshNodes".equals(cmd)) {
        exitCode = refreshNodes();
      } else if ("-finalizeUpgrade".equals(cmd)) {
        exitCode = finalizeUpgrade();
      } else if ("-upgradeProgress".equals(cmd)) {
        exitCode = upgradeProgress(argv, i);
      } else if ("-metasave".equals(cmd)) {
        exitCode = metaSave(argv, i);
      } else if (ClearQuotaCommand.matches(cmd)) {
        exitCode = new ClearQuotaCommand(argv, i, fs).runAll();
      } else if (SetQuotaCommand.matches(cmd)) {
        exitCode = new SetQuotaCommand(argv, i, fs).runAll();
      } else if (ClearSpaceQuotaCommand.matches(cmd)) {
        exitCode = new ClearSpaceQuotaCommand(argv, i, fs).runAll();
      } else if (SetSpaceQuotaCommand.matches(cmd)) {
        exitCode = new SetSpaceQuotaCommand(argv, i, fs).runAll();
      } else if ("-refreshServiceAcl".equals(cmd)) {
        exitCode = refreshServiceAcl();
      } else if ("-refreshUserToGroupsMappings".equals(cmd)) {
        exitCode = refreshUserToGroupsMappings();
      } else if ("-refreshSuperUserGroupsConfiguration".equals(cmd)) {
        exitCode = refreshSuperUserGroupsConfiguration();
      } else if ("-help".equals(cmd)) {
        if (i < argv.length) {
          printHelp(argv[i]);
        } else {
          printHelp("");
        }
      } else {
        exitCode = -1;
        System.err.println(cmd.substring(1) + ": Unknown command");
        printUsage("");
      }
    } catch (IllegalArgumentException arge) {
      exitCode = -1;
      System.err.println(cmd.substring(1) + ": " + arge.getLocalizedMessage());
      printUsage(cmd);
    } catch (RemoteException e) {
      //
      // This is a error returned by hadoop server. Print
      // out the first line of the error mesage, ignore the stack trace.
      exitCode = -1;
      try {
        String[] content;
        content = e.getLocalizedMessage().split("\n");
        System.err.println(cmd.substring(1) + ": "
                           + content[0]);
      } catch (Exception ex) {
        System.err.println(cmd.substring(1) + ": "
                           + ex.getLocalizedMessage());
      }
    } catch (Exception e) {
      exitCode = -1;
      System.err.println(cmd.substring(1) + ": "
                         + e.getLocalizedMessage());
    } 
    return exitCode;
  }

  /**
   * main() has some simple utility methods.
   * @param argv Command line parameters.
   * @exception Exception if the filesystem does not exist.
   */
  public static void main(String[] argv) throws Exception {
    int res = ToolRunner.run(new DFSAdmin(), argv);
    System.exit(res);
  }
}
