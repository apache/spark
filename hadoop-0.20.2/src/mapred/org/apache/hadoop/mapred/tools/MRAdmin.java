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
package org.apache.hadoop.mapred.tools;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.mapred.AdminOperationsProtocol;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobTracker;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.RefreshUserMappingsProtocol;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.RefreshAuthorizationPolicyProtocol;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Administrative access to Hadoop Map-Reduce.
 * 
 * Currently it only provides the ability to connect to the {@link JobTracker}
 * and 1) refresh the service-level authorization policy, 2) refresh queue acl
 * properties.
 */
public class MRAdmin extends Configured implements Tool {

  public MRAdmin() {
    super();
  }

  public MRAdmin(Configuration conf) {
    super(conf);
  }

  private static void printHelp(String cmd) {
    String summary = "hadoop mradmin is the command to execute Map-Reduce administrative commands.\n" +
    "The full syntax is: \n\n" +
    "hadoop mradmin [-refreshServiceAcl] [-refreshQueues] " +
    "[-refreshNodes] [-refreshUserToGroupsMappings] " +
    "[-refreshSuperUserGroupsConfiguration] [-help [cmd]]\n";

  String refreshServiceAcl = "-refreshServiceAcl: Reload the service-level authorization policy file\n" +
    "\t\tJobtracker will reload the authorization policy file.\n";

  String refreshQueues = "-refreshQueues: Reload the queue acls and state\n" +
    "\t\tJobTracker will reload the mapred-queues.xml file.\n";

  String refreshUserToGroupsMappings = 
    "-refreshUserToGroupsMappings: Refresh user-to-groups mappings\n";
  
  String refreshSuperUserGroupsConfiguration = 
    "-refreshSuperUserGroupsConfiguration: Refresh superuser proxy groups mappings\n";
  
  String refreshNodes =
    "-refreshNodes: Refresh the hosts information at the jobtracker.\n";
  
  String help = "-help [cmd]: \tDisplays help for the given command or all commands if none\n" +
    "\t\tis specified.\n";

  if ("refreshServiceAcl".equals(cmd)) {
    System.out.println(refreshServiceAcl);
  } else if ("refreshQueues".equals(cmd)) {
    System.out.println(refreshQueues);
  } else if ("refreshUserToGroupsMappings".equals(cmd)) {
    System.out.println(refreshUserToGroupsMappings);
  } else if ("refreshSuperUserGroupsConfiguration".equals(cmd)) {
    System.out.println(refreshSuperUserGroupsConfiguration);
  }  else if ("refreshNodes".equals(cmd)) {
    System.out.println(refreshNodes);
  } else if ("help".equals(cmd)) {
    System.out.println(help);
  } else {
    System.out.println(summary);
    System.out.println(refreshServiceAcl);
    System.out.println(refreshQueues);
    System.out.println(refreshUserToGroupsMappings);
    System.out.println(refreshSuperUserGroupsConfiguration);
    System.out.println(refreshNodes);
    System.out.println(help);
    System.out.println();
    ToolRunner.printGenericCommandUsage(System.out);
  }

}
  
  /**
   * Displays format of commands.
   * @param cmd The command that is being executed.
   */
  private static void printUsage(String cmd) {
    if ("-refreshServiceAcl".equals(cmd)) {
      System.err.println("Usage: java MRAdmin" + " [-refreshServiceAcl]");
    } else if ("-refreshQueues".equals(cmd)) {
      System.err.println("Usage: java MRAdmin" + " [-refreshQueues]");
    } else if ("-refreshUserToGroupsMappings".equals(cmd)) {
      System.err.println("Usage: java MRAdmin" + " [-refreshUserToGroupsMappings]");
    } else if ("-refreshSuperUserGroupsConfiguration".equals(cmd)) {
      System.err.println("Usage: java DFSAdmin"
                         + " [-refreshSuperUserGroupsConfiguration]");
    } else if ("-refreshNodes".equals(cmd)) {
      System.err.println("Usage: java MRAdmin" + " [-refreshNodes]");
    } else {
      System.err.println("Usage: java MRAdmin");
      System.err.println("           [-refreshServiceAcl]");
      System.err.println("           [-refreshQueues]");
      System.err.println("           [-refreshUserToGroupsMappings]");
      System.err.println("           [-refreshSuperUserGroupsConfiguration]");
      System.err.println("           [-refreshNodes]");
      System.err.println("           [-help [cmd]]");
      System.err.println();
      ToolRunner.printGenericCommandUsage(System.err);
    }
  }
  
  private static UserGroupInformation getUGI(Configuration conf
                                             ) throws IOException {
    return UserGroupInformation.getCurrentUser();
  }

  private int refreshAuthorizationPolicy() throws IOException {
    // Get the current configuration
    Configuration conf = getConf();
    
    // for security authorization
    // server principal for this call   
    // should be JT's one.
    JobConf jConf = new JobConf(conf);
    conf.set(CommonConfigurationKeys.HADOOP_SECURITY_SERVICE_USER_NAME_KEY, 
        jConf.get(JobTracker.JT_USER_NAME, ""));
    
    
    // Create the client
    RefreshAuthorizationPolicyProtocol refreshProtocol = 
      (RefreshAuthorizationPolicyProtocol) 
      RPC.getProxy(RefreshAuthorizationPolicyProtocol.class, 
                   RefreshAuthorizationPolicyProtocol.versionID, 
                   JobTracker.getAddress(conf), getUGI(conf), conf,
                   NetUtils.getSocketFactory(conf, 
                                             RefreshAuthorizationPolicyProtocol.class));
    
    // Refresh the authorization policy in-effect
    refreshProtocol.refreshServiceAcl();
    
    return 0;
  }

  private int refreshQueues() throws IOException {
    // Get the current configuration
    Configuration conf = getConf();
    
    // Create the client
    AdminOperationsProtocol adminOperationsProtocol = 
      (AdminOperationsProtocol) 
      RPC.getProxy(AdminOperationsProtocol.class, 
                   AdminOperationsProtocol.versionID, 
                   JobTracker.getAddress(conf), getUGI(conf), conf,
                   NetUtils.getSocketFactory(conf, 
                                             AdminOperationsProtocol.class));
    
    // Refresh the queue properties
    adminOperationsProtocol.refreshQueues();
    
    return 0;
  }

  /**
   * Command to ask the jobtracker to reread the hosts and excluded hosts 
   * file.
   * Usage: java MRAdmin -refreshNodes
   * @exception IOException 
   */
  private int refreshNodes() throws IOException {
    // Get the current configuration
    Configuration conf = getConf();
    
    // Create the client
    AdminOperationsProtocol adminOperationsProtocol = 
      (AdminOperationsProtocol) 
      RPC.getProxy(AdminOperationsProtocol.class, 
                   AdminOperationsProtocol.versionID, 
                   JobTracker.getAddress(conf), getUGI(conf), conf,
                   NetUtils.getSocketFactory(conf, 
                                             AdminOperationsProtocol.class));
    
    // Refresh the queue properties
    adminOperationsProtocol.refreshNodes();
    
    return 0;
  }

  
  /**
   * refreshSuperUserGroupsConfiguration {@link JobTracker}.
   * @return exitcode 0 on success, non-zero on failure
   * @throws IOException
   */
  public int refreshSuperUserGroupsConfiguration() throws IOException {
    // Get the current configuration
    Configuration conf = getConf();
    
    // for security authorization
    // server principal for this call   
    // should be JT's one.
    JobConf jConf = new JobConf(conf);
    conf.set(CommonConfigurationKeys.HADOOP_SECURITY_SERVICE_USER_NAME_KEY, 
        jConf.get(JobTracker.JT_USER_NAME, ""));
    
    // Create the client
    RefreshUserMappingsProtocol refreshProtocol = 
      (RefreshUserMappingsProtocol) 
      RPC.getProxy(RefreshUserMappingsProtocol.class, 
                   RefreshUserMappingsProtocol.versionID, 
                   JobTracker.getAddress(conf), getUGI(conf), conf,
                   NetUtils.getSocketFactory(conf, 
                       RefreshUserMappingsProtocol.class));
    
    // Refresh the user-to-groups mappings
    refreshProtocol.refreshSuperUserGroupsConfiguration();
    
    return 0;
  }
  
  /**
   * Refresh the user-to-groups mappings on the {@link JobTracker}.
   * @return exitcode 0 on success, non-zero on failure
   * @throws IOException
   */
  private int refreshUserToGroupsMappings() throws IOException {
    // Get the current configuration
    Configuration conf = getConf();

    // for security authorization
    // server principal for this call   
    // should be JT's one.
    JobConf jConf = new JobConf(conf);
    conf.set(CommonConfigurationKeys.HADOOP_SECURITY_SERVICE_USER_NAME_KEY, 
        jConf.get(JobTracker.JT_USER_NAME, ""));
    
    
    
    // Create the client
    RefreshUserMappingsProtocol refreshProtocol =
      (RefreshUserMappingsProtocol)
      RPC.getProxy(RefreshUserMappingsProtocol.class,
                   RefreshUserMappingsProtocol.versionID,
                   JobTracker.getAddress(conf), getUGI(conf), conf,
                   NetUtils.getSocketFactory(conf,
                                             RefreshUserMappingsProtocol.class));

    // Refresh the user-to-groups mappings
    refreshProtocol.refreshUserToGroupsMappings();

    return 0;
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 1) {
      printUsage("");
      return -1;
    }

    int exitCode = -1;
    int i = 0;
    String cmd = args[i++];

    //
    // verify that we have enough command line parameters
    //
    if ("-refreshServiceAcl".equals(cmd) || "-refreshQueues".equals(cmd)
        || "-refreshNodes".equals(cmd) ||
        "-refreshUserToGroupsMappings".equals(cmd) ||
        "-refreshSuperUserGroupsConfiguration".equals(cmd)
        ) {
      if (args.length != 1) {
        printUsage(cmd);
        return exitCode;
      }
    }
    
    exitCode = 0;
    try {
      if ("-refreshServiceAcl".equals(cmd)) {
        exitCode = refreshAuthorizationPolicy();
      } else if ("-refreshQueues".equals(cmd)) {
        exitCode = refreshQueues();
      } else if ("-refreshUserToGroupsMappings".equals(cmd)) {
        exitCode = refreshUserToGroupsMappings();
      } else if ("-refreshSuperUserGroupsConfiguration".equals(cmd)) {
        exitCode = refreshSuperUserGroupsConfiguration();
      } else if ("-refreshNodes".equals(cmd)) {
        exitCode = refreshNodes();
      } else if ("-help".equals(cmd)) {
        if (i < args.length) {
          printUsage(args[i]);
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

  public static void main(String[] args) throws Exception {
    int result = ToolRunner.run(new MRAdmin(), args);
    System.exit(result);
  }
}
