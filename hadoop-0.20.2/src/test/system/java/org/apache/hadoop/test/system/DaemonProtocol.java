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

package org.apache.hadoop.test.system;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.fs.permission.FsPermission;

/**
 * RPC interface of a given Daemon.
 */
public interface DaemonProtocol extends VersionedProtocol{
  long versionID = 1L;

  /**
   * Returns the Daemon configuration.
   * @return Configuration
   * @throws IOException in case of errors
   */
  Configuration getDaemonConf() throws IOException;

  /**
   * Check if the Daemon is alive.
   * 
   * @throws IOException
   *           if Daemon is unreachable.
   */
  void ping() throws IOException;

  /**
   * Check if the Daemon is ready to accept RPC connections.
   * 
   * @return true if Daemon is ready to accept RPC connection.
   * @throws IOException in case of errors
   */
  boolean isReady() throws IOException;

  /**
   * Get system level view of the Daemon process.
   * 
   * @return returns system level view of the Daemon process.
   * 
   * @throws IOException in case of errors
   */
  ProcessInfo getProcessInfo() throws IOException;
  
  /**
   * Return a file status object that represents the path.
   * @param path
   *          given path
   * @param local
   *          whether the path is local or not
   * @return a FileStatus object
   * @throws FileNotFoundException when the path does not exist;
   *         IOException see specific implementation
   */
  FileStatus getFileStatus(String path, boolean local) throws IOException;

  /**
   * Create a file with given permissions in a file system.
   * @param path - source path where the file has to create.
   * @param fileName - file name.
   * @param permission - file permissions.
   * @param local - identifying the path whether its local or not.
   * @throws IOException - if an I/O error occurs.
   */
  void createFile(String path, String fileName, 
      FsPermission permission, boolean local) throws IOException;
   
  /**
   * Create a folder with given permissions in a file system.
   * @param path - source path where the file has to be creating.
   * @param folderName - folder name.
   * @param permission - folder permissions.
   * @param local - identifying the path whether its local or not.
   * @throws IOException - if an I/O error occurs.
   */
  public void createFolder(String path, String folderName, 
      FsPermission permission, boolean local) throws IOException;

  /**
   * List the statuses of the files/directories in the given path if the path is
   * a directory.
   * 
   * @param path
   *          given path
   * @param local
   *          whether the path is local or not
   * @return the statuses of the files/directories in the given patch
   * @throws IOException in case of errors
   */
  FileStatus[] listStatus(String path, boolean local) throws IOException;
  
  /**
   * Enables a particular control action to be performed on the Daemon <br/>
   * 
   * @param action is a control action  to be enabled.
   * 
   * @throws IOException in case of errors
   */
  @SuppressWarnings("unchecked")
  void sendAction(ControlAction action) throws IOException;
  
  /**
   * Checks if the particular control action has be delivered to the Daemon 
   * component <br/>
   * 
   * @param action to be checked.
   * 
   * @return true if action is still in waiting queue of 
   *          actions to be delivered.
   * @throws IOException in case of errors
   */
  @SuppressWarnings("unchecked")
  boolean isActionPending(ControlAction action) throws IOException;
  
  /**
   * Removes a particular control action from the list of the actions which the
   * daemon maintains. <br/>
   * <i><b>Not to be directly called by Test Case or clients.</b></i>
   * @param action to be removed
   * @throws IOException in case of errors
   */
  
  @SuppressWarnings("unchecked")
  void removeAction(ControlAction action) throws IOException;
  
  /**
   * Clears out the list of control actions on the particular daemon.
   * <br/>
   * @throws IOException in case of errors
   */
  void clearActions() throws IOException;
  
  /**
   * Gets a list of pending actions which are targeted on the specified key. 
   * <br/>
   * <i><b>Not to be directly used by clients</b></i>
   * @param key target
   * @return list of actions.
   * @throws IOException in case of errors
   */
  @SuppressWarnings("unchecked")
  ControlAction[] getActions(Writable key) throws IOException;

  /**
   * Gets the number of times a particular pattern has been found in the 
   * daemons log file.<br/>
   * <b><i>Please note that search spans across all previous messages of
   * Daemon, so better practice is to get previous counts before an operation
   * and then re-check if the sequence of action has caused any problems</i></b>
   * @param pattern to look for in the daemon's log file
   * @param list Exceptions that will be ignored from log file. 
   * @return number of times the pattern if found in log file.
   * @throws IOException in case of errors
   */
  int getNumberOfMatchesInLogFile(String pattern, String[] list) 
      throws IOException;

  /**
   * Gets the user who started the particular daemon initially. <br/>
   * 
   * @return user who started the particular daemon.
   * @throws IOException in case of errors
   */
  String getDaemonUser() throws IOException;
  
  /**
   * It uses for suspending the process.
   * @param pid process id.
   * @return true if the process is suspended otherwise false.
   * @throws IOException if an I/O error occurs.
   */
  boolean suspendProcess(String pid) throws IOException;

  /**
   * It uses for resuming the suspended process.
   * @param pid process id
   * @return true if suspended process is resumed otherwise false.
   * @throws IOException if an I/O error occurs.
   */
  boolean resumeProcess(String pid) throws IOException;

}
