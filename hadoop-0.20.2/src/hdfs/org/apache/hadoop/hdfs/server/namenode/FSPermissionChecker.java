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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.permission.*;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;

/** Perform permission checking in {@link FSNamesystem}. */
class FSPermissionChecker {
  static final Log LOG = LogFactory.getLog(UserGroupInformation.class);

private final UserGroupInformation ugi;
  public final String user;
  private final Set<String> groups = new HashSet<String>();
  public final boolean isSuper;
  
  FSPermissionChecker(String fsOwner, String supergroup
      ) throws AccessControlException{
    try {
      ugi = UserGroupInformation.getCurrentUser();
    } catch (IOException e) {
      throw new AccessControlException(e); 
    } 

    groups.addAll(Arrays.asList(ugi.getGroupNames()));
    user = ugi.getShortUserName();

    isSuper = user.equals(fsOwner) || groups.contains(supergroup);
  }

  /**
   * Check if the callers group contains the required values.
   * @param group group to check
   */
  public boolean containsGroup(String group) {return groups.contains(group);}

  /**
   * Verify if the caller has the required permission. This will result into 
   * an exception if the caller is not allowed to access the resource.
   * @param owner owner of the system
   * @param supergroup supergroup of the system
   */
  public static void checkSuperuserPrivilege(UserGroupInformation owner, 
                                             String supergroup) 
                     throws AccessControlException {
    FSPermissionChecker checker = 
      new FSPermissionChecker(owner.getShortUserName(), supergroup);
    if (!checker.isSuper) {
      throw new AccessControlException("Access denied for user " 
          + checker.user + ". Superuser privilege is required");
    }
  }

  /**
   * Check whether current user have permissions to access the path.
   * Traverse is always checked.
   *
   * Parent path means the parent directory for the path.
   * Ancestor path means the last (the closest) existing ancestor directory
   * of the path.
   * Note that if the parent path exists,
   * then the parent path and the ancestor path are the same.
   *
   * For example, suppose the path is "/foo/bar/baz".
   * No matter baz is a file or a directory,
   * the parent path is "/foo/bar".
   * If bar exists, then the ancestor path is also "/foo/bar".
   * If bar does not exist and foo exists,
   * then the ancestor path is "/foo".
   * Further, if both foo and bar do not exist,
   * then the ancestor path is "/".
   *
   * @param doCheckOwner Require user to be the owner of the path?
   * @param ancestorAccess The access required by the ancestor of the path.
   * @param parentAccess The access required by the parent of the path.
   * @param access The access required by the path.
   * @param subAccess If path is a directory,
   * it is the access required of the path and all the sub-directories.
   * If path is not a directory, there is no effect.
   * @return a PermissionChecker object which caches data for later use.
   * @throws AccessControlException
   */
  void checkPermission(String path, INodeDirectory root, boolean doCheckOwner,
      FsAction ancestorAccess, FsAction parentAccess, FsAction access,
      FsAction subAccess) throws AccessControlException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("ACCESS CHECK: " + this
          + ", doCheckOwner=" + doCheckOwner
          + ", ancestorAccess=" + ancestorAccess
          + ", parentAccess=" + parentAccess
          + ", access=" + access
          + ", subAccess=" + subAccess);
    }
    // check if (parentAccess != null) && file exists, then check sb
    synchronized(root) {
      INode[] inodes = root.getExistingPathINodes(path);
      int ancestorIndex = inodes.length - 2;
      for(; ancestorIndex >= 0 && inodes[ancestorIndex] == null;
          ancestorIndex--);
      checkTraverse(inodes, ancestorIndex);

      if(parentAccess != null && parentAccess.implies(FsAction.WRITE)
          && inodes[inodes.length - 1] != null)
        checkStickyBit(inodes[inodes.length - 2], inodes[inodes.length - 1]);

      if (ancestorAccess != null && inodes.length > 1) {
        check(inodes, ancestorIndex, ancestorAccess);
      }
      if (parentAccess != null && inodes.length > 1) {
        check(inodes, inodes.length - 2, parentAccess);
      }
      if (access != null) {
        check(inodes[inodes.length - 1], access);
      }
      if (subAccess != null) {
        checkSubAccess(inodes[inodes.length - 1], subAccess);
      }
      if (doCheckOwner) {
        checkOwner(inodes[inodes.length - 1]);
      }
    }
  }

  private void checkOwner(INode inode) throws AccessControlException {
    if (inode != null && user.equals(inode.getUserName())) {
      return;
    }
    throw new AccessControlException("Permission denied");
  }

  private void checkTraverse(INode[] inodes, int last
      ) throws AccessControlException {
    for(int j = 0; j <= last; j++) {
      check(inodes[j], FsAction.EXECUTE);
    }
  }

  private void checkSubAccess(INode inode, FsAction access
      ) throws AccessControlException {
    if (inode == null || !inode.isDirectory()) {
      return;
    }

    Stack<INodeDirectory> directories = new Stack<INodeDirectory>();
    for(directories.push((INodeDirectory)inode); !directories.isEmpty(); ) {
      INodeDirectory d = directories.pop();
      check(d, access);

      for(INode child : d.getChildren()) {
        if (child.isDirectory()) {
          directories.push((INodeDirectory)child);
        }
      }
    }
  }

  private void check(INode[] inodes, int i, FsAction access
      ) throws AccessControlException {
    check(i >= 0? inodes[i]: null, access);
  }

  private void check(INode inode, FsAction access
      ) throws AccessControlException {
    if (inode == null) {
      return;
    }
    FsPermission mode = inode.getFsPermission();

    if (user.equals(inode.getUserName())) { //user class
      if (mode.getUserAction().implies(access)) { return; }
    }
    else if (groups.contains(inode.getGroupName())) { //group class
      if (mode.getGroupAction().implies(access)) { return; }
    }
    else { //other class
      if (mode.getOtherAction().implies(access)) { return; }
    }
    throw new AccessControlException("Permission denied: user=" + user
        + ", access=" + access + ", inode=" + inode);
  }

  private void checkStickyBit(INode parent, INode inode) throws AccessControlException {
    if(!parent.getFsPermission().getStickyBit()) {
      return;
    }

    // If this user is the directory owner, return
    if(parent.getUserName().equals(user)) {
      return;
    }

    // if this user is the file owner, return
    if(inode.getUserName().equals(user)) {
      return;
    }

    throw new AccessControlException("Permission denied by sticky bit setting:" +
      " user=" + user + ", inode=" + inode);
  }
}
