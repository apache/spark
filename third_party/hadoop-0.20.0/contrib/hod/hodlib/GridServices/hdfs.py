#Licensed to the Apache Software Foundation (ASF) under one
#or more contributor license agreements.  See the NOTICE file
#distributed with this work for additional information
#regarding copyright ownership.  The ASF licenses this file
#to you under the Apache License, Version 2.0 (the
#"License"); you may not use this file except in compliance
#with the License.  You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

#Unless required by applicable law or agreed to in writing, software
#distributed under the License is distributed on an "AS IS" BASIS,
#WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#See the License for the specific language governing permissions and
#limitations under the License.
"""define Hdfs as subclass of Service"""

# -*- python -*-

import os

from service import *
from hodlib.Hod.nodePool import *
from hodlib.Common.desc import CommandDesc
from hodlib.Common.util import get_exception_string, parseEquals

class HdfsExternal(MasterSlave):
  """dummy proxy to external HDFS instance"""

  def __init__(self, serviceDesc, workDirs, version):
    MasterSlave.__init__(self, serviceDesc, workDirs,None)
    self.launchedMaster = True
    self.masterInitialized = True
    self.version = version
    
  def getMasterRequest(self):
    return None

  def getMasterCommands(self, serviceDict):
    return []

  def getAdminCommands(self, serviceDict):
    return []

  def getWorkerCommands(self, serviceDict):
    return []

  def getMasterAddrs(self):
    attrs = self.serviceDesc.getfinalAttrs()
    addr = attrs['fs.default.name']
    return [addr]
  
  def setMasterParams(self, dict):
   self.serviceDesc.dict['final-attrs']['fs.default.name'] = "%s:%s" % \
     (dict['host'], dict['fs_port'])

   if self.version < 16:
    self.serviceDesc.dict['final-attrs']['dfs.info.port'] = \
                                    str(self.serviceDesc.dict['info_port'])
   else:
     # After Hadoop-2185
     self.serviceDesc.dict['final-attrs']['dfs.http.address'] = "%s:%s" % \
       (dict['host'], dict['info_port'])

  def getInfoAddrs(self):
    attrs = self.serviceDesc.getfinalAttrs()
    if self.version < 16:
      addr = attrs['fs.default.name']
      k,v = addr.split( ":")
      infoaddr = k + ':' + attrs['dfs.info.port']
    else:
      # After Hadoop-2185
      infoaddr = attrs['dfs.http.address']
    return [infoaddr]

class Hdfs(MasterSlave):

  def __init__(self, serviceDesc, nodePool, required_node, version, \
                                        format=True, upgrade=False,
                                        workers_per_ring = 1):
    MasterSlave.__init__(self, serviceDesc, nodePool, required_node)
    self.masterNode = None
    self.masterAddr = None
    self.runAdminCommands = True
    self.infoAddr = None
    self._isLost = False
    self.format = format
    self.upgrade = upgrade
    self.workers = []
    self.version = version
    self.workers_per_ring = workers_per_ring

  def getMasterRequest(self):
    req = NodeRequest(1, [], False)
    return req

  def getMasterCommands(self, serviceDict):

    masterCommands = []
    if self.format:
      masterCommands.append(self._getNameNodeCommand(True))

    if self.upgrade:
      masterCommands.append(self._getNameNodeCommand(False, True))
    else:
      masterCommands.append(self._getNameNodeCommand(False))

    return masterCommands

  def getAdminCommands(self, serviceDict):

    adminCommands = []
    if self.upgrade and self.runAdminCommands:
      adminCommands.append(self._getNameNodeAdminCommand('-safemode wait'))
      adminCommands.append(self._getNameNodeAdminCommand('-finalizeUpgrade',
                                                          True, True))

    self.runAdminCommands = False
    return adminCommands

  def getWorkerCommands(self, serviceDict):
    workerCmds = []
    for id in range(1, self.workers_per_ring + 1):
      workerCmds.append(self._getDataNodeCommand(str(id)))

    return workerCmds

  def setMasterNodes(self, list):
    node = list[0]
    self.masterNode = node
    
  def getMasterAddrs(self):
    return [self.masterAddr]

  def getInfoAddrs(self):
    return [self.infoAddr]

  def getWorkers(self):
    return self.workers

  def setMasterParams(self, list):
    dict = self._parseEquals(list)
    self.masterAddr = dict['fs.default.name']
    k,v = self.masterAddr.split( ":")
    self.masterNode = k
    if self.version < 16:
      self.infoAddr = self.masterNode + ':' + dict['dfs.info.port']
    else:
      # After Hadoop-2185
      self.infoAddr = dict['dfs.http.address']
   
  def _parseEquals(self, list):
    return parseEquals(list)
  
  def _setWorkDirs(self, workDirs, envs, attrs, parentDirs, subDir):
    namedir = None
    hadooptmpdir = None
    datadir = []

    for p in parentDirs:
      workDirs.append(p)
      workDirs.append(os.path.join(p, subDir))
      dir = os.path.join(p, subDir, 'dfs-data')
      datadir.append(dir)
      if not hadooptmpdir:
        # Not used currently, generating hadooptmpdir just in case
        hadooptmpdir = os.path.join(p, subDir, 'hadoop-tmp')

      if not namedir:
        namedir = os.path.join(p, subDir, 'dfs-name')

    workDirs.append(namedir)
    workDirs.extend(datadir)

    # FIXME!! use csv
    attrs['dfs.name.dir'] = namedir
    attrs['hadoop.tmp.dir'] = hadooptmpdir
    attrs['dfs.data.dir'] = ','.join(datadir)
    envs['HADOOP_ROOT_LOGGER'] = "INFO,DRFA"


  def _getNameNodeCommand(self, format=False, upgrade=False):
    sd = self.serviceDesc

    parentDirs = self.workDirs
    workDirs = []
    attrs = sd.getfinalAttrs().copy()
    envs = sd.getEnvs().copy()
    
    if 'fs.default.name' not in attrs:
      attrs['fs.default.name'] = 'fillinhostport'
 
    if self.version < 16:
     if 'dfs.info.port' not in attrs:
      attrs['dfs.info.port'] = 'fillinport'
    else:
      # Addressing Hadoop-2185, added the following. Earlier versions don't
      # care about this
      if 'dfs.http.address' not in attrs:
        attrs['dfs.http.address'] = 'fillinhostport'

    self._setWorkDirs(workDirs, envs, attrs, parentDirs, 'hdfs-nn')

    dict = { 'name' : 'namenode' }
    dict['program'] = os.path.join('bin', 'hadoop')
    argv = ['namenode']
    if format:
      argv.append('-format')
    elif upgrade:
      argv.append('-upgrade')
    dict['argv'] = argv
    dict['envs'] = envs
    dict['pkgdirs'] = sd.getPkgDirs()
    dict['workdirs'] = workDirs
    dict['final-attrs'] = attrs
    dict['attrs'] = sd.getAttrs()
    if format:
      dict['fg'] = 'true'
      dict['stdin'] = 'Y'
    cmd = CommandDesc(dict)
    return cmd

  def _getNameNodeAdminCommand(self, adminCommand, wait=True, ignoreFailures=False):
    sd = self.serviceDesc

    parentDirs = self.workDirs
    workDirs = []
    attrs = sd.getfinalAttrs().copy()
    envs = sd.getEnvs().copy()
    nn = self.masterAddr

    if nn == None:
      raise ValueError, "Can't get namenode address"

    attrs['fs.default.name'] = nn

    self._setWorkDirs(workDirs, envs, attrs, parentDirs, 'hdfs-nn')

    dict = { 'name' : 'dfsadmin' }
    dict['program'] = os.path.join('bin', 'hadoop')
    argv = ['dfsadmin']
    argv.append(adminCommand)
    dict['argv'] = argv
    dict['envs'] = envs
    dict['pkgdirs'] = sd.getPkgDirs()
    dict['workdirs'] = workDirs
    dict['final-attrs'] = attrs
    dict['attrs'] = sd.getAttrs()
    if wait:
      dict['fg'] = 'true'
      dict['stdin'] = 'Y'
    if ignoreFailures:
      dict['ignorefailures'] = 'Y'
    cmd = CommandDesc(dict)
    return cmd
 
  def _getDataNodeCommand(self, id):

    sd = self.serviceDesc

    parentDirs = self.workDirs
    workDirs = []
    attrs = sd.getfinalAttrs().copy()
    envs = sd.getEnvs().copy()
    nn = self.masterAddr

    if nn == None:
      raise ValueError, "Can't get namenode address"

    attrs['fs.default.name'] = nn

    if self.version < 16:
      if 'dfs.datanode.port' not in attrs:
        attrs['dfs.datanode.port'] = 'fillinport'
      if 'dfs.datanode.info.port' not in attrs:
        attrs['dfs.datanode.info.port'] = 'fillinport'
    else:
      # Adding the following. Hadoop-2185
      if 'dfs.datanode.address' not in attrs:
        attrs['dfs.datanode.address'] = 'fillinhostport'
      if 'dfs.datanode.http.address' not in attrs:
        attrs['dfs.datanode.http.address'] = 'fillinhostport'
    
    if self.version >= 18:
      # After HADOOP-3283
      # TODO: check for major as well as minor versions
      attrs['dfs.datanode.ipc.address'] = 'fillinhostport'
                    
    # unique workdirs in case of multiple datanodes per hodring
    pd = []
    for dir in parentDirs:
      dir = dir + "-" + id
      pd.append(dir)
    parentDirs = pd
    # end of unique workdirs

    self._setWorkDirs(workDirs, envs, attrs, parentDirs, 'hdfs-dn')

    dict = { 'name' : 'datanode' }
    dict['program'] = os.path.join('bin', 'hadoop')
    dict['argv'] = ['datanode']
    dict['envs'] = envs
    dict['pkgdirs'] = sd.getPkgDirs()
    dict['workdirs'] = workDirs
    dict['final-attrs'] = attrs
    dict['attrs'] = sd.getAttrs()
 
    cmd = CommandDesc(dict)
    return cmd

