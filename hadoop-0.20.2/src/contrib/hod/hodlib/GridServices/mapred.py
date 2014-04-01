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
"""define MapReduce as subclass of Service"""

# -*- python -*-

import os, copy, time

from service import *
from hodlib.Hod.nodePool import *
from hodlib.Common.desc import CommandDesc
from hodlib.Common.util import get_exception_string, parseEquals

class MapReduceExternal(MasterSlave):
  """dummy proxy to external MapReduce instance"""

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
    addr = attrs['mapred.job.tracker']
    return [addr]

  def needsMore(self):
    return 0

  def needsLess(self):
    return 0

  def setMasterParams(self, dict):
    self.serviceDesc['final-attrs']['mapred.job.tracker'] = "%s:%s" % (dict['host'], 
      dict['tracker_port'])
    
    if self.version < 16:
      self.serviceDesc.dict['final-attrs']['mapred.job.tracker.info.port'] = \
                                      str(self.serviceDesc.dict['info_port'])
    else:
      # After Hadoop-2185
      self.serviceDesc['final-attrs']['mapred.job.tracker.http.address'] = \
        "%s:%s" %(dict['host'], dict['info_port'])

  def getInfoAddrs(self):
    attrs = self.serviceDesc.getfinalAttrs()
    if self.version < 16:
      addr = attrs['mapred.job.tracker']
      k,v = addr.split( ":")
      infoaddr = k + ':' + attrs['mapred.job.tracker.info.port']
    else:
      # After Hadoop-2185
      # Note: earlier,we never respected mapred.job.tracker.http.address
      infoaddr = attrs['mapred.job.tracker.http.address']
    return [infoaddr]
  
class MapReduce(MasterSlave):

  def __init__(self, serviceDesc, workDirs,required_node, version,
                workers_per_ring = 1):
    MasterSlave.__init__(self, serviceDesc, workDirs,required_node)

    self.masterNode = None
    self.masterAddr = None
    self.infoAddr = None
    self.workers = []
    self.required_node = required_node
    self.version = version
    self.workers_per_ring = workers_per_ring

  def isLaunchable(self, serviceDict):
    hdfs = serviceDict['hdfs']
    if (hdfs.isMasterInitialized()):
      return True
    return False
  
  def getMasterRequest(self):
    req = NodeRequest(1, [], False)
    return req

  def getMasterCommands(self, serviceDict):

    hdfs = serviceDict['hdfs']

    cmdDesc = self._getJobTrackerCommand(hdfs)
    return [cmdDesc]

  def getAdminCommands(self, serviceDict):
    return []

  def getWorkerCommands(self, serviceDict):

    hdfs = serviceDict['hdfs']

    workerCmds = []
    for id in range(1, self.workers_per_ring + 1):
      workerCmds.append(self._getTaskTrackerCommand(str(id), hdfs))
      
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

  def requiredNode(self):
    return self.required_host

  def setMasterParams(self, list):
    dict = self._parseEquals(list)
    self.masterAddr = dict['mapred.job.tracker']
    k,v = self.masterAddr.split(":")
    self.masterNode = k
    if self.version < 16:
      self.infoAddr = self.masterNode + ':' + dict['mapred.job.tracker.info.port']
    else:
      # After Hadoop-2185
      self.infoAddr = dict['mapred.job.tracker.http.address']
  
  def _parseEquals(self, list):
    return parseEquals(list)

  def _setWorkDirs(self, workDirs, envs, attrs, parentDirs, subDir):
    local = []
    system = None
    temp = None
    hadooptmpdir = None
    dfsclient = []
    
    for p in parentDirs:
      workDirs.append(p)
      workDirs.append(os.path.join(p, subDir))
      dir = os.path.join(p, subDir, 'mapred-local')
      local.append(dir)
      if not system:
        system = os.path.join(p, subDir, 'mapred-system')
      if not temp:
        temp = os.path.join(p, subDir, 'mapred-temp')
      if not hadooptmpdir:
        # Not used currently, generating hadooptmpdir just in case
        hadooptmpdir = os.path.join(p, subDir, 'hadoop-tmp')
      dfsclientdir = os.path.join(p, subDir, 'dfs-client')
      dfsclient.append(dfsclientdir)
      workDirs.append(dfsclientdir)
    # FIXME!! use csv
    attrs['mapred.local.dir'] = ','.join(local)
    attrs['mapred.system.dir'] = 'fillindir'
    attrs['mapred.temp.dir'] = temp
    attrs['hadoop.tmp.dir'] = hadooptmpdir


    envs['HADOOP_ROOT_LOGGER'] = "INFO,DRFA"


  def _getJobTrackerCommand(self, hdfs):
    sd = self.serviceDesc

    parentDirs = self.workDirs
    workDirs = []
    attrs = sd.getfinalAttrs().copy()
    envs = sd.getEnvs().copy()

    if 'mapred.job.tracker' not in attrs:
      attrs['mapred.job.tracker'] = 'fillinhostport'

    if self.version < 16:
      if 'mapred.job.tracker.info.port' not in attrs:
        attrs['mapred.job.tracker.info.port'] = 'fillinport'
    else:
      # Addressing Hadoop-2185,
      if 'mapred.job.tracker.http.address' not in attrs:
        attrs['mapred.job.tracker.http.address'] = 'fillinhostport'

    attrs['fs.default.name'] = hdfs.getMasterAddrs()[0]

    self._setWorkDirs(workDirs, envs, attrs, parentDirs, 'mapred-jt')

    dict = { 'name' : 'jobtracker' }
    dict['version'] = self.version
    dict['program'] = os.path.join('bin', 'hadoop')
    dict['argv'] = ['jobtracker']
    dict['envs'] = envs
    dict['pkgdirs'] = sd.getPkgDirs()
    dict['workdirs'] = workDirs
    dict['final-attrs'] = attrs
    dict['attrs'] = sd.getAttrs()
    cmd = CommandDesc(dict)
    return cmd

  def _getTaskTrackerCommand(self, id, hdfs):

    sd = self.serviceDesc

    parentDirs = self.workDirs
    workDirs = []
    attrs = sd.getfinalAttrs().copy()
    envs = sd.getEnvs().copy()
    jt = self.masterAddr

    if jt == None:
      raise ValueError, "Can't get job tracker address"

    attrs['mapred.job.tracker'] = jt
    attrs['fs.default.name'] = hdfs.getMasterAddrs()[0]

    if self.version < 16:
      if 'tasktracker.http.port' not in attrs:
        attrs['tasktracker.http.port'] = 'fillinport'
      # earlier to 16, tasktrackers always took ephemeral port 0 for
      # tasktracker.report.bindAddress
    else:
      # Adding the following. Hadoop-2185
      if 'mapred.task.tracker.report.address' not in attrs:
        attrs['mapred.task.tracker.report.address'] = 'fillinhostport'
      if 'mapred.task.tracker.http.address' not in attrs:
        attrs['mapred.task.tracker.http.address'] = 'fillinhostport'

    # unique parentDirs in case of multiple tasktrackers per hodring
    pd = []
    for dir in parentDirs:
      dir = dir + "-" + id
      pd.append(dir)
    parentDirs = pd
    # end of unique workdirs

    self._setWorkDirs(workDirs, envs, attrs, parentDirs, 'mapred-tt')

    dict = { 'name' : 'tasktracker' }
    dict['program'] = os.path.join('bin', 'hadoop')
    dict['argv'] = ['tasktracker']
    dict['envs'] = envs
    dict['pkgdirs'] = sd.getPkgDirs()
    dict['workdirs'] = workDirs
    dict['final-attrs'] = attrs
    dict['attrs'] = sd.getAttrs()
    cmd = CommandDesc(dict)
    return cmd

