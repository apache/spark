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
"""Maui/Torque implementation of NodePool"""
# -*- python -*-

import os, sys, csv, socket, time, re, pprint

from hodlib.Hod.nodePool import *
from hodlib.Schedulers.torque import torqueInterface
from hodlib.Common.threads import simpleCommand
from hodlib.Common.util import get_exception_string, args_to_string, local_fqdn, \
                        TORQUE_USER_LIMITS_COMMENT_FIELD

class TorqueNodeSet(NodeSet):
  def __init__(self, id, numNodes, preferredList, isPreemptee):
    NodeSet.__init__(self, id, numNodes, preferredList, isPreemptee)
    self.qsubId = None
    self.addrList = []

  def _setQsubId(self, qsubId):
    self.qsubId = qsubId

  def _setAddrList(self, addrList):
    self.addrList = addrList

  def getAddrList(self):
    return self.addrList

class TorquePool(NodePool):
  def __init__(self, nodePoolDesc, cfg, log):
    NodePool.__init__(self, nodePoolDesc, cfg, log)

    environ = os.environ.copy()
    
    if self._cfg['resource_manager'].has_key('pbs-server'):
      environ['PBS_DEFAULT'] = self._cfg['resource_manager']['pbs-server']

    self.__torque = torqueInterface(
      self._cfg['resource_manager']['batch-home'], environ, self._log)

  def getAccountString(self):
    account = ''
    if self._cfg['resource_manager'].has_key('pbs-account'):
      account = self._cfg['resource_manager']['pbs-account']
    return account

  def __gen_submit_params(self, nodeSet, walltime = None, qosLevel = None, 
                          account = None):
    argList = []
    stdinList = []
    
    npd = self.nodePoolDesc
    
    def gen_stdin_list():
      # Here we are basically generating the standard input for qsub.
      #  Specifically a script to exec ringmaster.
      stdinList.append('#!/bin/sh')
      
      ringBin = os.path.join(self._cfg['hod']['base-dir'], 'bin', 
                             'ringmaster')
      ringArgs = [ringBin,]
      ringArgs.extend(self._cfg.get_args(exclude=('hod')))
      
      ringMasterCommand = args_to_string(ringArgs)
      
      self._log.debug("ringmaster cmd: %s" % ringMasterCommand)
      
      stdinList.append(ringMasterCommand)
      
    def gen_arg_list():      
      def process_qsub_attributes():
        rawAttributes = self.nodePoolDesc.getAttrs()
    
        # 'W:x' is used to specify torque management extentensions ie -W x= ...
        resourceManagementExtensions = ''
        if 'W:x' in rawAttributes:
          resourceManagementExtensions = rawAttributes['W:x']
    
        if qosLevel:
          if len(resourceManagementExtensions) > 0:
            resourceManagementExtensions += ';'
          resourceManagementExtensions += 'QOS:%s' % (qosLevel)
    
        rawAttributes['W:x'] = resourceManagementExtensions
        
        hostname = local_fqdn()
   
        # key values are expected to have string values. 
        rawAttributes['l:nodes'] = "%s" % nodeSet._getNumNodes()
        
        if walltime:
          rawAttributes['l:walltime'] = "%s" % walltime
        
        #create a dict of dictionaries for 
        # various arguments of torque
        cmds = {}
        for key in rawAttributes:
          value = rawAttributes[key]
    
          if key.find(':') == -1:
            raise ValueError, 'Syntax error: missing colon after %s in %s=%s' % (
              key, key, value)
    
          [option, subOption] = key.split(':', 1)
          if not option in cmds:
            cmds[option] = {}
          cmds[option][subOption] = value
        
        opts = []
        #create a string from this
        #dictionary of dictionaries createde above
        for k in cmds:
          csv = []
          nv = cmds[k]
          for n in nv:
            v = nv[n]
            if len(n) == 0:
              csv.append(v)
            else:
              csv.append('%s=%s' % (n, v))
          opts.append('-%s' % (k))
          opts.append(','.join(csv))
    
        for option in cmds:
          commandList = []
          for subOption in cmds[option]:
            value = cmds[option][subOption]
            if len(subOption) == 0:
                commandList.append(value)
            else:
                commandList.append("%s=%s" % (subOption, value))
          opts.append('-%s' % option)
          opts.append(','.join(commandList))
          
        return opts
      
      pkgdir = npd.getPkgDir()
  
      qsub = os.path.join(pkgdir, 'bin', 'qsub')
      sdd = self._cfg['servicedesc']
      
      gsvc = None
      for key in sdd:
        gsvc = sdd[key]
        break
      
      argList.extend(process_qsub_attributes())

      argList.extend(('-N', '"' + self._cfg['hod']['title'] + '"'))
      argList.extend(('-r','n'))

      if 'pbs-user' in self._cfg['resource_manager']:
        argList.extend(('-u', self._cfg['resource_manager']['pbs-user']))
  
      argList.extend(('-d','/tmp/'))
      if 'queue' in self._cfg['resource_manager']:
        queue = self._cfg['resource_manager']['queue']
        argList.extend(('-q',queue))
  
      # In HOD 0.4, we pass in an account string only if it is mentioned.
      # Also, we don't append userid to the account string, as HOD jobs run as the 
      # user running them, not as 'HOD' user.
      if self._cfg['resource_manager'].has_key('pbs-account'):
        argList.extend(('-A', (self._cfg['resource_manager']['pbs-account'])))
    
      if 'env-vars' in self._cfg['resource_manager']:
        qsub_envs = self._cfg['resource_manager']['env-vars']
        argList.extend(('-v', self.__keyValToString(qsub_envs)))

    gen_arg_list()
    gen_stdin_list()
    
    return argList, stdinList
    
  def __keyValToString(self, keyValList):
    ret = ""
    for key in keyValList:
      ret = "%s%s=%s," % (ret, key, keyValList[key])
    return ret[:-1]
  
  def newNodeSet(self, numNodes, preferred=[], isPreemptee=True, id=None):
    if not id:
      id = self.getNextNodeSetId()
    
    nodeSet = TorqueNodeSet(id, numNodes, preferred, isPreemptee)

    self.nodeSetDict[nodeSet.getId()] = nodeSet
    
    return nodeSet
      
  def submitNodeSet(self, nodeSet, walltime = None, qosLevel = None, 
                    account = None):

    argList, stdinList = self.__gen_submit_params(nodeSet, walltime, qosLevel, 
                                                  account)
    
    jobId, exitCode = self.__torque.qsub(argList, stdinList)
    
    ## UNUSED CODE: LINE ##
    nodeSet.qsubId = jobId

    return jobId, exitCode

  def freeNodeSet(self, nodeSet):
    
    exitCode = self.deleteJob(nodeSet.getId())
    
    del self.nodeSetDict[nodeSet.getId()]
  
    return exitCode
  
  def finalize(self):
    status = 0
    exitCode = 0
    for nodeSet in self.nodeSetDict.values():
      exitCode = self.freeNodeSet(nodeSet)
      
    if exitCode > 0 and exitCode != 153:
      status = 4
      
    return status
    
  ## UNUSED METHOD ?? ##
  def getWorkers(self):
    hosts = []
    
    qstatInfo = self.__torque(self.getServiceId())
    if qstatInfo:
      hosts = qstatInfop['exec_host']
    
    return hosts
 
  ## UNUSED METHOD ?? ##
  def pollNodeSet(self, nodeSet):
    status = NodeSet.COMPLETE  
    nodeSet = self.nodeSetDict[0] 

    qstatInfo = self.__torque(self.getServiceId())

    if qstatMap:    
      jobstate = qstatMap['job_state']
      exechost = qstatMap['exec_host']

    if jobstate == 'Q':
      status = NodeSet.PENDING
    elif exechost == None:
      status = NodeSet.COMMITTED
    else:
      nodeSet._setAddrList(exec_host)

    return status
        
  def getServiceId(self):
    id = None
    
    nodeSets = self.nodeSetDict.values()
    if len(nodeSets):
      id = nodeSets[0].qsubId
      
    if id == None:
      id = os.getenv('PBS_JOBID')
      
    return id

  def getJobInfo(self, jobId=None):

    jobNonExistentErrorCode = 153
    self.__jobInfo = { 'job_state' : False }
    
    if jobId == None:
      jobId = self.getServiceId()

    qstatInfo, exitCode = self.__torque.qstat(jobId)
    if exitCode == 0:
      self.__jobInfo = qstatInfo
    elif exitCode == jobNonExistentErrorCode:
      # This really means that the job completed
      # However, setting only job_state for now, not 
      # any other attributes, as none seem required.
      self.__jobInfo = { 'job_state' : 'C' }

    return self.__jobInfo

  def deleteJob(self, jobId):
    exitCode = self.__torque.qdel(jobId)
    return exitCode

  def isJobFeasible(self):
    comment = None
    msg = None
    if self.__jobInfo.has_key('comment'):
      comment = self.__jobInfo['comment']
    try:
      if comment:
        commentField = re.compile(self._cfg['hod']['job-feasibility-attr'])
        match = commentField.search(comment)
        if match:
          reqUsage = int(match.group(1))
          currentUsage = int(match.group(2))
          maxUsage = int(match.group(3))
          msg = "Current Usage:%s, Requested:%s, Maximum Limit:%s " % \
                                  (currentUsage, reqUsage, maxUsage)
          if reqUsage > maxUsage:
            return "Never", msg
          if reqUsage + currentUsage > maxUsage:
            return False, msg
    except Exception, e:
      self._log.error("Error in isJobFeasible : %s" %e)
      raise Exception(e)
    return True, msg
    
  def runWorkers(self, args):
    return self.__torque.pbsdsh(args)

  def updateWorkerInfo(self, workerInfoMap, jobId):
    workerInfoStr = ''
    for key in workerInfoMap.keys():
      workerInfoStr = '%s,%s:%s' % (workerInfoStr, key, workerInfoMap[key])
    exitCode = self.__torque.qalter("notes", workerInfoStr[1:], jobId)
    return exitCode

