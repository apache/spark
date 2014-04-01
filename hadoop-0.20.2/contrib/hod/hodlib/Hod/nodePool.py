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
"""defines nodepool and nodeset as abstract interface for batch system"""
# -*- python -*-

from hodlib.GridServices.service import *

class NodeSet:
  """a set of nodes as one allocation unit"""

  PENDING, COMMITTED, COMPLETE = range(3)

  def __init__(self, id, numNodes, preferredList, isPreemptee):
    self.id = id
    self.numNodes = numNodes
    self.isPreemptee = isPreemptee
    self.preferredList = preferredList
    self.cmdDescSet = []

  def getId(self):
    """returns a unique id of the nodeset"""
    return self.id

  def registerCommand(self, cmdDesc):
    """register a command to the nodeset"""
    self.cmdDescSet.append(cmdDesc)

  def getAddrList(self):
    """get list of node host names
    May return empty list if node set is not allocated yet"""
    raise NotImplementedError

  def _getNumNodes(self):
    return self.numNodes

  def _isPreemptee(self):
    return self.isPreemptee

  def _getPreferredList(self):
    return self.preferredList

  def _getCmdSet(self):
    return self.cmdDescSet

class NodePool:
  """maintains a collection of node sets as they get allocated.
  Also the base class for all kinds of nodepools. """

  def __init__(self, nodePoolDesc, cfg, log):
    self.nodePoolDesc = nodePoolDesc
    self.nodeSetDict = {}
    self._cfg = cfg
    self.nextNodeSetId = 0
    self._log = log
    

  def newNodeSet(self, numNodes, preferred=[], isPreemptee=True, id=None):
    """create a nodeset possibly with asked properties"""
    raise NotImplementedError

  def submitNodeSet(self, nodeSet, walltime = None, qosLevel = None, 
                    account = None, resourcelist = None):
    """submit the nodeset request to nodepool
    return False if error happened"""
    raise NotImplementedError

  def pollNodeSet(self, nodeSet):
    """return status of node set"""
    raise NotImplementedError

  def getWorkers(self):
    """return the hosts that comprise this nodepool"""
    raise NotImplementedError

  def runWorkers(self, nodeSet = None, args = []):
    """Run node set workers."""
    
    raise NotImplementedError
  
  def freeNodeSet(self, nodeset):
    """free a node set"""
    raise NotImplementedError

  def finalize(self):
    """cleans up all nodesets"""
    raise NotImplementedError

  def getServiceId(self):
    raise NotImplementedError
 
  def getJobInfo(self, jobId=None):
    raise NotImplementedError

  def deleteJob(self, jobId):
    """Delete a job, given it's id"""
    raise NotImplementedError

  def isJobFeasible(self):
    """Check if job can run by looking at any user/job limits"""
    raise NotImplementedError

  def updateWorkerInfo(self, workerInfoMap, jobId):
    """Update information about the workers started by this NodePool."""
    raise NotImplementedError

  def getAccountString(self):
    """Return the account string for this job"""
    raise NotImplementedError

  def getNextNodeSetId(self):
    id = self.nextNodeSetId
    self.nextNodeSetId += 1
    
    return id  
  
