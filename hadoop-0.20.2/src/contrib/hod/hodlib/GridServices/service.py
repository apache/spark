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
"""defines Service as abstract interface"""

# -*- python -*-
import random, socket

class Service:
  """ the service base class that all the 
  other services inherit from. """
  def __init__(self, serviceDesc, workDirs):
    self.serviceDesc = serviceDesc
    self.workDirs = workDirs

  def getName(self):
    return self.serviceDesc.getName()

  def getInfoAddrs(self):
    """Return a list of addresses that provide 
    information about the servie"""
    return []

  def isLost(self):
    """True if the service is down"""
    raise NotImplementedError

  def addNodes(self, nodeList):
    """add nodeSet"""
    raise NotImplementedError

  def removeNodes(self, nodeList):
    """remove a nodeset"""
    raise NotImplementedError

  def getWorkers(self):
     raise NotImplementedError

  def needsMore(self):
    """return number of nodes the service wants to add"""
    raise NotImplementedError

  def needsLess(self):
    """return number of nodes the service wants to remove"""
    raise NotImplementedError

class MasterSlave(Service):
  """ the base class for a master slave 
  service architecture. """
  def __init__(self, serviceDesc, workDirs,requiredNode):
    Service.__init__(self, serviceDesc, workDirs)
    self.launchedMaster = False
    self.masterInitialized = False
    self.masterAddress = 'none'
    self.requiredNode = requiredNode
    self.failedMsg = None
    self.masterFailureCount = 0

  def getRequiredNode(self):
    return self.requiredNode
 
  def getMasterRequest(self):
    """ the number of master you need
    to run for this service. """
    raise NotImplementedError
  
  def isLaunchable(self, serviceDict):
    """ if your service does not depend on
    other services. is set to true by default. """
    return True
  
  def getMasterCommands(self, serviceDict):
    """ a list of master commands you 
    want to run for this service. """
    raise NotImplementedError

  def getAdminCommands(self, serviceDict):
    """ a list of admin commands you 
    want to run for this service. """
    raise NotImplementedError

  def getWorkerCommands(self, serviceDict):
    """ a list of worker commands you want to 
    run for this service. """
    raise NotImplementedError

  def setMasterNodes(self, list):
    """ set the status of master nodes 
    after they start running on a node cluster. """
    raise NotImplementedError

  def addNodes(self, list):
    """ add nodes to a service. Not implemented
    currently. """
    raise NotImplementedError

  def getMasterAddrs(self):
    """ return the addresses of master. the 
    hostname:port to which worker nodes should
    connect. """
    raise NotImplementedError
  
  def setMasterParams(self, list):
    """ set the various master params 
    depending on what each hodring set 
    the master params to. """
    raise NotImplementedError

  def setlaunchedMaster(self):
    """ set the status of master launched
    to true. """
    self.launchedMaster = True

  def isMasterLaunched(self):
    """ return if a master has been launched
    for the service or not. """
    return self.launchedMaster

  def isMasterInitialized(self):
    """ return if a master if launched 
    has been initialized or not. """
    return self.masterInitialized

  def setMasterInitialized(self):
    """ set the master initialized to
    true. """
    self.masterInitialized = True
    # Reset failure related variables, as master is initialized successfully.
    self.masterFailureCount = 0
    self.failedMsg = None

  def getMasterAddress(self):
    """ it needs to change to reflect 
    more that one masters. Currently it 
    keeps a knowledge of where the master 
    was launched and to keep track if it was actually
    up or not. """
    return self.masterAddress

  def setMasterAddress(self, addr):
    self.masterAddress = addr

  def isExternal(self):
    return self.serviceDesc.isExternal()

  def setMasterFailed(self, err):
    """Sets variables related to Master failure"""
    self.masterFailureCount += 1
    self.failedMsg = err
    # When command is sent to HodRings, this would have been set to True.
    # Reset it to reflect the correct status.
    self.launchedMaster = False

  def getMasterFailed(self):
    return self.failedMsg
 
  def getMasterFailureCount(self):
    return self.masterFailureCount
 
class NodeRequest:
  """ A class to define 
  a node request. """
  def __init__(self, n, required = [], preferred = [], isPreemptee = True):
    self.numNodes = n
    self.preferred = preferred
    self.isPreemptee = isPreemptee
    self.required = required

  def setNumNodes(self, n):
    self.numNodes = n

  def setPreferredList(self, list):
    self.preferred = list

  def setIsPreemptee(self, flag):
    self.isPreemptee = flag


class ServiceUtil:
  """ this class should be moved out of 
  service.py to a util file"""
  localPortUsed = {}
    
  def getUniqRandomPort(h=None, low=50000, high=60000, retry=900, log=None):
    """This allocates a randome free port between low and high"""
    # We use a default value of 900 retries, which takes an agreeable
    # time limit of ~ 6.2 seconds to check 900 ports, in the worse case
    # of no available port in those 900.

    while retry > 0:
      n = random.randint(low, high)
      if n in ServiceUtil.localPortUsed:
        continue
      s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
      if not h:
        h = socket.gethostname()
      avail = False
      if log: log.debug("Trying to see if port %s is available"% n)
      try:
        s.bind((h, n))
        if log: log.debug("Yes, port %s is available" % n)
        avail = True
      except socket.error,e:
        if log: log.debug("Could not bind to the port %s. Reason %s" % (n,e))
        retry -= 1
        pass
      # The earlier code that used to be here had syntax errors. The code path
      # couldn't be followd anytime, so the error remained uncaught.
      # This time I stumbled upon the error
      s.close()

      if avail:
        ServiceUtil.localPortUsed[n] = True
        return n
    raise ValueError, "Can't find unique local port between %d and %d" % (low, high)
  
  getUniqRandomPort = staticmethod(getUniqRandomPort)
  
  def getUniqPort(h=None, low=40000, high=60000, retry=900, log=None):
    """get unique port on a host that can be used by service
    This and its consumer code should disappear when master
    nodes get allocatet by nodepool"""

    # We use a default value of 900 retries, which takes an agreeable
    # time limit of ~ 6.2 seconds to check 900 ports, in the worse case
    # of no available port in those 900.

    n  = low
    while retry > 0:
      n = n + 1
      if n in ServiceUtil.localPortUsed:
        continue
      s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
      if not h:
        h = socket.gethostname()
      avail = False
      if log: log.debug("Trying to see if port %s is available"% n)
      try:
        s.bind((h, n))
        if log: log.debug("Yes, port %s is available" % n)
        avail = True
      except socket.error,e:
        if log: log.debug("Could not bind to the port %s. Reason %s" % (n,e))
        retry -= 1
        pass
      s.close()

      if avail:
        ServiceUtil.localPortUsed[n] = True
        return n

    raise ValueError, "Can't find unique local port between %d and %d" % (low, high)

  getUniqPort = staticmethod(getUniqPort)
