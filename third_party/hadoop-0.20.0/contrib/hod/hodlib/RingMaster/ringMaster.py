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
#!/usr/bin/env python
"""manages services and nodepool"""
# -*- python -*-

import os, sys, random, time, sets, shutil, threading
import urllib, urlparse, re, getpass, pprint, signal, shutil

from pprint import pformat
from HTMLParser import HTMLParser

binfile = sys.path[0]
libdir = os.path.dirname(binfile)
sys.path.append(libdir)

import hodlib.Common.logger
from hodlib.RingMaster.idleJobTracker import JobTrackerMonitor, HadoopJobStatus

from hodlib.Common.threads import func 

from hodlib.Hod.nodePool import *
from hodlib.Common.util import *
from hodlib.Common.nodepoolutil import NodePoolUtil
from hodlib.Common.socketServers import hodXMLRPCServer
from hodlib.Common.socketServers import threadedHTTPServer
from hodlib.NodePools import *
from hodlib.NodePools.torque import *
from hodlib.GridServices import *
from hodlib.Common.descGenerator import *
from hodlib.Common.xmlrpc import hodXRClient
from hodlib.Common.miniHTMLParser import miniHTMLParser
from hodlib.Common.threads import simpleCommand

class ringMasterServer:
  """The RPC server that exposes all the master config
  changes. Also, one of these RPC servers runs as a proxy
  and all the hodring instances register with this proxy"""
  instance = None
  xmlrpc = None
  
  def __init__(self, cfg, log, logMasterSources, retry=5):
    try:
      from hodlib.Common.socketServers import twistedXMLRPCServer
      ringMasterServer.xmlrpc = twistedXMLRPCServer("", 
        cfg['ringmaster']['xrs-port-range'])
    except ImportError:
      log.info("Twisted interface not found. Using hodXMLRPCServer.")
      ringMasterServer.xmlrpc = hodXMLRPCServer("", 
        cfg['ringmaster']['xrs-port-range'])

    ringMasterServer.xmlrpc.register_instance(logMasterSources)
    self.logMasterSources = logMasterSources
    ringMasterServer.xmlrpc.serve_forever()
        
    while not ringMasterServer.xmlrpc.is_alive():
      time.sleep(.5)
          
    log.debug('Ringmaster RPC Server at %d' % 
                 ringMasterServer.xmlrpc.server_address[1])
    
  def startService(ss, cfg, np, log, rm):
    logMasterSources = _LogMasterSources(ss, cfg, np, log, rm)
    ringMasterServer.instance = ringMasterServer(cfg, log, logMasterSources)

  def stopService():
    ringMasterServer.xmlrpc.stop()
  
  def getPort():
    return ringMasterServer.instance.port

  def getAddress():
    return 'http://%s:%d/' % (socket.gethostname(), 
                              ringMasterServer.xmlrpc.server_address[1])
  
  startService = staticmethod(startService)
  stopService = staticmethod(stopService)
  getPort = staticmethod(getPort)
  getAddress = staticmethod(getAddress)
  
class _LogMasterSources:
  """All the methods that are run by the RPC server are
  added into this class """
  
  def __init__(self, serviceDict, cfg, np, log, rm):
    self.serviceDict = serviceDict
    self.tarSource = []
    self.tarSourceLock = threading.Lock()
    self.dict = {}
    self.count = {}
    self.logsourceList = []
    self.logsourceListLock = threading.Lock()
    self.masterParam = []
    self.masterParamLock = threading.Lock()
    self.verify = 'none'
    self.cmdLock = threading.Lock()
    self.cfg = cfg
    self.log = log
    self.np = np
    self.rm = rm 
    self.hdfsHost = None
    self.mapredHost = None
    self.maxconnect = self.cfg['ringmaster']['max-connect']
    self.log.debug("Using max-connect value %s"%self.maxconnect)

   
  def registerTarSource(self, hostname, url, addr=None):
    self.log.debug("registering: " + url)
    lock = self.tarSourceLock
    lock.acquire()
    self.dict[url] = url
    self.count[url] = 0
    # addr is None when ringMaster himself invokes this method
    if addr:
      c = self.count[addr]
      self.count[addr] = c - 1
    lock.release()
    if addr:
      str = "%s is done" % (addr)
      self.log.debug(str)
    return url

  def getTarList(self,hodring):   # this looks useful
    lock = self.tarSourceLock
    lock.acquire()
    leastkey = None
    leastval = -1
    for k, v in self.count.iteritems():
      if (leastval  == -1):
        leastval = v
        pass
      if (v <= leastval and v < self.maxconnect):
        leastkey = k
        leastval = v
    if (leastkey == None):
      url  = 'none'
    else:
      url = self.dict[leastkey]
      self.count[leastkey] = leastval + 1
      self.log.debug("%s %d" % (leastkey, self.count[leastkey]))
    lock.release()
    self.log.debug('sending url ' + url+" to "+hodring)  # this looks useful
    return url

  def tarDone(self, uri):
    str = "%s is done" % (uri)
    self.log.debug(str)
    lock = self.tarSourceLock
    lock.acquire()
    c = self.count[uri]
    self.count[uri] = c - 1
    lock.release()
    return uri

  def status(self):
    return True

# FIXME: this code is broken, it relies on a central service registry
#
#  def clusterStart(self, changedClusterParams=[]):
#    self.log.debug("clusterStart method invoked.")
#    self.dict = {}
#    self.count = {}
#    try:
#      if (len(changedClusterParams) > 0):
#        self.log.debug("Updating config.")
#        for param in changedClusterParams:
#          (key, sep1, val) = param.partition('=')
#          (i1, sep2, i2) = key.partition('.')
#          try:
#            prev = self.cfg[i1][i2]
#            self.rm.cfg[i1][i2] = val
#            self.cfg[i1][i2] = val
#            self.log.debug("\nModified [%s][%s]=%s to [%s][%s]=%s" % (i1, i2, prev, i1, i2, val))
#          except KeyError, e:
#            self.log.info("Skipping %s as no such config parameter found in ringmaster" % param)
#        self.log.debug("Regenerating Service Description.")
#        dGen = DescGenerator(self.rm.cfg)
#        self.rm.cfg['servicedesc'] = dGen.createServiceDescDict()
#        self.cfg['servicedesc'] = self.rm.cfg['servicedesc']
#  
#      self.rm.tar = None
#      if self.rm.cfg['ringmaster'].has_key('hadoop-tar-ball'):
#        self.rm.download = True
#        self.rm.tar = self.rm.cfg['ringmaster']['hadoop-tar-ball']
#        self.log.debug("self.rm.tar=%s" % self.rm.tar)
# 
#      self.rm.cd_to_tempdir()
#
#      self.rm.tarAddress = None 
#      hostname = socket.gethostname()
#      if (self.rm.download):
#        self.rm.basename = os.path.basename(self.rm.tar)
#        dest = os.path.join(os.getcwd(), self.rm.basename)
#        src =  self.rm.tar  
#        self.log.debug("cp %s -> %s" % (src, dest))
#        shutil.copy(src, dest) 
#        self.rm.tarAddress = "%s%s" % (self.rm.httpAddress, self.rm.basename)
#        self.registerTarSource(hostname, self.rm.tarAddress)
#        self.log.debug("Registered new tarAddress %s" % self.rm.tarAddress)
#      else:
#        self.log.debug("Download not set.")
#      
#      if (self.rm.tar != None):
#        self.cfg['hodring']['download-addr'] = self.rm.tarAddress
#        self.rm.cfg['hodring']['download-addr'] = self.rm.tarAddress
#
#      sdl = self.rm.cfg['servicedesc']
#      workDirs = self.rm.getWorkDirs(self.rm.cfg, True)
#      hdfsDesc = sdl['hdfs']
#      hdfs = None
#      if hdfsDesc.isExternal():
#        hdfs = HdfsExternal(hdfsDesc, workDirs)
#      else:
#        hdfs = Hdfs(hdfsDesc, workDirs, 0, False, True)
#    
#      self.rm.serviceDict[hdfs.getName()] = hdfs
#      mrDesc = sdl['mapred']
#      mr = None
#      if mrDesc.isExternal():
#        mr = MapReduceExternal(mrDesc, workDirs)
#      else:
#        mr = MapReduce(mrDesc, workDirs, 1)
#      self.rm.serviceDict[mr.getName()] = mr
#
#      ringList = self.rm.serviceClient.getServiceInfo(self.cfg['hodring']['userid'],
#        self.np.getServiceId(), 'hodring', 'hod') 
#    
#      slaveList = ringList
#      hdfsringXRAddress = None
#      # Start HDFS Master - Step 1
#      if not hdfsDesc.isExternal():
#        masterFound = False
#        for ring in ringList:
#          ringXRAddress = ring['xrs']
#          if ringXRAddress == None:
#            raise Exception("Could not get hodring XML-RPC server address.")
#          if  (ringXRAddress.find(self.hdfsHost) != -1):
#            ringClient = hodXRClient(ringXRAddress, None, None, 0, 0, 0, False, 0)
#            hdfsringXRAddress = ringXRAddress
#            self.log.debug("Invoking clusterStart on " + ringXRAddress + " (HDFS Master)")
#            ringClient.clusterStart()
#            masterFound = True 
#            slaveList.remove(ring)
#            break
#        if not masterFound:
#          raise Exception("HDFS Master host not found")
#        while hdfs.getInfoAddrs() == None:
#          self.log.debug("Waiting for HDFS Master (Name Node) to register dfs.info.port")
#          time.sleep(1)
#
#      # Start MAPRED Master - Step 2
#      if not mrDesc.isExternal():
#        masterFound = False
#        for ring in ringList:
#          ringXRAddress = ring['xrs']
#          if ringXRAddress == None:
#            raise Exception("Could not get hodring XML-RPC server address.")
#          if (not mrDesc.isExternal() and ringXRAddress.find(self.mapredHost) != -1):
#            ringClient = hodXRClient(ringXRAddress, None, None, 0, 0, 0, False, 0)
#            self.log.debug("Invoking clusterStart on " + ringXRAddress + " (MAPRED Master)")
#            ringClient.clusterStart()
#            masterFound = True 
#            slaveList.remove(ring)
#            break
#        if not masterFound:
#          raise Excpetion("MAPRED Master host not found")
#        while mr.getInfoAddrs() == None:
#          self.log.debug("Waiting for MAPRED Master (Job Tracker) to register \
# mapred.job.tracker.info.port")
#          time.sleep(1)
#
#      # Start Slaves - Step 3 
#      for ring in slaveList:
#          ringXRAddress = ring['xrs']
#          if ringXRAddress == None:
#            raise Exception("Could not get hodring XML-RPC server address.")
#          ringClient = hodXRClient(ringXRAddress, None, None, 0, 0, 0, False, 0)
#          self.log.debug("Invoking clusterStart on " + ringXRAddress + " (Slaves)")
#          ringThread = func(name='hodring_slaves_start', functionRef=ringClient.clusterStart())
#          ring['thread'] = ringThread
#          ringThread.start()
#
#      for ring in slaveList:
#        ringThread = ring['thread']
#        if ringThread == None:
#          raise Exception("Could not get hodring thread (Slave).")
#        ringThread.join()
#        self.log.debug("Completed clusterStart on " + ring['xrs'] + " (Slave)")
#
#      # Run Admin Commands on HDFS Master - Step 4
#      if not hdfsDesc.isExternal():
#        if hdfsringXRAddress == None:
#          raise Exception("HDFS Master host not found (to Run Admin Commands)")
#        ringClient = hodXRClient(hdfsringXRAddress, None, None, 0, 0, 0, False, 0)
#        self.log.debug("Invoking clusterStart(False) - Admin on "
#                       + hdfsringXRAddress + " (HDFS Master)")
#        ringClient.clusterStart(False)
#
#    except:
#      self.log.debug(get_exception_string())
#      return False
#
#    self.log.debug("Successfully started cluster.")
#    return True
#
#  def clusterStop(self):
#    self.log.debug("clusterStop method invoked.")
#    try:
#      hdfsAddr = self.getServiceAddr('hdfs')
#      if hdfsAddr.find(':') != -1:
#        h, p = hdfsAddr.split(':', 1)
#        self.hdfsHost = h
#        self.log.debug("hdfsHost: " + self.hdfsHost)
#      mapredAddr = self.getServiceAddr('mapred')
#      if mapredAddr.find(':') != -1:
#        h, p = mapredAddr.split(':', 1)
#        self.mapredHost = h
#        self.log.debug("mapredHost: " + self.mapredHost)
#      ringList = self.rm.serviceClient.getServiceInfo(self.cfg['hodring']['userid'],
#                                                      self.np.getServiceId(),
#                                                      'hodring', 'hod')
#      for ring in ringList:
#        ringXRAddress = ring['xrs']
#        if ringXRAddress == None:
#          raise Exception("Could not get hodring XML-RPC server address.")
#        ringClient = hodXRClient(ringXRAddress, None, None, 0, 0, 0, False)
#        self.log.debug("Invoking clusterStop on " + ringXRAddress)
#        ringThread = func(name='hodring_stop', functionRef=ringClient.clusterStop())
#        ring['thread'] = ringThread
#        ringThread.start()
#
#      for ring in ringList:
#        ringThread = ring['thread']
#        if ringThread == None:
#          raise Exception("Could not get hodring thread.")
#        ringThread.join()
#        self.log.debug("Completed clusterStop on " + ring['xrs'])
#
#    except:
#      self.log.debug(get_exception_string())
#      return False
#
#    self.log.debug("Successfully stopped cluster.")
#    
#    return True

  def getCommand(self, addr):
    """This method is called by the
    hodrings to get commands from
    the ringmaster"""
    lock = self.cmdLock
    cmdList = []
    lock.acquire()
    try:
      try:
        for v in self.serviceDict.itervalues():
          if (not v.isExternal()):
            if v.isLaunchable(self.serviceDict):
              # If a master is still not launched, or the number of 
              # retries for launching master is not reached, 
              # launch master
              if not v.isMasterLaunched() and \
                  (v.getMasterFailureCount() <= \
                      self.cfg['ringmaster']['max-master-failures']):
                cmdList = v.getMasterCommands(self.serviceDict)
                v.setlaunchedMaster()
                v.setMasterAddress(addr)
                break
        if cmdList == []:
          for s in self.serviceDict.itervalues():
            if (not v.isExternal()):
              if s.isMasterInitialized():
                cl = s.getWorkerCommands(self.serviceDict)
                cmdList.extend(cl)
              else:
                cmdList = []
                break
      except:
        self.log.debug(get_exception_string())
    finally:
      lock.release()
      pass
    
    cmd = addr + pformat(cmdList)
    self.log.debug("getCommand returning " + cmd)
    return cmdList
  
  def getAdminCommand(self, addr):
    """This method is called by the
    hodrings to get admin commands from
    the ringmaster"""
    lock = self.cmdLock
    cmdList = []
    lock.acquire()
    try:
      try:
        for v in self.serviceDict.itervalues():
          cmdList = v.getAdminCommands(self.serviceDict)
          if cmdList != []:
            break
      except Exception, e:
        self.log.debug(get_exception_string())
    finally:
      lock.release()
      pass
    cmd = addr + pformat(cmdList)
    self.log.debug("getAdminCommand returning " + cmd)
    return cmdList

  def addMasterParams(self, addr, vals):
    """This method is called by
    hodring to update any parameters
    its changed for the commands it was
    running"""
    self.log.debug('Comment: adding master params from %s' % addr)
    self.log.debug(pformat(vals))
    lock = self.masterParamLock
    lock.acquire()
    try:
      for v in self.serviceDict.itervalues():
        if v.isMasterLaunched():
          if (v.getMasterAddress() == addr):
            v.setMasterParams(vals)
            v.setMasterInitialized()
    except:
      self.log.debug(get_exception_string())
      pass
    lock.release()
            
    return addr

  def setHodRingErrors(self, addr, errors):
    """This method is called by the hodrings to update errors 
      it encountered while starting up"""
    self.log.critical("Hodring at %s failed with following errors:\n%s" \
                        % (addr, errors))
    lock = self.masterParamLock
    lock.acquire()
    try:
      for v in self.serviceDict.itervalues():
        if v.isMasterLaunched():
          if (v.getMasterAddress() == addr):
            # strip the PID part.
            idx = addr.rfind('_')
            if idx is not -1:
              addr = addr[:idx]
            v.setMasterFailed("Hodring at %s failed with following" \
                                " errors:\n%s" % (addr, errors))
    except:
      self.log.debug(get_exception_string())
      pass
    lock.release()
    return True

  def getKeys(self):
    lock= self.masterParamLock
    lock.acquire()
    keys = self.serviceDict.keys()
    lock.release()    
  
    return keys
  
  def getServiceAddr(self, name):
    addr = 'not found'
    self.log.debug("getServiceAddr name: %s" % name)
    lock= self.masterParamLock
    lock.acquire()
    try:
      service = self.serviceDict[name]
    except KeyError:
      pass
    else:
      self.log.debug("getServiceAddr service: %s" % service)
      # Check if we should give up ! If the limit on max failures is hit, 
      # give up.
      err = service.getMasterFailed()
      if (err is not None) and \
            (service.getMasterFailureCount() > \
                      self.cfg['ringmaster']['max-master-failures']):
        self.log.critical("Detected errors (%s) beyond allowed number"\
                            " of failures (%s). Flagging error to client" \
                            % (service.getMasterFailureCount(), \
                              self.cfg['ringmaster']['max-master-failures']))
        addr = "Error: " + err
      elif (service.isMasterInitialized()):
        addr = service.getMasterAddrs()[0]
      else:
        addr = 'not found'
    lock.release()
    self.log.debug("getServiceAddr addr %s: %s" % (name, addr))
    
    return addr

  def getURLs(self, name):
    addr = 'none'
    lock = self.masterParamLock
    lock.acquire()
    
    try:
      service = self.serviceDict[name]
    except KeyError:
      pass
    else:
      if (service.isMasterInitialized()):
        addr = service.getInfoAddrs()[0]
      
    lock.release()
    
    return addr

  def stopRM(self):
    """An XMLRPC call which will spawn a thread to stop the Ringmaster program."""
    # We spawn a thread here because we want the XMLRPC call to return. Calling
    # stop directly from here will also stop the XMLRPC server.
    try:
      self.log.debug("inside xml-rpc call to stop ringmaster")
      rmStopperThread = func('RMStopper', self.rm.stop)
      rmStopperThread.start()
      self.log.debug("returning from xml-rpc call to stop ringmaster")
      return True
    except:
      self.log.debug("Exception in stop: %s" % get_exception_string())
      return False

class RingMaster:
  def __init__(self, cfg, log, **kwds):
    """starts nodepool and services"""
    self.download = False
    self.httpServer = None
    self.cfg = cfg
    self.log = log
    self.__hostname = local_fqdn()
    self.workDirs = None 

    # ref to the idle job tracker object.
    self.__jtMonitor = None
    self.__idlenessDetected = False
    self.__stopInProgress = False
    self.__isStopped = False # to let main exit
    self.__exitCode = 0 # exit code with which the ringmaster main method should return

    self.workers_per_ring = self.cfg['ringmaster']['workers_per_ring']

    self.__initialize_signal_handlers()
    
    sdd = self.cfg['servicedesc']
    gsvc = None
    for key in sdd:
      gsvc = sdd[key]
      break
    
    npd = self.cfg['nodepooldesc']
    self.np = NodePoolUtil.getNodePool(npd, cfg, log)

    self.log.debug("Getting service ID.")
    
    self.serviceId = self.np.getServiceId()
    
    self.log.debug("Got service ID: %s" % self.serviceId)

    self.tarSrcLoc = None
    if self.cfg['ringmaster'].has_key('hadoop-tar-ball'):
      self.download = True
      self.tarSrcLoc = self.cfg['ringmaster']['hadoop-tar-ball']
 
    self.cd_to_tempdir()

    if (self.download):
      self.__copy_tarball(os.getcwd())
      self.basename = self.__find_tarball_in_dir(os.getcwd())
      if self.basename is None:
        raise Exception('Did not find tarball copied from %s in %s.'
                          % (self.tarSrcLoc, os.getcwd()))
      
    self.serviceAddr = to_http_url(self.cfg['ringmaster']['svcrgy-addr'])
    
    self.log.debug("Service registry @ %s" % self.serviceAddr)
    
    self.serviceClient = hodXRClient(self.serviceAddr)
    self.serviceDict  = {}
    try:
      sdl = self.cfg['servicedesc']

      workDirs = self.getWorkDirs(cfg)

      hdfsDesc = sdl['hdfs']
      hdfs = None
 
      # Determine hadoop Version
      hadoopVers = hadoopVersion(self.__getHadoopDir(), \
                                self.cfg['hodring']['java-home'], self.log)
     
      if (hadoopVers['major']==None) or (hadoopVers['minor']==None):
        raise Exception('Could not retrive the version of Hadoop.'
                        + ' Check the Hadoop installation or the value of the hodring.java-home variable.')
      if hdfsDesc.isExternal():
        hdfs = HdfsExternal(hdfsDesc, workDirs, version=int(hadoopVers['minor']))
        hdfs.setMasterParams( self.cfg['gridservice-hdfs'] )
      else:
        hdfs = Hdfs(hdfsDesc, workDirs, 0, version=int(hadoopVers['minor']),
                    workers_per_ring = self.workers_per_ring)

      self.serviceDict[hdfs.getName()] = hdfs
      
      mrDesc = sdl['mapred']
      mr = None
      if mrDesc.isExternal():
        mr = MapReduceExternal(mrDesc, workDirs, version=int(hadoopVers['minor']))
        mr.setMasterParams( self.cfg['gridservice-mapred'] )
      else:
        mr = MapReduce(mrDesc, workDirs,1, version=int(hadoopVers['minor']),
                       workers_per_ring = self.workers_per_ring)

      self.serviceDict[mr.getName()] = mr
    except:
      self.log.critical("Exception in creating Hdfs and Map/Reduce descriptor objects: \
                            %s." % get_exception_error_string())
      self.log.debug(get_exception_string())
      raise

    # should not be starting these in a constructor
    ringMasterServer.startService(self.serviceDict, cfg, self.np, log, self)
    
    self.rpcserver = ringMasterServer.getAddress()
    
    self.httpAddress = None   
    self.tarAddress = None 
    hostname = socket.gethostname()
    if (self.download):
      self.httpServer = threadedHTTPServer(hostname, 
        self.cfg['ringmaster']['http-port-range'])
      
      self.httpServer.serve_forever()
      self.httpAddress = "http://%s:%d/" % (self.httpServer.server_address[0], 
                                 self.httpServer.server_address[1])
      self.tarAddress = "%s%s" % (self.httpAddress, self.basename)
      
      ringMasterServer.instance.logMasterSources.registerTarSource(hostname, 
                                                                   self.tarAddress)
    else:
      self.log.debug("Download not set.")
    
    self.log.debug("%s %s %s %s %s" % (self.cfg['ringmaster']['userid'], 
      self.serviceId, self.__hostname, 'ringmaster', 'hod'))
    
    if self.cfg['ringmaster']['register']:      
      if self.httpAddress:
        self.serviceClient.registerService(self.cfg['ringmaster']['userid'], 
          self.serviceId, self.__hostname, 'ringmaster', 'hod', {
          'xrs' : self.rpcserver, 'http' : self.httpAddress })
      else:
        self.serviceClient.registerService(self.cfg['ringmaster']['userid'], 
          self.serviceId, self.__hostname, 'ringmaster', 'hod', {
          'xrs' : self.rpcserver, })
    
    self.log.debug("Registered with serivce registry: %s." % self.serviceAddr)
    
    hodRingPath = os.path.join(cfg['ringmaster']['base-dir'], 'bin', 'hodring')
    hodRingWorkDir = os.path.join(cfg['hodring']['temp-dir'], 'hodring' + '_' 
                                  + getpass.getuser())
    
    self.cfg['hodring']['hodring'] = [hodRingWorkDir,]
    self.cfg['hodring']['svcrgy-addr'] = self.cfg['ringmaster']['svcrgy-addr']
    self.cfg['hodring']['service-id'] = self.np.getServiceId()

    self.cfg['hodring']['ringmaster-xrs-addr'] = self.__url_to_addr(self.rpcserver)
    
    if (self.tarSrcLoc != None):
      cfg['hodring']['download-addr'] = self.tarAddress
 
    self.__init_job_tracker_monitor(ringMasterServer.instance.logMasterSources)

  def __init_job_tracker_monitor(self, logMasterSources):
    hadoopDir = self.__getHadoopDir()
    self.log.debug('hadoopdir=%s, java-home=%s' % \
                (hadoopDir, self.cfg['hodring']['java-home']))
    try:
      self.__jtMonitor = JobTrackerMonitor(self.log, self, 
                            self.cfg['ringmaster']['jt-poll-interval'], 
                            self.cfg['ringmaster']['idleness-limit'],
                            hadoopDir, self.cfg['hodring']['java-home'],
                            logMasterSources)
      self.log.debug('starting jt monitor')
      self.__jtMonitor.start()
    except:
      self.log.critical('Exception in running idle job tracker. This cluster cannot be deallocated if idle.\
                          Exception message: %s' % get_exception_error_string())
      self.log.debug('Exception details: %s' % get_exception_string())


  def __getHadoopDir(self):
    hadoopDir = None
    if self.cfg['ringmaster'].has_key('hadoop-tar-ball'):
      tarFile = os.path.join(os.getcwd(), self.basename)
      ret = untar(tarFile, os.getcwd())
      if not ret:
        raise Exception('Untarring tarfile %s to directory %s failed. Cannot find hadoop directory.' \
                            % (tarFile, os.getcwd()))
      hadoopDir = os.path.join(os.getcwd(), self.__get_dir(tarFile))
    else:
      hadoopDir = self.cfg['gridservice-mapred']['pkgs']
    self.log.debug('Returning Hadoop directory as: %s' % hadoopDir)
    return hadoopDir

  def __get_dir(self, name):
    """Return the root directory inside the tarball
    specified by name. Assumes that the tarball begins
    with a root directory."""
    import tarfile
    myTarFile = tarfile.open(name)
    hadoopPackage = myTarFile.getnames()[0]
    self.log.debug("tarball name : %s hadoop package name : %s" %(name,hadoopPackage))
    return hadoopPackage

  def __find_tarball_in_dir(self, dir):
    """Find the tarball among files specified in the given 
    directory. We need this method because how the tarball
    source URI is given depends on the method of copy and
    we can't get the tarball name from that.
    This method will fail if there are multiple tarballs
    in the directory with the same suffix."""
    files = os.listdir(dir)
    for file in files:
      if self.tarSrcLoc.endswith(file):
        return file
    return None

  def __copy_tarball(self, destDir):
    """Copy the hadoop tar ball from a remote location to the
    specified destination directory. Based on the URL it executes
    an appropriate copy command. Throws an exception if the command
    returns a non-zero exit code."""
    # for backwards compatibility, treat the default case as file://
    url = ''
    if self.tarSrcLoc.startswith('/'):
      url = 'file:/'
    src = '%s%s' % (url, self.tarSrcLoc)
    if src.startswith('file://'):
      src = src[len('file://')-1:]
      cpCmd = '/bin/cp'
      cmd = '%s %s %s' % (cpCmd, src, destDir)
      self.log.debug('Command to execute: %s' % cmd)
      copyProc = simpleCommand('remote copy', cmd)
      copyProc.start()
      copyProc.wait()
      copyProc.join()
      ret = copyProc.exit_code()
      self.log.debug('Completed command execution. Exit Code: %s.' % ret)

      if ret != 0:
        output = copyProc.output()
        raise Exception('Could not copy tarball using command %s. Exit code: %s. Output: %s' 
                        % (cmd, ret, output))
    else:
      raise Exception('Unsupported URL for file: %s' % src)

# input: http://hostname:port/. output: [hostname,port]
  def __url_to_addr(self, url):
    addr = url.rstrip('/')
    if addr.startswith('http://'):
      addr = addr.replace('http://', '', 1)
    addr_parts = addr.split(':')
    return [addr_parts[0], int(addr_parts[1])]

  def __initialize_signal_handlers(self): 
    def sigStop(sigNum, handler):
      sig_wrapper(sigNum, self.stop)
  
    signal.signal(signal.SIGTERM, sigStop)
    signal.signal(signal.SIGINT, sigStop)
    signal.signal(signal.SIGQUIT, sigStop)

  def __clean_up(self):
    tempDir = self.__get_tempdir()
    os.chdir(os.path.split(tempDir)[0])
    if os.path.exists(tempDir):
      shutil.rmtree(tempDir, True)
      
    self.log.debug("Cleaned up temporary dir: %s" % tempDir)

  def __get_tempdir(self):
    dir = os.path.join(self.cfg['ringmaster']['temp-dir'], 
                          "%s.%s.ringmaster" % (self.cfg['ringmaster']['userid'], 
                                                self.np.getServiceId()))
    return dir

  def getWorkDirs(self, cfg, reUse=False):

    if (not reUse) or (self.workDirs == None):
      import math
      frand = random.random()
      while math.ceil(frand) != math.floor(frand):
        frand = frand * 100

      irand = int(frand)
      uniq = '%s-%d-%s' % (socket.gethostname(), os.getpid(), irand)
      dirs = []
      parentDirs = cfg['ringmaster']['work-dirs']
      for p in parentDirs:
        dir = os.path.join(p, uniq)
        dirs.append(dir)
      self.workDirs = dirs

    return self.workDirs

  def _fetchLink(self, link, parentDir):
    parser = miniHTMLParser()
    self.log.debug("Checking link %s" %link)
    while link:

      # Get the file from the site and link
      input = urllib.urlopen(link)
      out = None
      contentType = input.info().gettype()
      isHtml = contentType == 'text/html'

      #print contentType
      if isHtml:
        parser.setBaseUrl(input.geturl())
      else:
        parsed = urlparse.urlparse(link)
        hp = parsed[1]
        h = hp
        p = None
        if hp.find(':') != -1:
          h, p = hp.split(':', 1)
        path = parsed[2]
        path = path.split('/')
        file = os.path.join(parentDir, h, p)
        for c in path:
          if c == '':
            continue
          file = os.path.join(file, c)

        try:
          self.log.debug('Creating %s' % file)
          dir, tail = os.path.split(file)
          if not os.path.exists(dir):
            os.makedirs(dir)
        except:
          self.log.debug(get_exception_string())

        out = open(file, 'w')

      bufSz = 8192
      buf = input.read(bufSz)
      while len(buf) > 0:
        if isHtml:
          # Feed the file into the HTML parser
          parser.feed(buf)
        if out:
          out.write(buf)
        buf = input.read(bufSz)

      input.close()
      if out:
        out.close()

      # Search the retfile here

      # Get the next link in level traversal order
      link = parser.getNextLink()
      
    parser.close()
    
  def _finalize(self):
    try:
      # FIXME: get dir from config
      dir = 'HOD-log-P%d' % (os.getpid())
      dir = os.path.join('.', dir)
    except:
      self.log.debug(get_exception_string())

    self.np.finalize()

  def handleIdleJobTracker(self):
    self.log.critical("Detected idle job tracker for %s seconds. The allocation will be cleaned up." \
                          % self.cfg['ringmaster']['idleness-limit'])
    self.__idlenessDetected = True

  def cd_to_tempdir(self):
    dir = self.__get_tempdir()
    
    if not os.path.exists(dir):
      os.makedirs(dir)
    os.chdir(dir)
    
    return dir
  
  def getWorkload(self):
    return self.workload

  def getHostName(self):
    return self.__hostname

  def start(self):
    """run the thread main loop"""
    
    self.log.debug("Entered start method.")
    hodring = os.path.join(self.cfg['ringmaster']['base-dir'], 
                           'bin', 'hodring')
    largs = [hodring]
    targs = self.cfg.get_args(section='hodring')
    largs.extend(targs) 
    
    hodringCmd = ""
    for item in largs:
      hodringCmd = "%s%s " % (hodringCmd, item)
      
    self.log.debug(hodringCmd)
    
    if self.np.runWorkers(largs) > 0:
      self.log.critical("Failed to start worker.")
    
    self.log.debug("Returned from runWorkers.")
    
    self._finalize()

  def __findExitCode(self):
    """Determine the exit code based on the status of the cluster or jobs run on them"""
    xmlrpcServer = ringMasterServer.instance.logMasterSources
    if xmlrpcServer.getServiceAddr('hdfs') == 'not found' or \
        xmlrpcServer.getServiceAddr('hdfs').startswith("Error: "):
      self.__exitCode = 7
    elif xmlrpcServer.getServiceAddr('mapred') == 'not found' or \
        xmlrpcServer.getServiceAddr('mapred').startswith("Error: "):
      self.__exitCode = 8
    else:
      clusterStatus = get_cluster_status(xmlrpcServer.getServiceAddr('hdfs'),
                                          xmlrpcServer.getServiceAddr('mapred'))
      if clusterStatus != 0:
        self.__exitCode = clusterStatus
      else:
        self.__exitCode = self.__findHadoopJobsExitCode()
    self.log.debug('exit code %s' % self.__exitCode)

  def __findHadoopJobsExitCode(self):
    """Determine the consolidate exit code of hadoop jobs run on this cluster, provided
       this information is available. Return 0 otherwise"""
    ret = 0
    failureStatus = 3
    failureCount = 0
    if self.__jtMonitor:
      jobStatusList = self.__jtMonitor.getJobsStatus()
      try:
        if len(jobStatusList) > 0:
          for jobStatus in jobStatusList:
            self.log.debug('job status for %s: %s' % (jobStatus.getJobId(), 
                                                      jobStatus.getStatus()))
            if jobStatus.getStatus() == failureStatus:
              failureCount = failureCount+1
        if failureCount > 0:
          if failureCount == len(jobStatusList): # all jobs failed
            ret = 16
          else:
            ret = 17
      except:
        self.log.debug('exception in finding hadoop jobs exit code' % get_exception_string())
    return ret

  def stop(self):
    self.log.debug("RingMaster stop method invoked.")
    if self.__stopInProgress or self.__isStopped:
      return
    self.__stopInProgress = True
    if ringMasterServer.instance is not None:
      self.log.debug('finding exit code')
      self.__findExitCode()
      self.log.debug('stopping ringmaster instance')
      ringMasterServer.stopService()
    else:
      self.__exitCode = 6
    if self.__jtMonitor is not None:
      self.__jtMonitor.stop()
    if self.httpServer:
      self.httpServer.stop()
      
    self.__clean_up()
    self.__isStopped = True

  def shouldStop(self):
    """Indicates whether the main loop should exit, either due to idleness condition, 
    or a stop signal was received"""
    return self.__idlenessDetected or self.__isStopped

  def getExitCode(self):
    """return the exit code of the program"""
    return self.__exitCode

def main(cfg,log):
  try:
    rm = None
    dGen = DescGenerator(cfg)
    cfg = dGen.initializeDesc()
    rm = RingMaster(cfg, log)
    rm.start()
    while not rm.shouldStop():
      time.sleep(1)
    rm.stop()
    log.debug('returning from main')
    return rm.getExitCode()
  except Exception, e:
    if log:
      log.critical(get_exception_string())
    raise Exception(e)
